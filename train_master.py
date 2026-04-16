"""
train_master_v9.py - KỶ NGUYÊN SĂN THANH KHOẢN (PRO TRADER)
Khắc phục Win Rate thấp: Ép AI chỉ học các điểm vào lệnh SAU KHI cá mập quét thanh khoản (Liquidity Sweep).
Đồng bộ 100% logic Stoploss/TakeProfit với môi trường Node.js.
"""

import pandas as pd
import numpy as np
import xgboost as xgb
import onnxmltools
from onnxmltools.convert.common.data_types import FloatTensorType
import warnings

warnings.filterwarnings('ignore')

FEATURES = [
    'vpin', 'funding_rate', 'ob_imb_norm', 
    'bull_rejection', 'bear_rejection', 'bear_trap', 'bull_trap',
    'trend_15m', 'trend_4h', 'dist_to_4h_ema', 
    'is_dead_zone', 'is_london_open', 'is_ny_open', 
    'exhaustion_ratio', 'spoofing_index', 
    'fvg_15m_bull_mitigation', 'fvg_15m_bear_mitigation', 
    'dist_to_swing_high', 'dist_to_swing_low', 'cum_delta_div'
]

TIME_LIMIT = 720 # Giới hạn ôm lệnh 12 tiếng giống hệt Node.js

def create_expert_labels(df):
    print("🎓 Người Thầy Ảo V9.0 đang rà soát các Cú Quét Thanh Khoản (Sweep)...")
    
    closes = df['close'].values
    highs = df['high'].values
    lows = df['low'].values
    opens = df['open'].values
    
    swing_highs = df['swing_high'].values
    swing_lows = df['swing_low'].values
    
    targets = np.full(len(df), 2) # Mặc định là 2 (Thị trường Rác/Nhiễu)
    
    for i in range(len(df) - TIME_LIMIT):
        # 1. ĐIỀU KIỆN TIÊN QUYẾT: QUÉT THANH KHOẢN (LIQUIDITY SWEEP)
        # Chỉ xét Long nếu nến hiện tại chọc thủng Đáy cũ nhưng đóng cửa cao hơn (Rút chân)
        is_bull_sweep = (lows[i] < swing_lows[i]) and (closes[i] > swing_lows[i])
        
        # Chỉ xét Short nếu nến hiện tại chọc thủng Đỉnh cũ nhưng đóng cửa thấp hơn (Rút râu trên)
        is_bear_sweep = (highs[i] > swing_highs[i]) and (closes[i] < swing_highs[i])
        
        # Nếu không có Sweep, lập tức bỏ qua (Lớp 2 - Nhiễu)
        if not is_bull_sweep and not is_bear_sweep:
            continue
            
        # 2. MÔ PHỎNG EXACT LOGIC CỦA DEEPTHINKER (ĐỒNG BỘ 100%)
        # Cắt lỗ cứng có đệm ATR để không bị quét lần 2
        atr_proxy = max(highs[i] - lows[i], abs(highs[i] - closes[i-1]))
        
        sl_price_long = swing_lows[i] - (atr_proxy * 0.5) 
        tp_price_long = swing_highs[i] + (atr_proxy * 1.0)
        
        sl_price_short = swing_highs[i] + (atr_proxy * 0.5)
        tp_price_short = swing_lows[i] - (atr_proxy * 1.0)
        
        window_h = highs[i+1 : i+TIME_LIMIT+1]
        window_l = lows[i+1 : i+TIME_LIMIT+1]
        
        if is_bull_sweep:
            hit_tp_idx = np.where(window_h >= tp_price_long)[0]
            hit_sl_idx = np.where(window_l <= sl_price_long)[0]
            first_tp = hit_tp_idx[0] if len(hit_tp_idx) > 0 else TIME_LIMIT + 1
            first_sl = hit_sl_idx[0] if len(hit_sl_idx) > 0 else TIME_LIMIT + 1
            
            # Nếu chạm TP trước SL, và Trend 4H đang tăng -> Lệnh Long Hoàn Hảo (1)
            if first_tp < first_sl and df['trend_4h'].iloc[i] == 1 and df['is_dead_zone'].iloc[i] == 0:
                targets[i] = 1
                
        elif is_bear_sweep:
            hit_tp_idx = np.where(window_l <= tp_price_short)[0]
            hit_sl_idx = np.where(window_h >= sl_price_short)[0]
            first_tp = hit_tp_idx[0] if len(hit_tp_idx) > 0 else TIME_LIMIT + 1
            first_sl = hit_sl_idx[0] if len(hit_sl_idx) > 0 else TIME_LIMIT + 1
            
            # Nếu chạm TP trước SL, và Trend 4H đang giảm -> Lệnh Short Hoàn Hảo (0)
            if first_tp < first_sl and df['trend_4h'].iloc[i] == -1 and df['is_dead_zone'].iloc[i] == 0:
                targets[i] = 0

    df['expert_target'] = targets
    return df

def train_and_export_v9(df):
    symbols = df['symbol'].unique()
    
    for symbol in symbols:
        print(f"\n🤖 Đang rèn luyện AI V9.0 (Săn Thanh Khoản) cho {symbol}...")
        coin_data = df[df['symbol'] == symbol].copy()
        labeled_df = create_expert_labels(coin_data)
        
        X = labeled_df[FEATURES].values
        y = labeled_df['expert_target'].values
        
        count_long = sum(y == 1)
        count_short = sum(y == 0)
        count_noise = sum(y == 2)
        
        print(f"🎯 Lệnh Tỉa LONG: {count_long} | Lệnh Tỉa SHORT: {count_short} | Bỏ qua (Nhiễu): {count_noise}")
        
        if count_long < 5 or count_short < 5:
            print(f"⚠️ {symbol} không có đủ Cú quét thanh khoản chuẩn. Cần thêm data. Tạm bỏ qua.")
            continue
            
        # Cân bằng trọng số (Khóa trần 20x)
        raw_weight_long = count_noise / (count_long + 1e-8)
        raw_weight_short = count_noise / (count_short + 1e-8)
        MAX_WEIGHT = 20.0 
        
        weights = np.ones(len(y))
        weights[y == 1] = min(raw_weight_long, MAX_WEIGHT)
        weights[y == 0] = min(raw_weight_short, MAX_WEIGHT)
        
        # Mô hình cực đoan hơn một chút để nhận diện nến quét (Tăng max_depth)
        model = xgb.XGBClassifier(
            objective='multi:softprob',
            num_class=3,
            n_estimators=200,
            max_depth=7,
            learning_rate=0.03,
            subsample=0.8,
            colsample_bytree=0.8,
            eval_metric='mlogloss',
            random_state=42
        )
        
        print("⏳ Đang huấn luyện AI học cách bắt dao rơi...")
        model.fit(X, y, sample_weight=weights)
        
        initial_type = [('float_input', FloatTensorType([None, len(FEATURES)]))]
        onnx_model = onnxmltools.convert_xgboost(model, initial_types=initial_type)
        
        model_path = f"model_{symbol}.onnx"
        with open(model_path, "wb") as f:
            f.write(onnx_model.SerializeToString())
            
        print(f"✅ Xuất xưởng thành công: {model_path}")

if __name__ == "__main__":
    input_file = "dataset_v8_29features.csv"
    print(f"📂 Đang nạp Sách giáo khoa từ: {input_file}")
    try:
        df_raw = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"❌ Lỗi: Không tìm thấy file {input_file}")
        exit()
        
    df_clean = df_raw.fillna(0)
    train_and_export_v9(df_clean)
    print("\n🎉 HOÀN TẤT V9! AI BÂY GIỜ CHỈ ĐÁNH KHI ĐÃ CÓ THANH KHOẢN BỊ QUÉT.")