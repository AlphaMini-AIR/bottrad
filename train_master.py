# train_master.py - Version 4.2 (Final - Bảo tồn tinh túy)
import pandas as pd
import numpy as np
import xgboost as xgb
import onnxmltools
import json
import os
from sklearn.model_selection import train_test_split
from sklearn.metrics import average_precision_score
from onnxmltools.convert.common.data_types import FloatTensorType

print("🧠 [AI PIPELINE] Khởi tạo hệ thống Discovery V4.2...")

# --- GIỮ NGUYÊN BỘ THAM SỐ CHIẾN THUẬT ĐỂ AI THỬ NGHIỆM ---
STRATEGY_PROFILES = {
    "SWING": {"risk": 0.01, "reward": 0.03, "atr_mult": 1.5, "min_prob": 0.70},
    "SCALP": {"risk": 0.002, "reward": 0.006, "atr_mult": 2.5, "min_prob": 0.65},
    "RANGE": {"risk": 0.005, "reward": 0.01, "atr_mult": 1.0, "min_prob": 0.60}
}

# ---------------------------------------------------------
# 1. TÍNH TOÁN QUANT FEATURES (GIỮ NGUYÊN TINH TÚY)
# ---------------------------------------------------------
def calculate_quant_features(df):
    df = df.sort_values(by=['symbol', 'timestamp']).copy()

    def apply_features(group):
        ret_1m = group['close'].pct_change(1)
        ret_20m = group['close'].pct_change(20)
        group['hurst_proxy'] = ret_20m.rolling(20).std() / (np.sqrt(20) * ret_1m.rolling(20).std() + 1e-8)

        rolling_pv = (group['close'] * group['volume']).rolling(window=240).sum()
        rolling_vol = group['volume'].rolling(window=240).sum() + 1e-8
        group['vwap_4h'] = rolling_pv / rolling_vol
        group['dist_vwap'] = (group['close'] - group['vwap_4h']) / group['vwap_4h']

        body = abs(group['close'] - group['open']) + 1e-8
        wick = group['high'] - group['low']
        group['wick_to_body'] = wick / body
        group['vol_acceleration'] = group['volume'] / (group['volume'].rolling(window=15).mean() + 1e-8)

        if 'funding_rate' in group.columns:
            group['funding_delta_12h'] = group['funding_rate'].diff(720).fillna(0)
        else:
            group['funding_delta_12h'] = 0.0
        return group

    result_dfs = []
    for sym, group in df.groupby('symbol'):
        result_dfs.append(apply_features(group.copy()))
    return pd.concat(result_dfs).dropna()

# ---------------------------------------------------------
# 2. GÁN NHÃN TRIPLE-BARRIER (GIỮ NGUYÊN LOGIC VECTORIZED)
# ---------------------------------------------------------
def apply_triple_barrier(df, risk, reward, time_limit=240):
    def process_labels(group):
        closes, highs, lows = group['close'].values, group['high'].values, group['low'].values
        labels = np.zeros(len(closes))

        for i in range(len(closes) - time_limit):
            tp_price, sl_price = closes[i] * (1 + reward), closes[i] * (1 - risk)
            f_highs, f_lows = highs[i+1 : i+time_limit+1], lows[i+1 : i+time_limit+1]

            hit_tp_mask = f_highs >= tp_price
            hit_sl_mask = f_lows <= sl_price

            hit_tp_idx = np.argmax(hit_tp_mask) if np.any(hit_tp_mask) else -1
            hit_sl_idx = np.argmax(hit_sl_mask) if np.any(hit_sl_mask) else -1

            if hit_tp_idx != -1 and (hit_sl_idx == -1 or hit_tp_idx < hit_sl_idx):
                labels[i] = 1 
            else:
                labels[i] = 0  

        group['target'] = labels
        return group.iloc[:-time_limit]

    result_dfs = []
    for sym, group in df.groupby('symbol'):
        result_dfs.append(process_labels(group.copy()))
    return pd.concat(result_dfs)

# ---------------------------------------------------------
# 3. HUẤN LUYỆN & KHÁM PHÁ CHIẾN THUẬT TỐI ƯU
# ---------------------------------------------------------
def train_and_export(df):
    features = ['hurst_proxy', 'dist_vwap', 'wick_to_body', 'vol_acceleration', 'funding_delta_12h']
    white_list_metadata = {}
    
    for symbol in df['symbol'].unique():
        print(f"\n🚀 Đang tìm 'Sở trường' cho {symbol}...")
        coin_raw = df[df['symbol'] == symbol].copy()
        
        best_aucpr = 0
        best_meta = None
        best_model = None

        # AI thử nghiệm từng bộ Profile chiến thuật
        for name, config in STRATEGY_PROFILES.items():
            labeled_df = apply_triple_barrier(coin_raw.copy(), config['risk'], config['reward'])
            
            if len(labeled_df) < 500: continue
            
            X, y = labeled_df[features].values, labeled_df['target'].values
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

            if np.sum(y_train == 1) < 5: continue # Bỏ qua nếu quá ít lệnh thắng để học

            model = xgb.XGBClassifier(
                n_estimators=150, max_depth=5, learning_rate=0.03,
                scale_pos_weight=(len(y_train) - np.sum(y_train)) / (np.sum(y_train) + 1e-8)
            )
            model.fit(X_train, y_train)

            y_pred_proba = model.predict_proba(X_test)[:, 1]
            aucpr = average_precision_score(y_test, y_pred_proba)
            print(f"   - Thử nghiệm {name}: AUCPR = {aucpr:.4f}")

            if aucpr > best_aucpr:
                best_aucpr = aucpr
                best_model = model
                best_meta = {**config, "strategy": name, "aucpr": round(aucpr, 4)}

        # Kiểm tra KCS: Nếu chiến thuật tốt nhất đạt chuẩn > 0.15
        if best_aucpr > 0.15:
            print(f"✅ CHỐT: {symbol} áp dụng chiến thuật {best_meta['strategy']}")
            white_list_metadata[symbol] = best_meta
            
            # Xuất bộ não ONNX riêng cho từng Coin để tối ưu độ chính xác
            initial_type = [('float_input', FloatTensorType([None, len(features)]))]
            onnx_model = onnxmltools.convert_xgboost(best_model, initial_types=initial_type)
            with open(f"model_{symbol}.onnx", "wb") as f:
                f.write(onnx_model.SerializeToString())
        else:
            print(f"❌ {symbol}: Không tìm thấy chiến thuật hiệu quả. Tạm giam.")

    with open("white_list.json", "w") as f:
        json.dump({"active_coins": white_list_metadata}, f, indent=4)
    print("\n📜 Đã cập nhật white_list.json với các bộ cấu hình động.")

if __name__ == "__main__":
    test_csv = 'dataset_multi_3months.csv'
    if os.path.exists(test_csv):
        df = pd.read_csv(test_csv)
        df = calculate_quant_features(df)
        train_and_export(df)