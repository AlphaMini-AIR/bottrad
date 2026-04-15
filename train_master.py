"""
train_master_v8_fixed.py - AI Quant Sniper V5.3 (BALANCED LABELS)
Sửa lỗi thiên lệch nhãn: mỗi thời điểm tạo 2 mẫu LONG và SHORT riêng biệt.
Huấn luyện mô hình XGBoost 8 đặc trưng và chọn chiến lược tối ưu.
"""

import pandas as pd
import numpy as np
import xgboost as xgb
import onnxmltools
import optuna
import json
import os
import glob
import warnings
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import average_precision_score
from onnxmltools.convert.common.data_types import FloatTensorType

warnings.filterwarnings('ignore')

# --- CẤU HÌNH CHIẾN LƯỢC (Khớp với white_list.json) ---
STRATEGY_PROFILES = {
    "SCALP": {"sl_mult": 1.0, "tp_mult": 2.5, "time_limit": 60,  "fee_buffer": 0.0012},
    "RANGE": {"sl_mult": 1.5, "tp_mult": 3.75, "time_limit": 240, "fee_buffer": 0.0015},
    "SWING": {"sl_mult": 3.0, "tp_mult": 7.5, "time_limit": 720, "fee_buffer": 0.0020}
}

# 8 ĐẶC TRƯNG THEO ĐÚNG THỨ TỰ AiEngine.js
FEATURES = [
    'hurst_proxy',
    'dist_vwap',
    'wick_to_body',
    'vol_acceleration',
    'funding_delta_12h',
    'atr_norm',
    'rsi',
    'vpin'
]

# ------------------------------------------------------------
# 1. TÍNH TOÁN CÁC ĐẶC TRƯNG KỸ THUẬT TỪ OHLCV
# ------------------------------------------------------------
def calculate_quant_features(df):
    """Tính 7 đặc trưng đầu tiên từ dữ liệu nến (VPIN sẽ merge sau)"""
    df = df.copy()
    if 'symbol' not in df.columns:
        df['symbol'] = 'DEFAULT'
    df = df.sort_values(by=['symbol', 'timestamp'])

    all_groups = []
    for symbol, group in df.groupby('symbol'):
        group = group.copy()
        
        # 1. Hurst Proxy
        ret1m = group['close'].pct_change(1)
        ret20m = group['close'].pct_change(20)
        std1m = ret1m.rolling(20).std(ddof=1)
        std20m = ret20m.rolling(20).std(ddof=1)
        group['hurst_proxy'] = std20m / (np.sqrt(20) * std1m + 1e-8)
        
        # 2. Distance to VWAP 4h
        pv = group['close'] * group['volume']
        vwap4h = pv.rolling(240).sum() / (group['volume'].rolling(240).sum() + 1e-8)
        group['dist_vwap'] = (group['close'] - vwap4h) / (vwap4h + 1e-8)
        
        # 3. Candle Behavior
        body = abs(group['close'] - group['open']) + 1e-8
        group['wick_to_body'] = (group['high'] - group['low']) / body
        group['vol_acceleration'] = group['volume'] / (group['volume'].rolling(15).mean() + 1e-8)
        
        # 4. Funding Delta 12h
        if 'funding_rate' in group.columns:
            group['funding_delta_12h'] = group['funding_rate'].diff(720).fillna(0)
        else:
            group['funding_delta_12h'] = 0.0
            
        # 5. ATR Norm
        h_l = group['high'] - group['low']
        h_pc = abs(group['high'] - group['close'].shift(1))
        l_pc = abs(group['low'] - group['close'].shift(1))
        tr = pd.concat([h_l, h_pc, l_pc], axis=1).max(axis=1)
        group['atr_norm'] = (tr.rolling(14).mean()) / (group['close'] + 1e-8)
        
        # 6. RSI 14
        delta = group['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        group['rsi'] = 100 - (100 / (1 + (gain / (loss + 1e-8))))
        
        all_groups.append(group)
        
    return pd.concat(all_groups).dropna()

# ------------------------------------------------------------
# 2. GÁN NHÃN CÂN BẰNG: MỖI THỜI ĐIỂM TẠO 2 MẪU (LONG & SHORT)
# ------------------------------------------------------------
def apply_triple_barrier_balanced(df, config):
    """
    Tạo DataFrame với mỗi hàng là một cặp (thời điểm, hướng).
    Nhãn = 1 nếu TP chạm trước SL cho hướng đó.
    """
    closes = df['close'].values
    highs = df['high'].values
    lows = df['low'].values
    atr = (df['atr_norm'] * closes).values
    limit = config['time_limit']
    fee_buffer = config['fee_buffer']
    tp_mult = config['tp_mult']
    sl_mult = config['sl_mult']

    rows = []
    n = len(closes)
    for i in range(n - limit):
        # ---- LONG ----
        tp_long = closes[i] + atr[i] * tp_mult + closes[i] * fee_buffer
        sl_long = closes[i] - atr[i] * sl_mult

        # ---- SHORT ----
        tp_short = closes[i] - atr[i] * tp_mult - closes[i] * fee_buffer
        sl_short = closes[i] + atr[i] * sl_mult

        window_h = highs[i+1 : i+limit+1]
        window_l = lows[i+1 : i+limit+1]

        # LONG
        tp_idx_long = np.where(window_h >= tp_long)[0]
        sl_idx_long = np.where(window_l <= sl_long)[0]
        first_tp = tp_idx_long[0] if len(tp_idx_long) > 0 else limit+1
        first_sl = sl_idx_long[0] if len(sl_idx_long) > 0 else limit+1
        long_win = 1 if first_tp < first_sl else 0

        # SHORT
        tp_idx_short = np.where(window_l <= tp_short)[0]
        sl_idx_short = np.where(window_h >= sl_short)[0]
        first_tp_s = tp_idx_short[0] if len(tp_idx_short) > 0 else limit+1
        first_sl_s = sl_idx_short[0] if len(sl_idx_short) > 0 else limit+1
        short_win = 1 if first_tp_s < first_sl_s else 0

        base_row = df.iloc[i].to_dict()
        # Dòng LONG
        row_long = base_row.copy()
        row_long['target'] = long_win
        row_long['side'] = 'LONG'
        rows.append(row_long)

        # Dòng SHORT
        row_short = base_row.copy()
        row_short['target'] = short_win
        row_short['side'] = 'SHORT'
        rows.append(row_short)

    return pd.DataFrame(rows)

# ------------------------------------------------------------
# 3. HUẤN LUYỆN & XUẤT ONNX CHO TỪNG COIN
# ------------------------------------------------------------
def train_and_export(df):
    white_list_metadata = {}
    symbols = df['symbol'].unique()

    for symbol in symbols:
        print(f"\n🧠 [ARCHITECT] Đang phân tích sở trường cho {symbol}...")
        coin_data = df[df['symbol'] == symbol].copy()
        
        best_score = -1
        best_profile = None
        best_params = None

        for name, config in STRATEGY_PROFILES.items():
            # Sử dụng hàm balanced mới
            labeled_df = apply_triple_barrier_balanced(coin_data.copy(), config)
            
            # Yêu cầu tối thiểu 2000 mẫu (do đã nhân đôi) và tỷ lệ win không quá thấp
            if len(labeled_df) < 2000 or labeled_df['target'].sum() < 100:
                continue

            X = labeled_df[FEATURES].values
            y = labeled_df['target'].values
            tscv = TimeSeriesSplit(n_splits=3)

            def objective(trial):
                params = {
                    'n_estimators': trial.suggest_int('n_estimators', 80, 200),
                    'max_depth': trial.suggest_int('max_depth', 3, 6),
                    'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1),
                    'subsample': 0.7,
                    'colsample_bytree': 0.7,
                    'scale_pos_weight': min((len(y) - sum(y)) / (sum(y) + 1e-8), 10.0),
                    'verbosity': 0,
                    'use_label_encoder': False,
                    'eval_metric': 'logloss'
                }
                scores = []
                for train_idx, val_idx in tscv.split(X):
                    if sum(y[train_idx]) < 10:
                        return 0.0
                    model = xgb.XGBClassifier(**params)
                    model.fit(X[train_idx], y[train_idx])
                    preds = model.predict_proba(X[val_idx])[:, 1]
                    scores.append(average_precision_score(y[val_idx], preds))
                return np.mean(scores)

            study = optuna.create_study(direction='maximize')
            optuna.logging.set_verbosity(optuna.logging.WARNING)
            study.optimize(objective, n_trials=20, show_progress_bar=False)

            if study.best_value > best_score:
                best_score = study.best_value
                best_profile = name
                best_params = study.best_params

        if best_score > 0.20:
            print(f"🔥 {symbol} CHỐT: {best_profile} | AUC-PR: {best_score:.4f}")
            
            # Huấn luyện mô hình cuối cùng
            final_df = apply_triple_barrier_balanced(coin_data.copy(), STRATEGY_PROFILES[best_profile])
            final_model = xgb.XGBClassifier(**best_params)
            final_model.fit(final_df[FEATURES].values, final_df['target'].values)
            
            # Xuất ONNX (8 đầu vào)
            initial_type = [('float_input', FloatTensorType([None, 8]))]
            onnx_model = onnxmltools.convert_xgboost(final_model, initial_types=initial_type)
            model_path = f"model_{symbol}.onnx"
            with open(model_path, "wb") as f:
                f.write(onnx_model.SerializeToString())
            print(f"   ✅ Đã xuất mô hình: {model_path}")
            
            # Lưu metadata cho white_list.json
            white_list_metadata[symbol] = {
                **STRATEGY_PROFILES[best_profile],
                "strategy": best_profile,
                "aucpr": round(best_score, 4)
            }
        else:
            print(f"⚠️ {symbol} LOẠI BỎ (AUC-PR <= 0.20).")

    with open("white_list.json", "w") as f:
        json.dump({"active_coins": white_list_metadata}, f, indent=4)
    print(f"\n📄 Đã cập nhật white_list.json với {len(white_list_metadata)} coin.")

# ------------------------------------------------------------
# 4. MAIN PIPELINE
# ------------------------------------------------------------
if __name__ == "__main__":
    if not os.path.exists('dataset_multi_3months.csv'):
        print("❌ Không tìm thấy file dataset_multi_3months.csv")
        exit(1)
        
    print("📂 Đang nạp file Nến thô...")
    df_raw = pd.read_csv('dataset_multi_3months.csv')
    df_raw['timestamp'] = df_raw['timestamp'].astype(int)
    
    print("🧮 Đang tính toán 7 Đặc trưng kỹ thuật...")
    df_feat = calculate_quant_features(df_raw)
    
    print("🔗 Đang tìm và ghép nối dữ liệu VPIN...")
    vpin_files = glob.glob('./vpin_cleaned/*_VPIN_Cleaned.csv')
    if vpin_files:
        vpin_dfs = []
        for f in vpin_files:
            symbol = os.path.basename(f).split('_')[0]
            vdf = pd.read_csv(f)
            vdf['timestamp'] = vdf['timestamp'].astype(int)
            vdf['symbol'] = symbol
            vpin_dfs.append(vdf)
        all_vpin_df = pd.concat(vpin_dfs, ignore_index=True)
        df_feat = pd.merge(df_feat, all_vpin_df, on=['symbol', 'timestamp'], how='left')
        df_feat['vpin'] = df_feat['vpin'].fillna(0.0)
        print("✅ Đã ghép VPIN thành công!")
    else:
        print("⚠️ CẢNH BÁO: Không tìm thấy thư mục 'vpin_cleaned'. Gán VPIN = 0 tạm thời.")
        df_feat['vpin'] = 0.0

    before_drop = len(df_feat)
    df_feat = df_feat.dropna(subset=FEATURES).reset_index(drop=True)
    print(f"🧹 Đã loại bỏ {before_drop - len(df_feat)} dòng chứa giá trị NaN.")
    
    if len(df_feat) == 0:
        print("❌ Không còn dữ liệu sau khi lọc NaN.")
        exit(1)

    train_and_export(df_feat)
    print("\n🎉 Hoàn tất huấn luyện! Hãy kiểm tra các file model_*.onnx và white_list.json.")