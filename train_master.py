import pandas as pd
import numpy as np
import xgboost as xgb
import onnxmltools
import optuna
import json
import os
import warnings
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import average_precision_score
from onnxmltools.convert.common.data_types import FloatTensorType

warnings.filterwarnings('ignore')

# --- DANH MỤC CHIẾN THUẬT (Khớp với StrategyRouter.js) ---
STRATEGY_PROFILES = {
    "SCALP": {"sl_mult": 1.0, "tp_mult": 2.5, "time_limit": 60,  "fee_buffer": 0.0012},
    "RANGE": {"sl_mult": 1.5, "tp_mult": 3.75, "time_limit": 240, "fee_buffer": 0.0015},
    "SWING": {"sl_mult": 3.0, "tp_mult": 7.5, "time_limit": 720, "fee_buffer": 0.0020}
}

# THỨ TỰ 7 ĐẶC TRƯNG - PHẢI KHỚP TUYỆT ĐỐI VỚI AiEngine.js
FEATURES = [
    'hurst_proxy',        # 1
    'dist_vwap',         # 2
    'wick_to_body',      # 3
    'vol_acceleration',  # 4
    'funding_delta_12h', # 5
    'atr_norm',          # 6
    'rsi'                # 7
]

def calculate_quant_features(df):
    """Tính toán 7 đặc trưng khớp với logic Node.js"""
    df = df.copy()
    if 'symbol' not in df.columns: df = df.reset_index()
    df = df.sort_values(by=['symbol', 'timestamp'])
    
    all_groups = []
    for symbol, group in df.groupby('symbol'):
        group = group.copy()
        
        # 1. Hurst Proxy (Dùng ddof=1 để khớp với N-1 trong JS)
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
        
        # 4. Funding Delta 12h (720 nến)
        if 'funding_rate' in group.columns:
            group['funding_delta_12h'] = group['funding_rate'].diff(720).fillna(0)
        else:
            group['funding_delta_12h'] = 0.0
            
        # 5. ATR Norm (Khớp logic JS: average_tr / price)
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

def apply_triple_barrier(df, config):
    """Dán nhãn linh động theo từng khung thời gian chiến thuật"""
    closes = df['close'].values
    highs = df['high'].values
    lows = df['low'].values
    # Sử dụng ATR tuyệt đối để tính giá TP/SL
    atr = (df['atr_norm'] * df['close']).values 
    
    labels = np.zeros(len(closes))
    limit = config['time_limit']

    for i in range(len(closes) - limit):
        # Tính toán giá mục tiêu
        tp_price = closes[i] + (atr[i] * config['tp_mult']) + (closes[i] * config['fee_buffer'])
        sl_price = closes[i] - (atr[i] * config['sl_mult'])
        
        window_highs = highs[i+1 : i+limit+1]
        window_lows = lows[i+1 : i+limit+1]
        
        tp_hit = np.where(window_highs >= tp_price)[0]
        sl_hit = np.where(window_lows <= sl_price)[0]
        
        first_tp = tp_hit[0] if len(tp_hit) > 0 else 999
        first_sl = sl_hit[0] if len(sl_hit) > 0 else 999
        
        if first_tp < first_sl and first_tp != 999:
            labels[i] = 1 # Thắng
            
    df['target'] = labels
    return df.iloc[:-limit]

def train_and_export(df):
    white_list_metadata = {}
    symbols = df['symbol'].unique()

    for symbol in symbols:
        print(f"\n🧠 [ARCHITECT] Đang phân tích sở trường cho {symbol}...")
        coin_data = df[df['symbol'] == symbol].copy()
        
        best_score = -1
        best_profile = None
        best_model = None
        best_params = None

        for name, config in STRATEGY_PROFILES.items():
            labeled_df = apply_triple_barrier(coin_data.copy(), config)
            if len(labeled_df) < 1000: continue # Đảm bảo đủ dữ liệu học
            
            X, y = labeled_df[FEATURES].values, labeled_df['target'].values
            tscv = TimeSeriesSplit(n_splits=3)
            
            def objective(trial):
                params = {
                    'n_estimators': trial.suggest_int('n_estimators', 80, 200),
                    'max_depth': trial.suggest_int('max_depth', 3, 6),
                    'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1),
                    'subsample': 0.7,
                    'colsample_bytree': 0.7,
                    'scale_pos_weight': (len(y) - sum(y)) / (sum(y) + 1e-8), # Xử lý lệch nhãn
                    'verbosity': 0
                }
                scores = []
                for train_idx, val_idx in tscv.split(X):
                    if sum(y[train_idx]) < 10: return 0
                    model = xgb.XGBClassifier(**params)
                    model.fit(X[train_idx], y[train_idx])
                    preds = model.predict_proba(X[val_idx])[:, 1]
                    scores.append(average_precision_score(y[val_idx], preds))
                return np.mean(scores)

            study = optuna.create_study(direction='maximize')
            study.optimize(objective, n_trials=15)
            
            if study.best_value > best_score:
                best_score = study.best_value
                best_profile = name
                best_params = study.best_params

        # Điều kiện duyệt: AUC-PR phải đạt ngưỡng "thông minh"
        if best_score > 0.20:
            print(f"🔥 {symbol} CHỐT: {best_profile} | Score: {best_score:.4f}")
            
            final_df = apply_triple_barrier(coin_data.copy(), STRATEGY_PROFILES[best_profile])
            final_model = xgb.XGBClassifier(**best_params)
            final_model.fit(final_df[FEATURES].values, final_df['target'].values)
            
            # Export ONNX (float_input, 7 dims)
            initial_type = [('float_input', FloatTensorType([None, 7]))]
            onnx_model = onnxmltools.convert_xgboost(final_model, initial_types=initial_type)
            with open(f"model_{symbol}.onnx", "wb") as f:
                f.write(onnx_model.SerializeToString())
            
            white_list_metadata[symbol] = {
                **STRATEGY_PROFILES[best_profile],
                "strategy": best_profile,
                "aucpr": round(best_score, 4)
            }
        else:
            print(f"⚠️ {symbol} LOẠI BỎ (Không tìm thấy thế mạnh phù hợp).")

    with open("white_list.json", "w") as f:
        json.dump({"active_coins": white_list_metadata}, f, indent=4)

if __name__ == "__main__":
    if os.path.exists('dataset_multi_3months.csv'):
        df = pd.read_csv('dataset_multi_3months.csv')
        df_feat = calculate_quant_features(df)
        train_and_export(df_feat)