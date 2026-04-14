# train_master.py - Version 5.0 (Perfected Quant Model)
import pandas as pd
import numpy as np
import xgboost as xgb
import onnxmltools
import json
import os
import warnings
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import average_precision_score
from onnxmltools.convert.common.data_types import FloatTensorType

# Tắt cảnh báo pandas để log console sạch sẽ
warnings.filterwarnings('ignore')

print("🧠 [AI PIPELINE] Khởi tạo hệ thống Discovery V5.0 (Perfected)...")

# --- BỘ THAM SỐ CHIẾN THUẬT ĐỂ AI THỬ NGHIỆM ---
STRATEGY_PROFILES = {
    # Swing: SL = 2 lần độ giật trung bình, TP = 4 lần (Né nhiễu tối đa, ăn trend dài)
    "SWING": {"sl_mult": 2.0, "tp_mult": 4.0, "min_prob": 0.70},
    # Range: Đánh trong biên độ vừa phải
    "RANGE": {"sl_mult": 1.0, "tp_mult": 2.0, "min_prob": 0.60},
    # Scalp: SL = 0.5 lần độ giật, đánh cực nhanh, chạy ngay
    "SCALP": {"sl_mult": 0.5, "tp_mult": 1.0, "min_prob": 0.65}
}
# ---------------------------------------------------------
# 1. TÍNH TOÁN QUANT FEATURES (7 ĐẶC TRƯNG CHUYÊN SÂU)
# ---------------------------------------------------------
def calculate_quant_features(df):
    df = df.sort_values(by=['symbol', 'timestamp']).copy()

    def apply_features(group):
        # 1. Hurst Proxy (Variance Ratio 20m)
        ret_1m = group['close'].pct_change(1)
        ret_20m = group['close'].pct_change(20)
        group['hurst_proxy'] = ret_20m.rolling(20).std() / (np.sqrt(20) * ret_1m.rolling(20).std() + 1e-8)

        # 2. Distance to VWAP 4h (240m)
        rolling_pv = (group['close'] * group['volume']).rolling(window=240).sum()
        rolling_vol = group['volume'].rolling(window=240).sum() + 1e-8
        group['vwap_4h'] = rolling_pv / rolling_vol
        group['dist_vwap'] = (group['close'] - group['vwap_4h']) / group['vwap_4h']

        # 3. Wick to Body & Volume Acceleration
        body = abs(group['close'] - group['open']) + 1e-8
        wick = group['high'] - group['low']
        group['wick_to_body'] = wick / body
        group['vol_acceleration'] = group['volume'] / (group['volume'].rolling(window=15).mean() + 1e-8)

        # 4. Funding Delta 12h
        if 'funding_rate' in group.columns:
            group['funding_delta_12h'] = group['funding_rate'].diff(720).fillna(0)
        else:
            group['funding_delta_12h'] = 0.0
            
        # 5. ATR Normalized (Đo lường độ biến động)
        group['prev_close'] = group['close'].shift(1)
        group['tr'] = np.maximum(group['high'] - group['low'], 
                      np.maximum(abs(group['high'] - group['prev_close']), 
                                 abs(group['low'] - group['prev_close'])))
        group['atr'] = group['tr'].rolling(14).mean()
        group['atr_norm'] = group['atr'] / group['close']
        
        # 6. RSI 14 (Đo lường động lượng)
        delta = group['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / (loss + 1e-8)
        group['rsi'] = 100 - (100 / (1 + rs))

        return group

    result_dfs = []
    for sym, group in df.groupby('symbol'):
        result_dfs.append(apply_features(group.copy()))
    return pd.concat(result_dfs).dropna()

# ---------------------------------------------------------
# 2. GÁN NHÃN TRIPLE-BARRIER (VÁ LỖI VA CHẠM NẾN)
# ---------------------------------------------------------
# ---------------------------------------------------------
# 2. GÁN NHÃN TRIPLE-BARRIER (SỬ DỤNG ATR DYNAMIC)
# ---------------------------------------------------------
def apply_triple_barrier(df, sl_mult, tp_mult, time_limit=240):
    def process_labels(group):
        closes = group['close'].values
        highs = group['high'].values
        lows = group['low'].values
        atrs = group['atr'].values # Lấy mảng ATR đã tính sẵn
        labels = np.zeros(len(closes))

        for i in range(len(closes) - time_limit):
            # Lấy ATR của nến hiện tại (nếu bị NaN do chưa đủ 14 nến thì lấy tạm 0.1% giá)
            current_atr = atrs[i] if not np.isnan(atrs[i]) else (closes[i] * 0.001)

            # Tính giá TP/SL động
            tp_price = closes[i] + (current_atr * tp_mult)
            sl_price = closes[i] - (current_atr * sl_mult)
            
            f_highs = highs[i+1 : i+time_limit+1]
            f_lows = lows[i+1 : i+time_limit+1]

            hit_tp_mask = f_highs >= tp_price
            hit_sl_mask = f_lows <= sl_price

            hit_tp_idx = np.argmax(hit_tp_mask) if np.any(hit_tp_mask) else -1
            hit_sl_idx = np.argmax(hit_sl_mask) if np.any(hit_sl_mask) else -1

            if hit_tp_idx != -1 and hit_sl_idx != -1:
                if hit_tp_idx < hit_sl_idx:
                    labels[i] = 1
                else:
                    labels[i] = 0 
            elif hit_tp_idx != -1 and hit_sl_idx == -1:
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
# 3. HUẤN LUYỆN CHÉO (WALK-FORWARD) & XUẤT ONNX
# ---------------------------------------------------------
def train_and_export(df):
    features = ['hurst_proxy', 'dist_vwap', 'wick_to_body', 'vol_acceleration', 'funding_delta_12h', 'atr_norm', 'rsi']
    white_list_metadata = {}
    
    # Chia dữ liệu thành 3 phase nối tiếp nhau để kiểm tra chéo
    tscv = TimeSeriesSplit(n_splits=3) 

    for symbol in df['symbol'].unique():
        print(f"\n🚀 Đang tìm 'Sở trường' cho {symbol}...")
        coin_raw = df[df['symbol'] == symbol].copy()
        
        best_aucpr = 0
        best_meta = None

        for name, config in STRATEGY_PROFILES.items():
            # Truyền sl_mult và tp_mult thay vì risk và reward
            labeled_df = apply_triple_barrier(coin_raw.copy(), config['sl_mult'], config['tp_mult'])
            if len(labeled_df) < 500: continue
            
            X, y = labeled_df[features].values, labeled_df['target'].values
            
            # Huấn luyện và đánh giá trên 3 pha thị trường khác nhau
            fold_aucpr = []
            for train_idx, test_idx in tscv.split(X):
                X_train, X_test = X[train_idx], X[test_idx]
                y_train, y_test = y[train_idx], y[test_idx]
                
                # Bỏ qua fold nếu không đủ mẫu lệnh thắng để AI học
                if np.sum(y_train == 1) < 5 or np.sum(y_test == 1) < 1: continue

                model = xgb.XGBClassifier(
                    n_estimators=150, max_depth=5, learning_rate=0.03,
                    scale_pos_weight=(len(y_train) - np.sum(y_train)) / (np.sum(y_train) + 1e-8),
                    eval_metric='aucpr'
                )
                model.fit(X_train, y_train)
                preds = model.predict_proba(X_test)[:, 1]
                fold_aucpr.append(average_precision_score(y_test, preds))

            avg_aucpr = np.mean(fold_aucpr) if fold_aucpr else 0
            print(f"   - Thử nghiệm {name}: AUCPR (Trung bình 3 pha) = {avg_aucpr:.4f}")

            if avg_aucpr > best_aucpr:
                best_aucpr = avg_aucpr
                best_meta = {**config, "strategy": name, "aucpr": round(avg_aucpr, 4)}

        # KCS: Vượt qua ngưỡng AUCPR 0.15 trong cả 3 pha mới được phép trade
        if best_aucpr > 0.15:
            print(f"✅ CHỐT: {symbol} áp dụng chiến thuật {best_meta['strategy']} (AUCPR: {best_aucpr:.4f})")
            white_list_metadata[symbol] = best_meta
            
            # BƯỚC QUYẾT ĐỊNH: Train lại não trên 100% dữ liệu lịch sử để lấy tri thức mới nhất
            llabeled_df = apply_triple_barrier(coin_raw.copy(), best_meta['sl_mult'], best_meta['tp_mult'])
            X_final, y_final = labeled_df[features].values, labeled_df['target'].values
            
            final_model = xgb.XGBClassifier(
                n_estimators=150, max_depth=5, learning_rate=0.03,
                scale_pos_weight=(len(y_final) - np.sum(y_final)) / (np.sum(y_final) + 1e-8)
            )
            final_model.fit(X_final, y_final)

            # Xuất model ONNX
            initial_type = [('float_input', FloatTensorType([None, len(features)]))]
            onnx_model = onnxmltools.convert_xgboost(final_model, initial_types=initial_type)
            with open(f"model_{symbol}.onnx", "wb") as f:
                f.write(onnx_model.SerializeToString())
        else:
            print(f"❌ {symbol}: Không vượt qua bài Test thị trường. Tạm giam.")

    # Xuất định dạng Object Dictionary chuẩn cho NodeJS
    with open("white_list.json", "w") as f:
        json.dump({"active_coins": white_list_metadata}, f, indent=4)
    print("\n📜 Đã cập nhật white_list.json với các bộ cấu hình động.")

if __name__ == "__main__":
    test_csv = 'dataset_multi_3months.csv'
    if os.path.exists(test_csv):
        df = pd.read_csv(test_csv)
        df = calculate_quant_features(df)
        train_and_export(df)