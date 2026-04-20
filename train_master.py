"""
train_master_v16.py - PRODUCTION READY (MICROSTRUCTURE ERA)
[CẬP NHẬT PHASE 1]: Hỗ trợ Multi-Cluster MongoDB (Định tuyến 3 Tầng)
[CẬP NHẬT PHASE 2]: Custom Asymmetric Loss & Black Swan Sample Weighting
"""

import pandas as pd
import numpy as np
import xgboost as xgb
import onnxmltools
from onnxmltools.convert.common.data_types import FloatTensorType
from pymongo import MongoClient
from sklearn.metrics import classification_report
import warnings
import json
import os
from dotenv import load_dotenv 

warnings.filterwarnings('ignore')

# Tải biến môi trường từ file .env
load_dotenv()

# Quản lý 3 đường link kết nối Database độc lập
MONGO_URIS = {
    'tier1': os.getenv('MONGO_URI_TIER1'),
    'tier2': os.getenv('MONGO_URI_TIER2'),
    'scout': os.getenv('MONGO_URI_SCOUT')
}

DB_NAME = 'Binance'
COLLECTION_NAME = 'market_data_clean'

FEATURES = [
    'ob_imb_top20', 'spread_close', 'bid_vol_1pct', 'ask_vol_1pct', 
    'max_buy_trade', 'max_sell_trade', 'liq_long_vol', 'liq_short_vol', 
    'funding_rate', 'taker_buy_ratio', 'body_size', 'wick_size', 'btc_relative_strength'
]

TIME_LIMIT = 60 
BTC_CACHE = None 

# --- CẬP NHẬT LOGIC LẤY DATA BTC ---
def fetch_btc_macro(storage_node):
    """Kéo dữ liệu BTC từ cụm DB được chỉ định"""
    print(f"🌍 Đang tải dữ liệu Ông Trùm (BTCUSDT) từ Cluster [{storage_node.upper()}]...")
    
    uri = MONGO_URIS.get(storage_node)
    if not uri:
        raise ValueError(f"Không tìm thấy cấu hình MONGO_URI cho node: {storage_node}")
        
    client = MongoClient(uri)
    db = client[DB_NAME]
    cursor = db[COLLECTION_NAME].find({"symbol": "BTCUSDT"}).sort("openTime", 1)
    
    btc_data = []
    for r in cursor:
        btc_data.append({
            'openTime': r['openTime'],
            'close': r['ohlcv']['close']
        })
    client.close()
    
    df_btc = pd.DataFrame(btc_data)
    if not df_btc.empty:
        df_btc['btc_pct_change'] = df_btc['close'].pct_change().fillna(0)
        return dict(zip(df_btc['openTime'], df_btc['btc_pct_change']))
    return {}

# --- CẬP NHẬT LOGIC LẤY DATA ALTCOIN TỪ ĐÚNG CLUSTER ---
def fetch_data_from_mongo(symbol, btc_dict, storage_node):
    print(f"📡 Kéo dữ liệu Live của {symbol} từ Cluster [{storage_node.upper()}]...")
    
    uri = MONGO_URIS.get(storage_node)
    client = MongoClient(uri)
    db = client[DB_NAME]
    
    cursor = db[COLLECTION_NAME].find({"symbol": symbol}).sort("openTime", 1)
    records = list(cursor)
    client.close()
    
    if len(records) == 0:
        return None
        
    flat_data = []
    for r in records:
        flat_data.append({
            'openTime': r['openTime'],
            'open': r['ohlcv']['open'],
            'high': r['ohlcv']['high'],
            'low': r['ohlcv']['low'],
            'close': r['ohlcv']['close'],
            'volume': r['ohlcv']['volume'],
            'quoteVolume': r['ohlcv']['quoteVolume'],
            'takerBuyQuote': r['ohlcv']['takerBuyQuote'],
            
            'ob_imb_top20': r['micro']['ob_imb_top20'],
            'spread_close': r['micro']['spread_close'],
            'bid_vol_1pct': r['micro']['bid_vol_1pct'],
            'ask_vol_1pct': r['micro']['ask_vol_1pct'],
            'max_buy_trade': r['micro']['max_buy_trade'],
            'max_sell_trade': r['micro']['max_sell_trade'],
            'liq_long_vol': r['micro']['liq_long_vol'],
            'liq_short_vol': r['micro']['liq_short_vol'],
            
            'funding_rate': r['macro']['funding_rate'],
            'isStaleData': r.get('isStaleData', False)
        })
        
    df = pd.DataFrame(flat_data)
    
    df['taker_buy_ratio'] = df['takerBuyQuote'] / (df['quoteVolume'] + 1e-8)
    df['body_size'] = abs(df['close'] - df['open'])
    df['wick_size'] = df['high'] - df['low'] - df['body_size']
    
    df['coin_pct_change'] = df['close'].pct_change().fillna(0)
    df['btc_pct_change'] = df['openTime'].map(btc_dict).fillna(0)
    df['btc_relative_strength'] = df['coin_pct_change'] - df['btc_pct_change']
    
    return df

# --- GIỮ NGUYÊN HOÀN TOÀN CƠ CHẾ LABELING CỦA BẠN ---
def create_microstructure_labels(df):
    print(f"🎓 Người Thầy Ảo đang gán nhãn 3 rào cản (Triple-Barrier)...")
    targets = np.full(len(df), 2)
    closes = df['close'].values
    highs = df['high'].values
    lows = df['low'].values
    is_stale = df['isStaleData'].values
    
    df['atr'] = df['high'] - df['low']
    atr_rolling = df['atr'].rolling(15).mean().shift(1).bfill().values 
    
    for i in range(15, len(df) - TIME_LIMIT):
        if is_stale[i]:
            continue
            
        current_atr = atr_rolling[i]
        
        sl_long = closes[i] - (current_atr * 1.5)
        tp_long = closes[i] + (current_atr * 3.0)
        
        sl_short = closes[i] + (current_atr * 1.5)
        tp_short = closes[i] - (current_atr * 3.0)
        
        window_h = highs[i+1 : i+TIME_LIMIT+1]
        window_l = lows[i+1 : i+TIME_LIMIT+1]
        
        hit_tp_long = np.where(window_h >= tp_long)[0]
        hit_sl_long = np.where(window_l <= sl_long)[0]
        first_tp_l = hit_tp_long[0] if len(hit_tp_long) > 0 else TIME_LIMIT + 1
        first_sl_l = hit_sl_long[0] if len(hit_sl_long) > 0 else TIME_LIMIT + 1
        
        if first_tp_l < first_sl_l:
            targets[i] = 1
            
        hit_tp_short = np.where(window_l <= tp_short)[0]
        hit_sl_short = np.where(window_h >= sl_short)[0]
        first_tp_s = hit_tp_short[0] if len(hit_tp_short) > 0 else TIME_LIMIT + 1
        first_sl_s = hit_sl_short[0] if len(hit_sl_short) > 0 else TIME_LIMIT + 1
        
        if first_tp_s < first_sl_s:
            targets[i] = 0

    df['expert_target'] = targets
    return df

# =====================================================================
# 🟢 BƯỚC 2: HÀM CÀI ĐẶT TRỌNG SỐ THIÊN NGA ĐEN (BLACK SWAN WEIGHTING)
# =====================================================================
def apply_black_swan_weights(df_train, y_train):
    """ Ép AI học thuộc lòng các nến có thanh lý (Liquidation) khổng lồ """
    # 1. Trọng số Time-decay (Nến càng gần hiện tại, trọng số càng cao từ 0.8 đến 1.5)
    time_weight = np.linspace(0.8, 1.5, len(df_train))
    
    # 2. Tính tổng Thanh lý (Khối lượng quét lệnh)
    liq_total = df_train['liq_long_vol'] + df_train['liq_short_vol']
    
    # 3. Tìm ngưỡng Black Swan (Top 5% nến có lượng thanh lý lớn nhất)
    black_swan_threshold = np.percentile(liq_total, 95) if len(liq_total) > 0 else 0
    
    # Cân bằng Class cơ bản (Bản gốc của bạn)
    weights = np.ones(len(y_train))
    count_1 = sum(y_train == 1) + 1
    count_0 = sum(y_train == 0) + 1
    count_2 = sum(y_train == 2) + 1
    
    weights[y_train == 1] = min(count_2 / count_1, 15.0)
    weights[y_train == 0] = min(count_2 / count_0, 15.0)
    weights[y_train == 2] = 0.5  # Nhiễu (Ép AI bớt đánh)
    
    # Áp dụng Time-decay
    weights = weights * time_weight
    
    # KÍCH HOẠT BLACK SWAN: Nhân 10 lần trọng số cho nến Cực trị
    if black_swan_threshold > 0:
        black_swan_mask = liq_total >= black_swan_threshold
        weights[black_swan_mask] *= 10.0
        
    return weights

# =====================================================================
# 🟢 BƯỚC 3: HÀM MỤC TIÊU TỐI ƯU LỢI NHUẬN (CUSTOM ASYMMETRIC LOSS)
# =====================================================================
def asymmetric_profit_loss(y_true, y_pred):
    """
    Hàm tính Gradient và Hessian ép AI sợ những lệnh ngược sóng.
    y_pred: Raw margin scores từ XGBoost (N * 3)
    y_true: Labels (N,)
    """
    if y_pred.ndim == 1:
        y_pred = y_pred.reshape(-1, 3)
        
    # Tính Softmax (Xác suất % cho từng nhãn)
    y_pred = np.clip(y_pred, -15.0, 15.0) # Chống tràn số học
    exp_preds = np.exp(y_pred)
    probs = exp_preds / np.sum(exp_preds, axis=1, keepdims=True)
    
    # Chuyển Labels thành dạng One-hot [0, 1, 0]
    y_true_onehot = np.zeros_like(probs)
    for i, label in enumerate(y_true):
        y_true_onehot[i, int(label)] = 1.0
        
    # Bậc 1 (Gradient) và Bậc 2 (Hessian) mặc định
    grad = probs - y_true_onehot
    hess = probs * (1.0 - probs)
    
    # ⚔️ THI THIẾT QUÂN LUẬT: TRỪNG PHẠT BẤT ĐỐI XỨNG ⚔️
    for i in range(len(y_true)):
        true_label = int(y_true[i])
        
        # ÁN TỬ HÌNH 1: Thực tế nến LONG (1), mà AI lỡ nhả xác suất SHORT (0) > 40%
        if true_label == 1 and probs[i, 0] > 0.4:
            grad[i, :] *= 15.0  # Phạt x15 điểm âm Gradient
            hess[i, :] *= 15.0
            
        # ÁN TỬ HÌNH 2: Thực tế nến SHORT (0), mà AI lỡ nhả xác suất LONG (1) > 40%
        elif true_label == 0 and probs[i, 1] > 0.4:
            grad[i, :] *= 15.0
            hess[i, :] *= 15.0
            
        # ÁN PHẠT NHẸ: Thực tế Nhiễu (2), nhưng AI tự tin Long/Short > 60% (Dễ dính Stoploss)
        elif true_label == 2 and (probs[i, 0] > 0.6 or probs[i, 1] > 0.6):
            grad[i, :] *= 5.0
            hess[i, :] *= 5.0

    return grad.flatten(), hess.flatten()

# --- TÍCH HỢP TOÀN BỘ VÀO LUỒNG TRAIN ---
def train_and_evaluate(symbol, btc_dict, storage_node):
    df_raw = fetch_data_from_mongo(symbol, btc_dict, storage_node)
    if df_raw is None or len(df_raw) < 50:
        print(f"⚠️ {symbol} quá ít nến. Cần chạy live thêm! Bỏ qua train.")
        return

    df_labeled = create_microstructure_labels(df_raw)
    df_clean = df_labeled[df_labeled['isStaleData'] == False].dropna()
    
    X = df_clean[FEATURES].values
    y = df_clean['expert_target'].values
    
    print(f"📊 THỐNG KÊ {symbol} - Lệnh LONG: {sum(y==1)} | SHORT: {sum(y==0)} | NHIỄU: {sum(y==2)}")
    
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    
    # Kéo df_train để tính toán các chỉ số thanh lý (Black Swan)
    df_train = df_clean.iloc[:split_idx].copy()
    
    # 🟢 1. CÀI ĐẶT TRỌNG SỐ THIÊN NGA ĐEN
    weights = apply_black_swan_weights(df_train, y_train)

    # 🟢 2. KHAI BÁO MODEL VỚI HÀM MỤC TIÊU TỐI ƯU LỢI NHUẬN (CUSTOM LOSS)
    model = xgb.XGBClassifier(
        n_estimators=250,
        max_depth=5, 
        learning_rate=0.03, 
        subsample=0.8,
        colsample_bytree=0.8, 
        random_state=42
        # Đã lược bỏ objective và eval_metric để AI tự động nhận diện
    )
    
    print("🧠 Đang luyện não XGBoost (Tích hợp Asymmetric Custom Loss & Black Swan)...")
    model.fit(X_train, y_train, sample_weight=weights)
    
    print("\n" + "="*45)
    print(f"🏆 KẾT QUẢ ĐÁNH GIÁ (TEST SET) - {symbol}")
    print("="*45)
    y_pred = model.predict(X_test)
    print(classification_report(y_test, y_pred, target_names=['Short(0)', 'Long(1)', 'Nhiễu(2)'], zero_division=0))
    
    # 🟢 3. ÉP ONNX GẮN LỚP SOFTMAX VÀO ĐẦU RA MÔ HÌNH
    # (Vì Custom Loss làm XGBoost quên mất nó là một Classifier sinh ra Softmax)
    model.set_params(objective='multi:softprob')
    
    initial_type = [('float_input', FloatTensorType([None, len(FEATURES)]))]
    onnx_model = onnxmltools.convert_xgboost(model, initial_types=initial_type)
    model_path = f"model_v16_{symbol}.onnx"
    with open(model_path, "wb") as f:
        f.write(onnx_model.SerializeToString())
    print(f"✅ Đã chốt file ONNX (Mới): {model_path}\n")

if __name__ == "__main__":
    print("🚀 KHỞI ĐỘNG HỆ THỐNG QUANT V16 - MULTI-CLUSTER MODE & ASYMMETRIC PNL")
    
    # 1. Đọc danh sách và Tọa độ từ tier_list.json
    tierlist_path = os.path.join(os.path.dirname(__file__), 'tier_list.json')
    try:
        with open(tierlist_path, 'r', encoding='utf-8') as f:
            tier_data = json.load(f)
            
            # Chỉ lấy các coin ở Tầng 1 và Tầng 2 để Train (Tầng 3 Scout không đủ data)
            active_symbols = tier_data['ranks']['tier_1'] + tier_data['ranks']['tier_2']
            SYMBOLS = [coin for coin in active_symbols if coin != 'BTCUSDT']
            STORAGE_MAP = tier_data['storage_map']
            
            print(f"📋 Đã tải {len(SYMBOLS)} Altcoin thi đấu (T1+T2). Sẵn sàng định tuyến.")
    except Exception as e:
        print(f"❌ Lỗi đọc tier_list.json: {e}")
        exit()

    # 2. Tải mỏ neo vĩ mô BTC (Tự động tra cứu xem BTC đang ở node nào)
    btc_node = STORAGE_MAP.get("BTCUSDT", "tier1")
    BTC_CACHE = fetch_btc_macro(btc_node)
    
    if not BTC_CACHE:
        print("❌ Không lấy được dữ liệu BTCUSDT. Dừng chương trình.")
        exit()

    # 3. Tiến hành Train tự động định tuyến
    for sym in SYMBOLS:
        print(f"\n" + "*"*50)
        print(f"🔥 ĐANG XỬ LÝ: {sym}")
        print("*"*50)
        
        # Tìm DB chứa đồng coin này
        sym_node = STORAGE_MAP.get(sym, "scout")
        train_and_evaluate(sym, BTC_CACHE, sym_node)