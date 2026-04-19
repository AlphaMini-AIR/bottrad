"""
train_master_v16.py - PRODUCTION READY (MICROSTRUCTURE ERA)
Đã vá lỗi Look-ahead Bias, tích hợp BTC Macro & Pessimistic Risk Engine.
"""

import pandas as pd
import numpy as np
import xgboost as xgb
import onnxmltools
from onnxmltools.convert.common.data_types import FloatTensorType
from pymongo import MongoClient
from sklearn.metrics import classification_report
import matplotlib.pyplot as plt
import warnings
import json  # <--- Thêm dòng này
import os

warnings.filterwarnings('ignore')

# Kết nối MongoDB
MONGO_URI = 'mongodb+srv://assistantsupdev_db_user:rCp0BrUushhwIKR8@binance.bjmukc0.mongodb.net/?appName=Binance'
DB_NAME = 'Binance'
COLLECTION_NAME = 'market_data_live'

# CHÚ Ý: Mảng này giờ có 13 Features (Thêm btc_relative_strength)
# Nhớ update mảng Features bên file Node.js (AiEngine.js) cho khớp!
FEATURES = [
    'ob_imb_top20', 'spread_close', 'bid_vol_1pct', 'ask_vol_1pct', 
    'max_buy_trade', 'max_sell_trade', 'liq_long_vol', 'liq_short_vol', 
    'funding_rate', 'taker_buy_ratio', 'body_size', 'wick_size', 'btc_relative_strength'
]

TIME_LIMIT = 60 # Scalping 60 phút
BTC_CACHE = None # Bộ nhớ đệm lưu data BTC để dùng chung cho mọi Altcoin

def fetch_btc_macro(client):
    """Kéo dữ liệu BTC 1 lần duy nhất để làm thước đo vĩ mô"""
    print("🌍 Đang tải dữ liệu Ông Trùm (BTCUSDT) để làm mốc tham chiếu...")
    db = client[DB_NAME]
    cursor = db[COLLECTION_NAME].find({"symbol": "BTCUSDT"}).sort("openTime", 1)
    
    btc_data = []
    for r in cursor:
        btc_data.append({
            'openTime': r['openTime'],
            'close': r['ohlcv']['close']
        })
    
    df_btc = pd.DataFrame(btc_data)
    if not df_btc.empty:
        # Tính % thay đổi giá của BTC từng phút
        df_btc['btc_pct_change'] = df_btc['close'].pct_change().fillna(0)
        # Chuyển thành Dictionary {openTime: pct_change} để tra cứu siêu tốc
        return dict(zip(df_btc['openTime'], df_btc['btc_pct_change']))
    return {}

def fetch_data_from_mongo(symbol, btc_dict):
    print(f"📡 Kéo dữ liệu Live của {symbol}...")
    client = MongoClient(MONGO_URI)
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
    
    # --- FEATURE ENGINEERING ---
    df['taker_buy_ratio'] = df['takerBuyQuote'] / (df['quoteVolume'] + 1e-8)
    df['body_size'] = abs(df['close'] - df['open'])
    df['wick_size'] = df['high'] - df['low'] - df['body_size']
    
    # Tính Sức mạnh tương đối so với BTC (Alpha Feature)
    df['coin_pct_change'] = df['close'].pct_change().fillna(0)
    df['btc_pct_change'] = df['openTime'].map(btc_dict).fillna(0)
    # coin tăng mạnh hơn BTC -> Số dương. Coin sập mạnh hơn BTC -> Số âm.
    df['btc_relative_strength'] = df['coin_pct_change'] - df['btc_pct_change']
    
    return df

def create_microstructure_labels(df):
    print(f"🎓 Người Thầy Ảo đang gán nhãn 3 rào cản (Triple-Barrier)...")
    
    targets = np.full(len(df), 2) # Mặc định là 2 (Rác/Nhiễu)
    closes = df['close'].values
    highs = df['high'].values
    lows = df['low'].values
    is_stale = df['isStaleData'].values
    
    # [VÁ LỖI BẢO MẬT]: Tính ATR và shift(1) để không nhìn thấy tương lai
    df['atr'] = df['high'] - df['low']
    atr_rolling = df['atr'].rolling(15).mean().shift(1).bfill().values 
    
    for i in range(15, len(df) - TIME_LIMIT):
        if is_stale[i]:
            continue
            
        current_atr = atr_rolling[i]
        
        # Risk/Reward 1:2
        sl_long = closes[i] - (current_atr * 1.5)
        tp_long = closes[i] + (current_atr * 3.0)
        
        sl_short = closes[i] + (current_atr * 1.5)
        tp_short = closes[i] - (current_atr * 3.0)
        
        window_h = highs[i+1 : i+TIME_LIMIT+1]
        window_l = lows[i+1 : i+TIME_LIMIT+1]
        
        # TÌM TÍN HIỆU LONG CÀNG SỚM CÀNG TỐT
        hit_tp_long = np.where(window_h >= tp_long)[0]
        hit_sl_long = np.where(window_l <= sl_long)[0]
        first_tp_l = hit_tp_long[0] if len(hit_tp_long) > 0 else TIME_LIMIT + 1
        first_sl_l = hit_sl_long[0] if len(hit_sl_long) > 0 else TIME_LIMIT + 1
        
        # Điều kiện khắt khe: Phải chạm TP trước, TRÁNH tuyệt đối cắn cả 2 trong 1 nến (first_tp_l == first_sl_l)
        if first_tp_l < first_sl_l:
            targets[i] = 1
            
        # TÌM TÍN HIỆU SHORT CÀNG SỚM CÀNG TỐT
        hit_tp_short = np.where(window_l <= tp_short)[0]
        hit_sl_short = np.where(window_h >= sl_short)[0]
        first_tp_s = hit_tp_short[0] if len(hit_tp_short) > 0 else TIME_LIMIT + 1
        first_sl_s = hit_sl_short[0] if len(hit_sl_short) > 0 else TIME_LIMIT + 1
        
        if first_tp_s < first_sl_s:
            targets[i] = 0

    df['expert_target'] = targets
    return df

def train_and_evaluate(symbol, btc_dict):
    df_raw = fetch_data_from_mongo(symbol, btc_dict)
    if df_raw is None or len(df_raw) < 50:
        print(f"⚠️ {symbol} quá ít nến (Dưới 1000 nến). Cần chạy live thêm! Bỏ qua train.")
        return

    df_labeled = create_microstructure_labels(df_raw)
    df_clean = df_labeled[df_labeled['isStaleData'] == False].dropna()
    
    X = df_clean[FEATURES].values
    y = df_clean['expert_target'].values
    
    print(f"📊 THỐNG KÊ {symbol} - Lệnh LONG: {sum(y==1)} | SHORT: {sum(y==0)} | NHIỄU: {sum(y==2)}")
    
    # Tách dữ liệu Chronological (80% Train, 20% Test)
    split_idx = int(len(X) * 0.8)
    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]
    
    # Cân bằng trọng số chống Thiên kiến cực đoan
    weights = np.ones(len(y_train))
    count_1, count_0, count_2 = sum(y_train == 1) + 1, sum(y_train == 0) + 1, sum(y_train == 2) + 1
    weights[y_train == 1] = min(count_2 / count_1, 15.0)
    weights[y_train == 0] = min(count_2 / count_0, 15.0)
    # Lệnh nhiễu (2) để trọng số thấp hơn 1 chút để AI tập trung tìm cơ hội
    weights[y_train == 2] = 0.5 

    model = xgb.XGBClassifier(
        objective='multi:softprob', num_class=3, n_estimators=250,
        max_depth=5, learning_rate=0.03, subsample=0.8,
        colsample_bytree=0.8, eval_metric='mlogloss', random_state=42
    )
    
    print("🧠 Đang luyện não XGBoost...")
    model.fit(X_train, y_train, sample_weight=weights)
    
    print("\n" + "="*45)
    print(f"🏆 KẾT QUẢ ĐÁNH GIÁ (TEST SET) - {symbol}")
    print("="*45)
    y_pred = model.predict(X_test)
    print(classification_report(y_test, y_pred, target_names=['Short(0)', 'Long(1)', 'Nhiễu(2)'], zero_division=0))
    
    # Xuất file ONNX cho Node.js
    initial_type = [('float_input', FloatTensorType([None, len(FEATURES)]))]
    onnx_model = onnxmltools.convert_xgboost(model, initial_types=initial_type)
    model_path = f"model_v16_{symbol}.onnx"
    with open(model_path, "wb") as f:
        f.write(onnx_model.SerializeToString())
    print(f"✅ Đã chốt file ONNX: {model_path}\n")

if __name__ == "__main__":
    print("🚀 KHỞI ĐỘNG HỆ THỐNG QUANT V16")
    
    # 1. Đọc danh sách coin từ white_list.json
    whitelist_path = os.path.join(os.path.dirname(__file__), 'white_list.json')
    try:
        with open(whitelist_path, 'r', encoding='utf-8') as f:
            whitelist_data = json.load(f)
            # Lấy danh sách coin, TỰ ĐỘNG LOẠI BỎ BTCUSDT khỏi danh sách train
            SYMBOLS = [coin for coin in whitelist_data['active_coins'].keys() if coin != 'BTCUSDT']
            print(f"📋 Đã tải {len(SYMBOLS)} Altcoin từ white_list.json: {SYMBOLS}")
    except FileNotFoundError:
        print(f"❌ Không tìm thấy file {whitelist_path}. Vui lòng kiểm tra lại!")
        exit()
    except Exception as e:
        print(f"❌ Lỗi đọc file JSON: {e}")
        exit()

    # 2. Tải mỏ neo vĩ mô BTC
    client = MongoClient(MONGO_URI)
    BTC_CACHE = fetch_btc_macro(client)
    client.close()
    
    if not BTC_CACHE:
        print("❌ Không lấy được dữ liệu BTCUSDT. Vui lòng kiểm tra lại MongoDB.")
        exit()

    # 3. Tiến hành Train tự động cho toàn bộ danh sách
    for sym in SYMBOLS:
        print(f"\n" + "*"*50)
        print(f"🔥 ĐANG XỬ LÝ: {sym}")
        print("*"*50)
        train_and_evaluate(sym, BTC_CACHE)