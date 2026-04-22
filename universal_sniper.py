"""
genesis_trainer.py - Lò luyện Não Khởi Nguyên
Nhiệm vụ: Train model đầu tiên từ 100k+ nến lịch sử, đồng bộ 100% với Node.js AI Engine.
"""
import pandas as pd
import numpy as np
import xgboost as xgb
import onnxmltools
from onnxmltools.convert.common.data_types import FloatTensorType
from pymongo import MongoClient
import os
import json
from dotenv import load_dotenv
import warnings

warnings.filterwarnings('ignore')
load_dotenv()

MONGO_URIS = {
    'tier1': os.getenv('MONGO_URI_TIER1'),
    'tier2': os.getenv('MONGO_URI_TIER2'),
    'scout': os.getenv('MONGO_URI_SCOUT')
}
DB_NAME = 'Binance'
COLLECTION_NAME = 'market_data_clean'

# 🚀 ĐỒNG BỘ TUYỆT ĐỐI VỚI NODE.JS (13 FEATURES)
FEATURES = [
    'ob_imb_top20', 'spread_close', 'bid_vol_1pct', 'ask_vol_1pct', 
    'max_buy_trade', 'max_sell_trade', 'liq_long_vol', 'liq_short_vol', 
    'funding_rate', 'taker_buy_ratio', 'body_size', 'wick_size', 'btc_relative_strength'
]

class UniversalFurnacePro:
    def __init__(self):
        self.model_path = "Universal_Scout.onnx" # Tên chuẩn để Node.js nhận diện Hot-Reload
        self.xgb_path = "model_universal_pro.xgb" # Lưu lại cho ban đêm học dồn
        self.btc_df = None

    def fetch_btc_benchmark(self):
        print("🌍 Đang nạp hệ quy chiếu BTCUSDT...")
        client = MongoClient(MONGO_URIS['tier1'])
        cursor = client[DB_NAME][COLLECTION_NAME].find({"symbol": "BTCUSDT"}).sort("openTime", 1)
        self.btc_df = pd.DataFrame([{ 'openTime': r['openTime'], 'btc_close': r['ohlcv']['close'] } for r in cursor])
        client.close()
        if not self.btc_df.empty:
            self.btc_df['btc_pct'] = self.btc_df['btc_close'].pct_change().fillna(0) * 100
            self.btc_df.set_index('openTime', inplace=True)

    def apply_triple_barrier_labeling(self, df):
        """Triple Barrier chuẩn xác (Không dùng bfill để tránh Lookahead Bias)"""
        targets = np.full(len(df), 2)
        
        # Bỏ bfill, dùng dropna ở cuối để dọn dẹp các nến đầu tiên thiếu ATR
        df['atr'] = (df['high'] - df['low']).rolling(15).mean() 
        
        closes = df['close'].values
        highs = df['high'].values
        lows = df['low'].values
        atr = df['atr'].values
        
        for i in range(15, len(df) - 30): # Bắt đầu từ 15 để tránh ATR bị NaN
            sl_l, tp_l = closes[i] - atr[i]*1.5, closes[i] + atr[i]*3.0
            sl_s, tp_s = closes[i] + atr[i]*1.5, closes[i] - atr[i]*3.0
            
            win_l = np.where(highs[i+1:i+31] >= tp_l)[0]
            lose_l = np.where(lows[i+1:i+31] <= sl_l)[0]
            if len(win_l) > 0 and (len(lose_l) == 0 or win_l[0] < lose_l[0]): targets[i] = 1
            
            win_s = np.where(lows[i+1:i+31] <= tp_s)[0]
            lose_s = np.where(highs[i+1:i+31] >= sl_s)[0]
            if len(win_s) > 0 and (len(lose_s) == 0 or win_s[0] < lose_s[0]): targets[i] = 0

        return targets

    def fetch_and_normalize(self, symbol, node):
        client = MongoClient(MONGO_URIS[node])
        cursor = client[DB_NAME][COLLECTION_NAME].find({"symbol": symbol}).sort("openTime", 1)
        records = list(cursor)
        client.close()
        
        if len(records) < 100: return None
        
        data = []
        for r in records:
            # Lấy các biến vi cấu trúc (nếu bị khuyết do Scout, Pandas sẽ tự ép thành NaN)
            micro = r.get('micro', {})
            macro = r.get('macro', {})
            ohlcv = r.get('ohlcv', {})
            
            data.append({
                'openTime': r['openTime'], 'open': ohlcv.get('open'), 'close': ohlcv.get('close'),
                'high': ohlcv.get('high'), 'low': ohlcv.get('low'),
                'quoteVolume': ohlcv.get('quoteVolume', 0), 
                'takerBuyQuote': ohlcv.get('takerBuyQuote', 0),
                
                # 13 TRƯỜNG CHUẨN KHỚP VỚI NODE.JS
                'ob_imb_top20': micro.get('ob_imb_top20', np.nan),
                'spread_close': micro.get('spread_close', np.nan),
                'bid_vol_1pct': micro.get('bid_vol_1pct', np.nan),
                'ask_vol_1pct': micro.get('ask_vol_1pct', np.nan),
                'max_buy_trade': micro.get('max_buy_trade', np.nan),
                'max_sell_trade': micro.get('max_sell_trade', np.nan),
                'liq_long_vol': micro.get('liq_long_vol', np.nan),
                'liq_short_vol': micro.get('liq_short_vol', np.nan),
                'funding_rate': macro.get('funding_rate', 0)
            })
        
        df = pd.DataFrame(data)
        
        # Tính toán các Feature phái sinh giống y hệt AiEngine.js
        df['coin_pct'] = df['close'].pct_change().fillna(0) * 100
        df['taker_buy_ratio'] = df['takerBuyQuote'] / (df['quoteVolume'] + 1e-8)
        df['body_size'] = np.where(df['open'] > 0, (abs(df['close'] - df['open']) / df['open']) * 100, 0)
        df['wick_size'] = np.where(df['open'] > 0, (((df['high'] - df['low']) - abs(df['close'] - df['open'])) / df['open']) * 100, 0)

        # Kết nối mỏ neo BTC
        df = df.join(self.btc_df[['btc_pct']], on='openTime', how='left').fillna(0)
        df['btc_relative_strength'] = df['coin_pct'] - df['btc_pct']

        # Gán nhãn
        df['target'] = self.apply_triple_barrier_labeling(df)
        
        return df.dropna() # Drop các dòng NaN do pct_change và rolling gây ra

    def ignite(self):
        self.fetch_btc_benchmark()
        
        # Đọc cấu hình
        if not os.path.exists('tier_list.json'):
            print("❌ Lỗi: Không tìm thấy tier_list.json")
            return
            
        with open('tier_list.json', 'r') as f:
            tier_data = json.load(f)
        
        all_symbols = tier_data['ranks'].get('tier_1', []) + tier_data['ranks'].get('tier_2', [])
        storage_map = tier_data.get('storage_map', {})
        
        big_data = []
        for sym in all_symbols:
            if sym == 'BTCUSDT': continue
            print(f"🔥 Đang chuẩn hóa và nạp {sym}...")
            df_sym = self.fetch_and_normalize(sym, storage_map.get(sym, 'tier1'))
            if df_sym is not None: big_data.append(df_sym)
            
        full_df = pd.concat(big_data)
        
        X = full_df[FEATURES].values
        y = full_df['target'].values
        
        # Trọng số cân bằng (Custom Weights)
        weights = np.ones(len(y))
        weights[y == 1] = 5.0
        weights[y == 0] = 5.0
        weights[y == 2] = 0.5
        # Nhấn mạnh các nến biến động mạnh bằng Wick Size (Tương đương Z-score Volume)
        weights[full_df['wick_size'] > full_df['wick_size'].quantile(0.9)] *= 2.0 

        print(f"🚀 Huấn luyện Bộ não Đồng nhất. Kích thước dữ liệu: {X.shape}")
        
        # Cấu hình XGBoost cho Genesis Model
        model = xgb.XGBClassifier(
            n_estimators=150, # 150 cây là đủ cho móng vững, để room cho học tăng dồn
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            objective='multi:softprob',
            tree_method='hist',
            missing=np.nan # 🚀 BẬT KHIÊN NAN Ở XGBOOST
        )
        
        model.fit(X, y, sample_weight=weights)
        
        # LƯU FILE CHO INCREMENTAL TRAINER (.xgb)
        model.save_model(self.xgb_path)
        
        # LƯU FILE CHO NODE.JS (.onnx)
        initial_type = [('float_input', FloatTensorType([None, len(FEATURES)]))]
        # 🚀 ÉP KÍCH HOẠT OPSET 13 CHO KHIÊN NAN
        onnx_model = onnxmltools.convert_xgboost(model, initial_types=initial_type, target_opset=13)
        with open(self.model_path, "wb") as f: 
            f.write(onnx_model.SerializeToString())
            
        print(f"✅ HOÀN TẤT! ")
        print(f"💾 File cho Node.js: {self.model_path}")
        print(f"💾 File cho Python: {self.xgb_path}")

if __name__ == "__main__":
    furnace = UniversalFurnacePro()
    furnace.ignite()