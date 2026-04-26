"""
universal_trainer.py - Bộ não thống nhất cho AI Quant Sniper V17
Chế độ hoạt động:
  1. Lò luyện khởi nguyên: python universal_trainer.py initial   -> Học từ nến lịch sử, gán nhãn Triple Barrier, chuẩn hóa tương đối.
  2. Học từ kinh nghiệm:   python universal_trainer.py incremental -> Đọc lịch sử Paper Trading, phạt cực nặng các lệnh lỗ, thưởng lệnh lãi.
"""

import sys
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
    'tier1': os.getenv('MONGO_URI_TIER1', 'mongodb://localhost:27017'),
    'tier2': os.getenv('MONGO_URI_TIER2', 'mongodb://localhost:27017'),
    'scout': os.getenv('MONGO_URI_SCOUT', 'mongodb://localhost:27017')
}
DB_NAME = 'Binance'
COLLECTION_CLEAN = 'market_data_clean'
COLLECTION_TRADES = 'scouttrades'

# 🚀 13 FEATURES GIỮ NGUYÊN TÊN ĐỂ THAM KHẢO (Không đưa vào DMatrix để tránh lỗi ONNX)
FEATURES = [
    'ob_imb_top20', 'spread_close', 'bid_vol_1pct', 'ask_vol_1pct',
    'max_buy_trade', 'max_sell_trade', 'liq_long_vol', 'liq_short_vol',
    'funding_rate', 'taker_buy_ratio', 'body_size', 'wick_size', 'btc_relative_strength'
]

class UniversalFurnace:
    def __init__(self):
        self.model = None
        self.btc_df = None
        self.model_path = "Universal_Scout.onnx"
        self.xgb_path = "model_universal_pro.xgb"
        
        self.params = {
            'objective': 'multi:softprob',
            'num_class': 3,  # 0: SHORT, 1: LONG, 2: HOLD
            'max_depth': 7,
            'learning_rate': 0.03,
            'subsample': 0.85,
            'colsample_bytree': 0.85,
            'tree_method': 'hist',
            'missing': np.nan 
        }

    def fetch_btc_benchmark(self):
        print("🌍 Đang nạp hệ quy chiếu BTCUSDT...")
        try:
            client = MongoClient(MONGO_URIS['tier1'])
            cursor = client[DB_NAME][COLLECTION_CLEAN].find({"symbol": "BTCUSDT"}).sort("openTime", 1)
            self.btc_df = pd.DataFrame([{ 'openTime': r['openTime'], 'btc_close': r['ohlcv']['close'] } for r in cursor])
            client.close()
            if not self.btc_df.empty:
                self.btc_df['btc_pct'] = self.btc_df['btc_close'].pct_change().fillna(0) * 100
                self.btc_df.set_index('openTime', inplace=True)
                print(f"✅ Đã nạp {len(self.btc_df)} nến BTC quy chiếu.")
        except Exception as e:
            print(f"⚠️ Lỗi nạp BTC Benchmark: {e}")

    def apply_triple_barrier_labeling(self, df):
        targets = np.full(len(df), 2) 
        
        df['atr_pct'] = ((df['high'] - df['low']) / df['close']).rolling(15).mean() 
        closes, highs, lows, atr_pct = df['close'].values, df['high'].values, df['low'].values, df['atr_pct'].values
        
        for i in range(15, len(df) - 30):
            sl_l, tp_l = closes[i] * (1 - atr_pct[i]*1.5), closes[i] * (1 + atr_pct[i]*3.0)
            sl_s, tp_s = closes[i] * (1 + atr_pct[i]*1.5), closes[i] * (1 - atr_pct[i]*3.0)
            
            win_l = np.where(highs[i+1:i+31] >= tp_l)[0]
            lose_l = np.where(lows[i+1:i+31] <= sl_l)[0]
            if len(win_l) > 0 and (len(lose_l) == 0 or win_l[0] < lose_l[0]): targets[i] = 1 
            
            win_s = np.where(lows[i+1:i+31] <= tp_s)[0]
            lose_s = np.where(highs[i+1:i+31] >= sl_s)[0]
            if len(win_s) > 0 and (len(lose_s) == 0 or win_s[0] < lose_s[0]): targets[i] = 0 

        return targets

    def fetch_and_normalize(self, symbol, node):
        client = MongoClient(MONGO_URIS[node])
        # 🛡️ BỘ LỌC DỮ LIỆU SẠCH: isStaleData = False
        cursor = client[DB_NAME][COLLECTION_CLEAN].find({
            "symbol": symbol, 
            "isStaleData": False
        }).sort("openTime", 1)
        
        records = list(cursor)
        client.close()
        
        if len(records) < 100: return None
        
        data = []
        for r in records:
            micro, macro, ohlcv = r.get('micro', {}), r.get('macro', {}), r.get('ohlcv', {})
            close_price = ohlcv.get('close', 1)
            quote_vol = ohlcv.get('quoteVolume', 1) + 1e-8
            
            data.append({
                'openTime': r['openTime'], 'open': ohlcv.get('open'), 'close': close_price,
                'high': ohlcv.get('high'), 'low': ohlcv.get('low'), 'quoteVolume': quote_vol, 
                'takerBuyQuote': ohlcv.get('takerBuyQuote', 0),
                'ob_imb_top20': micro.get('ob_imb_top20', 0),
                'spread_close': micro.get('spread_close', 0),
                'bid_vol_1pct': (micro.get('bid_vol_1pct', 0) * close_price) / quote_vol,
                'ask_vol_1pct': (micro.get('ask_vol_1pct', 0) * close_price) / quote_vol,
                'max_buy_trade': (micro.get('max_buy_trade', 0) * close_price) / quote_vol,
                'max_sell_trade': (micro.get('max_sell_trade', 0) * close_price) / quote_vol,
                'liq_long_vol': (micro.get('liq_long_vol', 0) * close_price) / quote_vol,
                'liq_short_vol': (micro.get('liq_short_vol', 0) * close_price) / quote_vol,
                'funding_rate': macro.get('funding_rate', 0)
            })
        
        df = pd.DataFrame(data)
        df = df[df['quoteVolume'] > 50000]
        if len(df) < 100: return None

        df['coin_pct'] = df['close'].pct_change().fillna(0) * 100
        df['taker_buy_ratio'] = df['takerBuyQuote'] / df['quoteVolume']
        df['body_size'] = np.where(df['open'] > 0, (abs(df['close'] - df['open']) / df['open']) * 100, 0)
        df['wick_size'] = np.where(df['open'] > 0, (((df['high'] - df['low']) - abs(df['close'] - df['open'])) / df['open']) * 100, 0)

        if self.btc_df is not None:
            df = df.join(self.btc_df[['btc_pct']], on='openTime', how='left').fillna(0)
            df['btc_relative_strength'] = df['coin_pct'] - df['btc_pct']
        else:
            df['btc_relative_strength'] = 0

        df['target'] = self.apply_triple_barrier_labeling(df)
        return df.dropna()

    def build_initial_dataset(self):
        if not os.path.exists('tier_list.json'):
            print("❌ Lỗi: Không tìm thấy tier_list.json")
            return None, None, None
            
        with open('tier_list.json', 'r') as f: tier_data = json.load(f)
        all_symbols = tier_data['ranks'].get('tier_1', []) + tier_data['ranks'].get('tier_2', []) + tier_data['ranks'].get('scout', [])
        storage_map = tier_data.get('storage_map', {})
        
        big_data = []
        for sym in all_symbols:
            if sym == 'BTCUSDT': continue
            node = storage_map.get(sym, 'scout')
            print(f"🔥 Đang trích xuất & chuẩn hóa {sym} từ {node}...")
            df_sym = self.fetch_and_normalize(sym, node)
            if df_sym is not None: big_data.append(df_sym)
            
        full_df = pd.concat(big_data)
        X = full_df[FEATURES].values
        y = full_df['target'].values
        
        weights = np.ones(len(y))
        weights[y == 1] = 6.0 
        weights[y == 0] = 6.0 
        weights[y == 2] = 0.5 
        weights[full_df['quoteVolume'] > full_df['quoteVolume'].quantile(0.85)] *= 1.5 
        
        return X, y, weights

    def build_experience_dataset(self):
        print("🧠 Đang thu thập trí khôn từ dữ liệu thực chiến...")
        client = MongoClient(MONGO_URIS['scout'])
        trades = list(client[DB_NAME][COLLECTION_TRADES].find({
            "status": "CLOSED",
            "features": {"$exists": True}
        }))
        client.close()

        if not trades: return None, None, None

        X_list, y_list, weights_list = [], [], []

        for t in trades:
            f = t['features']
            pnl = t.get('pnl', 0)
            trade_type = t.get('type', 'LONG')
            
            if not all(k in f for k in FEATURES): continue
            row = [f[k] for k in FEATURES]
            
            if pnl > 0:
                target = 1 if trade_type == 'LONG' else 0
                weight = 1.0 + min(pnl / 10.0, 5.0) 
            else:
                target = 2 
                weight = 5.0 + abs(pnl) 
                
            X_list.append(row)
            y_list.append(target)
            weights_list.append(weight)

        return np.array(X_list), np.array(y_list), np.array(weights_list)

    def export_onnx(self):
        print("⚙️ Đang biên dịch sang ONNX cho Node.js...")
        initial_type = [('float_input', FloatTensorType([None, len(FEATURES)]))]
        onnx_model = onnxmltools.convert_xgboost(self.model, initial_types=initial_type, target_opset=13)
        with open(self.model_path, "wb") as f:
            f.write(onnx_model.SerializeToString())
        print(f"✅ HOÀN TẤT: Đã xuất {self.model_path}")

    def run_initial(self):
        print("\n" + "="*50)
        print("🔥 BẮT ĐẦU CHẾ ĐỘ INITIAL (LÒ LUYỆN KHỞI NGUYÊN)")
        print("="*50)
        self.fetch_btc_benchmark()
        X, y, weights = self.build_initial_dataset()
        if X is None:
            print("❌ Không có dữ liệu để train.")
            return
            
        print(f"🚀 Huấn luyện Khởi nguyên. Kích thước: {X.shape}")
        # 🛠️ FIX LỖI ONNX: Bỏ tham số feature_names
        dtrain = xgb.DMatrix(X, label=y, weight=weights)
        self.model = xgb.train(self.params, dtrain, num_boost_round=200)
        self.model.save_model(self.xgb_path)
        self.export_onnx()

    def run_incremental(self):
        print("\n" + "="*50)
        print("🧠 BẮT ĐẦU CHẾ ĐỘ INCREMENTAL (HỌC TỪ KINH NGHIỆM)")
        print("="*50)
        if not os.path.exists(self.xgb_path):
            print("⚠️ Chưa có não gốc (.xgb). Hệ thống sẽ tự chạy chế độ Initial trước.")
            self.run_initial()
            return
            
        X, y, weights = self.build_experience_dataset()
        if X is None or len(X) < 100: 
            print("⏳ Chưa đủ dữ liệu kinh nghiệm thực chiến (Cần >100 lệnh).")
            return
            
        print(f"🚀 Tinh chỉnh trên {len(X)} lệnh thực tế...")
        # 🛠️ FIX LỖI ONNX: Bỏ tham số feature_names
        dtrain = xgb.DMatrix(X, label=y, weight=weights)
        self.model = xgb.train(self.params, dtrain, num_boost_round=50, xgb_model=self.xgb_path)
        
        self.model.save_model(self.xgb_path)
        self.export_onnx()
        print("📈 Bộ não đã trở nên thông minh hơn sau khi đúc kết kinh nghiệm!")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else 'initial'
    furnace = UniversalFurnace()
    
    if mode == 'initial':
        furnace.run_initial()
    elif mode == 'incremental':
        furnace.run_incremental()
    else:
        print("❌ Lệnh không hợp lệ. Dùng: python universal_trainer.py [initial|incremental]")