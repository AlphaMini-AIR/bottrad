import asyncio
import websockets
import json
import time
import numpy as np
from collections import deque
import redis.asyncio as redis
import msgpack
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

class CoinBuffer:
    def __init__(self, symbol):
        self.symbol = symbol.lower()
        # Ring Buffer lưu trữ tick (Trade và Depth)
        self.trades = deque(maxlen=5000) # Lưu lệnh khớp để tính VPIN, WhaleNet
        self.depth_updates = deque(maxlen=1000) # Lưu sổ lệnh để tính OFI, Spread
        
        self.is_warm = False
        self.last_ofi = 0.0
        self.mfa_ewma = 0.0 # Exponential Weighted Moving Average cho MFA
        
    def process_trade(self, data):
        """Xử lý luồng @aggTrade"""
        price = float(data['p'])
        qty = float(data['q'])
        is_buyer_maker = data['m'] # True nếu lệnh bán chủ động (Taker Sell)
        
        self.trades.append({
            'price': price,
            'vol': price * qty,
            'is_buy': not is_buyer_maker,
            'ts': data['E']
        })
        
        if len(self.trades) > 500:
            self.is_warm = True

    def process_depth(self, data):
        """Xử lý luồng @depth20@100ms"""
        bids = data.get('b', [])
        asks = data.get('a', [])
        if not bids or not asks: return
        
        best_bid = float(bids[0][0])
        best_ask = float(asks[0][0])
        
        self.depth_updates.append({
            'best_bid': best_bid,
            'best_ask': best_ask,
            'ts': data['E']
        })

    def calculate_features(self):
        if not self.is_warm or len(self.depth_updates) < 2 or len(self.trades) < 10:
            return None

        # 1. Tính SPREAD
        latest_depth = self.depth_updates[-1]
        spread = (latest_depth['best_ask'] - latest_depth['best_bid']) / latest_depth['best_ask']

        # 2. Tính VPIN & Taker Imbalance (Dựa trên 500 trade gần nhất)
        recent_trades = list(self.trades)[-500:]
        buy_vol = sum(t['vol'] for t in recent_trades if t['is_buy'])
        sell_vol = sum(t['vol'] for t in recent_trades if not t['is_buy'])
        total_vol = buy_vol + sell_vol + 1e-8
        
        vpin = abs(buy_vol - sell_vol) / total_vol
        
        # 3. Tính Whale Netflow (Các lệnh > 3 lần trung bình)
        avg_trade_size = total_vol / len(recent_trades)
        whale_buy = sum(t['vol'] for t in recent_trades if t['is_buy'] and t['vol'] > avg_trade_size * 3)
        whale_sell = sum(t['vol'] for t in recent_trades if not t['is_buy'] and t['vol'] > avg_trade_size * 3)
        whale_net = whale_buy - whale_sell

        # 4. Tính OFI (CVD Proxy) và MFA (Gia tốc)
        current_ofi = buy_vol - sell_vol
        mfa_raw = current_ofi - self.last_ofi
        
        # Làm mịn MFA bằng EWMA (Alpha = 0.2)
        self.mfa_ewma = (0.2 * mfa_raw) + (0.8 * self.mfa_ewma)
        self.last_ofi = current_ofi

        # 5. Tính ATR Proxy (Độ dao động trên 500 tick gần nhất)
        prices = [t['price'] for t in recent_trades]
        atr_proxy = (max(prices) - min(prices)) / latest_depth['best_ask']

        return {
            "symbol": self.symbol.upper(),
            "ts": int(time.time() * 1000),
            "is_warm": self.is_warm,
            "OFI": current_ofi,
            "VPIN": vpin,
            "MFA": self.mfa_ewma,
            "Spread": spread,
            "WhaleNet": whale_net,
            "ATR_proxy": atr_proxy
        }

class FeedHandler:
    def __init__(self):
        self.redis = redis.from_url(REDIS_URL)
        self.active_coins = {}
        self.ws_url = "wss://fstream.binance.com/stream"

    async def _manage_subscriptions(self, ws):
        """Lắng nghe lệnh Sub/Unsub từ Node.js qua Redis (Sẽ dùng ở Giai đoạn 2)"""
        # Tạm thời hardcode 2 coin để test luồng
        symbols = ['btcusdt', 'solusdt']
        for sym in symbols:
            self.active_coins[sym] = CoinBuffer(sym)
            
        streams = []
        for sym in symbols:
            streams.extend([f"{sym}@aggTrade", f"{sym}@depth20@100ms"])
            
        payload = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        }
        await ws.send(json.dumps(payload))
        print(f"📡 Đã cắm ống nghe WebSocket cho: {symbols}")

    async def connect_binance(self):
        async with websockets.connect(self.ws_url) as ws:
            await self._manage_subscriptions(ws)

            async for message in ws:
                try:
                    raw_data = json.loads(message)
                    if 'stream' in raw_data and 'data' in raw_data:
                        stream_name = raw_data['stream']
                        data = raw_data['data']
                        
                        symbol = data['s'].lower()
                        if symbol not in self.active_coins:
                            continue
                            
                        if '@aggTrade' in stream_name:
                            self.active_coins[symbol].process_trade(data)
                        elif '@depth' in stream_name:
                            self.active_coins[symbol].process_depth(data)
                except Exception as e:
                    pass # Bỏ qua lỗi parse nhiễu

    async def publish_features(self):
        """Vòng lặp định tuyến 100ms: Nén và đẩy dữ liệu lên Redis"""
        print("🚀 Bắt đầu luồng Publish Feature Vectors (100ms)...")
        while True:
            for symbol, buffer in self.active_coins.items():
                features = buffer.calculate_features()
                if features:
                    packed_data = msgpack.packb(features)
                    await self.redis.publish(f"market:features:{symbol.upper()}", packed_data)
            
            await asyncio.sleep(0.1) # Chu kỳ 100ms

    async def run(self):
        await asyncio.gather(
            self.connect_binance(),
            self.publish_features()
        )

if __name__ == "__main__":
    handler = FeedHandler()
    asyncio.run(handler.run())