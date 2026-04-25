import asyncio
import websockets
import json
import msgpack
import os
import time
import aiohttp
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

class FeatureBuffer:
    """Bộ đệm tính toán Features - Tích hợp ATR, VPIN, OFI, MFA"""
    def __init__(self, symbol):
        self.symbol = symbol.upper()
        self.is_warm = False
        self.data_count = 0
        
        # Giá cơ bản
        self.best_bid = 0.0
        self.best_ask = 0.0
        self.last_price = 0.0
        self.open_price = 0.0 
        self.high_price = 0.0 
        self.low_price = 0.0  
        self.quote_volume = 0.0
        
        # Chỉ số sổ lệnh
        self.ob_imb_top20 = 0.0
        self.spread_close = 0.0
        self.bid_vol_1pct = 0.0
        self.ask_vol_1pct = 0.0
        
        # Trade & Thanh lý
        self.max_buy_trade = 0.0
        self.max_sell_trade = 0.0
        self.liq_long_vol = 0.0   
        self.liq_short_vol = 0.0
        self.taker_buy_quote = 0.0
        
        # Các chỉ số bổ sung cho OrderManager
        self.atr14 = 0.002       # ATR 14 nến (tỷ lệ %), mặc định 0.2%
        self.vpin = 0.5          # Volume imbalance proxy
        self.ofi = 0.0           # Order Flow Imbalance (từ depth)
        self.mfa = 0.0           # Money Flow Acceleration
        self._last_ofi = 0.0     # Giá trị OFI kỳ trước để tính MFA

    def update_ticker(self, data):
        self.last_price = float(data.get('c', self.last_price))
        self.open_price = float(data.get('o', self.open_price))
        self.high_price = float(data.get('h', self.high_price))
        self.low_price = float(data.get('l', self.low_price))
        self.quote_volume = float(data.get('q', self.quote_volume))
        self.taker_buy_quote = float(data.get('Q', self.taker_buy_quote))
        
        # Tính ATR14 (dùng range 24h làm proxy, đơn vị %)
        if self.last_price > 0:
            self.atr14 = (self.high_price - self.low_price) / self.last_price
        
        # Tính VPIN (dùng imbalance từ taker buy quote)
        if self.quote_volume > 0:
            taker_sell_quote = self.quote_volume - self.taker_buy_quote
            self.vpin = abs(self.taker_buy_quote - taker_sell_quote) / self.quote_volume
        
        self.data_count += 1
        if self.data_count > 10 and self.open_price > 0:
            self.is_warm = True

    def update_depth(self, data):
        bids = data.get('bids', [])
        asks = data.get('asks', [])
        if not bids or not asks: return

        self.best_bid = float(bids[0][0])
        self.best_ask = float(asks[0][0])
        
        bid_vol = sum(float(v) for p, v in bids)
        ask_vol = sum(float(v) for p, v in asks)
        total_vol = bid_vol + ask_vol
        self.ob_imb_top20 = (bid_vol - ask_vol) / total_vol if total_vol > 0 else 0

        if self.last_price > 0:
            self.spread_close = (self.best_ask - self.best_bid) / self.last_price

        bid_1pct_price = self.best_bid * 0.99
        ask_1pct_price = self.best_ask * 1.01
        self.bid_vol_1pct = sum(float(v) for p, v in bids if float(p) >= bid_1pct_price)
        self.ask_vol_1pct = sum(float(v) for p, v in asks if float(p) <= ask_1pct_price)

        # Tính OFI (dùng ob_imb_top20 làm proxy, scale để có giá trị >15 khi mạnh)
        self.ofi = self.ob_imb_top20 * 100  # -100 đến 100

    def update_trade(self, data):
        qty = float(data.get('q', 0))
        is_buyer_maker = data.get('m', True) 
        if is_buyer_maker: # Lệnh Sell
            if qty > self.max_sell_trade: self.max_sell_trade = qty
        else: # Lệnh Buy
            if qty > self.max_buy_trade: self.max_buy_trade = qty

    def update_force_order(self, data):
        order_info = data.get('o', {})
        side = order_info.get('S', '')
        qty = float(order_info.get('q', 0))
        if side == 'SELL': 
            self.liq_long_vol += qty
        elif side == 'BUY':
            self.liq_short_vol += qty

    def extract_features(self, btc_pct, current_funding_rate):
        coin_pct = ((self.last_price - self.open_price) / self.open_price * 100) if self.open_price > 0 else 0
        taker_buy_ratio = self.taker_buy_quote / self.quote_volume if self.quote_volume > 0 else 0.5
        body_size = (abs(self.last_price - self.open_price) / self.open_price) * 100 if self.open_price > 0 else 0
        wick_size = (((self.high_price - self.low_price) - abs(self.last_price - self.open_price)) / self.open_price) * 100 if self.open_price > 0 else 0
        btc_relative_strength = coin_pct - btc_pct

        # Tính MFA (đạo hàm của OFI)
        self.mfa = self.ofi - self._last_ofi
        self._last_ofi = self.ofi

        features = {
            "symbol": self.symbol,
            "is_warm": self.is_warm,
            "best_ask": self.best_ask,
            "best_bid": self.best_bid,
            "last_price": self.last_price,
            
            # 13 features gốc cho ONNX
            "ob_imb_top20": float(self.ob_imb_top20),
            "spread_close": float(self.spread_close),
            "bid_vol_1pct": float(self.bid_vol_1pct),
            "ask_vol_1pct": float(self.ask_vol_1pct),
            "max_buy_trade": float(self.max_buy_trade),
            "max_sell_trade": float(self.max_sell_trade),
            "liq_long_vol": float(self.liq_long_vol), 
            "liq_short_vol": float(self.liq_short_vol),
            "funding_rate": float(current_funding_rate),
            "taker_buy_ratio": float(taker_buy_ratio),
            "body_size": float(body_size),
            "wick_size": float(wick_size),
            "btc_relative_strength": float(btc_relative_strength),
            
            # Các chỉ số bổ sung cho OrderManager (không cần đưa vào ONNX)
            "ATR14": float(self.atr14),        # Tỷ lệ %, ví dụ 0.003 = 0.3%
            "VPIN": float(self.vpin),          # 0..1
            "OFI": float(self.ofi),            # -100..100
            "MFA": float(self.mfa)             # thay đổi của OFI
        }

        # Reset spikes sau mỗi lần lấy mẫu
        self.max_buy_trade = 0.0
        self.max_sell_trade = 0.0
        self.liq_long_vol = 0.0
        self.liq_short_vol = 0.0

        return features


class FeedHandler:
    def __init__(self):
        self.redis = aioredis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379'))
        self.active_tasks = {}
        self.buffers = {}
        self.stream_refs = {} 
        self.btc_pct = 0.0 
        self.global_funding_rates = {}

    def _should_keep_stream(self, symbol):
        if symbol == 'btcusdt': return True
        refs = self.stream_refs.get(symbol, {})
        return refs.get('radar', False) or refs.get('trade', False)

    async def fetch_funding_rates(self):
        print("🌍 [REST] Bắt đầu luồng đồng bộ Funding Rate...")
        while True:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get("https://fapi.binance.com/fapi/v1/premiumIndex") as resp:
                        data = await resp.json()
                        for item in data:
                            self.global_funding_rates[item['symbol'].lower()] = float(item['lastFundingRate'])
            except Exception as e:
                print(f"⚠️ [REST] Lỗi cập nhật Funding: {e}")
            await asyncio.sleep(3600)

    async def connect_binance_ws(self, symbol):
        streams = f"{symbol}@ticker/{symbol}@depth20@100ms/{symbol}@aggTrade/{symbol}@forceOrder"
        url = f"wss://fstream.binance.com/stream?streams={streams}"
        print(f"🔗 [WS] Mở kết nối đa luồng cho {symbol.upper()}...")
        
        while self._should_keep_stream(symbol):
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    async for msg in ws:
                        if not self._should_keep_stream(symbol):
                            print(f"🛑 [WS] Ref Count = 0. Hủy diệt luồng {symbol.upper()} an toàn.")
                            break
                            
                        packet = json.loads(msg)
                        stream_name = packet.get('stream', '')
                        data = packet.get('data', {})
                        
                        buffer = self.buffers.get(symbol)
                        if not buffer: continue

                        if '@ticker' in stream_name:
                            buffer.update_ticker(data)
                            if symbol == 'btcusdt' and buffer.open_price > 0:
                                self.btc_pct = ((buffer.last_price - buffer.open_price) / buffer.open_price) * 100
                        elif '@depth20' in stream_name:
                            buffer.update_depth(data)
                        elif '@aggTrade' in stream_name:
                            buffer.update_trade(data)
                        elif '@forceOrder' in stream_name:
                            buffer.update_force_order(data)
                            
            except Exception as e:
                if self._should_keep_stream(symbol):
                    print(f"⚠️ [WS] Đứt cáp {symbol.upper()}: {e}. Đang Reconnect...")
                    await asyncio.sleep(2)

    async def listen_to_control_channels(self):
        pubsub = self.redis.pubsub()
     await pubsub.subscribe("radar:candidates", "system:keep_alive", "system:subscriptions")
        print("📡 [PYTHON] Đã mở siêu tai nghe Async Redis...")

        async for message in pubsub.listen():
            if message['type'] == 'message':
                try:
                    channel = message['channel'].decode('utf-8')
                    data = json.loads(message['data'].decode('utf-8'))
                    if not isinstance(data, dict) or 'symbol' not in data:
                        continue
                    symbol = data['symbol'].lower()
                    action = data.get('action', '')

                    if symbol not in self.stream_refs:
                        self.stream_refs[symbol] = {'radar': False, 'trade': False}

                    if channel == "radar:candidates":
                        if action == 'ADD':
                            self.stream_refs[symbol]['radar'] = True
                        elif action == 'REMOVE':
                            self.stream_refs[symbol]['radar'] = False

                    elif channel == "system:keep_alive":
                        if action == 'ENTER_TRADE':
                            self.stream_refs[symbol]['trade'] = True
                        elif action == 'EXIT_TRADE':
                            self.stream_refs[symbol]['trade'] = False
                    elif channel == "system:subscriptions":
                        if action == 'SUBSCRIBE':
                            self.stream_refs[symbol]['radar'] = True
                        elif action == 'UNSUBSCRIBE':
                            self.stream_refs[symbol]['radar'] = False
                    should_run = self._should_keep_stream(symbol)
                    is_running = symbol in self.active_tasks

                    if should_run and not is_running:
                        print(f"🎯 [BÁM ĐUÔI] Radar/Trade kích hoạt: {symbol.upper()}")
                        self.buffers[symbol] = FeatureBuffer(symbol)
                        task = asyncio.create_task(self.connect_binance_ws(symbol))
                        self.active_tasks[symbol] = task

                    elif not should_run and is_running:
                        print(f"🗑️ [DỌN DẸP] Ref Count = 0. Hủy kết nối {symbol.upper()}")
                        del self.active_tasks[symbol]
                        if symbol in self.buffers: del self.buffers[symbol]
                        del self.stream_refs[symbol]

                except Exception as e:
                    print(f"❌ [LỖI ĐIỀU PHỐI REDIS]: {e}")

    async def publish_features(self):
        print("🚀 [PYTHON] Khởi chạy Bơm máu thời gian thực (100ms)...")
        while True:
            for symbol, buffer in list(self.buffers.items()):
                if symbol == 'btcusdt' and not self._should_keep_stream('btcusdt'):
                    continue

                if buffer.is_warm:
                    current_funding = self.global_funding_rates.get(symbol, 0.0001)
                    features = buffer.extract_features(self.btc_pct, current_funding)
                    packed_data = msgpack.packb(features)
                    await self.redis.publish(f"market:features:{symbol.upper()}", packed_data)
            await asyncio.sleep(0.1) 

    async def run(self):
        print("🧠 [PYTHON ENGINE] V17 Khởi động chuẩn HFT...")
        self.stream_refs['btcusdt'] = {'radar': True, 'trade': True}
        self.buffers['btcusdt'] = FeatureBuffer('btcusdt')
        self.active_tasks['btcusdt'] = asyncio.create_task(self.connect_binance_ws('btcusdt'))
        
        await asyncio.gather(
            self.fetch_funding_rates(),
            self.publish_features(),
            self.listen_to_control_channels() 
        )

if __name__ == "__main__":
    handler = FeedHandler()
    asyncio.run(handler.run())