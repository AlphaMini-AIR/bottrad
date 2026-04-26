import asyncio
import websockets
import json
import msgpack
import os
import aiohttp
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()

class FeatureBuffer:
    """Bộ đệm tính toán Features - Tích hợp Smoothing OFI và Dòng tiền Cá Voi"""
    def __init__(self, symbol):
        self.symbol = symbol.lower()
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
        self.whale_netflow = 0.0  # Dòng tiền cá voi (>5000$)
        self.liq_long_vol = 0.0   
        self.liq_short_vol = 0.0
        self.taker_buy_quote = 0.0
        
        # Chỉ số AI nội bộ
        self.atr14 = 0.002
        self.vpin = 0.5
        self.ofi = 0.0
        self.ofi_smoothed = 0.0   # OFI đã lọc nhiễu
        self._last_ofi_smoothed = 0.0
        self.mfa = 0.0

    def update_ticker(self, data):
        self.last_price = float(data.get('c', self.last_price))
        self.open_price = float(data.get('o', self.open_price))
        self.high_price = float(data.get('h', self.high_price))
        self.low_price = float(data.get('l', self.low_price))
        self.quote_volume = float(data.get('q', self.quote_volume))
        self.taker_buy_quote = float(data.get('Q', self.taker_buy_quote))
        
        if self.last_price > 0:
            self.atr14 = (self.high_price - self.low_price) / self.last_price
        
        if self.quote_volume > 0:
            taker_sell_quote = self.quote_volume - self.taker_buy_quote
            self.vpin = abs(self.taker_buy_quote - taker_sell_quote) / self.quote_volume
        
        self.data_count += 1
        if self.data_count > 10 and self.open_price > 0:
            self.is_warm = True

    def update_depth(self, data):
        bids = data.get('b', [])
        asks = data.get('a', [])
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

        # Tính và làm mịn OFI (Exponential Moving Average)
        self.ofi = self.ob_imb_top20 * 100
        # Làm mịn: Lấy 70% giá trị cũ + 30% giá trị mới để tránh giật cục
        self.ofi_smoothed = (self.ofi_smoothed * 0.7) + (self.ofi * 0.3) 

    def update_trade(self, data):
        qty = float(data.get('q', 0))
        price = float(data.get('p', self.last_price))
        is_buyer_maker = data.get('m', True) 
        
        trade_value = qty * price

        if is_buyer_maker: # Lệnh Sell
            if qty > self.max_sell_trade: self.max_sell_trade = qty
            if trade_value > 5000: self.whale_netflow -= trade_value # Cá xả
        else: # Lệnh Buy
            if qty > self.max_buy_trade: self.max_buy_trade = qty
            if trade_value > 5000: self.whale_netflow += trade_value # Cá gom

    def update_force_order(self, data):
        order_info = data.get('o', {})
        side = order_info.get('S', '')
        qty = float(order_info.get('q', 0))
        if side == 'SELL': self.liq_long_vol += qty
        elif side == 'BUY': self.liq_short_vol += qty

    def extract_features(self, btc_pct, current_funding_rate):
        coin_pct = ((self.last_price - self.open_price) / self.open_price * 100) if self.open_price > 0 else 0
        taker_buy_ratio = self.taker_buy_quote / self.quote_volume if self.quote_volume > 0 else 0.5
        body_size = (abs(self.last_price - self.open_price) / self.open_price) * 100 if self.open_price > 0 else 0
        wick_size = (((self.high_price - self.low_price) - abs(self.last_price - self.open_price)) / self.open_price) * 100 if self.open_price > 0 else 0
        btc_relative_strength = coin_pct - btc_pct

        # Tính MFA dựa trên OFI đã làm mịn (Đạo hàm cực chuẩn)
        self.mfa = self.ofi_smoothed - self._last_ofi_smoothed
        self._last_ofi_smoothed = self.ofi_smoothed

        quote_vol_safe = self.quote_volume if self.quote_volume > 0 else 1e-8
        close_p = self.last_price

        features = {
            "symbol": self.symbol.upper(),
            "is_warm": self.is_warm,
            "best_ask": self.best_ask,
            "best_bid": self.best_bid,
            "last_price": self.last_price,
            
            # Chuẩn hóa Base Asset -> Quote Ratio
            "ob_imb_top20": float(self.ob_imb_top20),
            "spread_close": float(self.spread_close),
            "bid_vol_1pct": float((self.bid_vol_1pct * close_p) / quote_vol_safe),
            "ask_vol_1pct": float((self.ask_vol_1pct * close_p) / quote_vol_safe),
            "max_buy_trade": float((self.max_buy_trade * close_p) / quote_vol_safe),
            "max_sell_trade": float((self.max_sell_trade * close_p) / quote_vol_safe),
            "liq_long_vol": float((self.liq_long_vol * close_p) / quote_vol_safe), 
            "liq_short_vol": float((self.liq_short_vol * close_p) / quote_vol_safe),
            "funding_rate": float(current_funding_rate),
            "taker_buy_ratio": float(taker_buy_ratio),
            "body_size": float(body_size),
            "wick_size": float(wick_size),
            "btc_relative_strength": float(btc_relative_strength),
            
            # Chỉ số nội bộ cho OrderManager
            "ATR14": float(self.atr14),
            "VPIN": float(self.vpin),
            "OFI": float(self.ofi_smoothed),
            "MFA": float(self.mfa),
            "WHALE_NET": float(self.whale_netflow / quote_vol_safe) # Chuẩn hóa cá voi
        }

        # Reset spikes sau mỗi 100ms
        self.max_buy_trade = 0.0
        self.max_sell_trade = 0.0
        self.liq_long_vol = 0.0
        self.liq_short_vol = 0.0
        # Làm nhạt dần netflow của cá voi thay vì reset về 0 (Trí nhớ ngắn hạn)
        self.whale_netflow *= 0.5 

        return features


class FeedHandler:
    def __init__(self):
        self.redis = aioredis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379'))
        self.buffers = {}
        self.stream_refs = {} 
        self.active_streams = set() # Quản lý các luồng đang chạy trên WS
        self.ws_connection = None
        self.btc_pct = 0.0 
        self.global_funding_rates = {}

    async def fetch_funding_rates(self):
        print("🌍 [REST] Khởi chạy luồng đồng bộ Funding Rate (1 giờ/lần)...")
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

    def _get_stream_names(self, symbol):
        sym = symbol.lower()
        return [f"{sym}@ticker", f"{sym}@depth20@100ms", f"{sym}@aggTrade", f"{sym}@forceOrder"]

    async def send_ws_command(self, method, streams):
        if not self.ws_connection or not streams: return
        payload = {
            "method": method,
            "params": list(streams),
            "id": 1
        }
        try:
            await self.ws_connection.send(json.dumps(payload))
            # print(f"📡 [WS API] {method} {len(streams)} luồng thành công.")
        except Exception as e:
            print(f"❌ [WS API] Lỗi gửi lệnh {method}: {e}")

    async def connect_binance_multiplex(self):
        url = "wss://fstream.binance.com/stream"
        print("🔗 [WS] Mở Đường Ống MULTIPLEX Tổng tới Binance...")
        
        while True:
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    self.ws_connection = ws
                    print("✅ [WS] Đường ống HFT đã thông suốt!")
                    
                    # Nếu có luồng cũ (do đứt cáp nối lại), khôi phục ngay
                    if self.active_streams:
                        await self.send_ws_command("SUBSCRIBE", self.active_streams)

                    async for msg in ws:
                        packet = json.loads(msg)
                        stream_name = packet.get('stream', '')
                        data = packet.get('data', {})
                        
                        if not stream_name: continue

                        symbol = stream_name.split('@')[0].lower()
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
                print(f"⚠️ [WS] Đứt cáp tổng: {e}. Đang tái thiết lập kết nối...")
                self.ws_connection = None
                await asyncio.sleep(2)

    async def listen_to_control_channels(self):
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("system:subscriptions", "system:keep_alive")
        print("📡 [PYTHON] Đã mở siêu tai nghe lắng nghe Lệnh từ Radar & OrderManager...")

        async for message in pubsub.listen():
            if message['type'] == 'message':
                try:
                    data = json.loads(message['data'].decode('utf-8'))
                    if 'symbol' not in data: continue
                    
                    symbol = data['symbol'].lower()
                    action = data.get('action', '')
                    client = data.get('client', 'unknown')

                    if symbol not in self.stream_refs:
                        self.stream_refs[symbol] = {'radar': False, 'trade': False}

                    # Cập nhật Ref Count (Bộ đếm tham chiếu)
                    if action == 'SUBSCRIBE':
                        if client == 'radar': self.stream_refs[symbol]['radar'] = True
                    elif action == 'UNSUBSCRIBE':
                        if client == 'radar': self.stream_refs[symbol]['radar'] = False
                    elif action == 'ENTER_TRADE':
                        self.stream_refs[symbol]['trade'] = True
                    elif action == 'EXIT_TRADE':
                        self.stream_refs[symbol]['trade'] = False

                    # Đánh giá xem có nên giữ kết nối không
                    should_run = (symbol == 'btcusdt') or self.stream_refs[symbol]['radar'] or self.stream_refs[symbol]['trade']
                    is_running = symbol in self.buffers

                    streams = self._get_stream_names(symbol)

                    if should_run and not is_running:
                        print(f"🎯 [KẾT NỐI] Móc ống hút vào: {symbol.upper()}")
                        self.buffers[symbol] = FeatureBuffer(symbol)
                        self.active_streams.update(streams)
                        await self.send_ws_command("SUBSCRIBE", streams)

                    elif not should_run and is_running:
                        print(f"🗑️ [RÚT ỐNG] Bỏ theo dõi: {symbol.upper()}")
                        self.active_streams.difference_update(streams)
                        await self.send_ws_command("UNSUBSCRIBE", streams)
                        del self.buffers[symbol]
                        del self.stream_refs[symbol]

                except Exception as e:
                    print(f"❌ [LỖI REDIS LISTENER]: {e}")

    async def publish_features(self):
        print("🚀 [PYTHON] Khởi chạy Động cơ Bơm máu (100ms/nhịp)...")
        while True:
            for symbol, buffer in list(self.buffers.items()):
                if symbol == 'btcusdt' and not (self.stream_refs.get('btcusdt', {}).get('trade', False)):
                    continue # BTC chỉ dùng để tính Relative Strength, không publish nếu không trade

                if buffer.is_warm:
                    current_funding = self.global_funding_rates.get(symbol, 0.0001)
                    features = buffer.extract_features(self.btc_pct, current_funding)
                    packed_data = msgpack.packb(features)
                    await self.redis.publish(f"market:features:{symbol.upper()}", packed_data)
            await asyncio.sleep(0.1) 

    async def run(self):
        print("🧠 [PYTHON ENGINE] Data Feed V17 (Multiplexing) Khởi động...")
        
        # Mặc định phải luôn theo dõi BTC để tính Relative Strength cho các coin khác
        self.stream_refs['btcusdt'] = {'radar': True, 'trade': False}
        self.buffers['btcusdt'] = FeatureBuffer('btcusdt')
        self.active_streams.update(self._get_stream_names('btcusdt'))
        
        await asyncio.gather(
            self.connect_binance_multiplex(),
            self.fetch_funding_rates(),
            self.publish_features(),
            self.listen_to_control_channels() 
        )

if __name__ == "__main__":
    handler = FeedHandler()
    asyncio.run(handler.run())