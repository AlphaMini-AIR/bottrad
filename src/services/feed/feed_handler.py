import asyncio
import json
import os
import time
import random
from collections import deque
from typing import Any, Dict, Optional, Set

import msgpack
import redis.asyncio as aioredis
import websockets


# ============================================================
# FEED HANDLER V25.2 - PRODUCTION-STABLE LIVE MARKET DATA FEED
# ------------------------------------------------------------
# Vai trò:
# 1. Nhận SUBSCRIBE / UNSUBSCRIBE / ENTER_TRADE / EXIT_TRADE từ Redis.
# 2. Nhận radar:candidates và tự kích hoạt stream ref cho symbol.
# 3. Subscribe realtime Binance Futures WebSocket.
# 4. Build feature realtime cho OrderManager / ONNX.
# 5. Giữ nguyên 13 field ONNX cũ để tương thích.
# 6. Bổ sung bookTicker, kline_1m, microprice, quality flags.
# 7. Bổ sung liquidation quote, max trade quote, packet rate log.
# 8. Không đặt lệnh, không dùng API key, không quyết định trade.
# ============================================================


# ============================================================
# 1. HARD-CODED RUNTIME CONFIG
# ============================================================

REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")

BINANCE_PUBLIC_WS_URL = "wss://fstream.binance.com/public/stream"
BINANCE_MARKET_WS_URL = "wss://fstream.binance.com/market/stream"

SUBSCRIPTION_CHANNEL = "system:subscriptions"
KEEP_ALIVE_CHANNEL = "system:keep_alive"
RADAR_CANDIDATE_CHANNEL = "radar:candidates"
FEATURE_CHANNEL_PREFIX = "market:features"

# Giới hạn số stream trên 1 WebSocket connection.
# Tăng: chứa được nhiều stream hơn nhưng dễ chạm giới hạn Binance / nặng connection.
# Giảm: an toàn hơn nhưng ít symbol hơn.
# Khuyến nghị: giữ 900 để có buffer an toàn dưới ngưỡng thực tế.
MAX_STREAMS_PER_CONNECTION = 900

# Số symbol tối đa Radar được phép feed cùng lúc.
# Tăng: nhiều cơ hội hơn nhưng CPU/Redis/WebSocket nặng hơn.
# Giảm: nhẹ hơn nhưng dễ bỏ lỡ altcoin đang chạy.
# Khuyến nghị: giữ 80 cho 4 stream/symbol là khá cân bằng.
MAX_SYMBOLS_PER_HANDLER = 80

# Chu kỳ reconcile stream mong muốn với stream đang subscribe.
# Tăng: giảm spam control message nhưng phản ứng subscribe/unsubscribe chậm hơn.
# Giảm: phản ứng nhanh hơn nhưng có thể gửi control dày hơn.
# Khuyến nghị: giữ 0.5s.
CONTROL_FLUSH_INTERVAL_SEC = 0.5

# Khoảng cách tối thiểu giữa 2 control message gửi lên Binance.
# Tăng: an toàn rate-limit hơn nhưng subscribe chậm hơn.
# Giảm: nhanh hơn nhưng dễ bị Binance reject nếu spam.
# Khuyến nghị: giữ 0.25s.
CONTROL_MIN_INTERVAL_SEC = 0.25

# Số stream trong mỗi lệnh SUBSCRIBE/UNSUBSCRIBE.
# Tăng: ít message hơn nhưng payload lớn hơn.
# Giảm: payload nhỏ hơn nhưng nhiều message hơn.
# Khuyến nghị: giữ 200.
CONTROL_CHUNK_SIZE = 200

# Tần suất publish feature cho symbol đang có vị thế thật.
# Tăng: nhẹ Redis/CPU hơn nhưng OrderManager nhận dữ liệu chậm hơn.
# Giảm: realtime hơn nhưng nặng Redis hơn.
# Khuyến nghị: giữ 0.10s cho scalping.
PUBLISH_FAST_INTERVAL_SEC = 0.10

# Tần suất publish feature cho symbol chỉ do Radar theo dõi.
# Tăng: nhẹ hệ thống hơn nhưng tín hiệu vào lệnh chậm hơn.
# Giảm: nhạy hơn nhưng Redis/CPU tăng.
# Khuyến nghị: giữ 0.25s.
PUBLISH_RADAR_INTERVAL_SEC = 0.25

# Sleep của vòng publish.
# Tăng: giảm CPU nhưng publish kém mượt hơn.
# Giảm: mượt hơn nhưng tốn CPU hơn.
# Khuyến nghị: giữ 0.05s.
PUBLISH_LOOP_SLEEP_SEC = 0.05

# Tuổi tối đa của giá gần nhất để xem dữ liệu còn dùng được.
# Tăng: ít bị stale hơn nhưng có thể dùng giá cũ.
# Giảm: an toàn hơn nhưng dễ ngắt publish khi thị trường im.
# Khuyến nghị: giữ 8s.
MAX_PRICE_AGE_SEC = 8.0

# Tuổi tối đa của depth20.
# Tăng: đỡ stale hơn nhưng orderbook có thể cũ.
# Giảm: nghiêm hơn nhưng dễ làm feature không ready.
# Khuyến nghị: giữ 2.5s vì depth20 rất quan trọng.
MAX_DEPTH_AGE_SEC = 2.5

# Tuổi tối đa của mark price/funding.
# Tăng: ít stale hơn nhưng funding/mark có thể cũ.
# Giảm: nghiêm hơn nhưng không cần thiết vì mark/funding không phải micro tick.
# Khuyến nghị: giữ 15s.
MAX_MARK_AGE_SEC = 15.0

# Tuổi tối đa của bookTicker.
# Tăng: giảm tụt quality score khi best bid/ask không đổi.
# Giảm: nghiêm hơn nhưng dễ stale giả.
# Khuyến nghị: giữ 5s.
MAX_BOOK_TICKER_AGE_SEC = 5.0

# Tuổi tối đa của aggTrade gần nhất.
# Tăng: ít stale hơn khi thị trường ít giao dịch.
# Giảm: chỉ chấp nhận coin đang có trade rất mới.
# Khuyến nghị: giữ 5s.
MAX_TRADE_AGE_SEC = 5.0

# Tuổi tối đa của kline_1m.
# Tăng: ít stale hơn nhưng candle có thể quá cũ.
# Giảm: nghiêm hơn nhưng không cần vì kline 1m cập nhật theo giây trong nến.
# Khuyến nghị: giữ 70s.
MAX_KLINE_AGE_SEC = 70.0

# Tuổi tối đa của radar context.
# Tăng: giữ metadata Radar lâu hơn nhưng có thể lỗi thời.
# Giảm: context sạch hơn nhưng dễ mất lý do Radar.
# Khuyến nghị: giữ 10 phút.
MAX_RADAR_CONTEXT_AGE_SEC = 10 * 60.0

MIN_WARMUP_SEC = 2.0

# Cửa sổ local price để tính body/wick/pct ngắn hạn.
# Tăng: tín hiệu mượt hơn nhưng phản ứng chậm hơn.
# Giảm: nhạy hơn nhưng nhiễu hơn.
# Khuyến nghị: giữ 12s cho scalping.
LOCAL_PRICE_WINDOW_SEC = 12.0

# Cửa sổ local trade để tính taker ratio/VPIN.
# Tăng: mượt hơn, ít nhiễu hơn nhưng trễ hơn.
# Giảm: nhạy hơn nhưng dễ nhiễu bởi vài lệnh lớn.
# Khuyến nghị: giữ 12s.
LOCAL_TRADE_WINDOW_SEC = 12.0

USE_LOCAL_PRICE_FEATURES = True

# Ngưỡng USDT để xem một trade là whale trade.
# Tăng: chỉ bắt lệnh rất lớn, ít nhiễu hơn.
# Giảm: bắt nhiều whale hơn nhưng dễ nhiễu với coin thanh khoản thấp.
# Khuyến nghị: giữ 5000 cho altcoin futures.
WHALE_TRADE_USDT = 5000.0

RECONNECT_BASE_DELAY_SEC = 1.0
RECONNECT_MAX_DELAY_SEC = 60.0
WS_ROTATE_SEC = 23.5 * 3600
WS_MAX_QUEUE = 4096

# Thời gian giữ buffer sau khi symbol không còn radar/trade.
# Tăng: tránh mất warmup khi symbol quay lại nhưng tốn RAM hơn.
# Giảm: sạch RAM nhanh hơn nhưng dễ mất context.
# Khuyến nghị: giữ 5s.
BUFFER_CLEANUP_GRACE_SEC = 5.0

# Chu kỳ log trạng thái feed.
# Tăng: log ít hơn, đỡ rối.
# Giảm: dễ debug hơn nhưng log nhiều.
# Khuyến nghị: giữ 30s.
STREAM_STATE_LOG_INTERVAL_SEC = 30.0

GLOBAL_MARKET_STREAMS = {
    "!ticker@arr",
    "!markPrice@arr",
    "!forceOrder@arr",
}

SAFE_SYMBOL_REGEX_SUFFIX = "USDT"


# ============================================================
# 2. UTILS
# ============================================================

def now_ts() -> float:
    return time.time()


def now_ms() -> int:
    return int(time.time() * 1000)


def safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        if value is None:
            return fallback
        n = float(value)
        if n != n:
            return fallback
        return n
    except Exception:
        return fallback


def safe_int(value: Any, fallback: int = 0) -> int:
    try:
        if value is None:
            return fallback
        return int(value)
    except Exception:
        return fallback


def normalize_symbol(symbol: Any) -> str:
    return str(symbol or "").upper().strip()


def normalize_symbol_lower(symbol: Any) -> str:
    return normalize_symbol(symbol).lower()


def is_safe_symbol(symbol: str) -> bool:
    if not symbol:
        return False

    s = normalize_symbol(symbol)

    if not s.endswith(SAFE_SYMBOL_REGEX_SUFFIX):
        return False

    return all(ch.isupper() or ch.isdigit() for ch in s)


def jitter_delay(base: float, ratio: float = 0.25) -> float:
    spread = base * ratio
    return max(0.1, base - spread + random.random() * spread * 2)


def chunked(items, size: int):
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i:i + size]


def clamp01(value: float) -> float:
    return max(0.0, min(1.0, value))


# ============================================================
# 3. FEATURE BUFFER
# ============================================================

class FeatureBuffer:
    """
    Buffer realtime theo từng symbol.

    Giữ nguyên 13 field ONNX cũ:
    1. ob_imb_top20
    2. spread_close
    3. bid_vol_1pct
    4. ask_vol_1pct
    5. max_buy_trade
    6. max_sell_trade
    7. liq_long_vol
    8. liq_short_vol
    9. funding_rate
    10. taker_buy_ratio
    11. body_size
    12. wick_size
    13. btc_relative_strength
    """

    def __init__(self, symbol: str):
        self.symbol = normalize_symbol_lower(symbol)
        self.created_at = now_ts()
        self.is_warm = False

        self.last_price_ts = 0.0
        self.last_ticker_ts = 0.0
        self.last_depth_ts = 0.0
        self.last_trade_ts = 0.0
        self.last_force_order_ts = 0.0
        self.last_mark_ts = 0.0
        self.last_radar_ts = 0.0
        self.last_publish_ts = 0.0
        self.last_book_ticker_ts = 0.0
        self.last_kline_ts = 0.0

        self.best_bid = 0.0
        self.best_ask = 0.0
        self.best_bid_qty = 0.0
        self.best_ask_qty = 0.0
        self.last_price = 0.0
        self.mark_price = 0.0
        self.index_price = 0.0
        self.funding_rate = 0.0001
        self.next_funding_time = 0

        self.top_book_imbalance = 0.0
        self.mid_price = 0.0
        self.microprice = 0.0
        self.microprice_bias = 0.0
        self.spread_bps = 0.0

        self.open_price = 0.0
        self.high_price = 0.0
        self.low_price = 0.0
        self.quote_volume_24h = 0.0
        self.base_volume_24h = 0.0
        self.price_change_pct_24h = 0.0
        self.trade_count_24h = 0

        self.k1m_open = 0.0
        self.k1m_high = 0.0
        self.k1m_low = 0.0
        self.k1m_close = 0.0
        self.k1m_volume = 0.0
        self.k1m_quote_volume = 0.0
        self.k1m_trade_count = 0
        self.k1m_taker_buy_base = 0.0
        self.k1m_taker_buy_quote = 0.0
        self.k1m_is_closed = False
        self.k1m_start_time = 0
        self.k1m_close_time = 0

        self.k1m_range_pct = 0.0
        self.k1m_body_pct = 0.0
        self.k1m_wick_pct = 0.0
        self.k1m_close_position = 0.5
        self.k1m_taker_buy_ratio = 0.5

        self.price_window = deque(maxlen=10000)
        self.trade_window = deque(maxlen=20000)

        self.ob_imb_top20 = 0.0
        self.spread_close = 0.0
        self.bid_vol_1pct = 0.0
        self.ask_vol_1pct = 0.0
        self.bid_quote_1pct = 0.0
        self.ask_quote_1pct = 0.0

        self.max_buy_trade = 0.0
        self.max_sell_trade = 0.0
        self.max_buy_trade_quote = 0.0
        self.max_sell_trade_quote = 0.0
        self.whale_netflow = 0.0
        self.buy_quote_window = 0.0
        self.sell_quote_window = 0.0

        self.liq_long_vol = 0.0
        self.liq_short_vol = 0.0
        self.liq_long_quote = 0.0
        self.liq_short_quote = 0.0

        self.atr14 = 0.003
        self.vpin = 0.5
        self.ofi = 0.0
        self.ofi_smoothed = 0.0
        self._last_ofi_smoothed = 0.0
        self.mfa = 0.0

        self.radar: Dict[str, Any] = {}

    def _append_price(self, price: float):
        if price <= 0:
            return

        ts = now_ts()
        self.price_window.append((ts, price))
        self.last_price = price
        self.last_price_ts = ts
        self._prune_price_window(ts)

    def _append_trade(self, quote: float, is_taker_buy: bool):
        if quote <= 0:
            return

        ts = now_ts()
        self.trade_window.append((ts, quote, is_taker_buy))
        self._prune_trade_window(ts)

    def _prune_price_window(self, ts: Optional[float] = None):
        if ts is None:
            ts = now_ts()

        cutoff = ts - max(LOCAL_PRICE_WINDOW_SEC, 1.0)

        while self.price_window and self.price_window[0][0] < cutoff:
            self.price_window.popleft()

    def _prune_trade_window(self, ts: Optional[float] = None):
        if ts is None:
            ts = now_ts()

        cutoff = ts - max(LOCAL_TRADE_WINDOW_SEC, 1.0)

        while self.trade_window and self.trade_window[0][0] < cutoff:
            self.trade_window.popleft()

    def _local_price_stats(self) -> Optional[Dict[str, float]]:
        self._prune_price_window()

        if len(self.price_window) < 2:
            return None

        prices = [p for _, p in self.price_window if p > 0]

        if len(prices) < 2:
            return None

        open_p = prices[0]
        close_p = prices[-1]
        high_p = max(prices)
        low_p = min(prices)

        if open_p <= 0 or close_p <= 0:
            return None

        body_size = abs(close_p - open_p) / open_p * 100
        wick_size = max(0.0, (high_p - low_p) - abs(close_p - open_p)) / open_p * 100
        atr = (high_p - low_p) / close_p if close_p > 0 else self.atr14
        pct = (close_p - open_p) / open_p * 100

        return {
            "pct": float(pct),
            "body_size": float(body_size),
            "wick_size": float(wick_size),
            "atr": float(atr),
            "open": float(open_p),
            "high": float(high_p),
            "low": float(low_p),
            "close": float(close_p),
        }

    def _local_trade_stats(self) -> Dict[str, float]:
        self._prune_trade_window()

        buy_quote = 0.0
        sell_quote = 0.0

        for _, quote, is_taker_buy in self.trade_window:
            if is_taker_buy:
                buy_quote += quote
            else:
                sell_quote += quote

        total_quote = buy_quote + sell_quote
        taker_buy_ratio = buy_quote / total_quote if total_quote > 0 else 0.5
        vpin = abs(buy_quote - sell_quote) / total_quote if total_quote > 0 else 0.5

        self.buy_quote_window = buy_quote
        self.sell_quote_window = sell_quote
        self.vpin = vpin

        return {
            "buy_quote": buy_quote,
            "sell_quote": sell_quote,
            "total_quote": total_quote,
            "taker_buy_ratio": taker_buy_ratio,
            "vpin": vpin,
        }

    def get_local_pct(self) -> float:
        stats = self._local_price_stats()

        if stats:
            return float(stats["pct"])

        if self.open_price > 0 and self.last_price > 0:
            return (self.last_price - self.open_price) / self.open_price * 100

        return 0.0

    def update_ticker(self, data: dict):
        ts = now_ts()
        self.last_ticker_ts = ts

        last_price = safe_float(data.get("c"), self.last_price)

        self.open_price = safe_float(data.get("o"), self.open_price)
        self.high_price = safe_float(data.get("h"), self.high_price)
        self.low_price = safe_float(data.get("l"), self.low_price)
        self.quote_volume_24h = safe_float(data.get("q"), self.quote_volume_24h)
        self.base_volume_24h = safe_float(data.get("v"), self.base_volume_24h)
        self.price_change_pct_24h = safe_float(data.get("P"), self.price_change_pct_24h)
        self.trade_count_24h = safe_int(data.get("n"), self.trade_count_24h)

        if last_price > 0:
            self._append_price(last_price)

        if self.last_price > 0 and self.high_price > 0 and self.low_price > 0:
            self.atr14 = max(1e-8, (self.high_price - self.low_price) / self.last_price)

    def update_mark_price(self, data: dict):
        ts = now_ts()
        self.last_mark_ts = ts

        mark_price = safe_float(data.get("p"), self.mark_price)
        index_price = safe_float(data.get("i"), self.index_price)
        funding_rate = safe_float(data.get("r"), self.funding_rate)
        next_funding_time = safe_int(data.get("T"), self.next_funding_time)

        if mark_price > 0:
            self.mark_price = mark_price

        if index_price > 0:
            self.index_price = index_price

        self.funding_rate = funding_rate
        self.next_funding_time = next_funding_time

    def update_book_ticker(self, data: dict):
        self.last_book_ticker_ts = now_ts()

        best_bid = safe_float(data.get("b"), self.best_bid)
        best_bid_qty = safe_float(data.get("B"), self.best_bid_qty)
        best_ask = safe_float(data.get("a"), self.best_ask)
        best_ask_qty = safe_float(data.get("A"), self.best_ask_qty)

        if best_bid > 0:
            self.best_bid = best_bid

        if best_ask > 0:
            self.best_ask = best_ask

        if best_bid_qty >= 0:
            self.best_bid_qty = best_bid_qty

        if best_ask_qty >= 0:
            self.best_ask_qty = best_ask_qty

        if self.best_bid > 0 and self.best_ask > 0 and self.best_ask > self.best_bid:
            self.mid_price = (self.best_bid + self.best_ask) / 2.0
            self.spread_bps = (
                (self.best_ask - self.best_bid) / self.mid_price * 10000.0
                if self.mid_price > 0
                else 0.0
            )

            qty_sum = self.best_bid_qty + self.best_ask_qty

            if qty_sum > 0:
                self.top_book_imbalance = (self.best_bid_qty - self.best_ask_qty) / qty_sum
                self.microprice = (
                    self.best_ask * self.best_bid_qty + self.best_bid * self.best_ask_qty
                ) / qty_sum
                self.microprice_bias = (
                    (self.microprice - self.mid_price) / self.mid_price
                    if self.mid_price > 0
                    else 0.0
                )
            else:
                self.top_book_imbalance = 0.0
                self.microprice = self.mid_price
                self.microprice_bias = 0.0

    def update_depth(self, data: dict):
        bids = data.get("b", [])
        asks = data.get("a", [])

        if not bids or not asks:
            return

        self.last_depth_ts = now_ts()

        depth_best_bid = safe_float(bids[0][0])
        depth_best_ask = safe_float(asks[0][0])

        if depth_best_bid > 0:
            self.best_bid = depth_best_bid

        if depth_best_ask > 0:
            self.best_ask = depth_best_ask

        bid_vol = sum(safe_float(v) for _, v in bids)
        ask_vol = sum(safe_float(v) for _, v in asks)
        total_vol = bid_vol + ask_vol

        self.ob_imb_top20 = (bid_vol - ask_vol) / total_vol if total_vol > 0 else 0.0

        close_p = self.last_price if self.last_price > 0 else self.mark_price

        if close_p > 0 and self.best_ask > 0 and self.best_bid > 0:
            self.spread_close = (self.best_ask - self.best_bid) / close_p

            bid_1pct_price = self.best_bid * 0.99
            ask_1pct_price = self.best_ask * 1.01

            self.bid_vol_1pct = sum(
                safe_float(v)
                for p, v in bids
                if safe_float(p) >= bid_1pct_price
            )
            self.ask_vol_1pct = sum(
                safe_float(v)
                for p, v in asks
                if safe_float(p) <= ask_1pct_price
            )

            self.bid_quote_1pct = sum(
                safe_float(p) * safe_float(v)
                for p, v in bids
                if safe_float(p) >= bid_1pct_price
            )
            self.ask_quote_1pct = sum(
                safe_float(p) * safe_float(v)
                for p, v in asks
                if safe_float(p) <= ask_1pct_price
            )

        self.ofi = self.ob_imb_top20 * 100
        self.ofi_smoothed = self.ofi_smoothed * 0.7 + self.ofi * 0.3

    def update_trade(self, data: dict):
        self.last_trade_ts = now_ts()

        qty = safe_float(data.get("q"), 0.0)
        price = safe_float(data.get("p"), self.last_price)
        is_buyer_maker = data.get("m", True)

        if price > 0:
            self._append_price(price)

        trade_quote = qty * price
        taker_buy = not bool(is_buyer_maker)

        if trade_quote > 0:
            self._append_trade(trade_quote, taker_buy)

        if taker_buy:
            if qty > self.max_buy_trade:
                self.max_buy_trade = qty

            if trade_quote > self.max_buy_trade_quote:
                self.max_buy_trade_quote = trade_quote

            if trade_quote >= WHALE_TRADE_USDT:
                self.whale_netflow += trade_quote
        else:
            if qty > self.max_sell_trade:
                self.max_sell_trade = qty

            if trade_quote > self.max_sell_trade_quote:
                self.max_sell_trade_quote = trade_quote

            if trade_quote >= WHALE_TRADE_USDT:
                self.whale_netflow -= trade_quote

    def update_force_order(self, data: dict):
        self.last_force_order_ts = now_ts()

        order_info = data.get("o", {})
        side = order_info.get("S", "")
        qty = safe_float(order_info.get("q"), 0.0)
        price = safe_float(order_info.get("p"), self.last_price or self.mark_price)

        quote = qty * price if qty > 0 and price > 0 else 0.0

        if side == "SELL":
            self.liq_long_vol += qty
            self.liq_long_quote += quote
        elif side == "BUY":
            self.liq_short_vol += qty
            self.liq_short_quote += quote

    def update_kline_1m(self, data: dict):
        k = data.get("k", {})

        if not isinstance(k, dict):
            return

        self.last_kline_ts = now_ts()

        self.k1m_start_time = safe_int(k.get("t"), self.k1m_start_time)
        self.k1m_close_time = safe_int(k.get("T"), self.k1m_close_time)
        self.k1m_open = safe_float(k.get("o"), self.k1m_open)
        self.k1m_high = safe_float(k.get("h"), self.k1m_high)
        self.k1m_low = safe_float(k.get("l"), self.k1m_low)
        self.k1m_close = safe_float(k.get("c"), self.k1m_close)
        self.k1m_volume = safe_float(k.get("v"), self.k1m_volume)
        self.k1m_trade_count = safe_int(k.get("n"), self.k1m_trade_count)
        self.k1m_is_closed = bool(k.get("x", False))
        self.k1m_quote_volume = safe_float(k.get("q"), self.k1m_quote_volume)
        self.k1m_taker_buy_base = safe_float(k.get("V"), self.k1m_taker_buy_base)
        self.k1m_taker_buy_quote = safe_float(k.get("Q"), self.k1m_taker_buy_quote)

        if self.k1m_close > 0:
            self._append_price(self.k1m_close)

        if self.k1m_open > 0 and self.k1m_high > 0 and self.k1m_low > 0 and self.k1m_close > 0:
            candle_range = self.k1m_high - self.k1m_low
            candle_body = abs(self.k1m_close - self.k1m_open)

            self.k1m_range_pct = max(0.0, candle_range / self.k1m_open * 100.0)
            self.k1m_body_pct = max(0.0, candle_body / self.k1m_open * 100.0)
            self.k1m_wick_pct = max(0.0, candle_range - candle_body) / self.k1m_open * 100.0

            if candle_range > 0:
                self.k1m_close_position = clamp01((self.k1m_close - self.k1m_low) / candle_range)
            else:
                self.k1m_close_position = 0.5

            self.atr14 = max(1e-8, candle_range / self.k1m_close)

        if self.k1m_quote_volume > 0:
            self.k1m_taker_buy_ratio = clamp01(self.k1m_taker_buy_quote / self.k1m_quote_volume)
        else:
            self.k1m_taker_buy_ratio = 0.5

    def update_radar_candidate(self, candidate: dict):
        self.last_radar_ts = now_ts()

        self.radar = {
            "radar_score": safe_float(candidate.get("score"), 0.0),
            "radar_tier": str(candidate.get("tier") or ""),
            "radar_direction": str(candidate.get("direction") or ""),
            "radar_dir_conf": safe_float(candidate.get("dirConf"), 0.0),
            "radar_reason": str(candidate.get("reason") or ""),
            "radar_tags": candidate.get("tags") if isinstance(candidate.get("tags"), list) else [],
            "radar_scan_id": str(candidate.get("scanId") or ""),
            "radar_detected_at": safe_int(candidate.get("detectedAt"), 0),
            "radar_local_detected_at": safe_int(candidate.get("localDetectedAt"), 0),
            "radar_strong_momentum": bool(candidate.get("strongMomentum", False)),

            "radar_move_pct": safe_float(candidate.get("movePct"), 0.0),
            "radar_range_pct": safe_float(candidate.get("rangePct"), 0.0),
            "radar_volume_ratio": safe_float(candidate.get("volumeRatio"), 0.0),
            "radar_quote_volume_ratio": safe_float(candidate.get("quoteVolumeRatio"), 0.0),
            "radar_trades_ratio": safe_float(candidate.get("tradesRatio"), 0.0),
            "radar_spread_bps": safe_float(candidate.get("bookSpreadBps"), 0.0),

            "radar_trend_score": safe_float(candidate.get("trendScore"), 0.0),
            "radar_breakout_score": safe_float(candidate.get("breakoutScore"), 0.0),
            "radar_chop_penalty": safe_float(candidate.get("chopPenalty"), 0.0),
            "radar_wick_penalty": safe_float(candidate.get("wickPenalty"), 0.0),
            "radar_late_move_penalty": safe_float(candidate.get("lateMovePenalty"), 0.0),
            "radar_volatility_score": safe_float(candidate.get("volatilityScore"), 0.0),
        }

    def is_ready(self) -> bool:
        ts = now_ts()

        if ts - self.created_at < MIN_WARMUP_SEC:
            return False

        if self.last_price <= 0 and self.mark_price <= 0:
            return False

        if self.best_bid <= 0 or self.best_ask <= 0:
            return False

        if self.best_ask <= self.best_bid:
            return False

        if ts - self.last_price_ts > MAX_PRICE_AGE_SEC:
            return False

        if ts - self.last_depth_ts > MAX_DEPTH_AGE_SEC:
            return False

        self.is_warm = True
        return True

    def _age_ms(self, current_ts: float, last_ts: float) -> Optional[int]:
        if last_ts <= 0:
            return None
        return int((current_ts - last_ts) * 1000)

    def _build_quality_flags(
        self,
        price_age_ms: Optional[int],
        depth_age_ms: Optional[int],
        trade_age_ms: Optional[int],
        mark_price_age_ms: Optional[int],
        book_ticker_age_ms: Optional[int],
        kline_age_ms: Optional[int],
        radar_age_ms: Optional[int],
    ) -> Dict[str, Any]:
        has_fresh_price = price_age_ms is not None and price_age_ms <= MAX_PRICE_AGE_SEC * 1000
        has_fresh_depth = depth_age_ms is not None and depth_age_ms <= MAX_DEPTH_AGE_SEC * 1000
        has_fresh_trade = trade_age_ms is not None and trade_age_ms <= MAX_TRADE_AGE_SEC * 1000
        has_fresh_mark = mark_price_age_ms is not None and mark_price_age_ms <= MAX_MARK_AGE_SEC * 1000
        has_fresh_book = book_ticker_age_ms is not None and book_ticker_age_ms <= MAX_BOOK_TICKER_AGE_SEC * 1000
        has_fresh_kline = kline_age_ms is not None and kline_age_ms <= MAX_KLINE_AGE_SEC * 1000
        has_radar_context = radar_age_ms is not None and radar_age_ms <= MAX_RADAR_CONTEXT_AGE_SEC * 1000

        score = 0.0
        score += 0.22 if has_fresh_price else 0.0
        score += 0.20 if has_fresh_depth else 0.0
        score += 0.18 if has_fresh_trade else 0.0
        score += 0.18 if has_fresh_book else 0.0
        score += 0.08 if has_fresh_mark else 0.0
        score += 0.08 if has_fresh_kline else 0.0
        score += 0.06 if has_radar_context else 0.0

        feature_ready_score = clamp01(score)
        is_data_stale = feature_ready_score < 0.65

        return {
            "has_fresh_price": bool(has_fresh_price),
            "has_fresh_depth": bool(has_fresh_depth),
            "has_fresh_trade": bool(has_fresh_trade),
            "has_fresh_mark": bool(has_fresh_mark),
            "has_fresh_book": bool(has_fresh_book),
            "has_fresh_kline": bool(has_fresh_kline),
            "has_radar_context": bool(has_radar_context),
            "is_data_stale": bool(is_data_stale),
            "feature_ready_score": float(feature_ready_score),
        }

    def extract_features(self, btc_pct: float, fallback_funding_rate: float) -> Dict[str, Any]:
        local_stats = self._local_price_stats() if USE_LOCAL_PRICE_FEATURES else None
        trade_stats = self._local_trade_stats()

        if local_stats:
            coin_pct = local_stats["pct"]
            body_size = local_stats["body_size"]
            wick_size = local_stats["wick_size"]
            self.atr14 = local_stats["atr"]
        else:
            coin_pct = (
                (self.last_price - self.open_price) / self.open_price * 100
                if self.open_price > 0
                else 0.0
            )
            body_size = (
                abs(self.last_price - self.open_price) / self.open_price * 100
                if self.open_price > 0
                else 0.0
            )
            wick_size = (
                ((self.high_price - self.low_price) - abs(self.last_price - self.open_price))
                / self.open_price * 100
                if self.open_price > 0
                else 0.0
            )

        taker_buy_ratio = trade_stats["taker_buy_ratio"]
        btc_relative_strength = coin_pct - btc_pct

        self.mfa = self.ofi_smoothed - self._last_ofi_smoothed
        self._last_ofi_smoothed = self.ofi_smoothed

        close_p = self.last_price if self.last_price > 0 else (self.mark_price if self.mark_price > 0 else 1e-8)
        quote_vol_safe = self.quote_volume_24h if self.quote_volume_24h > 0 else 1e-8

        current_ts = now_ts()
        feature_ts = now_ms()

        mark_price_age_ms = self._age_ms(current_ts, self.last_mark_ts)
        depth_age_ms = self._age_ms(current_ts, self.last_depth_ts)
        price_age_ms = self._age_ms(current_ts, self.last_price_ts)
        trade_age_ms = self._age_ms(current_ts, self.last_trade_ts)
        radar_age_ms = self._age_ms(current_ts, self.last_radar_ts)
        book_ticker_age_ms = self._age_ms(current_ts, self.last_book_ticker_ts)
        kline_age_ms = self._age_ms(current_ts, self.last_kline_ts)

        funding_rate = self.funding_rate if self.last_mark_ts > 0 else fallback_funding_rate

        quality_flags = self._build_quality_flags(
            price_age_ms=price_age_ms,
            depth_age_ms=depth_age_ms,
            trade_age_ms=trade_age_ms,
            mark_price_age_ms=mark_price_age_ms,
            book_ticker_age_ms=book_ticker_age_ms,
            kline_age_ms=kline_age_ms,
            radar_age_ms=radar_age_ms,
        )

        total_max_trade_quote = self.max_buy_trade_quote + self.max_sell_trade_quote
        max_trade_quote_imbalance = (
            (self.max_buy_trade_quote - self.max_sell_trade_quote) / total_max_trade_quote
            if total_max_trade_quote > 0
            else 0.0
        )

        liq_net_quote = self.liq_short_quote - self.liq_long_quote

        features = {
            "symbol": self.symbol.upper(),
            "feature_ts": feature_ts,
            "ts": feature_ts,
            "timestamp": feature_ts,
            "is_warm": self.is_warm,

            "best_ask": float(self.best_ask),
            "best_bid": float(self.best_bid),
            "last_price": float(self.last_price or self.mark_price),
            "mark_price": float(self.mark_price or self.last_price),
            "index_price": float(self.index_price or self.mark_price or self.last_price),
            "next_funding_time": int(self.next_funding_time or 0),

            # =================================================
            # 13 FIELD ONNX CŨ - GIỮ NGUYÊN
            # =================================================
            "ob_imb_top20": float(self.ob_imb_top20),
            "spread_close": float(self.spread_close),
            "bid_vol_1pct": float((self.bid_vol_1pct * close_p) / quote_vol_safe),
            "ask_vol_1pct": float((self.ask_vol_1pct * close_p) / quote_vol_safe),
            "max_buy_trade": float((self.max_buy_trade * close_p) / quote_vol_safe),
            "max_sell_trade": float((self.max_sell_trade * close_p) / quote_vol_safe),
            "liq_long_vol": float((self.liq_long_vol * close_p) / quote_vol_safe),
            "liq_short_vol": float((self.liq_short_vol * close_p) / quote_vol_safe),
            "funding_rate": float(funding_rate),
            "taker_buy_ratio": float(taker_buy_ratio),
            "body_size": float(body_size),
            "wick_size": float(wick_size),
            "btc_relative_strength": float(btc_relative_strength),

            # =================================================
            # FIELD BỔ SUNG
            # =================================================
            "coin_pct": float(coin_pct),
            "btc_pct": float(btc_pct),
            "ATR14": float(self.atr14),
            "VPIN": float(self.vpin),
            "OFI": float(self.ofi_smoothed),
            "MFA": float(self.mfa),
            "WHALE_NET": float(self.whale_netflow / quote_vol_safe),

            "buy_quote_window": float(self.buy_quote_window),
            "sell_quote_window": float(self.sell_quote_window),
            "trade_quote_window": float(trade_stats["total_quote"]),
            "bid_quote_1pct": float(self.bid_quote_1pct),
            "ask_quote_1pct": float(self.ask_quote_1pct),

            "quote_volume_24h": float(self.quote_volume_24h),
            "base_volume_24h": float(self.base_volume_24h),
            "price_change_pct_24h": float(self.price_change_pct_24h),
            "trade_count_24h": int(self.trade_count_24h),

            "best_bid_qty": float(self.best_bid_qty),
            "best_ask_qty": float(self.best_ask_qty),
            "top_book_imbalance": float(self.top_book_imbalance),
            "mid_price": float(self.mid_price),
            "microprice": float(self.microprice),
            "microprice_bias": float(self.microprice_bias),
            "spread_bps": float(self.spread_bps),

            "k1m_open": float(self.k1m_open),
            "k1m_high": float(self.k1m_high),
            "k1m_low": float(self.k1m_low),
            "k1m_close": float(self.k1m_close),
            "k1m_volume": float(self.k1m_volume),
            "k1m_quote_volume": float(self.k1m_quote_volume),
            "k1m_trade_count": int(self.k1m_trade_count),
            "k1m_taker_buy_ratio": float(self.k1m_taker_buy_ratio),
            "k1m_range_pct": float(self.k1m_range_pct),
            "k1m_body_pct": float(self.k1m_body_pct),
            "k1m_wick_pct": float(self.k1m_wick_pct),
            "k1m_close_position": float(self.k1m_close_position),
            "k1m_is_closed": bool(self.k1m_is_closed),

            "liq_long_quote": float(self.liq_long_quote),
            "liq_short_quote": float(self.liq_short_quote),
            "liq_net_quote": float(liq_net_quote),

            "max_buy_trade_quote": float(self.max_buy_trade_quote),
            "max_sell_trade_quote": float(self.max_sell_trade_quote),
            "max_trade_quote_imbalance": float(max_trade_quote_imbalance),

            "mark_price_age_ms": mark_price_age_ms,
            "depth_age_ms": depth_age_ms,
            "price_age_ms": price_age_ms,
            "trade_age_ms": trade_age_ms,
            "radar_age_ms": radar_age_ms,
            "book_ticker_age_ms": book_ticker_age_ms,
            "kline_age_ms": kline_age_ms,
        }

        features.update(quality_flags)

        if self.radar:
            features.update(self.radar)

        self.max_buy_trade = 0.0
        self.max_sell_trade = 0.0
        self.max_buy_trade_quote = 0.0
        self.max_sell_trade_quote = 0.0

        self.liq_long_vol = 0.0
        self.liq_short_vol = 0.0
        self.liq_long_quote = 0.0
        self.liq_short_quote = 0.0

        self.whale_netflow *= 0.5

        return features


# ============================================================
# 4. FEED HANDLER
# ============================================================

class FeedHandler:
    def __init__(self):
        self.redis = aioredis.from_url(
            REDIS_URL,
            decode_responses=False,
            socket_keepalive=True,
            retry_on_timeout=True,
            health_check_interval=30,
        )

        self.buffers: Dict[str, FeatureBuffer] = {}
        self.stream_refs: Dict[str, Dict[str, bool]] = {}
        self.pending_buffer_cleanup: Dict[str, float] = {}

        self.global_funding_rates: Dict[str, float] = {}
        self.global_mark_prices: Dict[str, float] = {}
        self.global_index_prices: Dict[str, float] = {}
        self.global_next_funding_times: Dict[str, int] = {}

        self.desired_public_streams: Set[str] = set()
        self.desired_market_streams: Set[str] = set(GLOBAL_MARKET_STREAMS)

        self.ws_subscribed = {
            "public": set(),
            "market": set(),
        }

        self.ws_connections = {
            "public": None,
            "market": None,
        }

        self.control_locks = {
            "public": asyncio.Lock(),
            "market": asyncio.Lock(),
        }

        self.last_control_sent_ts = {
            "public": 0.0,
            "market": 0.0,
        }

        self.control_id = 1

        self.reconnect_attempts = {
            "public": 0,
            "market": 0,
        }

        self.btc_pct = 0.0
        self.last_state_log_ts = 0.0
        self.last_packet_stats_ts = now_ts()

        self.stats = {
            "packets": 0,
            "ticker_packets": 0,
            "mark_packets": 0,
            "depth_packets": 0,
            "trade_packets": 0,
            "force_packets": 0,
            "book_packets": 0,
            "kline_packets": 0,
            "published": 0,
            "publish_errors": 0,
            "control_messages": 0,
            "cleanup_buffers": 0,
        }

        self.last_packet_counts = dict(self.stats)

    # --------------------------------------------------------
    # Stream names
    # --------------------------------------------------------
    def _get_public_streams(self, symbol: str) -> Set[str]:
        sym = normalize_symbol_lower(symbol)
        return {
            f"{sym}@depth20@100ms",
            f"{sym}@bookTicker",
        }

    def _get_market_streams(self, symbol: str) -> Set[str]:
        sym = normalize_symbol_lower(symbol)
        return {
            f"{sym}@aggTrade",
            f"{sym}@kline_1m",
        }

    def _get_desired_streams(self, route: str) -> Set[str]:
        if route == "public":
            return self.desired_public_streams
        return self.desired_market_streams

    def _get_ws_url(self, route: str) -> str:
        return BINANCE_PUBLIC_WS_URL if route == "public" else BINANCE_MARKET_WS_URL

    # --------------------------------------------------------
    # Symbol lifecycle
    # --------------------------------------------------------
    def _ensure_symbol_ref(self, symbol: str):
        symbol = normalize_symbol_lower(symbol)

        if symbol not in self.stream_refs:
            self.stream_refs[symbol] = {
                "radar": False,
                "trade": False,
            }

    def _ensure_buffer(self, symbol: str):
        symbol = normalize_symbol_lower(symbol)

        if symbol in self.pending_buffer_cleanup:
            self.pending_buffer_cleanup.pop(symbol, None)

        if symbol not in self.buffers:
            self.buffers[symbol] = FeatureBuffer(symbol)

            if symbol in self.global_mark_prices:
                self.buffers[symbol].mark_price = self.global_mark_prices.get(symbol, 0.0)
                self.buffers[symbol].index_price = self.global_index_prices.get(symbol, 0.0)
                self.buffers[symbol].funding_rate = self.global_funding_rates.get(symbol, 0.0001)
                self.buffers[symbol].next_funding_time = self.global_next_funding_times.get(symbol, 0)
                self.buffers[symbol].last_mark_ts = now_ts()

    def _symbol_should_have_buffer(self, symbol: str) -> bool:
        symbol = normalize_symbol_lower(symbol)

        if symbol == "btcusdt":
            return True

        refs = self.stream_refs.get(symbol, {})
        return bool(refs.get("radar", False) or refs.get("trade", False))

    def _symbol_should_have_symbol_streams(self, symbol: str) -> bool:
        symbol = normalize_symbol_lower(symbol)

        refs = self.stream_refs.get(symbol, {})
        return bool(refs.get("radar", False) or refs.get("trade", False))

    def _schedule_buffer_cleanup(self, symbol: str):
        symbol = normalize_symbol_lower(symbol)

        if symbol == "btcusdt":
            return

        if symbol in self.buffers:
            self.pending_buffer_cleanup[symbol] = now_ts() + BUFFER_CLEANUP_GRACE_SEC

    def _apply_symbol_stream_state(self, symbol: str):
        symbol = normalize_symbol_lower(symbol)

        should_buffer = self._symbol_should_have_buffer(symbol)
        should_stream = self._symbol_should_have_symbol_streams(symbol)

        public_streams = self._get_public_streams(symbol)
        market_streams = self._get_market_streams(symbol)

        if should_buffer:
            self._ensure_buffer(symbol)
            self.pending_buffer_cleanup.pop(symbol, None)
        else:
            self._schedule_buffer_cleanup(symbol)

        if should_stream:
            self.desired_public_streams.update(public_streams)
            self.desired_market_streams.update(market_streams)
        else:
            self.desired_public_streams.difference_update(public_streams)
            self.desired_market_streams.difference_update(market_streams)

        self.desired_market_streams.update(GLOBAL_MARKET_STREAMS)

    def _active_symbol_count(self) -> int:
        return len([
            sym
            for sym, refs in self.stream_refs.items()
            if refs.get("radar", False) or refs.get("trade", False)
        ])

    async def cleanup_buffer_loop(self):
        print("🧹 [CLEANUP] Khởi chạy buffer cleanup loop...")

        while True:
            try:
                ts = now_ts()

                for symbol, cleanup_at in list(self.pending_buffer_cleanup.items()):
                    if symbol == "btcusdt":
                        self.pending_buffer_cleanup.pop(symbol, None)
                        continue

                    if ts < cleanup_at:
                        continue

                    refs = self.stream_refs.get(symbol, {})
                    still_active = bool(refs.get("radar", False) or refs.get("trade", False))

                    if still_active:
                        self.pending_buffer_cleanup.pop(symbol, None)
                        continue

                    if symbol in self.buffers:
                        del self.buffers[symbol]
                        self.stats["cleanup_buffers"] += 1
                        print(f"🗑️ [CLEANUP] Xóa buffer sau grace: {symbol.upper()}")

                    self.pending_buffer_cleanup.pop(symbol, None)
                    self.stream_refs.pop(symbol, None)

            except Exception as e:
                print(f"⚠️ [CLEANUP] Lỗi cleanup buffer: {e}")

            await asyncio.sleep(1.0)

    # --------------------------------------------------------
    # WS control reconcile
    # --------------------------------------------------------
    async def _send_control(self, route: str, method: str, streams: Set[str]) -> bool:
        ws = self.ws_connections.get(route)

        if ws is None or not streams:
            return False

        streams_list = list(streams)

        for part in chunked(streams_list, CONTROL_CHUNK_SIZE):
            elapsed = now_ts() - self.last_control_sent_ts[route]

            if elapsed < CONTROL_MIN_INTERVAL_SEC:
                await asyncio.sleep(CONTROL_MIN_INTERVAL_SEC - elapsed)

            payload = {
                "method": method,
                "params": part,
                "id": self.control_id,
            }
            self.control_id += 1

            await ws.send(json.dumps(payload))
            self.last_control_sent_ts[route] = now_ts()
            self.stats["control_messages"] += 1

            print(f"📡 [WS {route.upper()}] {method} {len(part)} streams")

        return True

    async def reconcile_route_streams(self, route: str):
        async with self.control_locks[route]:
            ws = self.ws_connections.get(route)

            if ws is None:
                return

            desired = set(self._get_desired_streams(route))
            subscribed = self.ws_subscribed[route]

            if len(desired) > MAX_STREAMS_PER_CONNECTION:
                print(
                    f"🚨 [WS {route.upper()}] Vượt giới hạn streams: "
                    f"{len(desired)} > {MAX_STREAMS_PER_CONNECTION}"
                )
                desired = set(list(desired)[:MAX_STREAMS_PER_CONNECTION])

            to_unsubscribe = subscribed - desired
            to_subscribe = desired - subscribed

            if to_unsubscribe:
                ok = await self._send_control(route, "UNSUBSCRIBE", to_unsubscribe)
                if ok:
                    subscribed.difference_update(to_unsubscribe)

            if to_subscribe:
                ok = await self._send_control(route, "SUBSCRIBE", to_subscribe)
                if ok:
                    subscribed.update(to_subscribe)

    async def control_reconcile_loop(self):
        print("🧭 [WS CONTROL] Khởi chạy reconcile loop...")

        while True:
            try:
                await self.reconcile_route_streams("public")
                await self.reconcile_route_streams("market")
                self._log_state_if_needed()
            except Exception as e:
                print(f"⚠️ [WS CONTROL] Lỗi reconcile: {e}")

            await asyncio.sleep(CONTROL_FLUSH_INTERVAL_SEC)

    # --------------------------------------------------------
    # WebSocket loops
    # --------------------------------------------------------
    async def connect_route_loop(self, route: str):
        url = self._get_ws_url(route)

        print(f"🔗 [WS {route.upper()}] Chuẩn bị kết nối {url}")

        while True:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=None,
                    max_queue=WS_MAX_QUEUE,
                    close_timeout=5,
                ) as ws:
                    self.ws_connections[route] = ws
                    self.ws_subscribed[route].clear()
                    self.reconnect_attempts[route] = 0

                    connected_at = now_ts()

                    print(f"✅ [WS {route.upper()}] Đã kết nối")

                    await self.reconcile_route_streams(route)

                    while True:
                        if now_ts() - connected_at >= WS_ROTATE_SEC:
                            print(
                                f"♻️ [WS {route.upper()}] Rotate connection sau "
                                f"{round((now_ts() - connected_at) / 3600, 2)} giờ"
                            )
                            await ws.close()
                            break

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            await self.handle_ws_packet(route, msg)
                        except asyncio.TimeoutError:
                            continue

            except Exception as e:
                self.ws_connections[route] = None
                self.ws_subscribed[route].clear()

                attempt = self.reconnect_attempts[route]
                self.reconnect_attempts[route] = min(attempt + 1, 20)

                delay = min(RECONNECT_MAX_DELAY_SEC, RECONNECT_BASE_DELAY_SEC * (2 ** attempt))
                delay = jitter_delay(delay)

                print(
                    f"⚠️ [WS {route.upper()}] Mất kết nối/lỗi: {e}. "
                    f"Reconnect sau {round(delay, 2)}s"
                )
                await asyncio.sleep(delay)

    # --------------------------------------------------------
    # WebSocket message handler
    # --------------------------------------------------------
    async def handle_ws_packet(self, route: str, msg: str):
        try:
            packet = json.loads(msg)
        except Exception:
            return

        if "stream" not in packet:
            return

        stream_name = packet.get("stream", "")
        data = packet.get("data", None)

        if not stream_name:
            return

        self.stats["packets"] += 1

        if stream_name == "!ticker@arr":
            if isinstance(data, list):
                self.stats["ticker_packets"] += 1

                for item in data:
                    if not isinstance(item, dict):
                        continue

                    symbol = normalize_symbol_lower(item.get("s", ""))

                    if not symbol:
                        continue

                    buffer = self.buffers.get(symbol)

                    if symbol == "btcusdt" and buffer is None:
                        self._ensure_buffer("btcusdt")
                        buffer = self.buffers.get("btcusdt")

                    if buffer:
                        buffer.update_ticker(item)

            return

        if stream_name in ("!markPrice@arr", "!markPrice@arr@1s"):
            if isinstance(data, list):
                self.stats["mark_packets"] += 1

                for item in data:
                    if not isinstance(item, dict):
                        continue

                    symbol = normalize_symbol_lower(item.get("s", ""))

                    if not symbol:
                        continue

                    mark_price = safe_float(item.get("p"), 0.0)
                    index_price = safe_float(item.get("i"), 0.0)
                    funding_rate = safe_float(item.get("r"), 0.0001)
                    next_funding_time = safe_int(item.get("T"), 0)

                    self.global_mark_prices[symbol] = mark_price
                    self.global_index_prices[symbol] = index_price
                    self.global_funding_rates[symbol] = funding_rate
                    self.global_next_funding_times[symbol] = next_funding_time

                    buffer = self.buffers.get(symbol)

                    if buffer:
                        buffer.update_mark_price(item)

            return

        if stream_name == "!forceOrder@arr":
            items = data if isinstance(data, list) else [data]
            self.stats["force_packets"] += 1

            for item in items:
                if not isinstance(item, dict):
                    continue

                order_info = item.get("o", {})
                symbol = normalize_symbol_lower(order_info.get("s", ""))
                buffer = self.buffers.get(symbol)

                if buffer:
                    buffer.update_force_order(item)

            return

        symbol = normalize_symbol_lower(stream_name.split("@")[0])
        buffer = self.buffers.get(symbol)

        if not buffer:
            return

        if "@depth20" in stream_name:
            self.stats["depth_packets"] += 1

            if isinstance(data, dict):
                buffer.update_depth(data)

        elif "@bookTicker" in stream_name:
            self.stats["book_packets"] += 1

            if isinstance(data, dict):
                buffer.update_book_ticker(data)

        elif "@aggTrade" in stream_name:
            self.stats["trade_packets"] += 1

            if isinstance(data, dict):
                buffer.update_trade(data)

        elif "@kline_1m" in stream_name:
            self.stats["kline_packets"] += 1

            if isinstance(data, dict):
                buffer.update_kline_1m(data)

    # --------------------------------------------------------
    # Redis control listener
    # --------------------------------------------------------
    async def listen_to_control_channels(self):
        print("📡 [REDIS] Listener system:subscriptions / system:keep_alive...")

        while True:
            pubsub = None

            try:
                pubsub = self.redis.pubsub()
                await pubsub.subscribe(SUBSCRIPTION_CHANNEL, KEEP_ALIVE_CHANNEL)

                print("✅ [REDIS] Đã subscribe control channels")

                async for message in pubsub.listen():
                    if message.get("type") != "message":
                        continue

                    raw = message.get("data")

                    if raw is None:
                        continue

                    try:
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")

                        data = json.loads(raw)
                    except Exception:
                        continue

                    channel = message.get("channel")

                    if isinstance(channel, bytes):
                        channel = channel.decode("utf-8")

                    if channel == KEEP_ALIVE_CHANNEL:
                        continue

                    await self._handle_control_message(data)

            except Exception as e:
                print(f"⚠️ [REDIS] Listener lỗi/mất kết nối: {e}. Thử lại sau 3s...")
                await asyncio.sleep(3)
            finally:
                try:
                    if pubsub:
                        await pubsub.close()
                except Exception:
                    pass

    async def _handle_control_message(self, data: dict):
        symbol_raw = data.get("symbol", "")
        symbol_upper = normalize_symbol(symbol_raw)

        if not symbol_upper or not is_safe_symbol(symbol_upper):
            return

        symbol = symbol_upper.lower()
        action = str(data.get("action", "")).upper()
        client = str(data.get("client", "unknown")).lower()

        is_new_symbol = symbol not in self.stream_refs

        if action == "SUBSCRIBE" and client == "radar":
            if is_new_symbol and self._active_symbol_count() >= MAX_SYMBOLS_PER_HANDLER:
                print(
                    f"⚠️ [LIMIT] Bỏ SUBSCRIBE {symbol_upper}, "
                    f"vượt MAX_SYMBOLS_PER_HANDLER={MAX_SYMBOLS_PER_HANDLER}"
                )
                return

        self._ensure_symbol_ref(symbol)

        before_running = symbol in self.buffers

        if action == "SUBSCRIBE":
            if client == "radar":
                self.stream_refs[symbol]["radar"] = True

        elif action == "UNSUBSCRIBE":
            if client == "radar":
                self.stream_refs[symbol]["radar"] = False

        elif action == "ENTER_TRADE":
            self.stream_refs[symbol]["trade"] = True

        elif action == "EXIT_TRADE":
            self.stream_refs[symbol]["trade"] = False

        else:
            return

        self._apply_symbol_stream_state(symbol)

        after_running = symbol in self.buffers

        if after_running and not before_running:
            print(f"🎯 [KẾT NỐI] Bắt đầu theo dõi: {symbol_upper} | client={client} action={action}")
        elif before_running and not after_running:
            print(f"🗑️ [RÚT ỐNG] Ngừng theo dõi: {symbol_upper} | client={client} action={action}")
        elif before_running and not self._symbol_should_have_buffer(symbol):
            print(
                f"⏳ [GRACE] Chờ cleanup buffer: {symbol_upper} "
                f"sau {BUFFER_CLEANUP_GRACE_SEC}s | client={client} action={action}"
            )

    # --------------------------------------------------------
    # Radar candidate listener
    # --------------------------------------------------------
    async def listen_to_radar_candidates(self):
        print("📡 [REDIS] Listener radar:candidates...")

        while True:
            pubsub = None

            try:
                pubsub = self.redis.pubsub()
                await pubsub.subscribe(RADAR_CANDIDATE_CHANNEL)

                print("✅ [REDIS] Đã subscribe radar:candidates")

                async for message in pubsub.listen():
                    if message.get("type") != "message":
                        continue

                    raw = message.get("data")

                    if raw is None:
                        continue

                    try:
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")

                        candidate = json.loads(raw)
                    except Exception:
                        continue

                    symbol_upper = normalize_symbol(candidate.get("symbol", ""))

                    if not symbol_upper or not is_safe_symbol(symbol_upper):
                        continue

                    symbol = symbol_upper.lower()

                    self._ensure_symbol_ref(symbol)
                    self.stream_refs[symbol]["radar"] = True
                    self._apply_symbol_stream_state(symbol)

                    buffer = self.buffers.get(symbol)

                    if buffer:
                        buffer.update_radar_candidate(candidate)

            except Exception as e:
                print(f"⚠️ [REDIS] radar:candidates listener lỗi: {e}. Thử lại sau 3s...")
                await asyncio.sleep(3)
            finally:
                try:
                    if pubsub:
                        await pubsub.close()
                except Exception:
                    pass

    # --------------------------------------------------------
    # Publish features
    # --------------------------------------------------------
    def _should_publish_symbol(self, symbol: str, buffer: FeatureBuffer, ts: float) -> bool:
        if symbol == "btcusdt" and not self.stream_refs.get("btcusdt", {}).get("trade", False):
            return False

        if not buffer.is_ready():
            return False

        refs = self.stream_refs.get(symbol, {})
        publish_interval = PUBLISH_FAST_INTERVAL_SEC if refs.get("trade", False) else PUBLISH_RADAR_INTERVAL_SEC

        if ts - buffer.last_publish_ts < publish_interval:
            return False

        return True

    async def publish_features(self):
        print("🚀 [PUBLISH] Khởi chạy feature publisher V25.2...")

        while True:
            ts = now_ts()

            btc_buffer = self.buffers.get("btcusdt")
            self.btc_pct = btc_buffer.get_local_pct() if btc_buffer else 0.0

            for symbol, buffer in list(self.buffers.items()):
                try:
                    if not self._should_publish_symbol(symbol, buffer, ts):
                        continue

                    fallback_funding = self.global_funding_rates.get(symbol, 0.0001)
                    features = buffer.extract_features(self.btc_pct, fallback_funding)

                    packed_data = msgpack.packb(features, use_bin_type=True)
                    await self.redis.publish(f"{FEATURE_CHANNEL_PREFIX}:{symbol.upper()}", packed_data)

                    buffer.last_publish_ts = ts
                    self.stats["published"] += 1

                except Exception as e:
                    self.stats["publish_errors"] += 1
                    print(f"⚠️ [PUBLISH] Lỗi publish {symbol.upper()}: {e}")

            await asyncio.sleep(PUBLISH_LOOP_SLEEP_SEC)

    # --------------------------------------------------------
    # State log
    # --------------------------------------------------------
    def _calc_rate(self, key: str, elapsed: float) -> float:
        if elapsed <= 0:
            return 0.0

        current = self.stats.get(key, 0)
        previous = self.last_packet_counts.get(key, 0)

        return max(0.0, (current - previous) / elapsed)

    def _log_state_if_needed(self):
        ts = now_ts()

        if ts - self.last_state_log_ts < STREAM_STATE_LOG_INTERVAL_SEC:
            return

        elapsed = max(1e-6, ts - self.last_packet_stats_ts)

        packet_rate_per_sec = self._calc_rate("packets", elapsed)
        publish_rate_per_sec = self._calc_rate("published", elapsed)
        depth_rate = self._calc_rate("depth_packets", elapsed)
        trade_rate = self._calc_rate("trade_packets", elapsed)
        book_rate = self._calc_rate("book_packets", elapsed)
        kline_rate = self._calc_rate("kline_packets", elapsed)

        self.last_packet_counts = dict(self.stats)
        self.last_packet_stats_ts = ts
        self.last_state_log_ts = ts

        active_symbols = self._active_symbol_count()
        buffers = len(self.buffers)
        public_desired = len(self.desired_public_streams)
        market_desired = len(self.desired_market_streams)

        print(
            f"📊 [FEED V25.2] activeSymbols={active_symbols} buffers={buffers} "
            f"publicStreams={public_desired}/{len(self.ws_subscribed['public'])} "
            f"marketStreams={market_desired}/{len(self.ws_subscribed['market'])} "
            f"pps={packet_rate_per_sec:.1f} "
            f"pubps={publish_rate_per_sec:.1f} "
            f"depth/s={depth_rate:.1f} "
            f"trade/s={trade_rate:.1f} "
            f"book/s={book_rate:.1f} "
            f"kline/s={kline_rate:.1f} "
            f"published={self.stats['published']} "
            f"errors={self.stats['publish_errors']} "
            f"cleanupBuffers={self.stats['cleanup_buffers']}"
        )

    # --------------------------------------------------------
    # Startup
    # --------------------------------------------------------
    async def run(self):
        print("🧠 [PYTHON ENGINE] FeedHandler V25.2 Production-Stable khởi động...")
        print(f"⚙️ [CONFIG] redis={REDIS_URL}")
        print(f"⚙️ [CONFIG] public_ws={BINANCE_PUBLIC_WS_URL}")
        print(f"⚙️ [CONFIG] market_ws={BINANCE_MARKET_WS_URL}")
        print(f"⚙️ [CONFIG] channels control={SUBSCRIPTION_CHANNEL} keep_alive={KEEP_ALIVE_CHANNEL}")
        print(f"⚙️ [CONFIG] channels radar={RADAR_CANDIDATE_CHANNEL} features={FEATURE_CHANNEL_PREFIX}:<SYMBOL>")
        print(f"⚙️ [CONFIG] publish radar={PUBLISH_RADAR_INTERVAL_SEC}s trade={PUBLISH_FAST_INTERVAL_SEC}s")
        print("⚙️ [CONFIG] bookTicker=ON kline_1m=ON microprice=ON quality_flags=ON liq_quote=ON max_trade_quote=ON radar_auto_ref=ON")

        self.stream_refs["btcusdt"] = {
            "radar": False,
            "trade": False,
        }
        self._ensure_buffer("btcusdt")

        await asyncio.gather(
            self.connect_route_loop("public"),
            self.connect_route_loop("market"),
            self.control_reconcile_loop(),
            self.publish_features(),
            self.listen_to_control_channels(),
            self.listen_to_radar_candidates(),
            self.cleanup_buffer_loop(),
        )


# ============================================================
# 5. MAIN
# ============================================================

if __name__ == "__main__":
    handler = FeedHandler()

    try:
        asyncio.run(handler.run())
    except KeyboardInterrupt:
        print("\n🛑 [FEED] Dừng bởi KeyboardInterrupt")