import asyncio
import json
import os
import time
import random
from collections import deque

import msgpack
import redis.asyncio as aioredis
import websockets
from dotenv import load_dotenv

load_dotenv()


# ============================================================
# FEED HANDLER V20 - BINANCE FUTURES LIVE PARITY DATA FEED
# ------------------------------------------------------------
# Cập nhật theo Binance USDⓈ-M Futures WebSocket tháng 4/2026.
#
# Vai trò:
# 1. Nhận lệnh SUBSCRIBE / UNSUBSCRIBE / ENTER_TRADE / EXIT_TRADE từ Redis.
# 2. Kết nối đúng routed endpoint Binance Futures:
#    - /public: depth/bookTicker/high-frequency public data.
#    - /market: aggTrade, ticker, markPrice, liquidation.
# 3. Publish realtime feature cho OrderManager/ONNX.
# 4. Giữ nguyên 13 field ONNX cũ:
#    - ob_imb_top20
#    - spread_close
#    - bid_vol_1pct
#    - ask_vol_1pct
#    - max_buy_trade
#    - max_sell_trade
#    - liq_long_vol
#    - liq_short_vol
#    - funding_rate
#    - taker_buy_ratio
#    - body_size
#    - wick_size
#    - btc_relative_strength
# 5. Bổ sung field mới để train model/scalping tốt hơn:
#    - local_quote_volume
#    - local_buy_quote
#    - local_sell_quote
#    - local_trade_count
#    - local_taker_buy_ratio
#    - trade_flow_imbalance
#    - mark_price
#    - mark_price_ma
#    - index_price
#    - next_funding_time
#    - mark_price_age_ms
#    - feature_ts
#    - schema_version
#
# Nguyên tắc:
# - Không phá schema cũ.
# - Không đổi tên 13 field ONNX cũ.
# - Dữ liệu train phải có timestamp, version, age/stale fields.
# - Taker buy ratio dùng local trade flow để phù hợp scalping.
# - Liquidation lưu theo notional USDT, không lưu base qty thô.
# ============================================================


# ============================================================
# 1. CONFIG
# ============================================================
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Binance Futures routed endpoints sau migration 2026.
# Không dùng legacy unrouted /ws hoặc /stream cũ.
BINANCE_PUBLIC_WS_BASE = os.getenv("BINANCE_PUBLIC_WS_BASE", "wss://fstream.binance.com/public")
BINANCE_MARKET_WS_BASE = os.getenv("BINANCE_MARKET_WS_BASE", "wss://fstream.binance.com/market")

# Dùng /stream không có initial streams, sau đó JSON SUBSCRIBE.
# Binance tài liệu 2026 vẫn hỗ trợ JSON SUBSCRIBE.
BINANCE_PUBLIC_WS_URL = os.getenv("BINANCE_PUBLIC_WS_URL", f"{BINANCE_PUBLIC_WS_BASE}/stream")
BINANCE_MARKET_WS_URL = os.getenv("BINANCE_MARKET_WS_URL", f"{BINANCE_MARKET_WS_BASE}/stream")

SUBSCRIPTION_CHANNEL = os.getenv("SUBSCRIPTION_CHANNEL", "system:subscriptions")
KEEP_ALIVE_CHANNEL = os.getenv("KEEP_ALIVE_CHANNEL", "system:keep_alive")
FEATURE_CHANNEL_PREFIX = os.getenv("FEATURE_CHANNEL_PREFIX", "market:features")

SCHEMA_VERSION = os.getenv("FEED_SCHEMA_VERSION", "feed_v20_2026_04_live_parity")

MAX_STREAMS_PER_CONNECTION = int(os.getenv("MAX_STREAMS_PER_CONNECTION", "900"))
MAX_SYMBOLS_PER_HANDLER = int(os.getenv("MAX_SYMBOLS_PER_HANDLER", "250"))

# Binance Futures 2026: 10 incoming messages/s/connection.
# Giữ dưới ngưỡng để tránh disconnect/IP ban.
CONTROL_FLUSH_INTERVAL_SEC = float(os.getenv("CONTROL_FLUSH_INTERVAL_SEC", "0.5"))
CONTROL_MIN_INTERVAL_SEC = float(os.getenv("CONTROL_MIN_INTERVAL_SEC", "0.35"))
CONTROL_CHUNK_SIZE = int(os.getenv("CONTROL_CHUNK_SIZE", "150"))

PUBLISH_FAST_INTERVAL_SEC = float(os.getenv("PUBLISH_FAST_INTERVAL_SEC", "0.10"))
PUBLISH_RADAR_INTERVAL_SEC = float(os.getenv("PUBLISH_RADAR_INTERVAL_SEC", "0.25"))
PUBLISH_LOOP_SLEEP_SEC = float(os.getenv("PUBLISH_LOOP_SLEEP_SEC", "0.05"))

MAX_PRICE_AGE_SEC = float(os.getenv("MAX_PRICE_AGE_SEC", "8"))
MAX_DEPTH_AGE_SEC = float(os.getenv("MAX_DEPTH_AGE_SEC", "2"))
MAX_MARK_AGE_SEC = float(os.getenv("MAX_MARK_AGE_SEC", "10"))
MIN_WARMUP_SEC = float(os.getenv("MIN_WARMUP_SEC", "2"))

LOCAL_PRICE_WINDOW_SEC = float(os.getenv("LOCAL_PRICE_WINDOW_SEC", "10"))
LOCAL_TRADE_WINDOW_SEC = float(os.getenv("LOCAL_TRADE_WINDOW_SEC", "10"))
USE_LOCAL_PRICE_FEATURES = os.getenv("USE_LOCAL_PRICE_FEATURES", "true").lower() == "true"
USE_LOCAL_TAKER_RATIO_FOR_ONNX = os.getenv("USE_LOCAL_TAKER_RATIO_FOR_ONNX", "true").lower() == "true"

WHALE_TRADE_USDT = float(os.getenv("WHALE_TRADE_USDT", "5000"))

RECONNECT_BASE_DELAY_SEC = float(os.getenv("RECONNECT_BASE_DELAY_SEC", "1"))
RECONNECT_MAX_DELAY_SEC = float(os.getenv("RECONNECT_MAX_DELAY_SEC", "60"))
WS_ROTATE_SEC = float(os.getenv("WS_ROTATE_SEC", str(23.5 * 3600)))
WS_MAX_QUEUE = int(os.getenv("WS_MAX_QUEUE", "4096"))

# Global market streams thuộc /market.
# !markPrice@arr@1s cho dữ liệu mark/index/funding nhanh hơn, phù hợp paper/live parity.
GLOBAL_MARKET_STREAMS = {
    "!ticker@arr",
    "!markPrice@arr@1s",
    "!forceOrder@arr"
}


# ============================================================
# 2. UTILS
# ============================================================
def now_ts() -> float:
    return time.time()


def now_ms() -> int:
    return int(time.time() * 1000)


def safe_float(value, fallback: float = 0.0) -> float:
    try:
        if value is None:
            return fallback
        return float(value)
    except (TypeError, ValueError):
        return fallback


def safe_int(value, fallback: int = 0) -> int:
    try:
        if value is None:
            return fallback
        return int(value)
    except (TypeError, ValueError):
        return fallback


def jitter_delay(base: float, ratio: float = 0.25) -> float:
    spread = base * ratio
    return max(0.1, base - spread + random.random() * spread * 2)


def chunked(items, size: int):
    items = list(items)
    for i in range(0, len(items), size):
        yield items[i:i + size]


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))


# ============================================================
# 3. FEATURE BUFFER
# ============================================================
class FeatureBuffer:
    """Bộ đệm feature realtime theo symbol.

    Ghi chú quan trọng:
    - 13 field ONNX cũ được giữ nguyên tên.
    - Các field mới chỉ bổ sung để tăng chất lượng training/scalping.
    - Liquidation được lưu theo USDT notional.
    - Local taker buy ratio lấy từ aggTrade window ngắn hạn, phù hợp bot dưới 15 phút.
    """

    def __init__(self, symbol: str):
        self.symbol = symbol.lower()
        self.created_at = now_ts()
        self.is_warm = False

        # Timestamp cập nhật từng nhóm dữ liệu.
        self.last_price_ts = 0.0
        self.last_ticker_ts = 0.0
        self.last_depth_ts = 0.0
        self.last_trade_ts = 0.0
        self.last_force_order_ts = 0.0
        self.last_mark_ts = 0.0

        # Giá cơ bản.
        self.best_bid = 0.0
        self.best_ask = 0.0
        self.last_price = 0.0

        # Mark/index/funding fields.
        self.mark_price = 0.0
        self.mark_price_ma = 0.0
        self.index_price = 0.0
        self.next_funding_time = 0
        self.funding_rate = 0.0001

        # 24h ticker fields để tương thích cũ và fallback.
        self.open_price = 0.0
        self.high_price = 0.0
        self.low_price = 0.0
        self.quote_volume = 0.0
        self.taker_buy_quote = 0.0
        self.ticker_trade_count = 0

        # Rolling price window cho body/wick/ATR ngắn hạn.
        # item: (ts, price)
        self.price_window = deque(maxlen=6000)

        # Rolling trade window cho flow/taker ratio ngắn hạn.
        # item: (ts, signed_quote_value, quote_value)
        # signed_quote_value > 0 = taker buy, < 0 = taker sell.
        self.trade_window = deque(maxlen=12000)

        # Order book.
        self.ob_imb_top20 = 0.0
        self.spread_close = 0.0
        self.spread_bps = 0.0
        self.bid_vol_1pct = 0.0
        self.ask_vol_1pct = 0.0

        # Trade & liquidation.
        # max_buy_trade/max_sell_trade giữ base qty để tương thích công thức ONNX cũ.
        self.max_buy_trade = 0.0
        self.max_sell_trade = 0.0

        # Thêm notional để train chính xác hơn.
        self.max_buy_trade_usdt = 0.0
        self.max_sell_trade_usdt = 0.0
        self.whale_netflow = 0.0

        # Liquidation lưu USDT notional.
        self.liq_long_vol = 0.0
        self.liq_short_vol = 0.0

        # Internal indicators.
        self.atr14 = 0.002
        self.vpin = 0.5
        self.ofi = 0.0
        self.ofi_smoothed = 0.0
        self._last_ofi_smoothed = 0.0
        self.mfa = 0.0

        self.last_publish_ts = 0.0

    # --------------------------------------------------------
    # Internal helpers: price window
    # --------------------------------------------------------
    def _append_price(self, price: float):
        if price <= 0:
            return

        ts = now_ts()
        self.price_window.append((ts, price))
        self.last_price = price
        self.last_price_ts = ts
        self._prune_price_window(ts)

    def _prune_price_window(self, ts: float = None):
        if ts is None:
            ts = now_ts()

        cutoff = ts - max(LOCAL_PRICE_WINDOW_SEC, 1.0)

        while self.price_window and self.price_window[0][0] < cutoff:
            self.price_window.popleft()

    def _local_price_stats(self):
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

        body_size = abs(close_p - open_p) / open_p * 100.0
        wick_size = max(0.0, (high_p - low_p) - abs(close_p - open_p)) / open_p * 100.0
        atr = (high_p - low_p) / close_p if close_p > 0 else self.atr14
        pct = (close_p - open_p) / open_p * 100.0

        return {
            "pct": pct,
            "body_size": body_size,
            "wick_size": wick_size,
            "atr": atr,
            "window_open": open_p,
            "window_high": high_p,
            "window_low": low_p,
            "window_close": close_p,
            "window_count": len(prices)
        }

    def get_local_pct(self) -> float:
        stats = self._local_price_stats()
        if stats:
            return float(stats["pct"])

        if self.open_price > 0 and self.last_price > 0:
            return (self.last_price - self.open_price) / self.open_price * 100.0

        return 0.0

    # --------------------------------------------------------
    # Internal helpers: trade window
    # --------------------------------------------------------
    def _append_trade(self, quote_value: float, is_taker_buy: bool):
        if quote_value <= 0:
            return

        ts = now_ts()
        signed_value = quote_value if is_taker_buy else -quote_value
        self.trade_window.append((ts, signed_value, quote_value))
        self._prune_trade_window(ts)

    def _prune_trade_window(self, ts: float = None):
        if ts is None:
            ts = now_ts()

        cutoff = ts - max(LOCAL_TRADE_WINDOW_SEC, 1.0)

        while self.trade_window and self.trade_window[0][0] < cutoff:
            self.trade_window.popleft()

    def _local_trade_stats(self):
        self._prune_trade_window()

        if not self.trade_window:
            return {
                "local_quote_volume": 0.0,
                "local_buy_quote": 0.0,
                "local_sell_quote": 0.0,
                "local_trade_count": 0,
                "local_taker_buy_ratio": 0.5,
                "trade_flow_imbalance": 0.0
            }

        local_quote_volume = 0.0
        local_buy_quote = 0.0
        local_sell_quote = 0.0

        for _, signed_value, quote_value in self.trade_window:
            local_quote_volume += quote_value

            if signed_value > 0:
                local_buy_quote += quote_value
            elif signed_value < 0:
                local_sell_quote += quote_value

        if local_quote_volume <= 0:
            local_taker_buy_ratio = 0.5
            trade_flow_imbalance = 0.0
        else:
            local_taker_buy_ratio = local_buy_quote / local_quote_volume
            trade_flow_imbalance = (local_buy_quote - local_sell_quote) / local_quote_volume

        return {
            "local_quote_volume": float(local_quote_volume),
            "local_buy_quote": float(local_buy_quote),
            "local_sell_quote": float(local_sell_quote),
            "local_trade_count": int(len(self.trade_window)),
            "local_taker_buy_ratio": float(clamp(local_taker_buy_ratio, 0.0, 1.0)),
            "trade_flow_imbalance": float(clamp(trade_flow_imbalance, -1.0, 1.0))
        }

    # --------------------------------------------------------
    # Updates từ WebSocket
    # --------------------------------------------------------
    def update_ticker(self, data: dict):
        ts = now_ts()
        self.last_ticker_ts = ts

        last_price = safe_float(data.get("c"), self.last_price)

        self.open_price = safe_float(data.get("o"), self.open_price)
        self.high_price = safe_float(data.get("h"), self.high_price)
        self.low_price = safe_float(data.get("l"), self.low_price)
        self.quote_volume = safe_float(data.get("q"), self.quote_volume)

        # Futures allTicker payload không luôn có taker buy quote.
        # Giữ fallback, không phụ thuộc field này cho scalping.
        self.taker_buy_quote = safe_float(data.get("Q"), self.taker_buy_quote)
        self.ticker_trade_count = safe_int(data.get("n"), self.ticker_trade_count)

        if last_price > 0:
            self._append_price(last_price)

        if self.last_price > 0 and self.high_price > 0 and self.low_price > 0:
            self.atr14 = max((self.high_price - self.low_price) / self.last_price, 1e-8)

        if self.quote_volume > 0 and self.taker_buy_quote > 0:
            taker_sell_quote = max(0.0, self.quote_volume - self.taker_buy_quote)
            self.vpin = abs(self.taker_buy_quote - taker_sell_quote) / self.quote_volume

    def update_mark_price(self, data: dict):
        """Update từ !markPrice@arr hoặc !markPrice@arr@1s.

        Payload 2026 có thể gồm:
        p  = mark price
        ap = mark price moving average
        i  = index price
        r  = funding rate
        T  = next funding time
        """
        ts = now_ts()
        self.last_mark_ts = ts

        mark_price = safe_float(data.get("p"), self.mark_price)
        mark_price_ma = safe_float(data.get("ap"), self.mark_price_ma)
        index_price = safe_float(data.get("i"), self.index_price)
        funding_rate = safe_float(data.get("r"), self.funding_rate)
        next_funding_time = safe_int(data.get("T"), self.next_funding_time)

        if mark_price > 0:
            self.mark_price = mark_price

        if mark_price_ma > 0:
            self.mark_price_ma = mark_price_ma

        if index_price > 0:
            self.index_price = index_price

        self.funding_rate = funding_rate
        self.next_funding_time = next_funding_time

    def update_depth(self, data: dict):
        bids = data.get("b", [])
        asks = data.get("a", [])

        if not bids or not asks:
            return

        self.last_depth_ts = now_ts()

        self.best_bid = safe_float(bids[0][0])
        self.best_ask = safe_float(asks[0][0])

        if self.best_bid <= 0 or self.best_ask <= 0 or self.best_ask <= self.best_bid:
            return

        bid_vol = sum(safe_float(v) for _, v in bids)
        ask_vol = sum(safe_float(v) for _, v in asks)
        total_vol = bid_vol + ask_vol

        self.ob_imb_top20 = (bid_vol - ask_vol) / total_vol if total_vol > 0 else 0.0

        mid_price = (self.best_bid + self.best_ask) / 2.0
        ref_price = self.last_price if self.last_price > 0 else mid_price

        if ref_price > 0:
            self.spread_close = (self.best_ask - self.best_bid) / ref_price
            self.spread_bps = self.spread_close * 10000.0

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

        self.ofi = self.ob_imb_top20 * 100.0
        self.ofi_smoothed = (self.ofi_smoothed * 0.7) + (self.ofi * 0.3)

    def update_trade(self, data: dict):
        self.last_trade_ts = now_ts()

        qty = safe_float(data.get("q"), 0.0)
        price = safe_float(data.get("p"), self.last_price)

        # m = true nghĩa là buyer là maker => taker là seller.
        is_buyer_maker = data.get("m", True)
        is_taker_buy = not bool(is_buyer_maker)

        if price > 0:
            self._append_price(price)

        trade_value = qty * price if qty > 0 and price > 0 else 0.0
        self._append_trade(trade_value, is_taker_buy)

        if is_taker_buy:
            if qty > self.max_buy_trade:
                self.max_buy_trade = qty
            if trade_value > self.max_buy_trade_usdt:
                self.max_buy_trade_usdt = trade_value
            if trade_value >= WHALE_TRADE_USDT:
                self.whale_netflow += trade_value
        else:
            if qty > self.max_sell_trade:
                self.max_sell_trade = qty
            if trade_value > self.max_sell_trade_usdt:
                self.max_sell_trade_usdt = trade_value
            if trade_value >= WHALE_TRADE_USDT:
                self.whale_netflow -= trade_value

    def update_force_order(self, data: dict):
        """Liquidation update.

        side SELL thường là long bị thanh lý.
        side BUY thường là short bị thanh lý.

        Lưu notional USDT thay vì base qty để feature scale tốt hơn giữa các symbol.
        """
        self.last_force_order_ts = now_ts()

        order_info = data.get("o", {})
        side = order_info.get("S", "")

        qty = safe_float(order_info.get("q"), 0.0)
        price = safe_float(order_info.get("ap"), 0.0)

        if price <= 0:
            price = safe_float(order_info.get("p"), self.last_price)

        notional = qty * price if qty > 0 and price > 0 else 0.0

        if notional <= 0:
            return

        if side == "SELL":
            self.liq_long_vol += notional
        elif side == "BUY":
            self.liq_short_vol += notional

    # --------------------------------------------------------
    # Readiness / stale guard
    # --------------------------------------------------------
    def is_ready(self) -> bool:
        ts = now_ts()

        if ts - self.created_at < MIN_WARMUP_SEC:
            return False

        if self.last_price <= 0 or self.best_bid <= 0 or self.best_ask <= 0:
            return False

        if self.best_ask <= self.best_bid:
            return False

        if ts - self.last_price_ts > MAX_PRICE_AGE_SEC:
            return False

        if ts - self.last_depth_ts > MAX_DEPTH_AGE_SEC:
            return False

        self.is_warm = True
        return True

    # --------------------------------------------------------
    # Extract features
    # --------------------------------------------------------
    def extract_features(self, btc_pct: float, btc_price_age_ms, fallback_funding_rate: float) -> dict:
        current_ts = now_ts()
        feature_ts = int(current_ts * 1000)

        local_stats = self._local_price_stats() if USE_LOCAL_PRICE_FEATURES else None
        local_trade_stats = self._local_trade_stats()

        if local_stats:
            coin_pct = local_stats["pct"]
            body_size = local_stats["body_size"]
            wick_size = local_stats["wick_size"]
            self.atr14 = max(local_stats["atr"], 1e-8)
            window_open = local_stats["window_open"]
            window_high = local_stats["window_high"]
            window_low = local_stats["window_low"]
            window_close = local_stats["window_close"]
            price_window_count = local_stats["window_count"]
        else:
            coin_pct = (
                (self.last_price - self.open_price) / self.open_price * 100.0
                if self.open_price > 0 and self.last_price > 0
                else 0.0
            )
            body_size = (
                abs(self.last_price - self.open_price) / self.open_price * 100.0
                if self.open_price > 0 and self.last_price > 0
                else 0.0
            )
            wick_size = (
                ((self.high_price - self.low_price) - abs(self.last_price - self.open_price)) / self.open_price * 100.0
                if self.open_price > 0 and self.high_price > 0 and self.low_price > 0
                else 0.0
            )
            window_open = self.open_price
            window_high = self.high_price
            window_low = self.low_price
            window_close = self.last_price
            price_window_count = len(self.price_window)

        taker_buy_ratio_24h = (
            self.taker_buy_quote / self.quote_volume
            if self.quote_volume > 0 and self.taker_buy_quote > 0
            else 0.5
        )

        local_taker_buy_ratio = local_trade_stats["local_taker_buy_ratio"]

        taker_buy_ratio = (
            local_taker_buy_ratio
            if USE_LOCAL_TAKER_RATIO_FOR_ONNX
            else taker_buy_ratio_24h
        )

        btc_relative_strength = coin_pct - btc_pct

        self.mfa = self.ofi_smoothed - self._last_ofi_smoothed
        self._last_ofi_smoothed = self.ofi_smoothed

        quote_vol_safe = self.quote_volume if self.quote_volume > 0 else 1e-8
        local_quote_safe = local_trade_stats["local_quote_volume"] if local_trade_stats["local_quote_volume"] > 0 else 1e-8
        close_p = self.last_price if self.last_price > 0 else 1e-8

        mark_price_age_ms = int((current_ts - self.last_mark_ts) * 1000) if self.last_mark_ts > 0 else None
        depth_age_ms = int((current_ts - self.last_depth_ts) * 1000) if self.last_depth_ts > 0 else None
        price_age_ms = int((current_ts - self.last_price_ts) * 1000) if self.last_price_ts > 0 else None
        trade_age_ms = int((current_ts - self.last_trade_ts) * 1000) if self.last_trade_ts > 0 else None
        ticker_age_ms = int((current_ts - self.last_ticker_ts) * 1000) if self.last_ticker_ts > 0 else None
        force_order_age_ms = int((current_ts - self.last_force_order_ts) * 1000) if self.last_force_order_ts > 0 else None

        funding_rate = self.funding_rate if self.last_mark_ts > 0 else fallback_funding_rate

        mark_price_safe = self.mark_price if self.mark_price > 0 else self.last_price
        mark_price_ma_safe = self.mark_price_ma if self.mark_price_ma > 0 else mark_price_safe
        index_price_safe = self.index_price if self.index_price > 0 else mark_price_safe

        mark_stale = (
            mark_price_age_ms is None or
            mark_price_age_ms > int(MAX_MARK_AGE_SEC * 1000)
        )

        # =====================================================
        # IMPORTANT:
        # 13 field ONNX cũ giữ nguyên tên.
        # Một số field cũ vẫn normalize bằng quote_volume 24h để không phá distribution model cũ.
        # Field mới local_* dùng cho training model sau.
        # =====================================================
        features = {
            "schema_version": SCHEMA_VERSION,
            "source": "binance_um_futures_ws",
            "feature_ts": feature_ts,

            "symbol": self.symbol.upper(),
            "is_warm": self.is_warm,

            "best_ask": float(self.best_ask),
            "best_bid": float(self.best_bid),
            "last_price": float(self.last_price),

            # =================================================
            # 13 FIELD CŨ CHO ONNX - KHÔNG ĐỔI TÊN
            # =================================================
            "ob_imb_top20": float(self.ob_imb_top20),
            "spread_close": float(self.spread_close),
            "bid_vol_1pct": float((self.bid_vol_1pct * close_p) / quote_vol_safe),
            "ask_vol_1pct": float((self.ask_vol_1pct * close_p) / quote_vol_safe),
            "max_buy_trade": float((self.max_buy_trade * close_p) / quote_vol_safe),
            "max_sell_trade": float((self.max_sell_trade * close_p) / quote_vol_safe),

            # V20: liq_* đã là USDT notional, nên chia trực tiếp cho quote_volume 24h.
            "liq_long_vol": float(self.liq_long_vol / quote_vol_safe),
            "liq_short_vol": float(self.liq_short_vol / quote_vol_safe),

            "funding_rate": float(funding_rate),
            "taker_buy_ratio": float(clamp(taker_buy_ratio, 0.0, 1.0)),
            "body_size": float(body_size),
            "wick_size": float(max(wick_size, 0.0)),
            "btc_relative_strength": float(btc_relative_strength),

            # =================================================
            # FIELD BỔ SUNG CHO LIVE PARITY / PAPEREXCHANGE
            # =================================================
            "mark_price": float(mark_price_safe),
            "mark_price_ma": float(mark_price_ma_safe),
            "index_price": float(index_price_safe),
            "next_funding_time": int(self.next_funding_time or 0),
            "mark_price_age_ms": mark_price_age_ms,
            "mark_price_stale": bool(mark_stale),

            # =================================================
            # FIELD BỔ SUNG CHO TRAINING / SCALPING
            # =================================================
            "coin_pct_local": float(coin_pct),
            "btc_pct_local": float(btc_pct),
            "btc_price_age_ms": btc_price_age_ms,

            "window_open": float(window_open or 0.0),
            "window_high": float(window_high or 0.0),
            "window_low": float(window_low or 0.0),
            "window_close": float(window_close or 0.0),
            "price_window_count": int(price_window_count),

            "local_quote_volume": float(local_trade_stats["local_quote_volume"]),
            "local_buy_quote": float(local_trade_stats["local_buy_quote"]),
            "local_sell_quote": float(local_trade_stats["local_sell_quote"]),
            "local_trade_count": int(local_trade_stats["local_trade_count"]),
            "local_taker_buy_ratio": float(local_taker_buy_ratio),
            "taker_buy_ratio_24h": float(clamp(taker_buy_ratio_24h, 0.0, 1.0)),
            "trade_flow_imbalance": float(local_trade_stats["trade_flow_imbalance"]),

            "max_buy_trade_usdt": float(self.max_buy_trade_usdt),
            "max_sell_trade_usdt": float(self.max_sell_trade_usdt),
            "max_buy_trade_local_norm": float(self.max_buy_trade_usdt / local_quote_safe),
            "max_sell_trade_local_norm": float(self.max_sell_trade_usdt / local_quote_safe),

            "liq_long_usdt": float(self.liq_long_vol),
            "liq_short_usdt": float(self.liq_short_vol),
            "liq_long_local_norm": float(self.liq_long_vol / local_quote_safe),
            "liq_short_local_norm": float(self.liq_short_vol / local_quote_safe),

            "spread_bps": float(self.spread_bps),
            "quote_volume_24h": float(self.quote_volume),
            "ticker_trade_count_24h": int(self.ticker_trade_count),

            "depth_age_ms": depth_age_ms,
            "price_age_ms": price_age_ms,
            "trade_age_ms": trade_age_ms,
            "ticker_age_ms": ticker_age_ms,
            "force_order_age_ms": force_order_age_ms,

            # Chỉ số nội bộ cho OrderManager.
            "ATR14": float(self.atr14),
            "VPIN": float(self.vpin),
            "OFI": float(self.ofi_smoothed),
            "MFA": float(self.mfa),
            "WHALE_NET": float(self.whale_netflow / quote_vol_safe),
            "WHALE_NET_LOCAL": float(self.whale_netflow / local_quote_safe)
        }

        # Reset spike sau mỗi lần publish.
        self.max_buy_trade = 0.0
        self.max_sell_trade = 0.0
        self.max_buy_trade_usdt = 0.0
        self.max_sell_trade_usdt = 0.0

        self.liq_long_vol = 0.0
        self.liq_short_vol = 0.0

        # Whale netflow decay để giữ memory ngắn, không reset cứng.
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
            health_check_interval=30
        )

        self.buffers = {}
        self.stream_refs = {}

        # Global mark/funding cache lấy từ !markPrice@arr@1s.
        self.global_funding_rates = {}
        self.global_mark_prices = {}
        self.global_mark_price_ma = {}
        self.global_index_prices = {}
        self.global_next_funding_times = {}

        self.desired_public_streams = set()
        self.desired_market_streams = set(GLOBAL_MARKET_STREAMS)

        self.ws_subscribed = {
            "public": set(),
            "market": set()
        }

        self.ws_connections = {
            "public": None,
            "market": None
        }

        self.control_locks = {
            "public": asyncio.Lock(),
            "market": asyncio.Lock()
        }

        self.last_control_sent_ts = {
            "public": 0.0,
            "market": 0.0
        }

        self.control_id = 1

        self.reconnect_attempts = {
            "public": 0,
            "market": 0
        }

        self.btc_pct = 0.0
        self.btc_price_age_ms = None

    # --------------------------------------------------------
    # Stream names
    # --------------------------------------------------------
    def _get_public_streams(self, symbol: str):
        sym = symbol.lower()

        # depth20@100ms thuộc /public.
        # Giữ giống file cũ để có top20 order book features.
        return {
            f"{sym}@depth20@100ms"
        }

    def _get_market_streams(self, symbol: str):
        sym = symbol.lower()

        # aggTrade thuộc /market theo mapping Binance 2026.
        return {
            f"{sym}@aggTrade"
        }

    def _get_desired_streams(self, route: str):
        if route == "public":
            return self.desired_public_streams
        return self.desired_market_streams

    def _get_ws_url(self, route: str):
        return BINANCE_PUBLIC_WS_URL if route == "public" else BINANCE_MARKET_WS_URL

    # --------------------------------------------------------
    # Symbol lifecycle
    # --------------------------------------------------------
    def _ensure_symbol_ref(self, symbol: str):
        if symbol not in self.stream_refs:
            self.stream_refs[symbol] = {
                "radar": False,
                "trade": False
            }

    def _ensure_buffer(self, symbol: str):
        symbol = symbol.lower()

        if symbol not in self.buffers:
            self.buffers[symbol] = FeatureBuffer(symbol)

            # Nếu đã có global mark/funding trước đó,
            # nạp ngay vào buffer mới để giảm warm-up live parity.
            if symbol in self.global_mark_prices:
                buffer = self.buffers[symbol]
                buffer.mark_price = self.global_mark_prices.get(symbol, 0.0)
                buffer.mark_price_ma = self.global_mark_price_ma.get(symbol, 0.0)
                buffer.index_price = self.global_index_prices.get(symbol, 0.0)
                buffer.funding_rate = self.global_funding_rates.get(symbol, 0.0001)
                buffer.next_funding_time = self.global_next_funding_times.get(symbol, 0)
                buffer.last_mark_ts = now_ts()

    def _symbol_should_have_buffer(self, symbol: str) -> bool:
        if symbol == "btcusdt":
            return True

        refs = self.stream_refs.get(symbol, {})
        return bool(refs.get("radar", False) or refs.get("trade", False))

    def _symbol_should_have_symbol_streams(self, symbol: str) -> bool:
        refs = self.stream_refs.get(symbol, {})
        return bool(refs.get("radar", False) or refs.get("trade", False))

    def _apply_symbol_stream_state(self, symbol: str):
        symbol = symbol.lower()

        should_buffer = self._symbol_should_have_buffer(symbol)
        should_stream = self._symbol_should_have_symbol_streams(symbol)

        public_streams = self._get_public_streams(symbol)
        market_streams = self._get_market_streams(symbol)

        if should_buffer:
            self._ensure_buffer(symbol)
        else:
            if symbol in self.buffers:
                del self.buffers[symbol]

        if should_stream:
            self.desired_public_streams.update(public_streams)
            self.desired_market_streams.update(market_streams)
        else:
            self.desired_public_streams.difference_update(public_streams)
            self.desired_market_streams.difference_update(market_streams)

        # Global market streams luôn giữ.
        self.desired_market_streams.update(GLOBAL_MARKET_STREAMS)

    # --------------------------------------------------------
    # WebSocket control reconcile
    # --------------------------------------------------------
    async def _send_control(self, route: str, method: str, streams):
        ws = self.ws_connections.get(route)

        if ws is None or not streams:
            return False

        streams = list(streams)

        for part in chunked(streams, CONTROL_CHUNK_SIZE):
            elapsed = now_ts() - self.last_control_sent_ts[route]

            if elapsed < CONTROL_MIN_INTERVAL_SEC:
                await asyncio.sleep(CONTROL_MIN_INTERVAL_SEC - elapsed)

            payload = {
                "method": method,
                "params": part,
                "id": self.control_id
            }

            self.control_id += 1

            await ws.send(json.dumps(payload))
            self.last_control_sent_ts[route] = now_ts()

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
                    f"🚨 [WS {route.upper()}] Vượt giới hạn streams an toàn: "
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
        print("🧭 [WS CONTROL] Khởi chạy bộ gom lệnh SUBSCRIBE/UNSUBSCRIBE...")

        while True:
            try:
                await self.reconcile_route_streams("public")
                await self.reconcile_route_streams("market")
            except Exception as e:
                print(f"⚠️ [WS CONTROL] Lỗi reconcile: {e}")

            await asyncio.sleep(CONTROL_FLUSH_INTERVAL_SEC)

    # --------------------------------------------------------
    # WebSocket connection loops
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
                    close_timeout=5
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
                                f"♻️ [WS {route.upper()}] Chủ động rotate connection sau "
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

                delay = min(
                    RECONNECT_MAX_DELAY_SEC,
                    RECONNECT_BASE_DELAY_SEC * (2 ** attempt)
                )

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

        # Response control dạng {"result": null, "id": 1}
        if "result" in packet and "id" in packet and "stream" not in packet:
            return

        stream_name = packet.get("stream", "")
        data = packet.get("data", None)

        # Một số mode có thể trả raw payload nếu không wrapped.
        # File này chủ động dùng /stream nên normally có stream/data.
        if not stream_name:
            return

        # -----------------------------
        # Global ticker stream /market
        # -----------------------------
        if stream_name == "!ticker@arr":
            if isinstance(data, list):
                for item in data:
                    symbol = str(item.get("s", "")).lower()
                    buffer = self.buffers.get(symbol)

                    if buffer:
                        buffer.update_ticker(item)
            return

        # -----------------------------
        # Global mark price / funding /market
        # -----------------------------
        if stream_name in ("!markPrice@arr", "!markPrice@arr@1s"):
            if isinstance(data, list):
                ts = now_ts()

                for item in data:
                    symbol = str(item.get("s", "")).lower()

                    if not symbol:
                        continue

                    mark_price = safe_float(item.get("p"), 0.0)
                    mark_price_ma = safe_float(item.get("ap"), 0.0)
                    index_price = safe_float(item.get("i"), 0.0)
                    funding_rate = safe_float(item.get("r"), 0.0001)
                    next_funding_time = safe_int(item.get("T"), 0)

                    self.global_mark_prices[symbol] = mark_price
                    self.global_mark_price_ma[symbol] = mark_price_ma
                    self.global_index_prices[symbol] = index_price
                    self.global_funding_rates[symbol] = funding_rate
                    self.global_next_funding_times[symbol] = next_funding_time

                    buffer = self.buffers.get(symbol)

                    if buffer:
                        buffer.update_mark_price(item)

                # BTC buffer có thể nhận mark cache, nhưng price local vẫn chủ yếu từ ticker/trade.
            return

        # -----------------------------
        # Global liquidation stream /market
        # -----------------------------
        if stream_name == "!forceOrder@arr":
            items = data if isinstance(data, list) else [data]

            for item in items:
                if not isinstance(item, dict):
                    continue

                order_info = item.get("o", {})
                symbol = str(order_info.get("s", "")).lower()

                buffer = self.buffers.get(symbol)

                if buffer:
                    buffer.update_force_order(item)
            return

        # -----------------------------
        # Per-symbol streams
        # -----------------------------
        symbol = stream_name.split("@")[0].lower()
        buffer = self.buffers.get(symbol)

        if not buffer:
            return

        if "@depth20" in stream_name:
            buffer.update_depth(data)
        elif "@aggTrade" in stream_name:
            buffer.update_trade(data)

    # --------------------------------------------------------
    # Redis control listener
    # --------------------------------------------------------
    async def listen_to_control_channels(self):
        print("📡 [REDIS] Khởi chạy listener system:subscriptions...")

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

                    symbol = str(data.get("symbol", "")).lower()

                    if not symbol:
                        continue

                    action = data.get("action", "")
                    client = data.get("client", "unknown")

                    self._ensure_symbol_ref(symbol)

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
                        continue

                    before_running = symbol in self.buffers

                    self._apply_symbol_stream_state(symbol)

                    after_running = symbol in self.buffers

                    if after_running and not before_running:
                        print(
                            f"🎯 [KẾT NỐI] Bắt đầu theo dõi: "
                            f"{symbol.upper()} | client={client} action={action}"
                        )

                    elif before_running and not after_running:
                        print(
                            f"🗑️ [RÚT ỐNG] Ngừng theo dõi: "
                            f"{symbol.upper()} | client={client} action={action}"
                        )

                    # Dọn ref nếu symbol không còn buffer, trừ BTC internal.
                    if symbol != "btcusdt" and symbol not in self.buffers:
                        self.stream_refs.pop(symbol, None)

            except Exception as e:
                print(f"⚠️ [REDIS] Listener lỗi/mất kết nối: {e}. Thử lại sau 3s...")
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
        # BTC chỉ dùng nội bộ để tính relative strength, không publish nếu không trade.
        if symbol == "btcusdt" and not self.stream_refs.get("btcusdt", {}).get("trade", False):
            return False

        if not buffer.is_ready():
            return False

        refs = self.stream_refs.get(symbol, {})

        publish_interval = (
            PUBLISH_FAST_INTERVAL_SEC
            if refs.get("trade", False)
            else PUBLISH_RADAR_INTERVAL_SEC
        )

        if ts - buffer.last_publish_ts < publish_interval:
            return False

        return True

    async def publish_features(self):
        print("🚀 [PUBLISH] Khởi chạy feature publisher V20...")

        while True:
            ts = now_ts()

            btc_buffer = self.buffers.get("btcusdt")

            if btc_buffer:
                self.btc_pct = btc_buffer.get_local_pct()
                self.btc_price_age_ms = (
                    int((ts - btc_buffer.last_price_ts) * 1000)
                    if btc_buffer.last_price_ts > 0
                    else None
                )
            else:
                self.btc_pct = 0.0
                self.btc_price_age_ms = None

            for symbol, buffer in list(self.buffers.items()):
                try:
                    if not self._should_publish_symbol(symbol, buffer, ts):
                        continue

                    fallback_funding = self.global_funding_rates.get(symbol, 0.0001)

                    features = buffer.extract_features(
                        btc_pct=self.btc_pct,
                        btc_price_age_ms=self.btc_price_age_ms,
                        fallback_funding_rate=fallback_funding
                    )

                    packed_data = msgpack.packb(features, use_bin_type=True)

                    await self.redis.publish(
                        f"{FEATURE_CHANNEL_PREFIX}:{symbol.upper()}",
                        packed_data
                    )

                    buffer.last_publish_ts = ts

                except Exception as e:
                    print(f"⚠️ [PUBLISH] Lỗi publish {symbol.upper()}: {e}")

            await asyncio.sleep(PUBLISH_LOOP_SLEEP_SEC)

    # --------------------------------------------------------
    # Startup
    # --------------------------------------------------------
    async def run(self):
        print("🧠 [PYTHON ENGINE] Data Feed V20 Live Parity khởi động...")
        print(f"⚙️ [CONFIG] schema={SCHEMA_VERSION}")
        print(f"⚙️ [CONFIG] public={BINANCE_PUBLIC_WS_URL}")
        print(f"⚙️ [CONFIG] market={BINANCE_MARKET_WS_URL}")
        print(f"⚙️ [CONFIG] publish radar={PUBLISH_RADAR_INTERVAL_SEC}s trade={PUBLISH_FAST_INTERVAL_SEC}s")
        print("⚙️ [CONFIG] routed endpoints /public + /market = ON")
        print("⚙️ [CONFIG] mark_price/index_price/mark_price_ma/next_funding_time = ON")
        print("⚙️ [CONFIG] local trade flow + local taker ratio = ON")

        # BTC luôn có buffer để tính relative strength từ !ticker@arr.
        self.stream_refs["btcusdt"] = {
            "radar": False,
            "trade": False
        }

        self._ensure_buffer("btcusdt")

        await asyncio.gather(
            self.connect_route_loop("public"),
            self.connect_route_loop("market"),
            self.control_reconcile_loop(),
            self.publish_features(),
            self.listen_to_control_channels()
        )


if __name__ == "__main__":
    handler = FeedHandler()
    asyncio.run(handler.run())