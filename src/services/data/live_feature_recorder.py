import asyncio
import gzip
import json
import os
import signal
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional

import msgpack
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()


# ============================================================
# LIVE FEATURE RECORDER V1
# ------------------------------------------------------------
# Vai trò:
# 1. Nghe Redis pubsub market:features:* từ FeedHandler.
# 2. Decode msgpack feature realtime.
# 3. Chỉ lấy latest snapshot mỗi symbol theo nhịp 1 giây.
# 4. Ghi ra file .jsonl.gz để tải về train thủ công.
# 5. Không can thiệp OrderManager, không ảnh hưởng trading.
#
# Output:
# data/live_features/YYYY-MM-DD/HH/SYMBOL.jsonl.gz
#
# Mỗi dòng là 1 JSON record:
# {
#   "_capture_id": "...",
#   "_snapshot_ts": 1710000000000,
#   "_recorder_ts": 1710000000123,
#   "_channel": "market:features:BTCUSDT",
#   "symbol": "BTCUSDT",
#   "feature_ts": ...,
#   ...
# }
# ============================================================


# ============================================================
# 1. CONFIG
# ============================================================
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

FEATURE_PATTERN = os.getenv("FEATURE_PATTERN", "market:features:*")

DATA_DIR = Path(os.getenv("RECORDER_DATA_DIR", "./data/live_features"))

# Nhịp ghi snapshot. Với mục tiêu train 1s, để 1.0.
SNAPSHOT_INTERVAL_SEC = float(os.getenv("SNAPSHOT_INTERVAL_SEC", "1.0"))

# Nếu feature cũ quá số giây này thì không ghi.
MAX_FEATURE_STALE_SEC = float(os.getenv("MAX_FEATURE_STALE_SEC", "3.0"))

# Flush file mỗi N dòng để giảm mất dữ liệu nếu crash.
FLUSH_EVERY_N_RECORDS = int(os.getenv("FLUSH_EVERY_N_RECORDS", "100"))

# Log thống kê mỗi N giây.
STATS_INTERVAL_SEC = float(os.getenv("STATS_INTERVAL_SEC", "30"))

# Giới hạn số symbol đang giữ trong RAM để tránh lỗi bất thường.
MAX_SYMBOLS_IN_RAM = int(os.getenv("MAX_SYMBOLS_IN_RAM", "500"))

# Nếu disk free thấp hơn mức này thì cảnh báo.
MIN_FREE_DISK_GB = float(os.getenv("MIN_FREE_DISK_GB", "2.0"))

# Có ghi cả feature không warm không?
# Khuyên false để dataset sạch hơn.
RECORD_NOT_WARM = os.getenv("RECORD_NOT_WARM", "false").lower() == "true"

# Có ghi pretty JSON không?
# Luôn false khi chạy thật để tiết kiệm dung lượng.
PRETTY_JSON = os.getenv("PRETTY_JSON", "false").lower() == "true"


# ============================================================
# 2. UTILS
# ============================================================
def now_ms() -> int:
    return int(time.time() * 1000)


def utc_date_hour(ts_ms: Optional[int] = None):
    if ts_ms is None:
        ts_ms = now_ms()

    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    date_str = dt.strftime("%Y-%m-%d")
    hour_str = dt.strftime("%H")
    iso_str = dt.isoformat()
    return date_str, hour_str, iso_str


def normalize_symbol(symbol: Any) -> str:
    return str(symbol or "").upper().strip()


def safe_number(value, fallback=0):
    try:
        if value is None:
            return fallback
        n = float(value)
        if n != n:
            return fallback
        return n
    except Exception:
        return fallback


def get_feature_ts_ms(feature: Dict[str, Any]) -> int:
    """
    Feed V20 có feature_ts.
    Nếu thiếu thì fallback sang timestamp/ts.
    Nếu vẫn thiếu thì dùng recorder time.
    """
    candidates = [
        feature.get("feature_ts"),
        feature.get("ts"),
        feature.get("timestamp"),
        feature.get("event_time"),
        feature.get("last_update_ts"),
    ]

    for value in candidates:
        n = safe_number(value, 0)
        if n > 0:
            # Nếu timestamp dạng giây.
            if n < 1e12:
                return int(n * 1000)
            return int(n)

    return now_ms()


def is_feature_warm(feature: Dict[str, Any]) -> bool:
    return bool(feature.get("is_warm", False))


def disk_free_gb(path: Path) -> float:
    try:
        path.mkdir(parents=True, exist_ok=True)
        stat = os.statvfs(str(path))
        return stat.f_bavail * stat.f_frsize / (1024 ** 3)
    except Exception:
        return 999.0


def json_dumps(record: Dict[str, Any]) -> str:
    if PRETTY_JSON:
        return json.dumps(record, ensure_ascii=False, indent=2, default=str)
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str)


# ============================================================
# 3. ROTATING JSONL.GZ WRITER
# ============================================================
class JsonlGzipWriter:
    """
    Giữ file handle mở theo path hiện tại để giảm overhead.
    Mỗi symbol mỗi giờ là một file riêng:
    data/live_features/YYYY-MM-DD/HH/SYMBOL.jsonl.gz
    """

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.handles = {}
        self.write_counts = {}
        self.total_written = 0

    def _get_path(self, symbol: str, snapshot_ts: int) -> Path:
        date_str, hour_str, _ = utc_date_hour(snapshot_ts)
        folder = self.base_dir / date_str / hour_str
        folder.mkdir(parents=True, exist_ok=True)
        return folder / f"{symbol}.jsonl.gz"

    def write(self, symbol: str, snapshot_ts: int, record: Dict[str, Any]):
        path = self._get_path(symbol, snapshot_ts)
        key = str(path)

        handle = self.handles.get(key)
        if handle is None:
            handle = gzip.open(path, mode="at", encoding="utf-8")
            self.handles[key] = handle
            self.write_counts[key] = 0

        line = json_dumps(record)
        handle.write(line)
        handle.write("\n")

        self.write_counts[key] += 1
        self.total_written += 1

        if self.write_counts[key] % FLUSH_EVERY_N_RECORDS == 0:
            handle.flush()

    def flush_all(self):
        for handle in self.handles.values():
            try:
                handle.flush()
            except Exception:
                pass

    def close_all(self):
        for handle in self.handles.values():
            try:
                handle.flush()
                handle.close()
            except Exception:
                pass

        self.handles.clear()
        self.write_counts.clear()


# ============================================================
# 4. RECORDER
# ============================================================
class LiveFeatureRecorder:
    def __init__(self):
        self.capture_id = str(uuid.uuid4())

        self.redis = aioredis.from_url(
            REDIS_URL,
            decode_responses=False,
            socket_keepalive=True,
            health_check_interval=30,
            retry_on_timeout=True,
        )

        self.writer = JsonlGzipWriter(DATA_DIR)

        # latest_by_symbol chỉ giữ snapshot mới nhất trong RAM.
        self.latest_by_symbol: Dict[str, Dict[str, Any]] = {}

        # Chống ghi trùng cùng feature_ts/symbol.
        self.last_written_feature_ts: Dict[str, int] = {}

        self.running = True

        self.stats = {
            "received": 0,
            "decoded": 0,
            "invalid": 0,
            "written": 0,
            "skipped_stale": 0,
            "skipped_not_warm": 0,
            "skipped_duplicate": 0,
            "started_at": now_ms(),
            "last_log_at": now_ms(),
        }

    async def stop(self):
        self.running = False
        self.writer.flush_all()
        self.writer.close_all()

        try:
            await self.redis.close()
        except Exception:
            pass

    def _extract_symbol_from_channel(self, channel: bytes) -> str:
        try:
            text = channel.decode("utf-8") if isinstance(channel, bytes) else str(channel)
            # market:features:BTCUSDT
            return normalize_symbol(text.split(":")[-1])
        except Exception:
            return ""

    def _sanitize_feature(self, feature: Dict[str, Any]) -> Dict[str, Any]:
        """
        Giữ gần như toàn bộ field Feed V20 để train.
        Chỉ thêm metadata recorder.
        """
        if not isinstance(feature, dict):
            return {}

        # Convert bytes key/value nếu có.
        clean = {}

        for k, v in feature.items():
            key = k.decode("utf-8") if isinstance(k, bytes) else str(k)

            if isinstance(v, bytes):
                try:
                    clean[key] = v.decode("utf-8")
                except Exception:
                    clean[key] = str(v)
            else:
                clean[key] = v

        return clean

    async def handle_message(self, channel: bytes, message: bytes):
        self.stats["received"] += 1

        try:
            feature = msgpack.unpackb(message, raw=False)
        except Exception:
            self.stats["invalid"] += 1
            return

        feature = self._sanitize_feature(feature)
        if not feature:
            self.stats["invalid"] += 1
            return

        symbol = normalize_symbol(feature.get("symbol")) or self._extract_symbol_from_channel(channel)
        if not symbol:
            self.stats["invalid"] += 1
            return

        if len(self.latest_by_symbol) >= MAX_SYMBOLS_IN_RAM and symbol not in self.latest_by_symbol:
            self.stats["invalid"] += 1
            return

        recorder_ts = now_ms()
        feature_ts = get_feature_ts_ms(feature)

        feature["_capture_id"] = self.capture_id
        feature["_recorder_ts"] = recorder_ts
        feature["_channel"] = channel.decode("utf-8") if isinstance(channel, bytes) else str(channel)
        feature["_feature_age_ms_at_receive"] = max(0, recorder_ts - feature_ts)

        self.latest_by_symbol[symbol] = feature
        self.stats["decoded"] += 1

    def _build_snapshot_record(self, symbol: str, feature: Dict[str, Any], snapshot_ts: int) -> Optional[Dict[str, Any]]:
        feature_ts = get_feature_ts_ms(feature)
        age_sec = (snapshot_ts - feature_ts) / 1000

        if age_sec > MAX_FEATURE_STALE_SEC:
            self.stats["skipped_stale"] += 1
            return None

        if not RECORD_NOT_WARM and not is_feature_warm(feature):
            self.stats["skipped_not_warm"] += 1
            return None

        last_feature_ts = self.last_written_feature_ts.get(symbol)
        if last_feature_ts == feature_ts:
            self.stats["skipped_duplicate"] += 1
            return None

        _, _, snapshot_iso = utc_date_hour(snapshot_ts)

        record = dict(feature)

        record["_capture_id"] = self.capture_id
        record["_snapshot_ts"] = snapshot_ts
        record["_snapshot_iso"] = snapshot_iso
        record["_snapshot_interval_sec"] = SNAPSHOT_INTERVAL_SEC
        record["_feature_age_ms_at_snapshot"] = max(0, snapshot_ts - feature_ts)
        record["_recorder_version"] = "live_feature_recorder_v1"

        # Đảm bảo symbol chuẩn.
        record["symbol"] = symbol

        self.last_written_feature_ts[symbol] = feature_ts
        return record

    async def snapshot_loop(self):
        print("💾 [RECORDER] Snapshot loop started")

        next_tick = time.time()

        while self.running:
            now = time.time()

            if now < next_tick:
                await asyncio.sleep(min(0.05, next_tick - now))
                continue

            snapshot_ts = now_ms()

            free_gb = disk_free_gb(DATA_DIR)
            if free_gb < MIN_FREE_DISK_GB:
                print(f"🚨 [DISK] Free disk thấp: {free_gb:.2f}GB < {MIN_FREE_DISK_GB}GB. Tạm ngưng ghi vòng này.")
                await asyncio.sleep(5)
                next_tick = time.time() + SNAPSHOT_INTERVAL_SEC
                continue

            items = list(self.latest_by_symbol.items())

            for symbol, feature in items:
                record = self._build_snapshot_record(symbol, feature, snapshot_ts)
                if not record:
                    continue

                try:
                    self.writer.write(symbol, snapshot_ts, record)
                    self.stats["written"] += 1
                except Exception as e:
                    print(f"⚠️ [WRITE] Lỗi ghi {symbol}: {e}")

            next_tick += SNAPSHOT_INTERVAL_SEC

            # Nếu loop bị trễ quá nhiều, reset lại nhịp để không backlog.
            if time.time() - next_tick > 3:
                next_tick = time.time() + SNAPSHOT_INTERVAL_SEC

    async def redis_listener_loop(self):
        print(f"📡 [REDIS] Connecting {REDIS_URL}")
        print(f"📡 [REDIS] PSubscribe {FEATURE_PATTERN}")

        while self.running:
            pubsub = None

            try:
                pubsub = self.redis.pubsub()
                await pubsub.psubscribe(FEATURE_PATTERN)

                print("✅ [REDIS] Recorder listening")

                async for message in pubsub.listen():
                    if not self.running:
                        break

                    if message.get("type") != "pmessage":
                        continue

                    channel = message.get("channel")
                    data = message.get("data")

                    if not channel or not data:
                        continue

                    await self.handle_message(channel, data)

            except Exception as e:
                print(f"⚠️ [REDIS] Listener error: {e}. Retry sau 3s...")
                await asyncio.sleep(3)

            finally:
                try:
                    if pubsub:
                        await pubsub.close()
                except Exception:
                    pass

    async def stats_loop(self):
        while self.running:
            await asyncio.sleep(STATS_INTERVAL_SEC)

            uptime_sec = max(1, (now_ms() - self.stats["started_at"]) / 1000)
            active_symbols = len(self.latest_by_symbol)
            free_gb = disk_free_gb(DATA_DIR)

            print(
                "📊 [RECORDER] "
                f"uptime={uptime_sec:.0f}s "
                f"symbols={active_symbols} "
                f"received={self.stats['received']} "
                f"decoded={self.stats['decoded']} "
                f"written={self.stats['written']} "
                f"stale={self.stats['skipped_stale']} "
                f"notWarm={self.stats['skipped_not_warm']} "
                f"dup={self.stats['skipped_duplicate']} "
                f"openFiles={len(self.writer.handles)} "
                f"diskFree={free_gb:.2f}GB "
                f"dir={DATA_DIR}"
            )

            self.writer.flush_all()

    async def run(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)

        print("🚀 [LIVE FEATURE RECORDER V1] Starting...")
        print(f"⚙️ capture_id={self.capture_id}")
        print(f"⚙️ redis={REDIS_URL}")
        print(f"⚙️ pattern={FEATURE_PATTERN}")
        print(f"⚙️ data_dir={DATA_DIR.resolve()}")
        print(f"⚙️ snapshot={SNAPSHOT_INTERVAL_SEC}s stale<={MAX_FEATURE_STALE_SEC}s warmOnly={not RECORD_NOT_WARM}")

        await asyncio.gather(
            self.redis_listener_loop(),
            self.snapshot_loop(),
            self.stats_loop(),
        )


# ============================================================
# 5. MAIN
# ============================================================
async def main():
    recorder = LiveFeatureRecorder()

    loop = asyncio.get_running_loop()

    async def shutdown():
        print("\n🛑 [RECORDER] Shutting down...")
        await recorder.stop()

    def _handle_signal():
        asyncio.create_task(shutdown())

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            pass

    try:
        await recorder.run()
    finally:
        await recorder.stop()


if __name__ == "__main__":
    asyncio.run(main())