#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import gc
import gzip
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import msgpack
import redis.asyncio as aioredis
from dotenv import load_dotenv

load_dotenv()


# ============================================================
# REDIS MULTI-KEY ARCHIVE EXPORTER V2 - SAFE SSD EXPORT
# ------------------------------------------------------------
# Dành cho key dạng:
# features:archive:BTCUSDT
# features:archive:ETHUSDT
# features:archive:1000PEPEUSDT
#
# Nhiệm vụ:
# 1. Scan toàn bộ Redis Stream theo pattern features:archive:*.
# 2. Không lấy dữ liệu 1 giờ gần nhất.
# 3. Export từng batch ra SSD dạng .jsonl.gz.
# 4. Ghi .tmp.gz trước, đóng gzip, fsync, rename final.
# 5. Ghi manifest.
# 6. Sau khi file + manifest thành công mới XDEL Redis IDs.
# 7. XTRIM từng stream để Redis dọn RAM.
# 8. Log rõ tiến độ theo coin / batch / tổng dòng.
# ============================================================


# ============================================================
# 1. CẤU HÌNH CỨNG
# ============================================================
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")

ARCHIVE_PATTERN = "features:archive:*"

EXPORT_DIR = Path("./data/live_features_archive")
MANIFEST_FILE = Path("./data/live_features_archive/_manifest.jsonl")

# Không export dữ liệu 1 giờ gần nhất để tránh đụng dữ liệu đang ghi.
SKIP_RECENT_SEC = 3600

# Mỗi batch đọc tối đa N dòng / 1 stream coin.
BATCH_SIZE = 5000

# Sau khi export file thành công thì xóa các ID khỏi Redis.
DELETE_AFTER_EXPORT = True

# Sau khi xử lý xong một stream, XTRIM MINID để Redis dọn RAM.
TRIM_AFTER_EXPORT = True

# Dừng nếu ổ cứng trống dưới mức này.
MIN_FREE_DISK_GB = 2.0

# Nếu True: chỉ scan/log, không ghi file, không xóa Redis.
DRY_RUN = False

EXPORT_VERSION = "redis_archive_multi_key_exporter_v2"


# ============================================================
# 2. UTILS
# ============================================================
def now_ms() -> int:
    return int(time.time() * 1000)


def utc_parts(ts_ms: Optional[int] = None) -> Dict[str, str]:
    if ts_ms is None:
        ts_ms = now_ms()

    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)

    return {
        "date": dt.strftime("%Y-%m-%d"),
        "hour": dt.strftime("%H"),
        "iso": dt.isoformat(),
        "compact": dt.strftime("%Y%m%dT%H%M%S")
    }


def stream_id_to_ms(stream_id: Any) -> int:
    if isinstance(stream_id, bytes):
        stream_id = stream_id.decode("utf-8", errors="replace")

    text = str(stream_id)

    try:
        return int(text.split("-")[0])
    except Exception:
        return 0


def stream_id_to_str(stream_id: Any) -> str:
    if isinstance(stream_id, bytes):
        return stream_id.decode("utf-8", errors="replace")
    return str(stream_id)


def key_to_str(key: Any) -> str:
    if isinstance(key, bytes):
        return key.decode("utf-8", errors="replace")
    return str(key)


def symbol_from_key(key: str) -> str:
    # features:archive:BTCUSDT -> BTCUSDT
    return key.split(":")[-1].upper().strip()


def safe_decode_bytes(value: Any) -> Any:
    if not isinstance(value, (bytes, bytearray)):
        return value

    raw = bytes(value)

    try:
        return msgpack.unpackb(raw, raw=False)
    except Exception:
        pass

    try:
        text = raw.decode("utf-8", errors="replace")
        stripped = text.strip()

        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return json.loads(stripped)
            except Exception:
                return text

        return text
    except Exception:
        return str(raw)


def normalize_stream_fields(fields: Dict[Any, Any]) -> Dict[str, Any]:
    clean = {}

    for k, v in fields.items():
        key = k.decode("utf-8", errors="replace") if isinstance(k, bytes) else str(k)
        clean[key] = safe_decode_bytes(v)

    # Trường hợp stream lưu cả feature trong 1 field.
    for container_key in ["data", "feature", "payload", "json", "msgpack"]:
        val = clean.get(container_key)

        if isinstance(val, dict):
            merged = dict(val)
            merged["_archive_container_key"] = container_key
            return merged

        if isinstance(val, str):
            stripped = val.strip()
            if stripped.startswith("{"):
                try:
                    merged = json.loads(stripped)
                    merged["_archive_container_key"] = container_key
                    return merged
                except Exception:
                    pass

    return clean


def json_dumps(record: Dict[str, Any]) -> str:
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str)


def disk_free_gb(path: Path) -> float:
    try:
        path.mkdir(parents=True, exist_ok=True)
        stat = os.statvfs(str(path))
        return stat.f_bavail * stat.f_frsize / (1024 ** 3)
    except Exception:
        return 999.0


def fsync_file(path: Path):
    with open(path, "rb") as f:
        os.fsync(f.fileno())


def append_manifest(record: Dict[str, Any]):
    MANIFEST_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(MANIFEST_FILE, "a", encoding="utf-8") as f:
        f.write(json_dumps(record))
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())


def build_export_paths(redis_key: str, symbol: str, start_id: str, end_id: str, count: int, cutoff_ms: int) -> Tuple[Path, Path]:
    start_ms = stream_id_to_ms(start_id) or cutoff_ms
    parts = utc_parts(start_ms)

    folder = EXPORT_DIR / parts["date"] / parts["hour"]
    folder.mkdir(parents=True, exist_ok=True)

    safe_start = start_id.replace("-", "_")
    safe_end = end_id.replace("-", "_")

    base_name = f"{symbol}_{safe_start}_{safe_end}_{count}"
    final_path = folder / f"{base_name}.jsonl.gz"
    tmp_path = folder / f"{base_name}.jsonl.gz.tmp"

    return tmp_path, final_path


# ============================================================
# 3. EXPORTER
# ============================================================
class RedisArchiveExporter:
    def __init__(self):
        self.export_id = str(uuid.uuid4())

        self.redis = aioredis.from_url(
            REDIS_URL,
            decode_responses=False,
            socket_keepalive=True,
            health_check_interval=30,
            retry_on_timeout=True,
        )

        self.stats = {
            "keys_found": 0,
            "keys_stream": 0,
            "keys_skipped": 0,
            "keys_completed": 0,
            "read": 0,
            "exported": 0,
            "deleted": 0,
            "batches": 0,
            "files": 0,
            "errors": 0,
            "started_at_ms": now_ms()
        }

    async def close(self):
        try:
            await self.redis.close()
        except Exception:
            pass

    async def scan_archive_keys(self) -> List[str]:
        keys = []

        async for raw_key in self.redis.scan_iter(match=ARCHIVE_PATTERN, count=500):
            key = key_to_str(raw_key)
            keys.append(key)

        keys = sorted(set(keys))
        self.stats["keys_found"] = len(keys)
        return keys

    async def get_key_type_and_len(self, key: str):
        key_type = await self.redis.type(key)

        if isinstance(key_type, bytes):
            key_type = key_type.decode("utf-8", errors="replace")

        if key_type == "stream":
            length = await self.redis.xlen(key)
        else:
            length = 0

        return key_type, int(length or 0)

    async def fetch_batch(self, key: str, cutoff_id: str):
        return await self.redis.xrange(
            key,
            min="-",
            max=cutoff_id,
            count=BATCH_SIZE
        )

    def convert_entries_to_records(self, redis_key: str, symbol: str, entries) -> List[Dict[str, Any]]:
        records = []
        seen_ids = set()

        for stream_id_raw, fields in entries:
            stream_id = stream_id_to_str(stream_id_raw)

            if stream_id in seen_ids:
                continue

            seen_ids.add(stream_id)

            feature = normalize_stream_fields(fields)

            if not isinstance(feature, dict):
                feature = {
                    "raw_value": str(feature)
                }

            record = dict(feature)

            # Đảm bảo symbol luôn có.
            record["symbol"] = str(record.get("symbol") or symbol).upper().strip()

            record["_redis_key"] = redis_key
            record["_redis_stream_id"] = stream_id
            record["_redis_stream_ms"] = stream_id_to_ms(stream_id)
            record["_export_id"] = self.export_id
            record["_export_version"] = EXPORT_VERSION
            record["_exported_at_ms"] = now_ms()
            record["_exported_at_iso"] = utc_parts(now_ms())["iso"]

            records.append(record)

        return records

    def write_records_atomic(self, redis_key: str, symbol: str, records: List[Dict[str, Any]], cutoff_ms: int) -> Path:
        if not records:
            raise RuntimeError("No records to write")

        start_id = records[0]["_redis_stream_id"]
        end_id = records[-1]["_redis_stream_id"]

        tmp_path, final_path = build_export_paths(redis_key, symbol, start_id, end_id, len(records), cutoff_ms)

        # Nếu file final đã tồn tại, coi như batch này đã từng export.
        if final_path.exists():
            print(f"♻️ [SKIP FILE EXISTS] {final_path}")
            return final_path

        if tmp_path.exists():
            tmp_path.unlink()

        with gzip.open(tmp_path, "wt", encoding="utf-8") as f:
            for record in records:
                f.write(json_dumps(record))
                f.write("\n")
            f.flush()

        fsync_file(tmp_path)

        os.replace(tmp_path, final_path)

        fsync_file(final_path)

        return final_path

    async def delete_stream_ids(self, key: str, ids: List[str]) -> int:
        if not ids:
            return 0

        if DRY_RUN or not DELETE_AFTER_EXPORT:
            return 0

        deleted = 0
        chunk_size = 1000

        for i in range(0, len(ids), chunk_size):
            chunk = ids[i:i + chunk_size]
            result = await self.redis.xdel(key, *chunk)
            deleted += int(result or 0)

        return deleted

    async def trim_stream(self, key: str, cutoff_id: str):
        if DRY_RUN or not TRIM_AFTER_EXPORT:
            return

        try:
            result = await self.redis.execute_command("XTRIM", key, "MINID", cutoff_id)
            print(f"🧹 [XTRIM] key={key} MINID={cutoff_id} result={result}")
        except Exception as e:
            print(f"⚠️ [XTRIM] key={key} không trim được: {e}")

    async def export_key(self, key: str, cutoff_id: str, cutoff_ms: int, index: int, total_keys: int):
        symbol = symbol_from_key(key)

        key_type, initial_len = await self.get_key_type_and_len(key)

        if key_type != "stream":
            self.stats["keys_skipped"] += 1
            print(f"⏭️ [KEY {index}/{total_keys}] skip {key} type={key_type}")
            return

        self.stats["keys_stream"] += 1

        if initial_len <= 0:
            self.stats["keys_completed"] += 1
            print(f"ℹ️ [KEY {index}/{total_keys}] {key} stream rỗng")
            return

        print("")
        print(f"🔎 [KEY {index}/{total_keys}] {key} symbol={symbol} initial_len={initial_len}")

        key_exported = 0
        key_deleted = 0
        key_batches = 0

        while True:
            free_gb = disk_free_gb(EXPORT_DIR)

            if free_gb < MIN_FREE_DISK_GB:
                raise RuntimeError(
                    f"Disk free thấp: {free_gb:.2f}GB < {MIN_FREE_DISK_GB}GB. Dừng để an toàn."
                )

            entries = await self.fetch_batch(key, cutoff_id)

            if not entries:
                print(
                    f"✅ [KEY DONE] {key} exported={key_exported} deleted={key_deleted} batches={key_batches}"
                )
                break

            self.stats["batches"] += 1
            key_batches += 1

            ids = [stream_id_to_str(e[0]) for e in entries]
            first_id = ids[0]
            last_id = ids[-1]

            current_len_before = await self.redis.xlen(key)

            print(
                f"📥 [BATCH] key={key} "
                f"batch={key_batches} "
                f"read={len(entries)} "
                f"range={first_id}→{last_id} "
                f"stream_len_before={current_len_before}"
            )

            records = self.convert_entries_to_records(key, symbol, entries)

            self.stats["read"] += len(records)

            if DRY_RUN:
                print(f"🧪 [DRY RUN] key={key} would_export={len(records)} would_delete={len(ids)}")
                break

            try:
                final_path = self.write_records_atomic(key, symbol, records, cutoff_ms)

                manifest_record = {
                    "export_id": self.export_id,
                    "export_version": EXPORT_VERSION,
                    "redis_key": key,
                    "symbol": symbol,
                    "first_id": first_id,
                    "last_id": last_id,
                    "records": len(records),
                    "file": str(final_path),
                    "cutoff_id": cutoff_id,
                    "cutoff_ms": cutoff_ms,
                    "cutoff_iso": utc_parts(cutoff_ms)["iso"],
                    "completed_at_ms": now_ms(),
                    "completed_at_iso": utc_parts(now_ms())["iso"]
                }

                append_manifest(manifest_record)

                deleted = await self.delete_stream_ids(key, ids)

                self.stats["exported"] += len(records)
                self.stats["deleted"] += deleted
                self.stats["files"] += 1

                key_exported += len(records)
                key_deleted += deleted

                current_len_after = await self.redis.xlen(key)

                print(
                    f"✅ [BATCH DONE] key={key} "
                    f"file={final_path} "
                    f"exported={len(records)} "
                    f"deleted={deleted} "
                    f"stream_len_after={current_len_after} "
                    f"total_exported={self.stats['exported']} "
                    f"total_deleted={self.stats['deleted']}"
                )

            except Exception as e:
                self.stats["errors"] += 1
                print(f"❌ [BATCH ERROR] key={key} error={e}")
                print("⚠️ Không xóa Redis IDs vì export batch chưa hoàn tất")
                raise

            del entries
            del records
            del ids
            gc.collect()

        await self.trim_stream(key, cutoff_id)
        self.stats["keys_completed"] += 1

    async def export_once(self):
        cutoff_ms = now_ms() - SKIP_RECENT_SEC * 1000
        cutoff_id = f"{cutoff_ms}-999999"

        keys = await self.scan_archive_keys()

        print("")
        print("============================================================")
        print("🚚 REDIS MULTI-KEY ARCHIVE EXPORT START")
        print("============================================================")
        print(f"redis={REDIS_URL}")
        print(f"pattern={ARCHIVE_PATTERN}")
        print(f"keys_found={len(keys)}")
        print(f"output_dir={EXPORT_DIR.resolve()}")
        print(f"manifest={MANIFEST_FILE.resolve()}")
        print(f"skip_recent={SKIP_RECENT_SEC}s")
        print(f"cutoff_id={cutoff_id}")
        print(f"cutoff_iso={utc_parts(cutoff_ms)['iso']}")
        print(f"batch_size={BATCH_SIZE}")
        print(f"delete_after_export={DELETE_AFTER_EXPORT}")
        print(f"trim_after_export={TRIM_AFTER_EXPORT}")
        print(f"dry_run={DRY_RUN}")
        print("============================================================")
        print("")

        if not keys:
            print("ℹ️ Không tìm thấy key archive nào")
            return

        for idx, key in enumerate(keys, 1):
            await self.export_key(key, cutoff_id, cutoff_ms, idx, len(keys))

        runtime_sec = (now_ms() - self.stats["started_at_ms"]) / 1000

        print("")
        print("============================================================")
        print("✅ REDIS MULTI-KEY ARCHIVE EXPORT DONE")
        print("============================================================")
        print(f"keys_found={self.stats['keys_found']}")
        print(f"keys_stream={self.stats['keys_stream']}")
        print(f"keys_skipped={self.stats['keys_skipped']}")
        print(f"keys_completed={self.stats['keys_completed']}")
        print(f"batches={self.stats['batches']}")
        print(f"files={self.stats['files']}")
        print(f"read={self.stats['read']}")
        print(f"exported={self.stats['exported']}")
        print(f"deleted={self.stats['deleted']}")
        print(f"errors={self.stats['errors']}")
        print(f"runtime={runtime_sec:.1f}s")
        print(f"manifest={MANIFEST_FILE}")
        print("============================================================")
        print("")


# ============================================================
# 4. MAIN
# ============================================================
async def main():
    exporter = RedisArchiveExporter()

    try:
        await exporter.export_once()
    finally:
        await exporter.close()


if __name__ == "__main__":
    asyncio.run(main())