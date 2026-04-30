#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import gc
import gzip
import hashlib
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
# REDIS MULTI-KEY ARCHIVE EXPORTER V2.1.1 - CURSOR SAFE + NO DUPLICATE READ PATCH
# ------------------------------------------------------------
# Dành cho key dạng:
# features:archive:BTCUSDT
# features:archive:ETHUSDT
# features:archive:1000PEPEUSDT
#
# Nhiệm vụ:
# 1. Scan toàn bộ Redis Stream theo pattern features:archive:*.
# 2. Không lấy dữ liệu gần nhất theo SKIP_RECENT_SEC.
# 3. Export từng batch ra SSD dạng .jsonl.gz.
# 4. Ghi .tmp trước, đóng gzip, fsync file, rename final, fsync directory.
# 5. Validate gzip/jsonl sau khi ghi.
# 6. Ghi manifest sau khi file hợp lệ.
# 7. Sau khi file + manifest thành công mới XDEL Redis IDs.
# 8. XTRIM từng stream để Redis dọn RAM.
# 9. Giữ record phẳng để train_dual_brain.py V3.1.4 đọc trực tiếp.
# ============================================================


# ============================================================
# 1. CONFIG HELPERS
# ============================================================
def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default

    text = str(raw).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False

    return default


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default

    try:
        return int(float(raw))
    except Exception:
        return default


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default

    try:
        return float(raw)
    except Exception:
        return default


# ============================================================
# 2. RUNTIME CONFIG
# ============================================================
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")

ARCHIVE_PATTERN = os.getenv("REDIS_ARCHIVE_PATTERN", "features:archive:*")

EXPORT_DIR = Path(os.getenv("REDIS_ARCHIVE_EXPORT_DIR", "./data/live_features_archive"))
MANIFEST_FILE = Path(os.getenv("REDIS_ARCHIVE_MANIFEST_FILE", "./data/live_features_archive/_manifest.jsonl"))

# Không export dữ liệu gần nhất để tránh đụng dữ liệu đang ghi.
SKIP_RECENT_SEC = env_int("REDIS_ARCHIVE_SKIP_RECENT_SEC", 3600)

# Mỗi batch đọc tối đa N dòng / 1 stream coin.
BATCH_SIZE = env_int("REDIS_ARCHIVE_BATCH_SIZE", 5000)

# Sau khi export file + manifest thành công thì xóa các ID khỏi Redis.
DELETE_AFTER_EXPORT = env_bool("REDIS_ARCHIVE_DELETE_AFTER_EXPORT", True)

# Sau khi xử lý xong một stream, XTRIM MINID để Redis dọn RAM.
TRIM_AFTER_EXPORT = env_bool("REDIS_ARCHIVE_TRIM_AFTER_EXPORT", True)

# Dừng nếu ổ cứng trống dưới mức này.
MIN_FREE_DISK_GB = env_float("REDIS_ARCHIVE_MIN_FREE_DISK_GB", 2.0)

# Nếu True: chỉ scan/log/convert, không ghi file, không xóa Redis, không trim.
DRY_RUN = env_bool("REDIS_ARCHIVE_DRY_RUN", False)
DRY_RUN_MAX_BATCHES_PER_KEY = env_int("REDIS_ARCHIVE_DRY_RUN_MAX_BATCHES_PER_KEY", 1)

EXPORT_VERSION = "redis_archive_multi_key_exporter_v2_1_1_cursor_safe_train_v3_1_4_aligned"


# ============================================================
# 3. UTILS
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


def next_stream_id(stream_id: str) -> str:
    try:
        ms, seq = str(stream_id).split("-", 1)
        return f"{int(ms)}-{int(seq) + 1}"
    except Exception:
        return stream_id


def key_to_str(key: Any) -> str:
    if isinstance(key, bytes):
        return key.decode("utf-8", errors="replace")
    return str(key)


def symbol_from_key(key: str) -> str:
    return key.split(":")[-1].upper().strip()


def exporter_hostname() -> str:
    try:
        return os.uname().nodename
    except Exception:
        return os.getenv("HOSTNAME", "")


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
    clean: Dict[str, Any] = {}

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


def fsync_dir(path: Path):
    fd = None
    try:
        fd = os.open(str(path), os.O_RDONLY)
        os.fsync(fd)
    except Exception:
        pass
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass


def file_sha1(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def validate_gzip_jsonl_file(path: Path, expected_count: int) -> Tuple[bool, int, str]:
    if not path.exists():
        return False, 0, "FILE_NOT_FOUND"

    count = 0

    try:
        with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                    if not isinstance(obj, dict):
                        return False, count, "JSON_NOT_OBJECT"
                    count += 1
                except Exception as e:
                    return False, count, f"JSON_ERROR:{e}"
    except Exception as e:
        return False, count, f"GZIP_ERROR:{e}"

    if count != expected_count:
        return False, count, f"COUNT_MISMATCH expected={expected_count} got={count}"

    return True, count, "OK"


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
# 4. EXPORTER
# ============================================================
class RedisArchiveExporter:
    def __init__(self):
        self.export_id = str(uuid.uuid4())
        started_at_ms = now_ms()

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
            "files_written": 0,
            "errors": 0,
            "bytes_written": 0,
            "manifest_written": 0,
            "files_existing_valid": 0,
            "files_corrupt_rewritten": 0,
            "dry_run_batches": 0,
            "symbol_mismatch": 0,
            "validate_errors": 0,
            "started_at_ms": started_at_ms,
            "started_at_iso": utc_parts(started_at_ms)["iso"],
            "completed_at_ms": 0,
            "completed_at_iso": "",
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

    async def fetch_batch(self, key: str, start_id: str, cutoff_id: str):
        return await self.redis.xrange(
            key,
            min=start_id,
            max=cutoff_id,
            count=BATCH_SIZE
        )

    def convert_entries_to_records(
        self,
        redis_key: str,
        symbol: str,
        entries,
        cutoff_ms: int
    ) -> List[Dict[str, Any]]:
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

            record_symbol = str(record.get("symbol") or "").upper().strip()
            key_symbol = str(symbol or "").upper().strip()

            if record_symbol:
                record["symbol"] = record_symbol
                record["_archive_symbol_from_key"] = key_symbol
                record["_archive_symbol_mismatch"] = bool(record_symbol != key_symbol)

                if record_symbol != key_symbol:
                    self.stats["symbol_mismatch"] += 1
            else:
                record["symbol"] = key_symbol
                record["_archive_symbol_from_key"] = key_symbol
                record["_archive_symbol_mismatch"] = False

            stream_ms = stream_id_to_ms(stream_id)

            record["_redis_key"] = redis_key
            record["_redis_stream_id"] = stream_id
            record["_redis_stream_ms"] = stream_ms
            record["_export_id"] = self.export_id
            record["_export_version"] = EXPORT_VERSION
            record["_exported_at_ms"] = now_ms()
            record["_exported_at_iso"] = utc_parts(now_ms())["iso"]
            record["_archive_key_type"] = "stream"
            record["_archive_cutoff_safe"] = bool(stream_ms <= cutoff_ms)

            records.append(record)

        return records

    def write_records_atomic(self, redis_key: str, symbol: str, records: List[Dict[str, Any]], cutoff_ms: int) -> Tuple[Path, int, int, bool, bool]:
        if not records:
            raise RuntimeError("No records to write")

        start_id = records[0]["_redis_stream_id"]
        end_id = records[-1]["_redis_stream_id"]

        tmp_path, final_path = build_export_paths(redis_key, symbol, start_id, end_id, len(records), cutoff_ms)

        existing_valid = False
        corrupt_rewritten = False

        if final_path.exists():
            ok, count, reason = validate_gzip_jsonl_file(final_path, len(records))

            if ok:
                print(f"♻️ [SKIP FILE EXISTS VALID] {final_path} count={count}")
                return final_path, count, final_path.stat().st_size, True, False

            self.stats["validate_errors"] += 1
            corrupt_ts = utc_parts(now_ms())["compact"]
            corrupt_path = final_path.with_name(f"{final_path.name}.corrupt.{corrupt_ts}")
            os.replace(final_path, corrupt_path)
            fsync_dir(final_path.parent)
            corrupt_rewritten = True
            print(f"⚠️ [CORRUPT FILE] {final_path} reason={reason} -> {corrupt_path}")

        if tmp_path.exists():
            tmp_path.unlink()

        with gzip.open(tmp_path, "wt", encoding="utf-8") as f:
            for record in records:
                f.write(json_dumps(record))
                f.write("\n")
            f.flush()

        fsync_file(tmp_path)

        os.replace(tmp_path, final_path)
        fsync_dir(final_path.parent)
        fsync_file(final_path)

        ok, valid_count, reason = validate_gzip_jsonl_file(final_path, len(records))
        if not ok:
            self.stats["validate_errors"] += 1
            raise RuntimeError(f"Final gzip validation failed: {final_path} reason={reason}")

        file_size = final_path.stat().st_size

        return final_path, valid_count, file_size, existing_valid, corrupt_rewritten

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
        if DRY_RUN:
            return

        if not DELETE_AFTER_EXPORT:
            print(f"🧹 [XTRIM] key={key} skip trim because DELETE_AFTER_EXPORT=False")
            return

        if not TRIM_AFTER_EXPORT:
            return

        try:
            result = await self.redis.execute_command("XTRIM", key, "MINID", cutoff_id)
            print(f"🧹 [XTRIM] key={key} cutoff_id={cutoff_id} result={result}")
        except Exception as e:
            print(f"⚠️ [XTRIM] key={key} cutoff_id={cutoff_id} không trim được: {e}")

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
        key_failed = False
        next_min_id = "-"

        while True:
            free_gb = disk_free_gb(EXPORT_DIR)

            if free_gb < MIN_FREE_DISK_GB:
                raise RuntimeError(
                    f"Disk free thấp: {free_gb:.2f}GB < {MIN_FREE_DISK_GB}GB. Dừng để an toàn."
                )

            entries = await self.fetch_batch(key, next_min_id, cutoff_id)

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
            next_min_id = next_stream_id(last_id)

            current_len_before = await self.redis.xlen(key)

            print(
                f"📥 [BATCH] key={key} "
                f"batch={key_batches} "
                f"read={len(entries)} "
                f"range={first_id}→{last_id} "
                f"stream_len_before={current_len_before}"
            )

            records = self.convert_entries_to_records(key, symbol, entries, cutoff_ms)

            self.stats["read"] += len(records)

            if DRY_RUN:
                self.stats["dry_run_batches"] += 1
                print(
                    f"🧪 [DRY RUN] key={key} "
                    f"batch={key_batches} "
                    f"would_export={len(records)} "
                    f"would_delete={len(ids)} "
                    f"first_id={first_id} "
                    f"last_id={last_id}"
                )

                if key_batches >= max(1, DRY_RUN_MAX_BATCHES_PER_KEY):
                    print(f"🧪 [DRY RUN] key={key} stop after {key_batches} batches")
                    break

                del entries
                del records
                del ids
                gc.collect()
                continue

            try:
                final_path, valid_count, file_size, existing_valid, corrupt_rewritten = self.write_records_atomic(
                    key,
                    symbol,
                    records,
                    cutoff_ms
                )

                if existing_valid:
                    self.stats["files_existing_valid"] += 1
                else:
                    self.stats["files_written"] += 1
                    self.stats["bytes_written"] += file_size

                if corrupt_rewritten:
                    self.stats["files_corrupt_rewritten"] += 1

                manifest_record = {
                    "manifest_key": f"{key}:{first_id}:{last_id}:{len(records)}",
                    "export_id": self.export_id,
                    "export_version": EXPORT_VERSION,
                    "redis_key": key,
                    "symbol": symbol,
                    "first_id": first_id,
                    "last_id": last_id,
                    "records": len(records),
                    "record_count_validated": valid_count,
                    "file": str(final_path),
                    "file_size_bytes": file_size,
                    "file_sha1": file_sha1(final_path),
                    "redis_stream_ms_start": stream_id_to_ms(first_id),
                    "redis_stream_ms_end": stream_id_to_ms(last_id),
                    "exporter_hostname": exporter_hostname(),
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
                self.stats["manifest_written"] += 1

                key_exported += len(records)
                key_deleted += deleted

                current_len_after = await self.redis.xlen(key)

                print(
                    f"✅ [BATCH DONE] key={key} "
                    f"file={final_path} "
                    f"exported={len(records)} "
                    f"validated={valid_count} "
                    f"bytes={file_size} "
                    f"deleted={deleted} "
                    f"stream_len_after={current_len_after} "
                    f"total_exported={self.stats['exported']} "
                    f"total_deleted={self.stats['deleted']}"
                )

            except Exception as e:
                self.stats["errors"] += 1
                key_failed = True
                print(f"❌ [BATCH ERROR] key={key} error={e}")
                print("⚠️ Không xóa Redis IDs vì export/manifest batch chưa hoàn tất")
                raise

            finally:
                del entries
                del records
                del ids
                gc.collect()

        if not key_failed:
            await self.trim_stream(key, cutoff_id)
            self.stats["keys_completed"] += 1

    async def export_once(self):
        cutoff_ms = now_ms() - SKIP_RECENT_SEC * 1000
        cutoff_id = f"{cutoff_ms}-999999"

        keys = await self.scan_archive_keys()

        print("")
        print("============================================================")
        print("🚚 REDIS MULTI-KEY ARCHIVE EXPORT START V2.1.1")
        print("============================================================")
        print(f"redis={REDIS_URL}")
        print(f"pattern={ARCHIVE_PATTERN}")
        print(f"version={EXPORT_VERSION}")
        print(f"keys_found={len(keys)}")
        print(f"output_dir={EXPORT_DIR.resolve()}")
        print(f"manifest={MANIFEST_FILE.resolve()}")
        print(f"skip_recent={SKIP_RECENT_SEC}s")
        print(f"cutoff_id={cutoff_id}")
        print(f"cutoff_iso={utc_parts(cutoff_ms)['iso']}")
        print(f"batch_size={BATCH_SIZE}")
        print(f"delete_after_export={DELETE_AFTER_EXPORT}")
        print(f"trim_after_export={TRIM_AFTER_EXPORT}")
        print(f"min_free_disk_gb={MIN_FREE_DISK_GB}")
        print(f"dry_run={DRY_RUN}")
        print(f"dry_run_max_batches_per_key={DRY_RUN_MAX_BATCHES_PER_KEY}")
        print("============================================================")
        print("")

        if not keys:
            print("ℹ️ Không tìm thấy key archive nào")
            return

        for idx, key in enumerate(keys, 1):
            await self.export_key(key, cutoff_id, cutoff_ms, idx, len(keys))

        completed_at_ms = now_ms()
        self.stats["completed_at_ms"] = completed_at_ms
        self.stats["completed_at_iso"] = utc_parts(completed_at_ms)["iso"]

        runtime_sec = (completed_at_ms - self.stats["started_at_ms"]) / 1000

        print("")
        print("============================================================")
        print("✅ REDIS MULTI-KEY ARCHIVE EXPORT DONE V2.1.1")
        print("============================================================")
        print(f"version={EXPORT_VERSION}")
        print(f"keys_found={self.stats['keys_found']}")
        print(f"keys_stream={self.stats['keys_stream']}")
        print(f"keys_skipped={self.stats['keys_skipped']}")
        print(f"keys_completed={self.stats['keys_completed']}")
        print(f"batches={self.stats['batches']}")
        print(f"files={self.stats['files']}")
        print(f"files_written={self.stats['files_written']}")
        print(f"files_existing_valid={self.stats['files_existing_valid']}")
        print(f"files_corrupt_rewritten={self.stats['files_corrupt_rewritten']}")
        print(f"read={self.stats['read']}")
        print(f"exported={self.stats['exported']}")
        print(f"deleted={self.stats['deleted']}")
        print(f"bytes_written={self.stats['bytes_written']}")
        print(f"manifest_written={self.stats['manifest_written']}")
        print(f"dry_run_batches={self.stats['dry_run_batches']}")
        print(f"symbol_mismatch={self.stats['symbol_mismatch']}")
        print(f"validate_errors={self.stats['validate_errors']}")
        print(f"errors={self.stats['errors']}")
        print(f"started_at_iso={self.stats['started_at_iso']}")
        print(f"completed_at_iso={self.stats['completed_at_iso']}")
        print(f"runtime={runtime_sec:.1f}s")
        print(f"manifest={MANIFEST_FILE}")
        print("============================================================")
        print("")


# ============================================================
# 5. MAIN
# ============================================================
async def main():
    exporter = RedisArchiveExporter()

    try:
        await exporter.export_once()
    finally:
        await exporter.close()


if __name__ == "__main__":
    asyncio.run(main())
