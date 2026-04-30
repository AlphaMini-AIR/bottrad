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
# REDIS ARCHIVE EXPORTER V1 - SAFE SSD EXPORT
# ------------------------------------------------------------
# Nhiệm vụ:
# 1. Đọc dữ liệu từ Redis Stream features:archive.
# 2. Không lấy dữ liệu 1 giờ gần nhất.
# 3. Ghi ra SSD dạng .jsonl.gz:
#    - ghi file .tmp trước
#    - đóng gzip hoàn chỉnh
#    - fsync
#    - rename atomically thành .jsonl.gz
#    - ghi manifest
# 4. Sau khi file + manifest thành công mới XDEL dữ liệu Redis.
# 5. Sau khi export xong, XTRIM MINID để Redis dọn RAM.
# 6. Log rõ tiến độ.
#
# Lưu ý:
# - File này dành cho Redis Stream.
# - Key mặc định: features:archive.
# - Không dùng Pub/Sub.
# ============================================================


# ============================================================
# 1. CẤU HÌNH CỨNG
# ============================================================

# Redis vẫn có thể lấy từ .env để bạn không phải hard-code host nếu đổi VPS.
REDIS_URL = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")

# Redis Stream chứa dữ liệu live archive.
ARCHIVE_KEY = "features:archive"

# Thư mục SSD lưu dữ liệu đã export.
EXPORT_DIR = Path("./data/live_features_archive")

# File manifest ghi lại các batch đã export thành công.
MANIFEST_FILE = Path("./data/live_features_archive/_manifest.jsonl")

# Không export dữ liệu 1 giờ gần nhất để tránh đụng dữ liệu đang ghi realtime.
SKIP_RECENT_SEC = 3600

# Mỗi batch đọc bao nhiêu dòng từ Redis Stream.
BATCH_SIZE = 5000

# Sau khi export file thành công thì xóa từng ID khỏi Redis.
DELETE_AFTER_EXPORT = True

# Sau khi export hết dữ liệu cũ hơn cutoff thì XTRIM MINID để Redis dọn RAM.
TRIM_AFTER_EXPORT = True

# Dừng nếu ổ cứng trống dưới ngưỡng này.
MIN_FREE_DISK_GB = 2.0

# Chạy thử, không ghi/xóa. Khi chạy thật để False.
DRY_RUN = False

# Version.
EXPORT_VERSION = "redis_archive_exporter_v1_hardcoded"


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


def safe_decode_bytes(value: Any) -> Any:
    if not isinstance(value, (bytes, bytearray)):
        return value

    raw = bytes(value)

    # 1. Thử decode msgpack.
    try:
        return msgpack.unpackb(raw, raw=False)
    except Exception:
        pass

    # 2. Thử decode UTF-8 / JSON.
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
    """
    Redis Stream có thể lưu theo nhiều kiểu:
    - data: msgpack bytes
    - feature: json/msgpack
    - payload: json/msgpack
    - hoặc nhiều field flat.
    Hàm này cố decode linh hoạt.
    """
    clean = {}

    for k, v in fields.items():
        key = k.decode("utf-8", errors="replace") if isinstance(k, bytes) else str(k)
        clean[key] = safe_decode_bytes(v)

    # Nếu toàn bộ feature nằm trong một field container.
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


def build_export_paths(start_id: str, end_id: str, count: int, cutoff_ms: int) -> Tuple[Path, Path]:
    start_ms = stream_id_to_ms(start_id) or cutoff_ms
    parts = utc_parts(start_ms)

    folder = EXPORT_DIR / parts["date"] / parts["hour"]
    folder.mkdir(parents=True, exist_ok=True)

    safe_start = start_id.replace("-", "_")
    safe_end = end_id.replace("-", "_")

    base_name = f"{ARCHIVE_KEY.replace(':', '_')}_{safe_start}_{safe_end}_{count}"
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

    async def check_key(self):
        key_type = await self.redis.type(ARCHIVE_KEY)

        if isinstance(key_type, bytes):
            key_type = key_type.decode("utf-8", errors="replace")

        if key_type != "stream":
            raise RuntimeError(
                f"Redis key {ARCHIVE_KEY} hiện là type={key_type}, không phải stream. "
                f"File này chỉ dùng cho Redis Stream."
            )

        length = await self.redis.xlen(ARCHIVE_KEY)
        return key_type, length

    async def fetch_batch(self, cutoff_id: str):
        """
        Luôn đọc từ đầu stream đến cutoff.
        Vì mỗi batch export xong sẽ XDEL, batch sau lại đọc từ đầu.
        """
        return await self.redis.xrange(
            ARCHIVE_KEY,
            min="-",
            max=cutoff_id,
            count=BATCH_SIZE
        )

    def convert_entries_to_records(self, entries) -> List[Dict[str, Any]]:
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

            record["_redis_key"] = ARCHIVE_KEY
            record["_redis_stream_id"] = stream_id
            record["_redis_stream_ms"] = stream_id_to_ms(stream_id)
            record["_export_id"] = self.export_id
            record["_export_version"] = EXPORT_VERSION
            record["_exported_at_ms"] = now_ms()
            record["_exported_at_iso"] = utc_parts(now_ms())["iso"]

            records.append(record)

        return records

    def write_records_atomic(self, records: List[Dict[str, Any]], cutoff_ms: int) -> Path:
        if not records:
            raise RuntimeError("No records to write")

        start_id = records[0]["_redis_stream_id"]
        end_id = records[-1]["_redis_stream_id"]

        tmp_path, final_path = build_export_paths(start_id, end_id, len(records), cutoff_ms)

        # Nếu final đã tồn tại nghĩa là batch id-range này đã ghi trước đó.
        # Không ghi lại để tránh trùng file.
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

        # Gzip đã close. Fsync file .tmp.
        fsync_file(tmp_path)

        # Atomic rename.
        os.replace(tmp_path, final_path)

        # Fsync file final.
        fsync_file(final_path)

        return final_path

    async def delete_stream_ids(self, ids: List[str]) -> int:
        if not ids:
            return 0

        if DRY_RUN or not DELETE_AFTER_EXPORT:
            return 0

        deleted = 0
        chunk_size = 1000

        for i in range(0, len(ids), chunk_size):
            chunk = ids[i:i + chunk_size]
            result = await self.redis.xdel(ARCHIVE_KEY, *chunk)
            deleted += int(result or 0)

        return deleted

    async def trim_stream(self, cutoff_id: str):
        if DRY_RUN or not TRIM_AFTER_EXPORT:
            return

        try:
            # MINID giúp Redis dọn entries cũ hơn cutoff.
            result = await self.redis.execute_command("XTRIM", ARCHIVE_KEY, "MINID", cutoff_id)
            print(f"🧹 [XTRIM] MINID {cutoff_id} result={result}")
        except Exception as e:
            print(f"⚠️ [XTRIM] Không trim được: {e}")

    async def export_once(self):
        key_type, initial_len = await self.check_key()

        cutoff_ms = now_ms() - SKIP_RECENT_SEC * 1000
        cutoff_id = f"{cutoff_ms}-999999"

        print("")
        print("============================================================")
        print("🚚 REDIS ARCHIVE EXPORT START")
        print("============================================================")
        print(f"redis={REDIS_URL}")
        print(f"key={ARCHIVE_KEY} type={key_type} initial_len={initial_len}")
        print(f"output_dir={EXPORT_DIR.resolve()}")
        print(f"manifest={MANIFEST_FILE.resolve()}")
        print(f"skip_recent={SKIP_RECENT_SEC}s")
        print(f"cutoff_id={cutoff_id} cutoff_iso={utc_parts(cutoff_ms)['iso']}")
        print(f"batch_size={BATCH_SIZE}")
        print(f"delete_after_export={DELETE_AFTER_EXPORT}")
        print(f"trim_after_export={TRIM_AFTER_EXPORT}")
        print(f"dry_run={DRY_RUN}")
        print("============================================================")
        print("")

        if initial_len <= 0:
            print("ℹ️ Stream rỗng, không có gì để export")
            return

        while True:
            free_gb = disk_free_gb(EXPORT_DIR)

            if free_gb < MIN_FREE_DISK_GB:
                raise RuntimeError(
                    f"Disk free thấp: {free_gb:.2f}GB < {MIN_FREE_DISK_GB}GB. Dừng để an toàn."
                )

            entries = await self.fetch_batch(cutoff_id)

            if not entries:
                print("✅ Không còn dữ liệu cũ hơn cutoff để export")
                break

            self.stats["batches"] += 1
            batch_no = self.stats["batches"]

            ids = [stream_id_to_str(e[0]) for e in entries]
            first_id = ids[0]
            last_id = ids[-1]

            current_len_before = await self.redis.xlen(ARCHIVE_KEY)

            print(
                f"📥 [BATCH {batch_no}] "
                f"read={len(entries)} "
                f"range={first_id} → {last_id} "
                f"stream_len_before={current_len_before}"
            )

            records = self.convert_entries_to_records(entries)
            self.stats["read"] += len(records)

            if DRY_RUN:
                print(f"🧪 [DRY RUN] would_export={len(records)} would_delete={len(ids)}")
                break

            try:
                final_path = self.write_records_atomic(records, cutoff_ms)

                manifest_record = {
                    "export_id": self.export_id,
                    "export_version": EXPORT_VERSION,
                    "redis_key": ARCHIVE_KEY,
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

                deleted = await self.delete_stream_ids(ids)

                self.stats["exported"] += len(records)
                self.stats["deleted"] += deleted
                self.stats["files"] += 1

                current_len_after = await self.redis.xlen(ARCHIVE_KEY)

                print(
                    f"✅ [BATCH {batch_no} DONE] "
                    f"file={final_path} "
                    f"exported={len(records)} "
                    f"deleted={deleted} "
                    f"stream_len_after={current_len_after} "
                    f"total_exported={self.stats['exported']} "
                    f"total_deleted={self.stats['deleted']}"
                )

            except Exception as e:
                self.stats["errors"] += 1
                print(f"❌ [BATCH {batch_no} ERROR] {e}")
                print("⚠️ Không xóa Redis IDs vì export batch chưa hoàn tất")
                raise

            # Dọn RAM Python sau mỗi batch.
            del entries
            del records
            del ids
            gc.collect()

        await self.trim_stream(cutoff_id)

        final_len = await self.redis.xlen(ARCHIVE_KEY)
        runtime_sec = (now_ms() - self.stats["started_at_ms"]) / 1000

        print("")
        print("============================================================")
        print("✅ REDIS ARCHIVE EXPORT DONE")
        print("============================================================")
        print(f"initial_len={initial_len}")
        print(f"final_len={final_len}")
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