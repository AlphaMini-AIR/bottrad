#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TRAIN DUAL BRAIN V2 - FULL LIFECYCLE TRADING TRAINER
============================================================

Mục tiêu:
1. Đọc toàn bộ dữ liệu .jsonl.gz từ SSD:
   - data/live_features/
   - hoặc data/live_features_archive/

2. Hỗ trợ dữ liệu cũ và dữ liệu mới:
   - Pub/Sub recorder jsonl.gz
   - Redis archive exporter jsonl.gz
   - Feed schema cũ 13 field
   - Feed schema mới có mark_price, spread_bps, local flow...

3. Train 2 bộ não:
   - EntryBrain: 0 = FLAT, 1 = LONG, 2 = SHORT
   - ExitBrain : 0 = HOLD, 1 = EXIT

4. Replay test full vòng đời lệnh:
   - EntryBrain quyết định vào/đứng ngoài
   - ExitBrain quyết định giữ/thoát
   - Có emergency stop
   - Có max hold
   - Có phí + slippage
   - Có vốn gốc, margin, leverage
   - Có ROI theo vốn và PnL USDT
   - Log tiến độ mỗi 5%

5. Export:
   - EntryBrain.json
   - EntryBrain.onnx
   - EntryBrain.features.json
   - ExitBrain.json
   - ExitBrain.onnx
   - ExitBrain.features.json
   - training_report.json
   - replay_trades.jsonl

Cài thư viện:
source .venv/bin/activate
pip install numpy pandas xgboost scikit-learn onnxmltools onnx

Chạy full:
python src/services/training/train_dual_brain.py \
  --data-dir ./data/live_features_archive \
  --output-dir ./models_test_full \
  --train-entry \
  --train-exit \
  --replay-test \
  --horizon 300 \
  --min-return 0.25 \
  --action-margin 0.08 \
  --fee-bps 8 \
  --max-gap-sec 5 \
  --min-episode-rows 900 \
  --max-rows 0 \
  --val-ratio 0.25 \
  --n-estimators 400 \
  --max-depth 4 \
  --learning-rate 0.035 \
  --initial-capital 200 \
  --fixed-margin 2 \
  --sim-leverage 8 \
  --entry-threshold 0.56 \
  --exit-threshold 0.55
"""

import argparse
import gzip
import json
import math
import os
import time
from collections import Counter, defaultdict, deque
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from sklearn.metrics import (
    accuracy_score,
    classification_report,
    confusion_matrix,
    precision_recall_fscore_support,
)

try:
    from xgboost import XGBClassifier
except Exception as e:
    print("❌ Thiếu xgboost. Cài bằng: pip install xgboost")
    raise e


# ============================================================
# 1. FEATURE LIST
# ============================================================
# Lưu ý:
# Các tên feature này phải khớp với OrderManager V21.
# Không tạo feature quá lạ mà OrderManager không build được.
# ============================================================

LEGACY_13_FEATURES = [
    "ob_imb_top20",
    "spread_close",
    "bid_vol_1pct",
    "ask_vol_1pct",
    "max_buy_trade",
    "max_sell_trade",
    "liq_long_vol",
    "liq_short_vol",
    "funding_rate",
    "taker_buy_ratio",
    "body_size",
    "wick_size",
    "btc_relative_strength",
]

ENHANCED_MARKET_FEATURES = [
    *LEGACY_13_FEATURES,

    "mark_price",
    "mark_price_ma",
    "index_price",
    "spread_bps",

    "coin_pct_local",
    "btc_pct_local",
    "local_quote_volume",
    "local_buy_quote",
    "local_sell_quote",
    "local_trade_count",
    "local_taker_buy_ratio",
    "taker_buy_ratio_24h",
    "trade_flow_imbalance",

    "max_buy_trade_usdt",
    "max_sell_trade_usdt",
    "max_buy_trade_local_norm",
    "max_sell_trade_local_norm",

    "liq_long_usdt",
    "liq_short_usdt",
    "liq_long_local_norm",
    "liq_short_local_norm",

    "ATR14",
    "VPIN",
    "OFI",
    "MFA",
    "WHALE_NET",
    "WHALE_NET_LOCAL",

    "price_age_ms",
    "depth_age_ms",
    "trade_age_ms",
    "mark_price_age_ms",
]

POSITION_EXTRA_FEATURES = [
    "position_side_long",
    "position_side_short",
    "position_roi",
    "position_best_roi",
    "position_worst_roi",
    "position_giveback_roi",
    "position_hold_sec",
    "position_leverage",
    "position_entry_distance_pct",
]

ENHANCED_POSITION_FEATURES = [
    *ENHANCED_MARKET_FEATURES,
    *POSITION_EXTRA_FEATURES,
]

ENTRY_LABEL_NAMES = {
    0: "FLAT",
    1: "LONG",
    2: "SHORT",
}

EXIT_LABEL_NAMES = {
    0: "HOLD",
    1: "EXIT",
}


# ============================================================
# 2. UTILS
# ============================================================

def now_ms() -> int:
    return int(time.time() * 1000)


def safe_float(value: Any, fallback: float = 0.0) -> float:
    try:
        if value is None:
            return fallback
        x = float(value)
        if not math.isfinite(x):
            return fallback
        return x
    except Exception:
        return fallback


def safe_int(value: Any, fallback: int = 0) -> int:
    try:
        if value is None:
            return fallback
        return int(float(value))
    except Exception:
        return fallback


def timestamp_to_ms(value: Any) -> int:
    n = safe_float(value, 0.0)
    if n <= 0:
        return 0
    if n < 1e12:
        return int(n * 1000)
    return int(n)


def normalize_symbol(value: Any) -> str:
    return str(value or "").upper().strip()


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def pct_return(entry_price: float, current_price: float, side: str) -> float:
    if entry_price <= 0 or current_price <= 0:
        return 0.0

    if side == "LONG":
        return (current_price - entry_price) / entry_price * 100.0

    return (entry_price - current_price) / entry_price * 100.0


def list_data_files(data_dir: Path) -> List[Path]:
    patterns = ["*.jsonl.gz", "*.json.gz", "*.jsonl", "*.json"]
    files = []

    for pattern in patterns:
        files.extend(data_dir.rglob(pattern))

    # Loại manifest khỏi dữ liệu train.
    files = [
        f for f in files
        if "_manifest" not in f.name.lower()
        and not f.name.endswith(".tmp")
    ]

    return sorted(files)


def json_dumps(obj: Dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"), default=str)


def save_json(path: Path, obj: Dict[str, Any]):
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2, default=str)


def append_jsonl(path: Path, obj: Dict[str, Any]):
    ensure_dir(path.parent)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json_dumps(obj))
        f.write("\n")


# ============================================================
# 3. ROBUST JSONL / GZIP READER
# ============================================================
# Đọc được cả gzip thiếu EOF marker do file copy khi recorder đang mở.
# Dữ liệu lỗi dòng nào bỏ dòng đó, không dừng cả batch.
# ============================================================

def iter_json_lines_from_file(path: Path) -> Iterable[Dict[str, Any]]:
    name = path.name.lower()

    if name.endswith(".gz"):
        try:
            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
                while True:
                    try:
                        line = f.readline()
                    except EOFError:
                        break
                    except Exception:
                        break

                    if not line:
                        break

                    line = line.strip()
                    if not line:
                        continue

                    try:
                        yield json.loads(line)
                    except Exception:
                        continue
        except Exception:
            return

    else:
        try:
            with open(path, "rt", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        yield json.loads(line)
                    except Exception:
                        continue
        except Exception:
            return


# ============================================================
# 4. SCHEMA NORMALIZATION
# ============================================================

def get_feature_ts(record: Dict[str, Any]) -> int:
    """
    Thứ tự ưu tiên timestamp:
    1. _snapshot_ts: recorder snapshot time
    2. feature_ts: feed feature time
    3. _redis_stream_ms: thời điểm XADD vào Redis Stream
    4. _recorder_ts / timestamp / ts...
    5. _exported_at_ms chỉ là fallback cuối, vì đó là thời điểm export ra SSD
    """
    candidates = [
        record.get("_snapshot_ts"),
        record.get("feature_ts"),
        record.get("_redis_stream_ms"),
        record.get("_recorder_ts"),
        record.get("timestamp"),
        record.get("ts"),
        record.get("event_time"),
        record.get("last_update_ts"),
        record.get("_exported_at_ms"),
    ]

    for value in candidates:
        ts = timestamp_to_ms(value)
        if ts > 0:
            return ts

    return 0


def get_price(record: Dict[str, Any]) -> float:
    """
    Futures nên ưu tiên mark_price nếu có.
    Nếu thiếu thì fallback last_price hoặc mid bid/ask.
    """
    mark = safe_float(record.get("mark_price"), 0.0)
    if mark > 0:
        return mark

    last = safe_float(record.get("last_price"), 0.0)
    if last > 0:
        return last

    bid = safe_float(record.get("best_bid"), 0.0)
    ask = safe_float(record.get("best_ask"), 0.0)
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0

    return 0.0


def normalize_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(record, dict):
        return None

    symbol = normalize_symbol(record.get("symbol"))
    ts = get_feature_ts(record)
    price = get_price(record)

    if not symbol or ts <= 0 or price <= 0:
        return None

    clean = dict(record)
    clean["symbol"] = symbol
    clean["_ts"] = int(ts)
    clean["_price"] = float(price)

    # --------------------------------------------------------
    # Giá / spread
    # --------------------------------------------------------
    clean["last_price"] = safe_float(clean.get("last_price"), price)
    clean["mark_price"] = safe_float(clean.get("mark_price"), price)
    clean["mark_price_ma"] = safe_float(clean.get("mark_price_ma"), clean["mark_price"])
    clean["index_price"] = safe_float(clean.get("index_price"), clean["mark_price"])

    clean["best_bid"] = safe_float(clean.get("best_bid"), 0.0)
    clean["best_ask"] = safe_float(clean.get("best_ask"), 0.0)

    spread_close = safe_float(clean.get("spread_close"), 0.0)
    spread_bps = safe_float(clean.get("spread_bps"), spread_close * 10000.0)

    if spread_close <= 0 and spread_bps > 0:
        spread_close = spread_bps / 10000.0
    if spread_bps <= 0 and spread_close > 0:
        spread_bps = spread_close * 10000.0

    clean["spread_close"] = spread_close
    clean["spread_bps"] = spread_bps

    # --------------------------------------------------------
    # Taker / local flow
    # --------------------------------------------------------
    clean["local_taker_buy_ratio"] = safe_float(
        clean.get("local_taker_buy_ratio"),
        safe_float(clean.get("taker_buy_ratio"), 0.5)
    )
    clean["taker_buy_ratio"] = safe_float(clean.get("taker_buy_ratio"), clean["local_taker_buy_ratio"])
    clean["taker_buy_ratio_24h"] = safe_float(clean.get("taker_buy_ratio_24h"), clean["taker_buy_ratio"])

    clean["local_taker_buy_ratio"] = min(1.0, max(0.0, clean["local_taker_buy_ratio"]))
    clean["taker_buy_ratio"] = min(1.0, max(0.0, clean["taker_buy_ratio"]))
    clean["taker_buy_ratio_24h"] = min(1.0, max(0.0, clean["taker_buy_ratio_24h"]))

    clean["local_quote_volume"] = safe_float(clean.get("local_quote_volume"), 0.0)
    clean["local_buy_quote"] = safe_float(clean.get("local_buy_quote"), 0.0)
    clean["local_sell_quote"] = safe_float(clean.get("local_sell_quote"), 0.0)
    clean["local_trade_count"] = safe_float(clean.get("local_trade_count"), 0.0)

    if clean["local_quote_volume"] <= 0:
        clean["local_quote_volume"] = safe_float(clean.get("quote_volume"), 0.0)

    if clean["local_buy_quote"] <= 0 and clean["local_quote_volume"] > 0:
        clean["local_buy_quote"] = clean["local_quote_volume"] * clean["local_taker_buy_ratio"]

    if clean["local_sell_quote"] <= 0 and clean["local_quote_volume"] > 0:
        clean["local_sell_quote"] = max(0.0, clean["local_quote_volume"] - clean["local_buy_quote"])

    qv = clean["local_quote_volume"]
    if "trade_flow_imbalance" not in clean:
        if qv > 0:
            clean["trade_flow_imbalance"] = (clean["local_buy_quote"] - clean["local_sell_quote"]) / qv
        else:
            clean["trade_flow_imbalance"] = 0.0

    # --------------------------------------------------------
    # Whale / liquidation fallback
    # --------------------------------------------------------
    clean["liq_long_usdt"] = safe_float(
        clean.get("liq_long_usdt"),
        safe_float(clean.get("liq_long_vol"), 0.0) * price
    )
    clean["liq_short_usdt"] = safe_float(
        clean.get("liq_short_usdt"),
        safe_float(clean.get("liq_short_vol"), 0.0) * price
    )

    local_quote_safe = clean["local_quote_volume"] if clean["local_quote_volume"] > 0 else 1e-8

    clean["liq_long_local_norm"] = safe_float(
        clean.get("liq_long_local_norm"),
        clean["liq_long_usdt"] / local_quote_safe
    )
    clean["liq_short_local_norm"] = safe_float(
        clean.get("liq_short_local_norm"),
        clean["liq_short_usdt"] / local_quote_safe
    )

    clean["max_buy_trade_usdt"] = safe_float(
        clean.get("max_buy_trade_usdt"),
        safe_float(clean.get("max_buy_trade"), 0.0) * price
    )
    clean["max_sell_trade_usdt"] = safe_float(
        clean.get("max_sell_trade_usdt"),
        safe_float(clean.get("max_sell_trade"), 0.0) * price
    )

    clean["max_buy_trade_local_norm"] = safe_float(
        clean.get("max_buy_trade_local_norm"),
        clean["max_buy_trade_usdt"] / local_quote_safe
    )
    clean["max_sell_trade_local_norm"] = safe_float(
        clean.get("max_sell_trade_local_norm"),
        clean["max_sell_trade_usdt"] / local_quote_safe
    )

    # --------------------------------------------------------
    # Đảm bảo toàn bộ feature numeric tồn tại
    # --------------------------------------------------------
    for name in ENHANCED_MARKET_FEATURES:
        clean[name] = safe_float(clean.get(name), 0.0)

    return clean


# ============================================================
# 5. LOAD DATA
# ============================================================

def load_records(data_dir: Path, max_rows: int = 0, verbose_every: int = 250_000) -> pd.DataFrame:
    files = list_data_files(data_dir)
    if not files:
        raise FileNotFoundError(f"Không tìm thấy file dữ liệu trong {data_dir}")

    print("")
    print("============================================================")
    print("📥 LOAD DATA")
    print("============================================================")
    print(f"data_dir={data_dir}")
    print(f"files_found={len(files)}")
    print(f"max_rows={max_rows if max_rows > 0 else 'NO LIMIT'}")

    rows = []
    total_raw = 0
    total_valid = 0

    for idx, file_path in enumerate(files, 1):
        for record in iter_json_lines_from_file(file_path):
            total_raw += 1
            clean = normalize_record(record)
            if clean is None:
                continue

            rows.append(clean)
            total_valid += 1

            if verbose_every and total_valid % verbose_every == 0:
                print(f"📥 loaded_valid={total_valid:,} raw={total_raw:,} current_file={file_path.name}")

            if max_rows and total_valid >= max_rows:
                break

        if idx % 50 == 0:
            print(f"📄 scanned_files={idx}/{len(files)} valid={total_valid:,}")

        if max_rows and total_valid >= max_rows:
            break

    if not rows:
        raise RuntimeError("Không có dòng dữ liệu hợp lệ để train")

    df = pd.DataFrame(rows)
    df["_ts"] = df["_ts"].astype(np.int64)
    df["_price"] = df["_price"].astype(float)
    df["symbol"] = df["symbol"].astype(str)

    df = df.sort_values(["symbol", "_ts"]).reset_index(drop=True)

    # Dedup chống trùng khi export chạy lại hoặc có batch ghi lại.
    before = len(df)
    df = df.drop_duplicates(subset=["symbol", "_ts"], keep="last").reset_index(drop=True)
    after = len(df)

    if before != after:
        print(f"🧹 dedup_removed={before - after:,} by symbol+timestamp")

    print("✅ LOAD DONE")
    print(f"rows={len(df):,}")
    print(f"symbols={df['symbol'].nunique()}")
    print(f"start_ts={int(df['_ts'].min())}")
    print(f"end_ts={int(df['_ts'].max())}")
    print("============================================================")
    print("")

    return df


# ============================================================
# 6. EPISODE SPLIT
# ============================================================

def assign_episodes(df: pd.DataFrame, max_gap_sec: float, min_episode_rows: int) -> pd.DataFrame:
    print("")
    print("============================================================")
    print("🧩 SPLIT EPISODES")
    print("============================================================")
    print(f"max_gap_sec={max_gap_sec}")
    print(f"min_episode_rows={min_episode_rows}")

    df = df.sort_values(["symbol", "_ts"]).copy()
    max_gap_ms = int(max_gap_sec * 1000)

    episode_ids = np.full(len(df), -1, dtype=np.int64)
    episode_id = 0

    for symbol, group_idx in df.groupby("symbol").groups.items():
        idxs = np.array(list(group_idx), dtype=np.int64)
        ts = df.loc[idxs, "_ts"].values

        if len(idxs) == 0:
            continue

        segment_start = 0

        for i in range(1, len(idxs)):
            gap = ts[i] - ts[i - 1]

            if gap > max_gap_ms:
                segment_idxs = idxs[segment_start:i]
                if len(segment_idxs) >= min_episode_rows:
                    episode_ids[segment_idxs] = episode_id
                    episode_id += 1
                segment_start = i

        segment_idxs = idxs[segment_start:]
        if len(segment_idxs) >= min_episode_rows:
            episode_ids[segment_idxs] = episode_id
            episode_id += 1

    df["episode_id"] = episode_ids
    df = df[df["episode_id"] >= 0].copy().reset_index(drop=True)

    if df.empty:
        raise RuntimeError("Không có episode đủ dài. Giảm --min-episode-rows hoặc kiểm tra dữ liệu.")

    ep_stats = df.groupby("episode_id").agg(
        symbol=("symbol", "first"),
        rows=("_ts", "count"),
        start_ts=("_ts", "min"),
        end_ts=("_ts", "max")
    )
    ep_stats["duration_min"] = (ep_stats["end_ts"] - ep_stats["start_ts"]) / 60000.0

    print("✅ EPISODE DONE")
    print(f"episodes={len(ep_stats):,}")
    print(f"rows_kept={len(df):,}")
    print(f"symbols={df['symbol'].nunique()}")
    print(f"avg_rows={ep_stats['rows'].mean():.1f}")
    print(f"avg_duration_min={ep_stats['duration_min'].mean():.1f}")
    print("top episode symbols:")
    print(ep_stats["symbol"].value_counts().head(10).to_string())
    print("============================================================")
    print("")

    return df


# ============================================================
# 7. FUTURE WINDOW
# ============================================================

def future_max_min(values: np.ndarray, horizon: int) -> Tuple[np.ndarray, np.ndarray]:
    """
    Với mỗi i, lấy max/min trong tương lai i+1 -> i+horizon.
    Dùng deque O(n).
    """
    n = len(values)
    future_max = np.full(n, np.nan, dtype=np.float64)
    future_min = np.full(n, np.nan, dtype=np.float64)

    maxdq = deque()
    mindq = deque()

    for i in range(n - 1, -1, -1):
        right = i + horizon

        while maxdq and maxdq[0] > right:
            maxdq.popleft()
        while mindq and mindq[0] > right:
            mindq.popleft()

        if maxdq:
            future_max[i] = values[maxdq[0]]
        if mindq:
            future_min[i] = values[mindq[0]]

        while maxdq and values[maxdq[-1]] <= values[i]:
            maxdq.pop()
        maxdq.append(i)

        while mindq and values[mindq[-1]] >= values[i]:
            mindq.pop()
        mindq.append(i)

    return future_max, future_min


# ============================================================
# 8. ENTRY DATASET
# ============================================================

def build_entry_dataset(
    df: pd.DataFrame,
    horizon: int,
    min_return: float,
    fee_bps: float,
    action_margin: float,
    sample_flat_ratio: float,
    random_seed: int
) -> Tuple[pd.DataFrame, pd.Series, pd.Series, pd.DataFrame]:

    rng = np.random.default_rng(random_seed)
    fee_pct = fee_bps / 100.0

    print("")
    print("============================================================")
    print("🏷️ BUILD ENTRY DATASET")
    print("============================================================")
    print(f"horizon={horizon}s")
    print(f"min_return={min_return}%")
    print(f"action_margin={action_margin}%")
    print(f"fee_bps={fee_bps} => fee_pct={fee_pct}%")

    parts = []

    for ep_id, ep in df.groupby("episode_id"):
        ep = ep.sort_values("_ts").copy().reset_index(drop=True)
        prices = ep["_price"].values.astype(np.float64)

        if len(prices) <= horizon + 5:
            continue

        fmax, fmin = future_max_min(prices, horizon)

        long_reward = (fmax - prices) / prices * 100.0 - fee_pct
        short_reward = (prices - fmin) / prices * 100.0 - fee_pct

        labels = np.zeros(len(ep), dtype=np.int64)

        long_ok = (long_reward >= min_return) & (long_reward >= short_reward + action_margin)
        short_ok = (short_reward >= min_return) & (short_reward >= long_reward + action_margin)

        labels[long_ok] = 1
        labels[short_ok] = 2

        valid = np.isfinite(long_reward) & np.isfinite(short_reward)

        ep = ep[valid].copy()
        ep["entry_label"] = labels[valid]
        ep["oracle_long_reward"] = long_reward[valid]
        ep["oracle_short_reward"] = short_reward[valid]
        ep["oracle_best_reward"] = np.maximum(long_reward[valid], short_reward[valid])

        parts.append(ep)

    if not parts:
        raise RuntimeError("Không tạo được entry dataset")

    data = pd.concat(parts, ignore_index=True)

    # Downsample FLAT để train nhanh hơn nhưng không làm não FOMO.
    # sample_flat_ratio = 3 nghĩa là giữ FLAT tối đa ~3 lần số non-flat.
    if 0 < sample_flat_ratio < 999:
        flat = data[data["entry_label"] == 0]
        non_flat = data[data["entry_label"] != 0]

        if len(flat) > 0 and len(non_flat) > 0:
            keep_flat_n = int(min(len(flat), max(len(non_flat) * sample_flat_ratio, 1000)))
            keep_idx = rng.choice(flat.index.values, size=keep_flat_n, replace=False)
            data = pd.concat([flat.loc[keep_idx], non_flat], ignore_index=True)
            data = data.sample(frac=1, random_state=random_seed).reset_index(drop=True)

    X = data[ENHANCED_MARKET_FEATURES].astype(np.float32)
    y = data["entry_label"].astype(np.int64)
    weights = build_class_weights(y, task="entry")

    dist = Counter(y.tolist())
    print("✅ ENTRY DATASET DONE")
    print(f"rows={len(data):,}")
    print(f"dist={dict(dist)}")
    for k in sorted(dist):
        print(f"  {ENTRY_LABEL_NAMES[k]}: {dist[k]:,} ({dist[k] / len(y) * 100:.2f}%)")
    print("============================================================")
    print("")

    return X, y, weights, data


# ============================================================
# 9. EXIT DATASET
# ============================================================

def build_exit_dataset(
    df: pd.DataFrame,
    horizon: int,
    min_return: float,
    fee_bps: float,
    max_hold_sec: int,
    entry_stride: int,
    random_seed: int,
    max_positions_per_episode: int
) -> Tuple[pd.DataFrame, pd.Series, pd.Series, pd.DataFrame]:

    rng = np.random.default_rng(random_seed)
    fee_pct = fee_bps / 100.0

    print("")
    print("============================================================")
    print("🏷️ BUILD EXIT DATASET")
    print("============================================================")
    print(f"horizon={horizon}s")
    print(f"min_return={min_return}%")
    print(f"max_hold_sec={max_hold_sec}")
    print(f"entry_stride={entry_stride}")
    print(f"fee_bps={fee_bps}")

    rows = []

    for ep_id, ep in df.groupby("episode_id"):
        ep = ep.sort_values("_ts").copy().reset_index(drop=True)
        prices = ep["_price"].values.astype(np.float64)

        if len(ep) <= horizon + 20:
            continue

        fmax, fmin = future_max_min(prices, horizon)

        long_reward = (fmax - prices) / prices * 100.0 - fee_pct
        short_reward = (prices - fmin) / prices * 100.0 - fee_pct

        candidates = []

        for i in range(0, len(ep) - horizon - 1, max(1, entry_stride)):
            if not np.isfinite(long_reward[i]) or not np.isfinite(short_reward[i]):
                continue

            if long_reward[i] >= min_return and long_reward[i] >= short_reward[i]:
                candidates.append((i, "LONG", long_reward[i]))
            elif short_reward[i] >= min_return and short_reward[i] > long_reward[i]:
                candidates.append((i, "SHORT", short_reward[i]))

        if not candidates:
            continue

        if max_positions_per_episode > 0 and len(candidates) > max_positions_per_episode:
            selected = rng.choice(len(candidates), size=max_positions_per_episode, replace=False)
            candidates = [candidates[i] for i in selected]

        for entry_i, side, entry_reward in candidates:
            entry_price = prices[entry_i]
            if entry_price <= 0:
                continue

            max_steps = min(max_hold_sec, len(ep) - entry_i - horizon - 1)
            if max_steps <= 5:
                continue

            best_roi = -999.0
            worst_roi = 999.0

            for step in range(1, max_steps):
                j = entry_i + step
                current_price = prices[j]
                if current_price <= 0:
                    continue

                current_roi = pct_return(entry_price, current_price, side) - fee_pct

                best_roi = max(best_roi, current_roi)
                worst_roi = min(worst_roi, current_roi)

                if side == "LONG":
                    future_best_roi = pct_return(entry_price, fmax[j], "LONG") - fee_pct
                else:
                    future_best_roi = pct_return(entry_price, fmin[j], "SHORT") - fee_pct

                if not np.isfinite(future_best_roi):
                    continue

                giveback = best_roi - current_roi
                hold_sec = (int(ep.loc[j, "_ts"]) - int(ep.loc[entry_i, "_ts"])) / 1000.0
                improvement = future_best_roi - current_roi

                # Label EXIT:
                # 1. Đang lời đủ nhưng tương lai không còn cải thiện rõ.
                # 2. Giveback lớn.
                # 3. Đang lỗ và giữ tiếp không cho edge tốt.
                # 4. Gần hết max hold.
                exit_label = 0

                if current_roi >= min_return and improvement < 0.05:
                    exit_label = 1
                if giveback >= max(0.15, min_return * 0.75):
                    exit_label = 1
                if current_roi <= -min_return and improvement < min_return:
                    exit_label = 1
                if step >= max_steps - 2:
                    exit_label = 1

                row = ep.loc[j].copy()

                row["position_side_long"] = 1.0 if side == "LONG" else 0.0
                row["position_side_short"] = 1.0 if side == "SHORT" else 0.0
                row["position_roi"] = current_roi
                row["position_best_roi"] = best_roi
                row["position_worst_roi"] = worst_roi
                row["position_giveback_roi"] = giveback
                row["position_hold_sec"] = hold_sec
                row["position_leverage"] = 1.0
                row["position_entry_distance_pct"] = pct_return(entry_price, current_price, side)
                row["exit_label"] = exit_label
                row["exit_future_best_roi"] = future_best_roi
                row["exit_improvement"] = improvement
                row["sim_entry_side"] = side
                row["sim_entry_index"] = entry_i

                rows.append(row)

    if not rows:
        raise RuntimeError("Không tạo được exit dataset")

    data = pd.DataFrame(rows)

    for name in ENHANCED_POSITION_FEATURES:
        if name not in data.columns:
            data[name] = 0.0
        data[name] = data[name].apply(lambda x: safe_float(x, 0.0))

    X = data[ENHANCED_POSITION_FEATURES].astype(np.float32)
    y = data["exit_label"].astype(np.int64)
    weights = build_class_weights(y, task="exit")

    dist = Counter(y.tolist())
    print("✅ EXIT DATASET DONE")
    print(f"rows={len(data):,}")
    print(f"dist={dict(dist)}")
    for k in sorted(dist):
        print(f"  {EXIT_LABEL_NAMES[k]}: {dist[k]:,} ({dist[k] / len(y) * 100:.2f}%)")
    print("============================================================")
    print("")

    return X, y, weights, data


# ============================================================
# 10. CLASS WEIGHT
# ============================================================

def build_class_weights(y: pd.Series, task: str) -> pd.Series:
    """
    Không dùng balanced weight quá mạnh vì dễ làm EntryBrain FOMO.
    Entry:
      FLAT cần trọng số đủ cao để não biết đứng ngoài.
    Exit:
      EXIT tăng nhẹ nhưng không ép thoát quá nhiều.
    """
    if task == "entry":
        manual = {
            0: 1.00,
            1: 1.35,
            2: 1.35,
        }
        return y.map(lambda cls: manual.get(int(cls), 1.0)).astype(np.float32)

    if task == "exit":
        manual = {
            0: 1.00,
            1: 1.25,
        }
        return y.map(lambda cls: manual.get(int(cls), 1.0)).astype(np.float32)

    return pd.Series(np.ones(len(y), dtype=np.float32), index=y.index)


# ============================================================
# 11. TRAIN / EVAL
# ============================================================

def train_val_split_by_time(X: pd.DataFrame, y: pd.Series, w: pd.Series, val_ratio: float):
    n = len(X)
    split = int(n * (1.0 - val_ratio))
    split = max(1, min(split, n - 1))

    return (
        X.iloc[:split].copy(),
        X.iloc[split:].copy(),
        y.iloc[:split].copy(),
        y.iloc[split:].copy(),
        w.iloc[:split].copy(),
        w.iloc[split:].copy(),
    )


def make_xgb_classifier(
    num_class: int,
    args,
):
    if num_class == 2:
        return XGBClassifier(
            objective="binary:logistic",
            eval_metric="logloss",
            tree_method="hist",
            max_depth=args.max_depth,
            n_estimators=args.n_estimators,
            learning_rate=args.learning_rate,
            subsample=args.subsample,
            colsample_bytree=args.colsample_bytree,
            reg_lambda=args.reg_lambda,
            reg_alpha=args.reg_alpha,
            n_jobs=args.n_jobs,
            random_state=args.seed,
        )

    return XGBClassifier(
        objective="multi:softprob",
        num_class=num_class,
        eval_metric="mlogloss",
        tree_method="hist",
        max_depth=args.max_depth,
        n_estimators=args.n_estimators,
        learning_rate=args.learning_rate,
        subsample=args.subsample,
        colsample_bytree=args.colsample_bytree,
        reg_lambda=args.reg_lambda,
        reg_alpha=args.reg_alpha,
        n_jobs=args.n_jobs,
        random_state=args.seed,
    )


def evaluate_classifier(model, X_val: pd.DataFrame, y_val: pd.Series, label_names: Dict[int, str], title: str) -> Dict[str, Any]:
    pred = model.predict(X_val)
    proba = model.predict_proba(X_val)

    acc = accuracy_score(y_val, pred)
    labels = sorted(label_names.keys())
    target_names = [label_names[i] for i in labels]

    report_text = classification_report(
        y_val,
        pred,
        labels=labels,
        target_names=target_names,
        zero_division=0
    )

    cm = confusion_matrix(y_val, pred, labels=labels)

    precision, recall, f1, support = precision_recall_fscore_support(
        y_val,
        pred,
        labels=labels,
        zero_division=0
    )

    class_metrics = {}
    for i, cls in enumerate(labels):
        class_metrics[label_names[cls]] = {
            "precision": float(precision[i]),
            "recall": float(recall[i]),
            "f1": float(f1[i]),
            "support": int(support[i]),
        }

    pred_dist = Counter(pred.tolist())
    true_dist = Counter(y_val.tolist())

    print("")
    print(f"================ {title} CLASSIFICATION EVALUATION ================")
    print(f"accuracy={acc:.4f}")
    print("true_distribution:")
    for cls in labels:
        print(f"  {label_names[cls]}: {true_dist.get(cls, 0):,}")
    print("pred_distribution:")
    for cls in labels:
        print(f"  {label_names[cls]}: {pred_dist.get(cls, 0):,}")
    print("")
    print(report_text)
    print("confusion_matrix:")
    print(cm)
    print("==================================================================")
    print("")

    avg_conf = float(np.mean(np.max(proba, axis=1))) if len(proba) else 0.0

    return {
        "title": title,
        "accuracy": float(acc),
        "true_distribution": {label_names.get(k, str(k)): int(v) for k, v in true_dist.items()},
        "pred_distribution": {label_names.get(k, str(k)): int(v) for k, v in pred_dist.items()},
        "class_metrics": class_metrics,
        "avg_confidence": avg_conf,
        "confusion_matrix": cm.tolist(),
        "classification_report": report_text,
    }


def train_model(
    X: pd.DataFrame,
    y: pd.Series,
    weights: pd.Series,
    label_names: Dict[int, str],
    args,
    title: str,
):
    X_train, X_val, y_train, y_val, w_train, w_val = train_val_split_by_time(X, y, weights, args.val_ratio)

    model = make_xgb_classifier(num_class=len(label_names), args=args)

    print("")
    print("============================================================")
    print(f"🚀 TRAIN {title}")
    print("============================================================")
    print(f"train_rows={len(X_train):,}")
    print(f"val_rows={len(X_val):,}")
    print(f"features={X.shape[1]}")
    print(f"classes={len(label_names)}")
    print(f"n_estimators={args.n_estimators}")
    print(f"max_depth={args.max_depth}")
    print(f"learning_rate={args.learning_rate}")
    print("============================================================")
    print("")

    model.fit(
        X_train,
        y_train,
        sample_weight=w_train,
        eval_set=[(X_val, y_val)],
        verbose=False
    )

    evaluation = evaluate_classifier(model, X_val, y_val, label_names, title)

    return model, evaluation


# ============================================================
# 12. EXPORT MODEL
# ============================================================

def save_feature_list(path: Path, features: List[str]):
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(features, f, ensure_ascii=False, indent=2)


def export_xgboost_json(model, path: Path):
    ensure_dir(path.parent)
    model.save_model(str(path))
    print(f"✅ saved XGBoost JSON: {path}")


def export_onnx(model, path: Path, n_features: int) -> bool:
    """
    Fix lỗi onnxmltools yêu cầu feature names dạng f0, f1, f2...
    """
    try:
        import onnxmltools
        from onnxmltools.convert.common.data_types import FloatTensorType

        booster = model.get_booster()
        booster.feature_names = [f"f{i}" for i in range(n_features)]
        booster.feature_types = ["float"] * n_features

        initial_types = [("float_input", FloatTensorType([None, n_features]))]

        onnx_model = onnxmltools.convert_xgboost(
            model,
            initial_types=initial_types,
            target_opset=15
        )

        ensure_dir(path.parent)
        with open(path, "wb") as f:
            f.write(onnx_model.SerializeToString())

        print(f"✅ exported ONNX: {path}")
        return True

    except Exception as e:
        print(f"⚠️ ONNX export failed for {path.name}: {e}")
        print("   JSON model vẫn đã lưu. Kiểm tra onnxmltools/onnx nếu cần.")
        return False


# ============================================================
# 13. REPLAY BACKTEST
# ============================================================

def build_single_market_vector(row: pd.Series) -> pd.DataFrame:
    data = {name: safe_float(row.get(name), 0.0) for name in ENHANCED_MARKET_FEATURES}
    return pd.DataFrame([data], columns=ENHANCED_MARKET_FEATURES).astype(np.float32)


def make_position_features(row: pd.Series, position: Dict[str, Any]) -> Dict[str, float]:
    price = safe_float(row.get("_price"), 0.0)
    entry_price = safe_float(position.get("entry_price"), price)
    side = position.get("side", "LONG")
    leverage = safe_float(position.get("leverage"), 1.0)

    price_return_pct = pct_return(entry_price, price, side)
    roi = price_return_pct * leverage

    best_roi = max(safe_float(position.get("best_roi"), roi), roi)
    worst_roi = min(safe_float(position.get("worst_roi"), roi), roi)

    hold_sec = max(0.0, (safe_int(row.get("_ts"), 0) - safe_int(position.get("entry_ts"), 0)) / 1000.0)

    return {
        "position_side_long": 1.0 if side == "LONG" else 0.0,
        "position_side_short": 1.0 if side == "SHORT" else 0.0,
        "position_roi": roi,
        "position_best_roi": best_roi,
        "position_worst_roi": worst_roi,
        "position_giveback_roi": best_roi - roi,
        "position_hold_sec": hold_sec,
        "position_leverage": leverage,
        "position_entry_distance_pct": price_return_pct,
    }


def build_single_exit_vector(row: pd.Series, position: Dict[str, Any]) -> pd.DataFrame:
    pos_features = make_position_features(row, position)
    data = {}

    for name in ENHANCED_POSITION_FEATURES:
        if name in pos_features:
            data[name] = pos_features[name]
        else:
            data[name] = safe_float(row.get(name), 0.0)

    return pd.DataFrame([data], columns=ENHANCED_POSITION_FEATURES).astype(np.float32)


def close_sim_position(position: Dict[str, Any], row: pd.Series, reason: str, args) -> Dict[str, Any]:
    exit_price = safe_float(row.get("_price"), 0.0)
    entry_price = safe_float(position.get("entry_price"), 0.0)
    side = position.get("side", "LONG")
    leverage = safe_float(position.get("leverage"), args.sim_leverage)

    gross_price_return_pct = pct_return(entry_price, exit_price, side)

    # fee_bps ở đây hiểu là phí + slippage round-trip tính theo giá.
    fee_pct = args.fee_bps / 100.0

    gross_roi = gross_price_return_pct * leverage
    fee_roi = fee_pct * leverage
    net_roi = gross_roi - fee_roi

    margin = args.fixed_margin
    pnl_usdt = margin * net_roi / 100.0

    hold_sec = max(0.0, (safe_int(row.get("_ts"), 0) - safe_int(position.get("entry_ts"), 0)) / 1000.0)

    return {
        "symbol": position.get("symbol"),
        "side": side,
        "entry_ts": position.get("entry_ts"),
        "exit_ts": safe_int(row.get("_ts"), 0),
        "entry_price": entry_price,
        "exit_price": exit_price,
        "gross_price_return_pct": gross_price_return_pct,
        "gross_roi": gross_roi,
        "fee_roi": fee_roi,
        "net_roi": net_roi,
        "pnl_usdt": pnl_usdt,
        "hold_sec": hold_sec,
        "reason": reason,
        "best_roi": safe_float(position.get("best_roi"), 0.0),
        "worst_roi": safe_float(position.get("worst_roi"), 0.0),
        "entry_prob": safe_float(position.get("prob"), 0.0),
    }


def summarize_trades(trades: List[Dict[str, Any]], capital_start: float, label: str) -> Dict[str, Any]:
    if not trades:
        return {
            "label": label,
            "trades": 0,
            "capital_start": capital_start,
            "capital_end": capital_start,
            "net_profit_usdt": 0.0,
            "roi_on_capital_pct": 0.0,
            "winrate": 0.0,
            "profit_factor": 0.0,
            "long_trades": 0,
            "short_trades": 0,
            "long_ratio": 0.0,
            "short_ratio": 0.0,
            "avg_net_roi": 0.0,
            "avg_hold_sec": 0.0,
            "max_win_usdt": 0.0,
            "max_loss_usdt": 0.0,
            "gross_win_usdt": 0.0,
            "gross_loss_usdt": 0.0,
        }

    pnls = np.array([safe_float(t.get("pnl_usdt"), 0.0) for t in trades], dtype=float)
    rois = np.array([safe_float(t.get("net_roi"), 0.0) for t in trades], dtype=float)
    holds = np.array([safe_float(t.get("hold_sec"), 0.0) for t in trades], dtype=float)

    wins = pnls[pnls > 0]
    losses = pnls[pnls <= 0]

    gross_win = float(wins.sum()) if len(wins) else 0.0
    gross_loss = float(abs(losses.sum())) if len(losses) else 0.0

    net_profit = float(pnls.sum())
    capital_end = capital_start + net_profit

    long_count = sum(1 for t in trades if t.get("side") == "LONG")
    short_count = sum(1 for t in trades if t.get("side") == "SHORT")

    return {
        "label": label,
        "trades": int(len(trades)),
        "capital_start": float(capital_start),
        "capital_end": float(capital_end),
        "net_profit_usdt": net_profit,
        "roi_on_capital_pct": float(net_profit / capital_start * 100.0) if capital_start > 0 else 0.0,
        "winrate": float(len(wins) / len(trades)),
        "profit_factor": float(gross_win / gross_loss) if gross_loss > 0 else 999.0,
        "long_trades": int(long_count),
        "short_trades": int(short_count),
        "long_ratio": float(long_count / len(trades)),
        "short_ratio": float(short_count / len(trades)),
        "avg_net_roi": float(rois.mean()) if len(rois) else 0.0,
        "median_net_roi": float(np.median(rois)) if len(rois) else 0.0,
        "avg_hold_sec": float(holds.mean()) if len(holds) else 0.0,
        "max_win_usdt": float(pnls.max()) if len(pnls) else 0.0,
        "max_loss_usdt": float(pnls.min()) if len(pnls) else 0.0,
        "gross_win_usdt": gross_win,
        "gross_loss_usdt": gross_loss,
    }


def replay_backtest_dual_brain(
    df: pd.DataFrame,
    entry_model,
    exit_model,
    args,
    output_dir: Path
) -> Dict[str, Any]:

    print("")
    print("============================================================")
    print("🎮 FULL LIFECYCLE REPLAY BACKTEST")
    print("============================================================")
    print("Mô phỏng:")
    print("EntryBrain: FLAT/LONG/SHORT")
    print("ExitBrain : HOLD/EXIT")
    print("Có phí/slippage, margin, leverage, emergency stop, max hold")
    print("============================================================")

    df = df.sort_values(["_ts", "symbol"]).copy().reset_index(drop=True)

    split = int(len(df) * (1.0 - args.val_ratio))
    split = max(0, min(split, len(df) - 1))

    test_df = df.iloc[split:].copy().reset_index(drop=True)
    if test_df.empty:
        print("⚠️ replay skipped: test_df empty")
        return {}

    print(f"test_rows={len(test_df):,}")
    print(f"val_ratio={args.val_ratio}")
    print(f"initial_capital={args.initial_capital}")
    print(f"fixed_margin={args.fixed_margin}")
    print(f"sim_leverage={args.sim_leverage}")
    print(f"fee_bps={args.fee_bps}")
    print(f"entry_threshold={args.entry_threshold}")
    print(f"exit_threshold={args.exit_threshold}")
    print(f"min_exit_gap={args.min_exit_gap}")
    print("============================================================")
    print("")

    capital = float(args.initial_capital)
    open_positions: Dict[str, Dict[str, Any]] = {}
    closed_trades: List[Dict[str, Any]] = []
    progress_reports: List[Dict[str, Any]] = []

    trade_path = output_dir / "replay_trades.jsonl"
    if trade_path.exists():
        trade_path.unlink()

    total_rows = len(test_df)
    next_progress = args.progress_step
    progress_step = args.progress_step

    entry_signals = Counter()
    exit_reasons = Counter()

    for i, row in test_df.iterrows():
        symbol = normalize_symbol(row.get("symbol"))
        ts = safe_int(row.get("_ts"), 0)
        price = safe_float(row.get("_price"), 0.0)

        if not symbol or ts <= 0 or price <= 0:
            continue

        # ----------------------------------------------------
        # 1. Quản lý lệnh đang mở
        # ----------------------------------------------------
        if symbol in open_positions:
            pos = open_positions[symbol]
            pos_features = make_position_features(row, pos)

            pos["best_roi"] = pos_features["position_best_roi"]
            pos["worst_roi"] = pos_features["position_worst_roi"]

            should_close = False
            close_reason = ""

            current_roi = pos_features["position_roi"]
            hold_sec = pos_features["position_hold_sec"]

            if current_roi <= args.sim_emergency_stop_roi:
                should_close = True
                close_reason = f"SIM_EMERGENCY_STOP_{current_roi:.2f}"

            elif hold_sec >= args.sim_max_hold_sec:
                should_close = True
                close_reason = "SIM_MAX_HOLD"

            elif exit_model is not None and hold_sec >= args.sim_min_hold_exit_sec:
                X_exit_one = build_single_exit_vector(row, pos)
                p_exit_arr = exit_model.predict_proba(X_exit_one)[0]

                if len(p_exit_arr) >= 2:
                    p_hold = float(p_exit_arr[0])
                    p_exit = float(p_exit_arr[1])

                    if p_exit >= args.exit_threshold and (p_exit - p_hold) >= args.min_exit_gap:
                        should_close = True
                        close_reason = f"SIM_EXIT_BRAIN_{p_exit:.3f}"

            if should_close:
                trade = close_sim_position(pos, row, close_reason, args)
                closed_trades.append(trade)
                capital += trade["pnl_usdt"]
                exit_reasons[close_reason.split("_")[1] if "_" in close_reason else close_reason] += 1
                append_jsonl(trade_path, trade)
                del open_positions[symbol]

        # ----------------------------------------------------
        # 2. Xét vào lệnh nếu symbol chưa có position
        # ----------------------------------------------------
        if symbol not in open_positions:
            X_entry_one = build_single_market_vector(row)
            p_entry = entry_model.predict_proba(X_entry_one)[0]

            if len(p_entry) >= 3:
                p_flat = float(p_entry[0])
                p_long = float(p_entry[1])
                p_short = float(p_entry[2])

                sorted_probs = sorted([p_flat, p_long, p_short], reverse=True)
                gap = sorted_probs[0] - sorted_probs[1]

                side = None
                prob = 0.0

                if (
                    p_long >= args.entry_threshold
                    and p_long > p_short
                    and p_long > p_flat
                    and gap >= args.action_margin
                ):
                    side = "LONG"
                    prob = p_long

                elif (
                    p_short >= args.entry_threshold
                    and p_short > p_long
                    and p_short > p_flat
                    and gap >= args.action_margin
                ):
                    side = "SHORT"
                    prob = p_short

                else:
                    entry_signals["FLAT"] += 1

                if side:
                    entry_signals[side] += 1

                    # Mô phỏng vốn margin cố định.
                    # Không mở nếu capital thấp hơn margin.
                    if capital >= args.fixed_margin:
                        open_positions[symbol] = {
                            "symbol": symbol,
                            "side": side,
                            "entry_ts": ts,
                            "entry_price": price,
                            "prob": prob,
                            "leverage": args.sim_leverage,
                            "best_roi": 0.0,
                            "worst_roi": 0.0,
                        }

        # ----------------------------------------------------
        # 3. Log tiến độ mỗi 5%
        # ----------------------------------------------------
        progress = (i + 1) / total_rows

        if progress >= next_progress:
            summary = summarize_trades(
                closed_trades,
                args.initial_capital,
                label=f"{int(next_progress * 100)}%"
            )
            summary["progress"] = float(progress)
            summary["open_positions"] = len(open_positions)
            summary["entry_signal_distribution"] = dict(entry_signals)
            summary["exit_reason_distribution"] = dict(exit_reasons)
            progress_reports.append(summary)

            print(
                f"📊 [REPLAY {int(next_progress * 100)}%] "
                f"trades={summary['trades']} "
                f"profit={summary['net_profit_usdt']:.4f}USDT "
                f"capital={summary['capital_end']:.4f} "
                f"roi={summary['roi_on_capital_pct']:.2f}% "
                f"winrate={summary['winrate'] * 100:.1f}% "
                f"PF={summary['profit_factor']:.2f} "
                f"LONG={summary['long_trades']} "
                f"SHORT={summary['short_trades']} "
                f"open={len(open_positions)}"
            )

            next_progress += progress_step

    # --------------------------------------------------------
    # 4. Đóng position còn mở ở cuối dữ liệu
    # --------------------------------------------------------
    if open_positions:
        last_rows = test_df.groupby("symbol").tail(1).set_index("symbol")

        for symbol, pos in list(open_positions.items()):
            if symbol in last_rows.index:
                row = last_rows.loc[symbol]
                trade = close_sim_position(pos, row, "SIM_FORCE_CLOSE_END", args)
                closed_trades.append(trade)
                capital += trade["pnl_usdt"]
                append_jsonl(trade_path, trade)

        open_positions.clear()

    final_summary = summarize_trades(
        closed_trades,
        args.initial_capital,
        label="final"
    )

    final_summary["progress_reports"] = progress_reports
    final_summary["entry_signal_distribution"] = dict(entry_signals)
    final_summary["exit_reason_distribution"] = dict(exit_reasons)
    final_summary["test_rows"] = int(len(test_df))
    final_summary["test_start_ts"] = int(test_df["_ts"].min())
    final_summary["test_end_ts"] = int(test_df["_ts"].max())
    final_summary["trades_file"] = str(trade_path)

    print("")
    print("================ REPLAY BACKTEST FINAL ================")
    print(f"trades={final_summary['trades']}")
    print(f"capital_start={final_summary['capital_start']:.4f}")
    print(f"capital_end={final_summary['capital_end']:.4f}")
    print(f"net_profit={final_summary['net_profit_usdt']:.4f} USDT")
    print(f"roi_on_capital={final_summary['roi_on_capital_pct']:.2f}%")
    print(f"winrate={final_summary['winrate'] * 100:.2f}%")
    print(f"profit_factor={final_summary['profit_factor']:.2f}")
    print(f"long_trades={final_summary['long_trades']}")
    print(f"short_trades={final_summary['short_trades']}")
    print(f"long_ratio={final_summary['long_ratio'] * 100:.1f}%")
    print(f"short_ratio={final_summary['short_ratio'] * 100:.1f}%")
    print(f"avg_net_roi={final_summary['avg_net_roi']:.3f}%")
    print(f"median_net_roi={final_summary.get('median_net_roi', 0):.3f}%")
    print(f"avg_hold_sec={final_summary['avg_hold_sec']:.1f}s")
    print(f"max_win={final_summary['max_win_usdt']:.4f} USDT")
    print(f"max_loss={final_summary['max_loss_usdt']:.4f} USDT")
    print(f"gross_win={final_summary['gross_win_usdt']:.4f} USDT")
    print(f"gross_loss={final_summary['gross_loss_usdt']:.4f} USDT")
    print(f"trades_file={trade_path}")
    print("=======================================================")
    print("")

    return final_summary


# ============================================================
# 14. MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--data-dir", type=str, required=True)
    parser.add_argument("--output-dir", type=str, required=True)

    parser.add_argument("--train-entry", action="store_true")
    parser.add_argument("--train-exit", action="store_true")
    parser.add_argument("--replay-test", action="store_true")

    parser.add_argument("--horizon", type=int, default=300)
    parser.add_argument("--min-return", type=float, default=0.25)
    parser.add_argument("--action-margin", type=float, default=0.08)
    parser.add_argument("--fee-bps", type=float, default=8.0)

    parser.add_argument("--max-gap-sec", type=float, default=5.0)
    parser.add_argument("--min-episode-rows", type=int, default=900)
    parser.add_argument("--max-rows", type=int, default=0)

    parser.add_argument("--sample-flat-ratio", type=float, default=3.0)

    parser.add_argument("--exit-max-hold-sec", type=int, default=900)
    parser.add_argument("--exit-entry-stride", type=int, default=10)
    parser.add_argument("--exit-max-positions-per-episode", type=int, default=250)

    parser.add_argument("--val-ratio", type=float, default=0.25)
    parser.add_argument("--seed", type=int, default=42)

    parser.add_argument("--n-estimators", type=int, default=400)
    parser.add_argument("--max-depth", type=int, default=4)
    parser.add_argument("--learning-rate", type=float, default=0.035)
    parser.add_argument("--subsample", type=float, default=0.85)
    parser.add_argument("--colsample-bytree", type=float, default=0.85)
    parser.add_argument("--reg-lambda", type=float, default=2.0)
    parser.add_argument("--reg-alpha", type=float, default=0.1)
    parser.add_argument("--n-jobs", type=int, default=-1)

    # Replay simulation config.
    parser.add_argument("--initial-capital", type=float, default=200.0)
    parser.add_argument("--fixed-margin", type=float, default=2.0)
    parser.add_argument("--sim-leverage", type=float, default=8.0)
    parser.add_argument("--entry-threshold", type=float, default=0.56)
    parser.add_argument("--exit-threshold", type=float, default=0.55)
    parser.add_argument("--min-exit-gap", type=float, default=0.06)
    parser.add_argument("--progress-step", type=float, default=0.05)
    parser.add_argument("--sim-emergency-stop-roi", type=float, default=-10.0)
    parser.add_argument("--sim-max-hold-sec", type=int, default=900)
    parser.add_argument("--sim-min-hold-exit-sec", type=int, default=10)

    args = parser.parse_args()

    if not args.train_entry and not args.train_exit:
        args.train_entry = True
        args.train_exit = True

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    ensure_dir(output_dir)

    start_time = time.time()

    print("")
    print("============================================================")
    print("🧠 TRAIN DUAL BRAIN V2 - FULL LIFECYCLE")
    print("============================================================")
    print(f"data_dir={data_dir}")
    print(f"output_dir={output_dir}")
    print(f"train_entry={args.train_entry}")
    print(f"train_exit={args.train_exit}")
    print(f"replay_test={args.replay_test}")
    print(f"horizon={args.horizon}s")
    print(f"min_return={args.min_return}%")
    print(f"action_margin={args.action_margin}%")
    print(f"fee_bps={args.fee_bps}")
    print(f"val_ratio={args.val_ratio}")
    print("============================================================")
    print("")

    df = load_records(data_dir, max_rows=args.max_rows)
    df = assign_episodes(df, max_gap_sec=args.max_gap_sec, min_episode_rows=args.min_episode_rows)

    report = {
        "created_at_ms": now_ms(),
        "args": vars(args),
        "data": {
            "rows": int(len(df)),
            "symbols": int(df["symbol"].nunique()),
            "episodes": int(df["episode_id"].nunique()),
            "start_ts": int(df["_ts"].min()),
            "end_ts": int(df["_ts"].max()),
        },
        "models": {}
    }

    entry_model = None
    exit_model = None

    # --------------------------------------------------------
    # EntryBrain
    # --------------------------------------------------------
    if args.train_entry:
        X_entry, y_entry, w_entry, entry_data = build_entry_dataset(
            df=df,
            horizon=args.horizon,
            min_return=args.min_return,
            fee_bps=args.fee_bps,
            action_margin=args.action_margin,
            sample_flat_ratio=args.sample_flat_ratio,
            random_seed=args.seed
        )

        entry_model, entry_eval = train_model(
            X_entry,
            y_entry,
            w_entry,
            ENTRY_LABEL_NAMES,
            args,
            title="EntryBrain"
        )

        entry_json = output_dir / "EntryBrain.json"
        entry_onnx = output_dir / "EntryBrain.onnx"
        entry_features = output_dir / "EntryBrain.features.json"

        export_xgboost_json(entry_model, entry_json)
        save_feature_list(entry_features, ENHANCED_MARKET_FEATURES)
        onnx_ok = export_onnx(entry_model, entry_onnx, len(ENHANCED_MARKET_FEATURES))

        report["models"]["EntryBrain"] = {
            "json_path": str(entry_json),
            "onnx_path": str(entry_onnx),
            "features_path": str(entry_features),
            "onnx_exported": bool(onnx_ok),
            "features": ENHANCED_MARKET_FEATURES,
            "evaluation": entry_eval,
        }

    # --------------------------------------------------------
    # ExitBrain
    # --------------------------------------------------------
    if args.train_exit:
        X_exit, y_exit, w_exit, exit_data = build_exit_dataset(
            df=df,
            horizon=args.horizon,
            min_return=args.min_return,
            fee_bps=args.fee_bps,
            max_hold_sec=args.exit_max_hold_sec,
            entry_stride=args.exit_entry_stride,
            random_seed=args.seed,
            max_positions_per_episode=args.exit_max_positions_per_episode
        )

        exit_model, exit_eval = train_model(
            X_exit,
            y_exit,
            w_exit,
            EXIT_LABEL_NAMES,
            args,
            title="ExitBrain"
        )

        exit_json = output_dir / "ExitBrain.json"
        exit_onnx = output_dir / "ExitBrain.onnx"
        exit_features = output_dir / "ExitBrain.features.json"

        export_xgboost_json(exit_model, exit_json)
        save_feature_list(exit_features, ENHANCED_POSITION_FEATURES)
        onnx_ok = export_onnx(exit_model, exit_onnx, len(ENHANCED_POSITION_FEATURES))

        report["models"]["ExitBrain"] = {
            "json_path": str(exit_json),
            "onnx_path": str(exit_onnx),
            "features_path": str(exit_features),
            "onnx_exported": bool(onnx_ok),
            "features": ENHANCED_POSITION_FEATURES,
            "evaluation": exit_eval,
        }

    # --------------------------------------------------------
    # Replay full lifecycle
    # --------------------------------------------------------
    if args.replay_test:
        if entry_model is None:
            print("⚠️ replay_test skipped vì chưa có EntryBrain")
        else:
            replay_report = replay_backtest_dual_brain(
                df=df,
                entry_model=entry_model,
                exit_model=exit_model,
                args=args,
                output_dir=output_dir
            )
            report["replay_backtest"] = replay_report

    report["runtime_sec"] = round(time.time() - start_time, 2)

    report_path = output_dir / "training_report.json"
    save_json(report_path, report)

    print("")
    print("============================================================")
    print("✅ TRAINING DONE")
    print("============================================================")
    print(f"output_dir={output_dir}")
    print(f"report={report_path}")
    print(f"runtime={report['runtime_sec']}s")
    print("")
    print("File sinh ra:")
    print("  EntryBrain.json")
    print("  EntryBrain.onnx")
    print("  EntryBrain.features.json")
    print("  ExitBrain.json")
    print("  ExitBrain.onnx")
    print("  ExitBrain.features.json")
    print("  training_report.json")
    print("  replay_trades.jsonl nếu bật --replay-test")
    print("============================================================")
    print("")


if __name__ == "__main__":
    main()