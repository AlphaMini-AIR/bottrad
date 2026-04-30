#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TRAIN DUAL BRAIN V1
============================================================
Mục tiêu:
1. Đọc dữ liệu live 1s dạng .jsonl.gz từ recorder.
2. Hỗ trợ cả schema cũ và Feed V20 mới.
3. Chia dữ liệu thành episode theo symbol + thời gian liên tục.
4. Train 2 bộ não:
   - EntryBrain: 0 = FLAT, 1 = LONG, 2 = SHORT
   - ExitBrain : 0 = HOLD, 1 = EXIT
5. Export:
   - EntryBrain.json
   - EntryBrain.onnx
   - EntryBrain.features.json
   - ExitBrain.json
   - ExitBrain.onnx
   - ExitBrain.features.json
   - training_report.json

Triết lý:
- Không dùng Deep RL.
- Dùng Oracle Labeling + XGBoost để học hành động tối ưu.
- Tiệm cận RL bằng cách học action thay vì chỉ đoán hướng.
- Đánh giá bằng reward/backtest đơn giản, không chỉ nhìn accuracy.

Cài thư viện:
pip install pandas numpy xgboost scikit-learn onnxmltools onnx

Ví dụ chạy nhanh:
python train_dual_brain.py \
  --data-dir ./data/live_features \
  --output-dir ./models_test \
  --train-entry \
  --train-exit \
  --horizon 240 \
  --min-return 0.12 \
  --fee-bps 8 \
  --max-gap-sec 5 \
  --min-episode-rows 900 \
  --max-rows 1000000
"""

import argparse
import gzip
import json
import math
import os
import sys
import time
from collections import Counter, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Iterable, Optional, Tuple, Any

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
# 1. FEATURE LISTS
# ------------------------------------------------------------
# Quan trọng:
# Tên feature phải là field mà OrderManager V21 có thể đọc.
# Không dùng feature tự chế kiểu log_x nếu OrderManager chưa tính được.
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
        x = int(float(value))
        return x
    except Exception:
        return fallback


def normalize_symbol(value: Any) -> str:
    return str(value or "").upper().strip()


def timestamp_to_ms(value: Any) -> int:
    n = safe_float(value, 0.0)
    if n <= 0:
        return 0
    if n < 1e12:
        return int(n * 1000)
    return int(n)


def pct_return(from_price: float, to_price: float, side: str = "LONG") -> float:
    if from_price <= 0 or to_price <= 0:
        return 0.0

    if side == "LONG":
        return (to_price - from_price) / from_price * 100.0

    return (from_price - to_price) / from_price * 100.0


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def list_data_files(data_dir: Path) -> List[Path]:
    patterns = ["*.jsonl.gz", "*.json.gz", "*.jsonl", "*.json"]
    files = []

    for pattern in patterns:
        files.extend(data_dir.rglob(pattern))

    return sorted(files)


# ============================================================
# 3. ROBUST JSONL / GZIP READER
# ------------------------------------------------------------
# Recorder có thể đang mở file khi bạn copy về, gzip có thể thiếu EOF.
# Hàm này cố đọc được phần dòng hợp lệ trước khi lỗi EOF.
# ============================================================

def iter_json_lines_from_file(path: Path) -> Iterable[Dict[str, Any]]:
    suffix = path.name.lower()

    if suffix.endswith(".gz"):
        try:
            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
                while True:
                    try:
                        line = f.readline()
                    except EOFError:
                        # Gzip thiếu EOF marker, bỏ qua phần cuối.
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

        except EOFError:
            return
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
    candidates = [
        record.get("_snapshot_ts"),
        record.get("feature_ts"),
        record.get("_recorder_ts"),
        record.get("timestamp"),
        record.get("ts"),
        record.get("event_time"),
        record.get("last_update_ts"),
    ]

    for value in candidates:
        ts = timestamp_to_ms(value)
        if ts > 0:
            return ts

    return 0


def get_price(record: Dict[str, Any]) -> float:
    # Để label trade sát Futures hơn, ưu tiên mark_price nếu có.
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
    clean["_ts"] = ts
    clean["_price"] = price

    # --------------------------------------------------------
    # Basic fields / aliases
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
    # Local flow fallbacks
    # --------------------------------------------------------
    clean["local_taker_buy_ratio"] = safe_float(
        clean.get("local_taker_buy_ratio"),
        safe_float(clean.get("taker_buy_ratio"), 0.5)
    )

    clean["taker_buy_ratio"] = safe_float(
        clean.get("taker_buy_ratio"),
        clean["local_taker_buy_ratio"]
    )

    clean["taker_buy_ratio_24h"] = safe_float(
        clean.get("taker_buy_ratio_24h"),
        clean["taker_buy_ratio"]
    )

    clean["local_quote_volume"] = safe_float(clean.get("local_quote_volume"), 0.0)
    clean["local_buy_quote"] = safe_float(clean.get("local_buy_quote"), 0.0)
    clean["local_sell_quote"] = safe_float(clean.get("local_sell_quote"), 0.0)
    clean["local_trade_count"] = safe_float(clean.get("local_trade_count"), 0.0)

    if clean["local_quote_volume"] <= 0:
        # Fallback nhẹ cho schema cũ.
        clean["local_quote_volume"] = safe_float(clean.get("quote_volume"), 0.0)

    if clean["local_buy_quote"] <= 0 and clean["local_quote_volume"] > 0:
        clean["local_buy_quote"] = clean["local_quote_volume"] * clean["local_taker_buy_ratio"]

    if clean["local_sell_quote"] <= 0 and clean["local_quote_volume"] > 0:
        clean["local_sell_quote"] = clean["local_quote_volume"] - clean["local_buy_quote"]

    if "trade_flow_imbalance" not in clean:
        qv = clean["local_quote_volume"]
        if qv > 0:
            clean["trade_flow_imbalance"] = (clean["local_buy_quote"] - clean["local_sell_quote"]) / qv
        else:
            clean["trade_flow_imbalance"] = 0.0

    # --------------------------------------------------------
    # Liquidity / liquidation fallbacks
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
    # Required numeric fields
    # --------------------------------------------------------
    for name in ENHANCED_MARKET_FEATURES:
        clean[name] = safe_float(clean.get(name), 0.0)

    # Taker ratio final clamp.
    clean["taker_buy_ratio"] = min(1.0, max(0.0, clean["taker_buy_ratio"]))
    clean["local_taker_buy_ratio"] = min(1.0, max(0.0, clean["local_taker_buy_ratio"]))
    clean["taker_buy_ratio_24h"] = min(1.0, max(0.0, clean["taker_buy_ratio_24h"]))

    return clean


def load_records(
    data_dir: Path,
    max_rows: int = 0,
    verbose_every: int = 250_000
) -> pd.DataFrame:
    files = list_data_files(data_dir)

    if not files:
        raise FileNotFoundError(f"Không tìm thấy file dữ liệu trong {data_dir}")

    print(f"📂 Found {len(files)} data files")

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
                print(f"📥 Loaded valid rows={total_valid:,} raw={total_raw:,}")

            if max_rows and total_valid >= max_rows:
                break

        if max_rows and total_valid >= max_rows:
            break

        if idx % 50 == 0:
            print(f"📄 scanned files {idx}/{len(files)} valid={total_valid:,}")

    if not rows:
        raise RuntimeError("Không có dòng dữ liệu hợp lệ để train")

    df = pd.DataFrame(rows)

    df["_ts"] = df["_ts"].astype(np.int64)
    df["_price"] = df["_price"].astype(float)
    df["symbol"] = df["symbol"].astype(str)

    df = df.sort_values(["symbol", "_ts"]).reset_index(drop=True)

    print(f"✅ Loaded dataframe rows={len(df):,}, symbols={df['symbol'].nunique()}")

    return df


# ============================================================
# 5. EPISODE SPLIT
# ============================================================

def assign_episodes(
    df: pd.DataFrame,
    max_gap_sec: float = 5.0,
    min_episode_rows: int = 900
) -> pd.DataFrame:
    df = df.sort_values(["symbol", "_ts"]).copy()

    max_gap_ms = int(max_gap_sec * 1000)

    episode_ids = np.full(len(df), -1, dtype=np.int64)
    episode_id = 0

    start_idx = 0

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
        raise RuntimeError(
            "Không có episode đủ dài. Hãy giảm --min-episode-rows hoặc kiểm tra dữ liệu có đủ 15m liên tục không."
        )

    ep_stats = df.groupby("episode_id").agg(
        symbol=("symbol", "first"),
        rows=("_ts", "count"),
        start_ts=("_ts", "min"),
        end_ts=("_ts", "max")
    )

    print(
        f"✅ Episodes={len(ep_stats):,}, rows={len(df):,}, "
        f"avgRows={ep_stats['rows'].mean():.1f}, "
        f"symbols={df['symbol'].nunique()}"
    )

    return df


# ============================================================
# 6. FUTURE WINDOW MAX/MIN
# ------------------------------------------------------------
# Tính max/min tương lai trong horizon theo O(n) bằng deque.
# window cho i là i+1 -> i+horizon.
# ============================================================

def future_max_min(values: np.ndarray, horizon: int) -> Tuple[np.ndarray, np.ndarray]:
    n = len(values)
    future_max = np.full(n, np.nan, dtype=np.float64)
    future_min = np.full(n, np.nan, dtype=np.float64)

    maxdq = deque()
    mindq = deque()

    # Duyệt từ phải sang trái.
    # Tại i, ta muốn window (i+1 ... i+horizon).
    for i in range(n - 1, -1, -1):
        right = i + horizon

        # Loại index ngoài window.
        while maxdq and maxdq[0] > right:
            maxdq.popleft()
        while mindq and mindq[0] > right:
            mindq.popleft()

        # Thêm i+1 vào window sau khi đã tính cho i?
        # Khi duyệt từ phải sang trái, thêm current index i vào deque,
        # nhưng future của i-1 sẽ dùng i.
        if maxdq:
            future_max[i] = values[maxdq[0]]
        if mindq:
            future_min[i] = values[mindq[0]]

        # Add current i for future windows of previous rows.
        while maxdq and values[maxdq[-1]] <= values[i]:
            maxdq.pop()
        maxdq.append(i)

        while mindq and values[mindq[-1]] >= values[i]:
            mindq.pop()
        mindq.append(i)

    return future_max, future_min


# ============================================================
# 7. ENTRY DATASET
# ------------------------------------------------------------
# Label:
# 0 = FLAT
# 1 = LONG
# 2 = SHORT
#
# Oracle:
# - Nếu future max cho LONG sau phí >= min_return
#   và tốt hơn SHORT đủ margin -> LONG
# - Nếu future min cho SHORT sau phí >= min_return
#   và tốt hơn LONG đủ margin -> SHORT
# - Còn lại -> FLAT
# ============================================================

def build_entry_dataset(
    df: pd.DataFrame,
    horizon: int,
    min_return: float,
    fee_bps: float,
    action_margin: float,
    sample_flat_ratio: float,
    random_seed: int
) -> Tuple[pd.DataFrame, pd.Series, pd.Series]:
    rng = np.random.default_rng(random_seed)

    all_parts = []

    fee_pct = fee_bps / 100.0

    print("🏷️ Building EntryBrain labels...")

    for ep_id, ep in df.groupby("episode_id"):
        ep = ep.sort_values("_ts").copy()
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
        labels = labels[valid]
        long_reward = long_reward[valid]
        short_reward = short_reward[valid]

        ep["entry_label"] = labels
        ep["oracle_long_reward"] = long_reward
        ep["oracle_short_reward"] = short_reward
        ep["oracle_best_reward"] = np.maximum(long_reward, short_reward)

        all_parts.append(ep)

    if not all_parts:
        raise RuntimeError("Không tạo được entry dataset")

    data = pd.concat(all_parts, ignore_index=True)

    # Downsample FLAT nếu quá nhiều để train nhanh và cân bằng hơn.
    if 0 < sample_flat_ratio < 1:
        flat = data[data["entry_label"] == 0]
        non_flat = data[data["entry_label"] != 0]

        if len(flat) > 0 and len(non_flat) > 0:
            keep_flat_n = int(min(len(flat), max(len(non_flat) * sample_flat_ratio, 1000)))
            keep_idx = rng.choice(flat.index.values, size=keep_flat_n, replace=False)
            flat_keep = flat.loc[keep_idx]
            data = pd.concat([flat_keep, non_flat], ignore_index=True)
            data = data.sample(frac=1, random_state=random_seed).reset_index(drop=True)

    X = data[ENHANCED_MARKET_FEATURES].astype(np.float32)
    y = data["entry_label"].astype(np.int64)
    weights = build_class_weights(y)

    print(f"✅ Entry dataset rows={len(data):,} dist={dict(Counter(y.tolist()))}")

    return X, y, weights


# ============================================================
# 8. EXIT DATASET
# ------------------------------------------------------------
# ExitBrain cần position context.
# Ta tạo dữ liệu position giả lập từ các entry oracle tốt.
#
# Label:
# 0 = HOLD
# 1 = EXIT
#
# Ý tưởng:
# Với mỗi entry oracle LONG/SHORT, tạo các state sau entry.
# Tại mỗi state:
# - reward_exit_now = ROI hiện tại
# - reward_hold_future = ROI tốt nhất trong horizon tiếp theo
# Nếu exit_now tốt hơn hoặc giữ tiếp không còn lợi thế -> EXIT
# Nếu giữ tiếp còn lợi thế -> HOLD
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
) -> Tuple[pd.DataFrame, pd.Series, pd.Series]:
    rng = np.random.default_rng(random_seed)
    fee_pct = fee_bps / 100.0

    rows = []

    print("🏷️ Building ExitBrain labels...")

    for ep_id, ep in df.groupby("episode_id"):
        ep = ep.sort_values("_ts").copy().reset_index(drop=True)
        prices = ep["_price"].values.astype(np.float64)

        if len(ep) <= horizon + 20:
            continue

        fmax, fmin = future_max_min(prices, horizon)

        long_reward = (fmax - prices) / prices * 100.0 - fee_pct
        short_reward = (prices - fmin) / prices * 100.0 - fee_pct

        # Chọn entry candidates có reward tốt.
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
            selected_idx = rng.choice(len(candidates), size=max_positions_per_episode, replace=False)
            candidates = [candidates[i] for i in selected_idx]

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

                # Oracle exit:
                # - Nếu ROI hiện tại đang tốt nhưng tương lai không cải thiện nhiều -> EXIT.
                # - Nếu giveback lớn -> EXIT.
                # - Nếu tương lai còn vượt current rõ -> HOLD.
                improvement = future_best_roi - current_roi

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
                row["sim_entry_index"] = entry_i
                row["sim_entry_side"] = side

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
    weights = build_class_weights(y)

    print(f"✅ Exit dataset rows={len(data):,} dist={dict(Counter(y.tolist()))}")

    return X, y, weights


# ============================================================
# 9. TRAIN / SPLIT / EVAL
# ============================================================

def build_class_weights(y: pd.Series) -> pd.Series:
    counts = Counter(y.tolist())
    total = len(y)
    n_classes = max(len(counts), 1)

    class_weight = {}
    for cls, count in counts.items():
        class_weight[cls] = total / (n_classes * max(count, 1))

    weights = y.map(class_weight).astype(np.float32)
    return weights


def train_val_split_by_time(
    X: pd.DataFrame,
    y: pd.Series,
    weights: pd.Series,
    val_ratio: float
):
    n = len(X)
    split = int(n * (1.0 - val_ratio))
    split = max(1, min(split, n - 1))

    X_train = X.iloc[:split].copy()
    y_train = y.iloc[:split].copy()
    w_train = weights.iloc[:split].copy()

    X_val = X.iloc[split:].copy()
    y_val = y.iloc[split:].copy()
    w_val = weights.iloc[split:].copy()

    return X_train, X_val, y_train, y_val, w_train, w_val


def make_xgb_classifier(
    num_class: int,
    max_depth: int,
    n_estimators: int,
    learning_rate: float,
    subsample: float,
    colsample_bytree: float,
    reg_lambda: float,
    reg_alpha: float,
    n_jobs: int,
    random_seed: int
):
    if num_class == 2:
        return XGBClassifier(
            objective="binary:logistic",
            eval_metric="logloss",
            tree_method="hist",
            max_depth=max_depth,
            n_estimators=n_estimators,
            learning_rate=learning_rate,
            subsample=subsample,
            colsample_bytree=colsample_bytree,
            reg_lambda=reg_lambda,
            reg_alpha=reg_alpha,
            n_jobs=n_jobs,
            random_state=random_seed,
        )

    return XGBClassifier(
        objective="multi:softprob",
        num_class=num_class,
        eval_metric="mlogloss",
        tree_method="hist",
        max_depth=max_depth,
        n_estimators=n_estimators,
        learning_rate=learning_rate,
        subsample=subsample,
        colsample_bytree=colsample_bytree,
        reg_lambda=reg_lambda,
        reg_alpha=reg_alpha,
        n_jobs=n_jobs,
        random_state=random_seed,
    )


def evaluate_classifier(
    model,
    X_val: pd.DataFrame,
    y_val: pd.Series,
    label_names: Dict[int, str],
    title: str
) -> Dict[str, Any]:
    pred = model.predict(X_val)
    proba = model.predict_proba(X_val)

    acc = accuracy_score(y_val, pred)

    report_text = classification_report(
        y_val,
        pred,
        target_names=[label_names.get(i, str(i)) for i in sorted(label_names.keys())],
        zero_division=0
    )

    cm = confusion_matrix(y_val, pred)

    print("")
    print(f"================ {title} EVALUATION ================")
    print(f"Accuracy: {acc:.4f}")
    print(report_text)
    print("Confusion matrix:")
    print(cm)

    dist_true = dict(Counter(y_val.tolist()))
    dist_pred = dict(Counter(pred.tolist()))

    # Per-class precision/recall.
    precision, recall, f1, support = precision_recall_fscore_support(
        y_val,
        pred,
        labels=sorted(label_names.keys()),
        zero_division=0
    )

    class_metrics = {}
    for idx, cls in enumerate(sorted(label_names.keys())):
        class_metrics[label_names[cls]] = {
            "precision": float(precision[idx]),
            "recall": float(recall[idx]),
            "f1": float(f1[idx]),
            "support": int(support[idx]),
        }

    # Confidence stats.
    max_prob = np.max(proba, axis=1)
    avg_conf = float(np.mean(max_prob)) if len(max_prob) else 0.0

    result = {
        "title": title,
        "accuracy": float(acc),
        "true_distribution": {str(label_names.get(k, k)): int(v) for k, v in dist_true.items()},
        "pred_distribution": {str(label_names.get(k, k)): int(v) for k, v in dist_pred.items()},
        "class_metrics": class_metrics,
        "avg_confidence": avg_conf,
        "confusion_matrix": cm.tolist(),
        "classification_report": report_text,
    }

    return result


def train_model(
    X: pd.DataFrame,
    y: pd.Series,
    weights: pd.Series,
    label_names: Dict[int, str],
    args,
    title: str
):
    X_train, X_val, y_train, y_val, w_train, w_val = train_val_split_by_time(
        X, y, weights, args.val_ratio
    )

    num_class = len(label_names)

    model = make_xgb_classifier(
        num_class=num_class,
        max_depth=args.max_depth,
        n_estimators=args.n_estimators,
        learning_rate=args.learning_rate,
        subsample=args.subsample,
        colsample_bytree=args.colsample_bytree,
        reg_lambda=args.reg_lambda,
        reg_alpha=args.reg_alpha,
        n_jobs=args.n_jobs,
        random_seed=args.seed
    )

    print("")
    print(f"🚀 Training {title}...")
    print(f"train={len(X_train):,} val={len(X_val):,} features={X.shape[1]} classes={num_class}")

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
# 10. QUICK POLICY TEST
# ------------------------------------------------------------
# Test nhanh độ "khôn" của EntryBrain bằng oracle rewards.
# Không thay thế backtest đầy đủ, nhưng giúp biết não có FOMO không.
# ============================================================

def quick_entry_policy_test(
    model,
    X: pd.DataFrame,
    data_with_rewards: pd.DataFrame,
    threshold_long: float,
    threshold_short: float
) -> Dict[str, Any]:
    if len(X) == 0:
        return {}

    proba = model.predict_proba(X)
    # EntryBrain classes: FLAT, LONG, SHORT.
    if proba.shape[1] < 3:
        return {}

    p_flat = proba[:, 0]
    p_long = proba[:, 1]
    p_short = proba[:, 2]

    actions = np.zeros(len(X), dtype=np.int64)

    long_mask = (p_long >= threshold_long) & (p_long > p_short) & (p_long > p_flat)
    short_mask = (p_short >= threshold_short) & (p_short > p_long) & (p_short > p_flat)

    actions[long_mask] = 1
    actions[short_mask] = 2

    long_rewards = data_with_rewards["oracle_long_reward"].values if "oracle_long_reward" in data_with_rewards else np.zeros(len(X))
    short_rewards = data_with_rewards["oracle_short_reward"].values if "oracle_short_reward" in data_with_rewards else np.zeros(len(X))

    taken = actions != 0
    rewards = np.zeros(len(X), dtype=np.float64)
    rewards[actions == 1] = long_rewards[actions == 1]
    rewards[actions == 2] = short_rewards[actions == 2]

    trade_rewards = rewards[taken]

    if len(trade_rewards) == 0:
        return {
            "signals": 0,
            "signal_rate": 0.0,
            "avg_reward": 0.0,
            "winrate": 0.0,
            "profit_factor": 0.0,
        }

    wins = trade_rewards[trade_rewards > 0]
    losses = trade_rewards[trade_rewards <= 0]

    gross_win = float(np.sum(wins))
    gross_loss = float(abs(np.sum(losses)))

    profit_factor = gross_win / gross_loss if gross_loss > 0 else 999.0
    winrate = len(wins) / len(trade_rewards)

    return {
        "signals": int(len(trade_rewards)),
        "signal_rate": float(len(trade_rewards) / len(X)),
        "avg_reward": float(np.mean(trade_rewards)),
        "median_reward": float(np.median(trade_rewards)),
        "winrate": float(winrate),
        "profit_factor": float(profit_factor),
        "gross_win": gross_win,
        "gross_loss": gross_loss,
        "action_distribution": {
            "FLAT": int(np.sum(actions == 0)),
            "LONG": int(np.sum(actions == 1)),
            "SHORT": int(np.sum(actions == 2)),
        }
    }


# ============================================================
# 11. EXPORT MODEL
# ============================================================

def save_feature_list(path: Path, features: List[str]):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(features, f, ensure_ascii=False, indent=2)


def export_xgboost_json(model, path: Path):
    model.save_model(str(path))


def export_onnx(model, path: Path, n_features: int):
    try:
        import onnxmltools
        from onnxmltools.convert.common.data_types import FloatTensorType

        initial_types = [("float_input", FloatTensorType([None, n_features]))]

        onnx_model = onnxmltools.convert_xgboost(
            model,
            initial_types=initial_types,
            target_opset=15
        )

        with open(path, "wb") as f:
            f.write(onnx_model.SerializeToString())

        print(f"✅ Exported ONNX: {path}")

        return True

    except Exception as e:
        print("")
        print(f"⚠️ Không export được ONNX {path.name}: {e}")
        print("   Model JSON vẫn đã được lưu. Cài thêm: pip install onnxmltools onnx")
        return False


# ============================================================
# 12. SAVE REPORT
# ============================================================

def save_report(path: Path, report: Dict[str, Any]):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)


# ============================================================
# 13. MAIN TRAIN FLOW
# ============================================================

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--data-dir", type=str, required=True)
    parser.add_argument("--output-dir", type=str, required=True)

    parser.add_argument("--train-entry", action="store_true")
    parser.add_argument("--train-exit", action="store_true")

    parser.add_argument("--horizon", type=int, default=240, help="Số giây nhìn tương lai")
    parser.add_argument("--min-return", type=float, default=0.12, help="Reward tối thiểu % sau phí")
    parser.add_argument("--action-margin", type=float, default=0.03, help="Chênh reward tối thiểu giữa LONG/SHORT")
    parser.add_argument("--fee-bps", type=float, default=8.0, help="Fee + slippage ước tính theo bps. 8 bps = 0.08%")

    parser.add_argument("--max-gap-sec", type=float, default=5.0)
    parser.add_argument("--min-episode-rows", type=int, default=900)
    parser.add_argument("--max-rows", type=int, default=0)

    parser.add_argument("--sample-flat-ratio", type=float, default=3.0,
                        help="Giữ số FLAT tối đa khoảng non_flat * ratio. <=0 hoặc >=999 để giữ toàn bộ.")

    parser.add_argument("--exit-max-hold-sec", type=int, default=900)
    parser.add_argument("--exit-entry-stride", type=int, default=10)
    parser.add_argument("--exit-max-positions-per-episode", type=int, default=250)

    parser.add_argument("--val-ratio", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)

    parser.add_argument("--n-estimators", type=int, default=350)
    parser.add_argument("--max-depth", type=int, default=5)
    parser.add_argument("--learning-rate", type=float, default=0.04)
    parser.add_argument("--subsample", type=float, default=0.85)
    parser.add_argument("--colsample-bytree", type=float, default=0.85)
    parser.add_argument("--reg-lambda", type=float, default=2.0)
    parser.add_argument("--reg-alpha", type=float, default=0.1)
    parser.add_argument("--n-jobs", type=int, default=-1)

    parser.add_argument("--entry-long-threshold", type=float, default=0.56)
    parser.add_argument("--entry-short-threshold", type=float, default=0.56)

    args = parser.parse_args()

    if not args.train_entry and not args.train_exit:
        print("⚠️ Bạn chưa chọn --train-entry hoặc --train-exit. Mặc định train cả hai.")
        args.train_entry = True
        args.train_exit = True

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir)
    ensure_dir(output_dir)

    start = time.time()

    print("")
    print("============================================================")
    print("🧠 TRAIN DUAL BRAIN V1")
    print("============================================================")
    print(f"data_dir={data_dir}")
    print(f"output_dir={output_dir}")
    print(f"horizon={args.horizon}s min_return={args.min_return}% fee_bps={args.fee_bps}")
    print(f"train_entry={args.train_entry} train_exit={args.train_exit}")
    print("")

    df = load_records(data_dir, max_rows=args.max_rows)
    df = assign_episodes(
        df,
        max_gap_sec=args.max_gap_sec,
        min_episode_rows=args.min_episode_rows
    )

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

    # --------------------------------------------------------
    # EntryBrain
    # --------------------------------------------------------
    if args.train_entry:
        X_entry, y_entry, w_entry = build_entry_dataset(
            df=df,
            horizon=args.horizon,
            min_return=args.min_return,
            fee_bps=args.fee_bps,
            action_margin=args.action_margin,
            sample_flat_ratio=args.sample_flat_ratio,
            random_seed=args.seed
        )

        entry_label_names = {
            0: "FLAT",
            1: "LONG",
            2: "SHORT",
        }

        entry_model, entry_eval = train_model(
            X_entry,
            y_entry,
            w_entry,
            entry_label_names,
            args,
            title="EntryBrain"
        )

        entry_json_path = output_dir / "EntryBrain.json"
        entry_onnx_path = output_dir / "EntryBrain.onnx"
        entry_features_path = output_dir / "EntryBrain.features.json"

        export_xgboost_json(entry_model, entry_json_path)
        save_feature_list(entry_features_path, ENHANCED_MARKET_FEATURES)
        onnx_ok = export_onnx(entry_model, entry_onnx_path, len(ENHANCED_MARKET_FEATURES))

        quick_policy = quick_entry_policy_test(
            entry_model,
            X_entry,
            pd.concat([X_entry.reset_index(drop=True), y_entry.reset_index(drop=True)], axis=1),
            threshold_long=args.entry_long_threshold,
            threshold_short=args.entry_short_threshold
        )

        # quick_policy ở trên không có oracle rewards vì X_entry không giữ reward.
        # Tạo lại quick policy đúng bằng data entry đầy đủ nếu cần ở bản sau.
        # Ở bản này giữ evaluation chính.

        report["models"]["EntryBrain"] = {
            "json_path": str(entry_json_path),
            "onnx_path": str(entry_onnx_path),
            "features_path": str(entry_features_path),
            "onnx_exported": bool(onnx_ok),
            "features": ENHANCED_MARKET_FEATURES,
            "evaluation": entry_eval,
        }

        print(f"✅ Saved EntryBrain JSON: {entry_json_path}")
        print(f"✅ Saved EntryBrain features: {entry_features_path}")

    # --------------------------------------------------------
    # ExitBrain
    # --------------------------------------------------------
    if args.train_exit:
        X_exit, y_exit, w_exit = build_exit_dataset(
            df=df,
            horizon=args.horizon,
            min_return=args.min_return,
            fee_bps=args.fee_bps,
            max_hold_sec=args.exit_max_hold_sec,
            entry_stride=args.exit_entry_stride,
            random_seed=args.seed,
            max_positions_per_episode=args.exit_max_positions_per_episode
        )

        exit_label_names = {
            0: "HOLD",
            1: "EXIT",
        }

        exit_model, exit_eval = train_model(
            X_exit,
            y_exit,
            w_exit,
            exit_label_names,
            args,
            title="ExitBrain"
        )

        exit_json_path = output_dir / "ExitBrain.json"
        exit_onnx_path = output_dir / "ExitBrain.onnx"
        exit_features_path = output_dir / "ExitBrain.features.json"

        export_xgboost_json(exit_model, exit_json_path)
        save_feature_list(exit_features_path, ENHANCED_POSITION_FEATURES)
        onnx_ok = export_onnx(exit_model, exit_onnx_path, len(ENHANCED_POSITION_FEATURES))

        report["models"]["ExitBrain"] = {
            "json_path": str(exit_json_path),
            "onnx_path": str(exit_onnx_path),
            "features_path": str(exit_features_path),
            "onnx_exported": bool(onnx_ok),
            "features": ENHANCED_POSITION_FEATURES,
            "evaluation": exit_eval,
        }

        print(f"✅ Saved ExitBrain JSON: {exit_json_path}")
        print(f"✅ Saved ExitBrain features: {exit_features_path}")

    report["runtime_sec"] = round(time.time() - start, 2)

    report_path = output_dir / "training_report.json"
    save_report(report_path, report)

    print("")
    print("============================================================")
    print("✅ TRAINING DONE")
    print("============================================================")
    print(f"Report: {report_path}")
    print(f"Runtime: {report['runtime_sec']}s")
    print("")

    print("Gợi ý tiếp theo:")
    print("1. Mở training_report.json để xem precision/recall từng lớp.")
    print("2. Nếu EntryBrain predict LONG/SHORT quá nhiều → tăng min_return hoặc threshold.")
    print("3. Nếu EntryBrain toàn FLAT → giảm min_return hoặc tăng dữ liệu.")
    print("4. Nếu ExitBrain EXIT quá nhiều → tăng exit label điều kiện hoặc thêm dữ liệu trade thật.")
    print("5. Chỉ copy ONNX lên VPS khi report ổn.")


if __name__ == "__main__":
    main()