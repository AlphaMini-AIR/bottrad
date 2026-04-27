"""
hourly_self_learning.py
============================================================
AI QUANT SNIPER - HOURLY SELF LEARNING LOOP
------------------------------------------------------------
Một file Python duy nhất để:
1. Đọc dữ liệu live feature đã lưu trong Redis Stream:
   features:archive:SYMBOL

2. Đọc thêm lịch sử lệnh CLOSED trong MongoDB nếu có:
   scouttrades / paper_trades / trades

3. Build dataset 2 class:
   class 0 = SHORT
   class 1 = LONG

4. Train model candidate:
   models/candidates/Universal_Scout_candidate_YYYYMMDD_HHMMSS.onnx

5. Gatekeeper kiểm định candidate.
   Nếu đạt:
   - backup ONNX hiện tại
   - promote candidate thành Universal_Scout.onnx
   - publish Redis command RELOAD_AI

   Nếu không đạt:
   - giữ nguyên ONNX cũ
   - chỉ lưu report để audit

Chạy test ngay không cần chờ 1h:
------------------------------------------------------------
python3 src/training/hourly_self_learning.py --once

Chạy loop mỗi 1h:
------------------------------------------------------------
python3 src/training/hourly_self_learning.py --loop

PM2:
------------------------------------------------------------
{
  name: "hourly-learning",
  script: "src/training/hourly_self_learning.py",
  interpreter: "python3",
  args: "--loop",
  autorestart: true,
  watch: false,
  max_memory_restart: "1200M",
  env: { PYTHONIOENCODING: "utf-8" }
}

Lưu ý quan trọng:
------------------------------------------------------------
File này KHÔNG tự promote bừa.
Candidate chỉ được thay ONNX nếu vượt gatekeeper.
Nếu dữ liệu trong 1h không đủ hoặc LONG/SHORT bị ngược hướng, nó sẽ từ chối.
============================================================
"""

import argparse
import json
import os
import shutil
import sys
import time
import warnings
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import xgboost as xgb
from dotenv import load_dotenv
from pymongo import MongoClient

import onnxmltools
from onnxmltools.convert.common.data_types import FloatTensorType

try:
    import redis
except ImportError:
    redis = None

warnings.filterwarnings("ignore")
load_dotenv()

# ============================================================
# 1. FEATURE SCHEMA - PHẢI KHỚP ORDERMANAGER
# ============================================================
FEATURES = [
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

NOTIONAL_FEATURES = [
    "bid_vol_1pct",
    "ask_vol_1pct",
    "max_buy_trade",
    "max_sell_trade",
    "liq_long_vol",
    "liq_short_vol",
]

CLASS_MAP = {0: "SHORT", 1: "LONG"}

# ============================================================
# 2. CONFIG
# ============================================================
@dataclass
class SelfLearningConfig:
    redis_url: str = os.getenv("REDIS_URL", "redis://127.0.0.1:6379")
    mongo_uri: Optional[str] = os.getenv("MONGO_URI_SCOUT")
    mongo_db: str = os.getenv("MONGO_DB_NAME", "Binance")

    archive_pattern: str = "features:archive:*"
    archive_hours: float = 1.0
    stream_count_per_symbol: int = 8000

    trade_collections: Tuple[str, ...] = ("scouttrades", "paper_trades", "trades")
    use_trade_history: bool = True
    trade_hours: float = 1.0

    horizon_seconds: int = 120
    min_return_pct: float = 0.20
    max_abs_return_pct: float = 8.0
    min_rows_per_symbol: int = 120
    min_total_samples: int = 1200

    use_dynamic_threshold: bool = True
    spread_multiplier: float = 3.0
    min_effective_return_pct: float = 0.15

    # Canonical normalization, giống trainer V3.
    canonical_normalization: bool = True
    quote_ref_window: int = 60
    min_quote_ref: float = 1.0
    raw_notional_detect_q95: float = 5.0

    clip_ob_imb: float = 1.0
    clip_spread_close: float = 0.02
    clip_funding_rate: float = 0.01
    clip_body_size: float = 0.08
    clip_wick_size: float = 0.12
    clip_btc_relative_strength: float = 10.0
    clip_log_notional_ratio: float = 8.0

    train_ratio: float = 0.70
    valid_ratio: float = 0.15
    test_ratio: float = 0.15

    num_boost_round: int = 220
    early_stopping_rounds: int = 35
    random_seed: int = 42
    use_undersample: bool = True
    undersample_ratio: float = 1.10
    max_symbol_ratio: Optional[float] = 0.18

    # Gatekeeper.
    promote: bool = True
    min_test_accuracy: float = 0.51
    max_prediction_bias: float = 0.60
    min_test_samples: int = 500
    require_ret_short_negative: bool = True
    require_ret_long_positive: bool = True
    min_abs_directional_return: float = 0.0001

    # File paths.
    live_onnx_path: str = "Universal_Scout.onnx"
    candidates_dir: str = "models/candidates"
    backups_dir: str = "models/backups"
    reports_dir: str = "reports/hourly_learning"

    # Loop.
    loop_interval_seconds: int = 3600
    run_immediately_in_loop: bool = True


# ============================================================
# 3. LOG / UTILS
# ============================================================
def log(msg: str) -> None:
    print(msg, flush=True)


def utc_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def ts_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def ensure_dirs(cfg: SelfLearningConfig) -> None:
    Path(cfg.candidates_dir).mkdir(parents=True, exist_ok=True)
    Path(cfg.backups_dir).mkdir(parents=True, exist_ok=True)
    Path(cfg.reports_dir).mkdir(parents=True, exist_ok=True)


def to_numeric_safe(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)


def normalize_timestamp(value: Any) -> int:
    if value is None:
        return 0
    try:
        if pd.isna(value):
            return 0
    except Exception:
        pass

    if isinstance(value, (pd.Timestamp, datetime)):
        return int(value.timestamp() * 1000)

    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")

    if isinstance(value, str):
        value = value.strip()
        if not value:
            return 0
        try:
            return int(pd.to_datetime(value, utc=True).timestamp() * 1000)
        except Exception:
            try:
                n = float(value)
            except Exception:
                return 0
    else:
        try:
            n = float(value)
        except Exception:
            return 0

    if n < 1e12:
        n *= 1000
    return int(n)


def stream_id_to_ms(stream_id: Any) -> int:
    if isinstance(stream_id, bytes):
        stream_id = stream_id.decode("utf-8", errors="ignore")
    s = str(stream_id)
    try:
        return int(s.split("-")[0])
    except Exception:
        return 0


def safe_json_loads(value: Any) -> Optional[Dict]:
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="ignore")
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    value = value.strip()
    if not value:
        return None
    try:
        obj = json.loads(value)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


def decode_redis_map(raw: Dict) -> Dict:
    out = {}
    for k, v in raw.items():
        if isinstance(k, bytes):
            k = k.decode("utf-8", errors="ignore")
        if isinstance(v, bytes):
            v_decoded = v.decode("utf-8", errors="ignore")
        else:
            v_decoded = v
        out[str(k)] = v_decoded
    return out


def pct_to_decimal_if_needed(series: pd.Series, q95_threshold: float = 0.30) -> pd.Series:
    s = to_numeric_safe(series, 0.0)
    q95 = float(s.abs().quantile(0.95)) if len(s) else 0.0
    if q95 > q95_threshold:
        return s / 100.0
    return s


# ============================================================
# 4. REDIS / MONGO CONNECT
# ============================================================
def get_redis_client(cfg: SelfLearningConfig):
    if redis is None:
        raise ImportError("Thiếu thư viện redis. Cài: pip install redis")
    return redis.Redis.from_url(cfg.redis_url, decode_responses=False, socket_timeout=10, socket_connect_timeout=10)


def get_mongo_client(cfg: SelfLearningConfig) -> Optional[MongoClient]:
    if not cfg.mongo_uri:
        return None
    return MongoClient(cfg.mongo_uri, serverSelectionTimeoutMS=10000)


# ============================================================
# 5. FLATTEN FEATURE RECORD
# ============================================================
def flatten_feature_payload(payload: Dict, symbol_hint: Optional[str], ts_hint: int) -> Dict:
    """Hỗ trợ nhiều format payload từ DataFeed/FeatureRecorder."""
    out: Dict[str, Any] = {}

    # Nếu payload bọc JSON trong data/payload/json/message.
    for key in ["data", "payload", "json", "message"]:
        nested = safe_json_loads(payload.get(key))
        if nested:
            payload = {**payload, **nested}
            break

    features = payload.get("features") or payload.get("feature") or {}
    micro = payload.get("micro") or {}
    macro = payload.get("macro") or {}
    ohlcv = payload.get("ohlcv") or {}

    out["symbol"] = payload.get("symbol") or payload.get("s") or symbol_hint

    # Timestamp.
    for tcol in ["timestamp", "ts", "time", "event_time", "openTime", "createdAt", "created_at"]:
        if tcol in payload:
            out["timestamp_ms"] = normalize_timestamp(payload.get(tcol))
            break
    if not out.get("timestamp_ms"):
        out["timestamp_ms"] = ts_hint

    # Price.
    for pcol in ["last_price", "mark_price", "close", "price", "c", "lastPrice"]:
        if pcol in payload:
            out["price"] = payload.get(pcol)
            break
    if "price" not in out and isinstance(ohlcv, dict):
        out["price"] = ohlcv.get("close")

    # OHLCV.
    if isinstance(ohlcv, dict):
        for k in ["open", "high", "low", "close", "volume", "quoteVolume", "takerBuyQuote"]:
            if k in ohlcv:
                out.setdefault(k, ohlcv.get(k))

    for k in ["open", "high", "low", "close", "volume", "quoteVolume", "quote_volume", "takerBuyQuote"]:
        if k in payload:
            out.setdefault(k, payload.get(k))

    # Features/nested.
    if isinstance(features, dict):
        for k, v in features.items():
            out.setdefault(k, v)
    if isinstance(micro, dict):
        for k, v in micro.items():
            out.setdefault(k, v)
    if isinstance(macro, dict):
        for k, v in macro.items():
            out.setdefault(k, v)

    for f in FEATURES:
        if f in payload:
            out[f] = payload.get(f)

    alias_map = {
        "fundingRate": "funding_rate",
        "takerBuyRatio": "taker_buy_ratio",
        "bodySize": "body_size",
        "wickSize": "wick_size",
        "btcRelativeStrength": "btc_relative_strength",
        "bidVol1Pct": "bid_vol_1pct",
        "askVol1Pct": "ask_vol_1pct",
        "maxBuyTrade": "max_buy_trade",
        "maxSellTrade": "max_sell_trade",
        "liqLongVol": "liq_long_vol",
        "liqShortVol": "liq_short_vol",
    }
    for src, dst in alias_map.items():
        if src in payload and dst not in out:
            out[dst] = payload.get(src)

    return out


# ============================================================
# 6. LOAD LIVE ARCHIVE FROM REDIS
# ============================================================
def scan_keys(r, pattern: str) -> List[bytes]:
    keys = []
    cursor = 0
    while True:
        cursor, batch = r.scan(cursor=cursor, match=pattern, count=500)
        keys.extend(batch)
        if cursor == 0:
            break
    return keys


def symbol_from_archive_key(key: Any) -> str:
    if isinstance(key, bytes):
        key = key.decode("utf-8", errors="ignore")
    return str(key).split(":")[-1].upper()


def load_live_archive_from_redis(cfg: SelfLearningConfig) -> pd.DataFrame:
    r = get_redis_client(cfg)
    cutoff_ms = utc_now_ms() - int(cfg.archive_hours * 3600 * 1000)
    keys = scan_keys(r, cfg.archive_pattern)

    log(f"📡 [HourlyLearning] Redis archive keys={len(keys)} pattern={cfg.archive_pattern} cutoff={cfg.archive_hours}h")

    rows = []
    for key in keys:
        symbol_hint = symbol_from_archive_key(key)
        try:
            # Đọc mới nhất trước rồi filter cutoff.
            entries = r.xrevrange(key, max="+", min="-", count=cfg.stream_count_per_symbol)
        except Exception as e:
            log(f"⚠️ Không đọc được stream {key}: {e}")
            continue

        for sid, raw_map in entries:
            sid_ms = stream_id_to_ms(sid)
            if sid_ms and sid_ms < cutoff_ms:
                # Vì xrevrange đang từ mới về cũ, gặp quá cutoff có thể break cho stream này.
                break
            payload = decode_redis_map(raw_map)
            row = flatten_feature_payload(payload, symbol_hint=symbol_hint, ts_hint=sid_ms)
            if row.get("timestamp_ms", 0) >= cutoff_ms:
                rows.append(row)

    if not rows:
        raise ValueError("Không có dữ liệu live archive trong Redis. Kiểm tra FeatureRecorder hoặc archive_hours.")

    df = pd.DataFrame(rows)
    log(f"✅ Đã đọc live archive: {len(df):,} rows | symbols={df.get('symbol', pd.Series()).nunique()}")
    return df


# ============================================================
# 7. OPTIONAL TRADE HISTORY
# ============================================================
def extract_features_from_trade(trade: Dict) -> Optional[Dict]:
    """Lấy features_at_entry từ trade nếu có, để bổ sung experience sample."""
    candidates = [
        trade.get("features_at_entry"),
        trade.get("entry_features"),
        trade.get("features"),
        trade.get("snapshot"),
    ]
    for c in candidates:
        if isinstance(c, dict):
            return c
    return None


def load_closed_trades_from_mongo(cfg: SelfLearningConfig) -> pd.DataFrame:
    if not cfg.use_trade_history:
        return pd.DataFrame()
    client = get_mongo_client(cfg)
    if client is None:
        return pd.DataFrame()

    cutoff_ms = utc_now_ms() - int(cfg.trade_hours * 3600 * 1000)
    db = client[cfg.mongo_db]
    rows = []

    for col in cfg.trade_collections:
        try:
            collection = db[col]
            query = {
                "$or": [
                    {"status": "CLOSED"},
                    {"state": "CLOSED"},
                    {"closed": True},
                ]
            }
            trades = list(collection.find(query).sort([("exit_time", -1)]).limit(1000))
        except Exception:
            continue

        for t in trades:
            exit_ts = 0
            for k in ["exit_time", "exitTime", "closed_at", "closedAt", "updatedAt", "timestamp"]:
                if k in t:
                    exit_ts = normalize_timestamp(t.get(k))
                    break
            if exit_ts and exit_ts < cutoff_ms:
                continue

            f = extract_features_from_trade(t)
            if not f:
                continue

            pnl = float(t.get("pnl", t.get("pnl_usdt", t.get("realizedPnl", 0))) or 0)
            side = str(t.get("side", t.get("type", ""))).upper()
            if side not in ["LONG", "SHORT"]:
                continue

            # Gán nhãn trade experience: lời rõ reinforce side, lỗ rõ học hướng ngược.
            # Lệnh pnl gần 0 bỏ qua để tránh nhiễu fee/slippage.
            if abs(pnl) < 0.02:
                continue

            if pnl > 0:
                target = 1 if side == "LONG" else 0
                weight_boost = 1.5 + min(abs(pnl), 5.0)
            else:
                target = 0 if side == "LONG" else 1
                weight_boost = 1.2 + min(abs(pnl), 5.0)

            row = flatten_feature_payload(f, symbol_hint=t.get("symbol"), ts_hint=exit_ts or utc_now_ms())
            row["target"] = target
            row["future_return_pct"] = float(t.get("pnl_pct", t.get("roi", pnl)))
            row["experience_weight_boost"] = weight_boost
            row["is_trade_experience"] = 1
            rows.append(row)

    client.close()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    log(f"🧠 Đã đọc trade experience: {len(df):,} rows")
    return df


# ============================================================
# 8. PREPARE / NORMALIZE FEATURES
# ============================================================
def ensure_feature_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "quoteVolume" not in df.columns and "quote_volume" in df.columns:
        df["quoteVolume"] = df["quote_volume"]

    if "taker_buy_ratio" not in df.columns and {"takerBuyQuote", "quoteVolume"}.issubset(df.columns):
        qv = to_numeric_safe(df["quoteVolume"], 0.0).replace(0, np.nan)
        df["taker_buy_ratio"] = (to_numeric_safe(df["takerBuyQuote"], 0.0) / qv).fillna(0.5)

    if "body_size" not in df.columns and {"open", "close"}.issubset(df.columns):
        op = to_numeric_safe(df["open"], 0.0).replace(0, np.nan)
        cl = to_numeric_safe(df["close"], 0.0)
        df["body_size"] = ((cl - op).abs() / op).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    if "wick_size" not in df.columns and {"open", "high", "low", "close"}.issubset(df.columns):
        op = to_numeric_safe(df["open"], 0.0).replace(0, np.nan)
        hi = to_numeric_safe(df["high"], 0.0)
        lo = to_numeric_safe(df["low"], 0.0)
        cl = to_numeric_safe(df["close"], 0.0)
        body = (cl - op).abs()
        wick = ((hi - lo) - body).clip(lower=0)
        df["wick_size"] = (wick / op).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    if "btc_relative_strength" not in df.columns:
        df["btc_relative_strength"] = 0.0

    missing = [f for f in FEATURES if f not in df.columns]
    if missing:
        log(f"⚠️ Thiếu feature {missing}. Sẽ fill 0.")
        for f in missing:
            df[f] = 0.0

    for f in FEATURES:
        df[f] = to_numeric_safe(df[f], 0.0)
    return df


def prepare_base_dataframe(df: pd.DataFrame, cfg: SelfLearningConfig) -> pd.DataFrame:
    df = df.copy()
    if "symbol" not in df.columns:
        raise ValueError("Dữ liệu thiếu symbol")

    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["timestamp_ms"] = df["timestamp_ms"].apply(normalize_timestamp)

    if "price" not in df.columns:
        for pcol in ["last_price", "mark_price", "close", "lastPrice"]:
            if pcol in df.columns:
                df["price"] = df[pcol]
                break
    if "price" not in df.columns:
        raise ValueError("Dữ liệu thiếu price/last_price/mark_price/close")

    df["price"] = to_numeric_safe(df["price"], np.nan)
    df = ensure_feature_columns(df)

    df = df[(df["symbol"] != "") & (df["timestamp_ms"] > 0) & (df["price"] > 0)].copy()
    df = df.drop_duplicates(subset=["symbol", "timestamp_ms"], keep="last")
    df = df.sort_values(["symbol", "timestamp_ms"]).reset_index(drop=True)

    if "quoteVolume" in df.columns:
        df["quote_volume_raw"] = to_numeric_safe(df["quoteVolume"], np.nan)
    elif "volume" in df.columns:
        df["quote_volume_raw"] = to_numeric_safe(df["volume"], 0.0) * df["price"]
    else:
        df["quote_volume_raw"] = np.nan

    return df


def build_quote_volume_ref(df: pd.DataFrame, cfg: SelfLearningConfig) -> pd.Series:
    q = to_numeric_safe(df.get("quote_volume_raw", pd.Series(index=df.index, dtype=float)), np.nan)
    q = q.replace([np.inf, -np.inf], np.nan)

    refs = []
    for _, part in df.groupby("symbol", sort=False):
        part_q = q.loc[part.index].copy()
        part_q = part_q.where(part_q > 0, np.nan)
        rolling = part_q.rolling(cfg.quote_ref_window, min_periods=max(5, cfg.quote_ref_window // 5)).median()
        median_val = float(part_q.median()) if part_q.notna().any() else np.nan
        if not np.isfinite(median_val) or median_val <= 0:
            median_val = cfg.min_quote_ref
        refs.append(rolling.fillna(median_val).clip(lower=cfg.min_quote_ref))

    if not refs:
        return pd.Series(np.ones(len(df)), index=df.index)
    return pd.concat(refs).sort_index().reindex(df.index).fillna(cfg.min_quote_ref).clip(lower=cfg.min_quote_ref)


def normalize_notional_feature(raw: pd.Series, quote_ref: pd.Series, cfg: SelfLearningConfig) -> Tuple[pd.Series, str]:
    x = to_numeric_safe(raw, 0.0).clip(lower=0)
    q95 = float(x.quantile(0.95)) if len(x) else 0.0
    if q95 > cfg.raw_notional_detect_q95:
        ratio = x / quote_ref.replace(0, np.nan)
        mode = "raw_notional_to_ratio"
    else:
        ratio = x
        mode = "already_ratio"
    ratio = ratio.replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(lower=0)
    return np.log1p(ratio).clip(0, cfg.clip_log_notional_ratio), mode


def apply_canonical_normalization(df: pd.DataFrame, cfg: SelfLearningConfig) -> Tuple[pd.DataFrame, Dict]:
    if not cfg.canonical_normalization:
        return df, {"enabled": False}

    df = df.sort_values(["symbol", "timestamp_ms"]).copy()
    df["quote_volume_ref"] = build_quote_volume_ref(df, cfg)

    report = {"enabled": True, "notional_feature_modes": {}}
    df["ob_imb_top20"] = to_numeric_safe(df["ob_imb_top20"], 0.0).clip(-cfg.clip_ob_imb, cfg.clip_ob_imb)
    df["spread_close"] = pct_to_decimal_if_needed(df["spread_close"], q95_threshold=0.30).clip(0, cfg.clip_spread_close)

    for f in NOTIONAL_FEATURES:
        df[f], mode = normalize_notional_feature(df[f], df["quote_volume_ref"], cfg)
        report["notional_feature_modes"][f] = mode

    df["funding_rate"] = to_numeric_safe(df["funding_rate"], 0.0).clip(-cfg.clip_funding_rate, cfg.clip_funding_rate)
    df["taker_buy_ratio"] = to_numeric_safe(df["taker_buy_ratio"], 0.5).clip(0, 1)
    df["body_size"] = pct_to_decimal_if_needed(df["body_size"], q95_threshold=0.30).abs().clip(0, cfg.clip_body_size)
    df["wick_size"] = pct_to_decimal_if_needed(df["wick_size"], q95_threshold=0.30).abs().clip(0, cfg.clip_wick_size)
    df["btc_relative_strength"] = to_numeric_safe(df["btc_relative_strength"], 0.0).clip(
        -cfg.clip_btc_relative_strength, cfg.clip_btc_relative_strength
    )

    for f in FEATURES:
        df[f] = to_numeric_safe(df[f], 0.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    return df, report


# ============================================================
# 9. LABELING
# ============================================================
def get_dynamic_threshold_pct(df_sym: pd.DataFrame, cfg: SelfLearningConfig) -> pd.Series:
    if not cfg.use_dynamic_threshold:
        return pd.Series(cfg.min_return_pct, index=df_sym.index)
    spread_pct = to_numeric_safe(df_sym["spread_close"], 0.0).clip(lower=0) * 100.0
    dyn = np.maximum(cfg.min_return_pct, spread_pct * cfg.spread_multiplier)
    dyn = np.maximum(dyn, cfg.min_effective_return_pct)
    return pd.Series(dyn, index=df_sym.index)


def label_one_symbol(df_sym: pd.DataFrame, cfg: SelfLearningConfig) -> pd.DataFrame:
    df_sym = df_sym.sort_values("timestamp_ms").reset_index(drop=True).copy()
    if len(df_sym) < cfg.min_rows_per_symbol:
        return pd.DataFrame()

    times = df_sym["timestamp_ms"].values.astype(np.int64)
    prices = df_sym["price"].values.astype(float)
    thresholds = get_dynamic_threshold_pct(df_sym, cfg).values.astype(float)

    target_times = times + int(cfg.horizon_seconds * 1000)
    future_idx = np.searchsorted(times, target_times, side="left")
    valid_mask = future_idx < len(df_sym)

    y = np.full(len(df_sym), -1, dtype=np.int32)
    future_return_pct = np.full(len(df_sym), np.nan, dtype=float)
    threshold_used_pct = np.full(len(df_sym), np.nan, dtype=float)

    idx_now = np.where(valid_mask)[0]
    idx_future = future_idx[valid_mask]
    if len(idx_now) == 0:
        return pd.DataFrame()

    ret = (prices[idx_future] - prices[idx_now]) / prices[idx_now] * 100.0
    th = thresholds[idx_now]
    future_return_pct[idx_now] = ret
    threshold_used_pct[idx_now] = th

    sane = np.abs(ret) <= cfg.max_abs_return_pct
    y[idx_now[(ret >= th) & sane]] = 1
    y[idx_now[(ret <= -th) & sane]] = 0

    df_sym["future_return_pct"] = future_return_pct
    df_sym["label_threshold_pct"] = threshold_used_pct
    df_sym["target"] = y
    df_sym["is_trade_experience"] = df_sym.get("is_trade_experience", 0)
    df_sym["experience_weight_boost"] = df_sym.get("experience_weight_boost", 1.0)

    return df_sym[df_sym["target"].isin([0, 1])].copy()


def build_labeled_dataset(live_df: pd.DataFrame, trade_df: pd.DataFrame, cfg: SelfLearningConfig) -> pd.DataFrame:
    chunks = []
    for sym, part in live_df.groupby("symbol", sort=False):
        labeled = label_one_symbol(part, cfg)
        if labeled.empty:
            continue
        chunks.append(labeled)

    if chunks:
        full = pd.concat(chunks, ignore_index=True)
    else:
        full = pd.DataFrame()

    # Trade experience đã có target, sau khi canonical normalize vẫn có FEATURES.
    if not trade_df.empty:
        trade_df = trade_df.copy()
        trade_df["is_trade_experience"] = 1
        if "experience_weight_boost" not in trade_df.columns:
            trade_df["experience_weight_boost"] = 1.5
        trade_df = trade_df[trade_df["target"].isin([0, 1])].copy()
        if not trade_df.empty:
            full = pd.concat([full, trade_df], ignore_index=True) if not full.empty else trade_df

    if full.empty:
        raise ValueError("Không build được labeled dataset từ live archive/trade history")

    full = full.sort_values("timestamp_ms").reset_index(drop=True)
    return full


# ============================================================
# 10. BALANCE / WEIGHTS / SPLIT
# ============================================================
def soft_cap_by_symbol(df: pd.DataFrame, cfg: SelfLearningConfig) -> pd.DataFrame:
    if cfg.max_symbol_ratio is None or cfg.max_symbol_ratio <= 0 or df.empty:
        return df
    rng = np.random.default_rng(cfg.random_seed)
    total = len(df)
    max_count = int(total * cfg.max_symbol_ratio)
    kept = []
    for sym, part in df.groupby("symbol"):
        if len(part) <= max_count:
            kept.extend(part.index.values)
        else:
            kept.extend(rng.choice(part.index.values, size=max_count, replace=False))
            log(f"⚖️ Soft cap symbol={sym}: {len(part):,} -> {max_count:,}")
    return df.loc[kept].sort_values("timestamp_ms").reset_index(drop=True)


def undersample_classes(df: pd.DataFrame, cfg: SelfLearningConfig) -> pd.DataFrame:
    if not cfg.use_undersample:
        return df
    rng = np.random.default_rng(cfg.random_seed)
    s = df[df["target"] == 0]
    l = df[df["target"] == 1]
    if s.empty or l.empty:
        return df
    min_count = min(len(s), len(l))
    max_keep = int(min_count * cfg.undersample_ratio)
    s_idx = rng.choice(s.index.values, size=min(len(s), max_keep), replace=False)
    l_idx = rng.choice(l.index.values, size=min(len(l), max_keep), replace=False)
    return df.loc[np.concatenate([s_idx, l_idx])].sort_values("timestamp_ms").reset_index(drop=True)


def compute_weights(df: pd.DataFrame, cfg: SelfLearningConfig) -> np.ndarray:
    y = df["target"].values.astype(int)
    total = len(y)
    short_count = max(1, int((y == 0).sum()))
    long_count = max(1, int((y == 1).sum()))
    class_weight = {0: total / (2 * short_count), 1: total / (2 * long_count)}

    symbol_counts = df["symbol"].value_counts().to_dict()
    avg_per_symbol = total / max(1, len(symbol_counts))

    w = np.ones(total, dtype=float)
    for i, (target, symbol) in enumerate(zip(y, df["symbol"].values)):
        sw = np.sqrt(avg_per_symbol / max(1, symbol_counts.get(symbol, 1)))
        w[i] = class_weight[int(target)] * sw

    ret_abs = to_numeric_safe(df["future_return_pct"], 0.0).abs().clip(upper=2.0).values
    w *= 1.0 + (ret_abs / 2.0) * 0.5

    if "experience_weight_boost" in df.columns:
        w *= to_numeric_safe(df["experience_weight_boost"], 1.0).clip(0.5, 6.0).values

    return w / np.mean(w)


def time_split(df: pd.DataFrame, cfg: SelfLearningConfig) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = df.sort_values("timestamp_ms").reset_index(drop=True)
    n = len(df)
    train_end = int(n * cfg.train_ratio)
    valid_end = int(n * (cfg.train_ratio + cfg.valid_ratio))
    return df.iloc[:train_end].copy(), df.iloc[train_end:valid_end].copy(), df.iloc[valid_end:].copy()


# ============================================================
# 11. TRAIN / EVALUATE / EXPORT
# ============================================================
def confusion_matrix_2class(y_true: np.ndarray, y_pred: np.ndarray) -> Dict:
    out = {"SHORT": {"pred_SHORT": 0, "pred_LONG": 0}, "LONG": {"pred_SHORT": 0, "pred_LONG": 0}}
    for yt, yp in zip(y_true, y_pred):
        row = "SHORT" if int(yt) == 0 else "LONG"
        col = "pred_SHORT" if int(yp) == 0 else "pred_LONG"
        out[row][col] += 1
    return out


def evaluate_model(model: xgb.Booster, df: pd.DataFrame, split: str) -> Dict:
    X = df[FEATURES].values.astype(np.float32)
    y = df["target"].values.astype(int)
    proba = model.predict(xgb.DMatrix(X))
    if proba.ndim == 1:
        proba = np.vstack([1.0 - proba, proba]).T
    pred = np.argmax(proba, axis=1)

    return {
        "split": split,
        "samples": int(len(df)),
        "accuracy": float((pred == y).mean()) if len(y) else 0.0,
        "pred_short_ratio": float((pred == 0).mean()) if len(y) else 0.0,
        "pred_long_ratio": float((pred == 1).mean()) if len(y) else 0.0,
        "avg_prob_short": float(np.mean(proba[:, 0])) if len(y) else 0.0,
        "avg_prob_long": float(np.mean(proba[:, 1])) if len(y) else 0.0,
        "avg_future_return_when_pred_short": float(df.loc[pred == 0, "future_return_pct"].mean()) if np.any(pred == 0) else None,
        "avg_future_return_when_pred_long": float(df.loc[pred == 1, "future_return_pct"].mean()) if np.any(pred == 1) else None,
        "confusion_matrix": confusion_matrix_2class(y, pred),
    }


def export_onnx(model: xgb.Booster, output_path: str) -> None:
    initial_type = [("float_input", FloatTensorType([None, len(FEATURES)]))]
    onnx_model = onnxmltools.convert_xgboost(model, initial_types=initial_type, target_opset=13)
    with open(output_path, "wb") as f:
        f.write(onnx_model.SerializeToString())


def train_candidate(df: pd.DataFrame, cfg: SelfLearningConfig, run_id: str) -> Tuple[str, str, Dict]:
    df = soft_cap_by_symbol(df, cfg)
    df = undersample_classes(df, cfg)

    if len(df) < cfg.min_total_samples:
        raise ValueError(f"Dataset quá nhỏ sau balance: {len(df)} < {cfg.min_total_samples}")

    short_n = int((df["target"] == 0).sum())
    long_n = int((df["target"] == 1).sum())
    log(f"⚖️ Final dataset: rows={len(df):,} SHORT={short_n:,} LONG={long_n:,} symbols={df['symbol'].nunique()}")

    train_df, valid_df, test_df = time_split(df, cfg)
    if len(test_df) < cfg.min_test_samples:
        raise ValueError(f"Test samples quá ít: {len(test_df)} < {cfg.min_test_samples}")

    params = {
        "objective": "multi:softprob",
        "num_class": 2,
        "eval_metric": ["mlogloss", "merror"],
        "max_depth": 4,
        "learning_rate": 0.035,
        "subsample": 0.85,
        "colsample_bytree": 0.85,
        "min_child_weight": 8,
        "gamma": 0.25,
        "reg_alpha": 0.08,
        "reg_lambda": 1.8,
        "tree_method": "hist",
        "seed": cfg.random_seed,
        "missing": np.nan,
    }

    dtrain = xgb.DMatrix(train_df[FEATURES].values.astype(np.float32), label=train_df["target"].values.astype(int), weight=compute_weights(train_df, cfg))
    dvalid = xgb.DMatrix(valid_df[FEATURES].values.astype(np.float32), label=valid_df["target"].values.astype(int), weight=compute_weights(valid_df, cfg))

    model = xgb.train(
        params,
        dtrain,
        num_boost_round=cfg.num_boost_round,
        evals=[(dtrain, "train"), (dvalid, "valid")],
        early_stopping_rounds=cfg.early_stopping_rounds,
        verbose_eval=25,
    )

    candidate_onnx = str(Path(cfg.candidates_dir) / f"Universal_Scout_candidate_{run_id}.onnx")
    candidate_xgb = str(Path(cfg.candidates_dir) / f"model_candidate_{run_id}.xgb")
    model.save_model(candidate_xgb)
    export_onnx(model, candidate_onnx)

    train_report = evaluate_model(model, train_df, "train")
    valid_report = evaluate_model(model, valid_df, "valid")
    test_report = evaluate_model(model, test_df, "test")

    report = {
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "class_map": CLASS_MAP,
        "features": FEATURES,
        "config": asdict(cfg),
        "dataset": {
            "samples": int(len(df)),
            "symbols": int(df["symbol"].nunique()),
            "short_samples": short_n,
            "long_samples": long_n,
            "by_symbol_top20": df["symbol"].value_counts().head(20).to_dict(),
            "trade_experience_rows": int(to_numeric_safe(df.get("is_trade_experience", pd.Series([0]*len(df))), 0).sum()),
        },
        "splits": {"train": train_report, "valid": valid_report, "test": test_report},
        "candidate_onnx": candidate_onnx,
        "candidate_xgb": candidate_xgb,
    }

    return candidate_onnx, candidate_xgb, report


# ============================================================
# 12. GATEKEEPER / PROMOTE
# ============================================================
def gatekeeper(report: Dict, cfg: SelfLearningConfig) -> Tuple[bool, List[str]]:
    reasons = []
    test = report["splits"]["test"]

    if test["samples"] < cfg.min_test_samples:
        reasons.append(f"TEST_SAMPLES_TOO_LOW {test['samples']} < {cfg.min_test_samples}")

    if test["accuracy"] < cfg.min_test_accuracy:
        reasons.append(f"TEST_ACC_TOO_LOW {test['accuracy']:.4f} < {cfg.min_test_accuracy}")

    max_bias = max(test["pred_short_ratio"], test["pred_long_ratio"])
    if max_bias > cfg.max_prediction_bias:
        reasons.append(f"PREDICTION_BIAS_TOO_HIGH {max_bias:.2%} > {cfg.max_prediction_bias:.2%}")

    ret_s = test.get("avg_future_return_when_pred_short")
    ret_l = test.get("avg_future_return_when_pred_long")

    if cfg.require_ret_short_negative:
        if ret_s is None or ret_s >= -cfg.min_abs_directional_return:
            reasons.append(f"SHORT_RET_NOT_NEGATIVE retS={ret_s}")

    if cfg.require_ret_long_positive:
        if ret_l is None or ret_l <= cfg.min_abs_directional_return:
            reasons.append(f"LONG_RET_NOT_POSITIVE retL={ret_l}")

    return len(reasons) == 0, reasons


def atomic_promote(candidate_onnx: str, cfg: SelfLearningConfig, r) -> str:
    live_path = Path(cfg.live_onnx_path)
    if not live_path.exists():
        raise FileNotFoundError(f"Không thấy live ONNX hiện tại: {live_path}")

    backup_path = Path(cfg.backups_dir) / f"Universal_Scout_backup_{ts_slug()}.onnx"
    shutil.copy2(live_path, backup_path)

    tmp_path = live_path.with_suffix(live_path.suffix + ".tmp")
    shutil.copy2(candidate_onnx, tmp_path)
    os.replace(tmp_path, live_path)

    # Reload AI trong OrderManager.
    try:
        r.publish("system:commands", json.dumps({"action": "RELOAD_AI"}))
        log("🔄 Đã publish RELOAD_AI")
    except Exception as e:
        log(f"⚠️ Promote thành công nhưng publish RELOAD_AI lỗi: {e}")

    return str(backup_path)


# ============================================================
# 13. ONE RUN
# ============================================================
def run_once(cfg: SelfLearningConfig) -> Dict:
    ensure_dirs(cfg)
    run_id = ts_slug()
    report_path = str(Path(cfg.reports_dir) / f"hourly_learning_report_{run_id}.json")

    log("\n" + "=" * 70)
    log("🧠 HOURLY SELF LEARNING - RUN ONCE")
    log("=" * 70)
    log(f"⚙️ archive_hours={cfg.archive_hours} horizon={cfg.horizon_seconds}s min_return={cfg.min_return_pct}% promote={cfg.promote}")

    r = get_redis_client(cfg)

    live_raw = load_live_archive_from_redis(cfg)
    live_base = prepare_base_dataframe(live_raw, cfg)
    live_canon, norm_report = apply_canonical_normalization(live_base, cfg)

    trade_raw = load_closed_trades_from_mongo(cfg)
    if not trade_raw.empty:
        trade_base = prepare_base_dataframe(trade_raw, cfg)
        trade_canon, _ = apply_canonical_normalization(trade_base, cfg)
    else:
        trade_canon = pd.DataFrame()

    labeled = build_labeled_dataset(live_canon, trade_canon, cfg)

    candidate_onnx = None
    candidate_xgb = None
    promoted = False
    backup_path = None
    gate_reasons = []

    try:
        candidate_onnx, candidate_xgb, report = train_candidate(labeled, cfg, run_id)
        report["canonical_normalization"] = norm_report

        passed, gate_reasons = gatekeeper(report, cfg)
        report["gatekeeper"] = {"passed": passed, "reasons": gate_reasons}

        if passed and cfg.promote:
            backup_path = atomic_promote(candidate_onnx, cfg, r)
            promoted = True
            report["promotion"] = {"promoted": True, "backup_path": backup_path, "live_onnx_path": cfg.live_onnx_path}
            log(f"✅ [PROMOTED] Candidate đã thay live ONNX | backup={backup_path}")
        elif passed and not cfg.promote:
            report["promotion"] = {"promoted": False, "reason": "promote_disabled"}
            log("🟡 Candidate đạt gatekeeper nhưng promote=false, giữ model cũ.")
        else:
            report["promotion"] = {"promoted": False, "reason": "gatekeeper_failed", "gate_reasons": gate_reasons}
            log("⛔ Candidate KHÔNG đạt gatekeeper. Giữ ONNX cũ.")
            for reason in gate_reasons:
                log(f"   - {reason}")

    except Exception as e:
        report = {
            "run_id": run_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "config": asdict(cfg),
            "error": str(e),
            "promotion": {"promoted": False, "reason": "exception"},
            "dataset_debug": {
                "live_raw_rows": int(len(live_raw)),
                "live_canon_rows": int(len(live_canon)),
                "trade_rows": int(len(trade_canon)) if not trade_canon.empty else 0,
                "labeled_rows": int(len(labeled)) if 'labeled' in locals() else 0,
            },
        }
        log(f"❌ Hourly learning failed: {e}")

    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    log(f"📄 Đã lưu report: {report_path}")
    if "splits" in report:
        t = report["splits"]["test"]
        log(
            f"📊 TEST acc={t['accuracy']:.4f} SHORT={t['pred_short_ratio']:.1%} LONG={t['pred_long_ratio']:.1%} "
            f"retS={t['avg_future_return_when_pred_short']} retL={t['avg_future_return_when_pred_long']}"
        )
    log(f"🏁 DONE | promoted={promoted}")
    return report


# ============================================================
# 14. LOOP
# ============================================================
def run_loop(cfg: SelfLearningConfig) -> None:
    log("🚀 [HourlyLearning] Loop started")
    if cfg.run_immediately_in_loop:
        run_once(cfg)

    while True:
        log(f"⏳ [HourlyLearning] Ngủ {cfg.loop_interval_seconds}s...")
        time.sleep(cfg.loop_interval_seconds)
        run_once(cfg)


# ============================================================
# 15. CLI
# ============================================================
def parse_args() -> SelfLearningConfig:
    parser = argparse.ArgumentParser(description="Hourly Self Learning for AI Quant Sniper")

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Chạy ngay 1 lần để test")
    mode.add_argument("--loop", action="store_true", help="Chạy loop mỗi 1h")

    parser.add_argument("--archive-hours", type=float, default=1.0)
    parser.add_argument("--trade-hours", type=float, default=1.0)
    parser.add_argument("--stream-count", type=int, default=8000)

    parser.add_argument("--horizon", type=int, default=120)
    parser.add_argument("--min-return", type=float, default=0.20)
    parser.add_argument("--min-total-samples", type=int, default=1200)
    parser.add_argument("--min-rows-per-symbol", type=int, default=120)

    parser.add_argument("--live-onnx", default="Universal_Scout.onnx")
    parser.add_argument("--no-promote", action="store_true", help="Chỉ train/evaluate candidate, không thay ONNX")

    parser.add_argument("--min-test-accuracy", type=float, default=0.51)
    parser.add_argument("--max-bias", type=float, default=0.60)
    parser.add_argument("--min-test-samples", type=int, default=500)
    parser.add_argument("--allow-long-ret-negative", action="store_true")
    parser.add_argument("--allow-short-ret-positive", action="store_true")

    parser.add_argument("--loop-interval", type=int, default=3600)
    parser.add_argument("--no-run-immediately", action="store_true")

    args = parser.parse_args()

    return SelfLearningConfig(
        archive_hours=args.archive_hours,
        trade_hours=args.trade_hours,
        stream_count_per_symbol=args.stream_count,
        horizon_seconds=args.horizon,
        min_return_pct=args.min_return,
        min_total_samples=args.min_total_samples,
        min_rows_per_symbol=args.min_rows_per_symbol,
        live_onnx_path=args.live_onnx,
        promote=not args.no_promote,
        min_test_accuracy=args.min_test_accuracy,
        max_prediction_bias=args.max_bias,
        min_test_samples=args.min_test_samples,
        require_ret_long_positive=not args.allow_long_ret_negative,
        require_ret_short_negative=not args.allow_short_ret_positive,
        loop_interval_seconds=args.loop_interval,
        run_immediately_in_loop=not args.no_run_immediately,
    )


if __name__ == "__main__":
    try:
        cfg = parse_args()
        if "--loop" in sys.argv:
            run_loop(cfg)
        else:
            run_once(cfg)
    except KeyboardInterrupt:
        log("\n🛑 Dừng hourly learning theo yêu cầu.")
        sys.exit(130)
    except Exception as e:
        log(f"\n❌ HOURLY SELF LEARNING FAILED: {e}")
        sys.exit(1)
