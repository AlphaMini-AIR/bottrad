"""
universal_live_trainer_v3_canonical.py
============================================================
BỘ TRAINER KHỞI ĐẦU 10/10 CHO AI QUANT SNIPER
MULTI-MONGO + CANONICAL NORMALIZATION + 2-CLASS SHORT/LONG
------------------------------------------------------------
Mục tiêu chính:
1. Đọc dữ liệu từ cả 3 MongoDB:
   - MONGO_URI_TIER1: coin lớn/tầng 1
   - MONGO_URI_TIER2: coin vừa/tầng 2
   - MONGO_URI_SCOUT: coin nhỏ/scout/live radar

2. Train model ONNX chỉ gồm 2 class:
   - class 0 = SHORT
   - class 1 = LONG

3. Không dùng HOLD trong model.
   Vùng nhiễu sẽ bị loại khỏi dataset.
   Việc "không vào lệnh" do OrderManager/RiskGuard/EntryFilter xử lý.

4. Chuẩn hóa dữ liệu coin lớn/nhỏ về cùng hệ quy chiếu:
   - volume/orderbook/liquidation -> ratio so với quoteVolume nền
   - price movement -> % / decimal so với giá
   - spread -> % / decimal so với giá
   - clip + log1p cố định, không dùng StandardScaler cần fit

5. Ưu tiên coin nhỏ/vừa nhưng vẫn học cấu trúc thị trường từ coin lớn.

6. Xuất:
   - Universal_Scout.onnx
   - model_universal_canonical_v3.xgb
   - training_report_canonical_v3.json
   - feature_schema_canonical_v3.json

Cách chạy khuyến nghị:
------------------------------------------------------------
python3 src/training/universal_live_trainer_v3_canonical.py train \
  --collection live_features \
  --horizon 180 \
  --min-return 0.25

Nếu chưa biết collection, để auto-detect:
python3 src/training/universal_live_trainer_v3_canonical.py train

Nếu muốn ưu tiên scout hơn:
python3 src/training/universal_live_trainer_v3_canonical.py train \
  --tier1-weight 0.90 \
  --tier2-weight 1.05 \
  --scout-weight 1.20

Lưu ý sống còn:
------------------------------------------------------------
Model này train trên CANONICAL FEATURES.
Live DataFeed/OrderManager cũng phải đưa 13 feature theo đúng canonical schema.
Nếu live vẫn feed raw absolute volume, model sẽ suy luận sai.
============================================================
"""

import argparse
import json
import os
import sys
import warnings
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import xgboost as xgb
from dotenv import load_dotenv
from pymongo import MongoClient

import onnxmltools
from onnxmltools.convert.common.data_types import FloatTensorType

warnings.filterwarnings("ignore")
load_dotenv()

# ============================================================
# 1. FEATURE SCHEMA - PHẢI KHỚP ORDERMANAGER.JS
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

DEFAULT_COLLECTIONS = [
    "live_features",
    "feature_archive",
    "features_archive",
    "market_features",
    "market_data_clean",
]

DEFAULT_DB_NAME = os.getenv("MONGO_DB_NAME", "Binance")

MONGO_URIS = {
    "tier1": os.getenv("MONGO_URI_TIER1"),
    "tier2": os.getenv("MONGO_URI_TIER2"),
    "scout": os.getenv("MONGO_URI_SCOUT"),
}

# Mặc định phục vụ mục tiêu đánh coin nhỏ/vừa:
# tier1 chỉ học cấu trúc thị trường, không cho áp đảo scout.
DEFAULT_TIER_WEIGHTS = {
    "tier1": 0.90,
    "tier2": 1.05,
    "scout": 1.20,
}


# ============================================================
# 2. CONFIG
# ============================================================
@dataclass
class TrainConfig:
    source: str = "mongo"
    db_name: str = DEFAULT_DB_NAME
    collection: Optional[str] = None
    csv_path: Optional[str] = None

    tiers: Tuple[str, ...] = ("tier1", "tier2", "scout")
    tier1_weight: float = DEFAULT_TIER_WEIGHTS["tier1"]
    tier2_weight: float = DEFAULT_TIER_WEIGHTS["tier2"]
    scout_weight: float = DEFAULT_TIER_WEIGHTS["scout"]

    horizon_seconds: int = 180
    min_return_pct: float = 0.25
    max_abs_return_pct: float = 8.0

    # Dynamic label threshold:
    # threshold = max(min_return_pct, spread_close_pct * spread_multiplier, min_effective_return_pct)
    use_dynamic_threshold: bool = True
    spread_multiplier: float = 3.0
    min_effective_return_pct: float = 0.18

    min_rows_per_symbol: int = 300
    min_total_samples: int = 1000

    train_ratio: float = 0.70
    valid_ratio: float = 0.15
    test_ratio: float = 0.15

    output_onnx: str = "Universal_Scout.onnx"
    output_xgb: str = "model_universal_canonical_v3.xgb"
    output_report: str = "training_report_canonical_v3.json"
    output_schema: str = "feature_schema_canonical_v3.json"

    random_seed: int = 42
    num_boost_round: int = 600
    early_stopping_rounds: int = 50

    max_bias_ratio_warn: float = 0.65
    use_undersample: bool = False
    undersample_ratio: float = 1.15

    max_tier_ratio: Optional[float] = 0.60
    max_symbol_ratio: Optional[float] = 0.15

    # Canonical normalization.
    canonical_normalization: bool = True
    quote_ref_window: int = 60
    min_quote_ref: float = 1.0

    # Nếu dữ liệu đã là ratio nhỏ, vẫn log1p nhẹ để giữ scale ổn định.
    # Nếu dữ liệu là raw notional lớn, sẽ chia quote_ref trước rồi log1p.
    raw_notional_detect_q95: float = 5.0

    # Clip cố định để live có thể áp dụng giống hệt, không cần scaler fit.
    clip_ob_imb: float = 1.0
    clip_spread_close: float = 0.02       # decimal, 0.02 = 2%
    clip_funding_rate: float = 0.01       # 1%
    clip_body_size: float = 0.08          # decimal, 8%
    clip_wick_size: float = 0.12          # decimal, 12%
    clip_btc_relative_strength: float = 10.0  # pct point nếu field là percent
    clip_log_notional_ratio: float = 8.0

    def tier_weights(self) -> Dict[str, float]:
        return {
            "tier1": self.tier1_weight,
            "tier2": self.tier2_weight,
            "scout": self.scout_weight,
        }


# ============================================================
# 3. UTILS
# ============================================================
def log(msg: str) -> None:
    print(msg, flush=True)


def to_numeric_safe(series: pd.Series, default: float = 0.0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").replace([np.inf, -np.inf], np.nan).fillna(default)


def detect_time_column(df: pd.DataFrame) -> str:
    candidates = ["timestamp", "ts", "time", "event_time", "openTime", "open_time", "createdAt", "created_at"]
    for col in candidates:
        if col in df.columns:
            return col
    raise ValueError(f"Không tìm thấy cột thời gian. Cần một trong: {candidates}")


def detect_price_column(df: pd.DataFrame) -> str:
    candidates = ["last_price", "mark_price", "close", "price", "c", "lastPrice"]
    for col in candidates:
        if col in df.columns:
            return col
    raise ValueError(f"Không tìm thấy cột giá. Cần một trong: {candidates}")


def normalize_timestamp(value) -> int:
    if pd.isna(value):
        return 0
    if isinstance(value, (pd.Timestamp, datetime)):
        return int(value.timestamp() * 1000)
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


def pct_to_decimal_if_needed(series: pd.Series, q95_threshold: float = 0.30) -> pd.Series:
    """
    Nếu field có vẻ đang là percent kiểu 1.2 nghĩa là 1.2%, đổi về decimal 0.012.
    Nếu field đã là decimal kiểu 0.012 thì giữ nguyên.
    """
    s = to_numeric_safe(series, 0.0)
    q95 = float(s.abs().quantile(0.95)) if len(s) else 0.0
    if q95 > q95_threshold:
        return s / 100.0
    return s


def flatten_record(record: Dict, source_tier: str) -> Dict:
    out = {"source_tier": source_tier}
    out["symbol"] = record.get("symbol") or record.get("s")

    for tcol in ["timestamp", "ts", "time", "event_time", "openTime", "createdAt", "created_at"]:
        if tcol in record:
            out[tcol] = record.get(tcol)

    for pcol in ["last_price", "mark_price", "close", "price", "lastPrice"]:
        if pcol in record:
            out[pcol] = record.get(pcol)

    features = record.get("features") or record.get("feature") or {}
    micro = record.get("micro") or {}
    macro = record.get("macro") or {}
    ohlcv = record.get("ohlcv") or {}

    if isinstance(features, dict):
        for k, v in features.items():
            out.setdefault(k, v)

    if isinstance(micro, dict):
        for k, v in micro.items():
            out.setdefault(k, v)

    if isinstance(macro, dict):
        for k, v in macro.items():
            out.setdefault(k, v)

    if isinstance(ohlcv, dict):
        out.setdefault("open", ohlcv.get("open"))
        out.setdefault("high", ohlcv.get("high"))
        out.setdefault("low", ohlcv.get("low"))
        out.setdefault("close", ohlcv.get("close"))
        out.setdefault("volume", ohlcv.get("volume"))
        out.setdefault("quoteVolume", ohlcv.get("quoteVolume"))
        out.setdefault("takerBuyQuote", ohlcv.get("takerBuyQuote"))

    # Root OHLCV fallback.
    for k in ["open", "high", "low", "close", "volume", "quoteVolume", "quote_volume", "takerBuyQuote"]:
        if k in record:
            out.setdefault(k, record.get(k))

    for f in FEATURES:
        if f in record:
            out[f] = record.get(f)

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
        if src in record and dst not in out:
            out[dst] = record.get(src)

    return out


# ============================================================
# 4. DATA LOADING
# ============================================================
def load_from_csv(cfg: TrainConfig) -> pd.DataFrame:
    if not cfg.csv_path:
        raise ValueError("Bạn chọn --source csv nhưng chưa truyền --csv")
    if not os.path.exists(cfg.csv_path):
        raise FileNotFoundError(f"Không tìm thấy CSV: {cfg.csv_path}")
    log(f"📄 Đang đọc CSV: {cfg.csv_path}")
    df = pd.read_csv(cfg.csv_path)
    if "source_tier" not in df.columns:
        df["source_tier"] = "csv"
    return df


def mongo_collection_exists(db, name: str) -> bool:
    try:
        return name in db.list_collection_names()
    except Exception:
        return False


def select_collection(db, requested_collection: Optional[str]) -> Optional[str]:
    collections = [requested_collection] if requested_collection else DEFAULT_COLLECTIONS
    for col in collections:
        if col and mongo_collection_exists(db, col):
            return col
    return None


def load_one_mongo_tier(tier: str, uri: str, cfg: TrainConfig) -> pd.DataFrame:
    log(f"\n📦 [{tier}] Kết nối MongoDB: db={cfg.db_name}")
    client = MongoClient(uri)
    db = client[cfg.db_name]

    selected = select_collection(db, cfg.collection)
    if not selected:
        client.close()
        log(f"⚠️ [{tier}] Không tìm thấy collection. Bỏ qua tier này.")
        return pd.DataFrame()

    log(f"📚 [{tier}] Đang đọc collection: {selected}")
    rows = []
    count = 0

    cursor = db[selected].find({}).sort([("symbol", 1)])
    for r in cursor:
        rows.append(flatten_record(r, tier))
        count += 1
        if count % 100000 == 0:
            log(f"   ... [{tier}] đã đọc {count:,} records")

    client.close()
    if not rows:
        log(f"⚪ [{tier}] Collection {selected} không có dữ liệu")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    log(f"✅ [{tier}] Đã đọc {len(df):,} records")
    return df


def load_from_all_mongo_tiers(cfg: TrainConfig) -> pd.DataFrame:
    frames = []
    for tier in cfg.tiers:
        tier = tier.strip().lower()
        if tier not in MONGO_URIS:
            log(f"⚠️ Tier không hợp lệ: {tier}. Bỏ qua.")
            continue
        uri = MONGO_URIS.get(tier)
        if not uri:
            log(f"⚠️ Thiếu MONGO_URI_{tier.upper()}. Bỏ qua {tier}.")
            continue
        try:
            df_tier = load_one_mongo_tier(tier, uri, cfg)
            if not df_tier.empty:
                frames.append(df_tier)
        except Exception as e:
            log(f"❌ [{tier}] Lỗi đọc Mongo: {e}")

    if not frames:
        raise ValueError("Không đọc được dữ liệu từ bất kỳ Mongo tier nào.")

    full = pd.concat(frames, ignore_index=True)
    log("\n" + "=" * 70)
    log("📊 RAW MULTI-TIER SUMMARY")
    log("=" * 70)
    log(str(full["source_tier"].value_counts()))
    log(f"✅ Tổng raw records: {len(full):,}")
    return full


# ============================================================
# 5. BASE PREPARATION + FEATURE COMPLETION
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
        log(f"⚠️ Thiếu feature {missing}. Sẽ fill 0. Nên kiểm tra lại DataFeed/Archive.")
        for f in missing:
            df[f] = 0.0

    for f in FEATURES:
        df[f] = to_numeric_safe(df[f], 0.0)

    return df


def prepare_base_dataframe(raw_df: pd.DataFrame, cfg: TrainConfig) -> pd.DataFrame:
    df = raw_df.copy()

    if "symbol" not in df.columns:
        raise ValueError("Dữ liệu thiếu cột symbol")
    if "source_tier" not in df.columns:
        df["source_tier"] = "unknown"

    time_col = detect_time_column(df)
    price_col = detect_price_column(df)

    df["symbol"] = df["symbol"].astype(str).str.upper().str.strip()
    df["source_tier"] = df["source_tier"].astype(str).str.lower().str.strip()
    df["timestamp_ms"] = df[time_col].apply(normalize_timestamp)
    df["price"] = to_numeric_safe(df[price_col], np.nan)

    df = ensure_feature_columns(df)

    df = df[(df["symbol"] != "") & (df["timestamp_ms"] > 0) & (df["price"] > 0)].copy()
    df = df.drop_duplicates(subset=["source_tier", "symbol", "timestamp_ms"], keep="last")
    df = df.sort_values(["source_tier", "symbol", "timestamp_ms"]).reset_index(drop=True)

    # Basic quote volume reference.
    if "quoteVolume" in df.columns:
        df["quote_volume_raw"] = to_numeric_safe(df["quoteVolume"], np.nan)
    elif "volume" in df.columns:
        df["quote_volume_raw"] = to_numeric_safe(df["volume"], 0.0) * df["price"]
    else:
        df["quote_volume_raw"] = np.nan

    df["quote_volume_raw"] = df["quote_volume_raw"].replace([np.inf, -np.inf], np.nan)

    log(f"✅ Base dataframe: {len(df):,} rows | symbols={df['symbol'].nunique()} | tiers={df['source_tier'].nunique()}")
    log("📊 Base rows by tier:")
    log(str(df["source_tier"].value_counts()))
    return df


# ============================================================
# 6. CANONICAL NORMALIZATION
# ============================================================
def build_quote_volume_ref(df: pd.DataFrame, cfg: TrainConfig) -> pd.Series:
    """
    Tạo quote_volume_ref theo từng tier/symbol.
    Ưu tiên quoteVolume thật. Nếu thiếu, fallback bằng median theo symbol/tier.
    Đây là mẫu số để quy đổi raw notional của coin lớn/nhỏ về cùng scale.
    """
    df = df.sort_values(["source_tier", "symbol", "timestamp_ms"]).copy()
    q = to_numeric_safe(df.get("quote_volume_raw", pd.Series(index=df.index, dtype=float)), np.nan)
    q = q.replace([np.inf, -np.inf], np.nan)

    refs = []
    for (_, _), part in df.groupby(["source_tier", "symbol"], sort=False):
        part_q = q.loc[part.index].copy()
        part_q = part_q.where(part_q > 0, np.nan)

        rolling = part_q.rolling(cfg.quote_ref_window, min_periods=max(5, cfg.quote_ref_window // 5)).median()
        median_val = float(part_q.median()) if part_q.notna().any() else np.nan
        if not np.isfinite(median_val) or median_val <= 0:
            median_val = cfg.min_quote_ref

        rolling = rolling.fillna(median_val).clip(lower=cfg.min_quote_ref)
        refs.append(rolling)

    if not refs:
        return pd.Series(np.ones(len(df)), index=df.index)

    out = pd.concat(refs).sort_index()
    return out.reindex(df.index).fillna(cfg.min_quote_ref).clip(lower=cfg.min_quote_ref)


def normalize_notional_feature(raw: pd.Series, quote_ref: pd.Series, cfg: TrainConfig, feature_name: str) -> pd.Series:
    """
    Nếu raw có vẻ là absolute notional lớn: ratio = raw / quote_ref.
    Nếu raw đã là ratio nhỏ: giữ như ratio.
    Sau đó log1p và clip.
    """
    x = to_numeric_safe(raw, 0.0).clip(lower=0)
    q95 = float(x.quantile(0.95)) if len(x) else 0.0

    if q95 > cfg.raw_notional_detect_q95:
        ratio = x / quote_ref.replace(0, np.nan)
        mode = "raw_notional_to_ratio"
    else:
        ratio = x
        mode = "already_ratio"

    ratio = ratio.replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(lower=0)
    normalized = np.log1p(ratio).clip(0, cfg.clip_log_notional_ratio)
    normalized.attrs["normalization_mode"] = mode
    return normalized


def apply_canonical_normalization(df: pd.DataFrame, cfg: TrainConfig) -> Tuple[pd.DataFrame, Dict]:
    """
    Chuẩn hóa 13 feature thành canonical scale.
    Không dùng scaler fit theo dataset để live có thể áp dụng y chang.
    """
    if not cfg.canonical_normalization:
        log("⚠️ canonical_normalization=false. Model sẽ train bằng feature scale hiện tại.")
        return df, {"enabled": False}

    df = df.sort_values(["source_tier", "symbol", "timestamp_ms"]).copy()
    quote_ref = build_quote_volume_ref(df, cfg)
    df["quote_volume_ref"] = quote_ref

    report = {
        "enabled": True,
        "quote_ref_window": cfg.quote_ref_window,
        "notional_feature_modes": {},
        "rules": {
            "ob_imb_top20": f"clip(-{cfg.clip_ob_imb}, {cfg.clip_ob_imb})",
            "spread_close": f"decimal percent of price, clip(0, {cfg.clip_spread_close})",
            "notional_features": "auto raw-notional/ratio -> log1p(value/quote_volume_ref or value) -> clip",
            "funding_rate": f"clip(-{cfg.clip_funding_rate}, {cfg.clip_funding_rate})",
            "taker_buy_ratio": "clip(0, 1)",
            "body_size": f"decimal body/price, clip(0, {cfg.clip_body_size})",
            "wick_size": f"decimal wick/price, clip(0, {cfg.clip_wick_size})",
            "btc_relative_strength": f"clip(-{cfg.clip_btc_relative_strength}, {cfg.clip_btc_relative_strength})",
        },
    }

    # 1. Orderbook imbalance.
    df["ob_imb_top20"] = to_numeric_safe(df["ob_imb_top20"], 0.0).clip(-cfg.clip_ob_imb, cfg.clip_ob_imb)

    # 2. Spread close: nếu đang là percent lớn, đổi về decimal.
    df["spread_close"] = pct_to_decimal_if_needed(df["spread_close"], q95_threshold=0.30).clip(0, cfg.clip_spread_close)

    # 3. Notional features: raw absolute -> ratio -> log1p.
    for f in NOTIONAL_FEATURES:
        normalized = normalize_notional_feature(df[f], df["quote_volume_ref"], cfg, f)
        report["notional_feature_modes"][f] = normalized.attrs.get("normalization_mode", "unknown")
        df[f] = normalized

    # 4. Funding.
    df["funding_rate"] = to_numeric_safe(df["funding_rate"], 0.0).clip(-cfg.clip_funding_rate, cfg.clip_funding_rate)

    # 5. Taker buy ratio.
    df["taker_buy_ratio"] = to_numeric_safe(df["taker_buy_ratio"], 0.5).clip(0, 1)

    # 6. Body/Wick: nếu đang là percent kiểu 1.2, đổi về 0.012.
    df["body_size"] = pct_to_decimal_if_needed(df["body_size"], q95_threshold=0.30).abs().clip(0, cfg.clip_body_size)
    df["wick_size"] = pct_to_decimal_if_needed(df["wick_size"], q95_threshold=0.30).abs().clip(0, cfg.clip_wick_size)

    # 7. BTC relative strength: giữ theo pct-point nếu field đang là %, hoặc decimal nếu live đang dùng decimal.
    # Vì OrderManager hiện tại chỉ dùng như scalar tương đối, clip rộng để không bóp quá mạnh.
    df["btc_relative_strength"] = to_numeric_safe(df["btc_relative_strength"], 0.0).clip(
        -cfg.clip_btc_relative_strength,
        cfg.clip_btc_relative_strength,
    )

    for f in FEATURES:
        df[f] = to_numeric_safe(df[f], 0.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    log("✅ Đã áp dụng canonical normalization cho 13 features.")
    log("📊 Notional feature modes:")
    log(str(report["notional_feature_modes"]))
    return df, report


# ============================================================
# 7. FEATURE STATS REPORT
# ============================================================
def feature_stats(df: pd.DataFrame) -> Dict:
    out = {}
    for f in FEATURES:
        s = to_numeric_safe(df[f], 0.0)
        out[f] = {
            "mean": float(s.mean()),
            "std": float(s.std()),
            "min": float(s.min()),
            "p01": float(s.quantile(0.01)),
            "p50": float(s.quantile(0.50)),
            "p99": float(s.quantile(0.99)),
            "max": float(s.max()),
        }
    return out


def feature_stats_by_group(df: pd.DataFrame, group_col: str) -> Dict:
    result = {}
    for group, part in df.groupby(group_col):
        result[str(group)] = feature_stats(part)
    return result


# ============================================================
# 8. LABELING
# ============================================================
def get_dynamic_threshold_pct(df_sym: pd.DataFrame, cfg: TrainConfig) -> pd.Series:
    if not cfg.use_dynamic_threshold:
        return pd.Series(cfg.min_return_pct, index=df_sym.index)

    # spread_close đang là decimal. Đổi sang pct rồi nhân multiplier.
    spread_pct = to_numeric_safe(df_sym["spread_close"], 0.0).clip(lower=0) * 100.0
    dyn = np.maximum(cfg.min_return_pct, spread_pct * cfg.spread_multiplier)
    dyn = np.maximum(dyn, cfg.min_effective_return_pct)
    return pd.Series(dyn, index=df_sym.index)


def label_one_symbol(df_sym: pd.DataFrame, cfg: TrainConfig) -> pd.DataFrame:
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

    current_prices = prices[idx_now]
    future_prices = prices[idx_future]
    ret = (future_prices - current_prices) / current_prices * 100.0

    th = thresholds[idx_now]
    future_return_pct[idx_now] = ret
    threshold_used_pct[idx_now] = th

    sane = np.abs(ret) <= cfg.max_abs_return_pct
    long_mask = (ret >= th) & sane
    short_mask = (ret <= -th) & sane

    y[idx_now[long_mask]] = 1
    y[idx_now[short_mask]] = 0

    df_sym["future_return_pct"] = future_return_pct
    df_sym["label_threshold_pct"] = threshold_used_pct
    df_sym["target"] = y

    return df_sym[df_sym["target"].isin([0, 1])].copy()


def build_labeled_dataset(df: pd.DataFrame, cfg: TrainConfig) -> pd.DataFrame:
    chunks = []
    for (tier, sym), df_sym in df.groupby(["source_tier", "symbol"], sort=False):
        labeled = label_one_symbol(df_sym, cfg)
        if labeled.empty:
            log(f"⚪ Bỏ {tier}/{sym}: không đủ mẫu sau labeling")
            continue
        short_n = int((labeled["target"] == 0).sum())
        long_n = int((labeled["target"] == 1).sum())
        avg_th = float(labeled["label_threshold_pct"].mean())
        log(f"🏷️ {tier}/{sym}: labeled={len(labeled):,} SHORT={short_n:,} LONG={long_n:,} avg_th={avg_th:.3f}%")
        chunks.append(labeled)

    if not chunks:
        raise ValueError("Không tạo được labeled dataset. Hãy giảm --min-return hoặc kiểm tra dữ liệu giá.")

    full = pd.concat(chunks, ignore_index=True).sort_values("timestamp_ms").reset_index(drop=True)
    short_total = int((full["target"] == 0).sum())
    long_total = int((full["target"] == 1).sum())
    log(f"✅ Tổng labeled dataset: {len(full):,} | SHORT={short_total:,} | LONG={long_total:,}")
    log("📊 Labeled rows by tier:")
    log(str(full["source_tier"].value_counts()))

    if len(full) < cfg.min_total_samples:
        raise ValueError(f"Dataset quá nhỏ: {len(full)} samples < {cfg.min_total_samples}.")
    return full


# ============================================================
# 9. BALANCING / WEIGHTS
# ============================================================
def soft_cap_by_group(df: pd.DataFrame, group_col: str, max_ratio: Optional[float], seed: int) -> pd.DataFrame:
    if max_ratio is None or max_ratio <= 0 or df.empty:
        return df
    rng = np.random.default_rng(seed)
    total = len(df)
    max_count = int(total * max_ratio)
    if max_count <= 0:
        return df

    kept_indices = []
    for group, part in df.groupby(group_col):
        if len(part) <= max_count:
            kept_indices.extend(part.index.values)
        else:
            sampled = rng.choice(part.index.values, size=max_count, replace=False)
            kept_indices.extend(sampled)
            log(f"⚖️ Soft cap {group_col}={group}: {len(part):,} -> {max_count:,}")

    return df.loc[kept_indices].sort_values("timestamp_ms").reset_index(drop=True)


def apply_optional_undersample(df: pd.DataFrame, cfg: TrainConfig) -> pd.DataFrame:
    if not cfg.use_undersample:
        return df
    rng = np.random.default_rng(cfg.random_seed)
    short_df = df[df["target"] == 0]
    long_df = df[df["target"] == 1]
    if short_df.empty or long_df.empty:
        return df

    min_count = min(len(short_df), len(long_df))
    max_keep = int(min_count * cfg.undersample_ratio)
    short_keep = min(len(short_df), max_keep)
    long_keep = min(len(long_df), max_keep)

    short_idx = rng.choice(short_df.index.values, size=short_keep, replace=False)
    long_idx = rng.choice(long_df.index.values, size=long_keep, replace=False)
    balanced = df.loc[np.concatenate([short_idx, long_idx])].sort_values("timestamp_ms").reset_index(drop=True)
    log(f"⚖️ Undersample: {len(df):,} -> {len(balanced):,}")
    return balanced


def compute_sample_weights(df: pd.DataFrame, cfg: TrainConfig) -> np.ndarray:
    y = df["target"].values.astype(int)
    total = len(y)

    short_count = max(1, int((y == 0).sum()))
    long_count = max(1, int((y == 1).sum()))
    class_weight = {
        0: total / (2.0 * short_count),
        1: total / (2.0 * long_count),
    }

    symbol_counts = df["symbol"].value_counts().to_dict()
    avg_per_symbol = total / max(1, len(symbol_counts))

    tier_counts = df["source_tier"].value_counts().to_dict()
    avg_per_tier = total / max(1, len(tier_counts))
    tier_weight_map = cfg.tier_weights()

    weights = np.ones(total, dtype=float)
    for i, (target, symbol, tier) in enumerate(zip(y, df["symbol"].values, df["source_tier"].values)):
        cw = class_weight[int(target)]
        sw = np.sqrt(avg_per_symbol / max(1, symbol_counts.get(symbol, 1)))
        tbalance = np.sqrt(avg_per_tier / max(1, tier_counts.get(tier, 1)))
        tbusiness = tier_weight_map.get(str(tier).lower(), 1.0)
        weights[i] = cw * sw * tbalance * tbusiness

    # Mẫu có future return rõ hơn được tăng nhẹ trọng số, không quá cực đoan.
    ret_abs = df["future_return_pct"].abs().clip(upper=2.0).values
    strength = 1.0 + (ret_abs / 2.0) * 0.5
    weights *= strength
    weights = weights / np.mean(weights)
    return weights


# ============================================================
# 10. TIME SPLIT
# ============================================================
def time_split(df: pd.DataFrame, cfg: TrainConfig) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    df = df.sort_values("timestamp_ms").reset_index(drop=True)
    n = len(df)
    train_end = int(n * cfg.train_ratio)
    valid_end = int(n * (cfg.train_ratio + cfg.valid_ratio))
    train_df = df.iloc[:train_end].copy()
    valid_df = df.iloc[train_end:valid_end].copy()
    test_df = df.iloc[valid_end:].copy()
    if train_df.empty or valid_df.empty or test_df.empty:
        raise ValueError("Train/valid/test split bị rỗng. Cần thêm dữ liệu.")
    return train_df, valid_df, test_df


# ============================================================
# 11. METRICS
# ============================================================
def confusion_matrix_2class(y_true: np.ndarray, y_pred: np.ndarray) -> Dict:
    out = {
        "SHORT": {"pred_SHORT": 0, "pred_LONG": 0},
        "LONG": {"pred_SHORT": 0, "pred_LONG": 0},
    }
    for yt, yp in zip(y_true, y_pred):
        row = "SHORT" if int(yt) == 0 else "LONG"
        col = "pred_SHORT" if int(yp) == 0 else "pred_LONG"
        out[row][col] += 1
    return out


def tier_symbol_summary(df: pd.DataFrame) -> Dict:
    out = {
        "by_tier": df["source_tier"].value_counts().to_dict(),
        "by_symbol_top20": df["symbol"].value_counts().head(20).to_dict(),
        "by_tier_and_target": {},
    }
    for (tier, target), part in df.groupby(["source_tier", "target"]):
        side = CLASS_MAP.get(int(target), str(target))
        out["by_tier_and_target"][f"{tier}_{side}"] = int(len(part))
    return out


def evaluate_model(model: xgb.Booster, df: pd.DataFrame, split_name: str) -> Dict:
    X = df[FEATURES].values.astype(np.float32)
    y = df["target"].values.astype(int)
    dmat = xgb.DMatrix(X)
    proba = model.predict(dmat)

    if proba.ndim == 1:
        proba_long = proba
        proba_short = 1.0 - proba_long
        proba = np.vstack([proba_short, proba_long]).T

    pred = np.argmax(proba, axis=1)
    accuracy = float((pred == y).mean())
    short_ratio = float((pred == 0).mean())
    long_ratio = float((pred == 1).mean())

    pred_short_ret = float(df.loc[pred == 0, "future_return_pct"].mean()) if np.any(pred == 0) else None
    pred_long_ret = float(df.loc[pred == 1, "future_return_pct"].mean()) if np.any(pred == 1) else None

    return {
        "split": split_name,
        "samples": int(len(df)),
        "accuracy": accuracy,
        "pred_short_ratio": short_ratio,
        "pred_long_ratio": long_ratio,
        "avg_prob_short": float(np.mean(proba[:, 0])),
        "avg_prob_long": float(np.mean(proba[:, 1])),
        "avg_future_return_when_pred_short": pred_short_ret,
        "avg_future_return_when_pred_long": pred_long_ret,
        "confusion_matrix": confusion_matrix_2class(y, pred),
        "data_summary": tier_symbol_summary(df),
    }


# ============================================================
# 12. TRAINER
# ============================================================
class UniversalLiveTrainerV3Canonical:
    def __init__(self, cfg: TrainConfig):
        self.cfg = cfg
        self.model: Optional[xgb.Booster] = None
        self.report: Dict = {}
        self.normalization_report: Dict = {}
        self.base_feature_stats: Dict = {}
        self.canonical_feature_stats: Dict = {}

        self.params = {
            "objective": "multi:softprob",
            "num_class": 2,
            "eval_metric": ["mlogloss", "merror"],
            "max_depth": 5,
            "learning_rate": 0.032,
            "subsample": 0.85,
            "colsample_bytree": 0.85,
            "min_child_weight": 10,
            "gamma": 0.25,
            "reg_alpha": 0.08,
            "reg_lambda": 1.8,
            "tree_method": "hist",
            "seed": cfg.random_seed,
            "missing": np.nan,
        }

    def load_raw_data(self) -> pd.DataFrame:
        if self.cfg.source == "csv":
            return load_from_csv(self.cfg)
        if self.cfg.source == "mongo":
            return load_from_all_mongo_tiers(self.cfg)
        raise ValueError("source không hợp lệ. Dùng mongo hoặc csv")

    def build_dataset(self) -> pd.DataFrame:
        raw = self.load_raw_data()
        base = prepare_base_dataframe(raw, self.cfg)
        self.base_feature_stats = {
            "overall": feature_stats(base),
            "by_tier": feature_stats_by_group(base, "source_tier"),
        }

        canonical, norm_report = apply_canonical_normalization(base, self.cfg)
        self.normalization_report = norm_report
        self.canonical_feature_stats = {
            "overall": feature_stats(canonical),
            "by_tier": feature_stats_by_group(canonical, "source_tier"),
        }

        labeled = build_labeled_dataset(canonical, self.cfg)
        labeled = soft_cap_by_group(labeled, "source_tier", self.cfg.max_tier_ratio, self.cfg.random_seed)
        labeled = soft_cap_by_group(labeled, "symbol", self.cfg.max_symbol_ratio, self.cfg.random_seed + 1)
        labeled = apply_optional_undersample(labeled, self.cfg)

        log("📊 Final labeled rows by tier:")
        log(str(labeled["source_tier"].value_counts()))
        log("📊 Final labeled rows by target:")
        log(str(labeled["target"].map(CLASS_MAP).value_counts()))
        return labeled

    def train(self) -> None:
        log("\n" + "=" * 70)
        log("🔥 UNIVERSAL LIVE TRAINER V3 CANONICAL - MULTI-TIER 2 CLASS")
        log("=" * 70)
        log(f"⚙️ tiers={self.cfg.tiers} | tier_weights={self.cfg.tier_weights()}")
        log(f"⚙️ horizon={self.cfg.horizon_seconds}s | min_return={self.cfg.min_return_pct}% | dynamic_threshold={self.cfg.use_dynamic_threshold}")
        log(f"⚙️ canonical_normalization={self.cfg.canonical_normalization}")
        log(f"⚙️ output: {self.cfg.output_onnx}")

        df = self.build_dataset()
        if len(df) < self.cfg.min_total_samples:
            raise ValueError(f"Dataset quá nhỏ sau xử lý: {len(df)} < {self.cfg.min_total_samples}")

        short_total = int((df["target"] == 0).sum())
        long_total = int((df["target"] == 1).sum())
        total = len(df)
        short_ratio = short_total / total
        long_ratio = long_total / total

        train_df, valid_df, test_df = time_split(df, self.cfg)
        w_train = compute_sample_weights(train_df, self.cfg)
        w_valid = compute_sample_weights(valid_df, self.cfg)

        X_train = train_df[FEATURES].values.astype(np.float32)
        y_train = train_df["target"].values.astype(int)
        X_valid = valid_df[FEATURES].values.astype(np.float32)
        y_valid = valid_df["target"].values.astype(int)

        dtrain = xgb.DMatrix(X_train, label=y_train, weight=w_train)
        dvalid = xgb.DMatrix(X_valid, label=y_valid, weight=w_valid)

        log(f"🚀 Train shape={X_train.shape} | Valid shape={X_valid.shape} | Test shape={(len(test_df), len(FEATURES))}")
        log(f"⚖️ Label balance full: SHORT={short_total:,} ({short_ratio:.1%}) | LONG={long_total:,} ({long_ratio:.1%})")
        log("📊 Dataset by tier:")
        log(str(df["source_tier"].value_counts()))

        evals = [(dtrain, "train"), (dvalid, "valid")]
        self.model = xgb.train(
            self.params,
            dtrain,
            num_boost_round=self.cfg.num_boost_round,
            evals=evals,
            early_stopping_rounds=self.cfg.early_stopping_rounds,
            verbose_eval=25,
        )

        self.model.save_model(self.cfg.output_xgb)
        log(f"✅ Đã lưu XGBoost model: {self.cfg.output_xgb}")

        train_report = evaluate_model(self.model, train_df, "train")
        valid_report = evaluate_model(self.model, valid_df, "valid")
        test_report = evaluate_model(self.model, test_df, "test")

        warnings_list = []
        for rep in [train_report, valid_report, test_report]:
            max_bias = max(rep["pred_short_ratio"], rep["pred_long_ratio"])
            if max_bias > self.cfg.max_bias_ratio_warn:
                warnings_list.append(
                    f"{rep['split']} prediction bias cao: SHORT={rep['pred_short_ratio']:.1%}, LONG={rep['pred_long_ratio']:.1%}"
                )

        # Sanity: pred LONG thì return trung bình nên dương, pred SHORT thì nên âm.
        if test_report["avg_future_return_when_pred_long"] is not None and test_report["avg_future_return_when_pred_long"] < 0:
            warnings_list.append("TEST: avg_future_return_when_pred_long < 0, model LONG có dấu hiệu ngược hướng.")
        if test_report["avg_future_return_when_pred_short"] is not None and test_report["avg_future_return_when_pred_short"] > 0:
            warnings_list.append("TEST: avg_future_return_when_pred_short > 0, model SHORT có dấu hiệu ngược hướng.")

        self.report = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "class_map": CLASS_MAP,
            "features": FEATURES,
            "config": asdict(self.cfg),
            "mongo_tiers": {
                "enabled_tiers": list(self.cfg.tiers),
                "tier_weights": self.cfg.tier_weights(),
                "db_name": self.cfg.db_name,
                "collection": self.cfg.collection or "auto",
            },
            "canonical_normalization": self.normalization_report,
            "xgb_params": self.params,
            "dataset": {
                "total_samples": int(total),
                "symbols": int(df["symbol"].nunique()),
                "tiers": int(df["source_tier"].nunique()),
                "short_samples": short_total,
                "long_samples": long_total,
                "short_ratio": short_ratio,
                "long_ratio": long_ratio,
                "time_min_ms": int(df["timestamp_ms"].min()),
                "time_max_ms": int(df["timestamp_ms"].max()),
                "summary": tier_symbol_summary(df),
            },
            "feature_stats_before_canonical": self.base_feature_stats,
            "feature_stats_after_canonical": self.canonical_feature_stats,
            "splits": {"train": train_report, "valid": valid_report, "test": test_report},
            "warnings": warnings_list,
        }

        self.export_onnx()
        self.export_report()
        self.export_schema()

        log("\n" + "=" * 70)
        log("📊 TRAINING SUMMARY")
        log("=" * 70)
        for rep in [train_report, valid_report, test_report]:
            log(
                f"{rep['split'].upper():5s} | samples={rep['samples']:,} | "
                f"acc={rep['accuracy']:.4f} | "
                f"pred SHORT={rep['pred_short_ratio']:.1%} LONG={rep['pred_long_ratio']:.1%} | "
                f"retS={rep['avg_future_return_when_pred_short']} retL={rep['avg_future_return_when_pred_long']}"
            )

        if warnings_list:
            log("\n⚠️ WARNINGS:")
            for w in warnings_list:
                log(f"- {w}")
        else:
            log("\n✅ Không phát hiện bias hoặc hướng ngược nghiêm trọng.")

        log("\n✅ HOÀN TẤT TRAINER V3 CANONICAL")

    def export_onnx(self) -> None:
        if self.model is None:
            raise RuntimeError("Model chưa được train")
        log("⚙️ Đang xuất ONNX 2-class [SHORT, LONG]...")
        initial_type = [("float_input", FloatTensorType([None, len(FEATURES)]))]
        onnx_model = onnxmltools.convert_xgboost(self.model, initial_types=initial_type, target_opset=13)
        with open(self.cfg.output_onnx, "wb") as f:
            f.write(onnx_model.SerializeToString())
        log(f"✅ Đã xuất ONNX: {self.cfg.output_onnx}")

    def export_report(self) -> None:
        with open(self.cfg.output_report, "w", encoding="utf-8") as f:
            json.dump(self.report, f, ensure_ascii=False, indent=2)
        log(f"✅ Đã lưu report: {self.cfg.output_report}")

    def export_schema(self) -> None:
        schema = {
            "input_name": "float_input",
            "input_shape": [None, len(FEATURES)],
            "features": FEATURES,
            "output_classes": {"0": "SHORT", "1": "LONG"},
            "order_manager_mapping": {"probShort": "probs[0]", "probLong": "probs[1]"},
            "multi_tier": {
                "enabled_tiers": list(self.cfg.tiers),
                "tier_weights": self.cfg.tier_weights(),
            },
            "canonical_normalization_required_in_live": True,
            "canonical_rules": self.normalization_report,
            "note": (
                "Model 2 class, train bằng canonical features. "
                "Live DataFeed/OrderManager phải đưa 13 feature cùng scale canonical, "
                "đặc biệt các field volume/orderbook/liquidation phải là log1p(ratio so với quote_volume_ref)."
            ),
        }
        with open(self.cfg.output_schema, "w", encoding="utf-8") as f:
            json.dump(schema, f, ensure_ascii=False, indent=2)
        log(f"✅ Đã lưu schema: {self.cfg.output_schema}")


# ============================================================
# 13. CLI
# ============================================================
def parse_tiers(value: str) -> Tuple[str, ...]:
    tiers = tuple([x.strip().lower() for x in value.split(",") if x.strip()])
    valid = {"tier1", "tier2", "scout"}
    bad = [t for t in tiers if t not in valid]
    if bad:
        raise argparse.ArgumentTypeError(f"Tier không hợp lệ: {bad}. Chỉ dùng tier1,tier2,scout")
    return tiers


def none_if_negative(value: float) -> Optional[float]:
    return None if value < 0 else value


def parse_args() -> TrainConfig:
    parser = argparse.ArgumentParser(description="Universal Live Trainer V3 Canonical - Multi-tier 2-class SHORT/LONG")
    parser.add_argument("mode", nargs="?", default="train", choices=["train"])

    parser.add_argument("--source", default="mongo", choices=["mongo", "csv"])
    parser.add_argument("--db", default=DEFAULT_DB_NAME)
    parser.add_argument("--collection", default=None)
    parser.add_argument("--csv", default=None)

    parser.add_argument("--tiers", type=parse_tiers, default=("tier1", "tier2", "scout"))
    parser.add_argument("--tier1-weight", type=float, default=DEFAULT_TIER_WEIGHTS["tier1"])
    parser.add_argument("--tier2-weight", type=float, default=DEFAULT_TIER_WEIGHTS["tier2"])
    parser.add_argument("--scout-weight", type=float, default=DEFAULT_TIER_WEIGHTS["scout"])

    parser.add_argument("--horizon", type=int, default=180)
    parser.add_argument("--min-return", type=float, default=0.25)
    parser.add_argument("--max-abs-return", type=float, default=8.0)

    parser.add_argument("--dynamic-threshold", action="store_true", default=True)
    parser.add_argument("--no-dynamic-threshold", dest="dynamic_threshold", action="store_false")
    parser.add_argument("--spread-multiplier", type=float, default=3.0)
    parser.add_argument("--min-effective-return", type=float, default=0.18)

    parser.add_argument("--min-rows-per-symbol", type=int, default=300)
    parser.add_argument("--min-total-samples", type=int, default=1000)

    parser.add_argument("--onnx", default="Universal_Scout.onnx")
    parser.add_argument("--xgb", default="model_universal_canonical_v3.xgb")
    parser.add_argument("--report", default="training_report_canonical_v3.json")
    parser.add_argument("--schema", default="feature_schema_canonical_v3.json")

    parser.add_argument("--boost-rounds", type=int, default=600)
    parser.add_argument("--early-stop", type=int, default=50)
    parser.add_argument("--seed", type=int, default=42)

    parser.add_argument("--undersample", action="store_true")
    parser.add_argument("--max-tier-ratio", type=float, default=0.60, help="-1 để tắt")
    parser.add_argument("--max-symbol-ratio", type=float, default=0.15, help="-1 để tắt")

    parser.add_argument("--no-canonical", dest="canonical", action="store_false", default=True)
    parser.add_argument("--quote-ref-window", type=int, default=60)
    parser.add_argument("--raw-notional-detect-q95", type=float, default=5.0)

    args = parser.parse_args()

    return TrainConfig(
        source=args.source,
        db_name=args.db,
        collection=args.collection,
        csv_path=args.csv,
        tiers=args.tiers,
        tier1_weight=args.tier1_weight,
        tier2_weight=args.tier2_weight,
        scout_weight=args.scout_weight,
        horizon_seconds=args.horizon,
        min_return_pct=args.min_return,
        max_abs_return_pct=args.max_abs_return,
        use_dynamic_threshold=args.dynamic_threshold,
        spread_multiplier=args.spread_multiplier,
        min_effective_return_pct=args.min_effective_return,
        min_rows_per_symbol=args.min_rows_per_symbol,
        min_total_samples=args.min_total_samples,
        output_onnx=args.onnx,
        output_xgb=args.xgb,
        output_report=args.report,
        output_schema=args.schema,
        random_seed=args.seed,
        num_boost_round=args.boost_rounds,
        early_stopping_rounds=args.early_stop,
        use_undersample=args.undersample,
        max_tier_ratio=none_if_negative(args.max_tier_ratio),
        max_symbol_ratio=none_if_negative(args.max_symbol_ratio),
        canonical_normalization=args.canonical,
        quote_ref_window=args.quote_ref_window,
        raw_notional_detect_q95=args.raw_notional_detect_q95,
    )


if __name__ == "__main__":
    try:
        cfg = parse_args()
        trainer = UniversalLiveTrainerV3Canonical(cfg)
        trainer.train()
    except KeyboardInterrupt:
        log("\n🛑 Dừng trainer theo yêu cầu người dùng.")
        sys.exit(130)
    except Exception as e:
        log(f"\n❌ TRAINER FAILED: {e}")
        sys.exit(1)

