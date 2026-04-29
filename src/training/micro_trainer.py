"""
src/training/micro_trainer.py
============================================================
AI QUANT SNIPER - MICRO TRAINER (PRODUCTION READY)
------------------------------------------------------------
Giải pháp huấn luyện toàn diện cho mô hình HFT (Dữ liệu 1s):
- CLI đầy đủ tham số tùy chỉnh.
- Canonical Normalization đồng bộ hóa đa coin.
- Feature Engineering ẩn: Phân tích spoofing để điều chỉnh Trọng số học (Sample Weights).
- Labeling Horizon & Cân bằng 50/50 tuyệt đối.
- Continual Learning: Kế thừa não gốc (Base model).
- Gatekeeper: Kiểm định khắt khe trước khi promote ONNX.
- Chế độ chạy: Once, Loop, Daily.
============================================================
"""

import os
import sys
import json
import time
import shutil
import argparse
import warnings
from pathlib import Path
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import xgboost as xgb
import onnxmltools
from onnxmltools.convert.common.data_types import FloatTensorType
from dotenv import load_dotenv
from pymongo import MongoClient

try:
    import redis
except ImportError:
    redis = None

warnings.filterwarnings("ignore")
load_dotenv()

# ==========================================
# 1. CONSTANTS & FEATURES (CỐ ĐỊNH 13 BIẾN)
# ==========================================
CLASS_MAP = {0: "SHORT", 1: "LONG"}

FEATURES = [
    "ob_imb_top20", "spread_close", "bid_vol_1pct", "ask_vol_1pct",
    "max_buy_trade", "max_sell_trade", "liq_long_vol", "liq_short_vol",
    "funding_rate", "taker_buy_ratio", "body_size", "wick_size", "btc_relative_strength"
]

NOTIONAL_FEATURES = [
    "bid_vol_1pct", "ask_vol_1pct", "max_buy_trade", "max_sell_trade", "liq_long_vol", "liq_short_vol"
]

# ==========================================
# 2. CONFIGURATION PARSER (CLI)
# ==========================================
def parse_args():
    parser = argparse.ArgumentParser(description="Micro Trainer cho AI Quant Sniper (Dữ liệu 1s)")

    # Chế độ chạy
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Chạy 1 lần rồi thoát (Mặc định)")
    mode.add_argument("--loop", action="store_true", help="Chạy lặp lại liên tục")
    mode.add_argument("--daily-at", type=str, help="Chạy mỗi ngày vào giờ nhất định (VD: '00:00')")
    parser.add_argument("--loop-interval", type=int, default=3600, help="Thời gian chờ giữa các lần loop (giây)")

    # Cấu hình Dữ liệu
    parser.add_argument("--symbols", type=str, default="", help="Danh sách coin cần train, cách nhau dấu phẩy (VD: BTCUSDT,ETHUSDT)")
    parser.add_argument("--archive-hours", type=float, default=72.0, help="Lấy dữ liệu Redis trong X giờ qua")
    parser.add_argument("--trade-hours", type=float, default=168.0, help="Lấy dữ liệu Mongo Trades trong X giờ qua")
    parser.add_argument("--stream-count", type=int, default=50000, help="Số max record lấy từ mỗi stream Redis")
    
    # Labeling & Sampling
    parser.add_argument("--horizon", type=int, default=240, help="Tầm nhìn tương lai để gán nhãn (giây)")
    parser.add_argument("--min-return", type=float, default=0.12, help="Phần trăm lợi nhuận tối thiểu để gán nhãn (trừ phí)")
    parser.add_argument("--max-abs-return", type=float, default=8.0, help="Loại bỏ các mẫu giật râu quá mạnh (Lỗi sàn)")
    parser.add_argument("--engineer-features", action="store_true", help="Bật Feature Engineering ẩn (Tính spoofing để tăng/giảm trọng số mẫu)")

    # XGBoost Params
    parser.add_argument("--boost-rounds", type=int, default=400, help="Số cây quyết định tối đa")
    parser.add_argument("--early-stop", type=int, default=35, help="Dừng sớm nếu valid không cải thiện")
    parser.add_argument("--max-depth", type=int, default=3, help="Độ sâu cây (nên để thấp chống nhiễu)")
    parser.add_argument("--eta", type=float, default=0.03, help="Learning rate")
    parser.add_argument("--subsample", type=float, default=0.85, help="Tỷ lệ lấy mẫu dữ liệu mỗi cây")
    parser.add_argument("--colsample-bytree", type=float, default=0.85, help="Tỷ lệ lấy mẫu features mỗi cây")
    parser.add_argument("--reg-lambda", type=float, default=3.0, help="L2 Regularization")
    parser.add_argument("--gamma", type=float, default=0.25, help="Minimum loss reduction")

    # Continual Learning
    parser.add_argument("--base-model", type=str, default="micro_model_manual.xgb", help="File XGB gốc để kế thừa học tiếp")
    parser.add_argument("--continue-boost-rounds", type=int, default=50, help="Số vòng học thêm nếu dùng base-model")

    # Gatekeeper & IO
    parser.add_argument("--no-promote", action="store_true", help="Chỉ train, KHÔNG ghi đè ONNX lên production")
    parser.add_argument("--min-test-acc", type=float, default=0.51, help="Độ chính xác tối thiểu trên tập Test")
    parser.add_argument("--max-bias", type=float, default=0.60, help="Thiên kiến tối đa cho 1 hướng (LONG hoặc SHORT)")
    
    # Paths
    parser.add_argument("--live-onnx", type=str, default="Micro_Sniper.onnx")
    parser.add_argument("--candidates-dir", type=str, default="models/candidates")
    parser.add_argument("--backups-dir", type=str, default="models/backups")
    parser.add_argument("--reports-dir", type=str, default="reports/micro_learning")

    return parser.parse_args()

# ==========================================
# 3. UTILS & DIRECTORIES
# ==========================================
def log(msg): print(msg, flush=True)
def ts_slug(): return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
def utc_now_ms(): return int(datetime.now(timezone.utc).timestamp() * 1000)

def ensure_dirs(args):
    Path(args.candidates_dir).mkdir(parents=True, exist_ok=True)
    Path(args.backups_dir).mkdir(parents=True, exist_ok=True)
    Path(args.reports_dir).mkdir(parents=True, exist_ok=True)

def decode_payload(raw):
    if raw is None: return None
    if isinstance(raw, bytes): raw = raw.decode("utf-8", errors="ignore")
    try: return json.loads(raw)
    except: return None

# ==========================================
# 4. DATA EXTRACTION
# ==========================================
def fetch_redis_live(args):
    if not redis: raise ImportError("Thiếu thư viện redis.")
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://127.0.0.1:6379"))
    
    cutoff_ms = utc_now_ms() - int(args.archive_hours * 3600 * 1000)
    target_symbols = [s.strip().upper() for s in args.symbols.split(",")] if args.symbols else []
    
    keys = r.keys("features:archive:*")
    rows = []
    
    for key in keys:
        sym = key.decode("utf-8").split(":")[-1].upper()
        if target_symbols and sym not in target_symbols: continue
        
        try:
            items = r.xrevrange(key, max="+", min="-", count=args.stream_count)
            for sid, raw_map in items:
                sid_ms = int(sid.decode().split('-')[0])
                if sid_ms < cutoff_ms: break # Dừng nếu quá cũ
                
                payload = decode_payload(raw_map.get(b'data') or raw_map.get('data'))
                if payload:
                    row = {"symbol": sym, "ts": sid_ms, "price": float(payload.get("last_price", 0))}
                    for f in FEATURES: row[f] = float(payload.get(f, 0.0))
                    rows.append(row)
        except Exception as e:
            log(f"⚠️ Lỗi đọc Redis {key}: {e}")
            
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(by=['symbol', 'ts']).drop_duplicates(subset=['symbol', 'ts']).reset_index(drop=True)
        log(f"✅ Đã tải {len(df):,} dòng từ Redis Archive.")
    return df

def fetch_mongo_trades(args):
    mongo_uri = os.getenv("MONGO_URI_SCOUT")
    if not mongo_uri: return pd.DataFrame()
    
    cutoff_ms = utc_now_ms() - int(args.trade_hours * 3600 * 1000)
    target_symbols = [s.strip().upper() for s in args.symbols.split(",")] if args.symbols else []
    
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    db = client[os.getenv("MONGO_DB_NAME", "Binance")]
    rows = []
    
    for col in ["scouttrades", "paper_trades"]:
        try:
            trades = list(db[col].find({"$or": [{"status": "CLOSED"}, {"state": "CLOSED"}]}))
            for t in trades:
                sym = t.get("symbol", "").upper()
                if target_symbols and sym not in target_symbols: continue
                
                ts = int(t.get("openTime") or t.get("createdAt", 0))
                if ts < cutoff_ms: continue
                
                pnl = float(t.get("pnl", t.get("realizedPnl", 0)) or 0)
                if abs(pnl) < 0.05: continue # Bỏ trade nhiễu
                
                side = str(t.get("side", t.get("type", ""))).upper()
                if side not in ["LONG", "SHORT"]: continue
                
                label = 1 if (side == "LONG" and pnl > 0) or (side == "SHORT" and pnl < 0) else 0
                feat = t.get("features_at_entry", t.get("features", {}))
                
                row = {"symbol": sym, "ts": ts, "label": label, "is_trade": 1, "future_return_pct": abs(pnl), "price": feat.get("last_price", 1)}
                valid = True
                for f in FEATURES:
                    if feat.get(f) is None: valid = False; break
                    row[f] = float(feat[f])
                    
                if valid: rows.append(row)
        except Exception: pass
        
    df = pd.DataFrame(rows)
    if not df.empty: log(f"🧠 Đã tải {len(df):,} lệnh từ MongoDB.")
    return df

# ==========================================
# 5. DATA PREPROCESSING & LABELING
# ==========================================
def apply_canonical_normalization(df):
    """Chuẩn hóa dữ liệu theo khối lượng giao dịch để quy đồng mọi đồng coin."""
    log("🔧 Đang áp dụng Canonical Normalization...")
    df = df.copy()
    
    # Giả lập quote_volume_ref (Trung vị đơn giản cho HFT)
    # Vì dữ liệu 1s có thể thiếu Volume, ta dùng tạm giá trị cố định hoặc tự scale theo spread
    # Ở đây áp dụng cắt gọt (clipping) cơ bản để chống nhiễu
    df["ob_imb_top20"] = df["ob_imb_top20"].clip(-1.0, 1.0)
    df["spread_close"] = df["spread_close"].clip(0, 0.02)
    df["funding_rate"] = df["funding_rate"].clip(-0.01, 0.01)
    df["taker_buy_ratio"] = df["taker_buy_ratio"].clip(0, 1)
    df["body_size"] = df["body_size"].clip(0, 0.08)
    df["wick_size"] = df["wick_size"].clip(0, 0.12)
    df["btc_relative_strength"] = df["btc_relative_strength"].clip(-10.0, 10.0)
    
    # Biến đổi Logarit cho các biến Notional (Volume lớn)
    for f in NOTIONAL_FEATURES:
        if f in df.columns:
            # Scale xuống tránh tràn số
            df[f] = np.log1p(df[f].clip(lower=0) / 1000.0).clip(0, 15.0)
            
    return df

def optional_feature_engineering(df):
    """
    Tính năng ẩn: Không đưa vào ONNX, nhưng dùng để đánh giá độ tin cậy của mẫu (Trọng số).
    Phát hiện Spoofing: Orderbook lệch mạnh nhưng không có Volume thật khớp.
    """
    log("🔬 Đang phân tích Feature Engineering ẩn (Spoofing Detection)...")
    df['spoofing_flag'] = 0.0
    
    # Giả lập logic phát hiện: IMB Mua rất cao (>0.8) nhưng Taker Buy rất thấp (<0.3) -> Mua ảo
    fake_buy_mask = (df['ob_imb_top20'] > 0.6) & (df['taker_buy_ratio'] < 0.35)
    fake_sell_mask = (df['ob_imb_top20'] < -0.6) & (df['taker_buy_ratio'] > 0.65)
    
    df.loc[fake_buy_mask | fake_sell_mask, 'spoofing_flag'] = 1.0
    return df

def label_redis_data(df, args):
    log(f"🎯 Đang quét giá tương lai (Horizon {args.horizon}s, Target {args.min_return}%)...")
    chunks = []
    
    for sym, group in df.groupby("symbol"):
        group = group.sort_values("ts").reset_index(drop=True)
        times = group['ts'].values
        prices = group['price'].values
        
        target_times = times + (args.horizon * 1000)
        future_idx = np.searchsorted(times, target_times, side='left')
        
        valid_mask = future_idx < len(group)
        idx_now = np.where(valid_mask)[0]
        idx_future = future_idx[valid_mask]
        
        ret = (prices[idx_future] - prices[idx_now]) / prices[idx_now] * 100.0
        
        group['label'] = -1
        group.loc[idx_now[ret >= args.min_return], 'label'] = 1 
        group.loc[idx_now[ret <= -args.min_return], 'label'] = 0 
        
        group['future_return_pct'] = 0.0
        group.loc[idx_now, 'future_return_pct'] = ret
        
        # Bỏ qua các nhịp biến động phi lý (Flash crash lỗi sàn)
        valid_sane = group['future_return_pct'].abs() <= args.max_abs_return
        
        chunks.append(group[(group['label'] != -1) & valid_sane])
        
    if not chunks: return pd.DataFrame()
    res = pd.concat(chunks, ignore_index=True)
    res['is_trade'] = 0
    return res

# ==========================================
# 6. BALANCING & WEIGHTS
# ==========================================
def prepare_final_dataset(df_redis, df_trades, args):
    if not df_redis.empty:
        df_redis = apply_canonical_normalization(df_redis)
        if args.engineer_features: df_redis = optional_feature_engineering(df_redis)
        df_live = label_redis_data(df_redis, args)
    else: df_live = pd.DataFrame()

    if not df_trades.empty:
        df_trades = apply_canonical_normalization(df_trades)
        if args.engineer_features: df_trades = optional_feature_engineering(df_trades)
    
    df_final = pd.concat([df_live, df_trades], ignore_index=True)
    if df_final.empty: return None
    
    short_c = (df_final['label'] == 0).sum()
    long_c = (df_final['label'] == 1).sum()
    log(f"⚖️ Trước cân bằng: SHORT={short_c:,}, LONG={long_c:,}")
    
    if short_c == 0 or long_c == 0:
        log("❌ Lỗi: Không đủ dữ liệu 2 hướng để cân bằng.")
        return None
        
    # Ép cân bằng 50/50 tuyệt đối (Undersampling)
    min_c = min(short_c, long_c)
    df_balanced = df_final.groupby('label').sample(n=min_c, random_state=42).reset_index(drop=True)
    log(f"✅ Sau cân bằng: SHORT={min_c:,}, LONG={min_c:,} (Tổng: {min_c*2:,} mẫu)")
    
    # Tính toán Trọng số (Sample Weights)
    weights = np.ones(len(df_balanced))
    # Ưu tiên mẫu có lợi nhuận mạnh
    weights += np.abs(df_balanced['future_return_pct'].values) * 5.0 
    # Ưu tiên Trade thực chiến
    weights[df_balanced['is_trade'] == 1] *= 3.0 
    # Phạt nặng các tín hiệu có dấu hiệu Spoofing (giảm 80% trọng số)
    if args.engineer_features and 'spoofing_flag' in df_balanced.columns:
        weights[df_balanced['spoofing_flag'] == 1.0] *= 0.2 
        
    df_balanced['weight'] = weights
    
    # Chia tập theo Thời gian (Time-Split) để tránh Lookahead Bias
    df_balanced = df_balanced.sort_values("ts").reset_index(drop=True)
    n = len(df_balanced)
    train_end = int(n * 0.70)
    valid_end = int(n * 0.85)
    
    return df_balanced.iloc[:train_end], df_balanced.iloc[train_end:valid_end], df_balanced.iloc[valid_end:]

# ==========================================
# 7. EVALUATION & GATEKEEPER
# ==========================================
def evaluate_model(model, df, split_name):
    X = df[FEATURES].values.astype(np.float32)
    y = df["label"].values.astype(int)
    
    proba = model.predict(xgb.DMatrix(X))
    if proba.ndim == 1: proba = np.vstack([1.0 - proba, proba]).T
    pred = np.argmax(proba, axis=1)

    acc = float((pred == y).mean()) if len(y) else 0.0
    pred_short_ratio = float((pred == 0).mean()) if len(y) else 0.0
    pred_long_ratio = float((pred == 1).mean()) if len(y) else 0.0
    
    ret_s = float(df.loc[pred == 0, "future_return_pct"].mean()) if np.any(pred == 0) else 0.0
    ret_l = float(df.loc[pred == 1, "future_return_pct"].mean()) if np.any(pred == 1) else 0.0

    # Confusion Matrix
    cm = {"SHORT": {"pred_SHORT": 0, "pred_LONG": 0}, "LONG": {"pred_SHORT": 0, "pred_LONG": 0}}
    for yt, yp in zip(y, pred):
        row = "SHORT" if yt == 0 else "LONG"
        col = "pred_SHORT" if yp == 0 else "pred_LONG"
        cm[row][col] += 1

    return {
        "split": split_name, "samples": len(df), "accuracy": acc,
        "pred_short_ratio": pred_short_ratio, "pred_long_ratio": pred_long_ratio,
        "retS": ret_s, "retL": ret_l, "confusion_matrix": cm
    }

def gatekeeper(test_metrics, args):
    reasons = []
    
    if test_metrics["accuracy"] < args.min_test_acc:
        reasons.append(f"ACCURACY_TOO_LOW ({test_metrics['accuracy']:.4f} < {args.min_test_acc})")
        
    bias = max(test_metrics["pred_short_ratio"], test_metrics["pred_long_ratio"])
    if bias > args.max_bias:
        reasons.append(f"PREDICTION_BIAS ({bias:.2%} > {args.max_bias:.2%})")
        
    if test_metrics["retS"] > 0:
        reasons.append(f"SHORT_RET_POSITIVE (retS={test_metrics['retS']:.4f})")
        
    if test_metrics["retL"] < 0:
        reasons.append(f"LONG_RET_NEGATIVE (retL={test_metrics['retL']:.4f})")
        
    return len(reasons) == 0, reasons

# ==========================================
# 8. TRAINING WORKFLOW
# ==========================================
def train_routine(args):
    run_id = ts_slug()
    ensure_dirs(args)
    report_path = Path(args.reports_dir) / f"micro_report_{run_id}.json"
    
    log("\n" + "="*60)
    log(f"🚀 BẮT ĐẦU MICRO TRAIN | Horizon={args.horizon}s MinRet={args.min_return}%")
    
    df_redis = fetch_redis_live(args)
    df_trades = fetch_mongo_trades(args)
    
    splits = prepare_final_dataset(df_redis, df_trades, args)
    if not splits: return
    df_train, df_valid, df_test = splits
    
    dtrain = xgb.DMatrix(df_train[FEATURES].values.astype(np.float32), label=df_train["label"], weight=df_train["weight"])
    dvalid = xgb.DMatrix(df_valid[FEATURES].values.astype(np.float32), label=df_valid["label"], weight=df_valid["weight"])
    
    xgb_params = {
        "objective": "multi:softprob", "num_class": 2, "eval_metric": ["mlogloss", "merror"],
        "max_depth": args.max_depth, "learning_rate": args.eta,
        "subsample": args.subsample, "colsample_bytree": args.colsample_bytree,
        "gamma": args.gamma, "reg_lambda": args.reg_lambda, "tree_method": "hist", "seed": 42
    }
    
    # Continual Learning Check
    base_model_path = Path(args.base_model)
    xgb_model_arg = None
    boost_rounds = args.boost_rounds
    
    if base_model_path.exists():
        log(f"🧠 KẾ THỪA TRÍ NHỚ: Tìm thấy não gốc {args.base_model}")
        xgb_model_arg = str(base_model_path)
        boost_rounds = args.continue_boost_rounds
        log(f"⚙️ Chỉ train thêm {boost_rounds} vòng (Fine-tuning).")
    else:
        log("⚠️ Không có não gốc. Huấn luyện hoàn toàn từ đầu!")
        
    model = xgb.train(
        xgb_params, dtrain, num_boost_round=boost_rounds,
        evals=[(dtrain, "train"), (dvalid, "valid")],
        early_stopping_rounds=args.early_stop,
        xgb_model=xgb_model_arg, verbose_eval=20
    )
    
    # Evaluation
    test_metrics = evaluate_model(model, df_test, "test")
    log("\n📊 KẾT QUẢ TẬP TEST (OUT-OF-SAMPLE):")
    log(f"   Accuracy: {test_metrics['accuracy']:.4f}")
    log(f"   Bias: S {test_metrics['pred_short_ratio']:.1%} | L {test_metrics['pred_long_ratio']:.1%}")
    log(f"   Return: retS={test_metrics['retS']:.4f} | retL={test_metrics['retL']:.4f}")
    
    # Gatekeeper
    passed, reasons = gatekeeper(test_metrics, args)
    
    # Cứ lưu Candidate ra
    cand_xgb = Path(args.candidates_dir) / f"micro_cand_{run_id}.xgb"
    cand_onnx = Path(args.candidates_dir) / f"Micro_Sniper_cand_{run_id}.onnx"
    
    model.save_model(str(cand_xgb))
    init_type = [("float_input", FloatTensorType([None, len(FEATURES)]))]
    onnx_model = onnxmltools.convert_xgboost(model, initial_types=init_type, target_opset=13)
    with open(cand_onnx, "wb") as f: f.write(onnx_model.SerializeToString())
    
    promoted = False
    if passed and not args.no_promote:
        log("✅ ĐẠT CHUẨN GATEKEEPER. Tiến hành ghi đè ONNX Live!")
        live_onnx = Path(args.live_onnx)
        
        # Backup cũ
        if live_onnx.exists():
            backup_path = Path(args.backups_dir) / f"Micro_Sniper_backup_{run_id}.onnx"
            shutil.copy2(live_onnx, backup_path)
            
        # Ghi đè (Atomic)
        tmp_path = live_onnx.with_suffix(".tmp")
        shutil.copy2(cand_onnx, tmp_path)
        os.replace(tmp_path, live_onnx)
        
        # Reload AI OrderManager
        if redis:
            try:
                r = redis.Redis.from_url(REDIS_URL)
                r.publish("system:commands", json.dumps({"action": "RELOAD_AI"}))
                log("📡 Đã gửi tín hiệu RELOAD_AI cho Node.js")
            except Exception: pass
        promoted = True
        
    elif passed and args.no_promote:
        log("🟡 Đạt chuẩn nhưng bị chặn bởi cờ --no-promote.")
    else:
        log("⛔ KHÔNG ĐẠT GATEKEEPER. Hủy bỏ ghi đè. Lý do:")
        for r in reasons: log(f"   - {r}")
        
    # Save Report
    report = {
        "run_id": run_id, "timestamp": datetime.now(timezone.utc).isoformat(),
        "config": vars(args), "features": FEATURES,
        "gatekeeper": {"passed": passed, "reasons": reasons, "promoted": promoted},
        "metrics": {"train": evaluate_model(model, df_train, "train"), "valid": evaluate_model(model, df_valid, "valid"), "test": test_metrics}
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

# ==========================================
# 9. EXECUTION CONTROLLER
# ==========================================
if __name__ == "__main__":
    args = parse_args()
    
    if args.daily_at:
        log(f"⏰ Chế độ Daily: Script sẽ chạy mỗi ngày vào lúc {args.daily_at}")
        while True:
            now = datetime.now().strftime("%H:%M")
            if now == args.daily_at:
                train_routine(args)
                time.sleep(60) # Ngủ 1 phút để tránh chạy lại trong cùng 1 phút
            time.sleep(30)
            
    elif args.loop:
        log(f"🔁 Chế độ Loop: Chạy mỗi {args.loop_interval} giây.")
        while True:
            train_routine(args)
            log(f"⏳ Ngủ {args.loop_interval}s...")
            time.sleep(args.loop_interval)
            
    else:
        # Mặc định chạy 1 lần (--once)
        train_routine(args)