"""
incremental_trainer.py - Cỗ máy Tiến hóa (Phần 2 - Bước 5) V17.2
Nhiệm vụ: Học tăng dồn, xử lý Mảng Features an toàn, Quản trị Tree Bloat và Custom Loss.
"""
import pandas as pd
import numpy as np
import xgboost as xgb
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

# Cấu hình tính năng
MODEL_PATH = "model_universal_pro.xgb"
MAX_TREES_CAP = 500 # Ngưỡng tối đa để chống phình to file ONNX

FEATURES = [
    'ob_imb_top20', 'spread_close', 'bid_vol_1pct', 'ask_vol_1pct', 
    'max_buy_trade', 'max_sell_trade', 'liq_long_vol', 'liq_short_vol', 
    'funding_rate', 'taker_buy_ratio', 'body_size', 'wick_size', 'btc_relative_strength'
]

# 🟢 HÀM MẤT MÁT BẤT ĐỐI XỨNG (Custom Asymmetric Loss)
def asymmetric_profit_loss(y_true, y_pred):
    y_pred = y_pred.reshape(-1, 3)
    exp_preds = np.exp(np.clip(y_pred, -15, 15))
    probs = exp_preds / np.sum(exp_preds, axis=1, keepdims=True)
    
    y_true_onehot = np.zeros_like(probs)
    for i, label in enumerate(y_true):
        y_true_onehot[i, int(label)] = 1.0
        
    grad = probs - y_true_onehot
    hess = probs * (1.0 - probs)
    
    # ⚔️ THI THIẾT QUÂN LUẬT: Nhân 15 lần trọng số nếu đoán sai hướng chính
    for i in range(len(y_true)):
        true_label = int(y_true[i])
        if true_label == 1 and probs[i, 0] > 0.4: # Thực tế Long mà đoán Short mạnh
            grad[i, :] *= 15.0
            hess[i, :] *= 15.0
        elif true_label == 0 and probs[i, 1] > 0.4: # Thực tế Short mà đoán Long mạnh
            grad[i, :] *= 15.0
            hess[i, :] *= 15.0
            
    return grad.flatten(), hess.flatten()

def run_incremental_training():
    print("🧠 [Evolution] Đang nạp dữ liệu từ core_memory để tiến hóa...")
    
    client = MongoClient(os.getenv('MONGO_URI_TIER1'))
    db = client['Binance']
    # Sắp xếp lấy 5000 lệnh mới nhất để model luôn bắt nhịp thị trường hiện tại
    core_data = list(db['core_memory'].find().sort("timestamp", -1).limit(5000))
    client.close()

    if not core_data:
        print("⚠️ Không có dữ liệu kinh nghiệm mới. Bỏ qua.")
        return

    # 🟢 VÁ LỖI 1: Xử lý Mảng (List) sang Dictionary trước khi tạo DataFrame
    df_list = []
    for r in core_data:
        feature_list = r['features']
        if len(feature_list) == len(FEATURES):
            # Map từng phần tử trong mảng vào đúng tên cột
            row_dict = {FEATURES[i]: feature_list[i] for i in range(len(FEATURES))}
            row_dict['label'] = r['label']
            df_list.append(row_dict)

    if not df_list:
        print("⚠️ Dữ liệu lỗi cấu trúc features. Bỏ qua.")
        return

    df = pd.DataFrame(df_list)
    X = df[FEATURES].values
    y = df['label'].values

    # 🟢 VÁ LỖI 2: Kích hoạt khiên NaN (missing=np.nan)
    dtrain = xgb.DMatrix(X, label=y, missing=np.nan)
    
    params = {
        'objective': 'multi:softprob',
        'num_class': 3,
        'max_depth': 5,
        'eta': 0.05, # Tăng nhẹ lên 0.05 để 50 cây đủ sức bẻ hướng sai lầm
        'subsample': 0.85,
        'colsample_bytree': 0.8
    }

    # 🟢 VÁ LỖI 3: Quản trị Tree Bloat (Tránh làm chậm Node.js)
    prev_model = None
    if os.path.exists(MODEL_PATH):
        temp_bst = xgb.Booster()
        temp_bst.load_model(MODEL_PATH)
        current_trees = temp_bst.num_boosted_rounds()
        
        if current_trees < MAX_TREES_CAP:
            prev_model = MODEL_PATH
            print(f"🚀 Nối thêm 50 cây vào model cũ (Hiện có: {current_trees} cây)...")
        else:
            print(f"♻️ Model đã đạt {current_trees} cây (Ngưỡng {MAX_TREES_CAP}). Tiến hành đào tạo lại (Retrain) từ đầu bằng Tinh Hoa...")

    # Huấn luyện
    model = xgb.train(
        params, 
        dtrain, 
        num_boost_round=50 if prev_model else 150, # Nếu Retrain thì cho 150 cây nền tảng
        obj=asymmetric_profit_loss, 
        xgb_model=prev_model
    )

    model.save_model(MODEL_PATH)
    print(f"✅ Đã chốt hạ phiên bản tiến hóa: {MODEL_PATH}")

if __name__ == "__main__":
    run_incremental_training()