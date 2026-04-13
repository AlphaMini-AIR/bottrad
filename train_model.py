# train_model.py - Bản Nâng Cao (Multi-Coin)
import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import onnxmltools
from onnxmltools.convert.common.data_types import FloatTensorType

print("🧠 [AI TRAINING] Khởi động Lõi Học Máy XGBoost (Multi-Coin)...")

df = pd.read_csv('dataset_multi_3months.csv')
df = df.sort_values(['symbol', 'timestamp'])

# 1. FEATURE ENGINEERING (Nâng cấp)
def add_features(group):
    group['returns_1m'] = group['close'].pct_change(1)
    group['returns_15m'] = group['close'].pct_change(15)
    group['returns_4h'] = group['close'].pct_change(240)
    # Thêm chỉ số sức mạnh tương đối (so với chính nó trong quá khứ)
    group['ema_20'] = group['close'].ewm(span=20).mean()
    group['rel_strength'] = group['close'] / group['ema_20']
    # Target: 15 phút sau tăng hay giảm
    group['target'] = (group['close'].shift(-15) > group['close']).astype(int)
    return group

df = df.groupby('symbol', group_keys=False).apply(add_features)
df = df.dropna()

features = ['returns_1m', 'returns_15m', 'returns_4h', 'rel_strength']
X = df[features].values
y = df['target'].values

# 2. HUẤN LUYỆN
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.15, shuffle=False)

model = xgb.XGBClassifier(n_estimators=150, max_depth=5, learning_rate=0.03)
print("⏳ Đang luyện não trên dữ liệu 3 tháng...")
model.fit(X_train, y_train)

# 3. ĐÁNH GIÁ & XUẤT ONNX
preds = model.predict(X_test)
print(f"✅ Độ chính xác đạt được: {accuracy_score(y_test, preds) * 100:.2f}%")

initial_type = [('float_input', FloatTensorType([None, len(features)]))]
onnx_model = onnxmltools.convert_xgboost(model, initial_types=initial_type)
with open("ai_quant_sniper.onnx", "wb") as f:
    f.write(onnx_model.SerializeToString())
print("💾 Đã lưu: ai_quant_sniper.onnx")