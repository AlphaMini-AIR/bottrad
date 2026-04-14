# train_master.py
import pandas as pd
import numpy as np
import os

print("🧠 [AI PIPELINE] Khởi tạo bộ máy tính toán Quant Features...")

def calculate_quant_features(df):
    """
    Nhận vào DataFrame chứa OHLVC + Vĩ mô, trả về DF đã gộp 4 Quant Features.
    """
    # Đảm bảo dữ liệu được sắp xếp theo thời gian để tính toán chuỗi (time-series) không bị sai
    df = df.sort_values(by=['symbol', 'timestamp']).copy()

    # Nhóm theo từng coin để tính toán riêng biệt (tránh nến BTC bị dính vào phép tính của ETH)
    def apply_features(group):
        # -------------------------------------------------------------------
        # 1. Hurst Proxy (Variance Ratio)
        # Phát hiện sớm trạng thái thị trường: > 1 là Trending, < 1 là Sideway
        # Công thức: Tỷ lệ biến động 20 nến so với biến động 1 nến
        # -------------------------------------------------------------------
        ret_1m = group['close'].pct_change(1)
        ret_20m = group['close'].pct_change(20)
        # Cộng thêm 1e-8 để tránh lỗi chia cho 0
        group['hurst_proxy'] = ret_20m.rolling(20).std() / (np.sqrt(20) * ret_1m.rolling(20).std() + 1e-8)

        # -------------------------------------------------------------------
        # 2. %_Distance_to_VWAP (Rolling 4h = 240 phút)
        # Tính độ "căng" của giá so với dòng tiền định chế (Volume Weighted Average Price)
        # -------------------------------------------------------------------
        rolling_pv = (group['close'] * group['volume']).rolling(window=240).sum()
        rolling_vol = group['volume'].rolling(window=240).sum() + 1e-8
        group['vwap_4h'] = rolling_pv / rolling_vol
        group['dist_vwap'] = (group['close'] - group['vwap_4h']) / group['vwap_4h']

        # -------------------------------------------------------------------
        # 3. Wick_to_Body & Vol_Acceleration (Cảm biến Fake Liquidation)
        # Wick_to_Body: Râu nến dài gấp mấy lần thân nến?
        # Vol_Acceleration: Gia tốc Volume hiện tại so với trung bình 15 phút trước
        # -------------------------------------------------------------------
        body = abs(group['close'] - group['open']) + 1e-8
        wick = group['high'] - group['low']
        group['wick_to_body'] = wick / body
        group['vol_acceleration'] = group['volume'] / (group['volume'].rolling(window=15).mean() + 1e-8)

        # -------------------------------------------------------------------
        # 4. Funding_Rate_Delta_12h (Mức thay đổi sau 720 phút)
        # Nhận diện FOMO/Hoảng loạn. Nếu có data funding thì tính, không có thì set = 0
        # -------------------------------------------------------------------
        if 'funding_rate' in group.columns:
            group['funding_delta_12h'] = group['funding_rate'].diff(720).fillna(0)
        else:
            group['funding_delta_12h'] = 0.0

        return group

    # Áp dụng cho từng nhóm coin
    df = df.groupby('symbol', group_keys=False).apply(apply_features)
    
    # Xóa các dòng bị NaN do quá trình tính Rolling sinh ra ở phần đầu của tập dữ liệu
    df = df.dropna()
    
    return df

# ==========================================
# 🧪 MÔ ĐUN TEST (Chỉ chạy khi gọi trực tiếp file này)
# ==========================================
if __name__ == "__main__":
    test_csv = 'dataset_multi_3months.csv'
    
    if not os.path.exists(test_csv):
        print(f"❌ Không tìm thấy file {test_csv}. Vui lòng kiểm tra lại.")
    else:
        print(f"⏳ Đang đọc dữ liệu từ {test_csv}...")
        raw_df = pd.read_csv(test_csv)
        print(f"Kích thước ban đầu: {raw_df.shape}")
        
        # Chạy hàm xử lý tính toán
        processed_df = calculate_quant_features(raw_df)
        
        print("\n✅ TÍNH TOÁN HOÀN TẤT! Trích xuất 5 dòng cuối cùng của 4 Features mới:")
        features_to_show = ['symbol', 'timestamp', 'close', 'hurst_proxy', 'dist_vwap', 'wick_to_body', 'vol_acceleration']
        print(processed_df[features_to_show].tail(5))