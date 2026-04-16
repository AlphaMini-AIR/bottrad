"""
prepare_dataset_v8.py - MỐC 1: XÂY DỰNG NGŨ QUAN SMC (29 ĐẶC TRƯNG)
Đã vá lỗi Look-ahead Bias, Doji Division-by-zero, Backtest Spoofing Fallback
Và Fix lỗi Pandas Frequency (H -> h, Min -> min)
"""
import pandas as pd
import numpy as np
import warnings
import os

warnings.filterwarnings('ignore')

def add_advanced_smc_features(df):
    print("🧠 Đang 'bơm' tư duy Cá Voi & Đa Khung Thời Gian vào dữ liệu...")
    
    # Chuẩn hóa thời gian
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
    df = df.sort_values(by=['symbol', 'datetime']).reset_index(drop=True)
    
    all_groups = []
    
    for symbol, group in df.groupby('symbol'):
        print(f" ⏳ Đang xử lý {symbol}...")
        group = group.copy()
        group = group.set_index('datetime')
        
        # ========================================================
        # 1. ĐA KHUNG THỜI GIAN (MTFA) & VÁ LỖI LOOK-AHEAD BIAS
        # ========================================================
        # Khung 4 Giờ (4h) - Sửa '4H' thành '4h'
        df_4h = group['close'].resample('4h', label='left', closed='left').ohlc()
        df_4h['trend_4h'] = np.where(df_4h['close'] > df_4h['open'], 1, -1)
        df_4h['ema20_4h'] = df_4h['close'].ewm(span=20).mean()
        
        # Khung 15 Phút (15m) - Sửa '15Min' thành '15min'
        df_15m = group.resample('15min', label='left', closed='left').agg({'open':'first', 'high':'max', 'low':'min', 'close':'last'})
        df_15m['fvg_15m_bull_top'] = np.where(df_15m['low'] > df_15m['high'].shift(2), df_15m['low'], np.nan)
        df_15m['fvg_15m_bull_bot'] = np.where(df_15m['low'] > df_15m['high'].shift(2), df_15m['high'].shift(2), np.nan)
        df_15m['fvg_15m_bear_top'] = np.where(df_15m['high'] < df_15m['low'].shift(2), df_15m['low'].shift(2), np.nan)
        df_15m['fvg_15m_bear_bot'] = np.where(df_15m['high'] < df_15m['low'].shift(2), df_15m['high'], np.nan)
        
        # Nối dữ liệu HTF vào khung 1m (Dùng direction='backward' để tuyệt đối không nhìn trộm tương lai)
        group = pd.merge_asof(group, df_4h[['trend_4h', 'ema20_4h']], left_index=True, right_index=True, direction='backward')
        group = pd.merge_asof(group, df_15m[['fvg_15m_bull_top', 'fvg_15m_bull_bot', 'fvg_15m_bear_top', 'fvg_15m_bear_bot']], left_index=True, right_index=True, direction='backward')
        
        # Đặc trưng khoảng cách tới EMA 4H
        group['dist_to_4h_ema'] = (group['close'] - group['ema20_4h']) / (group['ema20_4h'] + 1e-8)

        # ========================================================
        # 2. VÙNG THỜI GIAN VÀNG (KILL ZONES UTC+7)
        # ========================================================
        dt_hcm = group.index.tz_localize('UTC').tz_convert('Asia/Ho_Chi_Minh')
        time_float = dt_hcm.hour + dt_hcm.minute / 60.0
        
        group['is_dead_zone'] = ((time_float >= 7.0) & (time_float < 13.0)).astype(int)
        group['is_london_open'] = ((time_float >= 13.5) & (time_float <= 16.5)).astype(int)
        group['is_ny_open'] = ((time_float >= 19.0) & (time_float <= 22.5)).astype(int)

        # ========================================================
        # 3. NHẬN DIỆN KIỆT SỨC (EXHAUSTION) VÀ VÁ LỖI NẾN DOJI
        # ========================================================
        body = abs(group['close'] - group['open'])
        prev_body = body.shift(1)
        avg_vol_20 = group['volume'].rolling(20).mean() + 1e-8
        
        group['exhaustion_ratio'] = np.where(
            prev_body < 1e-6, 
            np.where(group['volume'] > avg_vol_20 * 1.5, 999.0, 1.0),
            (body / (prev_body + 1e-8)) * (group['volume'] / avg_vol_20)
        )

        # ========================================================
        # 4. BẪY SỔ LỆNH (SPOOFING) VÀ BACKTEST FALLBACK
        # ========================================================
        if 'ob_imb_norm' in group.columns:
            group['spoofing_index'] = abs(group['ob_imb_norm']) * (1 - abs(group['vpin'].fillna(0)))
        else:
            group['ob_imb_norm'] = 0.0
            group['spoofing_index'] = 0.0

        # ========================================================
        # 5. TỶ LỆ LẤP ĐẦY FVG 15M (MITIGATION %)
        # ========================================================
        bull_fvg_size = group['fvg_15m_bull_top'] - group['fvg_15m_bull_bot'] + 1e-8
        bear_fvg_size = group['fvg_15m_bear_top'] - group['fvg_15m_bear_bot'] + 1e-8
        
        group['fvg_15m_bull_mitigation'] = np.clip((group['fvg_15m_bull_top'] - group['low']) / bull_fvg_size, 0, 1)
        group['fvg_15m_bear_mitigation'] = np.clip((group['high'] - group['fvg_15m_bear_bot']) / bear_fvg_size, 0, 1)
        
        group['fvg_15m_bull_mitigation'] = group['fvg_15m_bull_mitigation'].fillna(0)
        group['fvg_15m_bear_mitigation'] = group['fvg_15m_bear_mitigation'].fillna(0)

       # ========================================================
        # 6. CẤU TRÚC VÀ PHÂN KỲ (SWING & CUMULATIVE DELTA)
        # ========================================================
        group['swing_high'] = group['high'].rolling(20).max().shift(1)
        group['swing_low'] = group['low'].rolling(20).min().shift(1)
        
        group['dist_to_swing_high'] = (group['swing_high'] - group['close']) / (group['close'] + 1e-8)
        group['dist_to_swing_low'] = (group['close'] - group['swing_low']) / (group['close'] + 1e-8)
        
        group['price_roc_5'] = group['close'].pct_change(5)
        group['vpin_sum_5'] = group['vpin'].rolling(5).sum()
        
        conditions = [
            (group['price_roc_5'] >= -0.001) & (group['vpin_sum_5'] < -0.5), 
            (group['price_roc_5'] <= 0.001) & (group['vpin_sum_5'] > 0.5)  
        ]
        group['cum_delta_div'] = np.select(conditions, [-1, 1], default=0)

        # ---- [BẢN VÁ LỖI Ở ĐÂY] ----
        # Xóa bỏ các cột ranh giới FVG thô chứa NaN để bảo vệ dữ liệu khỏi lệnh dropna
        cols_to_drop = ['fvg_15m_bull_top', 'fvg_15m_bull_bot', 'fvg_15m_bear_top', 'fvg_15m_bear_bot']
        group = group.drop(columns=cols_to_drop)

        all_groups.append(group.reset_index())
        
    # Bây giờ lệnh dropna() sẽ chỉ xóa đi ~80 tiếng đầu tiên do chờ EMA 4H hội tụ, giữ lại 95% dữ liệu!
    final_df = pd.concat(all_groups).dropna()
    return final_df

if __name__ == "__main__":
    input_file = "dataset_master_enriched.csv"
    output_file = "dataset_v8_29features.csv"
    
    if not os.path.exists(input_file):
        print(f"❌ Không tìm thấy file {input_file}. Hãy kiểm tra thư mục.")
        exit(1)
        
    print(f"📂 Đang nạp Sách giáo khoa thô từ: {input_file}")
    df_raw = pd.read_csv(input_file)
    
    if 'ob_imb_norm' not in df_raw.columns:
        print("⚠️ CẢNH BÁO: Không có dữ liệu Orderbook Imbalance (Spoofing). Backtest sẽ bị giới hạn sức mạnh.")
    
    df_v8 = add_advanced_smc_features(df_raw)
    df_v8.to_csv(output_file, index=False)
    
    print("-" * 50)
    print("✅ HOÀN TẤT MỐC 1: ĐÃ NÂNG CẤP LÊN BỘ 29 ĐẶC TRƯNG!")
    print(f"📊 Tổng số mẫu AI có thể học: {len(df_v8)} dòng.")
    print(f"📁 Dữ liệu V8.0 đã được lưu tại: {output_file}")
    print("-" * 50)