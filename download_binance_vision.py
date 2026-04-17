import pandas as pd
import requests
import zipfile
import io
import os
from datetime import datetime

# ================= CẤU HÌNH =================
SYMBOL = "SOLUSDT"
MONTHS_TO_FETCH = 3  # Lấy 3 tháng gần nhất
DATA_DIR = "data"    # Thư mục lưu file CSV
# ============================================

def get_last_n_months(n):
    """Tính toán danh sách N tháng hoàn chỉnh gần nhất (Định dạng YYYY-MM)"""
    today = pd.Timestamp.today()
    months = []
    for i in range(1, n + 1):
        # Lùi lại i tháng
        prev_month = today - pd.DateOffset(months=i)
        months.append(prev_month.strftime('%Y-%m'))
    months.reverse() # Sắp xếp theo thứ tự thời gian từ cũ đến mới
    return months

def download_and_process_binance_data():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        
    months = get_last_n_months(MONTHS_TO_FETCH)
    print(f"🚀 Bắt đầu tải dữ liệu {SYMBOL} cho các tháng: {months}")
    
    all_dataframes = []
    
    # Header gốc của Binance Vision (12 cột)
    binance_columns = [
        "open_time", "open", "high", "low", "close", "volume", 
        "close_time", "quote_volume", "trades", 
        "taker_buy_base", "taker_buy_quote", "ignore"
    ]
    
    for month in months:
        # Đường dẫn chuẩn của Binance Futures (UM - USD-M Futures)
        url = f"https://data.binance.vision/data/futures/um/monthly/klines/{SYMBOL}/1m/{SYMBOL}-1m-{month}.zip"
        
        print(f"⏳ Đang tải {month}...")
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                # Giải nén thẳng trên RAM, không lưu file zip ra máy
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    # Lấy tên file .csv bên trong file zip
                    csv_filename = z.namelist()[0]
                    with z.open(csv_filename) as f:
                        # Đọc vào Pandas
                        df = pd.read_csv(f, names=binance_columns)
                        
                        # Xóa 2 cột rác không cần thiết của Binance
                        df = df.drop(columns=["close_time", "ignore"])
                        
                        # Thêm cột symbol vào đầu tiên
                        df.insert(0, "symbol", SYMBOL)
                        
                        all_dataframes.append(df)
                        print(f"✅ Đã tải và xử lý xong {month} ({len(df)} nến)")
            else:
                print(f"❌ Lỗi 404: Không tìm thấy dữ liệu tháng {month} trên Binance (Có thể tháng này chưa chốt file).")
        except Exception as e:
            print(f"❌ Lỗi khi xử lý tháng {month}: {str(e)}")

    if all_dataframes:
        print("\n🔄 Đang gộp dữ liệu và lưu ra file CSV...")
        # Gộp tất cả các tháng lại thành 1 Dataframe
        final_df = pd.concat(all_dataframes, ignore_index=True)
        
        # Đổi tên cột chuẩn xác 100% theo format 11 trường đã chốt
        final_df.rename(columns={
            "open_time": "openTime",
            "quote_volume": "quoteVolume",
            "taker_buy_base": "takerBuyBase",
            "taker_buy_quote": "takerBuyQuote"
        }, inplace=True)
        
        # Sắp xếp lại thứ tự cột cho chuẩn
        expected_columns = [
            "symbol", "openTime", "open", "high", "low", "close", 
            "volume", "quoteVolume", "trades", "takerBuyBase", "takerBuyQuote"
        ]
        final_df = final_df[expected_columns]
        
        # Lưu ra CSV
        save_path = os.path.join(DATA_DIR, f"{SYMBOL}_history_raw_1m.csv")
        final_df.to_csv(save_path, index=False)
        print(f"🎉 HOÀN TẤT! Dữ liệu đã được lưu tại: {save_path} (Tổng cộng: {len(final_df)} nến)")
    else:
        print("⚠️ Không có dữ liệu nào được tải xuống.")

if __name__ == "__main__":
    download_and_process_binance_data()