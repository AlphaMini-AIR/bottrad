import pandas as pd
import glob
import os
import time

# Đường dẫn tới thư mục chứa các file CSV AggTrades
# Thay đổi './data' thành thư mục chứa file của bạn nếu cần
INPUT_DIR = './data' 
OUTPUT_DIR = './vpin_cleaned'

# Định nghĩa tên cột vì file Binance tải về thường không có header
BINANCE_COLUMNS = [
    'agg_trade_id', 'price', 'quantity', 'first_trade_id', 
    'last_trade_id', 'transact_time', 'is_buyer_maker'
]

def process_file(file_path):
    print(f"⏳ Đang xử lý: {os.path.basename(file_path)}...")
    start_time = time.time()
    
    # 1. Đọc file, chỉ lấy 3 cột cần thiết để tiết kiệm RAM
    df = pd.read_csv(
        file_path, 
        names=BINANCE_COLUMNS, 
        usecols=['quantity', 'transact_time', 'is_buyer_maker'],
        header=0 # Đổi thành None nếu file CSV của bạn không có dòng tiêu đề chữ
    )
    
    # 2. Xử lý logic Dòng tiền chủ động
    # is_buyer_maker = True -> Lệnh Maker (treo sẵn) là lệnh Mua -> Người khớp lệnh là phe BÁN CHỦ ĐỘNG
    df['taker_sell_vol'] = df['quantity'].where(df['is_buyer_maker'] == True, 0)
    df['taker_buy_vol'] = df['quantity'].where(df['is_buyer_maker'] == False, 0)
    
    # 3. Làm tròn thời gian về nến 1 phút (Mili-giây -> Chia lấy nguyên cho 60,000)
    df['timestamp_1m'] = (df['transact_time'] // 60000) * 60000
    
    # 4. Gộp nhóm theo từng phút và cộng dồn Volume (O(1) trong Pandas)
    minute_df = df.groupby('timestamp_1m').agg({
        'taker_buy_vol': 'sum',
        'taker_sell_vol': 'sum'
    }).reset_index()
    
    # 5. Tính toán VPIN
    total_vol = minute_df['taker_buy_vol'] + minute_df['taker_sell_vol']
    minute_df['vpin'] = (minute_df['taker_buy_vol'] - minute_df['taker_sell_vol']) / (total_vol + 1e-8)
    
    # 6. Dọn dẹp và giữ lại đúng 2 cột quan trọng
    final_df = minute_df[['timestamp_1m', 'vpin']]
    
    # Đổi tên cột cho chuẩn
    final_df.rename(columns={'timestamp_1m': 'timestamp'}, inplace=True)
    
    process_time = time.time() - start_time
    print(f"✅ Xong! {len(df)} ticks -> {len(final_df)} nến 1m. (Mất {process_time:.2f}s)")
    
    return final_df

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        
    csv_files = glob.glob(os.path.join(INPUT_DIR, '*.csv'))
    if not csv_files:
        print("❌ Không tìm thấy file CSV nào. Hãy kiểm tra lại INPUT_DIR.")
        return

    # Gom nhóm các file theo đồng coin (Ví dụ: gom tất cả NEARUSDT lại với nhau)
    coins = {}
    for file in csv_files:
        filename = os.path.basename(file)
        symbol = filename.split('-')[0] # Lấy 'NEARUSDT' từ 'NEARUSDT-aggTrades-...'
        if symbol not in coins:
            coins[symbol] = []
        coins[symbol].append(file)
        
    # Xử lý và nối các tháng lại cho từng coin
    for symbol, files in coins.items():
        print(f"\n🚀 BẮT ĐẦU GỘP {symbol} ({len(files)} tháng)...")
        df_list = []
        for file in sorted(files): # Sắp xếp theo thứ tự tháng
            df_list.append(process_file(file))
            
        # Nối tất cả các tháng của 1 coin lại thành 1 file duy nhất
        final_coin_df = pd.concat(df_list, ignore_index=True)
        final_coin_df = final_coin_df.sort_values('timestamp').drop_duplicates('timestamp')
        
        # Xuất file CSV siêu nhẹ
        output_path = os.path.join(OUTPUT_DIR, f"{symbol}_VPIN_Cleaned.csv")
        final_coin_df.to_csv(output_path, index=False)
        print(f"🎉 ĐÃ LƯU: {output_path} ({len(final_coin_df)} phút)")
if __name__ == "__main__":
    main()