"""
experience_filter.py - Lò thanh lọc 5% (Phần 2 - Bước 4) V17.1
Nhiệm vụ: Gán nhãn lại (Dynamic Relabeling) chuẩn xác, trích xuất tinh hoa và quản lý RAM.
"""
import pandas as pd
import numpy as np
from pymongo import MongoClient
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Kết nối các cụm DB
MONGO_URIS = {
    'tier1': os.getenv('MONGO_URI_TIER1'),
    'tier2': os.getenv('MONGO_URI_TIER2'),
    'scout': os.getenv('MONGO_URI_SCOUT')
}

def filter_and_relabel_experience():
    print("🧹 [ExperienceFilter] Đang bắt đầu thanh lọc dữ liệu 24h qua...")
    
    core_experience = []
    
    # CHỐNG NỔ RAM: Chỉ kéo dữ liệu của 24 giờ qua
    yesterday = datetime.utcnow() - timedelta(days=1)

    for node, uri in MONGO_URIS.items():
        if not uri: continue
        
        client = MongoClient(uri)
        db = client['Binance']
        
        # Chỉ kéo các lệnh vừa chốt sổ hôm nay
        trades_cursor = db['papertrades'].find({"closeTime": {"$gte": yesterday}})
        trades = list(trades_cursor)
        
        if not trades:
            client.close()
            continue

        df = pd.DataFrame(trades)
        
        # BẢO VỆ DỮ LIỆU RỖNG: Xóa các dòng không có Features hoặc Quỹ đạo giá
        df = df.dropna(subset=['aiFeatures', 'lowestDuringTrade', 'highestDuringTrade'])
        if df.empty:
            client.close()
            continue
        
        # 🟢 CHỨC NĂNG 1: DYNAMIC RE-LABELING (Tuyệt đối chuẩn xác)
        def adjust_label(row):
            # Mặc định mọi lệnh đều là 2 (Nhiễu - Không có giá trị xu hướng học thuật)
            final_label = 2 
            
            # CHỈ DUYỆT CÁC LỆNH CÓ LỢI NHUẬN DƯƠNG THỰC SỰ
            if row['outcome'] == 'WIN' and row['netPnl'] > 0:
                if row['side'] == 'LONG':
                    # Kiểm tra quỹ đạo: Thắng nhưng từng chạm ranh giới tử thần -> Loại
                    if row['lowestDuringTrade'] <= row['slPrice']:
                        final_label = 2 
                    else:
                        final_label = 1 # 💎 LONG cực chuẩn
                
                elif row['side'] == 'SHORT':
                    # Kiểm tra quỹ đạo: Thắng nhưng từng chạm ranh giới tử thần -> Loại
                    if row['highestDuringTrade'] >= row['slPrice']:
                        final_label = 2
                    else:
                        final_label = 0 # 💎 SHORT cực chuẩn

            return final_label

        df['final_label'] = df.apply(adjust_label, axis=1)
        
        # 🟢 CHỨC NĂNG 2: TRÍCH XUẤT 5% TINH HOA
        # BẢO VỆ CHỐNG CRASH: Cần đủ số lệnh mới tính 5%
        if len(df) >= 20:
            df['abs_pnl'] = df['netPnl'].abs()
            threshold = df['abs_pnl'].quantile(0.95) 
            elite_trades = df[df['abs_pnl'] >= threshold].copy()
        else:
            # Nếu hôm nay đi ít lệnh, lưu lại toàn bộ để gom đủ data
            elite_trades = df.copy() 
        
        # Đóng gói về JSON
        for _, row in elite_trades.iterrows():
            features = row['aiFeatures']
            # Kiểm tra nghiêm ngặt: Tensor phải đủ 13 cột mới được đưa vào lõi
            if isinstance(features, list) and len(features) == 13:
                core_experience.append({
                    'symbol': row['symbol'],
                    'features': features,
                    'label': row['final_label'],
                    'timestamp': row['closeTime'],
                    'source_node': node
                })
            
        # 🟢 CHỨC NĂNG 3: DỌN DẸP THÙNG RÁC AN TOÀN
        # Xóa các lệnh cũ hơn 24h. Dùng $lt cực kỳ nhẹ cho DB, không bị lỗi BSON limits
        try:
            db['papertrades'].delete_many({"closeTime": {"$lt": yesterday}})
        except Exception as e:
            print(f"⚠️ [ExperienceFilter] Cảnh báo khi dọn rác ở {node}: {e}")
        
        client.close()

    # Lưu 5% tinh hoa vào 'core_memory' tại Tier 1
    if core_experience:
        master_client = MongoClient(MONGO_URIS['tier1'])
        master_db = master_client['Binance']
        master_db['core_memory'].insert_many(core_experience)
        master_client.close()
        print(f"✅ Đã kết tinh {len(core_experience)} trải nghiệm tinh hoa vào core_memory.")
    else:
        print("ℹ️ Hôm nay không có trải nghiệm tinh hoa nào đủ tiêu chuẩn được trích xuất.")

if __name__ == "__main__":
    filter_and_relabel_experience()