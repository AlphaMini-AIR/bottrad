import pymongo
import pandas as pd
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

# -------------------------------------------------------------------------
# 1. CẤU HÌNH VÀ KẾT NỐI HỆ THỐNG
# -------------------------------------------------------------------------
# Tải các biến môi trường từ file .env để bảo mật thông tin kết nối
load_dotenv()

# Cấu hình đường dẫn tới 2 cụm Database độc lập của bạn (Tier-1 và Scout)
# Tier-1: Nơi lưu trữ chính và là nơi đặt "Trí nhớ dài hạn" (Core Memories)
# Scout: Nơi chứa các lệnh đánh thử (Paper Trade) của các coin mới/lạ
URIS = {
    "tier1": os.getenv("MONGO_URI_TIER1"),
    "scout": os.getenv("MONGO_URI_TIER2")
}

def run_experience_replay():
    """
    Tính năng chính: Quét toàn bộ hệ thống để nhặt những 'viên kim cương' dữ liệu.
    Cách hoạt động:
    - Thu gom tất cả các lệnh đã đóng trong 24h qua từ mọi nguồn.
    - Chọn ra 5% những lệnh có biến động PnL (Lãi/Lỗ) lớn nhất.
    - Sửa lại nhãn (Label) cho những lệnh thua để dạy AI cách làm đúng.
    """
    print(f"[{datetime.now()}] 🧠 BẮT ĐẦU QUY TRÌNH CHIẾT XUẤT KINH NGHIỆM...")
    
    all_trades_raw = []
    
    # --- BƯỚC A: THU THẬP DỮ LIỆU ĐA NGUỒN ---
    for name, uri in URIS.items():
        if not uri: continue
        try:
            client = pymongo.MongoClient(uri)
            db = client.get_default_database()
            
            # Chỉ lấy các lệnh đã có kết quả và có lưu 'aiFeatures' (Trạng thái não bộ lúc bóp cò)
            # Thời gian quét: Trong vòng 24 giờ qua
            yesterday = datetime.now() - timedelta(days=1)
            
            # Tìm trong bảng 'papertrades' (Tên collection khớp với PaperTrade.js của bạn)
            trades_cursor = db["papertrades"].find({
                "closeTime": {"$gte": yesterday},
                "aiFeatures": {"$exists": True, "$ne": None}
            })
            
            list_trades = list(trades_cursor)
            all_trades_raw.extend(list_trades)
            print(f"📡 Nguồn {name.upper()}: Tìm thấy {len(list_trades)} lệnh mới.")
            client.close()
        except Exception as e:
            print(f"❌ Lỗi kết nối nguồn {name}: {str(e)}")

    if not all_trades_raw:
        print("⚠️ Không tìm thấy lệnh Paper Trade nào đủ điều kiện để học. Kết thúc.")
        return

    # --- BƯỚC B: PHÂN TÍCH VÀ LỌC TINH HOA (TOP 5%) ---
    # Chuyển đổi sang DataFrame để xử lý toán học nhanh hơn
    df = pd.DataFrame(all_trades_raw)
    
    # Tính giá trị tuyệt đối của PnL. 
    # AI cần học từ cả Lãi đậm (Để làm tốt hơn) và Lỗ nặng (Để né tránh).
    df['absPnl'] = df['netPnl'].abs()
    
    # Sắp xếp các lệnh theo mức độ 'khốc liệt' giảm dần
    df = df.sort_values(by='absPnl', ascending=False)
    
    # Chọn ra 5% lệnh quan trọng nhất (Top-tier Experiences)
    top_count = max(1, int(len(all_trades_raw) * 0.05))
    top_experiences = df.head(top_count)

    print(f"📊 Đã chọn ra {top_count} lệnh 'tinh hoa' để chuyển vào Trí nhớ dài hạn.")

    # --- BƯỚC C: GÁN NHÃN LẠI ĐỘNG (DYNAMIC RE-LABELING) ---
    # Đây là bước quan trọng nhất để AI của bạn tự thông minh lên từ sai lầm
    memories_to_save = []
    for _, trade in top_experiences.iterrows():
        # Xử lý nhãn (Corrected Label):
        # - Nếu Thắng (WIN): AI đã làm đúng -> Nhãn học tập = Chiều của lệnh đó.
        # - Nếu Thua (LOSS): AI đã làm sai -> Nhãn học tập = ĐẢO NGƯỢC chiều lệnh.
        #   (VD: Dự đoán Long mà lỗ -> AI phải học rằng chỗ đó lẽ ra nên Short/Đứng ngoài).
        
        is_win = (trade['outcome'] == 'WIN')
        
        # Chuyển đổi SIDE (LONG/SHORT) thành số (1/0)
        actual_side_num = 1 if trade['side'] == 'LONG' else 0
        
        if is_win:
            corrected_label = actual_side_num
            learning_weight = 1.0 # Học bình thường
        else:
            # Ma thuật: Lật ngược nhãn để AI biết 'Sửa sai'
            corrected_label = 1 if actual_side_num == 0 else 0
            learning_weight = 3.0 # Nhân 3 trọng số: Bắt AI phải tập trung cực cao vào lỗi này

        # Đóng gói dữ liệu để đưa vào 'Lõi kinh nghiệm'
        memory_entry = {
            "symbol": trade['symbol'],
            "features": trade['aiFeatures'], # Trạng thái nến/vi cấu trúc lúc bóp cò
            "label": corrected_label,       # Nhãn đúng lẽ ra AI nên đánh
            "weight": learning_weight,      # Độ ưu tiên học tập
            "netPnl": trade['netPnl'],
            "outcome": trade['outcome'],
            "closeReason": trade.get('closeReason', 'UNKNOWN'),
            "memory_date": datetime.now()   # Ngày lưu vào trí nhớ
        }
        memories_to_save.append(memory_entry)

    # --- BƯỚC D: LƯU VÀO TIER-1 VÀ DỌN DẸP ---
    try:
        client_t1 = pymongo.MongoClient(URIS["tier1"])
        db_t1 = client_t1.get_default_database()
        
        # Ghi vào bảng 'core_memories' (Trí nhớ cốt lõi)
        if memories_to_save:
            db_t1["core_memories"].insert_many(memories_to_save)
            print(f"✅ Đã ghi {len(memories_to_save)} bài học vào bảng 'core_memories' tại Tier-1.")
        
        client_t1.close()
    except Exception as e:
        print(f"❌ Lỗi khi ghi vào Trí nhớ dài hạn: {str(e)}")

    print("🧹 [Cleanup] Quy trình hoàn tất. Dữ liệu thô sẽ được dọn dẹp bởi DataLifeCycle.js.")

if __name__ == "__main__":
    run_experience_replay()