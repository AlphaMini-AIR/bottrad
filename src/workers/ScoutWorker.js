/**
 * src/workers/ScoutWorker.js
 * Nhiệm vụ: Radar quét sàn Binance, tìm kiếm 3 "Kỳ lân" có Volatility và Volume tốt nhất.
 * Cập nhật an toàn vào tier_list.json (Bảo vệ dữ liệu bằng Atomic Write).
 */
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const https = require('https');
const cron = require('node-cron');

const TIER_LIST_PATH = path.join(__dirname, '../../tier_list.json');
const TIER_LIST_TEMP_PATH = path.join(__dirname, '../../tier_list.tmp.json');

// Danh sách đen: Không bao giờ pick Stablecoins hoặc Token đòn bẩy
const BLACKLIST_PATTERNS = ['UPUSDT', 'DOWNUSDT', 'BULLUSDT', 'BEARUSDT'];
const STABLECOINS = ['USDCUSDT', 'FDUSDUSDT', 'TUSDUSDT', 'BUSDUSDT', 'EURUSDT', 'USDPUSDT', 'DAIUSDT'];
const MIN_24H_VOLUME_USDT = 20000000; // Tối thiểu 20 triệu $ / ngày
const MAX_SCOUT_SLOTS = 3;

/**
 * Hàm Helper: Gọi API Binance bằng HTTP native (Không dùng thư viện ngoài để tránh lỗi)
 */
const fetchBinanceTickers = () => {
    return new Promise((resolve, reject) => {
        https.get('https://api.binance.com/api/v3/ticker/24hr', (res) => {
            let data = '';
            res.on('data', (chunk) => data += chunk);
            res.on('end', () => {
                try {
                    resolve(JSON.parse(data));
                } catch (e) {
                    reject(new Error("Lỗi Parse JSON từ Binance"));
                }
            });
        }).on('error', (err) => reject(err));
    });
};

/**
 * Thuật toán chính: Tuyển trạch viên
 */
const runScoutMission = async () => {
    console.log('\n🔎 [ScoutWorker] Bắt đầu quét radar toàn sàn Binance...');

    // 1. ĐỌC FILE CẤU HÌNH HIỆN TẠI (An toàn)
    let tierData;
    try {
        if (!fs.existsSync(TIER_LIST_PATH)) throw new Error("Không tìm thấy tier_list.json");
        tierData = JSON.parse(fs.readFileSync(TIER_LIST_PATH, 'utf-8'));
    } catch (err) {
        console.error('❌ [ScoutWorker] Lỗi đọc file hệ thống:', err.message);
        return; // Dừng ngay, không làm hỏng dữ liệu
    }

    // Khởi tạo các mảng nếu chưa có (Backup an toàn)
    tierData.ranks.tier_3 = tierData.ranks.tier_3 || [];
    tierData.scout_tracking = tierData.scout_tracking || {};

    const currentScoutCount = tierData.ranks.tier_3.length;
    if (currentScoutCount >= MAX_SCOUT_SLOTS) {
        console.log(`ℹ️ [ScoutWorker] Tầng 3 đã đầy (${MAX_SCOUT_SLOTS}/${MAX_SCOUT_SLOTS} slot). Bỏ qua nhiệm vụ.`);
        return;
    }

    const slotsToFill = MAX_SCOUT_SLOTS - currentScoutCount;
    console.log(`📡 [ScoutWorker] Tầng 3 đang trống ${slotsToFill} slot. Đang lấy dữ liệu thị trường...`);

    // 2. KÉO DỮ LIỆU BINANCE VÀ LỌC
    try {
        const allTickers = await fetchBinanceTickers();
        
        // Lấy danh sách các coin ĐANG CÓ trong hệ thống để không bị chọn trùng
        const existingCoins = [
            ...tierData.ranks.tier_1, 
            ...tierData.ranks.tier_2, 
            ...tierData.ranks.tier_3
        ];

        let candidates = allTickers.filter(t => {
            // Lọc cứng: Chỉ lấy USDT, không lấy Stablecoin, không lấy coin đòn bẩy
            if (!t.symbol.endsWith('USDT')) return false;
            if (STABLECOINS.includes(t.symbol)) return false;
            if (BLACKLIST_PATTERNS.some(p => t.symbol.includes(p))) return false;
            
            // Lọc Thanh khoản: Phải > 20 củ đô
            if (parseFloat(t.quoteVolume) < MIN_24H_VOLUME_USDT) return false;

            // Lọc Trùng lặp: Chưa có mặt trong giải đấu
            if (existingCoins.includes(t.symbol)) return false;

            return true;
        });

        // 3. THUẬT TOÁN CHẤM ĐIỂM (Volume x Volatility)
        candidates.forEach(c => {
            const high = parseFloat(c.highPrice);
            const low = parseFloat(c.lowPrice);
            // Tính biên độ dao động (Đại diện cho ATR)
            c.volatility = low > 0 ? ((high - low) / low) * 100 : 0; 
        });

        // Sắp xếp: Ưu tiên biên độ dao động lớn nhất trong nhóm có thanh khoản cao
        candidates.sort((a, b) => b.volatility - a.volatility);

        // Bốc ra số lượng "Kỳ lân" cần thiết
        const selectedCoins = candidates.slice(0, slotsToFill).map(c => c.symbol);

        if (selectedCoins.length === 0) {
            console.log('⚠️ [ScoutWorker] Không tìm thấy coin nào thỏa mãn điều kiện khắc nghiệt hiện tại.');
            return;
        }

        console.log(`🎉 [ScoutWorker] Đã tìm thấy ${selectedCoins.length} Kỳ lân mới:`, selectedCoins);

        // 4. CẬP NHẬT DỮ LIỆU VÀO MEMORY
        const nowIso = new Date().toISOString();
        
        selectedCoins.forEach(symbol => {
            tierData.ranks.tier_3.push(symbol);
            tierData.storage_map[symbol] = "scout"; // Gán hộ khẩu vật lý vào DB Scout
            tierData.scout_tracking[symbol] = {
                joined_at: nowIso,
                survived_days: 0
            };
        });
        tierData.last_update = nowIso;

        // 5. GHI FILE BẰNG KỸ THUẬT ATOMIC WRITE (CHỐNG MẤT DỮ LIỆU)
        const jsonString = JSON.stringify(tierData, null, 4);
        
        // Bước A: Ghi vào file nháp (Temp)
        fs.writeFileSync(TIER_LIST_TEMP_PATH, jsonString, 'utf-8');
        
        // Bước B: Nếu ghi file nháp thành công, mới Rename đè lên file chính
        // Hành động Rename của OS là Atomic (Xảy ra ngay lập tức), không có khe hở thời gian gây lỗi
        fs.renameSync(TIER_LIST_TEMP_PATH, TIER_LIST_PATH);

        console.log('✅ [ScoutWorker] Đã chốt danh sách và ghi file tier_list.json an toàn!');

    } catch (error) {
        console.error('❌ [ScoutWorker] Lỗi trong quá trình quét thị trường:', error.message);
    }
};

// ================= CẤU HÌNH CHẠY TỰ ĐỘNG =================
const IS_BACKUP_NODE = process.env.IS_BACKUP_NODE === 'true';

if (!IS_BACKUP_NODE) {
    // 1. Chạy ngay 1 lần lúc khởi động bot để lấp đầy Tầng 3 (nếu đang trống)
    setTimeout(() => {
        runScoutMission();
    }, 5000); // Đợi 5s sau khi bot khởi động để các kết nối ổn định

    // 2. Lên lịch chạy định kỳ: Sáng Thứ 2 hàng tuần lúc 08:00 AM
    cron.schedule('0 8 * * 1', () => {
        console.log('⏰ [CRON] Khởi động chiến dịch ScoutWorker đầu tuần...');
        runScoutMission();
    });
} else {
    console.log('ℹ️ [ScoutWorker] TẮT trên máy Backup để tránh gọi API sàn 2 lần.');
}

module.exports = { runScoutMission };