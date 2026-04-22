/**
 * src/workers/ScoutWorker.js
 * Nhiệm vụ: Radar quét sàn Binance, tìm kiếm 3 "Kỳ lân" có Volatility và Volume tốt nhất.
 * Cập nhật an toàn vào tier_list.json (Bảo vệ dữ liệu bằng Atomic Write).
 * MỚI (V17): Tích hợp cơ chế Cold Start - Khởi tạo dữ liệu NaN cho Universal AI Model.
 */
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const https = require('https');
const cron = require('node-cron');

// (TÙY CHỌN): Nạp InMemoryBuffer để ép dữ liệu NaN vào RAM ngay khi tìm thấy coin
const InMemoryBuffer = require('../services/data/InMemoryBuffer');

const TIER_LIST_PATH = path.join(__dirname, '../../tier_list.json');
const TIER_LIST_TEMP_PATH = path.join(__dirname, '../../tier_list.tmp.json');

// Danh sách đen: Không bao giờ pick Stablecoins hoặc Token đòn bẩy
const BLACKLIST_PATTERNS = ['UPUSDT', 'DOWNUSDT', 'BULLUSDT', 'BEARUSDT'];
const STABLECOINS = ['USDCUSDT', 'FDUSDUSDT', 'TUSDUSDT', 'BUSDUSDT', 'EURUSDT', 'USDPUSDT', 'DAIUSDT'];
const MIN_24H_VOLUME_USDT = 20000000; // Tối thiểu 20 triệu $ / ngày
const MAX_SCOUT_SLOTS = 3;

/**
 * =====================================================================
 * 🧠 MODULE: UNIVERSAL COLD START (KHỞI ĐỘNG LẠNH CHO AI)
 * =====================================================================
 * Mục đích: Coin mới list sàn hoặc mới đưa vào radar sẽ chưa có dữ liệu 
 * Sổ lệnh (Orderbook) hay Thanh lý (Liquidation).
 * Chỗ này liệt kê các trường đó ra để ép thành NaN.
 */
const MICRO_FEATURES_REQUIRED = [
    'ob_imb_top20', 'spread_close', 'bid_vol_1pct', 'ask_vol_1pct',
    'max_buy_trade', 'max_sell_trade', 'liq_long_vol', 'liq_short_vol'
];

/**
 * Hàm tạo dữ liệu "Khởi động lạnh" (Cold Start)
 * Được dùng ở đâu: Gọi ngay bên dưới khi Bot vừa pick được 1 coin Scout.
 * Tại sao phải dùng: Để tạo sẵn một object có chứa NaN. AiEngine sẽ đọc object này,
 * dịch NaN chuẩn IEEE 754 gửi vào ONNX -> ONNX tự động bỏ qua Sổ lệnh và chỉ đoán dựa trên Nến.
 */
const prepareColdStartData = (symbol) => {
    const coldStartData = {
        symbol: symbol,
        isColdStart: true, // Cờ hiệu báo cho hệ thống biết đây là coin bị mù dữ liệu
        micro: {}
    };

    // Ép cứng các trường Vi cấu trúc thành NaN
    MICRO_FEATURES_REQUIRED.forEach(feature => {
        coldStartData.micro[feature] = NaN;
    });

    return coldStartData;
};


/**
 * =====================================================================
 * 🛠️ MODULE CHÍNH: QUÉT RADAR (TUYỂN TRẠCH VIÊN)
 * =====================================================================
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
        const existingCoins = [
            ...tierData.ranks.tier_1,
            ...tierData.ranks.tier_2,
            ...tierData.ranks.tier_3
        ];

        let candidates = allTickers.filter(t => {
            if (!t.symbol.endsWith('USDT')) return false;
            if (STABLECOINS.includes(t.symbol)) return false;
            if (BLACKLIST_PATTERNS.some(p => t.symbol.includes(p))) return false;
            if (parseFloat(t.quoteVolume) < MIN_24H_VOLUME_USDT) return false;
            if (existingCoins.includes(t.symbol)) return false;
            return true;
        });

        // 3. THUẬT TOÁN CHẤM ĐIỂM (Volume x Volatility)
        candidates.forEach(c => {
            const high = parseFloat(c.highPrice);
            const low = parseFloat(c.lowPrice);
            const open = parseFloat(c.openPrice);
            const last = parseFloat(c.lastPrice);
            const trades = parseInt(c.count); // Số lượng lệnh khớp 24h

            // A. Raw Volatility: Độ dao động tổng (%)
            const rawVolatility = low > 0 ? ((high - low) / low) * 100 : 0;

            // B. Trend Quality (Chất lượng xu hướng): Từ 0 đến 1
            // Đo lường xem thân nến (Body) chiếm bao nhiêu % của toàn bộ biên độ (High-Low)
            // Càng gần 1 -> Giá chạy một mạch dứt khoát -> AI cực kỳ dễ đoán.
            // Càng gần 0 -> Nến Doji quét 2 đầu -> Bỏ qua.
            const bodySize = Math.abs(last - open);
            const totalRange = high - low;
            const trendQuality = totalRange > 0 ? (bodySize / totalRange) : 0;

            // C. Liquidity Bonus (Điểm cộng thanh khoản)
            // Coin có trên 100,000 trades/ngày nến sẽ rất mượt. Dưới mức đó dễ bị cá mập thao túng 1 cây nến.
            // Chuẩn hóa lấy mốc 100k, giới hạn hệ số nhân tối đa là 1.5
            const liquidityBonus = Math.min(trades / 100000, 1.5);

            // TỔNG ĐIỂM CHUNG
            // Chỉ những coin VỪA biên độ mạnh, VỪA xu hướng dứt khoát, VỪA nhiều lệnh khớp mới được lên Top.
            c.scoutScore = rawVolatility * trendQuality * liquidityBonus;
        });
        
        // Sắp xếp theo scoutScore thay vì volatility đơn thuần
        candidates.sort((a, b) => b.scoutScore - a.scoutScore);
        const selectedCoins = candidates.slice(0, slotsToFill).map(c => c.symbol);

        if (selectedCoins.length === 0) {
            console.log('⚠️ [ScoutWorker] Không tìm thấy coin nào thỏa mãn điều kiện khắc nghiệt hiện tại.');
            return;
        }

        console.log(`🎉 [ScoutWorker] Đã tìm thấy ${selectedCoins.length} Kỳ lân mới:`, selectedCoins);

        // 4. CẬP NHẬT DỮ LIỆU VÀO MEMORY & KÍCH HOẠT COLD START
        const nowIso = new Date().toISOString();

        selectedCoins.forEach(symbol => {
            tierData.ranks.tier_3.push(symbol);
            tierData.storage_map[symbol] = "scout"; // Gán hộ khẩu vật lý vào DB Scout

            // 🟢 TÍCH HỢP 1: Cập nhật tracking và Mode đánh cho AI
            tierData.scout_tracking[symbol] = {
                joined_at: nowIso,
                survived_days: 0,
                mode: 'UNIVERSAL_GENERALIST' // Báo cho AiEngine biết phải dùng Mô hình Tổng quát
            };

            // 🟢 TÍCH HỢP 2: Khởi tạo mảng NaN cho AI ngay lập tức
            const coldStartData = prepareColdStartData(symbol);

            // Ép mảng rỗng này vào RAM (InMemoryBuffer)
            // Mục đích: Nếu 1 giây sau AiEngine bóp cò gọi dự đoán, nó sẽ lấy được coldStartData
            // thay vì báo lỗi "Không tìm thấy dữ liệu Microstructure".
            if (InMemoryBuffer && typeof InMemoryBuffer.updateClosedCandle === 'function') {
                InMemoryBuffer.updateClosedCandle(symbol, coldStartData);
            }
        });
        tierData.last_update = nowIso;

        // 5. GHI FILE BẰNG KỸ THUẬT ATOMIC WRITE (CHỐNG MẤT DỮ LIỆU)
        const jsonString = JSON.stringify(tierData, null, 4);
        fs.writeFileSync(TIER_LIST_TEMP_PATH, jsonString, 'utf-8');
        fs.renameSync(TIER_LIST_TEMP_PATH, TIER_LIST_PATH);

        console.log('✅ [ScoutWorker] Đã chốt danh sách và kích hoạt Khởi động lạnh (Cold Start) thành công!');

    } catch (error) {
        console.error('❌ [ScoutWorker] Lỗi trong quá trình quét thị trường:', error.message);
    }
};

// ================= CẤU HÌNH CHẠY TỰ ĐỘNG =================
const IS_BACKUP_NODE = process.env.IS_BACKUP_NODE === 'true';

if (!IS_BACKUP_NODE) {
    setTimeout(() => { runScoutMission(); }, 5000);
    cron.schedule('0 8 * * 1', () => {
        console.log('⏰ [CRON] Khởi động chiến dịch ScoutWorker đầu tuần...');
        runScoutMission();
    });
} else {
    console.log('ℹ️ [ScoutWorker] TẮT trên máy Backup để tránh gọi API sàn 2 lần.');
}

module.exports = { runScoutMission };