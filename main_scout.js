/**
 * main_scout.js - AI Quant Sniper (Luồng Độc Lập Cho Tầng 3)
 * Nhạc trưởng điều phối Hệ thống Học máy Tổng quát (Universal Model).
 * Chạy song song không ảnh hưởng tới main.js cũ.
 */
require('dotenv').config();
const { connectDB } = require('./src/config/db');
const ExchangeInfo = require('./src/services/binance/ExchangeInfo');
const InMemoryBuffer = require('./src/services/data/InMemoryBuffer');
const StreamAggregator = require('./src/services/binance/StreamAggregator');
const AiEngine = require('./src/services/ai/AiEngine');
const PaperExchange = require('./src/services/execution/PaperExchange');
const { healDataGaps } = require('./src/services/data/GapFiller_Startup');
const fs = require('fs');
const path = require('path');

// 🟢 GỌI THỢ SĂN: Cho phép bot này tự động đi tìm coin mới
require('./src/workers/ScoutWorker');

// ⚠️ LƯU Ý SỐNG CÒN: KHÔNG gọi startApiServer() hay syncToMongo ở đây 
// để tránh đụng độ cổng (Port) và xung đột ghi đè với main.js đang chạy.

const TIER_LIST_PATH = path.join(__dirname, 'tier_list.json');

// Hàm này CHỈ LẤY coin Tầng 3
function getScoutSymbols() {
    try {
        if (!fs.existsSync(TIER_LIST_PATH)) return [];
        const data = JSON.parse(fs.readFileSync(TIER_LIST_PATH, 'utf8'));
        return data.ranks.tier_3 || [];
    } catch (e) {
        console.error('❌ Lỗi đọc danh sách coin:', e.message);
        return [];
    }
}

async function bootstrapScout() {
    console.log('🐺 [SCOUT SYSTEM] Đang khởi động Luồng Săn Mồi Độc Lập...');

    await connectDB();
    await ExchangeInfo.init();

    let scoutSymbols = getScoutSymbols();
    if (scoutSymbols.length === 0) {
        console.warn('⚠️ Tầng 3 đang trống. Đợi ScoutWorker đi săn mồi...');
        await new Promise(resolve => setTimeout(resolve, 6000));
        scoutSymbols = getScoutSymbols();
    }

    // Nạp dữ liệu và mở radar riêng cho Tầng 3
    await healDataGaps(scoutSymbols);
    await PaperExchange.initAccount(); // Nó sẽ móc nối chung vào quỹ 1000$ ở DB

    // Nạp Não Tập Trung (Bỏ qua 21 Não Cụ Thể)
    // Universal_Scout.onnx sẽ tự động được AiEngine.js nạp ở Constructor rồi.

    StreamAggregator.symbols = scoutSymbols;
    StreamAggregator.start();

    console.log(`\n🚀 [SCOUT SYSTEM] ĐÃ KẾT NỐI RADAR!`);
    console.log(`📊 Đang giám sát độc lập ${scoutSymbols.length} đồng coin Tầng 3.\n`);

    let lastProcessedMinute = -1;

    setInterval(async () => {
        const now = new Date();
        const currentMinute = now.getMinutes();

        // 1. Quản lý lệnh đang chạy của Tầng 3
        await PaperExchange.monitorTrades();

        // 2. Chờ thời điểm đóng nến 1 phút
        if (now.getSeconds() > 10 || currentMinute === lastProcessedMinute) return;
        lastProcessedMinute = currentMinute;

        const currentScoutSymbols = getScoutSymbols();

        // 🟢 CẬP NHẬT RADAR KHI CÓ MỒI MỚI
        if (currentScoutSymbols.length > StreamAggregator.symbols.length) {
            console.log(`📡 [SCOUT] Phát hiện mồi mới! Cắm thêm dây Radar...`);
            if (StreamAggregator.ws) StreamAggregator.ws.close();
            StreamAggregator.symbols = currentScoutSymbols;
            StreamAggregator.start();
        }

        for (const symbol of currentScoutSymbols) {
            try {
                if (!InMemoryBuffer.isReady(symbol)) continue;

                const sortedHistory = InMemoryBuffer.getHistoryObjects(symbol);
                if (!sortedHistory || sortedHistory.length < 16) continue;

                // Dùng AiEngine.js chung, nó sẽ tự động lấy Não Universal vì coin này ko có Model Cụ thể
                const prediction = await AiEngine.predict(sortedHistory, symbol);

                if (!prediction) continue;

                const { winProb, thinker, features, isUniversal } = prediction;

                // Bóp cò (Chỉ bóp khi đúng là đang dùng Não Universal)
                if (thinker && thinker.approved && isUniversal) {
                    await PaperExchange.openTrade(
                        symbol, thinker.side, thinker.limitPrice,
                        thinker.slPrice, thinker.tpPrice, thinker.trailingParams,
                        winProb, thinker.reason, features
                    );
                }
            } catch (error) {
                console.error(`❌ [ERROR] Scout AI ${symbol}:`, error.message);
            }
        }
    }, 500);
}

bootstrapScout().catch(err => {
    console.error('❌ Lỗi khởi động trầm trọng luồng Scout:', err);
    process.exit(1);
});