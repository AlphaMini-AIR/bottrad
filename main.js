/**
 * main.js - AI Quant Sniper V16.1 (Full League System Integration)
 * Nhạc trưởng điều phối Hệ thống Học máy, Radar Scouter & API Gateway.
 */
require('dotenv').config();
const { connectDB } = require('./src/config/db');
const ExchangeInfo = require('./src/services/binance/ExchangeInfo');
const InMemoryBuffer = require('./src/services/data/InMemoryBuffer');
const StreamAggregator = require('./src/services/binance/StreamAggregator');
const AiEngine = require('./src/services/ai/AiEngine');
const DeepThinker = require('./src/services/ai/DeepThinker');
const PaperExchange = require('./src/services/execution/PaperExchange');
const { healDataGaps } = require('./src/services/data/GapFiller_Startup');
const { startApiServer } = require('./src/api/server');

// Kích hoạt các dịch vụ chạy ngầm (Cron Jobs)
require('./src/services/data/syncToMongo');
require('./src/services/ops/DataLifeCycle');
require('./src/workers/ScoutWorker');

const fs = require('fs');
const path = require('path');

const TIER_LIST_PATH = path.join(__dirname, 'tier_list.json');

/**
 * Hàm lấy danh sách tất cả coin đang hoạt động từ 3 tầng
 */
function getActiveSymbolList() {
    try {
        if (!fs.existsSync(TIER_LIST_PATH)) return [];
        const data = JSON.parse(fs.readFileSync(TIER_LIST_PATH, 'utf8'));
        return [
            ...data.ranks.tier_1,
            ...data.ranks.tier_2,
            ...data.ranks.tier_3
        ];
    } catch (e) {
        console.error('❌ Lỗi đọc danh sách coin:', e.message);
        return [];
    }
}

async function bootstrap() {
    console.log('🦾 [SYSTEM] Đang khởi động AI Quant Sniper Core...');

    // 1. Khởi tạo Cơ sở dữ liệu (Kết nối 3 Clusters)
    await connectDB();

    // 2. Mở cổng API cho Dashboard Next.js
    startApiServer();

    // 3. Khởi tạo Thông tin Sàn & Danh sách Coin
    await ExchangeInfo.init();
    let symbolList = getActiveSymbolList();

    if (symbolList.length === 0) {
        console.warn('⚠️ Tier List trống. Đợi ScoutWorker tìm coin...');
        // Đợi 1 chút để ScoutWorker (đã require ở trên) kịp khởi tạo file nếu trống
        await new Promise(resolve => setTimeout(resolve, 6000));
        symbolList = getActiveSymbolList();
    }

    // 4. Tự chữa lành dữ liệu khuyết & Khởi tạo tài khoản giả lập
    await healDataGaps(symbolList);
    await PaperExchange.initAccount();

    // 5. Khởi động Tai Mắt (WebSocket Vi cấu trúc)
    StreamAggregator.symbols = symbolList;
    StreamAggregator.start();

    console.log(`\n🚀 [SYSTEM] V16.1 MASTERMIND ONLINE`);
    console.log(`📊 Đang giám sát: ${symbolList.length} đồng coin trên 3 Tầng.\n`);

    let lastProcessedMinute = -1;

    // ==========================================================
    // VÒNG LẶP KÉP (DUAL-LOOP ARCHITECTURE)
    // ==========================================================
    setInterval(async () => {
        const now = new Date();
        const currentMinute = now.getMinutes();

        // ------------------------------------------------------
        // LUỒNG 1: LÍNH BẮN TỈA (500ms/lần) - Quản trị lệnh đang chạy
        // ------------------------------------------------------
        await PaperExchange.monitorTrades();

        // ------------------------------------------------------
        // LUỒNG 2: TƯỚNG QUÂN AI (1 phút/lần) - Dự đoán & Bóp cò
        // ------------------------------------------------------
        if (now.getSeconds() > 10 || currentMinute === lastProcessedMinute) return;
        lastProcessedMinute = currentMinute;

        // Cập nhật lại danh sách coin (phòng trường hợp có coin mới từ ScoutWorker)
        const currentSymbols = getActiveSymbolList();

        for (const symbol of currentSymbols) {
            try {
                if (!InMemoryBuffer.isReady(symbol)) continue;

                // 🔍 KIỂM TRA SỰ TỒN TẠI CỦA MODEL TRƯỚC KHI DỰ ĐOÁN
                const modelPath = path.join(__dirname, `model_v16_${symbol}.onnx`);
                if (!fs.existsSync(modelPath)) {
                    // Nếu không có model, chỉ log 1 lần để biết coin đang trong giai đoạn "vắt sữa data"
                    if (now.getSeconds() < 1) console.log(`⏳ [AI] ${symbol} đang tích lũy data, chưa có model để dự đoán.`);
                    continue;
                }

                const sortedHistory = InMemoryBuffer.getHistoryObjects(symbol);
                if (!sortedHistory || sortedHistory.length < 16) continue;

                // Chạy AI dự đoán dựa trên file ONNX có sẵn
                const prediction = await AiEngine.predict(sortedHistory, symbol);
                const { winProb, features } = prediction;

                const decision = DeepThinker.evaluate(symbol, winProb, sortedHistory, features);

                // THỰC THI GIẢ LẬP (PAPER ONLY)
                if (decision.approved) {
                    await PaperExchange.openTrade(
                        symbol, decision.side, decision.limitPrice,
                        decision.slPrice, decision.tpPrice, decision.trailingParams,
                        decision.prob, decision.reason, features
                    );
                }
            } catch (error) {
                console.error(`❌ [ERROR] Vòng lặp AI ${symbol}:`, error.message);
            }
        }
    }, 500);
}

bootstrap().catch(err => {
    console.error('❌ Lỗi khởi động trầm trọng:', err);
    process.exit(1);
});