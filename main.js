/**
 * main.js - AI Quant Sniper V16.1 (Dual-Loop Architecture & Trailing Execution)
 * Nhạc trưởng điều phối Hệ thống Học máy & Giao dịch Tần suất cao.
 */
const connectDB = require('./src/config/db');
const ExchangeInfo = require('./src/services/binance/ExchangeInfo');
const InMemoryBuffer = require('./src/services/data/InMemoryBuffer');
const StreamAggregator = require('./src/services/binance/StreamAggregator');
const AiEngine = require('./src/services/ai/AiEngine');
const DeepThinker = require('./src/services/ai/DeepThinker');
const PaperExchange = require('./src/services/execution/PaperExchange'); // [V16.1] Module thực thi độc lập
const { healDataGaps } = require('./src/services/data/GapFiller_Startup');
const AutoRetrainScheduler = require('./src/services/ops/AutoRetrainScheduler');
require('./src/services/data/syncToMongo');

const fs = require('fs');
const path = require('path');

async function bootstrap() {
    // 1. Khởi tạo Cơ sở dữ liệu và Thông tin Sàn
    await connectDB();
    await ExchangeInfo.init();

    const whiteListData = JSON.parse(fs.readFileSync('./white_list.json', 'utf8'));
    const activeCoins = whiteListData.active_coins;
    const symbolList = Object.keys(activeCoins);

    // 2. Tự chữa lành dữ liệu khuyết trước khi start
    await healDataGaps(symbolList);

    // 3. Khởi tạo Tài khoản Quỹ 1000$ cho chế độ Giả lập
    await PaperExchange.initAccount();

    // 4. Khởi động Tai Mắt (WebSocket Vi cấu trúc)
    StreamAggregator.symbols = symbolList;
    StreamAggregator.start();

    // 5. Kích hoạt Lò phản ứng Học máy lúc 2:00 AM mỗi ngày
    AutoRetrainScheduler.init();

    console.log('\n🚀 [SYSTEM] V16.1 MASTERMIND ONLINE | Dual-Loop Architecture Active\n');

    let lastProcessedMinute = -1;

    // ==========================================================
    // VÒNG LẶP KÉP (DUAL-LOOP ARCHITECTURE) - TRÁI TIM HỆ THỐNG
    // ==========================================================
    setInterval(async () => {
        const now = new Date();
        const currentMinute = now.getMinutes();

        // ------------------------------------------------------
        // LUỒNG 1: LÍNH BẮN TỈA (Tốc độ: Nửa giây / lần)
        // Chuyên xử lý râu nến, Trailing Stop và quản trị rủi ro
        // ------------------------------------------------------
        await PaperExchange.monitorTrades();

        // ------------------------------------------------------
        // LUỒNG 2: TƯỚNG QUÂN AI (Tốc độ: 1 phút / lần)
        // Chuyên đọc Vi cấu trúc SMC và dự đoán Xu hướng
        // ------------------------------------------------------
        if (now.getSeconds() > 10 || currentMinute === lastProcessedMinute) return;
        lastProcessedMinute = currentMinute;

        for (const symbol of symbolList) {
            try {
                if (!InMemoryBuffer.isReady(symbol)) continue;

                const sortedHistory = InMemoryBuffer.getHistoryObjects(symbol);
                if (!sortedHistory || sortedHistory.length < 16) continue; // Cần ít nhất 16 nến để tính ATR

                // BƯỚC A: AI Engine tính toán 13 biến và dự đoán xác suất
                const prediction = await AiEngine.predict(sortedHistory, symbol);
                const { winProb, features } = prediction;

                // BƯỚC B: DeepThinker bẻ cong rủi ro & Kiểm định tín hiệu
                const decision = DeepThinker.evaluate(symbol, winProb, sortedHistory, features);

                // BƯỚC C: Truyền Tọa độ Tác chiến cho Lính Bắn Tỉa
                if (decision.approved) {
                    await PaperExchange.openTrade(
                        symbol, 
                        decision.side, 
                        decision.limitPrice, 
                        decision.slPrice, 
                        decision.tpPrice, 
                        decision.trailingParams, 
                        decision.prob, 
                        decision.reason
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