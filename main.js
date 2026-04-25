/**
 * main.js - AI Quant Sniper (DATA HARVESTER MODE)
 * Chế độ tĩnh lặng: Chỉ bật Radar tìm coin, hút dữ liệu Live và lưu vào MongoDB.
 * KHÔNG dự đoán AI, KHÔNG mở lệnh giao dịch.
 */ 
require('dotenv').config();
const { connectDB } = require('./src/config/db');
const ExchangeInfo = require('./src/services/binance/ExchangeInfo');
const InMemoryBuffer = require('./src/services/data/InMemoryBuffer');
const StreamAggregator = require('./src/services/binance/StreamAggregator');
// const AiEngine = require('./src/services/ai/AiEngine');
// const DeepThinker = require('./src/services/ai/DeepThinker');
// const PaperExchange = require('./src/services/execution/PaperExchange');
const { healDataGaps } = require('./src/services/data/GapFiller_Startup');
const { startApiServer } = require('./src/api/server');

// Kích hoạt các dịch vụ chạy ngầm (Cron Jobs)
require('./src/services/data/syncToMongo');     // <--- QUAN TRỌNG: Lưu data vào DB
require('./src/services/ops/DataLifeCycle');
require('./src/workers/ScoutWorker');           // <--- QUAN TRỌNG: Radar tìm coin

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
    console.log('🦾 [SYSTEM] Đang khởi động Chế độ THU THẬP DỮ LIỆU...');

    // 1. Khởi tạo Cơ sở dữ liệu
    await connectDB();

    // 2. Mở cổng API cho Dashboard
    startApiServer();

    // 3. Khởi tạo Thông tin Sàn & Danh sách Coin
    await ExchangeInfo.init();
    let symbolList = getActiveSymbolList();

    if (symbolList.length === 0) {
        console.warn('⚠️ Tier List trống. Đợi ScoutWorker tìm coin...');
        await new Promise(resolve => setTimeout(resolve, 6000));
        symbolList = getActiveSymbolList();
    }

    // 4. Tự chữa lành dữ liệu khuyết
    await healDataGaps(symbolList);
    
    // ĐÃ TẮT: Khởi tạo tài khoản giả lập
    // await PaperExchange.initAccount();

    // 5. Khởi động Tai Mắt (WebSocket Vi cấu trúc)
    StreamAggregator.symbols = symbolList;
    StreamAggregator.start();

    console.log(`\n🚀 [SYSTEM] DATA HARVESTER ONLINE`);
    console.log(`📊 Đang hút máu: ${symbolList.length} đồng coin vào MongoDB.\n`);

    let lastProcessedMinute = -1;

    // ==========================================================
    // VÒNG LẶP KIỂM TRA TRẠNG THÁI (HEARTBEAT LOOP)
    // ==========================================================
    setInterval(async () => {
        const now = new Date();
        const currentMinute = now.getMinutes();

        // ĐÃ TẮT: LUỒNG 1 - LÍNH BẮN TỈA
        // await PaperExchange.monitorTrades();

        // LUỒNG 2: Giám sát luồng dữ liệu (1 phút/lần)
        if (now.getSeconds() > 10 || currentMinute === lastProcessedMinute) return;
        lastProcessedMinute = currentMinute;

        const currentSymbols = getActiveSymbolList();
        let readyCount = 0;

        for (const symbol of currentSymbols) {
            if (InMemoryBuffer.isReady(symbol)) {
                readyCount++;
            }
            
            // ĐÃ TẮT: TOÀN BỘ KHỐI AI PREDICT VÀ ĐẶT LỆNH PAPER
            /*
            const modelPath = path.join(__dirname, `model_v16_${symbol}.onnx`);
            if (!fs.existsSync(modelPath)) continue;
            const sortedHistory = InMemoryBuffer.getHistoryObjects(symbol);
            if (!sortedHistory || sortedHistory.length < 16) continue;
            const prediction = await AiEngine.predict(sortedHistory, symbol);
            const decision = DeepThinker.evaluate(symbol, prediction.winProb, sortedHistory, prediction.features);
            if (decision.approved) { ... }
            */
        }
        
        console.log(`📡 [STATUS] ${readyCount}/${currentSymbols.length} coin đã sẵn sàng dữ liệu Live.`);

    }, 500);
}

bootstrap().catch(err => {
    console.error('❌ Lỗi khởi động trầm trọng:', err);
    process.exit(1);
});