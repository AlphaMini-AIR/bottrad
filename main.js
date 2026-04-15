/**
 * main.js - Version 5.3 (Microstructure Enhanced & Multi-threaded)
 */
const connectDB = require('./src/config/db');
const ExchangeInfo = require('./src/services/binance/ExchangeInfo');
const MasterDataFetcher = require('./src/services/data/fetch_master_data');
const InMemoryBuffer = require('./src/services/data/InMemoryBuffer');
const StreamAggregator = require('./src/services/binance/StreamAggregator');
const AiEngine = require('./src/services/ai/AiEngine');
const DeepThinker = require('./src/services/ai/DeepThinker');
const OrderManager = require('./src/services/execution/OrderManager');
const PaperExchange = require('./src/services/execution/PaperExchange');
const MarketData = require('./src/models/MarketData');
const fs = require('fs');

// [BƯỚC 3]: Import Tiến trình ngầm (Background Worker)
const TopTraderWorker = require('./src/workers/TopTraderWorker');

async function bootstrap() {
    await connectDB();
    await ExchangeInfo.init();

    if (!fs.existsSync('./white_list.json')) {
        console.error("❌ Không tìm thấy white_list.json");
        process.exit(1);
    }
    
    const rawWhiteList = JSON.parse(fs.readFileSync('./white_list.json'));
    const active_coins = Array.isArray(rawWhiteList.active_coins) 
        ? rawWhiteList.active_coins.reduce((acc, coin) => ({ ...acc, [coin]: { strategy: "SWING", min_prob: 0.6 } }), {})
        : rawWhiteList.active_coins;

    const symbolList = Object.keys(active_coins);

    // ==========================================
    // 🚀 KHỞI ĐỘNG TIẾN TRÌNH NGẦM (WORKER THREAD)
    // ==========================================
    TopTraderWorker.init(symbolList);
    TopTraderWorker.start();

    console.log('\n⏳ [SYSTEM] Đang nạp lịch sử nến từ Database lên RAM. Vui lòng đợi...');

    // 1. NẠP DỮ LIỆU BAN ĐẦU
    for (const symbol of symbolList) {
        await MasterDataFetcher.fetchAndSave(symbol);
        
        const history = await MarketData.find({ symbol }).sort({ timestamp: -1 }).limit(721);
        if (history.length >= 721) {
            InMemoryBuffer.hydrate(symbol, history.reverse());
        } else {
            console.warn(`⚠️ [WARNING] ${symbol} chưa đủ 721 nến trong DB.`);
        }
    }

    // 2. KHỞI ĐỘNG WEBSOCKET (5 Luồng Vi cấu trúc)
    StreamAggregator.symbols = symbolList;
    StreamAggregator.start();

    console.log('\n🚀 [SYSTEM] AI Quant Sniper V5.3 - Kỷ Nguyên Vi Cấu Trúc Sẵn Sàng!');

    // 3. VÒNG LẶP THỰC THI CHÍNH
    let lastProcessedMinute = -1;

    setInterval(async () => {
        const now = new Date();
        const currentMinute = now.getMinutes();

        // Chỉ thức dậy vào giây đầu tiên của phút mới
        if (now.getSeconds() < 1 || currentMinute === lastProcessedMinute) return;

        lastProcessedMinute = currentMinute; 

        const evaluationPromises = symbolList.map(async (symbol) => {
            try {
                if (!InMemoryBuffer.isReady(symbol)) return;

                // Vẫn dùng hàm getHistoryObjects để duy trì sự ổn định tương thích ngược
                const sortedHistory = InMemoryBuffer.getHistoryObjects(symbol);
                const currentPrice = sortedHistory[sortedHistory.length - 1].close;

                // Kích hoạt radar dò SL/TP cho Paper Trading
                await PaperExchange.monitorPrices({ [symbol]: currentPrice });

                // ==========================================
                // LUỒNG DỮ LIỆU ĐẾN TRẠM KIỂM SOÁT
                // ==========================================
                
                // 1. Lấy Xác suất (winProb) VÀ Bối cảnh (11 features) từ AI
                const prediction = await AiEngine.predict(sortedHistory, symbol);
                const winProb = prediction.winProb;
                const features = prediction.features; 

                // Lấy thông số Order Book Imbalance Realtime (Từ RAM)
                const currentObi = global.orderbookImbalanceRaw && global.orderbookImbalanceRaw[symbol] 
                    ? global.orderbookImbalanceRaw[symbol] 
                    : 1.0;

                // 2. Chuyền toàn bộ dữ liệu vào cho DeepThinker đánh giá (Luật cứng + AI)
                const decision = DeepThinker.evaluateLogic(
                    symbol, 
                    sortedHistory, 
                    winProb, 
                    currentPrice, 
                    currentObi, 
                    features
                );

                // 3. Nếu được duyệt, giao cho OrderManager thực thi
                if (decision.approved) {
                    await OrderManager.executeSignal(decision, currentPrice);
                }

            } catch (error) {
                console.error(`❌ [SYSTEM ERROR] Lỗi tại luồng ${symbol}:`, error.message);
            }
        });

        await Promise.allSettled(evaluationPromises);

    }, 500);
}

bootstrap();


