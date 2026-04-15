/**
 * main.js - AI Quant Sniper V5.3 (Shadow Mode)
 * Chức năng:
 *   - Kết nối WebSocket 5 luồng (kline, aggTrade, depth, markPrice, forceOrder).
 *   - Lưu trữ 721 nến (11 đặc trưng) vào RAM (Zero-GC).
 *   - Tính toán 11 đặc trưng, suy luận ONNX, lọc cứng qua DeepThinker.
 *   - Ghi lại tín hiệu (11 features) vào file signals_YYYY-MM-DD.json để huấn luyện.
 *   - KHÔNG thực thi lệnh thật (Shadow Mode).
 */

const connectDB = require('./src/config/db');
const ExchangeInfo = require('./src/services/binance/ExchangeInfo');
const InMemoryBuffer = require('./src/services/data/InMemoryBuffer');
const StreamAggregator = require('./src/services/binance/StreamAggregator');
const AiEngine = require('./src/services/ai/AiEngine');
const DeepThinker = require('./src/services/ai/DeepThinker');
const MarketData = require('./src/models/MarketData');
const TopTraderWorker = require('./src/workers/TopTraderWorker');
const fs = require('fs');
const path = require('path');

// ------------------------------------------------------------
// Hàm ghi log tín hiệu vào file JSON (định dạng JSON Lines)
// ------------------------------------------------------------
function logSignalToFile(symbol, decision, features11, currentPrice, winProb) {
    const now = new Date();
    const dateStr = now.toISOString().slice(0, 10); // YYYY-MM-DD
    const logFile = path.join(__dirname, `signals_${dateStr}.json`);

    const record = {
        timestamp: Date.now(),
        datetime: now.toISOString(),
        symbol,
        side: decision.side,
        winProb: winProb,
        edge: decision.side === 'LONG' ? winProb : (1 - winProb),
        price: currentPrice,
        slDistance: decision.slDistance,
        tpDistance: decision.tpDistance,
        suggestedRiskRatio: decision.suggestedRiskRatio,
        reason: decision.reason,
        features: Array.from(features11) // Float32Array -> mảng thường
    };

    // Ghi thêm một dòng JSON
    const line = JSON.stringify(record) + '\n';
    fs.appendFileSync(logFile, line);
}

// ------------------------------------------------------------
// Khởi động hệ thống
// ------------------------------------------------------------
async function bootstrap() {
    // 1. Kết nối MongoDB và tải cấu hình sàn
    await connectDB();
    await ExchangeInfo.init();

    // 2. Đọc danh sách coin từ white_list.json
    if (!fs.existsSync('./white_list.json')) {
        console.error("❌ Không tìm thấy white_list.json. Hãy chạy train_master_v8_fixed.py trước.");
        process.exit(1);
    }

    const whiteListData = JSON.parse(fs.readFileSync('./white_list.json'));
    const activeCoins = whiteListData.active_coins; // { BTCUSDT: {...}, SOLUSDT: {...}, ... }
    const symbolList = Object.keys(activeCoins);

    if (symbolList.length === 0) {
        console.error("❌ Danh sách coin trong white_list.json trống.");
        process.exit(1);
    }

    console.log(`📋 [WHITELIST] Các coin sẽ theo dõi: ${symbolList.join(', ')}`);

    // 3. Khởi động tiến trình ngầm TopTraderWorker
    TopTraderWorker.init(symbolList);
    TopTraderWorker.start();
    console.log(`⚙️ [WORKER] TopTraderWorker đã khởi động cho ${symbolList.length} coin.`);

    // 4. Nạp lịch sử 721 nến gần nhất từ MongoDB vào RAM
    console.log('\n⏳ [SYSTEM] Đang nạp lịch sử nến từ MongoDB lên RAM...');
    for (const symbol of symbolList) {
        try {
            const history = await MarketData.find({ symbol })
                .sort({ timestamp: -1 })
                .limit(721)
                .lean();

            if (history.length >= 721) {
                const sortedHistory = history.reverse(); // tăng dần thời gian
                InMemoryBuffer.hydrate(symbol, sortedHistory);
                console.log(`   ✅ ${symbol}: Đã nạp ${sortedHistory.length} nến.`);
            } else {
                console.warn(`   ⚠️ ${symbol}: Chỉ có ${history.length} nến trong DB. Bot sẽ chờ đủ 721 nến.`);
                InMemoryBuffer.initSymbol(symbol); // Vẫn khởi tạo buffer rỗng
            }
        } catch (err) {
            console.error(`   ❌ ${symbol}: Lỗi truy vấn DB - ${err.message}`);
            InMemoryBuffer.initSymbol(symbol);
        }
    }

    // 5. Khởi động WebSocket (5 luồng vi cấu trúc)
    StreamAggregator.symbols = symbolList;
    StreamAggregator.start();

    console.log('\n🚀 [SYSTEM] AI Quant Sniper V5.3 (Shadow Mode) đã sẵn sàng!');
    console.log('📌 Ghi chú: Hệ thống chỉ phân tích và ghi log, không thực thi lệnh.\n');

    // 6. Vòng lặp chính (kiểm tra mỗi 500ms, xử lý 1 lần/phút)
    let lastProcessedMinute = -1;

    setInterval(async () => {
        const now = new Date();
        const currentMinute = now.getMinutes();

        // Chỉ xử lý khi giây hiện tại nhỏ hơn 10 (đảm bảo nến mới đã được ghi vào buffer)
        // và tránh xử lý trùng lặp trong cùng một phút
        if (now.getSeconds() > 10 || currentMinute === lastProcessedMinute) return;

        lastProcessedMinute = currentMinute;

        for (const symbol of symbolList) {
            try {
                // Chỉ xử lý nếu buffer đã có đủ 721 nến
                if (!InMemoryBuffer.isReady(symbol)) {
                    continue;
                }

                // Lấy lịch sử 721 nến (dạng object, tương thích ngược)
                const sortedHistory = InMemoryBuffer.getHistoryObjects(symbol);
                if (!sortedHistory || sortedHistory.length < 721) continue;

                const currentCandle = sortedHistory[sortedHistory.length - 1];
                const currentPrice = currentCandle.close;

                // 1. Dự đoán từ AI (trả về winProb và features11)
                const prediction = await AiEngine.predict(sortedHistory, symbol);
                const winProb = prediction.winProb;
                const features11 = prediction.features;

                // 2. Lấy Orderbook Imbalance hiện tại từ RAM toàn cục
                const currentObi = global.orderbookImbalanceRaw?.[symbol] || 1.0;

                // 3. DeepThinker đánh giá tín hiệu (truyền meta để dùng sl_mult/tp_mult đúng chiến lược)
                const meta = activeCoins[symbol];
                const decision = DeepThinker.evaluateLogic(
                    symbol,
                    sortedHistory,
                    winProb,
                    currentPrice,
                    currentObi,
                    features11,
                    meta
                );

                // 4. Nếu tín hiệu được phê duyệt, ghi log và in ra màn hình
                if (decision.approved) {
                    const side = decision.side;
                    const edge = side === 'LONG' ? winProb : (1 - winProb);
                    console.log(`🔔 [${symbol}] TÍN HIỆU ${side} | winProb=${winProb.toFixed(4)} | Edge=${(edge*100).toFixed(1)}% | Giá=${currentPrice.toFixed(4)} | SL=${decision.slDistance.toFixed(4)} TP=${decision.tpDistance.toFixed(4)}`);

                    // GHI LOG 11 FEATURES VÀO FILE JSON
                    logSignalToFile(symbol, decision, features11, currentPrice, winProb);
                }
                // (Tùy chọn) Bỏ qua log khi bị từ chối để tránh quá nhiễu

            } catch (error) {
                console.error(`❌ [SYSTEM ERROR] ${symbol}:`, error.message);
            }
        }
    }, 500); // Kiểm tra mỗi 500ms
}

// ------------------------------------------------------------
// Chạy hệ thống
// ------------------------------------------------------------
bootstrap().catch(err => {
    console.error('❌ Lỗi nghiêm trọng khi khởi động:', err);
    process.exit(1);
});