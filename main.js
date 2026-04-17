/**
 * main.js - AI Quant Sniper V15 (Data Harvester & Shadow Simulator)
 */
const connectDB = require('./src/config/db');
const ExchangeInfo = require('./src/services/binance/ExchangeInfo');
const InMemoryBuffer = require('./src/services/data/InMemoryBuffer');
const StreamAggregator = require('./src/services/binance/StreamAggregator');
const AiEngine = require('./src/services/ai/AiEngine');
const DeepThinker = require('./src/services/ai/DeepThinker');
const MarketData = require('./src/models/MarketData');
const PaperTrade = require('./src/models/PaperTrade'); // Model mới
const { healDataGaps } = require('./src/services/data/GapFiller_Startup');
require('./src/services/data/syncToMongo'); // Tự động chạy cronjob đồng bộ mỗi giờ

const fs = require('fs');
const path = require('path');

// ------------------------------------------------------------
// 1. LOGIC GIẢ LẬP ĐÁNH THỬ (PAPER TRADING)
// ------------------------------------------------------------

// Mở lệnh giả
async function openPaperTrade(symbol, decision, currentPrice, winProb, features) {
    try {
        const slPrice = decision.side === 'LONG' ? currentPrice - decision.slDistance : currentPrice + decision.slDistance;
        const tpPrice = decision.side === 'LONG' ? currentPrice + decision.tpDistance : currentPrice - decision.tpDistance;

        await PaperTrade.create({
            symbol,
            side: decision.side,
            entryPrice: currentPrice,
            winProb,
            slPrice,
            tpPrice,
            riskRatio: decision.suggestedRiskRatio,
            reason: decision.reason,
            featuresAtEntry: Array.from(features),
            status: 'OPEN'
        });
        console.log(`🚀 [SHADOW] Đã mở lệnh giả ${decision.side} cho ${symbol} tại giá ${currentPrice}`);
    } catch (err) {
        console.error("❌ Lỗi mở lệnh giả:", err.message);
    }
}

// Kiểm tra các lệnh đang mở để chốt lời/cắt lỗ
async function monitorPaperTrades(symbol, currentPrice) {
    const openTrades = await PaperTrade.find({ symbol, status: 'OPEN' });
    
    for (const trade of openTrades) {
        let isClosed = false;
        let outcome = '';

        if (trade.side === 'LONG') {
            if (currentPrice >= trade.tpPrice) { isClosed = true; outcome = 'WIN'; }
            else if (currentPrice <= trade.slPrice) { isClosed = true; outcome = 'LOSS'; }
        } else {
            if (currentPrice <= trade.tpPrice) { isClosed = true; outcome = 'WIN'; }
            else if (currentPrice >= trade.slPrice) { isClosed = true; outcome = 'LOSS'; }
        }

        if (isClosed) {
            const pnlPercent = trade.side === 'LONG' 
                ? ((currentPrice - trade.entryPrice) / trade.entryPrice) * 100 
                : ((trade.entryPrice - currentPrice) / trade.entryPrice) * 100;
            
            // Giả định vốn 100$ mỗi lệnh để tính PnL USDT
            const pnlUsdt = (pnlPercent / 100) * 100;

            await PaperTrade.findByIdAndUpdate(trade._id, {
                status: 'CLOSED',
                exitPrice: currentPrice,
                exitTime: new Date(),
                pnlPercent,
                pnlUsdt,
                outcome
            });
            console.log(`🏁 [CLOSED] ${symbol} | Kết quả: ${outcome} | ROI: ${pnlPercent.toFixed(2)}% | PnL: $${pnlUsdt.toFixed(2)}`);
        }
    }
}

// ------------------------------------------------------------
// 2. KHỞI ĐỘNG HỆ THỐNG
// ------------------------------------------------------------
async function bootstrap() {
    await connectDB();
    await ExchangeInfo.init();

    const whiteListData = JSON.parse(fs.readFileSync('./white_list.json'));
    const activeCoins = whiteListData.active_coins;
    const symbolList = Object.keys(activeCoins);

    // BƯỚC QUAN TRỌNG: Tự chữa lành dữ liệu khuyết trước khi start
    await healDataGaps(symbolList);

    // Khởi động WebSocket Vi cấu trúc (V15)
    StreamAggregator.symbols = symbolList;
    StreamAggregator.start();

    console.log('\n🚀 [SYSTEM] V15 ONLINE | Shadow Trading & Data Vault Active\n');

    let lastProcessedMinute = -1;

    setInterval(async () => {
        const now = new Date();
        const currentMinute = now.getMinutes();

        // Kiểm tra monitor lệnh mỗi 500ms để đảm bảo thoát lệnh chuẩn xác nhất
        for (const symbol of symbolList) {
            const price = global.currentMarkPrice?.[symbol]; // Lấy giá từ StreamAggregator
            if (price) await monitorPaperTrades(symbol, price);
        }

        // Logic chính: Mỗi phút xử lý 1 lần nến đóng
        if (now.getSeconds() > 10 || currentMinute === lastProcessedMinute) return;
        lastProcessedMinute = currentMinute;

        for (const symbol of symbolList) {
            try {
                if (!InMemoryBuffer.isReady(symbol)) continue;

                const sortedHistory = InMemoryBuffer.getHistoryObjects(symbol);
                if (!sortedHistory || sortedHistory.length < 721) continue;

                const currentPrice = sortedHistory[sortedHistory.length - 1].close;

                // 1. AI Engine dự đoán
                const prediction = await AiEngine.predict(sortedHistory, symbol);
                const { winProb, features } = prediction;

                // 2. Lấy OBI từ RAM
                const currentObi = global.liveMicroData?.[symbol]?.ob_imb_top20 || 1.0;

                // 3. DeepThinker đánh giá lọc cứng
                const decision = DeepThinker.evaluateLogic(
                    symbol, sortedHistory, winProb, currentPrice, currentObi, features, activeCoins[symbol]
                );

                // 4. Nếu approved -> Thực hiện Shadow Trade
                if (decision.approved) {
                    await openPaperTrade(symbol, decision, currentPrice, winProb, features);
                }

            } catch (error) {
                console.error(`❌ [ERROR] ${symbol}:`, error.message);
            }
        }
    }, 500);
}

bootstrap().catch(err => {
    console.error('❌ Lỗi khởi động:', err);
    process.exit(1);
});