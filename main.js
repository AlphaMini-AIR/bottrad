/**
 * main.js - Version 4.2 (Fixed Execution & Radar)
 */
const connectDB = require('./src/config/db');
const ExchangeInfo = require('./src/services/binance/ExchangeInfo');
const MasterDataFetcher = require('./src/services/data/fetch_master_data');
const AiEngine = require('./src/services/ai/AiEngine');
const StrategyRouter = require('./src/services/ai/StrategyRouter');
const OrderManager = require('./src/services/execution/OrderManager'); // FIXED: Import OrderManager
const PaperExchange = require('./src/services/execution/PaperExchange'); // FIXED: Import Radar giả lập
const MarketData = require('./src/models/MarketData');
const fs = require('fs');

async function bootstrap() {
    await connectDB();
    await ExchangeInfo.init();

    console.log('\n🚀 [SYSTEM] AI Quant Sniper V4.2 Dynamic Orchestrator Ready!');

    setInterval(async () => {
        const now = new Date();
        if (now.getSeconds() !== 1) return;

        if (!fs.existsSync('./white_list.json')) return;
        const rawWhiteList = JSON.parse(fs.readFileSync('./white_list.json'));
        
        // FIXED: Chống lỗi parse nếu white_list chưa được update từ mảng sang object
        const active_coins = Array.isArray(rawWhiteList.active_coins) 
            ? rawWhiteList.active_coins.reduce((acc, coin) => ({ ...acc, [coin]: { strategy: "SWING", min_prob: 0.6 } }), {})
            : rawWhiteList.active_coins;

        for (const symbol in active_coins) {
            try {
                const meta = active_coins[symbol]; 
                
                await MasterDataFetcher.fetchAndSave(symbol);

                const history = await MarketData.find({ symbol }).sort({ timestamp: -1 }).limit(721);
                if (history.length < 721) continue;
                const sortedHistory = history.reverse();
                const currentPrice = sortedHistory[sortedHistory.length - 1].close;

                // --- FIXED: KÍCH HOẠT RADAR CHỐT LỜI/CẮT LỖ CHO PAPER TRADING ---
                await PaperExchange.monitorPrices({ [symbol]: currentPrice });

                // 1. Hỏi AI (Truyền thêm symbol để lấy đúng não)
                const winProb = await AiEngine.predict(sortedHistory, symbol);

                // 2. Trọng tài Router 
                const decision = await StrategyRouter.evaluate(symbol, sortedHistory, winProb, meta);

                if (decision.approved) {
                    // 3 & 4. FIXED: Đẩy qua OrderManager để tự động định tuyến Paper/Live và tính Risk
                    await OrderManager.executeSignal(decision, currentPrice);
                }
            } catch (error) {
                console.error(`❌ [SYSTEM ERROR] Lỗi tại ${symbol}:`, error.message);
            }
        }
    }, 1000);
}

bootstrap();