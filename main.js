/**
 * main.js - Version 4.2 (Final - Dynamic Integration)
 */
const connectDB = require('./src/config/db');
const ExchangeInfo = require('./src/services/binance/ExchangeInfo');
const MasterDataFetcher = require('./src/services/data/fetch_master_data');
const AiEngine = require('./src/services/ai/AiEngine');
const StrategyRouter = require('./src/services/ai/StrategyRouter');
const RiskManager = require('./src/services/execution/risk_manager');
const LiveExchange = require('./src/services/execution/LiveExchange');
const MarketData = require('./src/models/MarketData');
const fs = require('fs');

async function bootstrap() {
    await connectDB();
    await ExchangeInfo.init();
    // Khởi tạo AiEngine (Sẽ nạp model động theo từng coin)
    await AiEngine.init();

    console.log('\n🚀 [SYSTEM] AI Quant Sniper V4.2 Dynamic Orchestrator Ready!');

    setInterval(async () => {
        const now = new Date();
        if (now.getSeconds() !== 1) return;

        if (!fs.existsSync('./white_list.json')) return;
        const { active_coins } = JSON.parse(fs.readFileSync('./white_list.json'));

        // active_coins giờ là một Object: { "BTCUSDT": { strategy: "SWING", ... }, ... }
        for (const symbol in active_coins) {
            try {
                const meta = active_coins[symbol]; // Lấy "bí kíp" riêng của coin này
                
                await MasterDataFetcher.fetchAndSave(symbol);

                const history = await MarketData.find({ symbol }).sort({ timestamp: -1 }).limit(721);
                if (history.length < 721) continue;
                const sortedHistory = history.reverse();

                // 1. Hỏi AI (Nạp model riêng của đồng coin đó)
                const winProb = await AiEngine.predict(sortedHistory, symbol);

                // 2. Trọng tài Router (Nhận meta để biết lọc theo kiểu Swing hay Scalp)
                const decision = await StrategyRouter.evaluate(symbol, sortedHistory, winProb, meta);

                if (decision.approved) {
                    // 3. Kế toán RiskManager (Sử dụng đúng atr_mult mà AI đã học được)
                    const tradePlan = await RiskManager.processSignal(decision, sortedHistory, null, meta.atr_mult);

                    if (tradePlan) {
                        // 4. Bắn lệnh thật lên Binance
                        await LiveExchange.executeTrade(
                            tradePlan.symbol, tradePlan.side, tradePlan.size, 
                            tradePlan.price, tradePlan.slPrice, tradePlan.tpPrice
                        );
                    }
                }
            } catch (error) {
                console.error(`❌ [SYSTEM ERROR] Lỗi tại ${symbol}:`, error.message);
            }
        }
    }, 1000);
}

bootstrap();