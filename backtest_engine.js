/**
 * backtest_engine.js - Version 4.4 (14-Day Multi-Strategy Test)
 */
const connectDB = require('./src/config/db');
const ExchangeInfo = require('./src/services/binance/ExchangeInfo');
const AiEngine = require('./src/services/ai/AiEngine');
const StrategyRouter = require('./src/services/ai/StrategyRouter');
const RiskManager = require('./src/services/execution/risk_manager');
const MarketData = require('./src/models/MarketData');
const fs = require('fs');

async function runBacktest() {
    await connectDB();
    await ExchangeInfo.init();
    await AiEngine.init();

    const { active_coins } = JSON.parse(fs.readFileSync('./white_list.json'));
    const symbols = ['SOLUSDT', 'BTCUSDT', 'TAOUSDT', 'PEPEUSDT', 'ETHUSDT', 'NEARUSDT'];

    // CHỈNH THỜI GIAN: 14 ngày gần nhất
    const FOURTEEN_DAYS_MS = 14 * 24 * 60 * 60 * 1000;
    const startTime = Date.now() - FOURTEEN_DAYS_MS;

    let totalAccountBalance = 1000;
    const report = {};

    console.log(`\n🚀 [BACKTEST 14-DAY] Khởi động chiến dịch 6 coin...`);

    for (const symbol of symbols) {
        if (!active_coins[symbol]) {
            console.log(`⚠️ Skip ${symbol}: Chưa được AI gán sở trường.`);
            continue;
        }

        const meta = active_coins[symbol];
        let coinProfit = 0, winCount = 0, tradeCount = 0;

        const testData = await MarketData.find({
            symbol,
            timestamp: { $gte: startTime }
        }).sort({ timestamp: 1 }).lean();

        console.log(`🔍 Đang quét ${symbol} (${meta.strategy})...`);

        for (let i = 721; i < testData.length; i++) {
            const history = testData.slice(i - 721, i);
            const current = testData[i];

            const winProb = await AiEngine.predict(history, symbol);
            const decision = await StrategyRouter.evaluate(symbol, history, winProb, meta);

            if (decision.approved) {
                const tradePlan = await RiskManager.processSignal(decision, history, { balance: totalAccountBalance, current_total_risk: 0 }, meta.atr_mult);

                if (tradePlan) {
                    tradeCount++;
                    const outcome = simulateOutcome(testData.slice(i, i + 240), tradePlan);
                    const riskAmount = totalAccountBalance * 0.02; // Chốt rủi ro 2% mỗi lệnh

                    if (outcome === 'WIN') {
                        winCount++;
                        const p = riskAmount * 3;
                        totalAccountBalance += p;
                        coinProfit += p;
                    } else if (outcome === 'LOSS') {
                        const l = riskAmount;
                        totalAccountBalance -= l;
                        coinProfit -= l;
                    }
                }
            }
        }

        report[symbol] = {
            strategy: meta.strategy,
            trades: tradeCount,
            winRate: tradeCount > 0 ? ((winCount / tradeCount) * 100).toFixed(2) + '%' : '0%',
            profit: "$" + coinProfit.toFixed(2)
        };
    }

    console.log(`\n📊 BÁO CÁO TỔNG KẾT 14 NGÀY:`);
    console.table(report);
    console.log(`💵 Vốn cuối cùng: $${totalAccountBalance.toFixed(2)} | ROI: ${((totalAccountBalance - 1000) / 10).toFixed(2)}%`);
    process.exit();
}

function simulateOutcome(futureCandles, plan) {
    for (const candle of futureCandles) {
        if (plan.side === 'LONG') {
            if (candle.low <= plan.slPrice) return 'LOSS';
            if (candle.high >= plan.tpPrice) return 'WIN';
        } else {
            if (candle.high >= plan.slPrice) return 'LOSS';
            if (candle.low <= plan.tpPrice) return 'WIN';
        }
    }
    return 'DRAW';
}

runBacktest();