/**
 * backtest_engine.cjs 
 */
const connectDB = require('./src/config/db');
const ExchangeInfo = require('./src/services/binance/ExchangeInfo');
const AiEngine = require('./src/services/ai/AiEngine');
const DeepThinker = require('./src/services/ai/DeepThinker');
const MarketData = require('./src/models/MarketData');
const fs = require('fs');

const TAKER_FEE = 0.0005; // Phí 0.05%

async function runBacktest() {
    await connectDB();
    await ExchangeInfo.init();

    const { active_coins } = JSON.parse(fs.readFileSync('./white_list.json'));
    const symbols = ['SOLUSDT', 'TAOUSDT', 'NEARUSDT'];

    const FOURTEEN_DAYS_MS = 14 * 24 * 60 * 60 * 1000;
    const startTime = Date.now() - FOURTEEN_DAYS_MS;

    let totalAccountBalance = 1000;
    const report = {};

    console.log(`\n🚀 [BACKTEST 14-DAY] Đã gắn Giáp Bất Tử và Thông số Chuẩn...`);

    for (const symbol of symbols) {
        if (!active_coins[symbol]) continue;
        const meta = active_coins[symbol];
        let coinProfit = 0, winCount = 0, tradeCount = 0;

        const testData = await MarketData.find({
            symbol, timestamp: { $gte: startTime }
        }).sort({ timestamp: 1 }).lean();

        console.log(`🔍 Đang quét ${symbol} (${meta.strategy})...`);

        let lastTradeTime = 0;
        const cooldownMs = 4 * 60 * 60 * 1000; // Cooldown 4 TIẾNG

        for (let i = 721; i < testData.length; i++) {
            const current = testData[i];

            if (current.timestamp - lastTradeTime < cooldownMs) continue;

            const history = testData.slice(i - 721, i);
            const prediction = await AiEngine.predict(history, symbol);

            const decision = DeepThinker.evaluateLogic(
                symbol, history, prediction.winProb, current.close, current.ob_imb, prediction.features, meta
            );

            if (decision.approved) {
                const riskAmount = totalAccountBalance * decision.suggestedRiskRatio;
                const feePerCoin = current.close * TAKER_FEE * 2;
                const totalLossPerCoin = decision.slDistance + feePerCoin;
                const safeSize = riskAmount / totalLossPerCoin;

                const tradePlan = {
                    symbol: decision.symbol,
                    side: decision.side,
                    price: current.close,
                    size: safeSize,
                    slPrice: decision.side === 'LONG' ? current.close - decision.slDistance : current.close + decision.slDistance,
                    tpPrice: decision.side === 'LONG' ? current.close + decision.tpDistance : current.close - decision.tpDistance,
                };

                tradeCount++;
                const outcome = await simulateActiveTrade(symbol, i, testData, tradePlan);

                totalAccountBalance += outcome.netPnl;
                coinProfit += outcome.netPnl;
                if (outcome.netPnl > 0) winCount++;

                console.log(`   🔔 LỆNH ${tradeCount}: ${outcome.status} | ${outcome.reason}`);
                console.log(`   💸 PnL: ${outcome.netPnl > 0 ? '+' : ''}${outcome.netPnl.toFixed(2)}$ | Hold: ${outcome.candlesHeld} phút\n`);

                lastTradeTime = current.timestamp;
                i += outcome.candlesHeld;
            }
        }

        report[symbol] = {
            strategy: meta.strategy, trades: tradeCount,
            winRate: tradeCount > 0 ? ((winCount / tradeCount) * 100).toFixed(2) + '%' : '0%',
            profit: "$" + coinProfit.toFixed(2)
        };
    }

    console.log(`\n📊 BÁO CÁO TỔNG KẾT 14 NGÀY:`);
    console.table(report);
    console.log(`💵 Vốn cuối cùng: $${totalAccountBalance.toFixed(2)} | ROI: ${((totalAccountBalance - 1000) / 10).toFixed(2)}%`);
    process.exit();
}

async function simulateActiveTrade(symbol, startIndex, testData, plan) {
    const notional = plan.price * plan.size;
    const entryFee = notional * TAKER_FEE;
    let candlesHeld = 0;
    const maxHoldTime = 720; // 12 Tiếng

    let currentSL = plan.slPrice;
    let isBreakEven = false; 

    // DỜI SL KHI ĐÃ ĐI ĐƯỢC 80% QUÃNG ĐƯỜNG (Tránh bị quét Retest)
    const beTrigger = plan.side === 'LONG' 
        ? plan.price + (plan.tpPrice - plan.price) * 0.80 
        : plan.price - (plan.price - plan.tpPrice) * 0.80;

    // KHI DỜI SL, DỜI QUÁ ENTRY 0.15% ĐỂ BAO TRỌN PHÍ SÀN (Thực sự Hòa vốn $0.00)
    const feeBuffer = plan.price * 0.0015; 
    const beStopPrice = plan.side === 'LONG' 
        ? plan.price + feeBuffer 
        : plan.price - feeBuffer;

    for (let i = startIndex; i < Math.min(startIndex + maxHoldTime, testData.length); i++) {
        candlesHeld++;
        const candle = testData[i];

        // 1. KÉO SL HÒA VỐN BAO PHÍ
        if (!isBreakEven) {
            if (plan.side === 'LONG' && candle.high >= beTrigger) {
                currentSL = beStopPrice; 
                isBreakEven = true;
            } else if (plan.side === 'SHORT' && candle.low <= beTrigger) {
                currentSL = beStopPrice; 
                isBreakEven = true;
            }
        }

        // 2. CHECK CHẠM SL/TP
        if (plan.side === 'LONG') {
            if (candle.low <= currentSL) {
                const reason = isBreakEven ? 'Hòa vốn (Đã bao phí)' : 'Cắn SL Cứng';
                return calcPnL(plan, currentSL, entryFee, reason, candlesHeld);
            }
            if (candle.high >= plan.tpPrice) return calcPnL(plan, plan.tpPrice, entryFee, 'Chạm TP Cứng', candlesHeld);
        } else {
            if (candle.high >= currentSL) {
                const reason = isBreakEven ? 'Hòa vốn (Đã bao phí)' : 'Cắn SL Cứng';
                return calcPnL(plan, currentSL, entryFee, reason, candlesHeld);
            }
            if (candle.low <= plan.tpPrice) return calcPnL(plan, plan.tpPrice, entryFee, 'Chạm TP Cứng', candlesHeld);
        }
    }

    return calcPnL(plan, testData[Math.min(startIndex + maxHoldTime - 1, testData.length - 1)].close, entryFee, 'Hết time limit', candlesHeld);
}

function calcPnL(plan, exitPrice, entryFee, reason, candlesHeld) {
    const exitFee = exitPrice * plan.size * TAKER_FEE;
    let grossPnl = plan.side === 'LONG' ? (exitPrice - plan.price) * plan.size : (plan.price - exitPrice) * plan.size;
    const netPnl = grossPnl - entryFee - exitFee;
    return { status: netPnl > 0 ? 'WIN' : 'LOSS', netPnl, exitPrice, reason, candlesHeld };
}

runBacktest();