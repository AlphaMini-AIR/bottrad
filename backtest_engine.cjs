/**
 * backtest_engine.cjs - Version 11 (Limit Execution)
 */
const connectDB = require('./src/config/db');
const ExchangeInfo = require('./src/services/binance/ExchangeInfo');
const AiEngine = require('./src/services/ai/AiEngine');
const DeepThinker = require('./src/services/ai/DeepThinker');
const MarketData = require('./src/models/MarketData');
const fs = require('fs');

const MAKER_FEE = 0.0002; 
const TAKER_FEE = 0.0005; 

async function runBacktest() {
    await connectDB();
    await ExchangeInfo.init();

    const { active_coins } = JSON.parse(fs.readFileSync('./white_list.json'));
    const symbols = ['SOLUSDT', 'TAOUSDT', 'NEARUSDT'];

    const startTime = new Date('2026-04-01T00:00:00Z').getTime();
    const endTime = new Date('2026-04-16T23:59:59Z').getTime();

    let totalAccountBalance = 1000; 
    const INITIAL_CAPITAL = 1000;
    const report = {};

    console.log(`\n🚀 [BACKTEST V11] Khởi động chế độ Kê Lệnh Limit (C.E)...`);

    for (const symbol of symbols) {
        if (!active_coins[symbol]) continue;
        const meta = active_coins[symbol];
        let coinProfit = 0, winCount = 0, tradeCount = 0, missedLimitCount = 0;

        const testData = await MarketData.find({
            symbol, timestamp: { $gte: startTime, $lte: endTime }
        }).sort({ timestamp: 1 }).lean();

        console.log(`🔍 Đang quét ${symbol}...`);

        let lastTradeTime = 0;
        const cooldownMs = 1 * 60 * 60 * 1000; 

        for (let i = 721; i < testData.length; i++) {
            const current = testData[i];
            if (current.timestamp - lastTradeTime < cooldownMs) continue;

            const history = testData.slice(i - 721, i);
            const prediction = await AiEngine.predict(history, symbol);
            const decision = DeepThinker.evaluateLogic(
                symbol, history, prediction.winProb, current.close, current.ob_imb, prediction.features, meta
            );

            if (decision.approved) {
                // TÌM ĐIỂM KHỚP LIMIT TRONG 5 NẾN TỚI
                let isFilled = false;
                let fillIndex = i;
                for (let j = 1; j <= 5; j++) {
                    if (i + j >= testData.length) break;
                    let nextCandle = testData[i + j];
                    if ((decision.side === 'LONG' && nextCandle.low <= decision.limitEntryPrice) ||
                        (decision.side === 'SHORT' && nextCandle.high >= decision.limitEntryPrice)) {
                        isFilled = true;
                        fillIndex = i + j;
                        break;
                    }
                }

                if (!isFilled) {
                    missedLimitCount++;
                    // console.log(`   ⏳ Hủy Limit: Giá bay luôn không hồi về đón lệnh.`);
                    i += 5; // Bỏ qua 5 nến
                    continue;
                }

                // Nếu khớp Limit, setup vị thế với giá Limit
                const riskUsd = decision.suggestedRiskUSD; 
                const leverage = decision.suggestedLeverage || 10;
                
                const feePerCoin = (decision.limitEntryPrice * MAKER_FEE) + (decision.limitEntryPrice * TAKER_FEE);
                const totalLossPerCoin = decision.slDistance + feePerCoin;
                
                let safeSize = riskUsd / totalLossPerCoin;
                const precision = ExchangeInfo.getPrecision(symbol);
                let sizeDecimals = precision && precision.stepSize < 1 ? precision.stepSize.toString().split('.')[1].length : 3;
                safeSize = parseFloat((Math.floor(safeSize / (precision?.stepSize || 0.001)) * (precision?.stepSize || 0.001)).toFixed(sizeDecimals));

                const notional = safeSize * decision.limitEntryPrice;
                const requiredMargin = notional / leverage;

                if (safeSize <= 0 || notional < 5.1 || requiredMargin > totalAccountBalance) continue;

                const tradePlan = {
                    symbol: decision.symbol, side: decision.side, price: decision.limitEntryPrice, // Vào bằng Limit
                    size: safeSize, slDistance: decision.slDistance,
                    slPrice: decision.side === 'LONG' ? decision.limitEntryPrice - decision.slDistance : decision.limitEntryPrice + decision.slDistance,
                    tpPrice: decision.side === 'LONG' ? decision.limitEntryPrice + decision.tpDistance : decision.limitEntryPrice - decision.tpDistance
                };

                tradeCount++;
                // Bắt đầu mô phỏng từ cây nến khớp lệnh (fillIndex)
                const outcome = await simulateActiveTrade(symbol, fillIndex, testData, tradePlan);

                totalAccountBalance += outcome.netPnl;
                coinProfit += outcome.netPnl;
                if (outcome.netPnl > 0) winCount++;
                DeepThinker.reportTradeResult(symbol, outcome.netPnl > 0); // Báo cáo chuỗi thắng thua

                console.log(`   🎯 LỆNH ${tradeCount} [${new Date(testData[fillIndex].timestamp).toISOString().slice(11, 16)}]: ${outcome.status} | ${outcome.reason}`);
                console.log(`   💸 PnL: ${outcome.netPnl > 0 ? '+' : ''}${outcome.netPnl.toFixed(2)}$ | Risk: $${riskUsd} | Margin: $${requiredMargin.toFixed(1)} | Hold: ${outcome.candlesHeld}p\n`);

                lastTradeTime = current.timestamp;
                i = fillIndex + outcome.candlesHeld; 
            }
        }

        report[symbol] = {
            trades: tradeCount,
            missed: missedLimitCount,
            winRate: tradeCount > 0 ? ((winCount / tradeCount) * 100).toFixed(1) + '%' : '0%',
            profit: "$" + coinProfit.toFixed(2)
        };
    }

    const totalROI = ((totalAccountBalance - INITIAL_CAPITAL) / INITIAL_CAPITAL) * 100;
    console.log(`\n📊 BÁO CÁO TỔNG KẾT V11 (Limit Mode):`);
    console.table(report);
    console.log(`💵 Vốn cuối cùng: $${totalAccountBalance.toFixed(2)} | ROI thực tế: ${totalROI.toFixed(2)}%`);
    process.exit();
}

async function simulateActiveTrade(symbol, startIndex, testData, plan) {
    const notional = plan.price * plan.size;
    const entryFee = notional * MAKER_FEE; // Lệnh Limit luôn là Maker
    let candlesHeld = 0;
    const maxHoldTime = 720; 

    let currentSL = plan.slPrice;
    let isBreakEven = false; 
    let partialProfitRealized = 0; 

    const scaleOutTrigger = plan.side === 'LONG' ? plan.price + (plan.slDistance * 2.0) : plan.price - (plan.slDistance * 2.0);
    const trailingSLPrice = plan.side === 'LONG' ? plan.price + (plan.price * 0.001) : plan.price - (plan.price * 0.001);

    for (let i = startIndex; i < Math.min(startIndex + maxHoldTime, testData.length); i++) {
        candlesHeld++;
        const candle = testData[i];

        if (candlesHeld === 60 && !isBreakEven) {
            let currentPnlGross = plan.side === 'LONG' ? (candle.close - plan.price) * plan.size : (plan.price - candle.close) * plan.size;
            if (currentPnlGross < 0) return calcPnL(plan, candle.close, entryFee, 'Cắt lỗ Sớm (Hết giờ)', candlesHeld, TAKER_FEE, 0);
        }

        if (!isBreakEven) {
            if ((plan.side === 'LONG' && candle.high >= scaleOutTrigger) || 
                (plan.side === 'SHORT' && candle.low <= scaleOutTrigger)) {
                
                const partialSize = plan.size * 0.5; 
                const partialExitPrice = scaleOutTrigger;
                const partialExitFee = partialExitPrice * partialSize * MAKER_FEE;
                const partialEntryFee = entryFee * 0.5;

                let grossPartial = plan.side === 'LONG' ? (partialExitPrice - plan.price) * partialSize : (plan.price - partialExitPrice) * partialSize;
                
                partialProfitRealized = grossPartial - partialEntryFee - partialExitFee;
                plan.size -= partialSize; 
                currentSL = trailingSLPrice; 
                isBreakEven = true;
            }
        }

        if (plan.side === 'LONG') {
            if (candle.low <= currentSL) return calcPnL(plan, currentSL, entryFee * (isBreakEven ? 0.5 : 1), isBreakEven ? 'Chốt 50% & BE' : 'Cắn SL', candlesHeld, TAKER_FEE, partialProfitRealized);
            if (candle.high >= plan.tpPrice) return calcPnL(plan, plan.tpPrice, entryFee * (isBreakEven ? 0.5 : 1), 'Chạm Full TP', candlesHeld, MAKER_FEE, partialProfitRealized);
        } else {
            if (candle.high >= currentSL) return calcPnL(plan, currentSL, entryFee * (isBreakEven ? 0.5 : 1), isBreakEven ? 'Chốt 50% & BE' : 'Cắn SL', candlesHeld, TAKER_FEE, partialProfitRealized);
            if (candle.low <= plan.tpPrice) return calcPnL(plan, plan.tpPrice, entryFee * (isBreakEven ? 0.5 : 1), 'Chạm Full TP', candlesHeld, MAKER_FEE, partialProfitRealized);
        }
    }

    return calcPnL(plan, testData[Math.min(startIndex + maxHoldTime - 1, testData.length - 1)].close, entryFee * (isBreakEven ? 0.5 : 1), 'Hết time limit', candlesHeld, TAKER_FEE, partialProfitRealized);
}

function calcPnL(plan, exitPrice, remainingEntryFee, reason, candlesHeld, exitFeeType, partialProfit) {
    const exitFee = exitPrice * plan.size * exitFeeType;
    let grossPnl = plan.side === 'LONG' ? (exitPrice - plan.price) * plan.size : (plan.price - exitPrice) * plan.size;
    const netPnl = grossPnl - remainingEntryFee - exitFee + partialProfit;
    return { status: netPnl > 0 ? 'WIN' : 'LOSS', netPnl, exitPrice, reason, candlesHeld };
}

runBacktest();