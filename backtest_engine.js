/**
 * backtest_engine.js - Version 6.0 (Active Management & Dynamic PnL)
 */
const connectDB = require('./src/config/db');
const ExchangeInfo = require('./src/services/binance/ExchangeInfo');
const AiEngine = require('./src/services/ai/AiEngine');
const StrategyRouter = require('./src/services/ai/StrategyRouter');
const RiskManager = require('./src/services/execution/risk_manager');
const MarketData = require('./src/models/MarketData');
const fs = require('fs');

const TAKER_FEE = 0.0005; // Phí 0.05%

async function runBacktest() {
    await connectDB();
    await ExchangeInfo.init();

    const { active_coins } = JSON.parse(fs.readFileSync('./white_list.json'));
    const symbols = ['SOLUSDT', 'BTCUSDT', 'TAOUSDT', 'PEPEUSDT', 'ETHUSDT', 'NEARUSDT'];

    // CHỈNH THỜI GIAN: 14 ngày gần nhất
    const FOURTEEN_DAYS_MS = 14 * 24 * 60 * 60 * 1000;
    const startTime = Date.now() - FOURTEEN_DAYS_MS;

    let totalAccountBalance = 1000;
    const report = {};

    console.log(`\n🚀 [BACKTEST 14-DAY] Khởi động chiến dịch 6 coin (Active Management)...`);

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
            const winProb = await AiEngine.predict(history, symbol);
            const decision = await StrategyRouter.evaluate(symbol, history, winProb, meta);

            if (decision.approved) {
                // SỬA LỖI: Truyền meta.sl_mult thay vì meta.atr_mult
                const tradePlan = await RiskManager.processSignal(decision, history, { balance: totalAccountBalance, current_total_risk: 0 }, meta.sl_mult);

                if (tradePlan) {
                    tradeCount++;
                    
                    // CHẠY SIMULATION LỆNH (Kiểm tra từng phút tương lai)
                    const outcome = await simulateActiveTrade(symbol, i, testData, tradePlan);

                    totalAccountBalance += outcome.netPnl;
                    coinProfit += outcome.netPnl;
                    if (outcome.netPnl > 0) winCount++;

                    console.log(`   🔔 KẾT QUẢ LỆNH: ${outcome.status} | Lý do: ${outcome.reason}`);
                    console.log(`   💸 PnL: ${outcome.netPnl > 0 ? '+' : ''}${outcome.netPnl.toFixed(2)}$ | Kéo dài: ${outcome.candlesHeld} phút\n`);

                    // KHÓA VỊ THẾ: Nhảy cóc thời gian bằng đúng số phút đã giữ lệnh để không bị mở đè lệnh
                    i += outcome.candlesHeld; 
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

/**
 * Hàm mô phỏng quản trị lệnh từng phút (Active Management)
 */
async function simulateActiveTrade(symbol, startIndex, testData, plan) {
    const entryPrice = plan.price;
    const size = plan.size;
    const notional = entryPrice * size;
    const entryFee = notional * TAKER_FEE;

    let candlesHeld = 0;
    const maxHoldTime = 240; // Giữ tối đa 4 tiếng

    for (let i = startIndex; i < Math.min(startIndex + maxHoldTime, testData.length); i++) {
        candlesHeld++;
        const candle = testData[i];

        // 1. KIỂM TRA CHẠM STOPLOSS HOẶC TAKEPROFIT CỨNG
        if (plan.side === 'LONG') {
            if (candle.low <= plan.slPrice) return calcPnL(plan, plan.slPrice, entryFee, 'Cắn SL Cứng', candlesHeld);
            if (candle.high >= plan.tpPrice) return calcPnL(plan, plan.tpPrice, entryFee, 'Chạm TP Cứng', candlesHeld);
        } else {
            if (candle.high >= plan.slPrice) return calcPnL(plan, plan.slPrice, entryFee, 'Cắn SL Cứng', candlesHeld);
            if (candle.low <= plan.tpPrice) return calcPnL(plan, plan.tpPrice, entryFee, 'Chạm TP Cứng', candlesHeld);
        }

        // 2. KIỂM TRA ĐÓNG LỆNH CHỦ ĐỘNG (AI REVERSAL EXIT)
        // Chỉ kích hoạt cắt sớm sau khi lệnh đã chạy được ít nhất 2 phút để tránh nhiễu
        if (candlesHeld > 2) {
            const currentHistory = testData.slice(i - 721, i + 1);
            if (currentHistory.length >= 721) {
                const futureWinProb = await AiEngine.predict(currentHistory, symbol);
                const mockPosition = { side: plan.side };
                
                const exitCheck = StrategyRouter.evaluateEarlyExit(mockPosition, currentHistory, futureWinProb);

                if (exitCheck.shouldExit) {
                    return calcPnL(plan, candle.close, entryFee, `Đóng sớm: ${exitCheck.reason}`, candlesHeld);
                }
            }
        }
    }

    // Hết giờ mà chưa chạm gì thì đóng lệnh ở giá hiện tại
    const finalClose = testData[Math.min(startIndex + maxHoldTime - 1, testData.length - 1)].close;
    return calcPnL(plan, finalClose, entryFee, 'Hết thời gian giữ lệnh (Time Limit)', candlesHeld);
}

/**
 * Tính toán PnL chính xác có trừ phí sàn
 */
function calcPnL(plan, exitPrice, entryFee, reason, candlesHeld) {
    const exitNotional = exitPrice * plan.size;
    const exitFee = exitNotional * TAKER_FEE;

    let grossPnl = 0;
    if (plan.side === 'LONG') grossPnl = (exitPrice - plan.price) * plan.size;
    if (plan.side === 'SHORT') grossPnl = (plan.price - exitPrice) * plan.size;

    const netPnl = grossPnl - entryFee - exitFee;
    const status = netPnl > 0 ? 'WIN' : 'LOSS';

    return { status, netPnl, exitPrice, reason, candlesHeld };
}

runBacktest();