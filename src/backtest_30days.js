/**
 * src/backtest_30days.js - Sát hạch Đa Chế Độ
 */
const connectDB = require('./config/db');
const Candle1m = require('./models/Candle1m');
const AiEngine = require('./services/ai/AiEngine');

async function runDualBacktest() {
    await connectDB();
    const symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT'];
    const modes = [
        { name: 'STANDARD', rr: 1.5, threshold: 0.56 },
        { name: 'HIT_AND_RUN', rr: 1.0, threshold: 0.53 }
    ];

    await AiEngine.init();
    let finalTable = [];

    for (const symbol of symbols) {
        console.log(`\n🔍 Đang sát hạch ${symbol}...`);
        const data = await Candle1m.find({ symbol }).sort({ timestamp: 1 }).lean();

        for (const mode of modes) {
            let balance = 100.0;
            let position = null;
            let wins = 0, trades = 0;

            for (let i = 240; i < data.length; i++) {
                const cur = data[i];
                // Radar SL/TP
                if (position) {
                    const isWin = position.side === 'LONG' ? cur.high >= position.tp : cur.low <= position.tp;
                    const isLoss = position.side === 'LONG' ? cur.low <= position.sl : cur.high >= position.sl;

                    if (isWin || isLoss) {
                        const pnl = isWin ? (Math.abs(position.entry - position.tp) * position.size) : -(Math.abs(position.entry - position.sl) * position.size);
                        balance += pnl - (position.entry * position.size * 0.0005);
                        if (isWin) wins++;
                        trades++;
                        position = null;
                    }
                    continue;
                }

                // AI Phán quyết
                const winProb = await AiEngine.predict(cur, data[i-1], data[i-15], data[i-240]);
                let side = winProb > mode.threshold ? 'LONG' : (winProb < (1 - mode.threshold) ? 'SHORT' : null);

                if (side && balance > 5) {
                    const risk = 0.01;
                    const slDist = cur.close * risk;
                    const sl = side === 'LONG' ? cur.close - slDist : cur.close + slDist;
                    const tp = side === 'LONG' ? cur.close + (slDist * mode.rr) : cur.close - (slDist * mode.rr);
                    const size = (balance * 0.01) / slDist;
                    position = { side, entry: cur.close, sl, tp, size };
                    balance -= (cur.close * size * 0.0005);
                }
            }
            finalTable.push({ Coin: symbol, Chế_Độ: mode.name, Lệnh: trades, WinRate: `${((wins/trades)*100 || 0).toFixed(1)}%`, PnL: `${((balance-100)).toFixed(2)}%` });
        }
    }
    console.table(finalTable);
    process.exit(0);
}

runDualBacktest();