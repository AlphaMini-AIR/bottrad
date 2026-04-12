import mongoose from 'mongoose';
import { VerifiedSymbol } from './database/models/VerifiedSymbol';
import { getLearnedConfig } from './ml/historicalLearner';
import { calcRSI, calcATR, calcNadaraya } from './engine/indicators';

const MONGODB_URI = "mongodb+srv://assistantsupdev_db_user:rCp0BrUushhwIKR8@binance.bjmukc0.mongodb.net/?appName=Binance";
const INITIAL_CAPITAL = 100; // Vốn xuất phát 100$
const FEE_PER_SIDE = 0.0006; // 0.06% phí Binance Futures

const HistoricalKline = mongoose.models.HistoricalKline || mongoose.model('HistoricalKline', new mongoose.Schema({
    s: String, i: String, t: Number, o: Number, h: Number, l: Number, c: Number, v: Number
}));

// --- HELPER: TẠO CÁC MỐC THỜI GIAN NGẪU NHIÊN THEO NĂM ---
function generateYearlyPeriods(year: number, count = 4) {
    const periods = [];
    const oneDay = 24 * 60 * 60 * 1000;
    const yearStart = new Date(`${year}-01-01`).getTime();

    // Nếu là năm hiện tại (2026), chỉ lấy đến thời điểm bây giờ
    const yearEndLimit = year === 2026 ? Date.now() - (8 * oneDay) : new Date(`${year}-12-25`).getTime();

    for (let i = 0; i < count; i++) {
        const randomStart = yearStart + Math.random() * (yearEndLimit - yearStart);
        const randomEnd = randomStart + (7 * oneDay); // Test 7 ngày
        periods.push({ start: Math.floor(randomStart), end: Math.floor(randomEnd) });
    }
    return periods.sort((a, b) => a.start - b.start);
}

function predictSlope(prices: number[], lookback = 10): number {
    if (prices.length < lookback) return 0;
    const data = prices.slice(-lookback);
    let sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;
    const n = data.length;
    for (let i = 0; i < n; i++) {
        sumX += i; sumY += data[i]; sumXY += i * data[i]; sumX2 += i * i;
    }
    const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
    return (slope / data[n - 1]) * 100;
}

async function runYearlyStressTest() {
    await mongoose.connect(MONGODB_URI);
    console.log('--- 🛡️ SNIPER TIME-MACHINE: 2025 VS 2026 VALIDATION ---');

    const years = [2025, 2026];
    const verifiedCoins = await VerifiedSymbol.find({ status: 'ACTIVE' }).lean();

    for (const year of years) {
        console.log(`\n================= THỬ THÁCH NĂM ${year} =================`);
        const periods = generateYearlyPeriods(year);

        for (let p = 0; p < periods.length; p++) {
            const { start, end } = periods[p];
            let currentBalance = INITIAL_CAPITAL;
            let trades = 0;
            let wins = 0;

            for (const coin of verifiedCoins) {
                const config = await getLearnedConfig(coin.symbol);
                const klines = await HistoricalKline.find({
                    s: coin.symbol, i: '15m',
                    t: { $gte: start, $lte: end }
                }).sort({ t: 1 }).lean();

                if (klines.length < 50) continue;

                const closes = klines.map(k => k.c);
                const smoothCloses = calcNadaraya(closes, 20, 8);
                const rsiVals = calcRSI(closes, 14);
                const atrVals = calcATR(klines.map(k => k.h), klines.map(k => k.l), closes, 14);

                for (let i = 20; i < closes.length; i++) {
                    const slope = predictSlope(smoothCloses.slice(0, i + 1), 10);
                    const rVal = rsiVals[i - 14];

                    let side = 0;
                    // Nới lỏng nhẹ để tăng tần suất: Slope > 0.015 và RSI vùng 37/63
                    if (rVal <= config.optimalRsi.oversold + 2 && slope > 0.015) side = 1;
                    else if (rVal >= config.optimalRsi.overbought - 2 && slope < -0.015) side = -1;

                    if (side !== 0) {
                        const entryPrice = closes[i];
                        const atr = atrVals[i];
                        let sl = entryPrice - (atr * 1.5 * side);
                        const tp = entryPrice + (atr * 4.0 * side);
                        let maxPrice = entryPrice;

                        for (let j = i + 1; j < closes.length; j++) {
                            const curPrice = closes[j];
                            maxPrice = side === 1 ? Math.max(maxPrice, curPrice) : Math.min(maxPrice, curPrice);

                            // Trailing Stop 1.8 ATR (Chặt chẽ hơn để bảo vệ vốn)
                            const trailingSl = side === 1 ? maxPrice - (atr * 1.8) : maxPrice + (atr * 1.8);
                            if ((side === 1 && trailingSl > sl) || (side === -1 && trailingSl < sl)) sl = trailingSl;

                            const hitSL = (side === 1 && curPrice <= sl) || (side === -1 && curPrice >= sl);
                            const hitTP = (side === 1 && curPrice >= tp) || (side === -1 && curPrice <= tp);

                            if (hitSL || hitTP) {
                                // Tính PnL thực tế: PnL % trừ đi 0.12% phí (vào + ra)
                                const tradePnlPct = ((curPrice - entryPrice) / entryPrice * side) - (FEE_PER_SIDE * 2);

                                // Đi lệnh 15% vốn mỗi lệnh để tận dụng lãi kép
                                const posSize = currentBalance * 0.15;
                                currentBalance += posSize * tradePnlPct;

                                trades++;
                                if (tradePnlPct > 0) wins++;
                                i = j; break;
                            }
                        }
                    }
                }
            }

            const totalPnL = ((currentBalance - INITIAL_CAPITAL) / INITIAL_CAPITAL) * 100;
            console.log(`📍 Kỳ ${p + 1} (${new Date(start).toLocaleDateString()}): Trades: ${trades} | WR: ${((wins / trades) * 100 || 0).toFixed(1)}% | PnL: ${totalPnL.toFixed(2)}%`);
        }
    }
    await mongoose.disconnect();
}

runYearlyStressTest();