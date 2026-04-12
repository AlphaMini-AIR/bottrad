import mongoose from 'mongoose';
import axios from 'axios';
import { rsi } from 'technicalindicators';
import { VerifiedSymbol } from './database/models/VerifiedSymbol';
import { calcATR, calcNadaraya } from './engine/indicators';

const MONGODB_URI = "mongodb+srv://assistantsupdev_db_user:rCp0BrUushhwIKR8@binance.bjmukc0.mongodb.net/?appName=Binance";
const FEE = 0.0012;
const INITIAL_CAPITAL = 1000;
const TEST_ITERATIONS = 5; // 🎯 Số lần chạy thử để tính trung bình

const HistoricalKline = mongoose.models.HistoricalKline || mongoose.model('HistoricalKline', new mongoose.Schema({
    s: { type: String, index: true }, i: { type: String, index: true },
    t: { type: Number, index: true }, o: Number, h: Number, l: Number, c: Number, v: Number
}));

const SYMBOLS_100 = [
    'BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'ADAUSDT', 'AVAXUSDT', 'DOTUSDT', 'LINKUSDT', 'TRXUSDT',
    'MATICUSDT', 'LTCUSDT', 'BCHUSDT', 'NEARUSDT', 'OPUSDT', 'ARBUSDT', 'SUIUSDT', 'APTUSDT', 'TIAUSDT', 'SEIUSDT',
    'FETUSDT', 'RNDRUSDT', 'INJUSDT', 'STXUSDT', 'LDOUSDT', 'FILUSDT', 'HBARUSDT', 'KASUSDT', 'EGLDUSDT', 'VETUSDT',
    'NEOUSDT', 'FTMUSDT', 'MINAUSDT', 'ALGOUSDT', 'ROSEUSDT', 'KAVAUSDT', 'THETAUSDT', 'ATOMUSDT', 'SANDUSDT', 'MANAUSDT',
    'AXSUSDT', 'FLOWUSDT', 'GRTUSDT', 'CHZUSDT', 'CRVUSDT', 'AAVEUSDT', 'PYTHUSDT', 'JUPUSDT', 'DYDXUSDT', 'RUNEUSDT',
    'PENDLEUSDT', 'GMXUSDT', 'SNXUSDT', 'OCEANUSDT', 'ARKMUSDT', 'WLDUSDT', 'GALAUSDT', 'BEAMUSDT', 'IMXUSDT', 'YGGUSDT',
    'PEPEUSDT', 'WIFUSDT', 'BONKUSDT', 'FLOKIUSDT', 'ORDIUSDT', '1000SATSUSDT', 'BOMEUSDT', 'MEMEUSDT', 'JASMYUSDT', 'TRBUSDT',
    'LOOMUSDT', 'GASUSDT', 'BLZUSDT', 'FRONTUSDT', 'BIGTIMEUSDT', 'CYBERUSDT', 'MAVUSDT', 'IDUSDT', 'ANKRUSDT', 'REEFUSDT',
    'C98USDT', 'SFPUSDT', 'GMTUSDT', 'STRKUSDT', 'ZKUSDT', 'AEVOUSDT', 'ETHFIUSDT', 'METISUSDT', 'LUNCUSDT', 'USTCUSDT',
    'ZILUSDT', 'BATUSDT', 'DASHUSDT', 'ZECUSDT', 'IOTAUSDT', 'KSMUSDT', 'QTUMUSDT', 'OMGUSDT', 'ONTUSDT', 'RVNUSDT'
];

// --- 📥 CRAWLER (Giữ nguyên logic 3 tháng/1 năm) ---
async function crawlFullContext(symbol: string) {
    const config = [{ i: '1m', days: 90 }, { i: '4h', days: 365 }];
    for (const item of config) {
        let startTime = Date.now() - (item.days * 24 * 60 * 60 * 1000);
        while (startTime < Date.now()) {
            try {
                const url = `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=${item.i}&startTime=${startTime}&limit=1500`;
                const { data } = await axios.get(url);
                if (!data || data.length === 0) break;
                const ops = data.map((k: any) => ({
                    updateOne: { filter: { s: symbol, i: item.i, t: k[0] }, update: { $set: { s: symbol, i: item.i, t: k[0], o: k[1], h: k[2], l: k[3], c: k[4], v: k[5] } }, upsert: true }
                }));
                await HistoricalKline.bulkWrite(ops);
                startTime = data[data.length - 1][0] + 1;
                await new Promise(r => setTimeout(r, 120));
            } catch (e) { break; }
        }
    }
}

// --- 🧪 MONTE CARLO SIMULATOR ---
async function runMonteCarloTest(symbol: string, type: 'SCALP' | 'SWING') {
    const interval = type === 'SCALP' ? '1m' : '4h';
    const allKlines = await HistoricalKline.find({ s: symbol, i: interval }).sort({ t: 1 }).lean();
    if (allKlines.length < 1000) return -999;

    let totalIterationPnL = 0;

    // Chạy 5 lần trên các khoảng thời gian ngẫu nhiên
    for (let iter = 0; iter < TEST_ITERATIONS; iter++) {
        // Lấy ngẫu nhiên một đoạn dữ liệu (khoảng 70% tổng dữ liệu) để test tính ổn định
        const startIdx = Math.floor(Math.random() * (allKlines.length * 0.2));
        const klines = allKlines.slice(startIdx);

        const closes = klines.map(k => k.c);
        const smooth = calcNadaraya(closes, type === 'SCALP' ? 15 : 25, 8);
        const rsiVals = rsi({ period: 14, values: closes });
        const atrVals = calcATR(klines.map(k => k.h), klines.map(k => k.l), closes, 14);

        let balance = INITIAL_CAPITAL;
        const trailMult = type === 'SCALP' ? 1.3 : 3.5;
        const lookback = type === 'SCALP' ? 5 : 15;

        for (let i = 30; i < closes.length - 1; i++) {
            const slope = (smooth[i] - smooth[i - lookback]) / smooth[i - lookback] * 100;
            const rVal = rsiVals[i - 14];
            let side = 0;
            if (type === 'SCALP') {
                if (rVal <= 30 && slope > 0.04) side = 1;
                else if (rVal >= 70 && slope < -0.04) side = -1;
            } else {
                if (rVal <= 38 && slope > 0.015) side = 1;
                else if (rVal >= 62 && slope < -0.015) side = -1;
            }

            if (side !== 0) {
                const entry = closes[i];
                let sl = entry - (atrVals[i] * 1.5 * side);
                let maxP = entry;
                for (let j = i + 1; j < closes.length; j++) {
                    maxP = side === 1 ? Math.max(maxP, closes[j]) : Math.min(maxP, closes[j]);
                    const ts = side === 1 ? maxP - (atrVals[i] * trailMult) : maxP + (atrVals[i] * trailMult);
                    if ((side === 1 && ts > sl) || (side === -1 && ts < sl)) sl = ts;
                    if ((side === 1 && closes[j] <= sl) || (side === -1 && closes[j] >= sl)) {
                        balance += balance * 0.1 * (((closes[j] - entry) / entry * side) - FEE);
                        i = j; break;
                    }
                }
            }
        }
        const iterationPnL = ((balance - INITIAL_CAPITAL) / INITIAL_CAPITAL) * 100;
        totalIterationPnL += iterationPnL;
    }

    // Trả về trung bình cộng của 5 lần chạy
    return totalIterationPnL / TEST_ITERATIONS;
}

// --- 🏁 MAIN ---
async function main() {
    await mongoose.connect(MONGODB_URI);
    console.log('--- 🛡️ MONTE CARLO ELITE SCREENER: STABILITY FIRST ---');

    for (const sym of SYMBOLS_100) {
        if (await VerifiedSymbol.findOne({ symbol: sym, status: 'ACTIVE' })) continue;

        console.log(`\n[${sym}] 🛠️ Đang sát hạch 5 lần ngẫu nhiên...`);
        await crawlFullContext(sym);

        const avgScalpPnL = await runMonteCarloTest(sym, 'SCALP');
        const avgSwingPnL = await runMonteCarloTest(sym, 'SWING');

        // Chuẩn hóa lợi nhuận theo tháng để so sánh
        const scalpScore = avgScalpPnL / 3;
        const swingScore = avgSwingPnL / 12;

        let bestScore = Math.max(scalpScore, swingScore);
        let bestType: 'SCALP' | 'SWING' = scalpScore > swingScore ? 'SCALP' : 'SWING';

        if (bestScore <= 0.1) {
            console.log(`  ❌ ${sym}: Hiệu suất trung bình không đạt.`);
            await HistoricalKline.deleteMany({ s: sym });
            continue;
        }

        const top10 = await VerifiedSymbol.find({ status: 'ACTIVE' }).sort({ winRate: -1 }).limit(10).lean();

        let canJoin = false;
        if (top10.length < 10) canJoin = true;
        else if (bestScore > top10[top10.length - 1].winRate) {
            const loser = top10[top10.length - 1].symbol;
            await VerifiedSymbol.deleteOne({ symbol: loser });
            await HistoricalKline.deleteMany({ s: loser });
            canJoin = true;
        }

        if (canJoin) {
            await VerifiedSymbol.findOneAndUpdate({ symbol: sym }, {
                status: 'ACTIVE', winRate: bestScore, recommendedStrategy: bestType, lastTested: new Date()
            }, { upsert: true });
            console.log(`  ✅ ${sym} gia nhập Top 10. Avg Monthly PnL: ${bestScore.toFixed(2)}%`);
        } else {
            await HistoricalKline.deleteMany({ s: sym });
        }
    }
    await mongoose.disconnect();
}

main();