import mongoose from 'mongoose';
import axios from 'axios';
import { rsi } from 'technicalindicators';
import { VerifiedSymbol } from './database/models/VerifiedSymbol';
import { calcATR, calcNadaraya } from './engine/indicators';

const MONGODB_URI = "mongodb+srv://assistantsupdev_db_user:rCp0BrUushhwIKR8@binance.bjmukc0.mongodb.net/?appName=Binance";
const FEE = 0.0012;
const ALL_INTERVALS = ['1m', '15m', '1h', '4h', '1d'];

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
    'C98USDT', 'SFPUSDT', 'GMTUSDT', 'STRKUSDT', 'ZKUSDT', 'AEVOUSDT', 'ETHFIUSDT', 'METISUSDT', 'LUNCUSDT', 'USTCUSDT'
]

// --- MÁY CÀO ĐA TẦNG (1 NĂM) ---
async function crawlFullContext(symbol: string) {
    const oneYearAgo = Date.now() - (180 * 24 * 60 * 60 * 1000);

    for (const interval of ALL_INTERVALS) {
        let startTime = oneYearAgo;
        console.log(`  [Crawler] 📥 Đang nạp nến ${interval} cho ${symbol}...`);

        while (startTime < Date.now()) {
            try {
                const url = `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=${interval}&startTime=${startTime}&limit=1500`;
                const { data } = await axios.get(url);
                if (!data || data.length === 0) break;

                const ops = data.map((k: any) => ({
                    updateOne: {
                        filter: { s: symbol, i: interval, t: k[0] },
                        update: { $set: { s: symbol, i: interval, t: k[0], o: k[1], h: k[2], l: k[3], c: k[4], v: k[5] } },
                        upsert: true
                    }
                }));
                await HistoricalKline.bulkWrite(ops);
                startTime = data[data.length - 1][0] + 1;
                // Nghỉ ngắn để tránh bị Binance ban IP
                await new Promise(r => setTimeout(r, 150));
            } catch (e) {
                console.error(`  [Crawler] ⚠️ Lỗi nhẹ tại ${interval}, đang thử lại...`);
                await new Promise(r => setTimeout(r, 2000));
            }
        }
    }
}

// --- HÀM MÔ PHỎNG CHIẾN THUẬT ---
async function runSimulation(symbol: string, type: 'SCALP' | 'SWING') {
    // SCALP dùng nến 15m làm chuẩn, SWING dùng 4h
    const interval = type === 'SCALP' ? '15m' : '4h';
    const klines = await HistoricalKline.find({ s: symbol, i: interval }).sort({ t: 1 }).lean();

    if (klines.length < 500) return { pnl: -999, wr: 0 };

    const closes = klines.map(k => k.c);
    const smooth = calcNadaraya(closes, 20, 8);
    const rsiVals = rsi({ period: 14, values: closes });
    const atrVals = calcATR(klines.map(k => k.h), klines.map(k => k.l), closes, 14);

    const kVal = type === 'SCALP' ? 0.45 : 0.8;
    const trailMult = type === 'SCALP' ? 1.5 : 3.0;

    let equity = 0, trades = 0, wins = 0;

    for (let i = 30; i < closes.length - 1; i++) {
        const slope = (smooth[i] - smooth[i - 10]) / smooth[i - 10] * 100;
        const rVal = rsiVals[i - 14];
        let side = 0;

        if (rVal <= 35 && slope > 0.02) side = 1;
        else if (rVal >= 65 && slope < -0.02) side = -1;

        if (side !== 0) {
            const entry = closes[i];
            let sl = entry - (atrVals[i] * 1.5 * side);
            let maxP = entry;
            for (let j = i + 1; j < closes.length; j++) {
                maxP = side === 1 ? Math.max(maxP, closes[j]) : Math.min(maxP, closes[j]);
                const ts = side === 1 ? maxP - (atrVals[i] * trailMult) : maxP + (atrVals[i] * trailMult);
                if ((side === 1 && ts > sl) || (side === -1 && ts < sl)) sl = ts;

                if ((side === 1 && closes[j] <= sl) || (side === -1 && closes[j] >= sl)) {
                    const p = ((closes[j] - entry) / entry * side) - FEE;
                    equity += p; trades++; if (p > 0) wins++;
                    i = j; break;
                }
            }
        }
    }
    return { pnl: equity * 100, wr: (wins / trades) * 100, trades };
}

// --- TIẾN TRÌNH CHÍNH ---
async function main() {
    await mongoose.connect(MONGODB_URI);
    console.log('--- 🚀 ELITE 10 SCREEEN v10: FULL CONTEXT (1m -> 1d) ---');

    for (const sym of SYMBOLS_100) {
        // Kiểm tra xem đã có trong Top 10 chưa
        const existing = await VerifiedSymbol.findOne({ symbol: sym, status: 'ACTIVE' });
        if (existing) {
            console.log(`[${sym}] 🛡️ Đã có hộ khẩu Top 10. Giữ nguyên dữ liệu.`);
            continue;
        }

        console.log(`\n[${sym}] 🛠️ Đang sát hạch ứng viên...`);

        // 1. Cào đủ 5 khung nến trong 1 năm
        await crawlFullContext(sym);

        // 2. Chạy test kép
        const scalp = await runSimulation(sym, 'SCALP');
        const swing = await runSimulation(sym, 'SWING');

        const best = scalp.pnl > swing.pnl ? { ...scalp, type: 'SCALP' } : { ...swing, type: 'SWING' };

        // 3. So sánh với Top 10 hiện tại
        const top10 = await VerifiedSymbol.find({ status: 'ACTIVE' }).sort({ winRate: -1 }).limit(10).lean();

        let isElite = false;
        if (top10.length < 10) {
            isElite = true;
        } else {
            const weakest = top10[top10.length - 1];
            if (best.pnl > weakest.winRate) {
                console.log(`  🏆 ${sym} vượt mặt ${weakest.symbol}.`);
                await VerifiedSymbol.deleteOne({ symbol: weakest.symbol });
                // Xóa SẠCH 5 khung nến của thằng bị loại
                await HistoricalKline.deleteMany({ s: weakest.symbol });
                isElite = true;
            }
        }

        if (isElite) {
            await VerifiedSymbol.findOneAndUpdate({ symbol: sym }, {
                status: 'ACTIVE', winRate: best.pnl, recommendedStrategy: best.type, lastTested: new Date()
            }, { upsert: true });
            console.log(`  ✅ DUYỆT ${sym}: Giữ lại toàn bộ 5 khung nến.`);
        } else {
            console.log(`  ❌ ${sym} không đủ trình. XÓA SẠCH nến ứng viên.`);
            await HistoricalKline.deleteMany({ s: sym });
        }
    }
    console.log('\n--- ✅ HOÀN TẤT BỘ LỌC TINH ANH ---');
    await mongoose.disconnect();
}

main();