// src/ml/historicalLearner.ts
import mongoose from 'mongoose';
import axios from 'axios';
import { rsi } from 'technicalindicators';
import { SymbolConfig, type ISymbolConfig } from '../database/models/SymbolConfig';
import { calcATR, calcADX, calcNadaraya } from '../engine/indicators';

// --- CẤU HÌNH ---
const BIG_CAP_COINS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT'];
const SLIPPAGE_AND_FEE = 0.0012; // Phí 0.12% cho 1 vòng trade (Standard Futures)

// --- MODELS ---
const HistoricalKline = mongoose.models.HistoricalKline || mongoose.model('HistoricalKline', new mongoose.Schema({
    s: { type: String, index: true }, i: { type: String, index: true },
    t: { type: Number, index: true }, o: Number, h: Number, l: Number, c: Number, v: Number
}));

// --- HÀM BỔ TRỢ (HELPER) ---

/**
 * Cào nến từ Binance Futures nếu máy chưa có
 */
async function fetchAndSaveMissingData(symbol: string, interval: string) {
    try {
        console.log(`  [Fetcher] 📥 Đang nạp 1500 nến mới từ Binance cho ${symbol}...`);
        const url = `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=${interval}&limit=1500`;
        const response = await axios.get(url);

        const klines = response.data.map((k: any) => ({
            s: symbol,
            i: interval,
            t: k[0],
            o: parseFloat(k[1]),
            h: parseFloat(k[2]),
            l: parseFloat(k[3]),
            c: parseFloat(k[4]),
            v: parseFloat(k[5])
        }));

        // Lưu vào DB (Bỏ qua nếu trùng ID/Time)
        await HistoricalKline.insertMany(klines, { ordered: false }).catch(() => { });
        return true;
    } catch (error: any) {
        console.error(`  [Fetcher] ❌ Lỗi cào ${symbol}: ${error.message}`);
        return false;
    }
}

/**
 * Tính độ dốc (Slope) dựa trên hồi quy tuyến tính
 */
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

/**
 * 🚀 MÔ PHỎNG CHIẾN THUẬT HYBRID (NADARAYA + TRAILING STOP)
 */
function simulateHybrid(klines: any[], params: { os: number, ob: number, k: number }) {
    let equity = 0, trades = 0, wins = 0;

    const closes = klines.map(k => k.c);
    const highs = klines.map(k => k.h);
    const lows = klines.map(k => k.l);
    const opens = klines.map(k => k.o);
    const volumes = klines.map(k => k.v);

    // Làm mượt Nadaraya
    const smoothCloses = calcNadaraya(closes, 20, 8);
    const rsiVals = rsi({ period: 14, values: closes });
    const atrVals = calcATR(highs, lows, closes, 14);
    const adxResults = calcADX(highs, lows, closes, 14);

    for (let i = 30; i < closes.length - 1; i++) {
        const currentADX = adxResults[i - 14]?.adx || 0;
        const rVal = rsiVals[i - 14];
        const atr = atrVals[i];
        if (!atr) continue;

        const slope = predictSlope(smoothCloses.slice(0, i + 1), 10);
        const avgVol = volumes.slice(i - 20, i).reduce((a, b) => a + b, 0) / 20;

        let side = 0;
        if (currentADX >= 25) { // Breakout
            const prevRange = highs[i - 1] - lows[i - 1];
            const targetLong = opens[i] + (prevRange * params.k);
            const targetShort = opens[i] - (prevRange * params.k);
            if (closes[i] > targetLong && slope > 0.02 && volumes[i] > avgVol) side = 1;
            else if (closes[i] < targetShort && slope < -0.02 && volumes[i] > avgVol) side = -1;
        } else { // Pullback
            if (rVal <= params.os && slope > 0.015) side = 1;
            else if (rVal >= params.ob && slope < -0.015) side = -1;
        }

        if (side !== 0) {
            const entryPrice = closes[i];
            let sl = entryPrice - (atr * 1.5 * side);
            let maxSeen = entryPrice;

            for (let j = i + 1; j < closes.length; j++) {
                const curPrice = closes[j];
                maxSeen = side === 1 ? Math.max(maxSeen, curPrice) : Math.min(maxSeen, curPrice);

                const trailingSL = side === 1 ? maxSeen - (atr * 2.0) : maxSeen + (atr * 2.0);
                if ((side === 1 && trailingSL > sl) || (side === -1 && trailingSL < sl)) sl = trailingSL;

                const hitSL = (side === 1 && curPrice <= sl) || (side === -1 && curPrice >= sl);
                const hardTP = entryPrice + (atr * 5.0 * side);
                if (hitSL || (side === 1 && curPrice >= hardTP) || (side === -1 && curPrice <= hardTP)) {
                    const pnl = ((curPrice - entryPrice) / entryPrice * side) - SLIPPAGE_AND_FEE;
                    equity += pnl; trades++;
                    if (pnl > 0) wins++;
                    i = j; break;
                }
                if (j === closes.length - 1) i = j;
            }
        }
    }
    return { wr: trades > 0 ? (wins / trades) * 100 : 0, pnl: equity * 100, trades };
}

// --- MAIN PUBLIC FUNCTION ---

/**
 * Hàm học tập: Tự động cào nến -> Grid Search -> Trả kết quả
 */
export async function learnFromHistory(symbol: string) {
    const sym = symbol.toUpperCase();
    const interval = '15m';

    try {
        // 1. Kiểm tra & Cào dữ liệu nếu thiếu
        let count = await HistoricalKline.countDocuments({ s: sym, i: interval });
        if (count < 500) {
            await fetchAndSaveMissingData(sym, interval);
        }

        // 2. Tải dữ liệu từ DB
        const start = Date.now() - 30 * 86400000;
        const klines = await HistoricalKline.find({
            s: sym, i: interval, t: { $gte: start }
        }).sort({ t: 1 }).limit(1500).lean();

        if (klines.length < 500) return null;

        // 3. Grid Search tối ưu hóa
        const osRange = [25, 30, 35], obRange = [65, 70, 75], kRange = [0.4, 0.5, 0.6];
        let best = { wr: 0, pnl: -Infinity, os: 30, ob: 70, k: 0.5, trades: 0 };

        for (const os of osRange) {
            for (const ob of obRange) {
                for (const k of kRange) {
                    const res = simulateHybrid(klines, { os, ob, k });
                    if (res.pnl > best.pnl) best = { ...res, os, ob, k };
                }
            }
        }

        // 4. Lưu kết quả vào MongoDB SymbolConfig
        const config = await SymbolConfig.findOneAndUpdate({ symbol: sym }, {
            optimalRsi: { oversold: best.os, overbought: best.ob },
            optimalK: best.k,
            backtestWinRate: best.wr,
            lastLearned: new Date()
        }, { upsert: true, new: true });

        // In kết quả ra console để Hưng theo dõi
        console.log(`  📊 [${sym}] Lãi: ${best.pnl.toFixed(2)}% | Lệnh: ${best.trades} | K: ${best.k}`);

        return { config, pnl: best.pnl, trades: best.trades, wr: best.wr };
    } catch (e) {
        return null;
    }
}

export async function getLearnedConfig(symbol: string) {
    const c = await SymbolConfig.findOne({ symbol: symbol.toUpperCase() }).lean();
    return {
        optimalRsi: c?.optimalRsi || { oversold: 35, overbought: 65 },
        optimalK: c?.optimalK || 0.5,
        classifierConfig: { adxThreshold: 25 },
        dcaDropPercent: 2.5,
        maxDca: 2,
        takeProfitPercent: 2.0,
        fastScalpTpPercent: 1.0
    };
}