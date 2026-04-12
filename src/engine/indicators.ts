// src/engine/indicators.ts - Thư viện indicator mở rộng cho bot Sniper v3
import { SMA, EMA, MACD, RSI, BollingerBands, ATR, ADX } from 'technicalindicators'; // 🟢 Đã thêm ADX vào đây

export function calcEMA(values: number[], period: number) {
    return EMA.calculate({ period, values });
}

export function calcSMA(values: number[], period: number) {
    return SMA.calculate({ period, values });
}

export function calcNadaraya(closes: number[], window = 50, bandwidth = 8) {
    const result = [];
    for (let i = 0; i < closes.length; i++) {
        let sumW = 0;
        let sumWY = 0;
        for (let j = Math.max(0, i - window); j <= i; j++) {
            // Gaussian Kernel
            const weight = Math.exp(-Math.pow(i - j, 2) / (2 * Math.pow(bandwidth, 2)));
            sumW += weight;
            sumWY += weight * closes[j];
        }
        result.push(sumWY / sumW);
    }
    return result;
}

export function calcMACD(values: number[], fast = 12, slow = 26, signal = 9) {
    return MACD.calculate({
        values,
        fastPeriod: fast,
        slowPeriod: slow,
        signalPeriod: signal,
        SimpleMAOscillator: false,
        SimpleMASignal: false
    });
}

export function calcRSI(values: number[], period = 14) {
    return RSI.calculate({ values, period });
}

export function calcBollinger(values: number[], period = 20, stdDev = 2) {
    return BollingerBands.calculate({ period, stdDev, values });
}

// Tính toán Average True Range (ATR) để đo lường biến động
export function calcATR(highs: number[], lows: number[], closes: number[], period = 14) {
    return ATR.calculate({ high: highs, low: lows, close: closes, period });
}

// 🟢 BỔ SUNG: Tính Average Directional Index (ADX) để đo sức mạnh xu hướng
export function calcADX(highs: number[], lows: number[], closes: number[], period = 14) {
    return ADX.calculate({ high: highs, low: lows, close: closes, period });
}