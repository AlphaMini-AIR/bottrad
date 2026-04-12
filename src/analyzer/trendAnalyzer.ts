// src/analyzer/trendAnalyzer.ts
import { classifyMarket, ClassifierConfig } from '../ml/marketClassifier';
import type { KlineData, WhaleTrade, MoneyFlowData } from '../services/marketDataFetcher';
import { calcRSI, calcATR, calcADX, calcNadaraya } from '../engine/indicators';
// ─── INTERFACES ────────────────────────────────────────────────────────────────

export interface LearnedConfig {
  optimalRsi: { oversold: number; overbought: number };
  classifierConfig?: ClassifierConfig;
  dcaDropPercent?: number;
  optimalK?: number; // Giá trị K-Value từ ML (Mới)
}

export type Signal = 'LONG' | 'SHORT' | 'HOLD';
export type Duration = 'SWING' | 'FAST_SCALP';

export interface AnalysisResult {
  signal: Signal;
  score: number;
  duration: Duration;
  tpPrice?: number;
  slPrice?: number;
  predictedPath: number[];
  reason: string;
}

// ─── THUẬT TOÁN DỰ BÁO MOMENTUM ───────────────────────────────────────────────

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

// ─── MAIN FUNCTION: MULTI-STRATEGY ANALYZER ───────────────────────────────────

export function analyze3Tier(
  klines4H: KlineData[],
  klines1H: KlineData[],
  klines15M: KlineData[],
  whaleData: WhaleTrade[],
  learned: LearnedConfig,
  moneyFlow?: MoneyFlowData | null,
): AnalysisResult {

  const closes15M = klines15M.map(k => k.close);
  const smoothPrices = calcNadaraya(closes15M, 20, 8);

  const currentPrice = closes15M[closes15M.length - 1];
  const kValue = learned.optimalK || 0.5; // Mặc định 0.5 nếu chưa học được K

  // 1. PHÂN LOẠI THỊ TRƯỜNG DỰA TRÊN ADX (Đã nâng cấp ở Bước 2)
  const marketStatus = classifyMarket(
    klines15M.map(k => k.high),
    klines15M.map(k => k.low),
    closes15M,
    learned.classifierConfig
  );

  const rsi15M = calcRSI(closes15M, 14).pop() || 50;
  const slope15M = predictSlope(smoothPrices, 10);
  const atr = calcATR(klines15M.map(k => k.high), klines15M.map(k => k.low), closes15M, 14).pop() || currentPrice * 0.01;

  let signal: Signal = 'HOLD';
  let strategyUsed = '';

  // 2. LOGIC ĐA CHIẾN THUẬT (Hybrid Strategy)

  // 🟢 CHIẾN THUẬT A: VOLATILITY BREAKOUT (Khi ADX báo có Trend)
  if (marketStatus !== 'SIDEWAY') {
    const prevK = klines15M[klines15M.length - 2];
    const range = prevK.high - prevK.low;

    // Công thức: Target = Open + (Range * K)
    const targetLong = klines15M[klines15M.length - 1].open + (range * kValue);
    const targetShort = klines15M[klines15M.length - 1].open - (range * kValue);

    if (currentPrice > targetLong && slope15M > 0.01) {
      signal = 'LONG';
      strategyUsed = 'V-Breakout';
    } else if (currentPrice < targetShort && slope15M < -0.01) {
      signal = 'SHORT';
      strategyUsed = 'V-Breakout';
    }
  }

  // 🔵 CHIẾN THUẬT B: RSI PULLBACK (Khi ADX báo Sideway)
  else {
    if (rsi15M <= learned.optimalRsi.oversold && slope15M > 0.02) {
      signal = 'LONG';
      strategyUsed = 'Sideway-Pullback';
    } else if (rsi15M >= learned.optimalRsi.overbought && slope15M < -0.02) {
      signal = 'SHORT';
      strategyUsed = 'Sideway-Pullback';
    }
  }

  // 3. THIẾT LẬP RR 2:1 & BỘ LỌC PHÍ (Fee-Aware)
  const side = signal === 'LONG' ? 1 : -1;
  const slDistance = atr * 1.5;
  const tpDistance = slDistance * 2;

  const expectedProfitPct = tpDistance / currentPrice;
  const FEE_THRESHOLD = 0.004; // Tối thiểu 0.4% lợi nhuận mới đánh (để bù phí 0.12% x 2)

  if (signal !== 'HOLD' && expectedProfitPct < FEE_THRESHOLD) {
    return {
      signal: 'HOLD',
      score: 0,
      duration: 'SWING',
      predictedPath: [],
      reason: `LOẠI: Lãi dự kiến (${(expectedProfitPct * 100).toFixed(2)}%) không đủ bù phí.`
    };
  }

  return {
    signal,
    score: signal === 'HOLD' ? 0 : 85,
    duration: marketStatus === 'SIDEWAY' ? 'FAST_SCALP' : 'SWING',
    tpPrice: currentPrice + (tpDistance * side),
    slPrice: currentPrice - (slDistance * side),
    predictedPath: [],
    reason: `[${strategyUsed}] ${marketStatus} | RSI: ${rsi15M.toFixed(1)} | Slope: ${slope15M.toFixed(3)}`
  };
}