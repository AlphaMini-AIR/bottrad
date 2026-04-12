import { ema, rsi, atr } from 'technicalindicators';
import type { KlineData } from '../services/marketDataFetcher';
import type { WhaleTrade, MoneyFlowData } from '../services/marketDataFetcher';

// ─── INTERFACES ────────────────────────────────────────────────────────────────

export interface LearnedConfig {
  optimalRsi: {
    oversold: number;   // e.g. 30 — ngưỡng RSI pullback cho LONG
    overbought: number; // e.g. 70 — ngưỡng RSI pullback cho SHORT
  };
}

export type Signal = 'LONG' | 'SHORT' | 'HOLD';
export type Duration = 'SWING' | 'FAST_SCALP';

export interface AnalysisResult {
  signal: Signal;
  score: number;          // 0-100, mức độ tự tin
  duration: Duration;
  predictedPath: number[]; // 3 mức giá tiếp theo dựa trên ATR 15m
  reason: string;          // lý do chính cho signal
}

// ─── CONSTANTS ─────────────────────────────────────────────────────────────────

const EMA_PERIOD = 50;
const RSI_PERIOD = 14;
const ATR_PERIOD = 14;
const WHALE_LOOKBACK_MS = 15 * 60 * 1000; // 15 phút gần nhất

// ─── MAIN FUNCTION ─────────────────────────────────────────────────────────────

/**
 * Phân tích 3 tầng: Sóng Lớn (4H) → Sức mạnh (1H) → Vào lệnh (15M).
 * Nguyên tắc tối thượng: Bảo vệ vốn. Không chặn đầu xe lửa.
 *
 * @param klines4H  - Mảng nến 4H (tối thiểu 50 nến cho EMA 50)
 * @param klines1H  - Mảng nến 1H (tối thiểu 2 nến gần nhất)
 * @param klines15M - Mảng nến 15M (tối thiểu 14 nến cho RSI + ATR)
 * @param whaleData - Mảng dữ liệu cá mập gần nhất
 * @param learnedConfig - Cấu hình RSI động từ ML feedback loop
 */
export function analyze3Tier(
  klines4H: KlineData[],
  klines1H: KlineData[],
  klines15M: KlineData[],
  whaleData: WhaleTrade[],
  learnedConfig: LearnedConfig,
  moneyFlow?: MoneyFlowData | null,
): AnalysisResult {
  // ── Tầng 1: Sóng Lớn 4H (EMA 50) ──────────────────────────────────────────
  const tier1 = analyzeTier1(klines4H);
  if (tier1.signal === 'HOLD') {
    return buildResult('HOLD', 0, 'SWING', klines15M, tier1.reason);
  }

  // ── Tầng 2: Sức mạnh 1H (Volume confirmation) ──────────────────────────────
  const tier2 = analyzeTier2(klines1H, tier1.signal);
  if (tier2.signal === 'HOLD') {
    return buildResult('HOLD', 20, 'SWING', klines15M, tier2.reason);
  }

  // ── Tầng 3: Vào lệnh 15M (RSI pullback) ───────────────────────────────────
  const tier3 = analyzeTier3(klines15M, tier1.signal, learnedConfig);
  if (tier3.signal === 'HOLD') {
    return buildResult('HOLD', 40, 'SWING', klines15M, tier3.reason);
  }

  // ── Cộng hưởng Cá mập ──────────────────────────────────────────────────────
  const whaleConfluence = checkWhaleConfluence(whaleData, tier1.signal, klines15M);

  // ── Dòng tiền thông minh + phát hiện bẫy ──────────────────────────────────
  const moneyFlowResult = analyzeMoneyFlow(moneyFlow, tier1.signal);

  const duration: Duration = whaleConfluence.aligned ? 'FAST_SCALP' : 'SWING';
  const score = calculateScore(tier1.strength, tier2.strength, tier3.strength, whaleConfluence.aligned, moneyFlowResult);

  // Nếu phát hiện bẫy cao → HOLD
  if (moneyFlowResult.trapRisk >= 0.8 && score < 70) {
    const trapReason = [
      `T1:${tier1.reason}`,
      `T2:${tier2.reason}`,
      `T3:${tier3.reason}`,
      `MF:⚠️ BẪY ${(moneyFlowResult.trapRisk * 100).toFixed(0)}% - ${moneyFlowResult.reason}`,
    ].join(' | ');
    return buildResult('HOLD', Math.min(score, 25), 'SWING', klines15M, trapReason);
  }

  const reason = [
    `T1:${tier1.reason}`,
    `T2:${tier2.reason}`,
    `T3:${tier3.reason}`,
    whaleConfluence.aligned ? `Whale:${whaleConfluence.reason}` : null,
    moneyFlowResult.reason ? `MF:${moneyFlowResult.reason}` : null,
  ].filter(Boolean).join(' | ');

  return buildResult(tier1.signal, score, duration, klines15M, reason);
}

// ─── TẦNG 1: SÓNG LỚN 4H ─────────────────────────────────────────────────────

interface TierResult {
  signal: Signal;
  strength: number; // 0-1
  reason: string;
}

function analyzeTier1(klines4H: KlineData[]): TierResult {
  const closes = klines4H.map((k) => k.close);

  if (closes.length < EMA_PERIOD) {
    return { signal: 'HOLD', strength: 0, reason: `4H: Thiếu data (${closes.length}/${EMA_PERIOD} nến)` };
  }

  const emaValues = ema({ period: EMA_PERIOD, values: closes });

  if (emaValues.length === 0) {
    return { signal: 'HOLD', strength: 0, reason: '4H: Không tính được EMA 50' };
  }

  const currentPrice = closes[closes.length - 1];
  const currentEma = emaValues[emaValues.length - 1];
  const distance = ((currentPrice - currentEma) / currentEma) * 100;

  // Giá trên EMA 50 → chỉ LONG, dưới → chỉ SHORT
  if (currentPrice > currentEma) {
    return {
      signal: 'LONG',
      strength: Math.min(Math.abs(distance) / 5, 1), // normalize 0-5% → 0-1
      reason: `4H LONG: Price ${currentPrice.toFixed(2)} > EMA50 ${currentEma.toFixed(2)} (+${distance.toFixed(2)}%)`,
    };
  } else {
    return {
      signal: 'SHORT',
      strength: Math.min(Math.abs(distance) / 5, 1),
      reason: `4H SHORT: Price ${currentPrice.toFixed(2)} < EMA50 ${currentEma.toFixed(2)} (${distance.toFixed(2)}%)`,
    };
  }
}

// ─── TẦNG 2: SỨC MẠNH 1H ─────────────────────────────────────────────────────

function analyzeTier2(klines1H: KlineData[], direction: Signal): TierResult {
  if (klines1H.length < 2) {
    return { signal: 'HOLD', strength: 0, reason: '1H: Thiếu data (< 2 nến)' };
  }

  // Kiểm tra nến 1H thuận hướng 4H
  const lastCandle = klines1H[klines1H.length - 1];
  const prevCandle = klines1H[klines1H.length - 2];

  const isBullishCandle = lastCandle.close > lastCandle.open;
  const isBearishCandle = lastCandle.close < lastCandle.open;

  const isCandleAligned =
    (direction === 'LONG' && isBullishCandle) ||
    (direction === 'SHORT' && isBearishCandle);

  if (!isCandleAligned) {
    return {
      signal: 'HOLD',
      strength: 0,
      reason: `1H: Nến ngược hướng ${direction} (close ${lastCandle.close.toFixed(2)} vs open ${lastCandle.open.toFixed(2)})`,
    };
  }

  // Volume 2 nến gần nhất: nếu giảm dần → lực yếu, giảm strength nhưng vẫn cho qua
  const vol1 = prevCandle.volume;
  const vol2 = lastCandle.volume;

  if (vol2 < vol1 * 0.5) {
    // Volume giảm quá mạnh (>50%) → HOLD
    return {
      signal: 'HOLD',
      strength: 0,
      reason: `1H HOLD: Volume sụt mạnh (${vol1.toFixed(0)} → ${vol2.toFixed(0)}). Lực cạn.`,
    };
  }

  if (vol2 < vol1) {
    // Volume giảm nhẹ → vẫn cho qua nhưng strength thấp
    const volRatio = vol2 / vol1;
    return {
      signal: direction,
      strength: Math.min(volRatio * 0.5, 0.4), // Low strength khi volume giảm
      reason: `1H WEAK: Vol giảm nhẹ ${volRatio.toFixed(2)}x (${vol1.toFixed(0)} → ${vol2.toFixed(0)})`,
    };
  }

  // Volume tăng/đều → Đạt
  const volRatio = vol2 / vol1;
  return {
    signal: direction,
    strength: Math.min(volRatio / 2, 1), // ratio 2x → strength 1
    reason: `1H OK: Vol tăng ${volRatio.toFixed(2)}x (${vol1.toFixed(0)} → ${vol2.toFixed(0)})`,
  };
}

// ─── TẦNG 3: VÀO LỆNH 15M (RSI PULLBACK) ─────────────────────────────────────

function analyzeTier3(
  klines15M: KlineData[],
  direction: Signal,
  config: LearnedConfig,
): TierResult {
  const closes = klines15M.map((k) => k.close);

  if (closes.length < RSI_PERIOD + 1) {
    return { signal: 'HOLD', strength: 0, reason: `15M: Thiếu data (${closes.length}/${RSI_PERIOD + 1} nến)` };
  }

  const rsiValues = rsi({ period: RSI_PERIOD, values: closes });

  if (rsiValues.length === 0) {
    return { signal: 'HOLD', strength: 0, reason: '15M: Không tính được RSI' };
  }

  const currentRsi = rsiValues[rsiValues.length - 1];
  const { oversold, overbought } = config.optimalRsi;

  // LONG: Tìm pullback — RSI chạm/dưới ngưỡng oversold (giá hồi trong uptrend)
  if (direction === 'LONG') {
    if (currentRsi <= oversold) {
      const strength = 1 - (currentRsi / oversold); // RSI càng thấp, pullback càng sâu
      return {
        signal: 'LONG',
        strength: Math.min(strength + 0.3, 1),
        reason: `15M LONG: RSI ${currentRsi.toFixed(1)} ≤ ${oversold} (pullback zone)`,
      };
    }
    return {
      signal: 'HOLD',
      strength: 0,
      reason: `15M HOLD: RSI ${currentRsi.toFixed(1)} > ${oversold}. Chưa pullback, chờ nhịp hồi.`,
    };
  }

  // SHORT: Tìm pullback — RSI chạm/trên ngưỡng overbought (giá hồi trong downtrend)
  if (direction === 'SHORT') {
    if (currentRsi >= overbought) {
      const strength = (currentRsi - overbought) / (100 - overbought);
      return {
        signal: 'SHORT',
        strength: Math.min(strength + 0.3, 1),
        reason: `15M SHORT: RSI ${currentRsi.toFixed(1)} ≥ ${overbought} (pullback zone)`,
      };
    }
    return {
      signal: 'HOLD',
      strength: 0,
      reason: `15M HOLD: RSI ${currentRsi.toFixed(1)} < ${overbought}. Chưa pullback, chờ nhịp hồi.`,
    };
  }

  return { signal: 'HOLD', strength: 0, reason: '15M: Direction không xác định' };
}

// ─── CỘNG HƯỞNG CÁ MẬP ───────────────────────────────────────────────────────

interface WhaleConfluenceResult {
  aligned: boolean;
  reason: string;
}

function checkWhaleConfluence(
  whaleData: WhaleTrade[],
  direction: Signal,
  klines15M: KlineData[],
): WhaleConfluenceResult {
  if (whaleData.length === 0) {
    return { aligned: false, reason: 'Không có whale data' };
  }

  // Chỉ xét whale trades trong 15 phút gần nhất
  const now = klines15M.length > 0
    ? klines15M[klines15M.length - 1].closeTime
    : Date.now();

  const recentWhales = whaleData.filter(
    (w) => now - w.timestamp <= WHALE_LOOKBACK_MS,
  );

  if (recentWhales.length === 0) {
    return { aligned: false, reason: 'Whale trades quá cũ (> 15m)' };
  }

  // Tính net whale volume theo hướng
  let buyVol = 0;
  let sellVol = 0;
  for (const w of recentWhales) {
    if (w.side === 'BUY') buyVol += w.volume;
    else sellVol += w.volume;
  }

  const isAligned =
    (direction === 'LONG' && buyVol > sellVol) ||
    (direction === 'SHORT' && sellVol > buyVol);

  const netVol = direction === 'LONG' ? buyVol - sellVol : sellVol - buyVol;

  if (isAligned) {
    return {
      aligned: true,
      reason: `${recentWhales.length} whale(s) trùng hướng ${direction}, net $${netVol.toLocaleString('en-US', { maximumFractionDigits: 0 })}`,
    };
  }

  return {
    aligned: false,
    reason: `Whale ngược hướng (BUY: $${buyVol.toLocaleString('en-US', { maximumFractionDigits: 0 })}, SELL: $${sellVol.toLocaleString('en-US', { maximumFractionDigits: 0 })})`,
  };
}

// ─── MONEY FLOW ANALYSIS + TRAP DETECTION ──────────────────────────────────────

interface MoneyFlowResult {
  aligned: boolean;
  strength: number;  // 0-1
  trapRisk: number;  // 0-1, 1 = bẫy chắc chắn
  reason: string;
}

function analyzeMoneyFlow(moneyFlow: MoneyFlowData | null | undefined, direction: Signal): MoneyFlowResult {
  if (!moneyFlow) {
    return { aligned: false, strength: 0, trapRisk: 0, reason: '' };
  }

  let trapRisk = 0;
  let aligned = false;
  let strength = 0;
  const reasons: string[] = [];

  const { longShortRatio, fundingRate, openInterest, takerBuySellRatio } = moneyFlow;

  // 1. Long/Short Ratio Analysis (toàn bộ thị trường)
  const lsRatio = longShortRatio.longShortRatio;
  if (direction === 'LONG') {
    if (lsRatio > 2.5) {
      trapRisk += 0.2;
      reasons.push(`L/S ${lsRatio.toFixed(2)} quá đông Long, rủi ro squeeze`);
    } else if (lsRatio >= 1.0) {
      aligned = true;
      strength += 0.1;
      reasons.push(`L/S ${lsRatio.toFixed(2)} nghiêng Long`);
    } else {
      reasons.push(`L/S ${lsRatio.toFixed(2)} nghiêng Short`);
    }
  } else if (direction === 'SHORT') {
    if (lsRatio < 0.4) {
      trapRisk += 0.2;
      reasons.push(`L/S ${lsRatio.toFixed(2)} quá đông Short, rủi ro squeeze`);
    } else if (lsRatio <= 1.0) {
      aligned = true;
      strength += 0.1;
      reasons.push(`L/S ${lsRatio.toFixed(2)} nghiêng Short`);
    } else {
      reasons.push(`L/S ${lsRatio.toFixed(2)} nghiêng Long`);
    }
  }

  // 2. Top Trader Analysis (cá mập - quan trọng hơn retail)
  const topLs = moneyFlow.topTraderLongShort;
  const topPos = moneyFlow.topTraderPositions;
  if (topLs && topPos) {
    if (direction === 'LONG') {
      if (topPos.ratio > 1.2) {
        aligned = true;
        strength += 0.2;
        reasons.push(`Top traders Long ${topPos.longPosition.toFixed(0)}%`);
      } else if (topPos.ratio < 0.7) {
        trapRisk += 0.2;
        reasons.push(`Top traders Short ${topPos.shortPosition.toFixed(0)}%, ngược hướng`);
      }
      // Divergence: retail Long nhưng top traders Short → bẫy cao
      if (lsRatio > 1.5 && topPos.ratio < 0.8) {
        trapRisk += 0.25;
        reasons.push(`⚠ Divergence: Retail Long nhưng Top traders Short`);
      }
    } else if (direction === 'SHORT') {
      if (topPos.ratio < 0.8) {
        aligned = true;
        strength += 0.2;
        reasons.push(`Top traders Short ${topPos.shortPosition.toFixed(0)}%`);
      } else if (topPos.ratio > 1.3) {
        trapRisk += 0.2;
        reasons.push(`Top traders Long ${topPos.longPosition.toFixed(0)}%, ngược hướng`);
      }
      if (lsRatio < 0.6 && topPos.ratio > 1.2) {
        trapRisk += 0.25;
        reasons.push(`⚠ Divergence: Retail Short nhưng Top traders Long`);
      }
    }
  }

  // 3. Order Book Imbalance (sổ lệnh)
  const ob = moneyFlow.orderBook;
  if (ob && (ob.bidTotal > 0 || ob.askTotal > 0)) {
    if (direction === 'LONG' && ob.ratio > 1.3) {
      aligned = true;
      strength += 0.1;
      reasons.push(`OB Bid/Ask ${ob.ratio.toFixed(2)} mạnh mua`);
    } else if (direction === 'SHORT' && ob.ratio < 0.7) {
      aligned = true;
      strength += 0.1;
      reasons.push(`OB Bid/Ask ${ob.ratio.toFixed(2)} mạnh bán`);
    } else if (direction === 'LONG' && ob.ratio < 0.5) {
      trapRisk += 0.1;
      reasons.push(`OB yếu bên mua (${ob.ratio.toFixed(2)})`);
    } else if (direction === 'SHORT' && ob.ratio > 2.0) {
      trapRisk += 0.1;
      reasons.push(`OB yếu bên bán (${ob.ratio.toFixed(2)})`);
    }
  }

  // 4. Funding Rate Analysis
  if (direction === 'LONG' && fundingRate > 0.05) {
    trapRisk += 0.15;
    reasons.push(`FR ${fundingRate.toFixed(4)}% dương cao, Long trả phí`);
  } else if (direction === 'SHORT' && fundingRate < -0.05) {
    trapRisk += 0.15;
    reasons.push(`FR ${fundingRate.toFixed(4)}% âm cao, Short trả phí`);
  } else if (direction === 'LONG' && fundingRate < -0.01) {
    aligned = true;
    strength += 0.1;
    reasons.push(`FR ${fundingRate.toFixed(4)}% âm, thuận Long`);
  } else if (direction === 'SHORT' && fundingRate > 0.01) {
    aligned = true;
    strength += 0.1;
    reasons.push(`FR ${fundingRate.toFixed(4)}% dương, thuận Short`);
  }

  // 5. Open Interest Change
  const oiChange = openInterest.oiChangePercent;
  if (Math.abs(oiChange) > 10) {
    trapRisk += 0.1;
    reasons.push(`OI thay đổi ${oiChange > 0 ? '+' : ''}${oiChange.toFixed(1)}% mạnh`);
  } else if (oiChange > 3) {
    strength += 0.05;
    reasons.push(`OI +${oiChange.toFixed(1)}% tiền đang vào`);
  } else if (oiChange < -3) {
    reasons.push(`OI ${oiChange.toFixed(1)}% tiền đang rút`);
  }

  // 6. Taker Buy/Sell Ratio (áp lực thực tế)
  const takerRatio = takerBuySellRatio.ratio;
  if (direction === 'LONG' && takerRatio > 1.0) {
    aligned = true;
    strength += 0.1;
    reasons.push(`Taker B/S ${takerRatio.toFixed(2)} mua mạnh`);
  } else if (direction === 'SHORT' && takerRatio < 1.0) {
    aligned = true;
    strength += 0.1;
    reasons.push(`Taker B/S ${takerRatio.toFixed(2)} bán mạnh`);
  } else if (direction === 'LONG' && takerRatio < 0.7) {
    trapRisk += 0.1;
    reasons.push(`Taker B/S ${takerRatio.toFixed(2)} bán > mua, ngược Long`);
  } else if (direction === 'SHORT' && takerRatio > 1.3) {
    trapRisk += 0.1;
    reasons.push(`Taker B/S ${takerRatio.toFixed(2)} mua > bán, ngược Short`);
  }

  return {
    aligned,
    strength: Math.min(strength, 1),
    trapRisk: Math.min(trapRisk, 1),
    reason: reasons.join(', '),
  };
}

// ─── SCORING ───────────────────────────────────────────────────────────────────

function calculateScore(
  t1Strength: number,
  t2Strength: number,
  t3Strength: number,
  whaleAligned: boolean,
  moneyFlowResult?: MoneyFlowResult,
): number {
  // Trọng số: T1 (xu hướng lớn) > T3 (entry) > T2 (confirmation) > MF (money flow) > Whale (bonus)
  const base = t1Strength * 30 + t2Strength * 20 + t3Strength * 25;
  const whaleBonus = whaleAligned ? 8 : 0;
  const mfBonus = moneyFlowResult ? moneyFlowResult.strength * 12 : 0;
  const trapPenalty = moneyFlowResult ? moneyFlowResult.trapRisk * 15 : 0;
  return Math.min(Math.max(Math.round(base + whaleBonus + mfBonus - trapPenalty), 0), 100);
}

// ─── PREDICTED PATH (ATR-based) ────────────────────────────────────────────────

function calculatePredictedPath(klines15M: KlineData[], direction: Signal): number[] {
  if (klines15M.length < ATR_PERIOD + 1) {
    // Fallback: last close ± 0.1% steps
    const last = klines15M.length > 0 ? klines15M[klines15M.length - 1].close : 0;
    const step = last * 0.001;
    const mult = direction === 'LONG' ? 1 : -1;
    return [
      parseFloat((last + step * mult).toFixed(2)),
      parseFloat((last + step * 2 * mult).toFixed(2)),
      parseFloat((last + step * 3 * mult).toFixed(2)),
    ];
  }

  const highs = klines15M.map((k) => k.high);
  const lows = klines15M.map((k) => k.low);
  const closes = klines15M.map((k) => k.close);

  const atrValues = atr({
    period: ATR_PERIOD,
    high: highs,
    low: lows,
    close: closes,
  });

  const currentAtr = atrValues[atrValues.length - 1] || 0;
  const currentPrice = closes[closes.length - 1];
  const mult = direction === 'LONG' ? 1 : -1;

  // 3 mức giá tiếp theo: +0.5 ATR, +1.0 ATR, +1.5 ATR
  return [
    parseFloat((currentPrice + currentAtr * 0.5 * mult).toFixed(2)),
    parseFloat((currentPrice + currentAtr * 1.0 * mult).toFixed(2)),
    parseFloat((currentPrice + currentAtr * 1.5 * mult).toFixed(2)),
  ];
}

// ─── BUILDER ───────────────────────────────────────────────────────────────────

function buildResult(
  signal: Signal,
  score: number,
  duration: Duration,
  klines15M: KlineData[],
  reason: string,
): AnalysisResult {
  const predictedPath = signal === 'HOLD'
    ? []
    : calculatePredictedPath(klines15M, signal);

  return { signal, score, duration, predictedPath, reason };
}
