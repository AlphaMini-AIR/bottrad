import { ema } from 'technicalindicators';
import { ActivePosition, type IActivePosition } from '../database/models/ActivePosition';
import { PaperTrade } from '../database/models/PaperTrade';
import type { AnalysisResult, Signal } from '../analyzer/trendAnalyzer';
import type { KlineData } from '../services/marketDataFetcher';

// ─── INTERFACES ────────────────────────────────────────────────────────────────

export interface PositionLearnedConfig {
  dcaDropPercent: number;  // e.g. 2 → nhồi lệnh khi lỗ 2%
  maxDca: number;          // e.g. 3 → tối đa 3 lần DCA
  takeProfitPercent: number; // e.g. 3 → chốt lời swing ở 3%
  fastScalpTpPercent: number; // e.g. 1 → chốt lời scalp ở 1%
}

export type ManageAction =
  | { type: 'TAKE_PROFIT'; reason: string; pnl: number }
  | { type: 'STOP_LOSS'; reason: string; pnl: number }
  | { type: 'DCA'; newQuantity: number; newAvgPrice: number; reason: string }
  | { type: 'HOLD'; reason: string };

// ─── CONSTANTS ─────────────────────────────────────────────────────────────────

const SLIPPAGE_AND_FEE = 0.0006; // 0.04% taker fee + 0.02% slippage
const EMA_PERIOD = 50;
const DCA_MULTIPLIER = 1.5;

// ─── MAIN FUNCTION ─────────────────────────────────────────────────────────────

/**
 * Quản lý vị thế theo 3 quy tắc ưu tiên:
 *   1. Cắt Lỗ Cứng (4H gãy xu hướng) → Market Close ngay
 *   2. Chốt Lời Nhanh (FAST_SCALP + lãi > 1%) → Close
 *   3. Nhồi Lệnh DCA (lỗ chạm ngưỡng + 4H vẫn đúng hướng)
 *
 * Nguyên tắc: Bảo vệ vốn > Tối ưu lợi nhuận.
 */
export async function managePosition(
  position: IActivePosition,
  currentPrice: number,
  currentAnalysis: AnalysisResult,
  learnedConfig: PositionLearnedConfig,
  klines4H?: KlineData[],
): Promise<ManageAction> {
  const pnlPercent = calcPnlPercent(position, currentPrice);

  // ═══════════════════════════════════════════════════════════════════════════
  // RULE 0: CẮT LỖ CỨNG — 4H gãy xu hướng → Market Close TOÀN BỘ
  // Đây là quy tắc sinh tử. Không negotiate.
  // ═══════════════════════════════════════════════════════════════════════════
  const trendBroken = isTier1Broken(position.side, currentAnalysis, klines4H);
  if (trendBroken) {
    const reason = `STOP_LOSS: 4H gãy xu hướng ${position.side}. PnL: ${pnlPercent.toFixed(2)}%. Cắt lỗ toàn bộ.`;
    await closePosition(position, currentPrice, reason);
    const pnlUsdt = calcPnlUsdt(position, currentPrice);
    return { type: 'STOP_LOSS', reason, pnl: pnlUsdt };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // RULE 1: CHỐT LỜI NHANH — FAST_SCALP + lãi > fastScalpTpPercent
  // ═══════════════════════════════════════════════════════════════════════════
  if (position.duration === 'FAST_SCALP' && pnlPercent >= learnedConfig.fastScalpTpPercent) {
    const reason = `TAKE_PROFIT (FAST_SCALP): Lãi ${pnlPercent.toFixed(2)}% ≥ ${learnedConfig.fastScalpTpPercent}%. Chốt nhanh.`;
    await closePosition(position, currentPrice, reason);
    const pnlUsdt = calcPnlUsdt(position, currentPrice);
    return { type: 'TAKE_PROFIT', reason, pnl: pnlUsdt };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // RULE 2: CHỐT LỜI SWING — lãi > takeProfitPercent
  // ═══════════════════════════════════════════════════════════════════════════
  if (position.duration === 'SWING' && pnlPercent >= learnedConfig.takeProfitPercent) {
    const reason = `TAKE_PROFIT (SWING): Lãi ${pnlPercent.toFixed(2)}% ≥ ${learnedConfig.takeProfitPercent}%. Chốt.`;
    await closePosition(position, currentPrice, reason);
    const pnlUsdt = calcPnlUsdt(position, currentPrice);
    return { type: 'TAKE_PROFIT', reason, pnl: pnlUsdt };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // RULE 3: NHỒI LỆNH DCA — lỗ chạm ngưỡng + 4H vẫn đúng hướng
  // BẮT BUỘC kiểm tra Tầng 1 trước khi DCA. Không DCA vào xe lửa ngược chiều.
  // ═══════════════════════════════════════════════════════════════════════════
  if (pnlPercent <= -learnedConfig.dcaDropPercent) {
    // CHECK 1: Đã DCA tối đa chưa?
    if (position.dcaCount >= learnedConfig.maxDca) {
      return {
        type: 'HOLD',
        reason: `Lỗ ${pnlPercent.toFixed(2)}% nhưng đã DCA tối đa ${learnedConfig.maxDca} lần. Chờ 4H hoặc SL.`,
      };
    }

    // CHECK 2: Tầng 1 (4H) CÒN ĐÚNG xu hướng?
    const tier1Valid = isTier1StillValid(position.side, currentAnalysis, klines4H);
    if (!tier1Valid) {
      return {
        type: 'HOLD',
        reason: `Lỗ ${pnlPercent.toFixed(2)}% nhưng 4H không xác nhận ${position.side}. KHÔNG DCA — chờ SL hoặc hồi.`,
      };
    }

    // DCA: nhồi khối lượng x1.5
    const dcaResult = await executeDca(position, currentPrice);
    return {
      type: 'DCA',
      newQuantity: dcaResult.newTotalQty,
      newAvgPrice: dcaResult.newAvgPrice,
      reason: `DCA #${position.dcaCount + 1}: Lỗ ${pnlPercent.toFixed(2)}%, 4H vẫn ${position.side}. Avg ${position.avgEntryPrice.toFixed(2)} → ${dcaResult.newAvgPrice.toFixed(2)}`,
    };
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // DEFAULT: GIỮ VỊ THẾ
  // ═══════════════════════════════════════════════════════════════════════════
  return {
    type: 'HOLD',
    reason: `Giữ ${position.side} ${position.symbol}. PnL: ${pnlPercent.toFixed(2)}%. DCA: ${position.dcaCount}/${learnedConfig.maxDca}.`,
  };
}

// ─── HELPER: TÍNH PNL % ───────────────────────────────────────────────────────

function calcPnlPercent(position: IActivePosition, currentPrice: number): number {
  const avg = position.avgEntryPrice;
  if (position.side === 'LONG') {
    return ((currentPrice - avg) / avg) * 100;
  } else {
    return ((avg - currentPrice) / avg) * 100;
  }
}

// ─── HELPER: TÍNH PNL USDT (bao gồm slippage + phí) ─────────────────────────

function calcPnlUsdt(position: IActivePosition, exitPrice: number): number {
  const finalExitPrice = position.side === 'LONG'
    ? exitPrice * (1 - SLIPPAGE_AND_FEE)
    : exitPrice * (1 + SLIPPAGE_AND_FEE);
  return position.side === 'LONG'
    ? (finalExitPrice - position.avgEntryPrice) * position.totalQuantity
    : (position.avgEntryPrice - finalExitPrice) * position.totalQuantity;
}

// ─── HELPER: CHECK TẦNG 1 GÃY XU HƯỚNG ──────────────────────────────────────

/**
 * Kiểm tra xem Tầng 1 (4H) đã gãy xu hướng chưa.
 * Ưu tiên dùng klines4H trực tiếp (chính xác nhất).
 * Fallback: dùng currentAnalysis.signal.
 */
function isTier1Broken(
  positionSide: 'LONG' | 'SHORT',
  analysis: AnalysisResult,
  klines4H?: KlineData[],
): boolean {
  // Nếu có raw klines 4H → tính EMA trực tiếp (tránh phụ thuộc vào analysis cũ)
  if (klines4H && klines4H.length >= EMA_PERIOD) {
    const closes = klines4H.map((k) => k.close);
    const emaValues = ema({ period: EMA_PERIOD, values: closes });

    if (emaValues.length > 0) {
      const price = closes[closes.length - 1];
      const ema50 = emaValues[emaValues.length - 1];

      // LONG nhưng giá < EMA50 → gãy
      if (positionSide === 'LONG' && price < ema50) return true;
      // SHORT nhưng giá > EMA50 → gãy
      if (positionSide === 'SHORT' && price > ema50) return true;

      return false;
    }
  }

  // Fallback: dùng signal từ analysis
  if (positionSide === 'LONG' && analysis.signal === 'SHORT') return true;
  if (positionSide === 'SHORT' && analysis.signal === 'LONG') return true;

  return false;
}

/**
 * Kiểm tra Tầng 1 vẫn đúng hướng (cho DCA).
 * Nghiêm ngặt hơn: yêu cầu signal TRÙNG hướng, không chỉ "chưa gãy".
 */
function isTier1StillValid(
  positionSide: 'LONG' | 'SHORT',
  analysis: AnalysisResult,
  klines4H?: KlineData[],
): boolean {
  if (klines4H && klines4H.length >= EMA_PERIOD) {
    const closes = klines4H.map((k) => k.close);
    const emaValues = ema({ period: EMA_PERIOD, values: closes });

    if (emaValues.length > 0) {
      const price = closes[closes.length - 1];
      const ema50 = emaValues[emaValues.length - 1];

      if (positionSide === 'LONG') return price > ema50;
      if (positionSide === 'SHORT') return price < ema50;
    }
  }

  // Fallback: signal phải trùng hướng
  const mappedSignal: Signal = positionSide === 'LONG' ? 'LONG' : 'SHORT';
  return analysis.signal === mappedSignal;
}

// ─── HELPER: ĐÓNG VỊ THẾ ──────────────────────────────────────────────────────

async function closePosition(
  position: IActivePosition,
  exitPrice: number,
  reason: string,
): Promise<void> {
  // Tính giá sau slippage + phí
  const finalExitPrice = position.side === 'LONG'
    ? exitPrice * (1 - SLIPPAGE_AND_FEE)
    : exitPrice * (1 + SLIPPAGE_AND_FEE);

  const pnl = position.side === 'LONG'
    ? (finalExitPrice - position.avgEntryPrice) * position.totalQuantity
    : (position.avgEntryPrice - finalExitPrice) * position.totalQuantity;

  try {
    // 1. Cập nhật ActivePosition → CLOSED
    await ActivePosition.updateOne(
      { _id: position._id },
      {
        $set: {
          status: 'CLOSED',
          exitPrice: finalExitPrice,
          pnl,
          closeReason: reason,
          closedAt: new Date(),
        },
      },
    );

    // 2. Ghi PaperTrade (history record cho feedback loop)
    await PaperTrade.create({
      symbol: position.symbol,
      side: position.side === 'LONG' ? 'BUY' : 'SELL',
      entryPrice: position.entryPrice,
      avgEntryPrice: position.avgEntryPrice,
      exitPrice: finalExitPrice,
      quantity: position.totalQuantity,
      pnl,
      reason,
      duration: position.duration,
      score: position.score,
      dcaCount: position.dcaCount,
      predictedCandles: position.predictedCandles,
      closedAt: new Date(),
    });

    // 3. Cập nhật vốn giả lập (bao gồm phí)
    const { updateCapital } = await import('../services/capitalManager');
    await updateCapital(pnl, position.totalQuantity, position.avgEntryPrice, finalExitPrice, false); // isMaker=false (taker fee)

    const emoji = pnl >= 0 ? '✅' : '❌';
    console.log(
      `[PositionMgr] ${emoji} CLOSED ${position.side} ${position.symbol} | ` +
      `Entry: ${position.avgEntryPrice.toFixed(2)} → Exit: ${finalExitPrice.toFixed(2)} | ` +
      `PnL: ${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)} USDT | ${reason}`,
    );
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`[PositionMgr] Close failed: ${message}`);
  }
}

// ─── HELPER: NHỒI LỆNH DCA ───────────────────────────────────────────────────

async function executeDca(
  position: IActivePosition,
  currentPrice: number,
): Promise<{ newTotalQty: number; newAvgPrice: number }> {
  // DCA quantity = lastEntry quantity * 1.5
  const lastQty = position.dcaEntries.length > 0
    ? position.dcaEntries[position.dcaEntries.length - 1].quantity
    : position.totalQuantity;

  const dcaQty = lastQty * DCA_MULTIPLIER;

  // Giá DCA sau slippage
  const dcaPrice = position.side === 'LONG'
    ? currentPrice * (1 + SLIPPAGE_AND_FEE)
    : currentPrice * (1 - SLIPPAGE_AND_FEE);

  // Tính avg entry mới (weighted average)
  const oldCost = position.avgEntryPrice * position.totalQuantity;
  const dcaCost = dcaPrice * dcaQty;
  const newTotalQty = position.totalQuantity + dcaQty;
  const newAvgPrice = (oldCost + dcaCost) / newTotalQty;

  const newDcaEntry = { price: dcaPrice, quantity: dcaQty, timestamp: new Date() };

  try {
    // Trừ phí vốn cho phần DCA mới
    const { updateCapitalOnOpen } = await import('../services/capitalManager');
    const { capital, fee } = await updateCapitalOnOpen(dcaQty, dcaPrice, false);

    await ActivePosition.updateOne(
      { _id: position._id },
      {
        $set: {
          avgEntryPrice: newAvgPrice,
          totalQuantity: newTotalQty,
        },
        $inc: { dcaCount: 1 },
        $push: { dcaEntries: newDcaEntry },
      },
    );

    console.log(
      `[PositionMgr] 🔄 DCA ${position.side} ${position.symbol} | ` +
      `+${dcaQty.toFixed(4)} @ ${dcaPrice.toFixed(2)} | ` +
      `Avg: ${position.avgEntryPrice.toFixed(2)} → ${newAvgPrice.toFixed(2)} | ` +
      `Total Qty: ${newTotalQty.toFixed(4)} | DCA #${position.dcaCount + 1} | DCA fee: -${fee.toFixed(4)} | Capital: ${capital.toFixed(2)}`,
    );
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`[PositionMgr] DCA failed: ${message}`);
  }

  return { newTotalQty, newAvgPrice };
}

// ─── PUBLIC: MỞ VỊ THẾ MỚI ───────────────────────────────────────────────────

export async function openPosition(
  symbol: string,
  side: 'LONG' | 'SHORT',
  price: number,
  quantity: number,
  analysis: AnalysisResult,
): Promise<IActivePosition | null> {
  // Kiểm tra xem đã có vị thế OPEN cho symbol chưa
  const existing = await ActivePosition.findOne({ symbol, status: 'OPEN' });
  if (existing) {
    console.warn(`[PositionMgr] Đã có vị thế ${existing.side} ${symbol} đang mở. Bỏ qua.`);
    return null;
  }

  const entryPrice = side === 'LONG'
    ? price * (1 + SLIPPAGE_AND_FEE)
    : price * (1 - SLIPPAGE_AND_FEE);

  const predictedCandles = analysis.predictedPath.map((p, i) => ({
    price: p,
    timestamp: Date.now() + (i + 1) * 15 * 60 * 1000, // +15m intervals
  }));

  try {
    const position = await ActivePosition.create({
      symbol,
      side,
      entryPrice,
      avgEntryPrice: entryPrice,
      totalQuantity: quantity,
      dcaEntries: [{ price: entryPrice, quantity, timestamp: new Date() }],
      dcaCount: 0,
      duration: analysis.duration,
      score: analysis.score,
      predictedCandles,
      reason: analysis.reason,
      status: 'OPEN',
    });

    console.log(
      `[PositionMgr] 🟢 OPEN ${side} ${symbol} @ ${entryPrice.toFixed(2)} | ` +
      `Qty: ${quantity} | Score: ${analysis.score} | Duration: ${analysis.duration}`,
    );

    return position;
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`[PositionMgr] Open failed: ${message}`);
    return null;
  }
}

// ─── PUBLIC: LẤY CÁC VỊ THẾ ĐANG MỞ ─────────────────────────────────────────

export async function getOpenPositions(symbol?: string): Promise<IActivePosition[]> {
  const filter: Record<string, unknown> = { status: 'OPEN' };
  if (symbol) filter.symbol = symbol;

  return ActivePosition.find(filter).sort({ createdAt: -1 }).lean() as Promise<IActivePosition[]>;
}
