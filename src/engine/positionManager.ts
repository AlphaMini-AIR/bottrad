// src/engine/positionManager.ts
import mongoose from 'mongoose';
import { AnalysisResult } from '../analyzer/trendAnalyzer';

// ─── INTERFACES ────────────────────────────────────────────────────────────────

export interface PositionLearnedConfig {
  dcaDropPercent: number;
  maxDca: number;
  takeProfitPercent: number;
  fastScalpTpPercent: number;
}

export interface IActivePosition {
  symbol: string;
  side: 'LONG' | 'SHORT';
  entryPrice: number;
  quantity: number;
  leverage: number;
  tpPrice: number;
  slPrice: number;
  trailingStop: number;
  status: 'OPEN' | 'CLOSED';
  openedAt: Date;
}

// ─── DATABASE MODEL (Giả định Hưng đã có hoặc cần tạo) ────────────────────────

const PositionSchema = new mongoose.Schema({
  symbol: { type: String, required: true },
  side: { type: String, enum: ['LONG', 'SHORT'] },
  entryPrice: Number,
  quantity: Number,
  tpPrice: Number,
  slPrice: Number,
  trailingStop: Number,
  status: { type: String, default: 'OPEN' },
  openedAt: { type: Date, default: Date.now }
});

const ActivePosition = mongoose.models.ActivePosition || mongoose.model('ActivePosition', PositionSchema);

// ─── CORE LOGIC ────────────────────────────────────────────────────────────────

/**
 * 1. TÍNH TOÁN KHỐI LƯỢNG MUA DỰ TRÊN RỦI RO (Risk-Based Sizing)
 * Công thức: Quantity = (Vốn * % Rủi ro) / Khoảng cách Stop Loss
 */
export function calculateQuantity(
  balance: number,
  entryPrice: number,
  slPrice: number,
  riskPercent = 0.01 // Mặc định rủi ro 1% vốn ($1 trên $100)
): number {
  const riskAmount = balance * riskPercent;
  const slDistance = Math.abs(entryPrice - slPrice);

  if (slDistance === 0) return (balance * 0.1) / entryPrice; // Bảo vệ chia cho 0

  let quantity = riskAmount / slDistance;

  // Giới hạn an toàn: Không bao giờ đi quá 10% vốn thực tế (tránh cháy nếu flash crash)
  const maxNotional = balance * 0.1;
  if ((quantity * entryPrice) > maxNotional) {
    quantity = maxNotional / entryPrice;
  }

  return quantity;
}

/**
 * 2. MỞ VỊ THẾ (OPEN POSITION)
 */
export async function openPosition(
  symbol: string,
  side: 'LONG' | 'SHORT',
  price: number,
  balance: number,
  analysis: AnalysisResult
): Promise<IActivePosition | null> {
  try {
    // Sử dụng TP/SL đã tính toán từ Analyzer (RR 2:1)
    const slPrice = analysis.slPrice || (side === 'LONG' ? price * 0.98 : price * 1.02);
    const tpPrice = analysis.tpPrice || (side === 'LONG' ? price * 1.04 : price * 0.96);

    // Tính toán số lượng coin cần mua dựa trên rủi ro
    const quantity = calculateQuantity(balance, price, slPrice);

    const newPos = new ActivePosition({
      symbol,
      side,
      entryPrice: price,
      quantity,
      tpPrice,
      slPrice,
      trailingStop: slPrice, // Khởi tạo Trailing Stop tại điểm SL ban đầu
      status: 'OPEN'
    });

    await newPos.save();
    console.log(`[Position] 🚀 ĐÃ MỞ ${side} ${symbol} | Qty: ${quantity.toFixed(4)} | SL: ${slPrice}`);

    // Ở đây Hưng thêm code gọi API Binance (fapi.buy/fapi.sell)
    // return await binance.futuresOrder(side, symbol, quantity, price, ...);

    return newPos.toObject();
  } catch (err) {
    console.error(`[Position] Lỗi mở lệnh ${symbol}:`, err);
    return null;
  }
}

/**
 * 3. QUẢN LÝ VỊ THẾ ĐANG CHẠY (MANAGE POSITION)
 * Tích hợp Trailing Stop & Break-even logic
 */
export async function managePosition(
  pos: any,
  currentPrice: number,
  analysis: AnalysisResult,
  config: PositionLearnedConfig,
  klines4H: any[]
): Promise<{ type: string; pnl: number; reason: string }> {

  const side = pos.side === 'LONG' ? 1 : -1;
  const pnlPct = ((currentPrice - pos.entryPrice) / pos.entryPrice) * side;

  // 🟢 LOGIC A: TRAILING STOP (Dời SL lên khi có lãi)
  // Nếu giá đi được 1.5% lãi, chúng ta bắt đầu dời SL để "khóa" lợi nhuận
  if (pnlPct > 0.015) {
    const newTrailing = pos.side === 'LONG'
      ? currentPrice * 0.99  // Long: SL cách giá hiện tại 1% phía dưới
      : currentPrice * 1.01; // Short: SL cách giá hiện tại 1% phía trên

    if ((pos.side === 'LONG' && newTrailing > pos.trailingStop) ||
      (pos.side === 'SHORT' && newTrailing < pos.trailingStop)) {
      await ActivePosition.updateOne({ _id: pos._id }, { trailingStop: newTrailing });
      pos.trailingStop = newTrailing;
    }
  }

  // 🔴 LOGIC B: CHẠM SL HOẶC TRAILING STOP
  const hitSL = (pos.side === 'LONG' && currentPrice <= pos.trailingStop) ||
    (pos.side === 'SHORT' && currentPrice >= pos.trailingStop);

  if (hitSL) {
    await closePosition(pos._id, currentPrice, 'STOP_LOSS');
    return { type: 'STOP_LOSS', pnl: pnlPct, reason: 'Chạm Trailing/StopLoss' };
  }

  // 🟡 LOGIC C: CHẠM TAKE PROFIT (Nếu không muốn dùng Trailing hoàn toàn)
  const hitTP = (pos.side === 'LONG' && currentPrice >= pos.tpPrice) ||
    (pos.side === 'SHORT' && currentPrice <= pos.tpPrice);

  if (hitTP) {
    await closePosition(pos._id, currentPrice, 'TAKE_PROFIT');
    return { type: 'TAKE_PROFIT', pnl: pnlPct, reason: 'Chạm TP RR 2:1' };
  }

  return { type: 'HOLD', pnl: pnlPct, reason: 'Tiếp tục theo dõi' };
}

/**
 * 4. ĐÓNG VỊ THẾ
 */
export async function closePosition(id: string, price: number, reason: string) {
  await ActivePosition.updateOne({ _id: id }, { status: 'CLOSED', exitPrice: price, closedAt: new Date() });
  console.log(`[Position] 🔒 ĐÃ ĐÓNG LỆNH | Lý do: ${reason} | Giá: ${price}`);
}

/**
 * 5. LẤY DANH SÁCH VỊ THẾ ĐANG MỞ
 */
export async function getOpenPositions(symbol?: string): Promise<any[]> {
  const query: any = { status: 'OPEN' };
  if (symbol) query.symbol = symbol;
  return await ActivePosition.find(query).lean();
}