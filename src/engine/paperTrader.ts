/**
 * @deprecated Legacy paper trader — chỉ dùng trong integrationTest.
 * Production code sử dụng positionManager.openPosition() thay thế.
 */
import { getRealtimePrice } from '../database/redis';
import { PaperTrade } from '../database/models/PaperTrade';

const SLIPPAGE_AND_FEE = 0.0006; // 0.04% taker fee + 0.02% slippage
const DEFAULT_QUANTITY = 0.001;   // default quantity cho paper trade

export async function executeTrade(symbol: string, action: 'BUY' | 'SELL'): Promise<void> {
  // 1. Lấy giá realtime từ Redis
  const currentPrice = await getRealtimePrice(symbol);
  if (currentPrice === null) {
    console.error(`[PaperTrader] No price in Redis for ${symbol}, trade cancelled.`);
    return;
  }

  // 2. Giả lập trượt giá + phí giao dịch
  const finalPrice = action === 'BUY'
    ? currentPrice * (1 + SLIPPAGE_AND_FEE)
    : currentPrice * (1 - SLIPPAGE_AND_FEE);

  // 3. Lưu lệnh giả lập vào MongoDB
  try {
    await PaperTrade.create({
      symbol,
      side: action,
      entryPrice: finalPrice,
      avgEntryPrice: finalPrice,
      exitPrice: 0,
      quantity: DEFAULT_QUANTITY,
      pnl: 0,
      reason: 'WORKER_SIGNAL',
    });
    console.log(`[PaperTrader] ${action} ${symbol} @ ${finalPrice.toFixed(2)} (raw: ${currentPrice.toFixed(2)}, slip: ${(SLIPPAGE_AND_FEE * 100).toFixed(2)}%)`);
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`[PaperTrader] Save failed:`, message);
  }
}
