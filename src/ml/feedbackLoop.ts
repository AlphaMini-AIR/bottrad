import { PaperTrade } from '../database/models/PaperTrade';
import { SymbolConfig } from '../database/models/SymbolConfig';

/**
 * Feedback Loop: Đánh giá hiệu suất gần đây cho từng symbol,
 * tự động siết/nới RSI threshold và lưu vào MongoDB (SymbolConfig).
 */
export async function evaluateAndAdjust(): Promise<void> {
  try {
    // 1. Lấy 100 lệnh gần nhất
    const trades = await PaperTrade.find({ exitPrice: { $gt: 0 } })
      .sort({ closedAt: -1 })
      .limit(100)
      .lean();

    if (trades.length < 5) {
      console.log('[Feedback] Chưa đủ lệnh đã đóng (<5), bỏ qua.');
      return;
    }

    // 2. Nhóm theo symbol
    const bySymbol = new Map<string, Array<{ pnl: number }>>();
    for (const t of trades) {
      const sym = t.symbol as string;
      if (!bySymbol.has(sym)) bySymbol.set(sym, []);
      bySymbol.get(sym)!.push({ pnl: t.pnl as number });
    }

    // 3. Điều chỉnh RSI cho từng symbol
    for (const [symbol, symbolTrades] of bySymbol) {
      if (symbolTrades.length < 3) continue;

      const wins = symbolTrades.filter((t) => t.pnl > 0).length;
      const winRate = (wins / symbolTrades.length) * 100;
      const totalPnL = symbolTrades.reduce((sum, t) => sum + t.pnl, 0);

      // Load current config
      const config = await SymbolConfig.findOne({ symbol });
      if (!config) continue;

      const oldOversold = config.optimalRsi?.oversold ?? 30;
      const oldOverbought = config.optimalRsi?.overbought ?? 70;
      let newOversold = oldOversold;
      let newOverbought = oldOverbought;

      if (winRate < 40) {
        // Thua nhiều → siết RSI: entry khắt khe hơn
        newOversold = Math.max(20, oldOversold - 3);
        newOverbought = Math.min(80, oldOverbought + 3);
      } else if (winRate >= 60 && totalPnL > 0) {
        // Thắng tốt → nới RSI: bắt nhiều entry hơn
        newOversold = Math.min(40, oldOversold + 2);
        newOverbought = Math.max(60, oldOverbought - 2);
      }
      // Nếu winRate 40-60: giữ nguyên

      if (newOversold !== oldOversold || newOverbought !== oldOverbought) {
        await SymbolConfig.updateOne(
          { symbol },
          { $set: { 'optimalRsi.oversold': newOversold, 'optimalRsi.overbought': newOverbought } },
        );
        console.log(
          `[Feedback] ${symbol} | WR: ${winRate.toFixed(0)}% | PnL: ${totalPnL.toFixed(2)} | ` +
          `RSI: ${oldOversold}/${oldOverbought} → ${newOversold}/${newOverbought}`,
        );
      } else {
        console.log(
          `[Feedback] ${symbol} | WR: ${winRate.toFixed(0)}% | PnL: ${totalPnL.toFixed(2)} | RSI: giữ nguyên ${oldOversold}/${oldOverbought}`,
        );
      }
    }
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[Feedback] Evaluation error:', message);
  }
}
