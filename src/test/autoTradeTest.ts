// Tự động vào lệnh LONG/SHORT cho coin có score dự đoán cao nhất hiện tại
// Có thể xóa file này sau khi test
import { connectMongoDB } from '../database/mongodb';
import { getMultiTimeframeKlines } from '../services/marketDataFetcher';
import { getRadarSymbols } from '../index';
import { analyze3Tier } from '../analyzer/trendAnalyzer';
import { getLearnedConfig } from '../ml/historicalLearner';
import { openPosition } from '../engine/positionManager';

async function testAutoTradeTopCoin() {
  await connectMongoDB();
  const radarCoins = await getRadarSymbols();
  if (!radarCoins || radarCoins.length === 0) {
    console.log('Không có coin nào trong radar.');
    return;
  }

  let best: null | { symbol: string; analysis: any; price: number } = null;
  for (const coin of radarCoins) {
    const symbol = coin.symbol;
    const klines = await getMultiTimeframeKlines(symbol);
    if (!klines || !klines['4h'] || !klines['1h'] || !klines['15m'] || klines['15m'].length === 0) continue;
    const learnedConfig = await getLearnedConfig(symbol);
    const analysis = analyze3Tier(
      klines['4h'],
      klines['1h'],
      klines['15m'],
      [], // whaleData
      learnedConfig,
      null // moneyFlow
    );
    const lastCandle = klines['15m'][klines['15m'].length - 1];
    if (!best || analysis.score > best.analysis.score) {
      best = { symbol, analysis, price: lastCandle.close };
    }
  }

  if (!best) {
    console.log('Không tìm thấy coin nào đủ điều kiện vào lệnh.');
    return;
  }

  const { symbol, analysis, price } = best;
  // Nếu tín hiệu là HOLD thì ép thành LONG để test
  const side = (analysis.signal === 'LONG' || analysis.signal === 'SHORT') ? analysis.signal : 'LONG';
  const quantity = 100 / price; // 100 USDT mỗi lệnh
  const pos = await openPosition(symbol, side, price, quantity, analysis);
  if (pos) {
    console.log(`Đã vào lệnh TEST ${side} ${symbol} @ ${price.toFixed(2)} | Qty: ${quantity.toFixed(6)} | Score: ${analysis.score}`);
  } else {
    console.log('Không thể vào lệnh (có thể đã có lệnh mở hoặc lỗi khác).');
  }
}

// Chạy test
if (require.main === module) {
  testAutoTradeTopCoin().then(() => process.exit(0));
}
