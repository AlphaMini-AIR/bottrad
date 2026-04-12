// src/index.ts
import mongoose from 'mongoose';
import redis, { setRealtimePrice } from './database/redis';
// 🟢 Đã bỏ getWhaleData bị lỗi import
import { getMultiTimeframeKlines, getMoneyFlowData } from './services/marketDataFetcher';
import { analyze3Tier } from './analyzer/trendAnalyzer';
import { getLearnedConfig } from './ml/historicalLearner';
import { openPosition, managePosition, getOpenPositions } from './engine/positionManager';
import { circuitBreaker, runWithConcurrency } from './engine/riskManager';
import { VerifiedSymbol } from './database/models/VerifiedSymbol';
import { startFastScalper, updateTrendCache } from './engine/fastScalper';

const MONGODB_URI = "mongodb+srv://assistantsupdev_db_user:rCp0BrUushhwIKR8@binance.bjmukc0.mongodb.net/?appName=Binance";
const ANALYSIS_INTERVAL_MS = 2 * 60 * 1000;
const VIRTUAL_BALANCE = 100;

async function analyzeAndExecute(symbol: string) {
  try {
    const vSymbol = await VerifiedSymbol.findOne({ symbol, status: 'ACTIVE' });
    if (!vSymbol) return;

    const [klines, learnedConfig] = await Promise.all([
      getMultiTimeframeKlines(symbol),
      getLearnedConfig(symbol)
    ]);

    // 🟢 FIX LỖI ẢNH 2: Kiểm tra klines có tồn tại và đầy đủ các khung giờ không
    if (!klines || !klines['4h'] || !klines['1h'] || !klines['15m']) {
      console.log(`[Main] ⚠️ Bỏ qua ${symbol}: Dữ liệu nến bị thiếu hoặc null.`);
      return;
    }

    // 🟢 Lúc này TypeScript đã hiểu klines chắc chắn có dữ liệu -> Hết lỗi đỏ!
    const analysis = analyze3Tier(
      klines['4h'],
      klines['1h'],
      klines['15m'],
      [], // Whale data truyền mảng rỗng để fix lỗi import
      learnedConfig
    );

    const latestPrice = klines['15m'][klines['15m'].length - 1].close;
    updateTrendCache(symbol, analysis.signal);
    await setRealtimePrice(symbol, latestPrice);

    const openPos = await getOpenPositions(symbol);
    if (openPos.length > 0) {
      await managePosition(openPos[0], latestPrice, analysis, learnedConfig as any, klines['4h']);
    }
    else if (analysis.signal !== 'HOLD' && analysis.score >= 80) {
      if (circuitBreaker.canTrade().allowed) {
        await openPosition(
          symbol,
          analysis.signal,
          latestPrice,
          VIRTUAL_BALANCE,
          analysis
        );
      }
    }
  } catch (err) {
    console.error(`[Main] Lỗi nghiêm trọng tại ${symbol}:`, err);
  }
}

// Các hàm runAnalysisCycle và main giữ nguyên như bản trước...
async function runAnalysisCycle() {
  const activeCoins = await VerifiedSymbol.find({ status: 'ACTIVE' }).lean();
  if (activeCoins.length === 0) return;
  const symbols = activeCoins.map(c => c.symbol);
  await runWithConcurrency(symbols.map(sym => () => analyzeAndExecute(sym)), 5);
}

async function main() {
  await mongoose.connect(MONGODB_URI);
  console.log('🚀 SNIPER BOT v3 ONLINE - FIXED TYPES');
  startFastScalper();
  setInterval(runAnalysisCycle, ANALYSIS_INTERVAL_MS);
  runAnalysisCycle();
  process.on('SIGINT', async () => {
    await mongoose.disconnect();
    process.exit(0);
  });
}

main().catch(console.error);