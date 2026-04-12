/**
 * Quick E2E Test — Chạy tổng quan 1 vòng, mở lệnh thử, chờ 3 phút rồi đóng.
 *
 * Chạy: npx ts-node src/test/quickTradeTest.ts
 */

import dotenv from 'dotenv';
dotenv.config();

import mongoose from 'mongoose';
import { connectMongoDB } from '../database/mongodb';
import redis, { setRealtimePrice, getRealtimePrice } from '../database/redis';
import { getMultiTimeframeKlines, getRadarData, getMoneyFlowData, type WhaleTrade } from '../services/marketDataFetcher';
import { analyze3Tier, type AnalysisResult } from '../analyzer/trendAnalyzer';
import { getLearnedConfig, learnFromHistory } from '../ml/historicalLearner';
import { openPosition, managePosition, getOpenPositions } from '../engine/positionManager';
import type { PositionLearnedConfig } from '../engine/positionManager';

const TEST_SYMBOL = 'BTCUSDT';
const MONITOR_DURATION_MS = 3 * 60 * 1000; // 3 phút
const CHECK_INTERVAL_MS = 15_000;            // Check mỗi 15s

function wait(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function main(): Promise<void> {
  console.log('══════════════════════════════════════════════════════════════');
  console.log('  🧪 QUICK E2E TRADE TEST — Mở lệnh thử & chờ 3 phút');
  console.log('══════════════════════════════════════════════════════════════\n');

  // ═══ 1. KẾT NỐI ═══
  console.log('▸ Phase 1: Kết nối');
  await connectMongoDB();
  console.log('  ✅ MongoDB connected');
  await redis.ping();
  console.log('  ✅ Redis connected');

  // ═══ 2. LẤY DỮ LIỆU THỊ TRƯỜNG ═══
  console.log('\n▸ Phase 2: Lấy dữ liệu thị trường');
  const klines = await getMultiTimeframeKlines(TEST_SYMBOL);
  console.log(`  ✅ Klines: 4H=${klines['4h']?.length ?? 0} 1H=${klines['1h']?.length ?? 0} 15M=${klines['15m']?.length ?? 0}`);

  // ═══ 3. ML CONFIG ═══
  console.log('\n▸ Phase 3: Learned Config');
  const config = await getLearnedConfig(TEST_SYMBOL);
  console.log(`  ✅ RSI [${config.optimalRsi.oversold}/${config.optimalRsi.overbought}] DCA ${config.dcaDropPercent}% TP ${config.takeProfitPercent}%`);

  // ═══ 4. MONEY FLOW ═══
  console.log('\n▸ Phase 4: Money Flow');
  let moneyFlow = null;
  try {
    moneyFlow = await getMoneyFlowData(TEST_SYMBOL);
    console.log(`  ✅ L/S Ratio: ${moneyFlow.longShortRatio.longAccount.toFixed(1)}% Long / ${moneyFlow.longShortRatio.shortAccount.toFixed(1)}% Short`);
    console.log(`  ✅ Funding Rate: ${moneyFlow.fundingRate >= 0 ? '+' : ''}${moneyFlow.fundingRate.toFixed(4)}%`);
    console.log(`  ✅ OI: ${(moneyFlow.openInterest.oi / 1e6).toFixed(1)}M (${moneyFlow.openInterest.oiChangePercent >= 0 ? '+' : ''}${moneyFlow.openInterest.oiChangePercent.toFixed(1)}%)`);
    console.log(`  ✅ Taker B/S: ${moneyFlow.takerBuySellRatio.ratio.toFixed(3)}`);
    if (moneyFlow.topTraderLongShort) {
      console.log(`  ✅ Top Trader L/S Account: ${moneyFlow.topTraderLongShort.longAccount.toFixed(1)}%L / ${moneyFlow.topTraderLongShort.shortAccount.toFixed(1)}%S (ratio: ${moneyFlow.topTraderLongShort.ratio.toFixed(3)})`);
    }
    if (moneyFlow.topTraderPositions) {
      console.log(`  ✅ Top Trader L/S Position: ${moneyFlow.topTraderPositions.longPosition.toFixed(1)}%L / ${moneyFlow.topTraderPositions.shortPosition.toFixed(1)}%S (ratio: ${moneyFlow.topTraderPositions.ratio.toFixed(3)})`);
    }
    if (moneyFlow.orderBook) {
      console.log(`  ✅ Order Book: Bid ${(moneyFlow.orderBook.bidTotal / 1e6).toFixed(2)}M / Ask ${(moneyFlow.orderBook.askTotal / 1e6).toFixed(2)}M (ratio: ${moneyFlow.orderBook.ratio.toFixed(3)}, spread: ${moneyFlow.orderBook.spread.toFixed(4)}%)`);
    }
    if (moneyFlow.tradeCount24h != null) {
      console.log(`  ✅ Trade Count 24h: ${moneyFlow.tradeCount24h.toLocaleString()}`);
    }
    // Push to Redis
    await redis.set(`MONEYFLOW:${TEST_SYMBOL}`, JSON.stringify(moneyFlow), 'EX', 300);
  } catch (err) {
    console.log(`  ⚠️ Money flow error: ${err instanceof Error ? err.message : err}`);
  }

  // ═══ 5. PHÂN TÍCH 3-TIER ═══
  console.log('\n▸ Phase 5: Phân tích 3-Tier');
  const whaleData: WhaleTrade[] = [];
  const analysis = analyze3Tier(
    klines['4h']!, klines['1h']!, klines['15m']!, whaleData,
    { optimalRsi: config.optimalRsi },
    moneyFlow,
  );
  console.log(`  ✅ Signal: ${analysis.signal} | Score: ${analysis.score} | Duration: ${analysis.duration}`);
  console.log(`  ✅ Reason: ${analysis.reason}`);
  if (analysis.predictedPath.length > 0) {
    console.log(`  ✅ Predicted Path: ${analysis.predictedPath.map(p => p.toFixed(2)).join(' → ')}`);
  }

  // ═══ 6. MỞ LỆNH THỬ (BUỘC MỞ bất kể signal) ═══
  console.log('\n▸ Phase 6: Mở lệnh thử (force)');
  const latestPrice = klines['15m']![klines['15m']!.length - 1].close;
  await setRealtimePrice(TEST_SYMBOL, latestPrice);

  // Xóa vị thế test cũ nếu có
  const { ActivePosition } = await import('../database/models/ActivePosition');
  await ActivePosition.deleteMany({ symbol: `TEST_E2E_${TEST_SYMBOL}` });

  const testSymbol = `TEST_E2E_${TEST_SYMBOL}`;
  const side: 'LONG' | 'SHORT' = analysis.signal === 'SHORT' ? 'SHORT' : 'LONG';
  const quantity = 100 / latestPrice; // ~100 USDT

  // Force open: use actual analysis result
  const forceAnalysis: AnalysisResult = {
    signal: side,
    score: Math.max(analysis.score, 50),
    duration: analysis.duration || 'SWING',
    predictedPath: analysis.predictedPath.length > 0
      ? analysis.predictedPath
      : [latestPrice * (side === 'LONG' ? 1.01 : 0.99)],
    reason: `[TEST] Force entry based on analysis: ${analysis.reason}`,
  };

  const position = await openPosition(testSymbol, side, latestPrice, quantity, forceAnalysis);
  if (!position) {
    console.error('  ❌ Không mở được vị thế!');
    await cleanup();
    return;
  }

  console.log(`  ✅ Đã mở lệnh ${side} @ ${latestPrice.toFixed(2)}`);
  console.log(`     Qty: ${quantity.toFixed(6)} | ~100 USDT`);
  console.log(`     Position ID: ${position._id}`);

  // ═══ 7. MONITOR 3 PHÚT ═══
  console.log(`\n▸ Phase 7: Monitoring ${MONITOR_DURATION_MS / 60000} phút...`);
  const startTime = Date.now();
  let checkCount = 0;
  const posConfig: PositionLearnedConfig = {
    dcaDropPercent: config.dcaDropPercent,
    maxDca: config.maxDca,
    takeProfitPercent: config.takeProfitPercent,
    fastScalpTpPercent: config.fastScalpTpPercent,
  };

  while (Date.now() - startTime < MONITOR_DURATION_MS) {
    await wait(CHECK_INTERVAL_MS);
    checkCount++;

    // Fetch latest price
    const freshKlines = await getMultiTimeframeKlines(TEST_SYMBOL);
    const currentPrice = freshKlines['15m']![freshKlines['15m']!.length - 1].close;
    await setRealtimePrice(TEST_SYMBOL, currentPrice);

    // Re-analyze
    const freshAnalysis = analyze3Tier(
      freshKlines['4h']!, freshKlines['1h']!, freshKlines['15m']!, [],
      { optimalRsi: config.optimalRsi },
      moneyFlow,
    );

    // Get fresh position from DB
    const openPos = await getOpenPositions(testSymbol);
    if (openPos.length === 0) {
      console.log(`  [Check ${checkCount}] Vị thế đã đóng tự động!`);
      break;
    }

    const pos = openPos[0];
    const pnlPercent = side === 'LONG'
      ? ((currentPrice - pos.avgEntryPrice) / pos.avgEntryPrice * 100)
      : ((pos.avgEntryPrice - currentPrice) / pos.avgEntryPrice * 100);

    // Check position
    const action = await managePosition(pos, currentPrice, freshAnalysis, posConfig, freshKlines['4h']!);

    const elapsed = ((Date.now() - startTime) / 1000).toFixed(0);
    console.log(
      `  [Check ${checkCount}] ${elapsed}s | Price: ${currentPrice.toFixed(2)} | ` +
      `P&L: ${pnlPercent >= 0 ? '+' : ''}${pnlPercent.toFixed(3)}% | ` +
      `Signal: ${freshAnalysis.signal}(${freshAnalysis.score}) | ` +
      `Action: ${action.type} — ${action.reason}`,
    );

    if (action.type === 'TAKE_PROFIT' || action.type === 'STOP_LOSS') {
      console.log(`  🏁 Vị thế đã đóng: ${action.type}`);
      break;
    }
  }

  // ═══ 8. ĐÓNG LỆNH SAU 3 PHÚT (nếu còn mở) ═══
  console.log('\n▸ Phase 8: Đóng lệnh');
  const finalPos = await getOpenPositions(testSymbol);
  if (finalPos.length > 0) {
    const pos = finalPos[0];
    const freshKlines = await getMultiTimeframeKlines(TEST_SYMBOL);
    const finalPrice = freshKlines['15m']![freshKlines['15m']!.length - 1].close;
    const finalPnl = side === 'LONG'
      ? ((finalPrice - pos.avgEntryPrice) / pos.avgEntryPrice * 100)
      : ((pos.avgEntryPrice - finalPrice) / pos.avgEntryPrice * 100);

    // Force close via managePosition with reversed analysis
    const closeAnalysis: AnalysisResult = {
      signal: side === 'LONG' ? 'SHORT' : 'LONG',
      score: 90,
      duration: 'SWING',
      predictedPath: [finalPrice],
      reason: '[TEST] Force close after 3 minutes',
    };
    const closeAction = await managePosition(pos, finalPrice, closeAnalysis, posConfig, freshKlines['4h']!);
    console.log(`  ✅ Đóng lệnh: ${closeAction.type} — ${closeAction.reason}`);
    console.log(`  📊 Kết quả: ${finalPnl >= 0 ? '+' : ''}${finalPnl.toFixed(4)}% | Entry: ${pos.avgEntryPrice.toFixed(2)} → Exit: ${finalPrice.toFixed(2)}`);
  } else {
    console.log('  ✅ Vị thế đã đóng trước đó');
  }

  // ═══ 9. CLEANUP ═══
  console.log('\n▸ Cleanup');
  await ActivePosition.deleteMany({ symbol: testSymbol });
  console.log('  🧹 Đã xóa vị thế test');

  await cleanup();
}

async function cleanup(): Promise<void> {
  await mongoose.disconnect();
  redis.disconnect();
  console.log('\n══════════════════════════════════════════════════════════════');
  console.log('  ✅ QUICK E2E TEST COMPLETED');
  console.log('══════════════════════════════════════════════════════════════');
  process.exit(0);
}

main().catch((err) => {
  console.error('FATAL:', err);
  process.exit(1);
});
