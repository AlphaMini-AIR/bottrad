/**
 * Comprehensive Integration Test — Kiểm tra TOÀN BỘ hàm trong hệ thống.
 *
 * Chạy: npx ts-node src/test/integrationTest.ts
 *
 * Test flow:
 *   1. Kết nối MongoDB + Redis
 *   2. getMultiTimeframeKlines (Binance REST API)
 *   3. learnFromHistory + getLearnedConfig (Grid Search + MongoDB)
 *   4. analyze3Tier (pure computation, đủ data)
 *   5. openPosition + managePosition (all actions: HOLD, DCA, TP, SL)
 *   6. getOpenPositions + closePosition flow
 *   7. executeTrade (legacy paperTrader)
 *   8. evaluateAndAdjust (feedbackLoop)
 *   9. getRadarData + monitorWhaleTrades (WebSocket connect & cleanup)
 *   10. startMarketScanner (WebSocket connect & cleanup)
 *   11. Cleanup test data
 */

import { connectMongoDB } from '../database/mongodb';
import redis, { setRealtimePrice, getRealtimePrice } from '../database/redis';
import { getMultiTimeframeKlines, getRadarData, monitorWhaleTrades } from '../services/marketDataFetcher';
import { analyze3Tier, type AnalysisResult } from '../analyzer/trendAnalyzer';
import { learnFromHistory, getLearnedConfig } from '../ml/historicalLearner';
import { openPosition, managePosition, getOpenPositions } from '../engine/positionManager';
import { executeTrade } from '../engine/paperTrader';
import { evaluateAndAdjust } from '../ml/feedbackLoop';
import { startMarketScanner } from '../scanner/marketScanner';
import { ActivePosition } from '../database/models/ActivePosition';
import { PaperTrade } from '../database/models/PaperTrade';
import mongoose from 'mongoose';

// ─── HELPERS ───────────────────────────────────────────────────────────────────

const TEST_SYMBOL = 'BTCUSDT';
const TEST_PREFIX = 'TEST_INT_';
let passed = 0;
let failed = 0;
const testPositionIds: string[] = [];
const testTradeIds: string[] = [];

function ok(name: string, detail?: string): void {
  passed++;
  console.log(`  ✅ ${name}${detail ? ` — ${detail}` : ''}`);
}

function fail(name: string, err: unknown): void {
  failed++;
  const msg = err instanceof Error ? err.message : String(err);
  console.error(`  ❌ ${name} — ${msg}`);
}

function wait(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

// ─── TESTS ─────────────────────────────────────────────────────────────────────

async function testMongoDB(): Promise<void> {
  try {
    await connectMongoDB();
    const state = mongoose.connection.readyState;
    if (state !== 1) throw new Error(`readyState = ${state}, expected 1`);
    ok('MongoDB connect', `readyState = ${state}`);
  } catch (e) { fail('MongoDB connect', e); }
}

async function testRedis(): Promise<void> {
  try {
    const pong = await redis.ping();
    if (pong !== 'PONG') throw new Error(`ping returned "${pong}"`);
    ok('Redis ping', 'PONG');
  } catch (e) { fail('Redis ping', e); }
}

async function testRedisSetGet(): Promise<void> {
  try {
    await setRealtimePrice(`${TEST_PREFIX}BTC`, 99999.99);
    const price = await getRealtimePrice(`${TEST_PREFIX}BTC`);
    if (price !== 99999.99) throw new Error(`Expected 99999.99, got ${price}`);
    ok('Redis set/get price', `${price}`);
    // cleanup
    await redis.del(`price:${TEST_PREFIX}BTC`);
  } catch (e) { fail('Redis set/get price', e); }
}

async function testGetMultiTimeframeKlines(): Promise<Awaited<ReturnType<typeof getMultiTimeframeKlines>> | null> {
  try {
    const data = await getMultiTimeframeKlines(TEST_SYMBOL);
    const c4h = data['4h']?.length ?? 0;
    const c1h = data['1h']?.length ?? 0;
    const c15m = data['15m']?.length ?? 0;
    if (c4h === 0 && c1h === 0 && c15m === 0) throw new Error('All timeframes 0');
    ok('getMultiTimeframeKlines', `4H:${c4h} 1H:${c1h} 15M:${c15m}`);
    return data;
  } catch (e) { fail('getMultiTimeframeKlines', e); return null; }
}

async function testLearnFromHistory(): Promise<void> {
  try {
    const config = await learnFromHistory(TEST_SYMBOL);
    if (!config) throw new Error('learnFromHistory returned null');
    ok(
      'learnFromHistory',
      `RSI [${config.optimalRsi.oversold}/${config.optimalRsi.overbought}] PnL:${config.backtestPnl.toFixed(2)}% Sharpe proxy OK`,
    );
  } catch (e) { fail('learnFromHistory', e); }
}

async function testGetLearnedConfig(): Promise<Awaited<ReturnType<typeof getLearnedConfig>> | null> {
  try {
    const config = await getLearnedConfig(TEST_SYMBOL);
    if (!config.optimalRsi || typeof config.dcaDropPercent !== 'number') {
      throw new Error('Config missing required fields');
    }
    ok(
      'getLearnedConfig',
      `RSI [${config.optimalRsi.oversold}/${config.optimalRsi.overbought}] DCA ${config.dcaDropPercent}%×${config.maxDca} TP:${config.takeProfitPercent}% Scalp:${config.fastScalpTpPercent}%`,
    );
    return config;
  } catch (e) { fail('getLearnedConfig', e); return null; }
}

async function testAnalyze3Tier_WithFullData(): Promise<AnalysisResult | null> {
  try {
    // Fetch đủ 60 nến 4H (>50 cho EMA), 10 nến 1H, 20 nến 15M
    const [k4hRaw, k1hRaw, k15mRaw] = await Promise.all([
      fetchKlinesRaw(TEST_SYMBOL, '4h', 60),
      fetchKlinesRaw(TEST_SYMBOL, '1h', 10),
      fetchKlinesRaw(TEST_SYMBOL, '15m', 20),
    ]);
    if (!k4hRaw.length || !k1hRaw.length || !k15mRaw.length) {
      throw new Error('Fetch klines failed');
    }

    const config = await getLearnedConfig(TEST_SYMBOL);
    const result = analyze3Tier(k4hRaw, k1hRaw, k15mRaw, [], { optimalRsi: config.optimalRsi });

    if (!result.signal || typeof result.score !== 'number') {
      throw new Error('AnalysisResult missing fields');
    }

    ok(
      'analyze3Tier (full data)',
      `Signal:${result.signal} Score:${result.score} Duration:${result.duration} | ${result.reason}`,
    );
    return result;
  } catch (e) { fail('analyze3Tier (full data)', e); return null; }
}

async function testAnalyze3Tier_InsufficientData(): Promise<void> {
  try {
    // Chỉ 5 nến 4H → phải trả HOLD vì thiếu data cho EMA 50
    const fakeKlines = Array.from({ length: 5 }, (_, i) => ({
      openTime: Date.now() - (5 - i) * 14400000,
      open: 100000, high: 100100, low: 99900, close: 100050,
      volume: 1000, closeTime: Date.now() - (5 - i) * 14400000 + 14399999,
      quoteVolume: 100000000, trades: 500,
    }));
    const result = analyze3Tier(fakeKlines, fakeKlines, fakeKlines, [], {
      optimalRsi: { oversold: 30, overbought: 70 },
    });
    if (result.signal !== 'HOLD') throw new Error(`Expected HOLD, got ${result.signal}`);
    ok('analyze3Tier (insufficient data)', `Signal:HOLD — correct`);
  } catch (e) { fail('analyze3Tier (insufficient data)', e); }
}

async function testOpenPosition_And_ManageHOLD(
  analysis: AnalysisResult,
  config: Awaited<ReturnType<typeof getLearnedConfig>>,
): Promise<void> {
  const sym = `${TEST_PREFIX}HOLD`;
  try {
    await ActivePosition.deleteMany({ symbol: sym });
    const pos = await openPosition(sym, 'LONG', 100000, 0.001, analysis);
    if (!pos) throw new Error('openPosition returned null');
    testPositionIds.push(pos._id.toString());
    if (pos.status !== 'OPEN') throw new Error(`status = ${pos.status}`);
    ok('openPosition', `ID:${pos._id} Side:${pos.side} Avg:${pos.avgEntryPrice.toFixed(2)}`);

    // managePosition → HOLD (giá không đổi, PnL ~0%)
    const action = await managePosition(pos, pos.avgEntryPrice, analysis, {
      dcaDropPercent: config.dcaDropPercent,
      maxDca: config.maxDca,
      takeProfitPercent: config.takeProfitPercent,
      fastScalpTpPercent: config.fastScalpTpPercent,
    });
    if (action.type !== 'HOLD') throw new Error(`Expected HOLD, got ${action.type}`);
    ok('managePosition → HOLD', action.reason);
  } catch (e) { fail('openPosition + HOLD', e); }
}

async function testManagePosition_DCA(
  config: Awaited<ReturnType<typeof getLearnedConfig>>,
): Promise<void> {
  const sym = `${TEST_PREFIX}DCA`;
  try {
    await ActivePosition.deleteMany({ symbol: sym });

    const dummyAnalysis: AnalysisResult = {
      signal: 'LONG', score: 70, duration: 'SWING',
      predictedPath: [100000, 100500, 101000], reason: 'Test DCA',
    };
    const pos = await openPosition(sym, 'LONG', 100000, 0.001, dummyAnalysis);
    if (!pos) throw new Error('openPosition null');
    testPositionIds.push(pos._id.toString());

    // Giá giảm dcaDropPercent% → DCA
    const dropPrice = pos.avgEntryPrice * (1 - config.dcaDropPercent / 100 - 0.005);
    const action = await managePosition(pos, dropPrice, dummyAnalysis, {
      dcaDropPercent: config.dcaDropPercent,
      maxDca: config.maxDca,
      takeProfitPercent: config.takeProfitPercent,
      fastScalpTpPercent: config.fastScalpTpPercent,
    });
    if (action.type !== 'DCA') throw new Error(`Expected DCA, got ${action.type}: ${action.reason}`);
    ok('managePosition → DCA', action.reason);
  } catch (e) { fail('managePosition → DCA', e); }
}

async function testManagePosition_TakeProfit(
  config: Awaited<ReturnType<typeof getLearnedConfig>>,
): Promise<void> {
  const sym = `${TEST_PREFIX}TP`;
  try {
    await ActivePosition.deleteMany({ symbol: sym });

    const dummyAnalysis: AnalysisResult = {
      signal: 'LONG', score: 80, duration: 'SWING',
      predictedPath: [100000, 101000, 102000], reason: 'Test TP',
    };
    const pos = await openPosition(sym, 'LONG', 100000, 0.001, dummyAnalysis);
    if (!pos) throw new Error('openPosition null');
    testPositionIds.push(pos._id.toString());

    // Giá tăng takeProfitPercent% → TAKE_PROFIT
    const tpPrice = pos.avgEntryPrice * (1 + config.takeProfitPercent / 100 + 0.005);
    const action = await managePosition(pos, tpPrice, dummyAnalysis, {
      dcaDropPercent: config.dcaDropPercent,
      maxDca: config.maxDca,
      takeProfitPercent: config.takeProfitPercent,
      fastScalpTpPercent: config.fastScalpTpPercent,
    });
    if (action.type !== 'TAKE_PROFIT') throw new Error(`Expected TAKE_PROFIT, got ${action.type}: ${action.reason}`);
    ok('managePosition → TAKE_PROFIT', action.reason);
  } catch (e) { fail('managePosition → TAKE_PROFIT', e); }
}

async function testManagePosition_StopLoss(
  config: Awaited<ReturnType<typeof getLearnedConfig>>,
): Promise<void> {
  const sym = `${TEST_PREFIX}SL`;
  try {
    await ActivePosition.deleteMany({ symbol: sym });

    // Mở LONG nhưng analysis nói SHORT → gãy xu hướng → STOP_LOSS
    const openAnalysis: AnalysisResult = {
      signal: 'LONG', score: 60, duration: 'SWING',
      predictedPath: [100000], reason: 'Test SL',
    };
    const pos = await openPosition(sym, 'LONG', 100000, 0.001, openAnalysis);
    if (!pos) throw new Error('openPosition null');
    testPositionIds.push(pos._id.toString());

    // Analysis nói SHORT (gãy LONG) → STOP_LOSS
    const reversedAnalysis: AnalysisResult = {
      signal: 'SHORT', score: 70, duration: 'SWING',
      predictedPath: [99000], reason: 'Trend reversed',
    };
    const action = await managePosition(pos, 99500, reversedAnalysis, {
      dcaDropPercent: config.dcaDropPercent,
      maxDca: config.maxDca,
      takeProfitPercent: config.takeProfitPercent,
      fastScalpTpPercent: config.fastScalpTpPercent,
    });
    if (action.type !== 'STOP_LOSS') throw new Error(`Expected STOP_LOSS, got ${action.type}: ${action.reason}`);
    ok('managePosition → STOP_LOSS', action.reason);
  } catch (e) { fail('managePosition → STOP_LOSS', e); }
}

async function testManagePosition_FastScalp(
  config: Awaited<ReturnType<typeof getLearnedConfig>>,
): Promise<void> {
  const sym = `${TEST_PREFIX}SCALP`;
  try {
    await ActivePosition.deleteMany({ symbol: sym });

    const dummyAnalysis: AnalysisResult = {
      signal: 'LONG', score: 85, duration: 'FAST_SCALP',
      predictedPath: [100000, 100500], reason: 'Test Fast Scalp',
    };
    const pos = await openPosition(sym, 'LONG', 100000, 0.001, dummyAnalysis);
    if (!pos) throw new Error('openPosition null');
    testPositionIds.push(pos._id.toString());

    // Lãi fastScalpTpPercent% → TAKE_PROFIT (FAST_SCALP)
    const scalpPrice = pos.avgEntryPrice * (1 + config.fastScalpTpPercent / 100 + 0.005);
    const action = await managePosition(pos, scalpPrice, dummyAnalysis, {
      dcaDropPercent: config.dcaDropPercent,
      maxDca: config.maxDca,
      takeProfitPercent: config.takeProfitPercent,
      fastScalpTpPercent: config.fastScalpTpPercent,
    });
    if (action.type !== 'TAKE_PROFIT') throw new Error(`Expected TAKE_PROFIT, got ${action.type}: ${action.reason}`);
    ok('managePosition → FAST_SCALP TP', action.reason);
  } catch (e) { fail('managePosition → FAST_SCALP TP', e); }
}

async function testGetOpenPositions(): Promise<void> {
  try {
    const positions = await getOpenPositions();
    ok('getOpenPositions', `${positions.length} positions found`);
  } catch (e) { fail('getOpenPositions', e); }
}

async function testDuplicatePosition(): Promise<void> {
  const sym = `${TEST_PREFIX}DUP`;
  try {
    await ActivePosition.deleteMany({ symbol: sym });
    const analysis: AnalysisResult = {
      signal: 'LONG', score: 50, duration: 'SWING',
      predictedPath: [100000], reason: 'Test dup',
    };
    const pos1 = await openPosition(sym, 'LONG', 100000, 0.001, analysis);
    if (!pos1) throw new Error('First open failed');
    testPositionIds.push(pos1._id.toString());

    // Mở lần 2 → phải trả null (đã có vị thế OPEN)
    const pos2 = await openPosition(sym, 'LONG', 100000, 0.001, analysis);
    if (pos2 !== null) {
      testPositionIds.push(pos2._id.toString());
      throw new Error('Second open should return null (duplicate)');
    }
    ok('openPosition duplicate guard', 'Correctly rejected duplicate');
  } catch (e) { fail('openPosition duplicate guard', e); }
}

async function testExecuteTrade(): Promise<void> {
  try {
    // Set price in Redis first
    await setRealtimePrice(`${TEST_PREFIX}TRADE`, 50000);

    await executeTrade(`${TEST_PREFIX}TRADE`, 'BUY');

    // Verify PaperTrade was created
    const trade = await PaperTrade.findOne({ symbol: `${TEST_PREFIX}TRADE` }).lean();
    if (!trade) throw new Error('PaperTrade not created');
    if (!trade.avgEntryPrice) throw new Error(`avgEntryPrice missing: ${trade.avgEntryPrice}`);
    if (!trade.quantity) throw new Error(`quantity missing: ${trade.quantity}`);
    testTradeIds.push(trade._id.toString());

    ok('executeTrade (paperTrader)', `Side:${trade.side} Avg:${(trade.avgEntryPrice as number).toFixed(2)} Qty:${trade.quantity}`);
  } catch (e) { fail('executeTrade (paperTrader)', e); }
}

async function testEvaluateAndAdjust(): Promise<void> {
  try {
    await evaluateAndAdjust();
    ok('evaluateAndAdjust (feedbackLoop)', 'Ran without errors');
  } catch (e) { fail('evaluateAndAdjust (feedbackLoop)', e); }
}

async function testGetRadarData(): Promise<void> {
  try {
    const cleanup = getRadarData();
    await wait(3000);
    cleanup();
    ok('getRadarData', 'WebSocket connected & cleaned up');
  } catch (e) { fail('getRadarData', e); }
}

async function testMonitorWhaleTrades(): Promise<void> {
  try {
    const cleanup = monitorWhaleTrades([TEST_SYMBOL]);
    await wait(3000);
    cleanup();
    ok('monitorWhaleTrades', 'WebSocket connected & cleaned up');
  } catch (e) { fail('monitorWhaleTrades', e); }
}

async function testStartMarketScanner(): Promise<void> {
  try {
    const cleanup = startMarketScanner();
    await wait(3000);
    cleanup();
    ok('startMarketScanner', 'WebSocket connected & cleaned up');
  } catch (e) { fail('startMarketScanner', e); }
}

// ─── HELPER: fetch klines trực tiếp (cho analyze3Tier test) ───────────────────

type KlineRaw = [number, string, string, string, string, string, number, string, number, string, string, string];

async function fetchKlinesRaw(symbol: string, interval: string, limit: number) {
  const url = `https://fapi.binance.com/fapi/v1/klines?symbol=${encodeURIComponent(symbol)}&interval=${encodeURIComponent(interval)}&limit=${limit}`;
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10_000);
  try {
    const res = await fetch(url, { signal: controller.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const raw = (await res.json()) as KlineRaw[];
    return raw.map((r) => ({
      openTime: r[0],
      open: parseFloat(r[1]),
      high: parseFloat(r[2]),
      low: parseFloat(r[3]),
      close: parseFloat(r[4]),
      volume: parseFloat(r[5]),
      closeTime: r[6],
      quoteVolume: parseFloat(r[7]),
      trades: r[8],
    }));
  } finally {
    clearTimeout(timeout);
  }
}

// ─── CLEANUP ───────────────────────────────────────────────────────────────────

async function cleanup(): Promise<void> {
  try {
    // Xóa test positions
    const testSymbols = [
      `${TEST_PREFIX}HOLD`, `${TEST_PREFIX}DCA`, `${TEST_PREFIX}TP`,
      `${TEST_PREFIX}SL`, `${TEST_PREFIX}SCALP`, `${TEST_PREFIX}DUP`,
    ];
    const delPos = await ActivePosition.deleteMany({ symbol: { $in: testSymbols } });
    console.log(`  🧹 Cleaned ${delPos.deletedCount} test position(s)`);

    // Xóa test trades
    const delTrades = await PaperTrade.deleteMany({ symbol: `${TEST_PREFIX}TRADE` });
    console.log(`  🧹 Cleaned ${delTrades.deletedCount} test trade(s)`);

    // Xóa Redis test keys
    await redis.del(`price:${TEST_PREFIX}BTC`, `price:${TEST_PREFIX}TRADE`);
  } catch (e) {
    console.error('  ⚠️ Cleanup error:', e instanceof Error ? e.message : e);
  }
}

// ─── MAIN ──────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log('═══════════════════════════════════════════════════════════════');
  console.log('  COMPREHENSIVE INTEGRATION TEST — Trading Bot System');
  console.log('═══════════════════════════════════════════════════════════════\n');

  // ── Phase 1: Connections ──
  console.log('▸ Phase 1: Database Connections');
  await testMongoDB();
  await testRedis();
  await testRedisSetGet();

  // ── Phase 2: Market Data ──
  console.log('\n▸ Phase 2: Market Data (Binance REST API)');
  const klines = await testGetMultiTimeframeKlines();

  // ── Phase 3: ML Grid Search + Config ──
  console.log('\n▸ Phase 3: ML — learnFromHistory + getLearnedConfig');
  await testLearnFromHistory();
  const config = await testGetLearnedConfig();

  // ── Phase 4: 3-Tier Analysis ──
  console.log('\n▸ Phase 4: 3-Tier Analysis (full data + edge case)');
  const analysis = await testAnalyze3Tier_WithFullData();
  await testAnalyze3Tier_InsufficientData();

  // ── Phase 5: Position Management — All Actions ──
  console.log('\n▸ Phase 5: Position Management (HOLD, DCA, TP, SL, FAST_SCALP)');
  if (config) {
    const testAnalysis = analysis || {
      signal: 'LONG' as const, score: 60, duration: 'SWING' as const,
      predictedPath: [100000, 100500, 101000], reason: 'Fallback test',
    };
    await testOpenPosition_And_ManageHOLD(testAnalysis, config);
    await testManagePosition_DCA(config);
    await testManagePosition_TakeProfit(config);
    await testManagePosition_StopLoss(config);
    await testManagePosition_FastScalp(config);
    await testDuplicatePosition();
    await testGetOpenPositions();
  } else {
    fail('Phase 5 skipped', 'No learned config');
  }

  // ── Phase 6: Legacy Paper Trader ──
  console.log('\n▸ Phase 6: Legacy Paper Trader + Feedback Loop');
  await testExecuteTrade();
  await testEvaluateAndAdjust();

  // ── Phase 7: WebSocket Services (3s each) ──
  console.log('\n▸ Phase 7: WebSocket Services (3s per test)');
  await testGetRadarData();
  await testMonitorWhaleTrades();
  await testStartMarketScanner();

  // ── Cleanup ──
  console.log('\n▸ Cleanup');
  await cleanup();

  // ── Summary ──
  console.log('\n═══════════════════════════════════════════════════════════════');
  console.log(`  RESULTS: ${passed} passed, ${failed} failed, ${passed + failed} total`);
  if (failed === 0) {
    console.log('  ALL TESTS PASSED');
  } else {
    console.log(`  ${failed} TEST(S) FAILED — see details above`);
  }
  console.log('═══════════════════════════════════════════════════════════════');

  // Disconnect
  await mongoose.disconnect();
  redis.disconnect();

  process.exit(failed > 0 ? 1 : 0);
}

if (require.main === module) {
  main().catch((err) => {
    console.error('FATAL:', err);
    process.exit(1);
  });
}
