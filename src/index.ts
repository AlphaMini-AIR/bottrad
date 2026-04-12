import dotenv from 'dotenv';
dotenv.config();

import path from 'path';
import mongoose from 'mongoose';
import { connectMongoDB } from './database/mongodb';
import redis, { setRealtimePrice, getRealtimePrice } from './database/redis';
import { getMultiTimeframeKlines, getRadarData, monitorWhaleTrades, getMoneyFlowData } from './services/marketDataFetcher';
import { analyze3Tier, type AnalysisResult, type Signal } from './analyzer/trendAnalyzer';
import { getLearnedConfig, learnFromHistory } from './ml/historicalLearner';
import { evaluateAndAdjust } from './ml/feedbackLoop';
import { openPosition, managePosition, getOpenPositions, type PositionLearnedConfig } from './engine/positionManager';
import { circuitBreaker, apiRateLimiter, runWithConcurrency, type CircuitBreakerStatus } from './engine/riskManager';
import { SymbolConfig } from './database/models/SymbolConfig';
import type { WhaleTrade } from './services/marketDataFetcher';

// ─── CONFIG ────────────────────────────────────────────────────────────────────

const ANALYSIS_INTERVAL_MS = 5 * 60 * 1000;       // 5 phút
const LEARNER_CRON_HOUR = 0;                       // 00:00 mỗi ngày
const POSITION_SIZE_USDT = 100;                    // Mỗi vị thế giả lập 100 USDT
const MIN_SCORE_TO_OPEN = 35;                      // Score tối thiểu để mở lệnh (giảm từ 50 để bot học nhanh hơn)
const REDIS_RADAR_KEY = 'RADAR:HOT_COINS';
const REDIS_ANALYSIS_KEY = 'ANALYSIS:LATEST';      // Cho Frontend
const REDIS_POSITIONS_KEY = 'POSITIONS:OPEN';       // Cho Frontend
const REDIS_DASHBOARD_KEY = 'DASHBOARD:SUMMARY';    // Cho Frontend

// ─── STABLE COINS — Luôn phân tích, đòn bẩy cao, dễ dự đoán ─────────────────
const STABLE_COINS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'PAXGUSDT', 'XAUTUSDT'];

// ─── CLEANUP REGISTRY ──────────────────────────────────────────────────────────

const cleanups: Array<() => void> = [];
let analysisTimer: ReturnType<typeof setInterval> | null = null;
let learnerTimer: ReturnType<typeof setTimeout> | null = null;
let heartbeatTimer: ReturnType<typeof setInterval> | null = null;
let dashboardTimer: ReturnType<typeof setInterval> | null = null;

// ─── HELPER: LẤY WHALE DATA TỪ REDIS ──────────────────────────────────────────

async function getWhaleDataFromRedis(symbol: string): Promise<WhaleTrade[]> {
  try {
    const raw = await redis.lrange(`WHALE:${symbol}`, 0, 49);
    return raw.map((s) => JSON.parse(s) as WhaleTrade);
  } catch {
    return [];
  }
}

// ─── HELPER: LẤY DANH SÁCH RADAR TỪ REDIS ────────────────────────────────────

interface RadarCoin {
  symbol: string;
  price: number;
  quoteVolume: number;
}

export async function getRadarSymbols(): Promise<RadarCoin[]> {
  try {
    const raw = await redis.get(REDIS_RADAR_KEY);
    if (!raw) return [];
    return JSON.parse(raw) as RadarCoin[];
  } catch {
    return [];
  }
}

// ─── CORE: PHÂN TÍCH + GIAO DỊCH CHO 1 SYMBOL ────────────────────────────────

interface SymbolAnalysisReport {
  symbol: string;
  analysis: AnalysisResult;
  action: string;       // 'NEW_POSITION' | 'MANAGED:HOLD' | 'MANAGED:DCA' | 'MANAGED:TP' | 'MANAGED:SL' | 'NO_ACTION' | 'ERROR'
  detail: string;
  timestamp: number;
}

async function analyzeAndExecute(symbol: string): Promise<SymbolAnalysisReport> {
  const report: SymbolAnalysisReport = {
    symbol,
    analysis: { signal: 'HOLD', score: 0, duration: 'SWING', predictedPath: [], reason: '' },
    action: 'NO_ACTION',
    detail: '',
    timestamp: Date.now(),
  };

  try {
    // 1. Fetch klines đa khung thời gian
    const klines = await getMultiTimeframeKlines(symbol);

    if (!klines['4h'] || !klines['1h'] || !klines['15m']) {
      report.action = 'ERROR';
      report.detail = `Missing kline data: 4H=${!!klines['4h']} 1H=${!!klines['1h']} 15M=${!!klines['15m']}`;
      return report;
    }

    // 2. Load learned config
    const learnedConfig = await getLearnedConfig(symbol);

    // 3. Lấy whale data từ Redis
    const whaleData = await getWhaleDataFromRedis(symbol);

    // 3.5. Fetch dòng tiền thông minh
    let moneyFlow = null;
    try {
      moneyFlow = await getMoneyFlowData(symbol);
      // Push money flow vào Redis cho frontend
      await redis.set(`MONEYFLOW:${symbol}`, JSON.stringify(moneyFlow), 'EX', 300);
    } catch {
      // Không block phân tích nếu money flow lỗi
    }

    // 4. Chạy analyze3Tier
    const analysis = analyze3Tier(
      klines['4h'],
      klines['1h'],
      klines['15m'],
      whaleData,
      { optimalRsi: learnedConfig.optimalRsi },
      moneyFlow,
    );

    report.analysis = analysis;

    // 5. Lưu giá realtime
    const latestPrice = klines['15m'][klines['15m'].length - 1].close;
    await setRealtimePrice(symbol, latestPrice);

    // 6. Kiểm tra vị thế đang mở
    const openPositions = await getOpenPositions(symbol);
    const posConfig: PositionLearnedConfig = {
      dcaDropPercent: learnedConfig.dcaDropPercent,
      maxDca: learnedConfig.maxDca,
      takeProfitPercent: learnedConfig.takeProfitPercent,
      fastScalpTpPercent: learnedConfig.fastScalpTpPercent,
    };

    if (openPositions.length > 0) {
      // ═══ QUẢN LÝ VỊ THẾ ĐANG MỞ ═══
      const pos = openPositions[0];
      const action = await managePosition(pos, latestPrice, analysis, posConfig, klines['4h']);
      report.action = `MANAGED:${action.type}`;
      report.detail = action.reason;

      // Ghi nhận kết quả cho circuit breaker
      if (action.type === 'TAKE_PROFIT' || action.type === 'STOP_LOSS') {
        circuitBreaker.recordTrade(action.pnl);
      }
    } else if (analysis.signal !== 'HOLD' && analysis.score >= MIN_SCORE_TO_OPEN) {
      // ═══ KIỂM TRA CIRCUIT BREAKER TRƯỚC KHI MỞ LỆNH ═══
      const cbCheck = circuitBreaker.canTrade();
      if (!cbCheck.allowed) {
        report.action = 'NO_ACTION';
        report.detail = `🛑 Circuit Breaker: ${cbCheck.reason}`;
        return report;
      }

      // ═══ KIỂM TRA RATE LIMIT ═══
      if (apiRateLimiter.getUsagePercent() > 85) {
        report.action = 'NO_ACTION';
        report.detail = `⚠️ Rate limit cao (${apiRateLimiter.getUsagePercent().toFixed(0)}%), tạm dừng mở lệnh`;
        return report;
      }

      // ═══ MỞ VỊ THẾ MỚI ═══
      const maxPositionUsdt = Math.min(POSITION_SIZE_USDT, 1000 * 0.02); // Max 2% of $1000 capital
      const quantity = maxPositionUsdt / latestPrice;
      const side: 'LONG' | 'SHORT' = analysis.signal;
      const position = await openPosition(symbol, side, latestPrice, quantity, analysis);

      if (position) {
        report.action = 'NEW_POSITION';
        report.detail = `${side} @ ${latestPrice.toFixed(2)} | Qty: ${quantity.toFixed(6)} | Score: ${analysis.score}`;
      } else {
        report.action = 'NO_ACTION';
        report.detail = 'openPosition returned null (duplicate check)';
      }
    } else {
      report.action = 'NO_ACTION';
      report.detail = `Signal: ${analysis.signal}, Score: ${analysis.score} (min: ${MIN_SCORE_TO_OPEN})`;
    }

    return report;
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    report.action = 'ERROR';
    report.detail = msg;
    return report;
  }
}

// ─── CORE: CHU KỲ PHÂN TÍCH 5 PHÚT ───────────────────────────────────────────

async function runAnalysisCycle(): Promise<void> {
  const cycleStart = Date.now();
  console.log('\n══════════════════════════════════════════════════════════════');
  console.log(`[Cycle] Analysis cycle started at ${new Date().toISOString()}`);
  console.log('══════════════════════════════════════════════════════════════');

  // 1. Lấy danh sách Radar
  const radarCoins = await getRadarSymbols();
  if (radarCoins.length === 0) {
    console.warn('[Cycle] Radar empty — waiting for data...');
    return;
  }

  // ═══ MERGE STABLE COINS (luôn phân tích BTC, ETH, BNB, SOL, XRP) ═══
  const radarSymbols = new Set(radarCoins.map((c) => c.symbol));
  const missingStable = STABLE_COINS.filter((s) => !radarSymbols.has(s));
  const symbols = [...radarCoins.map((c) => c.symbol), ...missingStable];
  console.log(`[Cycle] Analyzing ${symbols.length} symbols (${missingStable.length} stable added): ${symbols.join(', ')}`);

  // 2. Chạy phân tích với concurrency control (5 symbols đồng thời, tránh burst API)
  const results = await runWithConcurrency(
    symbols.map((sym) => () => analyzeAndExecute(sym)),
    5,
  );

  // 3. Tổng hợp reports
  const reports: SymbolAnalysisReport[] = [];
  for (let i = 0; i < results.length; i++) {
    const r = results[i];
    if (r.status === 'fulfilled') {
      reports.push(r.value);
      const { symbol, analysis, action, detail } = r.value;
      const emoji = action.startsWith('MANAGED:TAKE_PROFIT') ? '💰'
        : action.startsWith('MANAGED:STOP_LOSS') ? '🛑'
        : action.startsWith('MANAGED:DCA') ? '🔄'
        : action === 'NEW_POSITION' ? '🟢'
        : action === 'ERROR' ? '❌'
        : '⏳';
      console.log(
        `  ${emoji} ${symbol} | ${analysis.signal} (${analysis.score}) ${analysis.duration} | ${action} | ${detail}`,
      );
    } else {
      console.error(`  ❌ ${symbols[i]} | Unhandled error: ${r.reason}`);
      reports.push({
        symbol: symbols[i],
        analysis: { signal: 'HOLD', score: 0, duration: 'SWING', predictedPath: [], reason: '' },
        action: 'ERROR',
        detail: String(r.reason),
        timestamp: Date.now(),
      });
    }
  }

  // 4. Push reports vào Redis cho Frontend
  await pushAnalysisToRedis(reports, radarCoins);

  const elapsed = ((Date.now() - cycleStart) / 1000).toFixed(1);
  console.log(`[Cycle] Done in ${elapsed}s. Next cycle in ${ANALYSIS_INTERVAL_MS / 60000} min.\n`);
}

// ─── REDIS: PUSH DATA CHO FRONTEND ────────────────────────────────────────────

async function pushAnalysisToRedis(
  reports: SymbolAnalysisReport[],
  radarCoins: RadarCoin[],
): Promise<void> {
  try {
    const pipeline = redis.pipeline();

    // 1. ANALYSIS:LATEST — mảng phân tích mới nhất cho tất cả coins
    pipeline.set(REDIS_ANALYSIS_KEY, JSON.stringify(reports), 'EX', 600); // TTL 10 phút

    // 2. ANALYSIS:<SYMBOL> — phân tích riêng từng coin (cho detail page)
    for (const r of reports) {
      pipeline.set(`ANALYSIS:${r.symbol}`, JSON.stringify(r), 'EX', 600);
    }

    // 3. POSITIONS:OPEN — danh sách vị thế đang mở
    const openPositions = await getOpenPositions();
    const positionsData = openPositions.map((p) => ({
      id: p._id,
      symbol: p.symbol,
      side: p.side,
      entryPrice: p.entryPrice,
      avgEntryPrice: p.avgEntryPrice,
      totalQuantity: p.totalQuantity,
      dcaCount: p.dcaCount,
      duration: p.duration,
      score: p.score,
      predictedCandles: p.predictedCandles,
      reason: p.reason,
      status: p.status,
      createdAt: p.createdAt,
    }));
    pipeline.set(REDIS_POSITIONS_KEY, JSON.stringify(positionsData), 'EX', 600);

    // 4. CONFIGS:LEARNED — ML learned parameters cho tất cả coins
    const symbolConfigs = await SymbolConfig.find({}).lean();
    const configsData = symbolConfigs.map((c) => ({
      symbol: c.symbol,
      optimalRsi: c.optimalRsi,
      dcaDropPercent: c.dcaDropPercent,
      maxDca: c.maxDca,
      takeProfitPercent: c.takeProfitPercent,
      fastScalpTpPercent: c.fastScalpTpPercent,
      backtestPnl: c.backtestPnl,
      backtestDrawdown: c.backtestDrawdown,
      backtestTrades: c.backtestTrades,
      backtestWinRate: c.backtestWinRate,
      lastLearned: c.lastLearned,
    }));
    pipeline.set('CONFIGS:LEARNED', JSON.stringify(configsData), 'EX', 600);

    // 5. CIRCUIT_BREAKER:STATUS — trạng thái bảo vệ rủi ro
    pipeline.set('CIRCUIT_BREAKER:STATUS', JSON.stringify(circuitBreaker.getStatus()), 'EX', 600);

    // 6. API_RATE_LIMIT — trạng thái rate limit
    pipeline.set('API_RATE_LIMIT', JSON.stringify({
      usagePercent: apiRateLimiter.getUsagePercent(),
      remaining: apiRateLimiter.getRemainingTokens(),
    }), 'EX', 60);

    // 7. STABLE:COINS — danh sách coin ổn định luôn được phân tích
    pipeline.set('STABLE:COINS', JSON.stringify(STABLE_COINS), 'EX', 600);

    await pipeline.exec();
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error('[Redis] Push analysis failed:', msg);
  }
}

// ─── REDIS: DASHBOARD SUMMARY (mỗi 30s) ──────────────────────────────────────

async function pushDashboardSummary(): Promise<void> {
  try {
    const [radarRaw, analysisRaw, positionsRaw] = await Promise.all([
      redis.get(REDIS_RADAR_KEY),
      redis.get(REDIS_ANALYSIS_KEY),
      redis.get(REDIS_POSITIONS_KEY),
    ]);

    const radar = radarRaw ? JSON.parse(radarRaw) : [];
    const analyses = analysisRaw ? JSON.parse(analysisRaw) : [];
    const positions = positionsRaw ? JSON.parse(positionsRaw) : [];

    const summary = {
      updatedAt: new Date().toISOString(),
      radar: {
        count: radar.length,
        topSymbols: radar.slice(0, 5).map((r: RadarCoin) => r.symbol),
      },
      analysis: {
        count: analyses.length,
        signals: {
          LONG: analyses.filter((a: SymbolAnalysisReport) => a.analysis.signal === 'LONG').length,
          SHORT: analyses.filter((a: SymbolAnalysisReport) => a.analysis.signal === 'SHORT').length,
          HOLD: analyses.filter((a: SymbolAnalysisReport) => a.analysis.signal === 'HOLD').length,
        },
      },
      positions: {
        open: positions.length,
        symbols: positions.map((p: { symbol: string }) => p.symbol),
      },
    };

    await redis.set(REDIS_DASHBOARD_KEY, JSON.stringify(summary), 'EX', 120);
  } catch (err: unknown) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error('[Dashboard] Summary push failed:', msg);
  }
}

// ─── CRON: HỌC TẬP MỖI NGÀY 00:00 ───────────────────────────────────────────

function scheduleDailyLearner(): void {
  const now = new Date();
  const next = new Date(now);
  next.setHours(LEARNER_CRON_HOUR, 0, 0, 0);

  // Nếu đã qua 00:00 hôm nay, lên lịch cho ngày mai
  if (next.getTime() <= now.getTime()) {
    next.setDate(next.getDate() + 1);
  }

  const msUntilMidnight = next.getTime() - now.getTime();
  console.log(
    `[Learner] Scheduled daily run at ${next.toISOString()} ` +
    `(in ${(msUntilMidnight / 3600_000).toFixed(1)}h)`,
  );

  learnerTimer = setTimeout(async () => {
    await runDailyLearner();

    // Sau khi chạy xong, lên lịch cho ngày hôm sau
    scheduleDailyLearner();
  }, msUntilMidnight);
}

async function runDailyLearner(): Promise<void> {
  console.log('\n══════════════════════════════════════════════════════════════');
  console.log(`[Learner] Daily learning started at ${new Date().toISOString()}`);
  console.log('══════════════════════════════════════════════════════════════');

  const radarCoins = await getRadarSymbols();
  if (radarCoins.length === 0) {
    console.warn('[Learner] Radar empty. Skipping.');
    return;
  }

  const symbols = radarCoins.map((c) => c.symbol);
  console.log(`[Learner] Learning for ${symbols.length} symbols: ${symbols.join(', ')}`);

  for (let i = 0; i < symbols.length; i++) {
    try {
      await learnFromHistory(symbols[i]);
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`[Learner] ${symbols[i]} failed: ${msg}`);
    }

    // Rate limit: 5s giữa mỗi symbol
    if (i < symbols.length - 1) {
      await new Promise((r) => setTimeout(r, 5000));
    }
  }

  console.log('[Learner] Daily learning completed.\n');

  // Feedback loop: đánh giá hiệu suất và tự điều chỉnh RSI
  console.log('[Feedback] Running post-learning evaluation...');
  await evaluateAndAdjust();
  console.log('[Feedback] Evaluation completed.\n');
}

// ─── GRACEFUL SHUTDOWN ─────────────────────────────────────────────────────────

function gracefulShutdown(signal: string): void {
  console.log(`\n[Main] ${signal} received. Shutting down gracefully...`);

  // Dừng timers
  if (analysisTimer) clearInterval(analysisTimer);
  if (learnerTimer) clearTimeout(learnerTimer);
  if (heartbeatTimer) clearInterval(heartbeatTimer);
  if (dashboardTimer) clearInterval(dashboardTimer);

  // Dừng tất cả WebSocket/services
  for (const cleanup of cleanups) {
    try { cleanup(); } catch {}
  }

  // Đóng connections
  redis.disconnect();
  mongoose.disconnect().catch(() => {});

  setTimeout(() => process.exit(0), 2000);
}

// ─── MAIN ──────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log('══════════════════════════════════════════════════════════════');
  console.log('  🚀 TRADING BOT — System Architect Mode');
  console.log('══════════════════════════════════════════════════════════════\n');

  // ═══ 1. KẾT NỐI DATABASE ═══
  await connectMongoDB();
  console.log('[Main] ✅ MongoDB connected');

  await redis.ping();
  console.log('[Main] ✅ Redis connected');

  // ═══ 2. KHỞI CHẠY RADAR (liên tục cập nhật RADAR:HOT_COINS) ═══
  const cleanupRadar = getRadarData();
  cleanups.push(cleanupRadar);
  console.log('[Main] ✅ Radar started → Redis RADAR:HOT_COINS');

  // ═══ 3. ĐỢI RADAR CÓ DATA (tối đa 15s) ═══
  let radarReady = false;
  for (let i = 0; i < 15; i++) {
    await new Promise((r) => setTimeout(r, 1000));
    const radarCoins = await getRadarSymbols();
    if (radarCoins.length > 0) {
      radarReady = true;
      console.log(`[Main] ✅ Radar has ${radarCoins.length} coins after ${i + 1}s`);
      break;
    }
  }
  if (!radarReady) {
    console.warn('[Main] ⚠️ Radar still empty after 15s. Will retry in cycle.');
  }

  // ═══ 4. KHỞI CHẠY WHALE MONITOR (cho top coins) ═══
  const radarCoins = await getRadarSymbols();
  if (radarCoins.length > 0) {
    const whaleSymbols = radarCoins.slice(0, 5).map((c) => c.symbol);
    const cleanupWhale = monitorWhaleTrades(whaleSymbols);
    cleanups.push(cleanupWhale);
    console.log(`[Main] ✅ Whale monitor started for: ${whaleSymbols.join(', ')}`);
  }

  // ═══ 5. CHẠY CHU KỲ PHÂN TÍCH ĐẦU TIÊN ═══
  console.log('[Main] Running first analysis cycle...');
  await runAnalysisCycle();

  // ═══ 6. LỊCH PHÂN TÍCH MỖI 5 PHÚT ═══
  analysisTimer = setInterval(() => {
    runAnalysisCycle().catch((err: unknown) => {
      const msg = err instanceof Error ? err.message : String(err);
      console.error('[Main] Analysis cycle error:', msg);
    });
  }, ANALYSIS_INTERVAL_MS);
  console.log(`[Main] ✅ Analysis cycle scheduled every ${ANALYSIS_INTERVAL_MS / 60000} min`);

  // ═══ 7. LỊCH HỌC TẬP HÀNG NGÀY 00:00 ═══
  scheduleDailyLearner();

  // ═══ 8. DASHBOARD SUMMARY MỖI 30S ═══
  dashboardTimer = setInterval(() => {
    pushDashboardSummary().catch(() => {});
  }, 30_000);
  await pushDashboardSummary(); // Push ngay lần đầu
  console.log('[Main] ✅ Dashboard summary → Redis DASHBOARD:SUMMARY (every 30s)');

  // ═══ 9. HEARTBEAT MỖI 60S ═══
  heartbeatTimer = setInterval(() => {
    const mem = process.memoryUsage();
    console.log(
      `[Heartbeat] ${new Date().toISOString()} | RSS: ${(mem.rss / 1024 / 1024).toFixed(1)}MB | Heap: ${(mem.heapUsed / 1024 / 1024).toFixed(1)}MB`,
    );
  }, 60_000);

  // ═══ 10. GRACEFUL SHUTDOWN ═══
  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
  process.on('SIGINT', () => gracefulShutdown('SIGINT'));

  console.log('\n══════════════════════════════════════════════════════════════');
  console.log('  🟢 TRADING BOT IS RUNNING');
  console.log('══════════════════════════════════════════════════════════════');
  console.log('  Redis Keys for Next.js Frontend:');
  console.log(`    GET ${REDIS_RADAR_KEY}        → Top 10 coins (volume)  `);
  console.log(`    GET ${REDIS_ANALYSIS_KEY}     → Latest analysis all coins`);
  console.log(`    GET ANALYSIS:<SYMBOL>          → Detail per coin       `);
  console.log(`    GET ${REDIS_POSITIONS_KEY}      → Open positions        `);
  console.log(`    GET ${REDIS_DASHBOARD_KEY}   → Dashboard summary     `);
  console.log(`    GET WHALE:<SYMBOL>              → Whale trades list     `);
  console.log(`    GET price:<SYMBOL>              → Realtime price        `);
  console.log('══════════════════════════════════════════════════════════════\n');
}

main().catch((err) => {
  console.error('[Main] Fatal error:', err);
  process.exit(1);
});
