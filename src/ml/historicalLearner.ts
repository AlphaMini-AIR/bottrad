import { ema, rsi } from 'technicalindicators';
import { SymbolConfig, type ISymbolConfig } from '../database/models/SymbolConfig';
import type { KlineData } from '../services/marketDataFetcher';
import { connectMongoDB } from '../database/mongodb';

// ─── CONFIG ────────────────────────────────────────────────────────────────────

const BINANCE_FAPI_BASE = 'https://fapi.binance.com';
const KLINE_INTERVAL = '15m';
const LOOKBACK_DAYS = 7;
const KLINES_PER_DAY_15M = 96;  // 24h * 4 = 96 nến 15m / ngày
const TOTAL_KLINES = LOOKBACK_DAYS * KLINES_PER_DAY_15M; // 672
const KLINES_4H_NEEDED = 100;  // ~16.7 ngày nến 4H (đủ cho EMA 50 + 7 ngày overlap)
const INTERVAL_4H_MS = 4 * 60 * 60 * 1000;

const EMA_PERIOD = 50;
const RSI_PERIOD = 14;
const SLIPPAGE_AND_FEE = 0.0006;

const LEARN_INTERVAL_MS = 24 * 60 * 60 * 1000; // 24h

// ─── GRID SEARCH RANGES ───────────────────────────────────────────────────────

const RSI_OVERSOLD_RANGE = [20, 22, 25, 28, 30, 32, 35, 38, 40];
const RSI_OVERBOUGHT_RANGE = [60, 62, 65, 68, 70, 72, 75, 78, 80];
const DCA_DROP_RANGE = [1, 1.5, 2, 2.5, 3, 3.5, 4];
const MAX_DCA_RANGE = [1, 2, 3];

// ─── INTERFACES ────────────────────────────────────────────────────────────────

interface BacktestParams {
    rsiOversold: number;
    rsiOverbought: number;
    dcaDropPercent: number;
    maxDca: number;
}

interface BacktestResult {
    params: BacktestParams;
    totalPnl: number;
    maxDrawdown: number;     // % drawdown tệ nhất (số âm)
    trades: number;
    wins: number;
    winRate: number;
    sharpeProxy: number;     // PnL / |Drawdown| — càng cao càng tốt
}

type KlineRaw = [
    number, string, string, string, string, string,
    number, string, number, string, string, string
];

// ─── MAIN: learnFromHistory ────────────────────────────────────────────────────

/**
 * Walk-Forward Optimization: Tải 7 ngày nến 15M, Grid Search các thông số,
 * tìm bộ tối ưu, lưu vào SymbolConfig.
 *
 * Được thiết kế chạy ngầm mỗi 24h cho từng symbol.
 */
export async function learnFromHistory(symbol: string): Promise<ISymbolConfig | null> {
    const tag = `[Learner][${symbol}]`;

    try {
        // ── 1. Tải nến 15M 7 ngày ─────────────────────────────────────────────
        console.log(`${tag} Fetching ${TOTAL_KLINES} klines 15M (${LOOKBACK_DAYS}d)...`);
        const klines15M = await fetchHistoricalKlines(symbol.toUpperCase(), KLINE_INTERVAL, TOTAL_KLINES);

        if (klines15M.length < RSI_PERIOD + 1) {
            console.warn(`${tag} Insufficient 15M data: ${klines15M.length} klines. Skip.`);
            return null;
        }

        // ── 1b. Tải nến 4H riêng biệt (EMA 50 cần ≥50 nến 4H) ──────────────
        console.log(`${tag} Fetching ${KLINES_4H_NEEDED} klines 4H for EMA 50...`);
        const klines4H = await fetchHistoricalKlines(symbol.toUpperCase(), '4h', KLINES_4H_NEEDED);

        if (klines4H.length < EMA_PERIOD) {
            console.warn(`${tag} Insufficient 4H data: ${klines4H.length} klines (need ${EMA_PERIOD}). Skip.`);
            return null;
        }

        // ── 1c. Build trend map: 4H EMA 50 → trend mỗi nến 15M ──────────────
        const trend4H = buildTrend4HMap(klines15M, klines4H);
        const validTrends = trend4H.filter((t) => t !== null).length;
        console.log(`${tag} Loaded ${klines15M.length} klines 15M + ${klines4H.length} klines 4H. Trend coverage: ${validTrends}/${klines15M.length}`);

        // ── 2. Grid Search ────────────────────────────────────────────────────
        const results = runGridSearch(klines15M, trend4H);

        if (results.length === 0) {
            console.warn(`${tag} Grid Search returned 0 results. Skip.`);
            return null;
        }

        // ── 3. Chọn bộ thông số tốt nhất ─────────────────────────────────────
        // Tiêu chí: sharpeProxy cao nhất (PnL cao + Drawdown thấp)
        // Fallback nếu sharpe bằng nhau: ưu tiên Drawdown nhỏ hơn
        results.sort((a, b) => {
            if (b.sharpeProxy !== a.sharpeProxy) return b.sharpeProxy - a.sharpeProxy;
            return Math.abs(a.maxDrawdown) - Math.abs(b.maxDrawdown);
        });

        const best = results[0];

        console.log(
            `${tag} Best params: RSI [${best.params.rsiOversold}/${best.params.rsiOverbought}] ` +
            `DCA ${best.params.dcaDropPercent}%×${best.params.maxDca} | ` +
            `PnL: ${best.totalPnl.toFixed(2)}% | DD: ${best.maxDrawdown.toFixed(2)}% | ` +
            `Trades: ${best.trades} | WR: ${best.winRate.toFixed(1)}% | Sharpe: ${best.sharpeProxy.toFixed(3)}`,
        );

        // Log top 5 để so sánh
        const top5 = results.slice(0, 5);
        console.log(`${tag} Top 5 configs:`);
        for (const r of top5) {
            console.log(
                `  RSI [${r.params.rsiOversold}/${r.params.rsiOverbought}] ` +
                `DCA ${r.params.dcaDropPercent}%×${r.params.maxDca} → ` +
                `PnL: ${r.totalPnl.toFixed(2)}% DD: ${r.maxDrawdown.toFixed(2)}% WR: ${r.winRate.toFixed(1)}% Sharpe: ${r.sharpeProxy.toFixed(3)}`,
            );
        }

        // ── 4. Lưu vào MongoDB SymbolConfig ───────────────────────────────────
        const config = await SymbolConfig.findOneAndUpdate(
            { symbol: symbol.toUpperCase() },
            {
                $set: {
                    optimalRsi: {
                        oversold: best.params.rsiOversold,
                        overbought: best.params.rsiOverbought,
                    },
                    dcaDropPercent: best.params.dcaDropPercent,
                    maxDca: best.params.maxDca,
                    takeProfitPercent: 1.5,
                    fastScalpTpPercent: 1.0,
                    backtestPnl: best.totalPnl,
                    backtestDrawdown: best.maxDrawdown,
                    backtestTrades: best.trades,
                    backtestWinRate: best.winRate,
                    lastLearned: new Date(),
                },
            },
            { upsert: true, returnDocument: 'after' },
        );

        console.log(`${tag} ✅ SymbolConfig saved. Next learn in 24h.`);
        return config;
    } catch (err: unknown) {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`${tag} Error: ${message}`);
        return null;
    }
}

// ─── BUILD 4H TREND MAP ───────────────────────────────────────────────────────

/**
 * Tính EMA 50 trên nến 4H thực sự, sau đó map mỗi nến 15M → trend 4H tương ứng.
 * Đây là cách chính xác để backtest chiến lược multi-timeframe.
 */
function buildTrend4HMap(
    klines15M: KlineData[],
    klines4H: KlineData[],
): ('LONG' | 'SHORT' | null)[] {
    const closes4H = klines4H.map((k) => k.close);
    const ema50_4H = ema({ period: EMA_PERIOD, values: closes4H });

    // Build map: 4H openTime → trend
    const trendByTime = new Map<number, 'LONG' | 'SHORT'>();
    for (let i = 0; i < ema50_4H.length; i++) {
        const idx4H = i + (EMA_PERIOD - 1);
        const trend: 'LONG' | 'SHORT' = closes4H[idx4H] > ema50_4H[i] ? 'LONG' : 'SHORT';
        trendByTime.set(klines4H[idx4H].openTime, trend);
    }

    // Map mỗi nến 15M → trend của nến 4H cha
    return klines15M.map((k15m) => {
        const parent4HOpen = Math.floor(k15m.openTime / INTERVAL_4H_MS) * INTERVAL_4H_MS;
        return trendByTime.get(parent4HOpen) ?? null;
    });
}

// ─── GRID SEARCH ENGINE ───────────────────────────────────────────────────────

function runGridSearch(klines: KlineData[], trend4H: ('LONG' | 'SHORT' | null)[]): BacktestResult[] {
    const results: BacktestResult[] = [];

    const closes = klines.map((k) => k.close);

    // Pre-compute RSI 14 trên 15M (dùng chung, chỉ thay đổi ngưỡng)
    const rsi14Values = rsi({ period: RSI_PERIOD, values: closes });

    // RSI bắt đầu từ index RSI_PERIOD trong mảng closes
    const tradeStartIdx = RSI_PERIOD;

    if (tradeStartIdx >= closes.length) return results;

    let totalCombos = 0;

    for (const rsiOS of RSI_OVERSOLD_RANGE) {
        for (const rsiOB of RSI_OVERBOUGHT_RANGE) {
            if (rsiOB - rsiOS < 20) continue;

            for (const dcaDrop of DCA_DROP_RANGE) {
                for (const maxDca of MAX_DCA_RANGE) {
                    const params: BacktestParams = {
                        rsiOversold: rsiOS,
                        rsiOverbought: rsiOB,
                        dcaDropPercent: dcaDrop,
                        maxDca,
                    };

                    const result = simulateStrategy(
                        klines, closes, trend4H, rsi14Values,
                        tradeStartIdx, params,
                    );

                    if (result.trades >= 1) {
                        results.push(result);
                    }

                    totalCombos++;
                }
            }
        }
    }

    // ── Diagnostic: phân tích tại sao 0 trades ──
    if (results.length === 0) {
        let longZone = 0, shortZone = 0, noTrend = 0;
        let rsiMin = 100, rsiMax = 0;
        let longEntryCount = 0, shortEntryCount = 0;

        for (let i = tradeStartIdx; i < closes.length; i++) {
            const rsiIdx = i - RSI_PERIOD;
            if (rsiIdx < 0 || rsiIdx >= rsi14Values.length) continue;

            const trend = trend4H[i];
            const rsiVal = rsi14Values[rsiIdx];

            if (trend === 'LONG') longZone++;
            else if (trend === 'SHORT') shortZone++;
            else noTrend++;

            if (rsiVal < rsiMin) rsiMin = rsiVal;
            if (rsiVal > rsiMax) rsiMax = rsiVal;

            if (trend === 'LONG' && rsiVal <= 40) longEntryCount++;
            if (trend === 'SHORT' && rsiVal >= 60) shortEntryCount++;
        }

        console.log(`[Learner][Diag] Tradeable: ${closes.length - tradeStartIdx} candles`);
        console.log(`[Learner][Diag] 4H Trend: LONG=${longZone} SHORT=${shortZone} noData=${noTrend}`);
        console.log(`[Learner][Diag] RSI 15M range: ${rsiMin.toFixed(1)} - ${rsiMax.toFixed(1)}`);
        console.log(`[Learner][Diag] LONG pullback (4H LONG & RSI≤40): ${longEntryCount}`);
        console.log(`[Learner][Diag] SHORT pullback (4H SHORT & RSI≥60): ${shortEntryCount}`);
    }

    console.log(`[Learner] Grid Search: ${totalCombos} combos tested, ${results.length} valid.`);
    return results;
}

// ─── BACKTEST SIMULATOR ────────────────────────────────────────────────────────

interface SimPosition {
    side: 'LONG' | 'SHORT';
    avgPrice: number;
    totalQty: number;
    dcaCount: number;
    entryIdx: number;
}

function simulateStrategy(
    klines: KlineData[],
    closes: number[],
    trend4H: ('LONG' | 'SHORT' | null)[],
    rsi14Values: number[],
    startIdx: number,
    params: BacktestParams,
): BacktestResult {
    let equity = 0;         // % PnL tích lũy
    let peakEquity = 0;
    let maxDrawdown = 0;
    let trades = 0;
    let wins = 0;
    let position: SimPosition | null = null;

    for (let i = startIdx; i < closes.length; i++) {
        const price = closes[i];
        const rsiIdx = i - RSI_PERIOD;

        if (rsiIdx < 0 || rsiIdx >= rsi14Values.length) continue;

        const trend = trend4H[i];
        if (trend === null) continue; // Không có dữ liệu 4H tại thời điểm này

        const rsiVal = rsi14Values[rsiIdx];

        // ── Xác định xu hướng từ nến 4H thực sự (EMA 50 trên 4H) ──
        // trend đã được tính sẵn trong trend4H map

        // ── Quản lý vị thế đang mở ──
        if (position) {
            const pnlPct = position.side === 'LONG'
                ? ((price - position.avgPrice) / position.avgPrice) * 100
                : ((position.avgPrice - price) / position.avgPrice) * 100;

            // STOP LOSS: xu hướng gãy
            if (
                (position.side === 'LONG' && trend === 'SHORT') ||
                (position.side === 'SHORT' && trend === 'LONG')
            ) {
                const netPnl = pnlPct - (SLIPPAGE_AND_FEE * 100); // exit fee only (entry đã tính vào avgPrice)
                equity += netPnl;
                trades++;
                if (netPnl > 0) wins++;
                position = null;

                trackDrawdown();
                continue;
            }

            // TAKE PROFIT: lãi > 1.5% (cố định cho backtest)
            if (pnlPct >= 1.5) {
                const netPnl = pnlPct - (SLIPPAGE_AND_FEE * 100);
                equity += netPnl;
                trades++;
                if (netPnl > 0) wins++;
                position = null;

                trackDrawdown();
                continue;
            }

            // DCA: lỗ chạm ngưỡng + xu hướng vẫn đúng
            if (pnlPct <= -params.dcaDropPercent && position.dcaCount < params.maxDca) {
                const trendStillValid =
                    (position.side === 'LONG' && trend === 'LONG') ||
                    (position.side === 'SHORT' && trend === 'SHORT');

                if (trendStillValid) {
                    // DCA x1.5
                    const dcaQty = position.totalQty * 1.5;
                    const dcaPrice = position.side === 'LONG'
                        ? price * (1 + SLIPPAGE_AND_FEE)
                        : price * (1 - SLIPPAGE_AND_FEE);

                    const oldCost = position.avgPrice * position.totalQty;
                    const dcaCost = dcaPrice * dcaQty;
                    position.totalQty += dcaQty;
                    position.avgPrice = (oldCost + dcaCost) / position.totalQty;
                    position.dcaCount++;
                }
            }

            continue;
        }

        // ── Mở vị thế mới: RSI pullback ──
        if (trend === 'LONG' && rsiVal <= params.rsiOversold) {
            const entryPrice = price * (1 + SLIPPAGE_AND_FEE);
            position = {
                side: 'LONG',
                avgPrice: entryPrice,
                totalQty: 1,
                dcaCount: 0,
                entryIdx: i,
            };
        } else if (trend === 'SHORT' && rsiVal >= params.rsiOverbought) {
            const entryPrice = price * (1 - SLIPPAGE_AND_FEE);
            position = {
                side: 'SHORT',
                avgPrice: entryPrice,
                totalQty: 1,
                dcaCount: 0,
                entryIdx: i,
            };
        }
    }

    // Đóng vị thế còn lại cuối backtest (market close)
    if (position) {
        const lastPrice = closes[closes.length - 1];
        const pnlPct = position.side === 'LONG'
            ? ((lastPrice - position.avgPrice) / position.avgPrice) * 100
            : ((position.avgPrice - lastPrice) / position.avgPrice) * 100;

        const netPnl = pnlPct - (SLIPPAGE_AND_FEE * 100);
        equity += netPnl;
        trades++;
        if (netPnl > 0) wins++;
        trackDrawdown();
    }

    const winRate = trades > 0 ? (wins / trades) * 100 : 0;

    // Sharpe proxy: risk-adjusted return
    // Nếu drawdown = 0 (no losses), dùng PnL trực tiếp
    const sharpeProxy = maxDrawdown === 0
        ? equity
        : equity / Math.abs(maxDrawdown);

    return {
        params,
        totalPnl: equity,
        maxDrawdown,
        trades,
        wins,
        winRate,
        sharpeProxy,
    };

    function trackDrawdown(): void {
        if (equity > peakEquity) peakEquity = equity;
        const dd = equity - peakEquity;
        if (dd < maxDrawdown) maxDrawdown = dd;
    }
}

// ─── FETCH HISTORICAL KLINES ──────────────────────────────────────────────────

/**
 * Binance giới hạn 1500 klines/request.
 * 7 ngày × 96 = 672 klines → 1 request đủ.
 * Nếu cần nhiều hơn, sẽ phân trang tự động.
 */
async function fetchHistoricalKlines(
    symbol: string,
    interval: string,
    total: number,
): Promise<KlineData[]> {
    const MAX_PER_REQUEST = 1500;
    const allKlines: KlineData[] = [];

    let endTime = Date.now();
    let remaining = total;

    while (remaining > 0) {
        const limit = Math.min(remaining, MAX_PER_REQUEST);
        const url =
            `${BINANCE_FAPI_BASE}/fapi/v1/klines?symbol=${encodeURIComponent(symbol)}` +
            `&interval=${encodeURIComponent(interval)}&limit=${limit}&endTime=${endTime}`;

        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 15_000);

        try {
            const res = await fetch(url, { signal: controller.signal });

            if (!res.ok) {
                const body = await res.text().catch(() => '');
                throw new Error(`HTTP ${res.status}: ${body}`);
            }

            const raw = (await res.json()) as KlineRaw[];

            if (raw.length === 0) break;

            const parsed = raw.map(parseKline);
            allKlines.unshift(...parsed); // prepend (cũ nhất ở đầu)

            // endTime cho request tiếp = openTime nến cũ nhất - 1
            endTime = raw[0][0] - 1;
            remaining -= raw.length;

            // Nếu API trả ít hơn limit → hết data
            if (raw.length < limit) break;
        } finally {
            clearTimeout(timeout);
        }
    }

    return allKlines;
}

function parseKline(raw: KlineRaw): KlineData {
    return {
        openTime: raw[0],
        open: parseFloat(raw[1]),
        high: parseFloat(raw[2]),
        low: parseFloat(raw[3]),
        close: parseFloat(raw[4]),
        volume: parseFloat(raw[5]),
        closeTime: raw[6],
        quoteVolume: parseFloat(raw[7]),
        trades: raw[8],
    };
}

// ─── SCHEDULER: CHẠY NGẦM MỖI 24H ───────────────────────────────────────────

let learnerTimer: ReturnType<typeof setInterval> | null = null;

/**
 * Khởi động learner cho danh sách symbols. Chạy ngay lần đầu, sau đó mỗi 24h.
 * Trả về cleanup function.
 */
export function startHistoricalLearner(symbols: string[]): () => void {
    console.log(`[Learner] Starting for ${symbols.length} symbols. Interval: 24h.`);

    // Chạy ngay lần đầu (stagger 5s giữa mỗi symbol để tránh rate limit)
    runBatch(symbols);

    // Schedule 24h
    learnerTimer = setInterval(() => {
        console.log('[Learner] 24h cycle triggered.');
        runBatch(symbols);
    }, LEARN_INTERVAL_MS);

    return () => {
        if (learnerTimer) {
            clearInterval(learnerTimer);
            learnerTimer = null;
        }
        console.log('[Learner] Stopped.');
    };
}

async function runBatch(symbols: string[]): Promise<void> {
    for (let i = 0; i < symbols.length; i++) {
        const sym = symbols[i];
        try {
            await learnFromHistory(sym);
        } catch (err: unknown) {
            const message = err instanceof Error ? err.message : String(err);
            console.error(`[Learner] ${sym} failed: ${message}`);
        }

        // Stagger: chờ 5s giữa mỗi symbol (tránh Binance rate limit)
        if (i < symbols.length - 1) {
            await delay(5000);
        }
    }
}

function delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

// ─── PUBLIC: LẤY CONFIG CHO SYMBOL ────────────────────────────────────────────

/**
 * Lấy SymbolConfig đã learned. Nếu chưa có, trả về default.
 */
export async function getLearnedConfig(symbol: string): Promise<{
    optimalRsi: { oversold: number; overbought: number };
    dcaDropPercent: number;
    maxDca: number;
    takeProfitPercent: number;
    fastScalpTpPercent: number;
}> {
    const config = await SymbolConfig.findOne({ symbol: symbol.toUpperCase() }).lean();

    if (config) {
        return {
            optimalRsi: config.optimalRsi,
            dcaDropPercent: config.dcaDropPercent,
            maxDca: config.maxDca,
            takeProfitPercent: config.takeProfitPercent,
            fastScalpTpPercent: config.fastScalpTpPercent,
        };
    }

    // Default nếu chưa có data
    return {
        optimalRsi: { oversold: 30, overbought: 70 },
        dcaDropPercent: 2,
        maxDca: 3,
        takeProfitPercent: 3,
        fastScalpTpPercent: 1,
    };
}

// ─── TEST: Chỉ chạy khi gọi trực tiếp (npx ts-node), KHÔNG chạy khi import ──

if (require.main === module) {
    (async () => {
        await connectMongoDB();
        console.log('Đang bắt đầu quá trình học tập cho BTCUSDT...');
        const bestConfig = await learnFromHistory('BTCUSDT');
        console.log('Kết quả thông số tối ưu tìm được:', JSON.stringify(bestConfig, null, 2));
        process.exit(0);
    })();
}
