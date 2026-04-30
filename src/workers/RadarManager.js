const axios = require('axios');
const Redis = require('ioredis');

// ============================================================
// RADAR MANAGER V24 - PRODUCTION-STABLE PROFIT OPPORTUNITY RADAR 3M
// ------------------------------------------------------------
// Vai trò:
// 1. Quét Binance Futures USDT-M Perpetual.
// 2. Không vào lệnh.
// 3. Không quyết định trade.
// 4. Chỉ lọc altcoin có cơ hội biến động tốt để Feed theo dõi.
// 5. Publish SUBSCRIBE / UNSUBSCRIBE cho FeedHandler.
// 6. Ghi macro:scores, radar:hotlist, radar:watchlist.
//
// Triết lý V24:
// - Giữ Radar đơn giản, ổn định, không overfit bằng quá nhiều indicator.
// - Ưu tiên tính vận hành thật: Binance time sync, Redis publish chắc hơn,
//   slot lifecycle rõ ràng, stale slot pruning, filterStats đầy đủ.
// - Không chọn coin khi thiếu dữ liệu cần thiết như ticker/bookTicker.
// - Không publish spam khi update nhỏ.
// - Output đủ giàu để Feed/Order tiếp tục xử lý realtime.
// ============================================================


// ============================================================
// 1. HARD-CODED RUNTIME CONFIG
// ============================================================

const REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';

const CHANNELS = {
    CANDIDATES: 'radar:candidates',
    SUBSCRIPTIONS: 'system:subscriptions',
    MACRO_SCORES: 'macro:scores',
    HOTLIST: 'radar:hotlist',
    WATCHLIST: 'radar:watchlist'
};

const BINANCE_FAPI_BASE = 'https://fapi.binance.com';

const RADAR_INTERVAL = '3m';
const RADAR_INTERVAL_MS = 3 * 60 * 1000;

// Chờ nến chạy 45s để giảm spike ảo đầu nến.
const SCAN_OFFSET_MS = 45 * 1000;
const MIN_CANDLE_ELAPSED_MS = 45 * 1000;

// Top symbol mới mỗi scan.
const MAX_RADAR_SLOTS = 30;

// Tổng slot active tối đa, bao gồm sticky slot.
const MAX_ACTIVE_SLOTS = 40;
const MIN_FORCE_KEEP_MS = 2 * 60 * 1000;

// Cooldown sau khi remove để tránh subscribe/unsubscribe lặp lại.
const RESUBSCRIBE_COOLDOWN_MS = 6 * 60 * 1000;
const REMOVED_CACHE_TTL_MS = 30 * 60 * 1000;

// Slot cũ quá lâu không được cập nhật sẽ bị dọn để tránh giữ rác khi thị trường đổi pha.
const MAX_SLOT_STALE_MS = 15 * 60 * 1000;

// Chặn republish spam cùng một symbol.
const MIN_REPUBLISH_INTERVAL_MS = 60 * 1000;

const OPPORTUNITY_THRESHOLD = 0.5;
const HOT_THRESHOLD = 0.65;

const MIN_24H_QUOTE_VOLUME = 3_000_000;

const EXCLUDED_SYMBOLS = new Set([
    'BTCUSDT',
    'ETHUSDT',
    'BNBUSDT',
    'SOLUSDT',
    'XRPUSDT',
    'ADAUSDT',
    'DOGEUSDT',
    'TRXUSDT',
    'LTCUSDT',
    'BCHUSDT',
    'LINKUSDT',
    'AVAXUSDT',
    'DOTUSDT',
    'UNIUSDT',
    'ATOMUSDT',
    'ETCUSDT',
    'AAVEUSDT',
    'NEARUSDT',
    'FILUSDT'
]);

const SAFE_SYMBOL_REGEX = /^[A-Z0-9]+USDT$/;

const KLINE_LIMIT = 48;

const MIN_ABS_MOVE = 0.002;
const MIN_RANGE = 0.003;
const MIN_BODY_STRENGTH = 0.0;

const MIN_VOLUME_RATIO = 1.5;
const MIN_QUOTE_VOLUME_RATIO = 1.5;
const MIN_TRADES_RATIO = 1.3;

const MIN_TRIGGER_COUNT = 3;

// Spread cao dễ chết vì slippage/fee khi scalping.
const MAX_SPREAD_BPS = 15;

const MAX_PROJECTION_FACTOR = 4;

const MIN_HOLD_MS = 6 * 60 * 1000;
const RADAR_LIST_TTL_SEC = 10 * 60;

const REQUEST_TIMEOUT_MS = 10_000;
const MAX_RETRIES = 2;
const RETRY_BASE_DELAY_MS = 500;
const RETRY_RATE_LIMIT_DELAY_MS = 5000;

const BATCH_SIZE = 5;
const BATCH_DELAY_MS = 250;

const SYMBOLS_CACHE_MS = 15 * 60 * 1000;
const TICKER_CACHE_MS = 10 * 1000;
const BOOK_TICKER_CACHE_MS = 3 * 1000;
const BINANCE_TIME_SYNC_MS = 5 * 60 * 1000;

// Nếu scan kéo dài quá lâu so với interval thì cảnh báo để giảm batch/slot sau này.
const SCAN_WARN_DURATION_MS = 120 * 1000;


// ============================================================
// 2. REDIS CLIENTS
// ============================================================

const pubClient = new Redis(REDIS_URL);
const dataClient = new Redis(REDIS_URL);

pubClient.on('error', err => console.error('❌ Redis pubClient error:', err.message));
dataClient.on('error', err => console.error('❌ Redis dataClient error:', err.message));


// ============================================================
// 3. STATE
// ============================================================

let currentSlots = new Map(); // symbol -> candidate
let activeSubscribedSymbols = new Set();
let recentlyRemovedSymbols = new Map(); // symbol -> removedAt

let tradableSymbolsCache = {
    updatedAt: 0,
    symbols: []
};

let tickerCache = {
    updatedAt: 0,
    map: new Map()
};

let bookTickerCache = {
    updatedAt: 0,
    map: new Map()
};

let binanceTimeState = {
    updatedAt: 0,
    offsetMs: 0,
    lastServerTime: 0
};

let isScanning = false;
let nextScanTimer = null;
let scanSeq = 0;


// ============================================================
// 4. UTILS
// ============================================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function safeNumber(value, fallback = 0) {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
}

function round(value, digits = 6) {
    const factor = 10 ** digits;
    return Math.round(value * factor) / factor;
}

function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
}

function jitter(ms, ratio = 0.2) {
    const spread = ms * ratio;
    return Math.round(ms - spread + Math.random() * spread * 2);
}

function mean(values) {
    if (!values.length) return 0;
    return values.reduce((sum, v) => sum + v, 0) / values.length;
}

function logScaleScore(value, target) {
    if (value <= 1) return 0;
    return clamp(Math.log(value) / Math.log(target), 0, 1);
}

function pctChange(from, to) {
    if (from <= 0 || to <= 0) return 0;
    return (to - from) / from;
}

function klineHigh(k) {
    return safeNumber(k[2]);
}

function klineLow(k) {
    return safeNumber(k[3]);
}

function klineClose(k) {
    return safeNumber(k[4]);
}

function binanceNow() {
    return Date.now() + binanceTimeState.offsetMs;
}

function getBinanceUsedWeight(headers) {
    if (!headers) return null;
    const value = headers['x-mbx-used-weight-1m'] || headers['X-MBX-USED-WEIGHT-1M'];
    return value ? Number(value) : null;
}

function isRateLimitError(error) {
    const status = error && error.response ? error.response.status : null;
    return status === 429 || status === 418;
}

function isRetryableNetworkError(error) {
    if (isRateLimitError(error)) return true;
    const code = error ? error.code : null;
    return ['ECONNRESET', 'ETIMEDOUT', 'ECONNABORTED', 'EAI_AGAIN', 'ENOTFOUND'].includes(code);
}

function createFilterStats() {
    return {
        missingTicker: 0,
        missingBookTicker: 0,
        lowLiquidity: 0,
        highSpread: 0,
        klineInvalid: 0,
        candleTooEarly: 0,
        invalidPrice: 0,
        lowBaseline: 0,
        weakPriceAction: 0,
        lowVolume: 0,
        lowTrades: 0,
        lowRange: 0,
        weakBody: 0,
        highWick: 0,
        choppy: 0,
        lateWatch: 0,
        lowScore: 0,
        cooldownSkip: 0,
        staleRemoved: 0,
        forceRemoved: 0,
        errors: 0
    };
}

function isInResubscribeCooldown(symbol) {
    const removedAt = recentlyRemovedSymbols.get(symbol);
    if (!removedAt) return false;
    return Date.now() - removedAt < RESUBSCRIBE_COOLDOWN_MS;
}

function cleanupRemovedCache() {
    const now = Date.now();

    for (const [symbol, removedAt] of recentlyRemovedSymbols.entries()) {
        if (now - removedAt > REMOVED_CACHE_TTL_MS) {
            recentlyRemovedSymbols.delete(symbol);
        }
    }
}

function hasImportantTagChange(oldTags = [], newTags = []) {
    const importantTags = [
        'STRONG_MOMENTUM',
        'BREAKOUT_UP',
        'BREAKOUT_DOWN',
        'PRICE_IMPULSE_STRONG',
        'RANGE_EXPANSION_STRONG',
        'BUY_FLOW_STRONG',
        'SELL_FLOW_STRONG',
        'LATE_MOVE_RISK',
        'LONG_WICK_RISK',
        'CHOPPY_STRUCTURE'
    ];

    return importantTags.some(tag =>
        oldTags.includes(tag) !== newTags.includes(tag)
    );
}

function shouldRepublishCandidate(oldCandidate, newCandidate) {
    if (!oldCandidate || !newCandidate) return true;

    const lastPublishedAt = safeNumber(oldCandidate.lastPublishedAt, 0);
    const recentlyPublished = Date.now() - lastPublishedAt < MIN_REPUBLISH_INTERVAL_MS;

    const criticalChange =
        oldCandidate.tier !== newCandidate.tier ||
        oldCandidate.direction !== newCandidate.direction;

    if (criticalChange) return true;

    if (recentlyPublished) return false;

    return (
        Math.abs(newCandidate.score - oldCandidate.score) >= 0.08 ||
        hasImportantTagChange(oldCandidate.tags, newCandidate.tags)
    );
}

function validateConfig() {
    const errors = [];

    if (MAX_RADAR_SLOTS <= 0) errors.push('MAX_RADAR_SLOTS must be > 0');
    if (MAX_ACTIVE_SLOTS < MAX_RADAR_SLOTS) errors.push('MAX_ACTIVE_SLOTS must be >= MAX_RADAR_SLOTS');
    if (MIN_CANDLE_ELAPSED_MS < 15_000) errors.push('MIN_CANDLE_ELAPSED_MS too low');
    if (SCAN_OFFSET_MS < MIN_CANDLE_ELAPSED_MS) errors.push('SCAN_OFFSET_MS should be >= MIN_CANDLE_ELAPSED_MS');
    if (OPPORTUNITY_THRESHOLD >= HOT_THRESHOLD) errors.push('OPPORTUNITY_THRESHOLD must be < HOT_THRESHOLD');
    if (MAX_PROJECTION_FACTOR > 6) errors.push('MAX_PROJECTION_FACTOR too high for scalping radar');
    if (BATCH_SIZE <= 0) errors.push('BATCH_SIZE must be > 0');

    if (errors.length) {
        throw new Error(`[CONFIG] Invalid config: ${errors.join('; ')}`);
    }
}


// ============================================================
// 5. HTTP CLIENT CÓ RETRY/BACKOFF
// ============================================================

async function binanceGet(path, params = {}, options = {}) {
    const maxRetries = options.maxRetries ?? MAX_RETRIES;
    const timeout = options.timeout ?? REQUEST_TIMEOUT_MS;

    let lastError = null;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
            const response = await axios.get(`${BINANCE_FAPI_BASE}${path}`, {
                params,
                timeout
            });

            const usedWeight = getBinanceUsedWeight(response.headers);
            if (usedWeight && usedWeight > 1800) {
                console.warn(`⚠️ [BINANCE] Used weight 1m cao: ${usedWeight}`);
            }

            return response;
        } catch (error) {
            lastError = error;

            if (!isRetryableNetworkError(error) || attempt === maxRetries) {
                throw error;
            }

            const delay = isRateLimitError(error)
                ? RETRY_RATE_LIMIT_DELAY_MS
                : RETRY_BASE_DELAY_MS * (attempt + 1);

            const status = error && error.response ? error.response.status : 'NA';
            const code = error && error.code ? error.code : 'NA';

            console.warn(`⚠️ [BINANCE] Retry ${attempt + 1}/${maxRetries} path=${path} status=${status} code=${code} delay=${delay}ms`);
            await sleep(jitter(delay));
        }
    }

    throw lastError;
}

async function syncBinanceTime(force = false) {
    const now = Date.now();

    if (!force && binanceTimeState.updatedAt > 0 && now - binanceTimeState.updatedAt < BINANCE_TIME_SYNC_MS) {
        return binanceTimeState.offsetMs;
    }

    try {
        const localBefore = Date.now();
        const response = await binanceGet('/fapi/v1/time', {}, { maxRetries: 1, timeout: 5000 });
        const localAfter = Date.now();

        const serverTime = safeNumber(response.data.serverTime);
        if (serverTime <= 0) return binanceTimeState.offsetMs;

        const localMidpoint = Math.round((localBefore + localAfter) / 2);
        const offsetMs = serverTime - localMidpoint;

        binanceTimeState = {
            updatedAt: now,
            offsetMs,
            lastServerTime: serverTime
        };

        if (Math.abs(offsetMs) > 1500) {
            console.warn(`⚠️ [TIME] Local clock lệch Binance khoảng ${offsetMs}ms`);
        }

        return offsetMs;
    } catch (error) {
        console.warn(`⚠️ [TIME] Không sync được Binance time, dùng offset cũ=${binanceTimeState.offsetMs}ms | ${error.message}`);
        return binanceTimeState.offsetMs;
    }
}


// ============================================================
// 6. FETCH SYMBOLS / TICKERS
// ============================================================

async function fetchTradableSymbols() {
    const now = Date.now();

    if (
        tradableSymbolsCache.symbols.length > 0 &&
        now - tradableSymbolsCache.updatedAt < SYMBOLS_CACHE_MS
    ) {
        return tradableSymbolsCache.symbols;
    }

    try {
        const response = await binanceGet('/fapi/v1/exchangeInfo');

        const symbols = response.data.symbols
            .filter(item => item.status === 'TRADING')
            .filter(item => item.contractType === 'PERPETUAL')
            .filter(item => item.quoteAsset === 'USDT')
            .map(item => item.symbol)
            .filter(symbol => SAFE_SYMBOL_REGEX.test(symbol))
            .filter(symbol => !EXCLUDED_SYMBOLS.has(symbol));

        tradableSymbolsCache = {
            updatedAt: now,
            symbols
        };

        console.log(`🌐 [RADAR] Tradable filtered symbols: ${symbols.length}`);
        return symbols;
    } catch (error) {
        console.error('❌ [RADAR] Lỗi exchangeInfo:', error.message);

        if (tradableSymbolsCache.symbols.length > 0) {
            console.warn('⚠️ [RADAR] Dùng cache symbols cũ');
            return tradableSymbolsCache.symbols;
        }

        return [];
    }
}

async function fetch24hTickerMap() {
    const now = Date.now();

    if (tickerCache.map.size > 0 && now - tickerCache.updatedAt < TICKER_CACHE_MS) {
        return tickerCache.map;
    }

    try {
        const response = await binanceGet('/fapi/v1/ticker/24hr');
        const map = new Map();

        for (const item of response.data) {
            map.set(item.symbol, item);
        }

        tickerCache = {
            updatedAt: now,
            map
        };

        return map;
    } catch (error) {
        console.error('❌ [RADAR] Lỗi ticker 24h:', error.message);

        if (tickerCache.map.size > 0) {
            console.warn('⚠️ [RADAR] Dùng cache ticker cũ');
            return tickerCache.map;
        }

        return new Map();
    }
}

async function fetchBookTickerMap() {
    const now = Date.now();

    if (bookTickerCache.map.size > 0 && now - bookTickerCache.updatedAt < BOOK_TICKER_CACHE_MS) {
        return bookTickerCache.map;
    }

    try {
        const response = await binanceGet('/fapi/v1/ticker/bookTicker');
        const map = new Map();

        for (const item of response.data) {
            map.set(item.symbol, item);
        }

        bookTickerCache = {
            updatedAt: now,
            map
        };

        return map;
    } catch (error) {
        console.error('❌ [RADAR] Lỗi bookTicker:', error.message);

        if (bookTickerCache.map.size > 0) {
            console.warn('⚠️ [RADAR] Dùng cache bookTicker cũ');
            return bookTickerCache.map;
        }

        return new Map();
    }
}


// ============================================================
// 7. CHART CONTEXT FILTER
// ============================================================

function analyzeChartContext(closedHistory, current, previousClose, directionHint = 'WATCH') {
    const contextTags = [];

    const closeNow = safeNumber(current[4]);
    const openNow = safeNumber(current[1]);
    const highNow = safeNumber(current[2]);
    const lowNow = safeNumber(current[3]);

    const closes = closedHistory.map(klineClose).filter(v => v > 0);
    const highs = closedHistory.map(klineHigh).filter(v => v > 0);
    const lows = closedHistory.map(klineLow).filter(v => v > 0);

    if (
        closeNow <= 0 ||
        openNow <= 0 ||
        highNow <= 0 ||
        lowNow <= 0 ||
        previousClose <= 0 ||
        closes.length < 12 ||
        highs.length < 12 ||
        lows.length < 12
    ) {
        return {
            trendScore: 0,
            breakoutScore: 0,
            chopPenalty: 0.3,
            wickPenalty: 0.3,
            lateMovePenalty: 0,
            volatilityScore: 0,
            recentMove6: 0,
            recentMove12: 0,
            contextTags: ['CONTEXT_INSUFFICIENT']
        };
    }

    const close6Ago = closes[Math.max(0, closes.length - 6)];
    const close12Ago = closes[Math.max(0, closes.length - 12)];

    const recentMove6 = pctChange(close6Ago, closeNow);
    const recentMove12 = pctChange(close12Ago, closeNow);

    const directionForContext = directionHint === 'LONG' || directionHint === 'SHORT'
        ? directionHint
        : closeNow >= previousClose ? 'LONG' : 'SHORT';

    let trend6Aligned = 0;
    let trend12Aligned = 0;

    if (directionForContext === 'LONG') {
        trend6Aligned = recentMove6 > 0 ? clamp(recentMove6 / 0.018, 0, 1) : 0;
        trend12Aligned = recentMove12 > 0 ? clamp(recentMove12 / 0.035, 0, 1) : 0;
    } else {
        trend6Aligned = recentMove6 < 0 ? clamp(Math.abs(recentMove6) / 0.018, 0, 1) : 0;
        trend12Aligned = recentMove12 < 0 ? clamp(Math.abs(recentMove12) / 0.035, 0, 1) : 0;
    }

    let trendScore = trend6Aligned * 0.6 + trend12Aligned * 0.4;

    const recentHigh12 = Math.max(...highs.slice(-12));
    const recentLow12 = Math.min(...lows.slice(-12));
    const range12 = recentHigh12 - recentLow12;

    let positionScore = 0;

    if (range12 > 0) {
        const posInRange = (closeNow - recentLow12) / range12;

        if (directionForContext === 'LONG') {
            positionScore = clamp((posInRange - 0.5) / 0.5, 0, 1);
        } else {
            positionScore = clamp((0.5 - posInRange) / 0.5, 0, 1);
        }
    }

    trendScore = clamp(trendScore * 0.7 + positionScore * 0.3, 0, 1);

    if (trendScore >= 0.45) {
        contextTags.push('TREND_CONTEXT_OK');
    }

    const recentHigh6 = Math.max(...highs.slice(-6));
    const recentLow6 = Math.min(...lows.slice(-6));
    const recentHigh12Closed = Math.max(...highs.slice(-12));
    const recentLow12Closed = Math.min(...lows.slice(-12));

    let breakoutScore = 0;

    if (directionForContext === 'LONG') {
        const breakout6 = closeNow > recentHigh6
            ? clamp((closeNow - recentHigh6) / previousClose / 0.006, 0, 1)
            : 0;

        const breakout12 = closeNow > recentHigh12Closed
            ? clamp((closeNow - recentHigh12Closed) / previousClose / 0.01, 0, 1)
            : 0;

        breakoutScore = clamp(breakout6 * 0.55 + breakout12 * 0.45, 0, 1);

        if (breakoutScore >= 0.25) {
            contextTags.push('BREAKOUT_UP');
        }
    } else {
        const breakout6 = closeNow < recentLow6
            ? clamp((recentLow6 - closeNow) / previousClose / 0.006, 0, 1)
            : 0;

        const breakout12 = closeNow < recentLow12Closed
            ? clamp((recentLow12Closed - closeNow) / previousClose / 0.01, 0, 1)
            : 0;

        breakoutScore = clamp(breakout6 * 0.55 + breakout12 * 0.45, 0, 1);

        if (breakoutScore >= 0.25) {
            contextTags.push('BREAKOUT_DOWN');
        }
    }

    const last12Closes = closes.slice(-12);
    const returns = [];

    for (let i = 1; i < last12Closes.length; i++) {
        returns.push(pctChange(last12Closes[i - 1], last12Closes[i]));
    }

    const sumAbsReturn = returns.reduce((sum, r) => sum + Math.abs(r), 0);
    const netMove = Math.abs(pctChange(last12Closes[0], last12Closes[last12Closes.length - 1]));

    let choppiness = 0;

    if (netMove > 0.0001) {
        choppiness = sumAbsReturn / netMove;
    } else {
        choppiness = sumAbsReturn > 0.006 ? 10 : 1;
    }

    const directionChanges = returns.reduce((count, r, i) => {
        if (i === 0) return count;
        const prev = returns[i - 1];
        if ((prev > 0 && r < 0) || (prev < 0 && r > 0)) return count + 1;
        return count;
    }, 0);

    let chopPenalty = 0;

    if (choppiness <= 2.2) {
        chopPenalty = 0;
    } else if (choppiness <= 4.5) {
        chopPenalty = clamp((choppiness - 2.2) / 2.3 * 0.5, 0, 0.5);
    } else {
        chopPenalty = clamp(0.5 + (choppiness - 4.5) / 5.5 * 0.5, 0.5, 1);
    }

    chopPenalty = clamp(chopPenalty + clamp(directionChanges / 10, 0, 0.35), 0, 1);

    if (chopPenalty >= 0.45) {
        contextTags.push('CHOPPY_STRUCTURE');
    } else {
        contextTags.push('CLEAN_STRUCTURE');
    }

    const candleRange = highNow - lowNow;
    const body = Math.abs(closeNow - openNow);

    let wickPenalty = 0;

    if (candleRange > 0) {
        const upperWick = highNow - Math.max(openNow, closeNow);
        const lowerWick = Math.min(openNow, closeNow) - lowNow;
        const upperWickRatio = clamp(upperWick / candleRange, 0, 1);
        const lowerWickRatio = clamp(lowerWick / candleRange, 0, 1);
        const bodyRatio = clamp(body / candleRange, 0, 1);
        const totalWickRatio = clamp(1 - bodyRatio, 0, 1);

        if (directionForContext === 'LONG') {
            wickPenalty = upperWickRatio >= 0.45
                ? clamp((upperWickRatio - 0.35) / 0.55, 0, 1)
                : clamp(totalWickRatio * 0.25, 0, 0.25);
        } else {
            wickPenalty = lowerWickRatio >= 0.45
                ? clamp((lowerWickRatio - 0.35) / 0.55, 0, 1)
                : clamp(totalWickRatio * 0.25, 0, 0.25);
        }

        if (bodyRatio < 0.25 && totalWickRatio > 0.65) {
            wickPenalty = clamp(wickPenalty + 0.35, 0, 1);
        }
    }

    if (wickPenalty >= 0.45) {
        contextTags.push('LONG_WICK_RISK');
    }

    let lateMovePenalty = 0;

    if (Math.abs(recentMove6) > 0.035) {
        lateMovePenalty += clamp((Math.abs(recentMove6) - 0.035) / 0.035 * 0.65, 0, 0.65);
    }

    if (Math.abs(recentMove12) > 0.055) {
        lateMovePenalty += clamp((Math.abs(recentMove12) - 0.055) / 0.055 * 0.65, 0, 0.65);
    }

    lateMovePenalty = clamp(lateMovePenalty, 0, 1);

    if (lateMovePenalty >= 0.35) {
        contextTags.push('LATE_MOVE_RISK');
    }

    const last12Klines = closedHistory.slice(-12);
    const ranges = last12Klines.map(k => {
        const h = klineHigh(k);
        const l = klineLow(k);
        const c = klineClose(k);
        if (h <= 0 || l <= 0 || c <= 0) return 0;
        return (h - l) / c;
    }).filter(v => v > 0);

    const avgRange = mean(ranges);

    let volatilityScore = 0;

    if (avgRange < 0.0025) {
        volatilityScore = clamp(avgRange / 0.0025 * 0.35, 0, 0.35);
    } else if (avgRange <= 0.018) {
        volatilityScore = clamp(0.35 + (avgRange - 0.0025) / 0.0155 * 0.65, 0.35, 1);
    } else {
        volatilityScore = clamp(1 - (avgRange - 0.018) / 0.035, 0.25, 1);
    }

    if (volatilityScore >= 0.45) {
        contextTags.push('VOLATILITY_OK');
    }

    return {
        trendScore: round(trendScore, 4),
        breakoutScore: round(breakoutScore, 4),
        chopPenalty: round(chopPenalty, 4),
        wickPenalty: round(wickPenalty, 4),
        lateMovePenalty: round(lateMovePenalty, 4),
        volatilityScore: round(volatilityScore, 4),
        recentMove6: round(recentMove6, 6),
        recentMove12: round(recentMove12, 6),
        contextTags
    };
}


// ============================================================
// 8. ANALYZE SYMBOL
// ============================================================

function getSpreadBps(symbol, bookTickerMap, filterStats) {
    const item = bookTickerMap.get(symbol);

    if (!item) {
        filterStats.missingBookTicker += 1;
        return null;
    }

    const bid = safeNumber(item.bidPrice);
    const ask = safeNumber(item.askPrice);

    if (bid <= 0 || ask <= 0 || ask <= bid) {
        filterStats.missingBookTicker += 1;
        return null;
    }

    const mid = (bid + ask) / 2;
    return ((ask - bid) / mid) * 10000;
}

async function analyzeSymbol(symbol, ticker24h, bookTickerMap, filterStats, scanId) {
    try {
        if (!ticker24h) {
            filterStats.missingTicker += 1;
            return null;
        }

        const bookSpreadBps = getSpreadBps(symbol, bookTickerMap, filterStats);

        // Production-stable: thiếu bookTicker thì không chọn coin, vì scalping rất sợ spread/slippage mù.
        if (bookSpreadBps === null) {
            return null;
        }

        if (bookSpreadBps > MAX_SPREAD_BPS) {
            filterStats.highSpread += 1;
            return null;
        }

        const response = await binanceGet('/fapi/v1/klines', {
            symbol,
            interval: RADAR_INTERVAL,
            limit: KLINE_LIMIT
        }, {
            maxRetries: 1
        });

        const klines = response.data;

        if (!Array.isArray(klines) || klines.length < KLINE_LIMIT) {
            filterStats.klineInvalid += 1;
            return null;
        }

        const current = klines[klines.length - 1];
        const previous = klines[klines.length - 2];
        const closedHistory = klines.slice(0, -1);

        const openTime = safeNumber(current[0]);
        const openNow = safeNumber(current[1]);
        const highNow = safeNumber(current[2]);
        const lowNow = safeNumber(current[3]);
        const closeNow = safeNumber(current[4]);
        const volumeNow = safeNumber(current[5]);
        const quoteVolumeNow = safeNumber(current[7]);
        const tradesNow = safeNumber(current[8]);
        const takerBuyBaseNow = safeNumber(current[9]);
        const takerBuyQuoteNow = safeNumber(current[10]);
        const previousClose = safeNumber(previous[4]);

        if (
            openTime <= 0 ||
            openNow <= 0 ||
            highNow <= 0 ||
            lowNow <= 0 ||
            closeNow <= 0 ||
            previousClose <= 0 ||
            volumeNow <= 0 ||
            quoteVolumeNow <= 0
        ) {
            filterStats.invalidPrice += 1;
            return null;
        }

        const elapsedRawMs = binanceNow() - openTime;
        const elapsedMs = clamp(elapsedRawMs, 0, RADAR_INTERVAL_MS);

        if (elapsedMs < MIN_CANDLE_ELAPSED_MS) {
            filterStats.candleTooEarly += 1;
            return null;
        }

        let projectionFactor = 1;

        if (elapsedMs > 0 && elapsedMs < RADAR_INTERVAL_MS) {
            projectionFactor = RADAR_INTERVAL_MS / elapsedMs;
        }

        projectionFactor = clamp(projectionFactor, 1, MAX_PROJECTION_FACTOR);

        const projectedVolume = volumeNow * projectionFactor;
        const projectedQuoteVolume = quoteVolumeNow * projectionFactor;
        const projectedTrades = tradesNow * projectionFactor;

        const historyVolumes = closedHistory.map(k => safeNumber(k[5])).filter(v => v > 0);
        const historyQuoteVolumes = closedHistory.map(k => safeNumber(k[7])).filter(v => v > 0);
        const historyTrades = closedHistory.map(k => safeNumber(k[8])).filter(v => v > 0);

        const avgVolume = mean(historyVolumes);
        const avgQuoteVolume = mean(historyQuoteVolumes);
        const avgTrades = mean(historyTrades);

        if (avgVolume <= 0 || avgQuoteVolume <= 0 || avgTrades <= 0) {
            filterStats.lowBaseline += 1;
            return null;
        }

        const volumeRatio = projectedVolume / avgVolume;
        const quoteVolumeRatio = projectedQuoteVolume / avgQuoteVolume;
        const tradesRatio = projectedTrades / avgTrades;

        const priceChange3m = (closeNow - previousClose) / previousClose;
        const absMove = Math.abs(priceChange3m);

        const range3m = (highNow - lowNow) / previousClose;
        const candleRange = highNow - lowNow;
        const bodyStrength = candleRange > 0
            ? Math.abs(closeNow - openNow) / candleRange
            : 0;

        const takerBuyRatio = volumeNow > 0
            ? takerBuyBaseNow / volumeNow
            : 0.5;

        const takerBuyQuoteRatio = quoteVolumeNow > 0
            ? takerBuyQuoteNow / quoteVolumeNow
            : takerBuyRatio;

        const quoteVolume24h = safeNumber(ticker24h.quoteVolume);
        const priceChange24h = safeNumber(ticker24h.priceChangePercent) / 100;

        const hasPriceAction =
            absMove >= MIN_ABS_MOVE ||
            (range3m >= MIN_RANGE && bodyStrength >= MIN_BODY_STRENGTH);

        if (!hasPriceAction) {
            filterStats.weakPriceAction += 1;
            return null;
        }

        if (volumeRatio < MIN_VOLUME_RATIO && quoteVolumeRatio < MIN_QUOTE_VOLUME_RATIO) {
            filterStats.lowVolume += 1;
            return null;
        }

        if (tradesRatio < MIN_TRADES_RATIO) {
            filterStats.lowTrades += 1;
            return null;
        }

        if (range3m < MIN_RANGE) {
            filterStats.lowRange += 1;
            return null;
        }

        if (bodyStrength < MIN_BODY_STRENGTH) {
            filterStats.weakBody += 1;
            return null;
        }

        const strongMomentum =
            absMove >= 0.006 &&
            range3m >= 0.009 &&
            bodyStrength >= 0.5 &&
            tradesRatio >= 1.8 &&
            (volumeRatio >= 2.5 || quoteVolumeRatio >= 2.5);

        let direction = 'WATCH';

        if (priceChange3m > 0 && takerBuyRatio >= 0.52) {
            direction = 'LONG';
        } else if (priceChange3m < 0 && takerBuyRatio <= 0.48) {
            direction = 'SHORT';
        }

        const flowBias = (takerBuyRatio - 0.5) * 2;
        const quoteFlowBias = (takerBuyQuoteRatio - 0.5) * 2;

        const signedMoveScore = clamp(Math.abs(priceChange3m) / 0.015, 0, 1);
        const flowScore = clamp(Math.abs(flowBias), 0, 1);
        const quoteFlowScore = clamp(Math.abs(quoteFlowBias), 0, 1);

        let dirConf =
            signedMoveScore * 0.55 +
            flowScore * 0.25 +
            quoteFlowScore * 0.20;

        dirConf = round(dirConf, 4);

        const chartContext = analyzeChartContext(
            closedHistory,
            current,
            previousClose,
            direction
        );

        const {
            trendScore,
            breakoutScore,
            chopPenalty,
            wickPenalty,
            lateMovePenalty,
            volatilityScore,
            recentMove6,
            recentMove12,
            contextTags
        } = chartContext;

        if (wickPenalty >= 0.75 && bodyStrength < 0.42) {
            filterStats.highWick += 1;
            return null;
        }

        if (!strongMomentum && chopPenalty >= 0.82 && breakoutScore < 0.2) {
            filterStats.choppy += 1;
            return null;
        }

        if (!strongMomentum && lateMovePenalty >= 0.75 && direction === 'WATCH') {
            filterStats.lateWatch += 1;
            return null;
        }

        const tags = [];

        if (volumeRatio >= 3) tags.push('VOLUME_SPIKE');
        else if (volumeRatio >= MIN_VOLUME_RATIO) tags.push('VOLUME_WAKEUP');

        if (quoteVolumeRatio >= 3) tags.push('QUOTE_VOLUME_SPIKE');
        else if (quoteVolumeRatio >= MIN_QUOTE_VOLUME_RATIO) tags.push('QUOTE_VOLUME_WAKEUP');

        if (tradesRatio >= 2.5) tags.push('TRADE_COUNT_SPIKE');
        else if (tradesRatio >= MIN_TRADES_RATIO) tags.push('TRADE_COUNT_WAKEUP');

        if (absMove >= 0.008) tags.push('PRICE_IMPULSE_STRONG');
        else if (absMove >= MIN_ABS_MOVE) tags.push('PRICE_WAKEUP');

        if (range3m >= 0.012) tags.push('RANGE_EXPANSION_STRONG');
        else if (range3m >= MIN_RANGE) tags.push('RANGE_EXPANSION');

        if (bodyStrength >= 0.55) tags.push('STRONG_BODY');
        else if (bodyStrength >= MIN_BODY_STRENGTH) tags.push('BODY_CONFIRMED');

        if (takerBuyRatio >= 0.62 || takerBuyQuoteRatio >= 0.62) tags.push('BUY_FLOW_STRONG');
        if (takerBuyRatio <= 0.38 || takerBuyQuoteRatio <= 0.38) tags.push('SELL_FLOW_STRONG');

        if (strongMomentum) tags.push('STRONG_MOMENTUM');

        for (const tag of contextTags) {
            if (!tags.includes(tag)) tags.push(tag);
        }

        if (tags.length < MIN_TRIGGER_COUNT) {
            filterStats.weakPriceAction += 1;
            return null;
        }

        const moveScore = clamp(absMove / 0.025, 0, 1);
        const rangeScore = clamp(range3m / 0.035, 0, 1);
        const volumeScore = logScaleScore(volumeRatio, 15);
        const quoteVolumeScore = logScaleScore(quoteVolumeRatio, 15);
        const tradesScore = logScaleScore(tradesRatio, 10);
        const bodyScore = clamp(bodyStrength, 0, 1);
        const flowConfidence = clamp(Math.abs(takerBuyRatio - 0.5) * 2, 0, 1);
        const liquidityScore = clamp(
            Math.log10(Math.max(quoteVolume24h, MIN_24H_QUOTE_VOLUME) / MIN_24H_QUOTE_VOLUME + 1) / 2,
            0,
            1
        );

        const qualityBreakdown = {
            moveScore: round(moveScore, 4),
            rangeScore: round(rangeScore, 4),
            volumeScore: round(volumeScore, 4),
            quoteVolumeScore: round(quoteVolumeScore, 4),
            tradesScore: round(tradesScore, 4),
            bodyScore: round(bodyScore, 4),
            flowConfidence: round(flowConfidence, 4),
            liquidityScore: round(liquidityScore, 4),
            trendScore,
            breakoutScore,
            wickPenalty,
            chopPenalty,
            lateMovePenalty,
            volatilityScore
        };

        let score =
            moveScore * 0.22 +
            rangeScore * 0.14 +
            volumeScore * 0.14 +
            quoteVolumeScore * 0.10 +
            tradesScore * 0.10 +
            bodyScore * 0.08 +
            flowConfidence * 0.06 +
            breakoutScore * 0.08 +
            trendScore * 0.04 +
            liquidityScore * 0.04;

        score -= wickPenalty * 0.08;
        score -= chopPenalty * 0.08;
        score -= lateMovePenalty * 0.10;

        if (direction !== 'WATCH') {
            score += clamp(dirConf * 0.05, 0, 0.05);
        }

        score += clamp(volatilityScore * 0.03, 0, 0.03);

        if (strongMomentum) {
            score += 0.025;
        }

        score = clamp(score, 0, 1);

        if (score < OPPORTUNITY_THRESHOLD) {
            filterStats.lowScore += 1;
            return null;
        }

        const tier = score >= HOT_THRESHOLD ? 'HOT' : 'WATCH';

        return {
            symbol,
            direction,
            tier,
            score: round(score, 4),
            dirConf,

            priceChange3m: round(priceChange3m, 6),
            movePct: round(priceChange3m * 100, 3),
            range3m: round(range3m, 6),
            rangePct: round(range3m * 100, 3),
            bodyStrength: round(bodyStrength, 4),

            volumeRatio: round(volumeRatio, 4),
            quoteVolumeRatio: round(quoteVolumeRatio, 4),
            tradesRatio: round(tradesRatio, 4),

            // Backward-compatible fields.
            volumeZ: round(volumeRatio, 4),
            quoteVolumeZ: round(quoteVolumeRatio, 4),
            tradesZ: round(tradesRatio, 4),

            takerBuyRatio: round(takerBuyRatio, 4),
            takerBuyQuoteRatio: round(takerBuyQuoteRatio, 4),
            bookSpreadBps: round(bookSpreadBps, 3),
            quoteVolume24h: round(quoteVolume24h, 2),
            priceChange24h: round(priceChange24h, 6),
            projectionFactor: round(projectionFactor, 4),
            elapsedMs: Math.round(elapsedMs),

            trendScore,
            breakoutScore,
            chopPenalty,
            wickPenalty,
            lateMovePenalty,
            volatilityScore,
            recentMove6,
            recentMove12,
            contextTags,
            qualityBreakdown,

            strongMomentum,

            tags,
            reason: `${tier}_${direction}_OPPORTUNITY_3M_V24`,
            scanId,
            detectedAt: binanceNow(),
            localDetectedAt: Date.now()
        };
    } catch (error) {
        filterStats.errors += 1;
        return null;
    }
}


// ============================================================
// 9. REDIS PUBLISH / STATE
// ============================================================

async function publishSubscribe(symbol) {
    if (activeSubscribedSymbols.has(symbol)) return false;

    await pubClient.publish(CHANNELS.SUBSCRIPTIONS, JSON.stringify({
        action: 'SUBSCRIBE',
        symbol,
        client: 'radar'
    }));

    activeSubscribedSymbols.add(symbol);
    return true;
}

async function publishUnsubscribe(symbol) {
    if (!activeSubscribedSymbols.has(symbol)) return false;

    await pubClient.publish(CHANNELS.SUBSCRIPTIONS, JSON.stringify({
        action: 'UNSUBSCRIBE',
        symbol,
        client: 'radar'
    }));

    activeSubscribedSymbols.delete(symbol);
    return true;
}

async function publishCandidate(candidate) {
    await pubClient.publish(CHANNELS.CANDIDATES, JSON.stringify(candidate));
}

async function removeSlot(symbol, reason = 'EXPIRED') {
    const slot = currentSlots.get(symbol);
    if (!slot) return false;

    currentSlots.delete(symbol);
    recentlyRemovedSymbols.set(symbol, Date.now());

    try {
        await dataClient.hdel(CHANNELS.MACRO_SCORES, symbol);
        await publishUnsubscribe(symbol);
    } catch (error) {
        console.error(`❌ [RADAR] removeSlot Redis/publish error ${symbol}:`, error.message);
    }

    console.log(`❌ [RADAR] Loại ${symbol} ${slot.direction} | score=${slot.score} | reason=${reason}`);
    return true;
}

async function syncRedisLists(activeCandidates) {
    const hot = activeCandidates.filter(c => c.tier === 'HOT');
    const watch = activeCandidates.filter(c => c.tier === 'WATCH');

    const pipeline = dataClient.pipeline();

    pipeline.set(CHANNELS.HOTLIST, JSON.stringify(hot), 'EX', RADAR_LIST_TTL_SEC);
    pipeline.set(CHANNELS.WATCHLIST, JSON.stringify(watch), 'EX', RADAR_LIST_TTL_SEC);

    for (const [symbol, slot] of currentSlots.entries()) {
        pipeline.hset(CHANNELS.MACRO_SCORES, symbol, slot.score);
    }

    await pipeline.exec();
}

async function pruneStaleSlots(filterStats) {
    const now = Date.now();
    const staleSymbols = [];

    for (const [symbol, slot] of currentSlots.entries()) {
        const updatedAt = safeNumber(slot.updatedAt, slot.addedAt || now);
        if (now - updatedAt >= MAX_SLOT_STALE_MS) {
            staleSymbols.push(symbol);
        }
    }

    for (const symbol of staleSymbols) {
        const removed = await removeSlot(symbol, 'STALE_SLOT');
        if (removed) filterStats.staleRemoved += 1;
    }
}

async function enforceActiveSlotLimit(filterStats) {
    if (currentSlots.size <= MAX_ACTIVE_SLOTS) return 0;

    const now = Date.now();

    const removable = Array.from(currentSlots.values())
        .map(slot => ({
            symbol: slot.symbol,
            score: safeNumber(slot.score),
            ageMs: now - safeNumber(slot.addedAt, now)
        }))
        .filter(item => item.ageMs >= MIN_FORCE_KEEP_MS)
        .sort((a, b) => a.score - b.score);

    let removed = 0;

    while (currentSlots.size > MAX_ACTIVE_SLOTS && removable.length > 0) {
        const item = removable.shift();
        const didRemove = await removeSlot(item.symbol, 'ACTIVE_SLOTS_LIMIT');
        if (didRemove) removed += 1;
    }

    filterStats.forceRemoved += removed;

    if (currentSlots.size > MAX_ACTIVE_SLOTS) {
        console.warn(
            `⚠️ [RADAR] currentSlots vẫn vượt giới hạn ${currentSlots.size}/${MAX_ACTIVE_SLOTS} ` +
            `vì các slot còn lại mới dưới ${Math.round(MIN_FORCE_KEEP_MS / 1000)}s`
        );
    }

    return removed;
}


// ============================================================
// 10. MAIN SCAN
// ============================================================

async function scanRadar() {
    if (isScanning) {
        console.warn('⚠️ [RADAR] Bỏ qua scan mới vì scan trước chưa xong');
        return;
    }

    isScanning = true;
    const startedAt = Date.now();
    const filterStats = createFilterStats();
    const scanId = `radar-${++scanSeq}-${startedAt}`;

    try {
        cleanupRemovedCache();
        await syncBinanceTime(false);

        console.log('');
        console.log(
            `📡 [RADAR] Profit Opportunity scan ${RADAR_INTERVAL} | ` +
            `top=${MAX_RADAR_SLOTS} | maxActive=${MAX_ACTIVE_SLOTS} | ` +
            `threshold=${OPPORTUNITY_THRESHOLD} | hot=${HOT_THRESHOLD} | scanId=${scanId}`
        );

        const symbols = await fetchTradableSymbols();
        if (symbols.length === 0) {
            console.warn('⚠️ [RADAR] Không có symbol để quét');
            return;
        }

        const tickerMap = await fetch24hTickerMap();
        const bookTickerMap = await fetchBookTickerMap();

        if (tickerMap.size === 0) {
            console.warn('⚠️ [RADAR] Không có ticker 24h');
            return;
        }

        if (bookTickerMap.size === 0) {
            console.warn('⚠️ [RADAR] Không có bookTicker, bỏ scan để tránh chọn coin mù spread');
            return;
        }

        const liquidSymbols = [];

        for (const symbol of symbols) {
            const ticker = tickerMap.get(symbol);

            if (!ticker) {
                filterStats.missingTicker += 1;
                continue;
            }

            const quoteVolume = safeNumber(ticker.quoteVolume);

            if (quoteVolume < MIN_24H_QUOTE_VOLUME) {
                filterStats.lowLiquidity += 1;
                continue;
            }

            liquidSymbols.push(symbol);
        }

        console.log(`🔎 [RADAR] Sau lọc thanh khoản >= ${MIN_24H_QUOTE_VOLUME}: ${liquidSymbols.length}/${symbols.length} symbols`);

        const candidates = [];

        for (let i = 0; i < liquidSymbols.length; i += BATCH_SIZE) {
            const batch = liquidSymbols.slice(i, i + BATCH_SIZE);

            const results = await Promise.all(batch.map(symbol => {
                const ticker24h = tickerMap.get(symbol);
                return analyzeSymbol(symbol, ticker24h, bookTickerMap, filterStats, scanId);
            }));

            for (const result of results) {
                if (result) candidates.push(result);
            }

            if (i + BATCH_SIZE < liquidSymbols.length) {
                await sleep(BATCH_DELAY_MS);
            }
        }

        candidates.sort((a, b) => b.score - a.score);

        const topCandidates = candidates.slice(0, MAX_RADAR_SLOTS);
        const acceptedSymbols = new Set();

        let added = 0;
        let updated = 0;
        let published = 0;

        for (const candidate of topCandidates) {
            const old = currentSlots.get(candidate.symbol);

            if (!old && isInResubscribeCooldown(candidate.symbol)) {
                filterStats.cooldownSkip += 1;
                continue;
            }

            if (!old) {
                const slot = {
                    ...candidate,
                    addedAt: Date.now(),
                    updatedAt: Date.now(),
                    lastPublishedAt: Date.now()
                };

                currentSlots.set(candidate.symbol, slot);
                acceptedSymbols.add(candidate.symbol);

                try {
                    await publishSubscribe(candidate.symbol);
                    await publishCandidate(slot);
                    published += 1;
                } catch (error) {
                    console.error(`❌ [RADAR] Publish new candidate lỗi ${candidate.symbol}:`, error.message);
                }

                added += 1;

                console.log(
                    `➕ [RADAR] ${candidate.tier} ${candidate.symbol} ${candidate.direction} | ` +
                    `score=${candidate.score} | dir=${candidate.dirConf} | ` +
                    `move=${candidate.movePct}% | range=${candidate.rangePct}% | ` +
                    `vol=${candidate.volumeRatio} | trades=${candidate.tradesRatio} | ` +
                    `trend=${candidate.trendScore} | brk=${candidate.breakoutScore} | ` +
                    `chop=${candidate.chopPenalty} | wick=${candidate.wickPenalty} | ` +
                    `late=${candidate.lateMovePenalty} | spread=${candidate.bookSpreadBps}bps | ` +
                    `strong=${candidate.strongMomentum ? 'Y' : 'N'} | ` +
                    `tags=${candidate.tags.slice(0, 6).join(',')}`
                );
            } else {
                const shouldRepublish = shouldRepublishCandidate(old, candidate);

                const slot = {
                    ...old,
                    ...candidate,
                    addedAt: old.addedAt,
                    updatedAt: Date.now(),
                    lastPublishedAt: shouldRepublish ? Date.now() : old.lastPublishedAt
                };

                currentSlots.set(candidate.symbol, slot);
                acceptedSymbols.add(candidate.symbol);

                updated += 1;

                if (shouldRepublish) {
                    try {
                        await publishCandidate(slot);
                        published += 1;
                    } catch (error) {
                        console.error(`❌ [RADAR] Republish candidate lỗi ${candidate.symbol}:`, error.message);
                    }

                    console.log(
                        `🔁 [RADAR] UPDATE ${candidate.tier} ${candidate.symbol} ${candidate.direction} | ` +
                        `score=${candidate.score} | old=${old.score} | ` +
                        `move=${candidate.movePct}% | vol=${candidate.volumeRatio} | ` +
                        `tags=${candidate.tags.slice(0, 6).join(',')}`
                    );
                }
            }
        }

        const toRemove = [];

        for (const [symbol, slot] of currentSlots.entries()) {
            if (!acceptedSymbols.has(symbol)) {
                const ageMs = Date.now() - safeNumber(slot.addedAt, Date.now());

                if (ageMs >= MIN_HOLD_MS) {
                    toRemove.push(symbol);
                } else {
                    const remainMin = Math.ceil((MIN_HOLD_MS - ageMs) / 60000);
                    console.log(`🔒 [STICKY] Giữ ${symbol} ${slot.direction} | còn ${remainMin} phút`);
                }
            }
        }

        for (const symbol of toRemove) {
            await removeSlot(symbol, 'NOT_IN_TOP_AND_HOLD_EXPIRED');
        }

        await pruneStaleSlots(filterStats);
        const forcedRemoved = await enforceActiveSlotLimit(filterStats);

        const activeCandidates = Array.from(currentSlots.values())
            .sort((a, b) => b.score - a.score);

        await syncRedisLists(activeCandidates);

        const hotCount = activeCandidates.filter(c => c.tier === 'HOT').length;
        const watchCount = activeCandidates.filter(c => c.tier === 'WATCH').length;

        const elapsedSec = round((Date.now() - startedAt) / 1000, 2);

        console.log(
            `[FILTER] missingTicker=${filterStats.missingTicker} | missingBook=${filterStats.missingBookTicker} | ` +
            `lowLiq=${filterStats.lowLiquidity} | highSpread=${filterStats.highSpread} | ` +
            `early=${filterStats.candleTooEarly} | lowVol=${filterStats.lowVolume} | ` +
            `lowTrades=${filterStats.lowTrades} | lowRange=${filterStats.lowRange} | ` +
            `weakBody=${filterStats.weakBody} | highWick=${filterStats.highWick} | ` +
            `choppy=${filterStats.choppy} | lateWatch=${filterStats.lateWatch} | ` +
            `lowScore=${filterStats.lowScore} | cooldown=${filterStats.cooldownSkip} | ` +
            `stale=${filterStats.staleRemoved} | force=${filterStats.forceRemoved} | errors=${filterStats.errors}`
        );

        console.log(
            `📊 [RADAR] Candidates=${candidates.length} | Top=${topCandidates.length} | ` +
            `Added=${added} | Updated=${updated} | Published=${published} | ` +
            `CooldownSkip=${filterStats.cooldownSkip} | ForceRemoved=${forcedRemoved} | ` +
            `Hot=${hotCount} | Watch=${watchCount} | Slots=${currentSlots.size} | ` +
            `Time=${elapsedSec}s | TimeOffset=${binanceTimeState.offsetMs}ms`
        );

        if (Date.now() - startedAt > SCAN_WARN_DURATION_MS) {
            console.warn(`⚠️ [RADAR] Scan kéo dài ${elapsedSec}s, cần xem lại số symbol/batch/API latency`);
        }
    } catch (error) {
        console.error('❌ [RADAR] scanRadar error:', error.message);
    } finally {
        isScanning = false;
    }
}


// ============================================================
// 11. ALIGNED SCHEDULER
// ============================================================

function getNextAlignedScanDelay() {
    const now = binanceNow();

    let next = Math.floor(now / RADAR_INTERVAL_MS) * RADAR_INTERVAL_MS + SCAN_OFFSET_MS;
    if (next <= now) next += RADAR_INTERVAL_MS;

    return Math.max(1000, next - now);
}

function scheduleNextScan() {
    if (nextScanTimer) clearTimeout(nextScanTimer);

    const delay = getNextAlignedScanDelay();
    const nextAt = new Date(Date.now() + delay).toISOString();

    console.log(`⏱️ [RADAR] Scan kế tiếp tại ${nextAt} | sau ${round(delay / 1000, 2)}s`);

    nextScanTimer = setTimeout(async () => {
        await scanRadar();
        scheduleNextScan();
    }, delay);
}


// ============================================================
// 12. STARTUP / SHUTDOWN
// ============================================================

async function startup() {
    validateConfig();

    console.log('👁️ RadarManager V24 - Production-Stable Profit Opportunity Radar 3M khởi động!');
    console.log(
        `⚙️ [CONFIG] interval=${RADAR_INTERVAL}, top=${MAX_RADAR_SLOTS}, maxActive=${MAX_ACTIVE_SLOTS}, ` +
        `offset=${SCAN_OFFSET_MS / 1000}s, minElapsed=${MIN_CANDLE_ELAPSED_MS / 1000}s, ` +
        `excludeMajors=ON, min24hVol=${MIN_24H_QUOTE_VOLUME}, ` +
        `threshold=${OPPORTUNITY_THRESHOLD}, hot=${HOT_THRESHOLD}, ` +
        `maxProjection=${MAX_PROJECTION_FACTOR}x, batch=${BATCH_SIZE}, delay=${BATCH_DELAY_MS}ms`
    );

    await syncBinanceTime(true);

    await scanRadar();
    scheduleNextScan();
}

async function shutdown() {
    console.log('');
    console.log('🛑 [RADAR] Đang tắt RadarManager...');

    if (nextScanTimer) clearTimeout(nextScanTimer);

    try {
        const symbols = Array.from(activeSubscribedSymbols);

        for (const symbol of symbols) {
            await publishUnsubscribe(symbol);
        }

        await pubClient.quit();
        await dataClient.quit();
    } catch (error) {
        console.error('❌ [RADAR] Shutdown error:', error.message);
    }

    process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

startup().catch(error => {
    console.error('❌ [RADAR] Startup error:', error.message);
    process.exit(1);
});
