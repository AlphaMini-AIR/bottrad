const axios = require('axios');
const Redis = require('ioredis');
const fs = require('fs');

// ============================================================
// RADAR MANAGER V20 - OPPORTUNITY RADAR FUTURES 15M-
// ------------------------------------------------------------
// Mục tiêu chính:
// 1. Quét toàn bộ Binance Futures USDT-M Perpetual đang trading.
// 2. Không trực tiếp vào lệnh, không quyết định trade cuối.
// 3. Chỉ lọc danh sách coin có dấu hiệu bất thường / tiềm năng.
// 4. Bắt rộng cơ hội hơn bản Impulse Radar cũ.
// 5. Hỗ trợ direction: LONG / SHORT / WATCH.
// 6. Không ép direction quá sớm, tránh bỏ sót coin tốt.
// 7. Dùng trigger-count + opportunity-score thay vì hard filter quá cứng.
// 8. Có tag tín hiệu để các tầng sau phân tích sâu hơn.
// 9. Giữ nguyên luồng Redis cũ:
//    - radar:candidates
//    - system:subscriptions
//    - macro:scores
// 10. Có thêm channel mới không bắt buộc:
//    - radar:watchlist
//    - radar:hotlist
//    - radar:signals
// ============================================================


// ============================================================
// 1. HÀM TIỆN ÍCH CƠ BẢN
// ============================================================
function intervalToMs(interval) {
    const text = String(interval || '3m');
    const unit = text.slice(-1);
    const value = Number(text.slice(0, -1));

    if (!Number.isFinite(value) || value <= 0) return 3 * 60 * 1000;
    if (unit === 'm') return value * 60 * 1000;
    if (unit === 'h') return value * 60 * 60 * 1000;
    if (unit === 'd') return value * 24 * 60 * 60 * 1000;

    return 3 * 60 * 1000;
}

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

function average(values) {
    const arr = values.filter(v => Number.isFinite(v));
    if (arr.length === 0) return 0;
    return arr.reduce((sum, v) => sum + v, 0) / arr.length;
}

function std(values) {
    const arr = values.filter(v => Number.isFinite(v));
    if (arr.length < 2) return 0;

    const avg = average(arr);
    const variance = average(arr.map(v => (v - avg) ** 2));
    return Math.sqrt(variance);
}

function pctChange(from, to) {
    if (!Number.isFinite(from) || !Number.isFinite(to) || from <= 0) return 0;
    return (to - from) / from;
}

// Với radar tầng đầu, nên quản lý slot theo SYMBOL, không theo SYMBOL:DIRECTION.
// Lý do: radar chỉ cần biết coin nào đáng theo dõi, direction sau có thể đổi từ WATCH -> LONG/SHORT.
function buildSlotKey(symbol) {
    return symbol;
}

// Jitter chỉ dùng cho retry/backoff network, không dùng để chấm điểm trading.
function jitter(ms, ratio = 0.2) {
    const spread = ms * ratio;
    return Math.round(ms - spread + Math.random() * spread * 2);
}


// ============================================================
// 2. ĐỌC CẤU HÌNH
// ============================================================
let config;

try {
    config = JSON.parse(fs.readFileSync('./system_config.json', 'utf8'));
} catch (e) {
    config = {
        REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379',
        CHANNELS: {
            CANDIDATES: 'radar:candidates',
            WATCHLIST: 'radar:watchlist',
            HOTLIST: 'radar:hotlist',
            SIGNALS: 'radar:signals',
            MACRO_SCORES: 'macro:scores',
            SUBSCRIPTIONS: 'system:subscriptions'
        },

        // Số coin tối đa ưu tiên cao trong mỗi vòng quét.
        MAX_RADAR_SLOTS: 40,

        // Hard cap tổng slot đang giữ, bao gồm cả sticky slot.
        MAX_TOTAL_RADAR_SLOTS: 60,

        // Radar này là opportunity scanner, nên threshold rộng hơn bản cũ.
        OPPORTUNITY_THRESHOLD: 0.50,
        HOT_THRESHOLD: 0.68,

        // Giữ tương thích macro threshold cũ.
        MACRO_THRESHOLD: 0.50,

        // Khung scan chính.
        IMPULSE_INTERVAL: '3m',
        IMPULSE_KLINE_LIMIT: 31,

        // Lọc thanh khoản 24h.
        MIN_24H_QUOTE_VOLUME: 1000000,

        // Bộ lọc opportunity mềm.
        MIN_TRIGGER_COUNT: 2,
        MIN_VOLUME_Z: 1.5,
        MIN_QUOTE_VOLUME_Z: 1.4,
        MIN_TRADES_Z: 1.4,
        MIN_PRICE_MOVE_3M: 0.0035,
        MIN_RANGE_3M: 0.005,
        MIN_BODY_STRENGTH: 0.25,

        // Direction chỉ là gợi ý cho tầng sau.
        LONG_TAKER_BUY_RATIO: 0.53,
        SHORT_TAKER_BUY_RATIO: 0.47,

        // Projected volume cho nến chưa đóng.
        MIN_CANDLE_ELAPSED_MS: 15000,
        MAX_CANDLE_ELAPSED_RATIO: 1,
        MAX_PROJECTION_FACTOR: 6,

        // Chống spread xấu.
        MAX_SPREAD_BPS: 12,

        // API safety.
        BATCH_SIZE: 5,
        BATCH_DELAY_MS: 250,
        REQUEST_TIMEOUT_MS: 10000,
        MAX_RETRIES: 2,
        RETRY_BASE_DELAY_MS: 500,
        RETRY_RATE_LIMIT_DELAY_MS: 5000,

        // Cache.
        SYMBOLS_CACHE_MS: 15 * 60 * 1000,
        TICKER_CACHE_MS: 10 * 1000,
        BOOK_TICKER_CACHE_MS: 3000,
        MARKET_CONTEXT_CACHE_MS: 15000,

        // Scheduler.
        SCAN_OFFSET_MS: 8000,

        // Sticky slot: giữ symbol một thời gian để tầng sau kịp phân tích.
        MIN_HOLD_MS: 8 * 60 * 1000,

        // Publish update khi score thay đổi đáng kể.
        REPUBLISH_SCORE_DELTA: 0.04,
        REPUBLISH_MIN_INTERVAL_MS: 60 * 1000
    };
}


// ============================================================
// 3. DEFAULT CONFIG ĐỂ TƯƠNG THÍCH FILE system_config.json CŨ
// ============================================================
config.REDIS_URL = config.REDIS_URL || process.env.REDIS_URL || 'redis://localhost:6379';

config.CHANNELS = config.CHANNELS || {};
config.CHANNELS.CANDIDATES = config.CHANNELS.CANDIDATES || 'radar:candidates';
config.CHANNELS.WATCHLIST = config.CHANNELS.WATCHLIST || 'radar:watchlist';
config.CHANNELS.HOTLIST = config.CHANNELS.HOTLIST || 'radar:hotlist';
config.CHANNELS.SIGNALS = config.CHANNELS.SIGNALS || 'radar:signals';
config.CHANNELS.MACRO_SCORES = config.CHANNELS.MACRO_SCORES || 'macro:scores';
config.CHANNELS.SUBSCRIPTIONS = config.CHANNELS.SUBSCRIPTIONS || 'system:subscriptions';

config.MAX_RADAR_SLOTS = config.MAX_RADAR_SLOTS ?? 40;
config.MAX_TOTAL_RADAR_SLOTS = config.MAX_TOTAL_RADAR_SLOTS ?? Math.floor(config.MAX_RADAR_SLOTS * 1.5);

config.OPPORTUNITY_THRESHOLD = config.OPPORTUNITY_THRESHOLD ?? 0.50;
config.HOT_THRESHOLD = config.HOT_THRESHOLD ?? 0.68;
config.MACRO_THRESHOLD = config.MACRO_THRESHOLD ?? config.OPPORTUNITY_THRESHOLD;

config.IMPULSE_INTERVAL = config.IMPULSE_INTERVAL || '3m';
config.IMPULSE_INTERVAL_MS = config.IMPULSE_INTERVAL_MS ?? intervalToMs(config.IMPULSE_INTERVAL);
config.IMPULSE_KLINE_LIMIT = config.IMPULSE_KLINE_LIMIT ?? 31;

config.MIN_24H_QUOTE_VOLUME = config.MIN_24H_QUOTE_VOLUME ?? 1000000;

config.MIN_TRIGGER_COUNT = config.MIN_TRIGGER_COUNT ?? 2;
config.MIN_VOLUME_Z = config.MIN_VOLUME_Z ?? 1.5;
config.MIN_QUOTE_VOLUME_Z = config.MIN_QUOTE_VOLUME_Z ?? 1.4;
config.MIN_TRADES_Z = config.MIN_TRADES_Z ?? 1.4;
config.MIN_PRICE_MOVE_3M = config.MIN_PRICE_MOVE_3M ?? 0.0035;
config.MIN_RANGE_3M = config.MIN_RANGE_3M ?? 0.005;
config.MIN_BODY_STRENGTH = config.MIN_BODY_STRENGTH ?? 0.25;

config.LONG_TAKER_BUY_RATIO = config.LONG_TAKER_BUY_RATIO ?? 0.53;
config.SHORT_TAKER_BUY_RATIO = config.SHORT_TAKER_BUY_RATIO ?? 0.47;

config.MIN_CANDLE_ELAPSED_MS = config.MIN_CANDLE_ELAPSED_MS ?? 15000;
config.MAX_CANDLE_ELAPSED_RATIO = config.MAX_CANDLE_ELAPSED_RATIO ?? 1;
config.MAX_PROJECTION_FACTOR = config.MAX_PROJECTION_FACTOR ?? 6;

config.MAX_SPREAD_BPS = config.MAX_SPREAD_BPS ?? 12;

config.BATCH_SIZE = config.BATCH_SIZE ?? 5;
config.BATCH_DELAY_MS = config.BATCH_DELAY_MS ?? 250;
config.REQUEST_TIMEOUT_MS = config.REQUEST_TIMEOUT_MS ?? 10000;
config.MAX_RETRIES = config.MAX_RETRIES ?? 2;
config.RETRY_BASE_DELAY_MS = config.RETRY_BASE_DELAY_MS ?? 500;
config.RETRY_RATE_LIMIT_DELAY_MS = config.RETRY_RATE_LIMIT_DELAY_MS ?? 5000;

config.SYMBOLS_CACHE_MS = config.SYMBOLS_CACHE_MS ?? 15 * 60 * 1000;
config.TICKER_CACHE_MS = config.TICKER_CACHE_MS ?? 10 * 1000;
config.BOOK_TICKER_CACHE_MS = config.BOOK_TICKER_CACHE_MS ?? 3000;
config.MARKET_CONTEXT_CACHE_MS = config.MARKET_CONTEXT_CACHE_MS ?? 15000;

config.SCAN_OFFSET_MS = config.SCAN_OFFSET_MS ?? 8000;
config.MIN_HOLD_MS = config.MIN_HOLD_MS ?? 8 * 60 * 1000;

config.REPUBLISH_SCORE_DELTA = config.REPUBLISH_SCORE_DELTA ?? 0.04;
config.REPUBLISH_MIN_INTERVAL_MS = config.REPUBLISH_MIN_INTERVAL_MS ?? 60 * 1000;

const BINANCE_FAPI_BASE = 'https://fapi.binance.com';

const pubClient = new Redis(config.REDIS_URL);
const dataClient = new Redis(config.REDIS_URL);


// ============================================================
// 4. BỘ NHỚ TRẠNG THÁI
// ============================================================
let currentRadarSlots = new Map();

// Giữ danh sách symbol đã gửi SUBSCRIBE để không subscribe trùng.
let activeSubscribedSymbols = new Set();

// Cache score mạnh nhất theo symbol để ghi macro:scores.
const macroScoreCache = new Map();

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

let marketContextCache = {
    updatedAt: 0,
    context: {
        btcReturn5m: 0,
        ethReturn5m: 0,
        marketBias: 'NEUTRAL',
        volatilityMode: 'NORMAL'
    }
};

let isScanning = false;
let nextScanTimer = null;


// ============================================================
// 5. HTTP CLIENT CÓ RETRY VÀ BACKOFF
// ============================================================
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
    return [
        'ECONNRESET',
        'ETIMEDOUT',
        'ECONNABORTED',
        'EAI_AGAIN',
        'ENOTFOUND'
    ].includes(code);
}

async function binanceGet(path, params = {}, options = {}) {
    const maxRetries = options.maxRetries ?? config.MAX_RETRIES;
    const timeout = options.timeout ?? config.REQUEST_TIMEOUT_MS;
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
                ? config.RETRY_RATE_LIMIT_DELAY_MS
                : config.RETRY_BASE_DELAY_MS * (attempt + 1);

            const status = error && error.response ? error.response.status : 'NA';
            const code = error && error.code ? error.code : 'NA';

            console.warn(
                `⚠️ [BINANCE] Retry ${attempt + 1}/${maxRetries} path=${path} status=${status} code=${code} delay=${delay}ms`
            );

            await sleep(jitter(delay));
        }
    }

    throw lastError;
}


// ============================================================
// 6. LẤY 100% SYMBOL USDT-M PERPETUAL ĐANG TRADING
// ============================================================
async function fetchTradableUSDTPerpetualSymbols() {
    const now = Date.now();

    if (
        tradableSymbolsCache.symbols.length > 0 &&
        now - tradableSymbolsCache.updatedAt < config.SYMBOLS_CACHE_MS
    ) {
        return tradableSymbolsCache.symbols;
    }

    try {
        const response = await binanceGet('/fapi/v1/exchangeInfo');

        const symbols = response.data.symbols
            .filter(item => item.status === 'TRADING')
            .filter(item => item.contractType === 'PERPETUAL')
            .filter(item => item.quoteAsset === 'USDT')
            .map(item => item.symbol);

        tradableSymbolsCache = {
            updatedAt: now,
            symbols
        };

        console.log(`🌐 [RADAR] USDT-M Perpetual đang trading: ${symbols.length} symbols`);
        return symbols;
    } catch (error) {
        console.error('❌ Lỗi lấy exchangeInfo:', error.message);

        if (tradableSymbolsCache.symbols.length > 0) {
            console.warn('⚠️ [RADAR] Dùng lại cache symbol cũ do exchangeInfo lỗi');
            return tradableSymbolsCache.symbols;
        }

        return [];
    }
}


// ============================================================
// 7. LẤY 24H TICKER TOÀN THỊ TRƯỜNG
// ============================================================
async function fetch24hTickerMap() {
    const now = Date.now();

    if (
        tickerCache.map.size > 0 &&
        now - tickerCache.updatedAt < config.TICKER_CACHE_MS
    ) {
        return tickerCache.map;
    }

    try {
        const response = await binanceGet('/fapi/v1/ticker/24hr');
        const tickerMap = new Map();

        for (const ticker of response.data) {
            tickerMap.set(ticker.symbol, ticker);
        }

        tickerCache = {
            updatedAt: now,
            map: tickerMap
        };

        return tickerMap;
    } catch (error) {
        console.error('❌ Lỗi lấy ticker 24h:', error.message);

        if (tickerCache.map.size > 0) {
            console.warn('⚠️ [RADAR] Dùng lại cache ticker cũ do ticker API lỗi');
            return tickerCache.map;
        }

        return new Map();
    }
}


// ============================================================
// 8. LẤY BOOK TICKER TOÀN THỊ TRƯỜNG ĐỂ ĐO SPREAD
// ------------------------------------------------------------
// Spread quá rộng sẽ làm bot scalping dễ bị ăn phí + trượt giá.
// Radar không vào lệnh nhưng vẫn nên loại bớt coin spread xấu.
// ============================================================
async function fetchBookTickerMap() {
    const now = Date.now();

    if (
        bookTickerCache.map.size > 0 &&
        now - bookTickerCache.updatedAt < config.BOOK_TICKER_CACHE_MS
    ) {
        return bookTickerCache.map;
    }

    try {
        const response = await binanceGet('/fapi/v1/ticker/bookTicker');
        const bookMap = new Map();

        for (const item of response.data) {
            bookMap.set(item.symbol, item);
        }

        bookTickerCache = {
            updatedAt: now,
            map: bookMap
        };

        return bookMap;
    } catch (error) {
        console.error('❌ Lỗi lấy bookTicker:', error.message);

        if (bookTickerCache.map.size > 0) {
            console.warn('⚠️ [RADAR] Dùng lại cache bookTicker cũ do API lỗi');
            return bookTickerCache.map;
        }

        return new Map();
    }
}


// ============================================================
// 9. MARKET CONTEXT BTC / ETH
// ------------------------------------------------------------
// Radar không trade theo BTC, nhưng context giúp score tốt hơn.
// Nếu toàn market đang biến động mạnh, coin alt dễ có cơ hội hơn.
// Nếu BTC/ETH cực xấu, vẫn publish nhưng gắn context cho tầng sau.
// ============================================================
async function fetchMarketContext() {
    const now = Date.now();

    if (now - marketContextCache.updatedAt < config.MARKET_CONTEXT_CACHE_MS) {
        return marketContextCache.context;
    }

    try {
        const [btcResponse, ethResponse] = await Promise.all([
            binanceGet('/fapi/v1/klines', {
                symbol: 'BTCUSDT',
                interval: '1m',
                limit: 6
            }, { maxRetries: 1 }),
            binanceGet('/fapi/v1/klines', {
                symbol: 'ETHUSDT',
                interval: '1m',
                limit: 6
            }, { maxRetries: 1 })
        ]);

        const btcKlines = btcResponse.data || [];
        const ethKlines = ethResponse.data || [];

        const btcFirstClose = safeNumber(btcKlines[0]?.[4]);
        const btcLastClose = safeNumber(btcKlines[btcKlines.length - 1]?.[4]);

        const ethFirstClose = safeNumber(ethKlines[0]?.[4]);
        const ethLastClose = safeNumber(ethKlines[ethKlines.length - 1]?.[4]);

        const btcReturn5m = pctChange(btcFirstClose, btcLastClose);
        const ethReturn5m = pctChange(ethFirstClose, ethLastClose);

        const absBtc = Math.abs(btcReturn5m);
        const absEth = Math.abs(ethReturn5m);

        let marketBias = 'NEUTRAL';

        if (btcReturn5m > 0.0025 && ethReturn5m > 0.0025) {
            marketBias = 'RISK_ON';
        } else if (btcReturn5m < -0.0025 && ethReturn5m < -0.0025) {
            marketBias = 'RISK_OFF';
        }

        let volatilityMode = 'NORMAL';

        if (absBtc >= 0.006 || absEth >= 0.008) {
            volatilityMode = 'HIGH';
        } else if (absBtc <= 0.001 && absEth <= 0.0015) {
            volatilityMode = 'LOW';
        }

        const context = {
            btcReturn5m: round(btcReturn5m, 6),
            ethReturn5m: round(ethReturn5m, 6),
            marketBias,
            volatilityMode
        };

        marketContextCache = {
            updatedAt: now,
            context
        };

        return context;
    } catch (error) {
        console.warn('⚠️ [RADAR] Không lấy được market context:', error.message);
        return marketContextCache.context;
    }
}


// ============================================================
// 10. TÍNH SPREAD BPS
// ============================================================
function calculateSpreadBps(bookTicker) {
    if (!bookTicker) return null;

    const bid = safeNumber(bookTicker.bidPrice);
    const ask = safeNumber(bookTicker.askPrice);

    if (bid <= 0 || ask <= 0 || ask < bid) return null;

    const mid = (bid + ask) / 2;
    if (mid <= 0) return null;

    return ((ask - bid) / mid) * 10000;
}


// ============================================================
// 11. BUILD TAG TÍN HIỆU
// ------------------------------------------------------------
// Tags giúp tầng sau biết coin đang bất thường kiểu gì.
// Đây là điểm quan trọng hơn direction trong radar tầng đầu.
// ============================================================
function buildSignalTags(metrics) {
    const tags = [];

    if (metrics.volumeZ >= 3) tags.push('VOLUME_SPIKE');
    else if (metrics.volumeZ >= config.MIN_VOLUME_Z) tags.push('VOLUME_WAKEUP');

    if (metrics.quoteVolumeZ >= 3) tags.push('QUOTE_VOLUME_SPIKE');
    else if (metrics.quoteVolumeZ >= config.MIN_QUOTE_VOLUME_Z) tags.push('QUOTE_VOLUME_WAKEUP');

    if (metrics.tradeCountZ >= 2.5) tags.push('TRADE_COUNT_SPIKE');
    else if (metrics.tradeCountZ >= config.MIN_TRADES_Z) tags.push('TRADE_COUNT_WAKEUP');

    if (metrics.absPriceChange3m >= 0.01) tags.push('PRICE_IMPULSE_STRONG');
    else if (metrics.absPriceChange3m >= config.MIN_PRICE_MOVE_3M) tags.push('PRICE_WAKEUP');

    if (metrics.range3m >= 0.012) tags.push('RANGE_EXPANSION_STRONG');
    else if (metrics.range3m >= config.MIN_RANGE_3M) tags.push('RANGE_EXPANSION');

    if (metrics.bodyStrength >= 0.6) tags.push('STRONG_BODY');
    else if (metrics.bodyStrength >= config.MIN_BODY_STRENGTH) tags.push('BODY_CONFIRMED');

    if (metrics.upperWickRatio >= 0.45 && metrics.range3m >= config.MIN_RANGE_3M) {
        tags.push('UPPER_WICK_ACTIVITY');
    }

    if (metrics.lowerWickRatio >= 0.45 && metrics.range3m >= config.MIN_RANGE_3M) {
        tags.push('LOWER_WICK_ACTIVITY');
    }

    if (metrics.takerBuyRatio >= 0.58) tags.push('BUY_FLOW_STRONG');
    else if (metrics.takerBuyRatio >= config.LONG_TAKER_BUY_RATIO) tags.push('BUY_FLOW');

    if (metrics.takerBuyRatio <= 0.42) tags.push('SELL_FLOW_STRONG');
    else if (metrics.takerBuyRatio <= config.SHORT_TAKER_BUY_RATIO) tags.push('SELL_FLOW');

    if (metrics.projectionFactor > 1.5) tags.push('PROJECTED_VOLUME');

    if (metrics.spreadBps !== null && metrics.spreadBps <= 4) tags.push('TIGHT_SPREAD');
    if (metrics.spreadBps !== null && metrics.spreadBps > config.MAX_SPREAD_BPS) tags.push('WIDE_SPREAD');

    if (metrics.marketContext.marketBias === 'RISK_ON') tags.push('MARKET_RISK_ON');
    if (metrics.marketContext.marketBias === 'RISK_OFF') tags.push('MARKET_RISK_OFF');
    if (metrics.marketContext.volatilityMode === 'HIGH') tags.push('MARKET_VOLATILE');

    return tags;
}


// ============================================================
// 12. GỢI Ý DIRECTION NHẸ, KHÔNG ÉP QUÁ SỚM
// ------------------------------------------------------------
// Radar chỉ đưa suggestedDirection.
// Tầng sau mới quyết định hướng thật.
// ============================================================
function inferSuggestedDirection(metrics) {
    let suggestedDirection = 'WATCH';

    const buyBias = metrics.takerBuyRatio - 0.5;
    const flowStrength = Math.abs(buyBias) * 2;
    const moveStrength = Math.min(metrics.absPriceChange3m / 0.01, 1);
    const bodyStrength = clamp(metrics.bodyStrength, 0, 1);

    let directionConfidence = clamp(
        flowStrength * 0.45 +
        moveStrength * 0.35 +
        bodyStrength * 0.20,
        0,
        1
    );

    if (
        metrics.priceChange3m > 0 &&
        metrics.takerBuyRatio >= config.LONG_TAKER_BUY_RATIO
    ) {
        suggestedDirection = 'LONG';
    }

    if (
        metrics.priceChange3m < 0 &&
        metrics.takerBuyRatio <= config.SHORT_TAKER_BUY_RATIO
    ) {
        suggestedDirection = 'SHORT';
    }

    // Nếu giá chạy lên nhưng taker sell áp đảo, hướng chưa sạch.
    if (
        metrics.priceChange3m > 0 &&
        metrics.takerBuyRatio < 0.48
    ) {
        suggestedDirection = 'WATCH';
        directionConfidence *= 0.65;
    }

    // Nếu giá giảm nhưng taker buy hấp thụ mạnh, hướng chưa sạch.
    if (
        metrics.priceChange3m < 0 &&
        metrics.takerBuyRatio > 0.52
    ) {
        suggestedDirection = 'WATCH';
        directionConfidence *= 0.65;
    }

    return {
        suggestedDirection,
        directionConfidence: round(directionConfidence, 4)
    };
}


// ============================================================
// 13. CHẤM ĐIỂM OPPORTUNITY
// ------------------------------------------------------------
// Đây là score để quyết định coin có đáng đưa vào radar không.
// Không phải score để vào lệnh.
// ============================================================
function inferOpportunityScore(metrics) {
    const moveScore = Math.min(metrics.absPriceChange3m / 0.012, 1);
    const volumeScore = Math.min(metrics.volumeZ / 4, 1);
    const quoteVolumeScore = Math.min(metrics.quoteVolumeZ / 4, 1);
    const tradeCountScore = Math.min(metrics.tradeCountZ / 3.5, 1);
    const rangeScore = Math.min(metrics.range3m / 0.015, 1);
    const bodyScore = Math.min(metrics.bodyStrength, 1);

    const flowImbalance = Math.abs(metrics.takerBuyRatio - 0.5) * 2;
    const flowScore = clamp(flowImbalance, 0, 1);

    let spreadScore = 0.7;
    if (metrics.spreadBps !== null) {
        spreadScore = clamp(1 - metrics.spreadBps / config.MAX_SPREAD_BPS, 0, 1);
    }

    let marketScore = 0.5;

    if (metrics.marketContext.volatilityMode === 'HIGH') {
        marketScore = 0.75;
    }

    if (metrics.marketContext.marketBias === 'RISK_ON' && metrics.priceChange3m > 0) {
        marketScore = 0.8;
    }

    if (metrics.marketContext.marketBias === 'RISK_OFF' && metrics.priceChange3m < 0) {
        marketScore = 0.8;
    }

    // Trọng số được thiết kế cho radar tầng đầu:
    // volume/range/trades quan trọng vì nhiệm vụ là bắt bất thường,
    // không phải đo xác suất thắng cuối.
    const score =
        volumeScore * 0.22 +
        quoteVolumeScore * 0.13 +
        tradeCountScore * 0.14 +
        moveScore * 0.17 +
        rangeScore * 0.14 +
        bodyScore * 0.07 +
        flowScore * 0.06 +
        spreadScore * 0.04 +
        marketScore * 0.03;

    return round(clamp(score, 0, 1), 4);
}


// ============================================================
// 14. PHÂN TÍCH OPPORTUNITY CHO 1 SYMBOL
// ------------------------------------------------------------
// Đây là lõi radar mới.
// Thay vì bắt buộc đủ tất cả điều kiện,
// dùng trigger-count để bắt rộng hơn nhưng vẫn tránh coin rác.
// ============================================================
async function analyzeOpportunity(symbol, bookTicker, marketContext) {
    try {
        const response = await binanceGet('/fapi/v1/klines', {
            symbol,
            interval: config.IMPULSE_INTERVAL,
            limit: config.IMPULSE_KLINE_LIMIT
        }, { maxRetries: 1 });

        const klines = response.data;

        if (!Array.isArray(klines) || klines.length < config.IMPULSE_KLINE_LIMIT) {
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
        const numberOfTrades = safeNumber(current[8]);
        const takerBuyBaseVolume = safeNumber(current[9]);
        const takerBuyQuoteVolume = safeNumber(current[10]);
        const previousClose = safeNumber(previous[4]);

        if (
            openTime <= 0 ||
            openNow <= 0 ||
            highNow <= 0 ||
            lowNow <= 0 ||
            closeNow <= 0 ||
            previousClose <= 0 ||
            volumeNow <= 0
        ) {
            return null;
        }

        // ----------------------------------------------------
        // 14.1 PROJECTED VOLUME CHO NẾN CHƯA ĐÓNG
        // ----------------------------------------------------
        const elapsedRawMs = Date.now() - openTime;
        const intervalMs = config.IMPULSE_INTERVAL_MS;
        const maxElapsedMs = intervalMs * config.MAX_CANDLE_ELAPSED_RATIO;
        const elapsedMs = clamp(elapsedRawMs, 0, maxElapsedMs);

        // Không phân tích quá sớm đầu nến để tránh projection bị ảo.
        if (elapsedMs < config.MIN_CANDLE_ELAPSED_MS) {
            return null;
        }

        let projectionFactor = 1;

        if (elapsedMs > 0 && elapsedMs < intervalMs) {
            projectionFactor = Math.min(
                intervalMs / elapsedMs,
                config.MAX_PROJECTION_FACTOR
            );
        }

        const projectedVolume = volumeNow * projectionFactor;
        const projectedQuoteVolume = quoteVolumeNow * projectionFactor;
        const projectedTrades = numberOfTrades * projectionFactor;
        const projectedTakerBuyBaseVolume = takerBuyBaseVolume * projectionFactor;
        const projectedTakerBuyQuoteVolume = takerBuyQuoteVolume * projectionFactor;

        // ----------------------------------------------------
        // 14.2 BASELINE CHỈ LẤY NẾN ĐÃ ĐÓNG
        // ----------------------------------------------------
        const historyVolumes = closedHistory.map(k => safeNumber(k[5])).filter(v => v > 0);
        const historyQuoteVolumes = closedHistory.map(k => safeNumber(k[7])).filter(v => v > 0);
        const historyTrades = closedHistory.map(k => safeNumber(k[8])).filter(v => v > 0);

        if (historyVolumes.length === 0) return null;

        const avgVolume = average(historyVolumes);
        const avgQuoteVolume = average(historyQuoteVolumes);
        const avgTrades = average(historyTrades);

        if (avgVolume <= 0) return null;

        // ----------------------------------------------------
        // 14.3 METRICS BIẾN ĐỘNG
        // ----------------------------------------------------
        const priceChange3m = pctChange(previousClose, closeNow);
        const absPriceChange3m = Math.abs(priceChange3m);

        const volumeZ = projectedVolume / avgVolume;
        const quoteVolumeZ = avgQuoteVolume > 0
            ? projectedQuoteVolume / avgQuoteVolume
            : volumeZ;

        const tradeCountZ = avgTrades > 0
            ? projectedTrades / avgTrades
            : 1;

        const range3m = (highNow - lowNow) / previousClose;
        const candleRange = highNow - lowNow;

        const bodyStrength = candleRange > 0
            ? Math.abs(closeNow - openNow) / candleRange
            : 0;

        const upperWick = candleRange > 0
            ? highNow - Math.max(openNow, closeNow)
            : 0;

        const lowerWick = candleRange > 0
            ? Math.min(openNow, closeNow) - lowNow
            : 0;

        const upperWickRatio = candleRange > 0
            ? upperWick / candleRange
            : 0;

        const lowerWickRatio = candleRange > 0
            ? lowerWick / candleRange
            : 0;

        // Taker buy ratio dùng volume thật, không cần projected,
        // vì tỷ lệ mua/bán trong phần nến đã chạy vẫn có ý nghĩa.
        const takerBuyRatio = volumeNow > 0
            ? takerBuyBaseVolume / volumeNow
            : 0.5;

        const spreadBps = calculateSpreadBps(bookTicker);

        // ----------------------------------------------------
        // 14.4 LOẠI COIN SPREAD QUÁ XẤU
        // ----------------------------------------------------
        // Với scalping, spread quá rộng làm tầng sau tốn tài nguyên vô ích.
        // Nhưng nếu không lấy được spread thì không loại, tránh miss do API lỗi.
        if (spreadBps !== null && spreadBps > config.MAX_SPREAD_BPS) {
            return null;
        }

        const metrics = {
            symbol,
            openTime,
            elapsedMs,
            intervalMs,
            projectionFactor,

            openNow,
            highNow,
            lowNow,
            closeNow,
            previousClose,

            priceChange3m,
            absPriceChange3m,
            range3m,
            bodyStrength,
            upperWickRatio,
            lowerWickRatio,

            volumeNow,
            projectedVolume,
            quoteVolumeNow,
            projectedQuoteVolume,
            avgVolume,
            avgQuoteVolume,
            volumeZ,
            quoteVolumeZ,

            numberOfTrades,
            projectedTrades,
            avgTrades,
            tradeCountZ,

            takerBuyRatio,
            takerBuyBaseVolume,
            projectedTakerBuyBaseVolume,
            takerBuyQuoteVolume,
            projectedTakerBuyQuoteVolume,

            spreadBps,
            marketContext
        };

        // ----------------------------------------------------
        // 14.5 TRIGGER COUNT - LỌC MỀM
        // ----------------------------------------------------
        // Radar tầng đầu không nên bắt buộc đủ tất cả điều kiện.
        // Chỉ cần có ít nhất N dấu hiệu bất thường.
        let triggerCount = 0;
        const triggerReasons = [];

        if (absPriceChange3m >= config.MIN_PRICE_MOVE_3M) {
            triggerCount++;
            triggerReasons.push('PRICE_MOVE');
        }

        if (volumeZ >= config.MIN_VOLUME_Z) {
            triggerCount++;
            triggerReasons.push('VOLUME_Z');
        }

        if (quoteVolumeZ >= config.MIN_QUOTE_VOLUME_Z) {
            triggerCount++;
            triggerReasons.push('QUOTE_VOLUME_Z');
        }

        if (tradeCountZ >= config.MIN_TRADES_Z) {
            triggerCount++;
            triggerReasons.push('TRADE_COUNT_Z');
        }

        if (range3m >= config.MIN_RANGE_3M) {
            triggerCount++;
            triggerReasons.push('RANGE');
        }

        if (bodyStrength >= config.MIN_BODY_STRENGTH) {
            triggerCount++;
            triggerReasons.push('BODY');
        }

        // Trường hợp đặc biệt:
        // Nếu volume/trade tăng cực mạnh nhưng giá chưa chạy,
        // vẫn nên đưa vào WATCH vì có thể là giai đoạn gom/xả trước biến động.
        const earlyActivity =
            volumeZ >= 2.5 &&
            tradeCountZ >= 2 &&
            range3m >= config.MIN_RANGE_3M * 0.65;

        if (triggerCount < config.MIN_TRIGGER_COUNT && !earlyActivity) {
            return null;
        }

        const tags = buildSignalTags(metrics);
        const opportunityScore = inferOpportunityScore(metrics);

        if (opportunityScore < config.OPPORTUNITY_THRESHOLD && !earlyActivity) {
            return null;
        }

        const {
            suggestedDirection,
            directionConfidence
        } = inferSuggestedDirection(metrics);

        const priority = opportunityScore >= config.HOT_THRESHOLD
            ? 'HOT'
            : 'WATCH';

        const candidate = {
            // Giữ field cũ cho tương thích downstream.
            symbol,
            direction: suggestedDirection,
            score: opportunityScore,

            // Field mới rõ nghĩa hơn.
            suggestedDirection,
            opportunityScore,
            directionConfidence,
            priority,

            signalType: 'OPPORTUNITY',
            timeframe: config.IMPULSE_INTERVAL,
            reason: `${priority}_OPPORTUNITY_${config.IMPULSE_INTERVAL}`,

            tags,
            triggerCount,
            triggerReasons,

            priceChange3m: round(priceChange3m, 6),
            absPriceChange3m: round(absPriceChange3m, 6),
            volumeZ: round(volumeZ, 4),
            quoteVolumeZ: round(quoteVolumeZ, 4),
            tradeCountZ: round(tradeCountZ, 4),
            range3m: round(range3m, 6),
            bodyStrength: round(bodyStrength, 4),
            upperWickRatio: round(upperWickRatio, 4),
            lowerWickRatio: round(lowerWickRatio, 4),
            takerBuyRatio: round(takerBuyRatio, 4),

            volumeNow: round(volumeNow, 4),
            projectedVolume: round(projectedVolume, 4),
            avgVolume: round(avgVolume, 4),

            quoteVolumeNow: round(quoteVolumeNow, 2),
            projectedQuoteVolume: round(projectedQuoteVolume, 2),
            avgQuoteVolume: round(avgQuoteVolume, 2),

            numberOfTrades,
            projectedTrades: round(projectedTrades, 2),
            avgTrades: round(avgTrades, 2),

            spreadBps: spreadBps === null ? null : round(spreadBps, 4),

            openNow,
            highNow,
            lowNow,
            closeNow,
            previousClose,

            elapsedMs: Math.round(elapsedMs),
            projectionFactor: round(projectionFactor, 4),

            marketContext,

            detectedAt: Date.now()
        };

        return candidate;
    } catch (error) {
        return null;
    }
}


// ============================================================
// 15. PLACEHOLDER KIỂM TRA MANIPULATION
// ------------------------------------------------------------
// Giữ lại hook này để sau có thể thêm:
// - blacklist symbol
// - funding quá dị
// - spread bất thường
// - coin bị pump dump quá bẩn
// ============================================================
async function checkOnChainManipulation(symbol) {
    return false;
}


// ============================================================
// 16. ĐỒNG BỘ SCORE VÀO REDIS HASH macro:scores
// ------------------------------------------------------------
// Giữ tương thích cũ: macro:scores vẫn là hash symbol -> score.
// Score ở đây là opportunityScore, không phải xác suất thắng.
// ============================================================
async function syncMacroScores() {
    if (currentRadarSlots.size === 0) return;

    const pipeline = dataClient.pipeline();

    for (const [, slot] of currentRadarSlots.entries()) {
        macroScoreCache.set(slot.symbol, slot.opportunityScore);
        pipeline.hset(
            config.CHANNELS.MACRO_SCORES,
            slot.symbol,
            slot.opportunityScore
        );
    }

    await pipeline.exec();
}


// ============================================================
// 17. PUBLISH REDIS - GIỮ NGUYÊN LUỒNG CŨ + THÊM LUỒNG MỚI
// ============================================================
function publishSubscribe(symbol) {
    if (activeSubscribedSymbols.has(symbol)) return;

    pubClient.publish(config.CHANNELS.SUBSCRIPTIONS, JSON.stringify({
        action: 'SUBSCRIBE',
        symbol,
        client: 'radar'
    }));

    activeSubscribedSymbols.add(symbol);
}

function publishUnsubscribeIfNoActiveSlot(symbol) {
    const stillHasSlot = [...currentRadarSlots.values()]
        .some(slot => slot.symbol === symbol);

    if (stillHasSlot) return;
    if (!activeSubscribedSymbols.has(symbol)) return;

    pubClient.publish(config.CHANNELS.SUBSCRIPTIONS, JSON.stringify({
        action: 'UNSUBSCRIBE',
        symbol,
        client: 'radar'
    }));

    activeSubscribedSymbols.delete(symbol);
}

function publishCandidate(candidate, eventType = 'NEW') {
    const payload = {
        ...candidate,
        eventType,
        publishedAt: Date.now()
    };

    // Luồng cũ: downstream cũ vẫn nghe được.
    pubClient.publish(config.CHANNELS.CANDIDATES, JSON.stringify(payload));

    // Luồng mới: signal đầy đủ.
    pubClient.publish(config.CHANNELS.SIGNALS, JSON.stringify(payload));

    // Tách watchlist / hotlist để tầng sau dễ ưu tiên.
    if (candidate.priority === 'HOT') {
        pubClient.publish(config.CHANNELS.HOTLIST, JSON.stringify(payload));
    } else {
        pubClient.publish(config.CHANNELS.WATCHLIST, JSON.stringify(payload));
    }
}


// ============================================================
// 18. XÓA SLOT RADAR
// ============================================================
async function removeRadarSlot(slotKey, reason = 'EXPIRED') {
    const slot = currentRadarSlots.get(slotKey);
    if (!slot) return;

    currentRadarSlots.delete(slotKey);

    const stillHasSameSymbol = [...currentRadarSlots.values()]
        .some(s => s.symbol === slot.symbol);

    if (!stillHasSameSymbol) {
        await dataClient.hdel(config.CHANNELS.MACRO_SCORES, slot.symbol);
        macroScoreCache.delete(slot.symbol);
        publishUnsubscribeIfNoActiveSlot(slot.symbol);
    }

    console.log(
        `❌ [RADAR] Loại ${slot.symbol} ${slot.suggestedDirection} | score=${slot.opportunityScore} | reason=${reason}`
    );
}


// ============================================================
// 19. HARD CAP SLOT
// ------------------------------------------------------------
// Sticky giúp không miss cơ hội,
// nhưng hard cap tránh giữ quá nhiều coin cũ làm nặng downstream.
// ============================================================
async function enforceHardCap() {
    if (currentRadarSlots.size <= config.MAX_TOTAL_RADAR_SLOTS) return;

    const sortedSlots = [...currentRadarSlots.entries()]
        .sort((a, b) => {
            const scoreDiff = a[1].opportunityScore - b[1].opportunityScore;
            if (scoreDiff !== 0) return scoreDiff;

            return a[1].updatedAt - b[1].updatedAt;
        });

    while (
        currentRadarSlots.size > config.MAX_TOTAL_RADAR_SLOTS &&
        sortedSlots.length > 0
    ) {
        const [slotKey] = sortedSlots.shift();
        await removeRadarSlot(slotKey, 'HARD_CAP_EXCEEDED');
    }
}


// ============================================================
// 20. CÓ NÊN REPUBLISH UPDATE KHÔNG?
// ------------------------------------------------------------
// Nếu score tăng/giảm mạnh hoặc đã lâu chưa publish,
// gửi lại candidate để tầng sau cập nhật trạng thái.
// ============================================================
function shouldRepublishCandidate(oldSlot, candidate) {
    if (!oldSlot) return true;

    const scoreDelta = Math.abs(
        candidate.opportunityScore - oldSlot.opportunityScore
    );

    if (scoreDelta >= config.REPUBLISH_SCORE_DELTA) {
        return true;
    }

    if (candidate.priority !== oldSlot.priority) {
        return true;
    }

    if (candidate.suggestedDirection !== oldSlot.suggestedDirection) {
        return true;
    }

    const lastPublishAt = oldSlot.lastPublishedAt || oldSlot.addedAt || 0;
    if (Date.now() - lastPublishAt >= config.REPUBLISH_MIN_INTERVAL_MS) {
        return true;
    }

    return false;
}


// ============================================================
// 21. VÒNG LẶP RADAR CHÍNH
// ============================================================
async function scanRadar() {
    if (isScanning) {
        console.warn('⚠️ [RADAR] Bỏ qua scan mới vì scan trước chưa xong');
        return;
    }

    isScanning = true;
    const startedAt = Date.now();

    try {
        console.log('');
        console.log(
            `📡 [RADAR] Opportunity scan ${config.IMPULSE_INTERVAL} | threshold=${config.OPPORTUNITY_THRESHOLD} | hot=${config.HOT_THRESHOLD}`
        );

        const symbols = await fetchTradableUSDTPerpetualSymbols();

        if (symbols.length === 0) {
            console.warn('⚠️ [RADAR] Không có symbol để quét');
            return;
        }

        const [tickerMap, bookTickerMap, marketContext] = await Promise.all([
            fetch24hTickerMap(),
            fetchBookTickerMap(),
            fetchMarketContext()
        ]);

        if (tickerMap.size === 0) {
            console.warn('⚠️ [RADAR] Không có ticker 24h để lọc thanh khoản');
            return;
        }

        const liquidSymbols = symbols.filter(symbol => {
            const ticker = tickerMap.get(symbol);
            if (!ticker) return false;

            const quoteVolume = safeNumber(ticker.quoteVolume);
            return quoteVolume >= config.MIN_24H_QUOTE_VOLUME;
        });

        console.log(
            `🔎 [RADAR] Thanh khoản >= ${config.MIN_24H_QUOTE_VOLUME}: ${liquidSymbols.length}/${symbols.length} symbols | market=${marketContext.marketBias}/${marketContext.volatilityMode}`
        );

        const candidates = [];

        for (let i = 0; i < liquidSymbols.length; i += config.BATCH_SIZE) {
            const batch = liquidSymbols.slice(i, i + config.BATCH_SIZE);

            const results = await Promise.all(batch.map(async symbol => {
                const isManipulated = await checkOnChainManipulation(symbol);
                if (isManipulated) return null;

                const bookTicker = bookTickerMap.get(symbol);
                return analyzeOpportunity(symbol, bookTicker, marketContext);
            }));

            for (const result of results) {
                if (result) candidates.push(result);
            }

            if (i + config.BATCH_SIZE < liquidSymbols.length) {
                await sleep(config.BATCH_DELAY_MS);
            }
        }

        candidates.sort((a, b) => b.opportunityScore - a.opportunityScore);

        const topCandidates = candidates.slice(0, config.MAX_RADAR_SLOTS);
        const newSlotSet = new Set(topCandidates.map(c => buildSlotKey(c.symbol)));

        let addedCount = 0;
        let updatedCount = 0;
        let publishedCount = 0;
        let hotCount = 0;
        let watchCount = 0;

        // ----------------------------------------------------
        // 21.1 THÊM HOẶC CẬP NHẬT SLOT
        // ----------------------------------------------------
        for (const candidate of topCandidates) {
            const slotKey = buildSlotKey(candidate.symbol);
            const oldSlot = currentRadarSlots.get(slotKey);

            if (!oldSlot) {
                const newSlot = {
                    ...candidate,
                    addedAt: Date.now(),
                    updatedAt: Date.now(),
                    lastSeenAt: Date.now(),
                    lastPublishedAt: Date.now()
                };

                currentRadarSlots.set(slotKey, newSlot);

                publishSubscribe(candidate.symbol);
                publishCandidate(candidate, 'NEW');

                addedCount++;
                publishedCount++;

                if (candidate.priority === 'HOT') hotCount++;
                else watchCount++;

                console.log(
                    `➕ [RADAR] ${candidate.priority} ${candidate.symbol} ${candidate.suggestedDirection} | score=${candidate.opportunityScore} | dirConf=${candidate.directionConfidence} | move=${round(candidate.priceChange3m * 100, 2)}% | volZ=${candidate.volumeZ} | tradesZ=${candidate.tradeCountZ} | tags=${candidate.tags.slice(0, 4).join(',')}`
                );
            } else {
                const shouldPublish = shouldRepublishCandidate(oldSlot, candidate);

                const updatedSlot = {
                    ...oldSlot,
                    ...candidate,
                    addedAt: oldSlot.addedAt,
                    updatedAt: Date.now(),
                    lastSeenAt: Date.now(),
                    lastPublishedAt: shouldPublish
                        ? Date.now()
                        : oldSlot.lastPublishedAt
                };

                currentRadarSlots.set(slotKey, updatedSlot);
                updatedCount++;

                if (shouldPublish) {
                    publishCandidate(candidate, 'UPDATE');
                    publishedCount++;
                }

                if (candidate.priority === 'HOT') hotCount++;
                else watchCount++;
            }
        }

        // ----------------------------------------------------
        // 21.2 LOẠI SLOT KHÔNG CÒN TOP SAU THỜI GIAN STICKY
        // ----------------------------------------------------
        const slotsToDrop = [];

        for (const [slotKey, slot] of currentRadarSlots.entries()) {
            if (!newSlotSet.has(slotKey)) {
                const ageMs = Date.now() - slot.addedAt;

                if (ageMs >= config.MIN_HOLD_MS) {
                    slotsToDrop.push(slotKey);
                } else {
                    const remainMinutes = Math.ceil(
                        (config.MIN_HOLD_MS - ageMs) / 60000
                    );

                    console.log(
                        `🔒 [STICKY] Giữ ${slot.symbol} ${slot.suggestedDirection} dù không còn top | còn ${remainMinutes} phút`
                    );
                }
            }
        }

        for (const slotKey of slotsToDrop) {
            await removeRadarSlot(slotKey, 'MIN_HOLD_EXPIRED');
        }

        await enforceHardCap();
        await syncMacroScores();

        const elapsedSec = round((Date.now() - startedAt) / 1000, 2);

        console.log(
            `📊 [RADAR] Candidates=${candidates.length} | Top=${topCandidates.length} | Added=${addedCount} | Updated=${updatedCount} | Published=${publishedCount} | Hot=${hotCount} | Watch=${watchCount} | Slots=${currentRadarSlots.size} | Time=${elapsedSec}s`
        );
    } catch (error) {
        console.error('❌ [RADAR] Lỗi scanRadar:', error.message);
    } finally {
        isScanning = false;
    }
}


// ============================================================
// 22. SCHEDULER CHỐNG TRÔI NHỊP
// ------------------------------------------------------------
// Canh đúng mốc nến 3m + offset.
// Ví dụ offset 8s: scan sau khi nến mới chạy 8s.
// Nhưng do MIN_CANDLE_ELAPSED_MS mặc định 15s,
// bạn nên để SCAN_OFFSET_MS >= 15000 nếu muốn scan ngay vòng đầu có dữ liệu.
// Nếu offset thấp hơn, analyzeOpportunity sẽ tự bỏ qua nến quá non.
// ============================================================
function getNextAlignedScanDelay() {
    const now = Date.now();
    const intervalMs = config.IMPULSE_INTERVAL_MS;
    const offsetMs = config.SCAN_OFFSET_MS;

    let next = Math.floor(now / intervalMs) * intervalMs + offsetMs;
    if (next <= now) next += intervalMs;

    return next - now;
}

function scheduleNextScan() {
    if (nextScanTimer) clearTimeout(nextScanTimer);

    const delay = getNextAlignedScanDelay();
    const nextAt = new Date(Date.now() + delay).toISOString();

    console.log(
        `⏱️ [RADAR] Scan kế tiếp tại ${nextAt} | sau ${round(delay / 1000, 2)}s`
    );

    nextScanTimer = setTimeout(async () => {
        await scanRadar();
        scheduleNextScan();
    }, delay);
}


// ============================================================
// 23. KHỞI ĐỘNG
// ============================================================
console.log('👁️ RadarManager V20 - Opportunity Radar Futures 15M- khởi động!');
console.log(
    `⚙️ [CONFIG] batch=${config.BATCH_SIZE}, delay=${config.BATCH_DELAY_MS}ms, opportunity=${config.OPPORTUNITY_THRESHOLD}, hot=${config.HOT_THRESHOLD}, triggerCount=${config.MIN_TRIGGER_COUNT}, projectedVolume=ON, maxProjection=${config.MAX_PROJECTION_FACTOR}x, scheduler=ALIGNED`
);

scanRadar().finally(() => {
    scheduleNextScan();
});

setInterval(() => {
    syncMacroScores().catch(error => {
        console.error('❌ [RADAR] Lỗi syncMacroScores:', error.message);
    });
}, 15 * 60 * 1000);


// ============================================================
// 24. XỬ LÝ LỖI REDIS
// ============================================================
pubClient.on('error', err => {
    console.error('❌ Redis pubClient error:', err.message);
});

dataClient.on('error', err => {
    console.error('❌ Redis dataClient error:', err.message);
});


// ============================================================
// 25. GRACEFUL SHUTDOWN
// ============================================================
async function shutdown() {
    console.log('');
    console.log('🛑 [RADAR] Đang tắt RadarManager...');

    if (nextScanTimer) clearTimeout(nextScanTimer);

    try {
        await pubClient.quit();
        await dataClient.quit();
    } catch (error) {
        console.error('❌ Lỗi khi đóng Redis:', error.message);
    }

    process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);