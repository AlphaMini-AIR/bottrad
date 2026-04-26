const axios = require('axios');
const Redis = require('ioredis');
const fs = require('fs');

// ============================================================
// RADAR MANAGER V19 - IMPULSE RADAR 3M HAI CHIỀU 10/10
// ------------------------------------------------------------
// Mục tiêu:
// 1. Quét toàn bộ Binance Futures USDT-M Perpetual đang trading.
// 2. Bắt biến động lớn theo 3 phút cho cả LONG và SHORT.
// 3. Không dùng Math.random() để chấm điểm trading.
// 4. Fix lỗi volume nến chưa đóng bằng projected volume.
// 5. Giảm rủi ro IP ban bằng batch an toàn + retry/backoff.
// 6. Chống trôi nhịp bằng scheduler canh mốc nến 3m.
// 7. Giữ nguyên luồng Redis cũ: radar:candidates, system:subscriptions, macro:scores.
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

function buildSlotKey(symbol, direction) {
    return `${symbol}:${direction}`;
}

function getOppositeDirection(direction) {
    return direction === 'LONG' ? 'SHORT' : 'LONG';
}

// Jitter chỉ dùng cho retry/backoff network, không dùng để chấm score trading.
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
            MACRO_SCORES: 'macro:scores',
            SUBSCRIPTIONS: 'system:subscriptions'
        },
        MAX_RADAR_SLOTS: 30,
        MACRO_THRESHOLD: 0.6,
        IMPULSE_THRESHOLD: 0.65,
        IMPULSE_INTERVAL: '3m',
        IMPULSE_KLINE_LIMIT: 31,
        MIN_24H_QUOTE_VOLUME: 1000000,
        MIN_VOLUME_Z: 2,
        MIN_PRICE_MOVE_3M: 0.006,
        MIN_RANGE_3M: 0.008,
        MIN_BODY_STRENGTH: 0.45,
        LONG_TAKER_BUY_RATIO: 0.55,
        SHORT_TAKER_BUY_RATIO: 0.45,
        MIN_CANDLE_ELAPSED_MS: 5000,
        MAX_CANDLE_ELAPSED_RATIO: 1,
        BATCH_SIZE: 5,
        BATCH_DELAY_MS: 250,
        REQUEST_TIMEOUT_MS: 10000,
        MAX_RETRIES: 2,
        RETRY_BASE_DELAY_MS: 500,
        RETRY_RATE_LIMIT_DELAY_MS: 5000,
        SYMBOLS_CACHE_MS: 15 * 60 * 1000,
        TICKER_CACHE_MS: 10 * 1000,
        SCAN_OFFSET_MS: 8000,
        MIN_HOLD_MS: 10 * 60 * 1000
    };
}

// ============================================================
// 3. DEFAULT CONFIG ĐỂ TƯƠNG THÍCH FILE system_config.json CŨ
// ============================================================
config.REDIS_URL = config.REDIS_URL || process.env.REDIS_URL || 'redis://localhost:6379';
config.CHANNELS = config.CHANNELS || {};
config.CHANNELS.CANDIDATES = config.CHANNELS.CANDIDATES || 'radar:candidates';
config.CHANNELS.MACRO_SCORES = config.CHANNELS.MACRO_SCORES || 'macro:scores';
config.CHANNELS.SUBSCRIPTIONS = config.CHANNELS.SUBSCRIPTIONS || 'system:subscriptions';

config.MAX_RADAR_SLOTS = config.MAX_RADAR_SLOTS ?? 30;
config.MACRO_THRESHOLD = config.MACRO_THRESHOLD ?? 0.6;
config.IMPULSE_THRESHOLD = config.IMPULSE_THRESHOLD ?? config.MACRO_THRESHOLD ?? 0.65;
config.IMPULSE_INTERVAL = config.IMPULSE_INTERVAL || '3m';
config.IMPULSE_INTERVAL_MS = config.IMPULSE_INTERVAL_MS ?? intervalToMs(config.IMPULSE_INTERVAL);
config.IMPULSE_KLINE_LIMIT = config.IMPULSE_KLINE_LIMIT ?? 31;
config.MIN_24H_QUOTE_VOLUME = config.MIN_24H_QUOTE_VOLUME ?? 1000000;
config.MIN_VOLUME_Z = config.MIN_VOLUME_Z ?? 2;
config.MIN_PRICE_MOVE_3M = config.MIN_PRICE_MOVE_3M ?? 0.006;
config.MIN_RANGE_3M = config.MIN_RANGE_3M ?? 0.008;
config.MIN_BODY_STRENGTH = config.MIN_BODY_STRENGTH ?? 0.45;
config.LONG_TAKER_BUY_RATIO = config.LONG_TAKER_BUY_RATIO ?? 0.55;
config.SHORT_TAKER_BUY_RATIO = config.SHORT_TAKER_BUY_RATIO ?? 0.45;
config.MIN_CANDLE_ELAPSED_MS = config.MIN_CANDLE_ELAPSED_MS ?? 5000;
config.MAX_CANDLE_ELAPSED_RATIO = config.MAX_CANDLE_ELAPSED_RATIO ?? 1;
config.BATCH_SIZE = config.BATCH_SIZE ?? 5;
config.BATCH_DELAY_MS = config.BATCH_DELAY_MS ?? 250;
config.REQUEST_TIMEOUT_MS = config.REQUEST_TIMEOUT_MS ?? 10000;
config.MAX_RETRIES = config.MAX_RETRIES ?? 2;
config.RETRY_BASE_DELAY_MS = config.RETRY_BASE_DELAY_MS ?? 500;
config.RETRY_RATE_LIMIT_DELAY_MS = config.RETRY_RATE_LIMIT_DELAY_MS ?? 5000;
config.SYMBOLS_CACHE_MS = config.SYMBOLS_CACHE_MS ?? 15 * 60 * 1000;
config.TICKER_CACHE_MS = config.TICKER_CACHE_MS ?? 10 * 1000;
config.SCAN_OFFSET_MS = config.SCAN_OFFSET_MS ?? 8000;
config.MIN_HOLD_MS = config.MIN_HOLD_MS ?? 10 * 60 * 1000;

const BINANCE_FAPI_BASE = 'https://fapi.binance.com';
const pubClient = new Redis(config.REDIS_URL);
const dataClient = new Redis(config.REDIS_URL);

// ============================================================
// 4. BỘ NHỚ TRẠNG THÁI
// ============================================================

// Slot key dạng SYMBOL:DIRECTION, ví dụ PEPEUSDT:LONG.
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
    return ['ECONNRESET', 'ETIMEDOUT', 'ECONNABORTED', 'EAI_AGAIN', 'ENOTFOUND'].includes(code);
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
            console.warn(`⚠️ [BINANCE] Retry ${attempt + 1}/${maxRetries} path=${path} status=${status} code=${code} delay=${delay}ms`);
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

    if (tradableSymbolsCache.symbols.length > 0 && now - tradableSymbolsCache.updatedAt < config.SYMBOLS_CACHE_MS) {
        return tradableSymbolsCache.symbols;
    }

    try {
        const response = await binanceGet('/fapi/v1/exchangeInfo');
        const symbols = response.data.symbols
            .filter(item => item.status === 'TRADING')
            .filter(item => item.contractType === 'PERPETUAL')
            .filter(item => item.quoteAsset === 'USDT')
            .map(item => item.symbol);

        tradableSymbolsCache = { updatedAt: now, symbols };
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

    if (tickerCache.map.size > 0 && now - tickerCache.updatedAt < config.TICKER_CACHE_MS) {
        return tickerCache.map;
    }

    try {
        const response = await binanceGet('/fapi/v1/ticker/24hr');
        const tickerMap = new Map();

        for (const ticker of response.data) {
            tickerMap.set(ticker.symbol, ticker);
        }

        tickerCache = { updatedAt: now, map: tickerMap };
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
// 8. PHÂN TÍCH IMPULSE 3 PHÚT CHO 1 SYMBOL
// ------------------------------------------------------------
// Fix quan trọng: dùng projectedVolume thay vì volumeNow thô.
// Nếu nến mới chạy 60s, volume được nội suy lên scale 180s.
// ============================================================
async function analyze3mImpulse(symbol) {
    try {
        const response = await binanceGet('/fapi/v1/klines', {
            symbol,
            interval: config.IMPULSE_INTERVAL,
            limit: config.IMPULSE_KLINE_LIMIT
        }, { maxRetries: 1 });

        const klines = response.data;
        if (!Array.isArray(klines) || klines.length < config.IMPULSE_KLINE_LIMIT) return null;

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

        if (openTime <= 0 || openNow <= 0 || highNow <= 0 || lowNow <= 0 || closeNow <= 0 || previousClose <= 0 || volumeNow <= 0) {
            return null;
        }

        // ----------------------------------------------------
        // 8.1 PROJECTED VOLUME CHO NẾN CHƯA ĐÓNG
        // ----------------------------------------------------
        const elapsedRawMs = Date.now() - openTime;
        const intervalMs = config.IMPULSE_INTERVAL_MS;
        const maxElapsedMs = intervalMs * config.MAX_CANDLE_ELAPSED_RATIO;
        const elapsedMs = clamp(elapsedRawMs, 0, maxElapsedMs);

        // Bỏ qua vài giây đầu để tránh volume bị nội suy quá ảo.
        if (elapsedMs < config.MIN_CANDLE_ELAPSED_MS) return null;

        let projectionFactor = 1;
        if (elapsedMs > 0 && elapsedMs < intervalMs) {
            projectionFactor = intervalMs / elapsedMs;
        }

        const projectedVolume = volumeNow * projectionFactor;
        const projectedQuoteVolume = quoteVolumeNow * projectionFactor;
        const projectedTakerBuyBaseVolume = takerBuyBaseVolume * projectionFactor;
        const projectedTakerBuyQuoteVolume = takerBuyQuoteVolume * projectionFactor;

        // ----------------------------------------------------
        // 8.2 BASELINE CHỈ LẤY NẾN ĐÃ ĐÓNG
        // ----------------------------------------------------
        const historyVolumes = closedHistory.map(k => safeNumber(k[5])).filter(v => v > 0);
        const historyQuoteVolumes = closedHistory.map(k => safeNumber(k[7])).filter(v => v > 0);
        if (historyVolumes.length === 0) return null;

        const avgVolume = historyVolumes.reduce((sum, v) => sum + v, 0) / historyVolumes.length;
        const avgQuoteVolume = historyQuoteVolumes.length > 0
            ? historyQuoteVolumes.reduce((sum, v) => sum + v, 0) / historyQuoteVolumes.length
            : 0;

        if (avgVolume <= 0) return null;

        // ----------------------------------------------------
        // 8.3 METRICS BIẾN ĐỘNG
        // ----------------------------------------------------
        const priceChange3m = (closeNow - previousClose) / previousClose;
        const absPriceChange3m = Math.abs(priceChange3m);
        const volumeZ = projectedVolume / avgVolume;
        const quoteVolumeZ = avgQuoteVolume > 0 ? projectedQuoteVolume / avgQuoteVolume : volumeZ;
        const range3m = (highNow - lowNow) / previousClose;
        const candleRange = highNow - lowNow;
        const bodyStrength = candleRange > 0 ? Math.abs(closeNow - openNow) / candleRange : 0;
        const takerBuyRatio = volumeNow > 0 ? takerBuyBaseVolume / volumeNow : 0.5;

        // ----------------------------------------------------
        // 8.4 BỘ LỌC CỨNG GIẢM NHIỄU
        // ----------------------------------------------------
        if (absPriceChange3m < config.MIN_PRICE_MOVE_3M) return null;
        if (volumeZ < config.MIN_VOLUME_Z) return null;
        if (range3m < config.MIN_RANGE_3M) return null;
        if (bodyStrength < config.MIN_BODY_STRENGTH) return null;

        let direction = null;
        if (priceChange3m > 0 && takerBuyRatio >= config.LONG_TAKER_BUY_RATIO) direction = 'LONG';
        if (priceChange3m < 0 && takerBuyRatio <= config.SHORT_TAKER_BUY_RATIO) direction = 'SHORT';
        if (!direction) return null;

        const metrics = {
            symbol,
            direction,
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
            volumeNow,
            projectedVolume,
            quoteVolumeNow,
            projectedQuoteVolume,
            avgVolume,
            avgQuoteVolume,
            volumeZ,
            quoteVolumeZ,
            range3m,
            bodyStrength,
            takerBuyRatio,
            takerBuyBaseVolume,
            projectedTakerBuyBaseVolume,
            takerBuyQuoteVolume,
            projectedTakerBuyQuoteVolume,
            numberOfTrades
        };

        const score = inferImpulseScore(metrics);
        if (score < config.IMPULSE_THRESHOLD) return null;

        return {
            symbol,
            direction,
            score,
            priceChange3m: round(priceChange3m, 6),
            volumeZ: round(volumeZ, 4),
            quoteVolumeZ: round(quoteVolumeZ, 4),
            range3m: round(range3m, 6),
            bodyStrength: round(bodyStrength, 4),
            takerBuyRatio: round(takerBuyRatio, 4),
            volumeNow: round(volumeNow, 4),
            projectedVolume: round(projectedVolume, 4),
            avgVolume: round(avgVolume, 4),
            quoteVolumeNow: round(quoteVolumeNow, 2),
            projectedQuoteVolume: round(projectedQuoteVolume, 2),
            elapsedMs: Math.round(elapsedMs),
            projectionFactor: round(projectionFactor, 4),
            numberOfTrades,
            reason: `${direction}_IMPULSE_3M`,
            detectedAt: Date.now()
        };
    } catch (error) {
        return null;
    }
}

// ============================================================
// 9. CHẤM ĐIỂM IMPULSE DETERMINISTIC
// ============================================================
function inferImpulseScore(metrics) {
    const absMoveScore = Math.min(metrics.absPriceChange3m / 0.03, 1);
    const volumeScore = Math.min(metrics.volumeZ / 5, 1);
    const rangeScore = Math.min(metrics.range3m / 0.04, 1);
    const bodyScore = Math.min(metrics.bodyStrength, 1);

    let imbalanceScore = 0;
    if (metrics.direction === 'LONG') imbalanceScore = Math.max(0, (metrics.takerBuyRatio - 0.5) / 0.5);
    if (metrics.direction === 'SHORT') imbalanceScore = Math.max(0, (0.5 - metrics.takerBuyRatio) / 0.5);

    const score =
        absMoveScore * 0.30 +
        volumeScore * 0.30 +
        rangeScore * 0.20 +
        bodyScore * 0.10 +
        imbalanceScore * 0.10;

    return round(score, 4);
}

// ============================================================
// 10. PLACEHOLDER KIỂM TRA MANIPULATION
// ============================================================
async function checkOnChainManipulation(symbol) {
    return false;
}

// ============================================================
// 11. ĐỒNG BỘ SCORE VÀO REDIS HASH macro:scores
// ============================================================
async function syncMacroScores() {
    if (currentRadarSlots.size === 0) return;

    const symbolBestScore = new Map();

    for (const [, slot] of currentRadarSlots.entries()) {
        const oldScore = symbolBestScore.get(slot.symbol) ?? 0;
        if (slot.score > oldScore) symbolBestScore.set(slot.symbol, slot.score);
    }

    const pipeline = dataClient.pipeline();

    for (const [symbol, score] of symbolBestScore.entries()) {
        macroScoreCache.set(symbol, score);
        pipeline.hset(config.CHANNELS.MACRO_SCORES, symbol, score);
    }

    await pipeline.exec();
}

// ============================================================
// 12. GIỮ NGUYÊN LUỒNG GỬI SUBSCRIBE / UNSUBSCRIBE
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
    const stillHasSlot = [...currentRadarSlots.values()].some(slot => slot.symbol === symbol);
    if (stillHasSlot) return;
    if (!activeSubscribedSymbols.has(symbol)) return;

    pubClient.publish(config.CHANNELS.SUBSCRIPTIONS, JSON.stringify({
        action: 'UNSUBSCRIBE',
        symbol,
        client: 'radar'
    }));

    activeSubscribedSymbols.delete(symbol);
}

function publishCandidate(candidate) {
    pubClient.publish(config.CHANNELS.CANDIDATES, JSON.stringify(candidate));
}

// ============================================================
// 13. XÓA SLOT RADAR
// ============================================================
async function removeRadarSlot(slotKey, reason = 'EXPIRED') {
    const slot = currentRadarSlots.get(slotKey);
    if (!slot) return;

    currentRadarSlots.delete(slotKey);

    const stillHasSameSymbol = [...currentRadarSlots.values()].some(s => s.symbol === slot.symbol);

    if (!stillHasSameSymbol) {
        await dataClient.hdel(config.CHANNELS.MACRO_SCORES, slot.symbol);
        macroScoreCache.delete(slot.symbol);
        publishUnsubscribeIfNoActiveSlot(slot.symbol);
    }

    console.log(`❌ [RADAR] Loại ${slot.symbol} ${slot.direction} | reason=${reason}`);
}

// ============================================================
// 14. VÒNG LẶP RADAR CHÍNH
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
        console.log(`📡 [RADAR] Quét Impulse ${config.IMPULSE_INTERVAL} hai chiều | threshold=${config.IMPULSE_THRESHOLD}`);

        const symbols = await fetchTradableUSDTPerpetualSymbols();
        if (symbols.length === 0) {
            console.warn('⚠️ [RADAR] Không có symbol để quét');
            return;
        }

        const tickerMap = await fetch24hTickerMap();
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

        console.log(`🔎 [RADAR] Sau lọc thanh khoản >= ${config.MIN_24H_QUOTE_VOLUME}: ${liquidSymbols.length}/${symbols.length} symbols`);

        const candidates = [];

        for (let i = 0; i < liquidSymbols.length; i += config.BATCH_SIZE) {
            const batch = liquidSymbols.slice(i, i + config.BATCH_SIZE);

            const results = await Promise.all(batch.map(async symbol => {
                const isManipulated = await checkOnChainManipulation(symbol);
                if (isManipulated) return null;
                return analyze3mImpulse(symbol);
            }));

            for (const result of results) {
                if (result) candidates.push(result);
            }

            if (i + config.BATCH_SIZE < liquidSymbols.length) {
                await sleep(config.BATCH_DELAY_MS);
            }
        }

        candidates.sort((a, b) => b.score - a.score);

        const topCandidates = candidates.slice(0, config.MAX_RADAR_SLOTS);
        const newSlotSet = new Set(topCandidates.map(c => buildSlotKey(c.symbol, c.direction)));

        // Thêm slot mới hoặc cập nhật slot cũ.
        for (const candidate of topCandidates) {
            const slotKey = buildSlotKey(candidate.symbol, candidate.direction);
            const oppositeSlotKey = buildSlotKey(candidate.symbol, getOppositeDirection(candidate.direction));

            if (currentRadarSlots.has(oppositeSlotKey)) {
                await removeRadarSlot(oppositeSlotKey, 'DIRECTION_FLIPPED');
            }

            if (!currentRadarSlots.has(slotKey)) {
                currentRadarSlots.set(slotKey, {
                    ...candidate,
                    addedAt: Date.now(),
                    updatedAt: Date.now()
                });

                publishSubscribe(candidate.symbol);
                publishCandidate(candidate);

                console.log(`➕ [RADAR] Thêm ${candidate.symbol} ${candidate.direction} | score=${candidate.score} | move=${round(candidate.priceChange3m * 100, 2)}% | volZ=${candidate.volumeZ} | proj=${candidate.projectionFactor}x`);
            } else {
                const oldSlot = currentRadarSlots.get(slotKey);
                currentRadarSlots.set(slotKey, {
                    ...oldSlot,
                    ...candidate,
                    addedAt: Date.now(),
                    updatedAt: Date.now()
                });
            }
        }

        // Loại slot hết điều kiện sau thời gian sticky.
        const slotsToDrop = [];

        for (const [slotKey, slot] of currentRadarSlots.entries()) {
            if (!newSlotSet.has(slotKey)) {
                const ageMs = Date.now() - slot.addedAt;

                if (ageMs >= config.MIN_HOLD_MS) {
                    slotsToDrop.push(slotKey);
                } else {
                    const remainMinutes = Math.ceil((config.MIN_HOLD_MS - ageMs) / 60000);
                    console.log(`🔒 [STICKY] Giữ ${slot.symbol} ${slot.direction} dù không còn top | còn ${remainMinutes} phút`);
                }
            }
        }

        for (const slotKey of slotsToDrop) {
            await removeRadarSlot(slotKey, 'MIN_HOLD_EXPIRED');
        }

        await syncMacroScores();

        const elapsedSec = round((Date.now() - startedAt) / 1000, 2);
        console.log(`📊 [RADAR] Candidates=${candidates.length} | Top=${topCandidates.length} | Slots=${currentRadarSlots.size} | Time=${elapsedSec}s`);
    } catch (error) {
        console.error('❌ [RADAR] Lỗi scanRadar:', error.message);
    } finally {
        isScanning = false;
    }
}

// ============================================================
// 15. SCHEDULER CHỐNG TRÔI NHỊP
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
    console.log(`⏱️ [RADAR] Scan kế tiếp tại ${nextAt} | sau ${round(delay / 1000, 2)}s`);

    nextScanTimer = setTimeout(async () => {
        await scanRadar();
        scheduleNextScan();
    }, delay);
}

// ============================================================
// 16. KHỞI ĐỘNG
// ============================================================
console.log('👁️ RadarManager V19 - Impulse Radar 3M Hai Chiều 10/10 khởi động!');
console.log(`⚙️ [CONFIG] batch=${config.BATCH_SIZE}, delay=${config.BATCH_DELAY_MS}ms, projectedVolume=ON, scheduler=ALIGNED`);

scanRadar().finally(() => {
    scheduleNextScan();
});

setInterval(() => {
    syncMacroScores().catch(error => {
        console.error('❌ [RADAR] Lỗi syncMacroScores:', error.message);
    });
}, 15 * 60 * 1000);

// ============================================================
// 17. XỬ LÝ LỖI REDIS
// ============================================================
pubClient.on('error', err => {
    console.error('❌ Redis pubClient error:', err.message);
});

dataClient.on('error', err => {
    console.error('❌ Redis dataClient error:', err.message);
});

// ============================================================
// 18. GRACEFUL SHUTDOWN
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
