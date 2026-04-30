const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const fs = require('fs');
const path = require('path');
const ort = require('onnxruntime-node');
const mongoose = require('mongoose');

require('dotenv').config({ path: path.join(__dirname, '../../../.env') });

// ============================================================
// ORDER MANAGER V21 - DUAL BRAIN POLICY EXECUTOR
// ------------------------------------------------------------
// Mục tiêu:
// 1. Giữ nguyên luồng cũ: Redis features, macro:scores, subscriptions,
//    PaperExchange/LiveExchange, RiskGuard, Mongo restore.
// 2. Không sửa logic tính phí/lợi nhuận/ví trong Exchange.
//    Exchange vẫn là nơi tính fee, PnL, liquidation, wallet.
// 3. Nâng cấp "bộ não":
//    - LEGACY_2_CLASS: model cũ [SHORT, LONG].
//    - ACTION_4_CLASS: 1 model [FLAT, LONG, SHORT, EXIT].
//    - DUAL_BRAIN: EntryBrain [FLAT, LONG, SHORT]
//                  ExitBrain  [HOLD, EXIT]
// 4. OrderManager không còn là bộ não hard-code chính.
//    Nó là executor + safety shell:
//    - kiểm feature stale/spread/bad book
//    - max active trades
//    - cooldown
//    - risk guard
//    - emergency stop
//    - đồng bộ exchange closed/liquidation
// 5. Tương thích Feed V20:
//    - feature_ts
//    - schema_version
//    - mark_price
//    - mark_price_ma
//    - index_price
//    - local_taker_buy_ratio
//    - local_quote_volume
//    - trade_flow_imbalance
//    - spread_bps
//    - age fields
// 6. Tương thích model cũ 13 features.
// ============================================================


// ============================================================
// 1. LOAD CONFIG
// ============================================================
const ROOT_DIR = path.join(__dirname, '../../../');
const configPath = path.join(ROOT_DIR, 'system_config.json');

let config;
try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    console.error('❌ [FATAL] Thiếu hoặc lỗi file system_config.json:', e.message);
    process.exit(1);
}

const REDIS_URL = config.REDIS_URL || process.env.REDIS_URL || 'redis://localhost:6379';

const CHANNELS = {
    FEATURES: config.CHANNELS?.FEATURES || 'market:features:*',
    MACRO_SCORES: config.CHANNELS?.MACRO_SCORES || 'macro:scores',
    SUBSCRIPTIONS: config.CHANNELS?.SUBSCRIPTIONS || 'system:subscriptions',
    COMMANDS: config.CHANNELS?.COMMANDS || 'system:commands'
};

const TRADING = config.TRADING || {};

const EXCHANGE_MODE = String(
    process.env.EXCHANGE_MODE ||
    TRADING.EXCHANGE_MODE ||
    'PAPER'
).toUpperCase();

const BRAIN_MODE = String(
    process.env.BRAIN_MODE ||
    TRADING.BRAIN_MODE ||
    'LEGACY_2_CLASS'
).toUpperCase();

// Supported:
// LEGACY_2_CLASS
// ACTION_4_CLASS
// DUAL_BRAIN

const MAX_ACTIVE_TRADES = Number(TRADING.MAX_ACTIVE_TRADES || 3);

// Model paths.
const LEGACY_MODEL_PATH = path.resolve(
    ROOT_DIR,
    process.env.LEGACY_MODEL_PATH ||
    TRADING.LEGACY_MODEL_PATH ||
    'Universal_Scout.onnx'
);

const ENTRY_MODEL_PATH = path.resolve(
    ROOT_DIR,
    process.env.ENTRY_MODEL_PATH ||
    TRADING.ENTRY_MODEL_PATH ||
    'EntryBrain.onnx'
);

const EXIT_MODEL_PATH = path.resolve(
    ROOT_DIR,
    process.env.EXIT_MODEL_PATH ||
    TRADING.EXIT_MODEL_PATH ||
    'ExitBrain.onnx'
);

const ACTION_MODEL_PATH = path.resolve(
    ROOT_DIR,
    process.env.ACTION_MODEL_PATH ||
    TRADING.ACTION_MODEL_PATH ||
    'ActionBrain.onnx'
);

const MODEL_VERSION = process.env.MODEL_VERSION || TRADING.MODEL_VERSION || 'OrderManager_V21_DualBrain';

// Thresholds.
const LEGACY_LONG_THRESHOLD = Number(TRADING.AI_LONG_THRESHOLD || TRADING.LEGACY_LONG_THRESHOLD || 0.60);
const LEGACY_SHORT_THRESHOLD = Number(TRADING.AI_SHORT_THRESHOLD || TRADING.LEGACY_SHORT_THRESHOLD || 0.60);

const ENTRY_LONG_THRESHOLD = Number(TRADING.ENTRY_LONG_THRESHOLD || 0.56);
const ENTRY_SHORT_THRESHOLD = Number(TRADING.ENTRY_SHORT_THRESHOLD || 0.56);
const ENTRY_FLAT_THRESHOLD = Number(TRADING.ENTRY_FLAT_THRESHOLD || 0.45);

const ACTION_LONG_THRESHOLD = Number(TRADING.ACTION_LONG_THRESHOLD || 0.56);
const ACTION_SHORT_THRESHOLD = Number(TRADING.ACTION_SHORT_THRESHOLD || 0.56);
const ACTION_EXIT_THRESHOLD = Number(TRADING.ACTION_EXIT_THRESHOLD || 0.55);

const EXIT_THRESHOLD = Number(TRADING.EXIT_THRESHOLD || 0.55);
const HOLD_THRESHOLD = Number(TRADING.HOLD_THRESHOLD || 0.50);

const MIN_ENTRY_ACTION_GAP = Number(TRADING.MIN_ENTRY_ACTION_GAP || 0.08);
const MIN_EXIT_ACTION_GAP = Number(TRADING.MIN_EXIT_ACTION_GAP || 0.06);
const MIN_LEGACY_PROB_GAP = Number(TRADING.MIN_LEGACY_PROB_GAP || 0.10);

const MIN_MACRO_SCORE = Number(TRADING.MIN_MACRO_SCORE || 0.40);

// Feature validation.
const MAX_SPREAD_CLOSE = Number(TRADING.MAX_SPREAD_CLOSE || 0.0025);
const MAX_SPREAD_BPS = Number(TRADING.MAX_SPREAD_BPS || 25);
const MAX_ATR_PERCENT = Number(TRADING.MAX_ATR_PERCENT || 0.20);
const MIN_ATR_PERCENT = Number(TRADING.MIN_ATR_PERCENT || 0.00001);
const DEFAULT_ATR_PERCENT = Number(TRADING.DEFAULT_ATR_PERCENT || 0.003);

const MAX_FEATURE_AGE_MS = Number(TRADING.MAX_FEATURE_AGE_MS || 3500);
const MAX_PRICE_AGE_MS = Number(TRADING.MAX_PRICE_AGE_MS || 9000);
const MAX_DEPTH_AGE_MS = Number(TRADING.MAX_DEPTH_AGE_MS || 3000);
const MAX_MARK_PRICE_AGE_MS = Number(TRADING.MAX_MARK_PRICE_AGE_MS || 12000);
const REQUIRE_MARK_PRICE = TRADING.REQUIRE_MARK_PRICE === true;

// Cooldowns.
const COOLDOWN_AFTER_CLOSE_MS = Number(TRADING.COOLDOWN_AFTER_CLOSE_MS || 15 * 60 * 1000);
const COOLDOWN_AFTER_FAIL_MS = Number(TRADING.COOLDOWN_AFTER_FAIL_MS || 60 * 1000);
const COOLDOWN_AFTER_LIQUIDATION_MS = Number(TRADING.COOLDOWN_AFTER_LIQUIDATION_MS || 30 * 60 * 1000);
const COOLDOWN_CLEAN_INTERVAL_MS = Number(TRADING.COOLDOWN_CLEAN_INTERVAL_MS || 60 * 1000);
const PREDICTION_EMIT_INTERVAL_MS = Number(TRADING.PREDICTION_EMIT_INTERVAL_MS || 1000);
const MACRO_SYNC_INTERVAL_MS = Number(TRADING.MACRO_SYNC_INTERVAL_MS || 5000);

// Entry controls.
const ENTRY_CONFIRM_TICKS = Number(TRADING.ENTRY_CONFIRM_TICKS || 2);
const ENTRY_CONFIRM_TTL_MS = Number(TRADING.ENTRY_CONFIRM_TTL_MS || 15 * 1000);
const MIN_SECONDS_AFTER_SUBSCRIBE = Number(TRADING.MIN_SECONDS_AFTER_SUBSCRIBE || 2);
const COUNTER_RADAR_MIN_PROB = Number(TRADING.COUNTER_RADAR_MIN_PROB || 0.70);

// Safety exits.
// Đây là lớp sinh tồn, không phải bộ não chính.
const EMERGENCY_STOP_ROI = Number(TRADING.EMERGENCY_STOP_ROI || -10);
const MAX_HOLD_MS = Number(TRADING.MAX_HOLD_MS || 15 * 60 * 1000);
const MIN_HOLD_BEFORE_ANY_EXIT_MS = Number(TRADING.MIN_HOLD_BEFORE_ANY_EXIT_MS || 5 * 1000);
const MIN_HOLD_BEFORE_MODEL_EXIT_MS = Number(TRADING.MIN_HOLD_BEFORE_MODEL_EXIT_MS || 10 * 1000);

// Rule backup exit là tùy chọn.
// Nếu muốn ONNX quyết định exit gần như toàn bộ, để false.
const RULE_EXIT_BACKUP_ENABLED = TRADING.RULE_EXIT_BACKUP_ENABLED === true;
const TAKE_PROFIT_ROI = Number(TRADING.TAKE_PROFIT_ROI || 12);
const SOFT_STOP_ROI = Number(TRADING.SOFT_STOP_ROI || -6);
const TRAILING_ACTIVATE_ROI = Number(TRADING.TRAILING_ACTIVATE_ROI || 6);
const TRAILING_GIVEBACK_ROI = Number(TRADING.TRAILING_GIVEBACK_ROI || 3);

// Leverage.
const MIN_LEVERAGE = Number(TRADING.MIN_LEVERAGE || 3);
const MAX_LEVERAGE = Number(TRADING.MAX_LEVERAGE || 20);
const DEFAULT_LEVERAGE = Number(TRADING.DEFAULT_LEVERAGE || 8);
const USE_PROBABILITY_LEVERAGE = TRADING.USE_PROBABILITY_LEVERAGE !== false;

// EV switch.
const EV_SWITCH_ENABLED = TRADING.EV_SWITCH_ENABLED !== false;
const EV_SWITCH_MIN_PROB_DELTA = Number(TRADING.EV_SWITCH_MIN_PROB_DELTA || 0.14);
const EV_SWITCH_PROTECT_PROFIT_ROI = Number(TRADING.EV_SWITCH_PROTECT_PROFIT_ROI || 6);

// Fallbacks.
const FALLBACK_TO_LEGACY_IF_NEW_BRAIN_MISSING = TRADING.FALLBACK_TO_LEGACY_IF_NEW_BRAIN_MISSING !== false;
const ALLOW_ENTRY_WITH_LEGACY_IN_DUAL_MODE = TRADING.ALLOW_ENTRY_WITH_LEGACY_IN_DUAL_MODE === true;


// ============================================================
// 2. EXCHANGE SELECTOR
// ------------------------------------------------------------
// Không sửa fee/PnL/wallet logic.
// PaperExchange/LiveExchange vẫn chịu trách nhiệm mô phỏng/thực thi.
// ============================================================
let exchange;
if (EXCHANGE_MODE === 'LIVE' || EXCHANGE_MODE === 'LIVE_DRY_RUN') {
    exchange = require('./LiveExchange');
} else {
    exchange = require('./PaperExchange');
}

let riskGuard = null;
try {
    riskGuard = require('./RiskGuard');
} catch (e) {
    riskGuard = null;
}


// ============================================================
// 3. MONGO INIT
// ============================================================
let mongoReady = false;
if (process.env.MONGO_URI_SCOUT) {
    mongoose.connect(process.env.MONGO_URI_SCOUT)
        .then(() => {
            mongoReady = true;
            console.log('📦 [MONGO] Order Manager connected');
        })
        .catch(err => console.error('❌ [MONGO] Connect error:', err.message));
} else {
    console.warn('⚠️ [MONGO] Missing MONGO_URI_SCOUT. Trade persistence may fail.');
}


// ============================================================
// 4. REDIS CLIENTS
// ============================================================
const subClient = new Redis(REDIS_URL);
const cmdClient = new Redis(REDIS_URL);
const pubClient = new Redis(REDIS_URL);
const dataClient = new Redis(REDIS_URL);

for (const [name, client] of Object.entries({ subClient, cmdClient, pubClient, dataClient })) {
    client.on('error', err => console.error(`❌ [REDIS ${name}]`, err.message));
}


// ============================================================
// 5. MEMORY STATE
// ============================================================
const activeTrades = new Map();
const latestFeatures = new Map();
const macroCache = new Map();
const cooldowns = new Map();
const isExecuting = new Set();
const predictionEmitTimestamps = new Map();
const recentlyClosed = new Map();
const entryConfirmState = new Map();

const RECENT_CLOSE_TTL_MS = 10 * 1000;

let tradingPaused = false;
let pauseReason = '';

const omStats = {
    startedAt: Date.now(),
    features: 0,
    invalidFeatures: 0,
    legacyPredictions: 0,
    entryPredictions: 0,
    exitPredictions: 0,
    actionPredictions: 0,
    entriesOpened: 0,
    exitsClosed: 0,
    entryRejected: 0,
    exitRejected: 0,
    riskBlocked: 0
};


// ============================================================
// 6. UTILS
// ============================================================
function safeNumber(value, fallback = 0) {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
}

function normalizeSymbol(symbol) {
    return String(symbol || '').toUpperCase().trim();
}

function nowMs() {
    return Date.now();
}

function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
}

function fileExists(p) {
    try {
        return fs.existsSync(p);
    } catch (e) {
        return false;
    }
}

function readJsonIfExists(p, fallback = null) {
    try {
        if (!fileExists(p)) return fallback;
        return JSON.parse(fs.readFileSync(p, 'utf8'));
    } catch (e) {
        console.warn(`⚠️ [JSON] Không đọc được ${p}: ${e.message}`);
        return fallback;
    }
}

function resolveFeatureListPath(modelPath) {
    // Hỗ trợ:
    // EntryBrain.onnx.features.json
    // EntryBrain.features.json
    const p1 = `${modelPath}.features.json`;
    const p2 = modelPath.replace(/\.onnx$/i, '.features.json');

    if (fileExists(p1)) return p1;
    if (fileExists(p2)) return p2;

    return null;
}

function logThought(symbol, message) {
    const ts = nowMs();
    const sym = normalizeSymbol(symbol) || 'SYSTEM';
    console.log(`[${sym}] ${message}`);

    try {
        pubClient.publish('dashboard:logs', JSON.stringify({
            symbol: sym,
            msg: message,
            ts
        }));
    } catch (e) {}
}

function syncStream(symbol, action) {
    const sym = normalizeSymbol(symbol);
    if (!sym || !action) return;

    try {
        pubClient.publish(CHANNELS.SUBSCRIPTIONS, JSON.stringify({
            action,
            symbol: sym,
            client: 'oms'
        }));
    } catch (e) {}
}

function setCooldown(symbol, durationMs, reason = '') {
    const sym = normalizeSymbol(symbol);
    if (!sym || durationMs <= 0) return;

    cooldowns.set(sym, nowMs() + durationMs);

    if (reason) {
        logThought(sym, `⏳ [COOLDOWN] ${Math.round(durationMs / 1000)}s | ${reason}`);
    }
}

function markRecentlyClosed(symbol) {
    recentlyClosed.set(normalizeSymbol(symbol), nowMs() + RECENT_CLOSE_TTL_MS);
}

function isRecentlyClosed(symbol) {
    const sym = normalizeSymbol(symbol);
    const until = recentlyClosed.get(sym);
    return until && nowMs() < until;
}

function cleanupRecentClosed() {
    const now = nowMs();

    for (const [sym, until] of recentlyClosed.entries()) {
        if (now > until) recentlyClosed.delete(sym);
    }
}

function getCurrentRoiPercent(trade, currentPrice) {
    if (!trade || !trade.entryPrice || !currentPrice) return 0;

    const priceMovePct = trade.type === 'LONG'
        ? ((currentPrice - trade.entryPrice) / trade.entryPrice) * 100
        : ((trade.entryPrice - currentPrice) / trade.entryPrice) * 100;

    return priceMovePct * safeNumber(trade.leverage, 1);
}

function getFeatureTimestampMs(feature) {
    const candidates = [
        feature?.feature_ts,
        feature?.ts,
        feature?.timestamp,
        feature?.event_time,
        feature?.last_update_ts
    ];

    for (const value of candidates) {
        const n = safeNumber(value, 0);
        if (n > 0) {
            return n < 1e12 ? Math.round(n * 1000) : Math.round(n);
        }
    }

    return 0;
}

function getFeatureAgeMs(feature) {
    const ts = getFeatureTimestampMs(feature);
    if (ts <= 0) return null;
    return Math.max(0, nowMs() - ts);
}

function getRadarDirection(feature) {
    const raw =
        feature?.radarDirection ||
        feature?.radar_direction ||
        feature?.suggestedDirection ||
        feature?.suggested_direction ||
        feature?.side ||
        feature?.signal_side ||
        feature?.impulseSide ||
        feature?.impulse_side;

    const v = String(raw || '').toUpperCase();

    if (v === 'LONG' || v === 'SHORT') return v;
    return null;
}

function getEvalPrice(feature) {
    // ROI/liq gần thực tế futures nên ưu tiên mark_price.
    const mark = safeNumber(feature?.mark_price, 0);
    if (mark > 0) return mark;

    return safeNumber(feature?.last_price, 0);
}

function getEntryReferencePrice(feature, side) {
    // Không thay Exchange. Chỉ truyền giá tham chiếu sát market hơn:
    // LONG market thường khớp gần ask.
    // SHORT market thường khớp gần bid.
    const bid = safeNumber(feature?.best_bid, 0);
    const ask = safeNumber(feature?.best_ask, 0);
    const last = safeNumber(feature?.last_price, 0);

    if (side === 'LONG' && ask > 0) return ask;
    if (side === 'SHORT' && bid > 0) return bid;

    return last;
}

function getExitReferencePrice(feature, side) {
    // Đóng LONG bán ra gần bid.
    // Đóng SHORT mua lại gần ask.
    const bid = safeNumber(feature?.best_bid, 0);
    const ask = safeNumber(feature?.best_ask, 0);
    const last = safeNumber(feature?.last_price, 0);

    if (side === 'LONG' && bid > 0) return bid;
    if (side === 'SHORT' && ask > 0) return ask;

    return last;
}

function calculateLeverageFromProb(prob) {
    if (!USE_PROBABILITY_LEVERAGE) return DEFAULT_LEVERAGE;

    // Mapping bảo thủ:
    // 0.50 -> min
    // 0.75+ -> max
    const edge = clamp((prob - 0.50) / 0.25, 0, 1);
    const lev = Math.round(MIN_LEVERAGE + edge * (MAX_LEVERAGE - MIN_LEVERAGE));

    return clamp(lev, MIN_LEVERAGE, MAX_LEVERAGE);
}


// ============================================================
// 7. FEATURE VECTORS
// ------------------------------------------------------------
// LEGACY_13 giữ y nguyên model cũ.
// Enhanced feature list sẽ lấy từ *.features.json nếu trainer tạo ra.
// Nếu không có list, fallback về LEGACY_13 để không crash.
// ============================================================
const LEGACY_13_FEATURES = [
    'ob_imb_top20',
    'spread_close',
    'bid_vol_1pct',
    'ask_vol_1pct',
    'max_buy_trade',
    'max_sell_trade',
    'liq_long_vol',
    'liq_short_vol',
    'funding_rate',
    'taker_buy_ratio',
    'body_size',
    'wick_size',
    'btc_relative_strength'
];

const ENHANCED_MARKET_FALLBACK_FEATURES = [
    ...LEGACY_13_FEATURES,

    'mark_price',
    'mark_price_ma',
    'index_price',
    'spread_bps',

    'coin_pct_local',
    'btc_pct_local',
    'local_quote_volume',
    'local_buy_quote',
    'local_sell_quote',
    'local_trade_count',
    'local_taker_buy_ratio',
    'taker_buy_ratio_24h',
    'trade_flow_imbalance',

    'max_buy_trade_usdt',
    'max_sell_trade_usdt',
    'max_buy_trade_local_norm',
    'max_sell_trade_local_norm',

    'liq_long_usdt',
    'liq_short_usdt',
    'liq_long_local_norm',
    'liq_short_local_norm',

    'ATR14',
    'VPIN',
    'OFI',
    'MFA',
    'WHALE_NET',
    'WHALE_NET_LOCAL',

    'price_age_ms',
    'depth_age_ms',
    'trade_age_ms',
    'mark_price_age_ms'
];

const POSITION_EXTRA_FEATURES = [
    'position_side_long',
    'position_side_short',
    'position_roi',
    'position_best_roi',
    'position_worst_roi',
    'position_giveback_roi',
    'position_hold_sec',
    'position_leverage',
    'position_entry_distance_pct'
];

const ENHANCED_POSITION_FALLBACK_FEATURES = [
    ...ENHANCED_MARKET_FALLBACK_FEATURES,
    ...POSITION_EXTRA_FEATURES
];

function getFeatureValueByName(feature, name, positionContext = null) {
    if (!name) return 0;

    if (positionContext && Object.prototype.hasOwnProperty.call(positionContext, name)) {
        return safeNumber(positionContext[name], 0);
    }

    if (Object.prototype.hasOwnProperty.call(feature, name)) {
        return safeNumber(feature[name], 0);
    }

    // Aliases để tương thích dữ liệu cũ/mới.
    switch (name) {
        case 'spread_bps':
            return safeNumber(feature.spread_bps, safeNumber(feature.spread_close, 0) * 10000);

        case 'local_taker_buy_ratio':
            return safeNumber(feature.local_taker_buy_ratio, safeNumber(feature.taker_buy_ratio, 0.5));

        case 'taker_buy_ratio_24h':
            return safeNumber(feature.taker_buy_ratio_24h, safeNumber(feature.taker_buy_ratio, 0.5));

        case 'coin_pct_local':
            return safeNumber(feature.coin_pct_local, 0);

        case 'btc_pct_local':
            return safeNumber(feature.btc_pct_local, 0);

        case 'mark_price':
            return safeNumber(feature.mark_price, safeNumber(feature.last_price, 0));

        case 'mark_price_ma':
            return safeNumber(feature.mark_price_ma, safeNumber(feature.mark_price, safeNumber(feature.last_price, 0)));

        case 'index_price':
            return safeNumber(feature.index_price, safeNumber(feature.mark_price, safeNumber(feature.last_price, 0)));

        case 'price_age_ms':
            return safeNumber(feature.price_age_ms, getFeatureAgeMs(feature) || 0);

        default:
            return 0;
    }
}

function buildVectorFromFeatureList(feature, featureList, positionContext = null) {
    const arr = featureList.map(name => {
        const value = getFeatureValueByName(feature, name, positionContext);
        if (!Number.isFinite(value)) return 0;
        return value;
    });

    return Float32Array.from(arr);
}

function buildLegacy13Vector(feature) {
    return buildVectorFromFeatureList(feature, LEGACY_13_FEATURES);
}

function buildEnhancedMarketVector(feature, featureList = null) {
    return buildVectorFromFeatureList(
        feature,
        featureList && featureList.length ? featureList : ENHANCED_MARKET_FALLBACK_FEATURES
    );
}

function buildPositionContext(trade, feature) {
    const evalPrice = getEvalPrice(feature);
    const entryPrice = safeNumber(trade?.entryPrice, evalPrice);
    const currentRoi = getCurrentRoiPercent(trade, evalPrice);
    const bestRoi = safeNumber(trade?.bestRoi, currentRoi);
    const worstRoi = safeNumber(trade?.worstRoi, currentRoi);
    const holdSec = Math.max(0, (nowMs() - safeNumber(trade?.openTime, nowMs())) / 1000);
    const leverage = safeNumber(trade?.leverage, 1);
    const side = trade?.type === 'SHORT' ? 'SHORT' : 'LONG';

    const entryDistancePct = entryPrice > 0
        ? side === 'LONG'
            ? ((evalPrice - entryPrice) / entryPrice) * 100
            : ((entryPrice - evalPrice) / entryPrice) * 100
        : 0;

    return {
        position_side_long: side === 'LONG' ? 1 : 0,
        position_side_short: side === 'SHORT' ? 1 : 0,
        position_roi: currentRoi,
        position_best_roi: bestRoi,
        position_worst_roi: worstRoi,
        position_giveback_roi: bestRoi - currentRoi,
        position_hold_sec: holdSec,
        position_leverage: leverage,
        position_entry_distance_pct: entryDistancePct
    };
}

function buildEnhancedPositionVector(feature, trade, featureList = null) {
    const positionContext = buildPositionContext(trade, feature);

    return buildVectorFromFeatureList(
        feature,
        featureList && featureList.length ? featureList : ENHANCED_POSITION_FALLBACK_FEATURES,
        positionContext
    );
}


// ============================================================
// 8. FEATURE VALIDATION
// ============================================================
function isFeatureValid(feature) {
    if (!feature || typeof feature !== 'object') {
        return { ok: false, reason: 'EMPTY_FEATURE' };
    }

    if (!feature.is_warm) {
        return { ok: false, reason: 'NOT_WARM' };
    }

    const symbol = normalizeSymbol(feature.symbol);
    if (!symbol) {
        return { ok: false, reason: 'NO_SYMBOL' };
    }

    const price = safeNumber(feature.last_price, 0);
    if (price <= 0) {
        return { ok: false, reason: 'BAD_PRICE' };
    }

    const bid = safeNumber(feature.best_bid, 0);
    const ask = safeNumber(feature.best_ask, 0);

    if (bid <= 0 || ask <= 0 || ask <= bid) {
        return { ok: false, reason: 'BAD_BOOK' };
    }

    const spread = safeNumber(feature.spread_close, 0);
    const spreadBps = safeNumber(feature.spread_bps, spread * 10000);

    if (spread < 0 || spread > MAX_SPREAD_CLOSE) {
        return { ok: false, reason: `SPREAD_TOO_WIDE_${spread}` };
    }

    if (spreadBps < 0 || spreadBps > MAX_SPREAD_BPS) {
        return { ok: false, reason: `SPREAD_BPS_TOO_WIDE_${spreadBps}` };
    }

    const atr = safeNumber(feature.ATR14, DEFAULT_ATR_PERCENT);
    if (atr < MIN_ATR_PERCENT || atr > MAX_ATR_PERCENT) {
        return { ok: false, reason: `BAD_ATR_${atr}` };
    }

    const takerBuyRatio = safeNumber(feature.taker_buy_ratio, 0.5);
    if (takerBuyRatio < 0 || takerBuyRatio > 1) {
        return { ok: false, reason: 'BAD_TAKER_RATIO' };
    }

    const featureAgeMs = getFeatureAgeMs(feature);
    if (featureAgeMs !== null && featureAgeMs > MAX_FEATURE_AGE_MS) {
        return { ok: false, reason: `FEATURE_STALE_${featureAgeMs}ms` };
    }

    const priceAgeMs = safeNumber(feature.price_age_ms, -1);
    if (priceAgeMs >= 0 && priceAgeMs > MAX_PRICE_AGE_MS) {
        return { ok: false, reason: `PRICE_STALE_${priceAgeMs}ms` };
    }

    const depthAgeMs = safeNumber(feature.depth_age_ms, -1);
    if (depthAgeMs >= 0 && depthAgeMs > MAX_DEPTH_AGE_MS) {
        return { ok: false, reason: `DEPTH_STALE_${depthAgeMs}ms` };
    }

    const markPrice = safeNumber(feature.mark_price, 0);
    const markAgeMs = safeNumber(feature.mark_price_age_ms, -1);

    if (REQUIRE_MARK_PRICE && markPrice <= 0) {
        return { ok: false, reason: 'NO_MARK_PRICE' };
    }

    if (markAgeMs >= 0 && markAgeMs > MAX_MARK_PRICE_AGE_MS) {
        return { ok: false, reason: `MARK_STALE_${markAgeMs}ms` };
    }

    const vector = buildLegacy13Vector(feature);
    for (const value of vector) {
        if (!Number.isFinite(value)) {
            return { ok: false, reason: 'NON_FINITE_LEGACY_VECTOR' };
        }
    }

    return { ok: true };
}


// ============================================================
// 9. MODEL LOADER / INFERENCE
// ============================================================
const modelState = {
    legacy: null,
    entry: null,
    exit: null,
    action: null
};

async function loadOnnxModel(label, modelPath, defaultFeatureList) {
    if (!fileExists(modelPath)) {
        console.warn(`⚠️ [AI ${label}] Model không tồn tại: ${modelPath}`);
        return null;
    }

    try {
        const session = await ort.InferenceSession.create(modelPath);
        const inputName = session.inputNames[0];
        const outputName = session.outputNames[session.outputNames.length - 1];

        if (!inputName || !outputName) {
            throw new Error('Không tìm thấy input/output name');
        }

        const featureListPath = resolveFeatureListPath(modelPath);
        let featureList = readJsonIfExists(featureListPath, null);

        if (featureList && !Array.isArray(featureList)) {
            // Một số trainer lưu dạng {features: [...]}
            featureList = Array.isArray(featureList.features) ? featureList.features : null;
        }

        if (!featureList || featureList.length === 0) {
            featureList = defaultFeatureList || LEGACY_13_FEATURES;
        }

        console.log(
            `🧠 [AI ${label}] Online | model=${path.basename(modelPath)} | input=${inputName} | output=${outputName} | features=${featureList.length}`
        );

        return {
            label,
            modelPath,
            session,
            inputName,
            outputName,
            featureList
        };
    } catch (e) {
        console.error(`❌ [AI ${label}] Load error ${modelPath}: ${e.message}`);
        return null;
    }
}

async function initAI() {
    modelState.legacy = null;
    modelState.entry = null;
    modelState.exit = null;
    modelState.action = null;

    // Legacy luôn cố load để fallback.
    modelState.legacy = await loadOnnxModel(
        'LEGACY',
        LEGACY_MODEL_PATH,
        LEGACY_13_FEATURES
    );

    if (BRAIN_MODE === 'DUAL_BRAIN') {
        modelState.entry = await loadOnnxModel(
            'ENTRY',
            ENTRY_MODEL_PATH,
            ENHANCED_MARKET_FALLBACK_FEATURES
        );

        modelState.exit = await loadOnnxModel(
            'EXIT',
            EXIT_MODEL_PATH,
            ENHANCED_POSITION_FALLBACK_FEATURES
        );

        if (!modelState.entry && !FALLBACK_TO_LEGACY_IF_NEW_BRAIN_MISSING) {
            throw new Error('BRAIN_MODE=DUAL_BRAIN nhưng không load được EntryBrain');
        }

        if (!modelState.exit) {
            console.warn('⚠️ [AI EXIT] Không có ExitBrain. OM sẽ chỉ dùng emergency/maxHold/rule backup nếu bật.');
        }
    }

    if (BRAIN_MODE === 'ACTION_4_CLASS') {
        modelState.action = await loadOnnxModel(
            'ACTION4',
            ACTION_MODEL_PATH,
            ENHANCED_POSITION_FALLBACK_FEATURES
        );

        if (!modelState.action && !FALLBACK_TO_LEGACY_IF_NEW_BRAIN_MISSING) {
            throw new Error('BRAIN_MODE=ACTION_4_CLASS nhưng không load được ActionBrain');
        }
    }

    if (BRAIN_MODE === 'LEGACY_2_CLASS' && !modelState.legacy) {
        throw new Error('BRAIN_MODE=LEGACY_2_CLASS nhưng không load được Universal_Scout.onnx');
    }
}

async function runOnnx(model, vector) {
    if (!model || !model.session) return null;

    const tensor = new ort.Tensor('float32', vector, [1, vector.length]);
    const feeds = {};
    feeds[model.inputName] = tensor;

    const output = await model.session.run(feeds);
    const outTensor = output[model.outputName];

    if (!outTensor || !outTensor.data) return null;

    const arr = Array.from(outTensor.data).map(v => safeNumber(v, NaN));
    if (!arr.length || arr.some(v => !Number.isFinite(v))) return null;

    return arr;
}

function normalizeProbArray(arr) {
    if (!Array.isArray(arr) || arr.length === 0) return [];

    // Nếu đã là probability hợp lệ.
    const sum = arr.reduce((a, b) => a + b, 0);
    const allPositive = arr.every(v => v >= 0);

    if (allPositive && sum > 0.98 && sum < 1.02) {
        return arr.map(v => clamp(v, 0, 1));
    }

    // Softmax fallback cho logits.
    const max = Math.max(...arr);
    const exps = arr.map(v => Math.exp(v - max));
    const expSum = exps.reduce((a, b) => a + b, 0);

    if (expSum <= 0) return arr.map(() => 0);

    return exps.map(v => v / expSum);
}

function parseLegacyOutput(arr) {
    const probs = normalizeProbArray(arr);
    if (!probs || probs.length < 2) return null;

    // Mapping cũ: [SHORT, LONG].
    return {
        probShort: safeNumber(probs[0], 0),
        probLong: safeNumber(probs[1], 0)
    };
}

function parseEntryOutput(arr) {
    const probs = normalizeProbArray(arr);

    if (!probs || probs.length < 3) return null;

    // EntryBrain: [FLAT, LONG, SHORT].
    return {
        probFlat: safeNumber(probs[0], 0),
        probLong: safeNumber(probs[1], 0),
        probShort: safeNumber(probs[2], 0)
    };
}

function parseExitOutput(arr) {
    const probs = normalizeProbArray(arr);

    if (!probs || probs.length < 2) return null;

    // ExitBrain: [HOLD, EXIT].
    return {
        probHold: safeNumber(probs[0], 0),
        probExit: safeNumber(probs[1], 0)
    };
}

function parseAction4Output(arr) {
    const probs = normalizeProbArray(arr);

    if (!probs || probs.length < 4) return null;

    // ActionBrain: [FLAT, LONG, SHORT, EXIT].
    return {
        probFlat: safeNumber(probs[0], 0),
        probLong: safeNumber(probs[1], 0),
        probShort: safeNumber(probs[2], 0),
        probExit: safeNumber(probs[3], 0)
    };
}

async function runLegacyBrain(feature) {
    if (!modelState.legacy) return null;

    const vector = buildLegacy13Vector(feature);
    const arr = await runOnnx(modelState.legacy, vector);
    const parsed = parseLegacyOutput(arr);

    if (parsed) omStats.legacyPredictions += 1;

    return parsed;
}

async function runEntryBrain(feature) {
    if (!modelState.entry) return null;

    const vector = buildEnhancedMarketVector(feature, modelState.entry.featureList);
    const arr = await runOnnx(modelState.entry, vector);
    const parsed = parseEntryOutput(arr);

    if (parsed) omStats.entryPredictions += 1;

    return parsed;
}

async function runExitBrain(feature, trade) {
    if (!modelState.exit || !trade) return null;

    const vector = buildEnhancedPositionVector(feature, trade, modelState.exit.featureList);
    const arr = await runOnnx(modelState.exit, vector);
    const parsed = parseExitOutput(arr);

    if (parsed) omStats.exitPredictions += 1;

    return parsed;
}

async function runActionBrain(feature, trade = null) {
    if (!modelState.action) return null;

    let vector;

    // ActionBrain có thể dùng position context nếu đang có lệnh.
    if (trade) {
        vector = buildEnhancedPositionVector(feature, trade, modelState.action.featureList);
    } else {
        vector = buildEnhancedMarketVector(feature, modelState.action.featureList);
    }

    const arr = await runOnnx(modelState.action, vector);
    const parsed = parseAction4Output(arr);

    if (parsed) omStats.actionPredictions += 1;

    return parsed;
}


// ============================================================
// 10. DECISION HELPERS
// ============================================================
function makeFlatDecision(reason, raw = null) {
    return {
        action: 'FLAT',
        side: null,
        prob: 0,
        reason,
        raw
    };
}

function decideEntryFromLegacy(legacy) {
    if (!legacy) return makeFlatDecision('NO_LEGACY_OUTPUT');

    const { probLong, probShort } = legacy;
    const gap = Math.abs(probLong - probShort);

    if (gap < MIN_LEGACY_PROB_GAP) {
        return makeFlatDecision(`LEGACY_GAP_TOO_SMALL_${gap.toFixed(3)}`, legacy);
    }

    if (probLong >= LEGACY_LONG_THRESHOLD && probLong > probShort) {
        return {
            action: 'ENTER',
            side: 'LONG',
            prob: probLong,
            reason: `LEGACY_LONG_${probLong.toFixed(3)}`,
            raw: legacy
        };
    }

    if (probShort >= LEGACY_SHORT_THRESHOLD && probShort > probLong) {
        return {
            action: 'ENTER',
            side: 'SHORT',
            prob: probShort,
            reason: `LEGACY_SHORT_${probShort.toFixed(3)}`,
            raw: legacy
        };
    }

    return makeFlatDecision('LEGACY_NO_ACTION', legacy);
}

function decideEntryFromEntryBrain(entry) {
    if (!entry) return makeFlatDecision('NO_ENTRY_OUTPUT');

    const { probFlat, probLong, probShort } = entry;

    const best = Math.max(probFlat, probLong, probShort);
    const sorted = [probFlat, probLong, probShort].sort((a, b) => b - a);
    const gap = sorted[0] - sorted[1];

    if (best === probFlat && probFlat >= ENTRY_FLAT_THRESHOLD) {
        return makeFlatDecision(`ENTRY_FLAT_${probFlat.toFixed(3)}`, entry);
    }

    if (gap < MIN_ENTRY_ACTION_GAP) {
        return makeFlatDecision(`ENTRY_GAP_TOO_SMALL_${gap.toFixed(3)}`, entry);
    }

    if (probLong >= ENTRY_LONG_THRESHOLD && probLong > probShort && probLong > probFlat) {
        return {
            action: 'ENTER',
            side: 'LONG',
            prob: probLong,
            reason: `ENTRY_LONG_${probLong.toFixed(3)} flat=${probFlat.toFixed(3)}`,
            raw: entry
        };
    }

    if (probShort >= ENTRY_SHORT_THRESHOLD && probShort > probLong && probShort > probFlat) {
        return {
            action: 'ENTER',
            side: 'SHORT',
            prob: probShort,
            reason: `ENTRY_SHORT_${probShort.toFixed(3)} flat=${probFlat.toFixed(3)}`,
            raw: entry
        };
    }

    return makeFlatDecision('ENTRY_NO_ACTION', entry);
}

function decideEntryFromAction4(action4) {
    if (!action4) return makeFlatDecision('NO_ACTION4_OUTPUT');

    const { probFlat, probLong, probShort, probExit } = action4;

    const best = Math.max(probFlat, probLong, probShort, probExit);
    const sorted = [probFlat, probLong, probShort, probExit].sort((a, b) => b - a);
    const gap = sorted[0] - sorted[1];

    if (best === probFlat) {
        return makeFlatDecision(`ACTION4_FLAT_${probFlat.toFixed(3)}`, action4);
    }

    // Khi chưa có lệnh, EXIT được xem là FLAT.
    if (best === probExit) {
        return makeFlatDecision(`ACTION4_EXIT_WHILE_FLAT_${probExit.toFixed(3)}`, action4);
    }

    if (gap < MIN_ENTRY_ACTION_GAP) {
        return makeFlatDecision(`ACTION4_ENTRY_GAP_TOO_SMALL_${gap.toFixed(3)}`, action4);
    }

    if (probLong >= ACTION_LONG_THRESHOLD && probLong > probShort && probLong > probFlat) {
        return {
            action: 'ENTER',
            side: 'LONG',
            prob: probLong,
            reason: `ACTION4_LONG_${probLong.toFixed(3)}`,
            raw: action4
        };
    }

    if (probShort >= ACTION_SHORT_THRESHOLD && probShort > probLong && probShort > probFlat) {
        return {
            action: 'ENTER',
            side: 'SHORT',
            prob: probShort,
            reason: `ACTION4_SHORT_${probShort.toFixed(3)}`,
            raw: action4
        };
    }

    return makeFlatDecision('ACTION4_NO_ENTRY', action4);
}

function decideExitFromExitBrain(exitDecision) {
    if (!exitDecision) {
        return {
            shouldExit: false,
            reason: 'NO_EXIT_OUTPUT',
            raw: null
        };
    }

    const { probHold, probExit } = exitDecision;
    const gap = probExit - probHold;

    if (probExit >= EXIT_THRESHOLD && gap >= MIN_EXIT_ACTION_GAP) {
        return {
            shouldExit: true,
            prob: probExit,
            reason: `EXIT_BRAIN probExit=${probExit.toFixed(3)} hold=${probHold.toFixed(3)} gap=${gap.toFixed(3)}`,
            raw: exitDecision
        };
    }

    return {
        shouldExit: false,
        prob: probExit,
        reason: `EXIT_BRAIN_HOLD exit=${probExit.toFixed(3)} hold=${probHold.toFixed(3)}`,
        raw: exitDecision
    };
}

function decideExitFromAction4(action4) {
    if (!action4) {
        return {
            shouldExit: false,
            reason: 'NO_ACTION4_EXIT_OUTPUT',
            raw: null
        };
    }

    const { probFlat, probLong, probShort, probExit } = action4;
    const holdLike = Math.max(probFlat, probLong, probShort);
    const gap = probExit - holdLike;

    if (probExit >= ACTION_EXIT_THRESHOLD && gap >= MIN_EXIT_ACTION_GAP) {
        return {
            shouldExit: true,
            prob: probExit,
            reason: `ACTION4_EXIT probExit=${probExit.toFixed(3)} gap=${gap.toFixed(3)}`,
            raw: action4
        };
    }

    return {
        shouldExit: false,
        prob: probExit,
        reason: `ACTION4_HOLD exit=${probExit.toFixed(3)} holdLike=${holdLike.toFixed(3)}`,
        raw: action4
    };
}


// ============================================================
// 11. DASHBOARD / EXPERIENCE LOGS
// ============================================================
function emitPrediction(symbol, payload) {
    const sym = normalizeSymbol(symbol);
    const now = nowMs();
    const lastEmit = predictionEmitTimestamps.get(sym) || 0;

    if (now - lastEmit < PREDICTION_EMIT_INTERVAL_MS) return;

    try {
        pubClient.publish('dashboard:predictions', JSON.stringify({
            symbol: sym,
            ts: now,
            ...payload
        }));

        predictionEmitTimestamps.set(sym, now);
    } catch (e) {}
}

function logDecision(symbol, type, payload) {
    try {
        pubClient.publish('decision:raw', JSON.stringify({
            symbol: normalizeSymbol(symbol),
            type,
            ts: nowMs(),
            ...payload
        }));
    } catch (e) {}
}

function logExperience(symbol, tradeType, entryPrice, exitPrice, prob, features, reason, extra = {}) {
    const sym = normalizeSymbol(symbol);

    if (!sym || !entryPrice || !exitPrice) return;

    const pnlPct = tradeType === 'LONG'
        ? ((exitPrice - entryPrice) / entryPrice) * 100
        : ((entryPrice - exitPrice) / entryPrice) * 100;

    try {
        pubClient.publish('experience:raw', JSON.stringify({
            symbol: sym,
            ts_exit: nowMs(),
            trade_type: tradeType,
            prob_score: prob,
            pnl_pct: parseFloat(pnlPct.toFixed(4)),
            reason,
            features_at_entry: features,
            ...extra
        }));
    } catch (e) {}
}

function logStatsIfNeeded() {
    const total = omStats.legacyPredictions + omStats.entryPredictions + omStats.actionPredictions;

    if (total > 0 && total % 500 === 0) {
        const uptimeMin = ((Date.now() - omStats.startedAt) / 60000).toFixed(1);

        logThought(
            'SYSTEM',
            `📊 [OM STATS] uptime=${uptimeMin}m features=${omStats.features} invalid=${omStats.invalidFeatures} legacy=${omStats.legacyPredictions} entry=${omStats.entryPredictions} exit=${omStats.exitPredictions} action4=${omStats.actionPredictions} opened=${omStats.entriesOpened} closed=${omStats.exitsClosed} riskBlocked=${omStats.riskBlocked}`
        );
    }
}


// ============================================================
// 12. MACRO SCORE SYNC
// ============================================================
async function syncMacroScores() {
    try {
        const scores = await dataClient.hgetall(CHANNELS.MACRO_SCORES);

        for (const [sym, score] of Object.entries(scores)) {
            const parsed = parseFloat(score);
            if (Number.isFinite(parsed)) {
                macroCache.set(normalizeSymbol(sym), parsed);
            }
        }
    } catch (e) {
        console.error('⚠️ [MACRO] Sync error:', e.message);
    }
}


// ============================================================
// 13. RESTORE ACTIVE TRADES
// ============================================================
async function restoreStateFromExchange() {
    try {
        if (typeof exchange.restoreOpenTrades !== 'function') return;

        await exchange.restoreOpenTrades();

        if (typeof exchange.getActivePositions !== 'function') return;

        const restored = exchange.getActivePositions();

        for (const [symbolRaw, position] of restored.entries()) {
            const symbol = normalizeSymbol(symbolRaw);
            if (!symbol || !position) continue;

            activeTrades.set(symbol, {
                entryPrice: safeNumber(position.entryPrice),
                highestPrice: safeNumber(position.highestPrice, position.entryPrice),
                lowestPrice: safeNumber(position.lowestPrice, position.entryPrice),
                type: position.type === 'SHORT' ? 'SHORT' : 'LONG',
                prob: safeNumber(position.prob, 0),
                probLong: position.probLong,
                probShort: position.probShort,
                macroScore: position.macroScore,
                leverage: safeNumber(position.leverage, 1),
                features: position.features || null,
                openTime: safeNumber(position.openTime, nowMs()),
                bestRoi: 0,
                worstRoi: 0,
                lifecycle: 'RESTORED',
                restored: true,
                brainMode: position.brainMode || BRAIN_MODE
            });

            syncStream(symbol, 'ENTER_TRADE');

            logThought(
                symbol,
                `♻️ [RESTORE] ${position.type} @ ${safeNumber(position.entryPrice).toFixed(6)}`
            );
        }
    } catch (error) {
        console.error('❌ [RESTORE] Error:', error.message);
    }
}


// ============================================================
// 14. RISK GUARD WRAPPERS
// ============================================================
async function checkRiskGuard(symbol, action, feature, context) {
    if (!riskGuard) return { allowed: true };

    try {
        if (typeof riskGuard.canOpenTrade === 'function') {
            return await riskGuard.canOpenTrade(symbol, action, feature, context);
        }

        if (typeof riskGuard.check === 'function') {
            return await riskGuard.check(symbol, action, feature, context);
        }
    } catch (error) {
        console.error(`⚠️ [RISK] Error ${symbol}:`, error.message);
        return {
            allowed: false,
            reason: 'RISK_GUARD_ERROR'
        };
    }

    return { allowed: true };
}

async function riskRecordOpened(symbol) {
    if (riskGuard && typeof riskGuard.recordTradeOpened === 'function') {
        try {
            await riskGuard.recordTradeOpened(symbol);
        } catch (e) {}
    }
}

async function riskRecordClosed(symbol, pnl) {
    if (riskGuard && typeof riskGuard.recordTradeClosed === 'function') {
        try {
            await riskGuard.recordTradeClosed(symbol, pnl);
        } catch (e) {}
    }
}

async function riskPause(reason) {
    tradingPaused = true;
    pauseReason = reason || 'manual';

    if (riskGuard && typeof riskGuard.pauseTrading === 'function') {
        try {
            await riskGuard.pauseTrading(pauseReason);
        } catch (e) {}
    }
}

async function riskResume() {
    tradingPaused = false;
    pauseReason = '';

    if (riskGuard && typeof riskGuard.resumeTrading === 'function') {
        try {
            await riskGuard.resumeTrading();
        } catch (e) {}
    }
}


// ============================================================
// 15. ENTRY SAFETY FILTERS
// ------------------------------------------------------------
// Đây không phải bộ não, chỉ là safety.
// ============================================================
function checkEntrySafety(symbol, entryDecision, feature) {
    const sym = normalizeSymbol(symbol);

    if (!entryDecision || entryDecision.action !== 'ENTER') {
        return {
            ok: false,
            reason: entryDecision?.reason || 'NO_ENTRY_DECISION'
        };
    }

    const side = entryDecision.side;
    const finalProb = safeNumber(entryDecision.prob, 0);

    const ageMs = getFeatureAgeMs(feature);
    if (ageMs !== null) {
        const ageSec = ageMs / 1000;
        if (ageSec < MIN_SECONDS_AFTER_SUBSCRIBE) {
            return {
                ok: false,
                reason: `FEATURE_TOO_FRESH_${ageSec.toFixed(1)}s`
            };
        }
    }

    const radarDirection = getRadarDirection(feature);
    if (radarDirection && radarDirection !== side && finalProb < COUNTER_RADAR_MIN_PROB) {
        return {
            ok: false,
            reason: `COUNTER_RADAR_NEED_${COUNTER_RADAR_MIN_PROB}_got_${finalProb.toFixed(3)} radar=${radarDirection}`
        };
    }

    const bid = safeNumber(feature.best_bid, 0);
    const ask = safeNumber(feature.best_ask, 0);

    if (bid <= 0 || ask <= 0 || ask <= bid) {
        return {
            ok: false,
            reason: 'BAD_EXECUTION_BOOK'
        };
    }

    return { ok: true };
}

function checkEntryConfirmation(symbol, side, finalProb) {
    const sym = normalizeSymbol(symbol);
    const now = nowMs();
    const prev = entryConfirmState.get(sym);

    if (!prev || prev.side !== side || now - prev.firstSeenAt > ENTRY_CONFIRM_TTL_MS) {
        entryConfirmState.set(sym, {
            side,
            count: 1,
            firstSeenAt: now,
            lastSeenAt: now,
            maxProb: finalProb
        });

        if (ENTRY_CONFIRM_TICKS > 1) {
            return {
                ok: false,
                reason: `ENTRY_CONFIRM_1/${ENTRY_CONFIRM_TICKS}`
            };
        }

        return { ok: true };
    }

    prev.count += 1;
    prev.lastSeenAt = now;
    prev.maxProb = Math.max(prev.maxProb, finalProb);

    entryConfirmState.set(sym, prev);

    if (prev.count < ENTRY_CONFIRM_TICKS) {
        return {
            ok: false,
            reason: `ENTRY_CONFIRM_${prev.count}/${ENTRY_CONFIRM_TICKS}`
        };
    }

    entryConfirmState.delete(sym);
    return { ok: true };
}


// ============================================================
// 16. CLOSE TRADE HELPER
// ============================================================
async function closeActiveTrade(symbol, price, reason, exitFeature = null, options = {}) {
    const sym = normalizeSymbol(symbol);
    const trade = activeTrades.get(sym);

    if (!trade) return null;

    if (isExecuting.has(sym) && !options.force) return null;
    isExecuting.add(sym);

    try {
        const closePrice = safeNumber(price, getExitReferencePrice(exitFeature || {}, trade.type));

        logThought(
            sym,
            `🛑 [ĐÓNG LỆNH] ${reason} @ ${safeNumber(closePrice).toFixed(6)}`
        );

        const result = await exchange.closeTrade(
            sym,
            closePrice,
            reason,
            exitFeature
        );

        if (!result) {
            logThought(sym, '⛔ [CLOSE FAILED] Exchange không xác nhận đóng. Giữ active trade để tránh lệch.');
            return null;
        }

        activeTrades.delete(sym);
        syncStream(sym, 'EXIT_TRADE');

        logExperience(
            sym,
            trade.type,
            trade.entryPrice,
            closePrice,
            trade.prob,
            trade.features,
            reason,
            {
                brainMode: trade.brainMode,
                entryDecision: trade.entryDecision || null,
                exitDecision: options.exitDecision || null
            }
        );

        await riskRecordClosed(
            sym,
            safeNumber(result?.netPnL ?? result?.netPnl ?? result?.netPnL ?? 0)
        );

        const cooldown = reason === 'LIQUIDATED'
            ? COOLDOWN_AFTER_LIQUIDATION_MS
            : (options.cooldownMs ?? COOLDOWN_AFTER_CLOSE_MS);

        setCooldown(sym, cooldown, reason);
        markRecentlyClosed(sym);

        omStats.exitsClosed += 1;

        return result;
    } catch (error) {
        console.error(`❌ [CLOSE] Error ${sym}:`, error.message);
        return null;
    } finally {
        isExecuting.delete(sym);
    }
}

async function handleExchangeClosedEvent(symbol, tickEvent, feature) {
    const sym = normalizeSymbol(symbol);
    const trade = activeTrades.get(sym);

    if (!trade) {
        syncStream(sym, 'EXIT_TRADE');
        return;
    }

    activeTrades.delete(sym);
    syncStream(sym, 'EXIT_TRADE');

    const closePrice = safeNumber(
        tickEvent.closePrice ||
        tickEvent.result?.closePrice ||
        getExitReferencePrice(feature, trade.type) ||
        feature.last_price
    );

    const reason = tickEvent.reason || 'EXCHANGE_CLOSED';

    logThought(
        sym,
        `💀 [EXCHANGE CLOSED] ${reason} @ ${closePrice.toFixed(6)}`
    );

    logExperience(
        sym,
        trade.type,
        trade.entryPrice,
        closePrice,
        trade.prob,
        trade.features,
        reason,
        {
            brainMode: trade.brainMode,
            exchangeEvent: tickEvent
        }
    );

    await riskRecordClosed(sym, safeNumber(tickEvent.result?.netPnL || 0));

    const cooldownMs = reason === 'LIQUIDATED'
        ? COOLDOWN_AFTER_LIQUIDATION_MS
        : COOLDOWN_AFTER_CLOSE_MS;

    setCooldown(sym, cooldownMs, reason);
    markRecentlyClosed(sym);

    omStats.exitsClosed += 1;
}


// ============================================================
// 17. ACTIVE TRADE MANAGEMENT
// ------------------------------------------------------------
// Exchange updateTick vẫn được gọi để giữ mô phỏng/liquidation/fee/PnL.
// Exit thường do ExitBrain/ActionBrain quyết định.
// Rule hard chỉ là safety.
// ============================================================
async function manageActiveTrade(symbol, feature) {
    const sym = normalizeSymbol(symbol);
    const trade = activeTrades.get(sym);

    if (!trade) return false;

    const evalPrice = getEvalPrice(feature);
    const exitReferencePrice = getExitReferencePrice(feature, trade.type);

    if (evalPrice <= 0 || exitReferencePrice <= 0) return true;

    // Giữ logic exchange updateTick để mô phỏng sát account.
    if (typeof exchange.updateTick === 'function') {
        const tickEvent = await exchange.updateTick(sym, evalPrice, feature);

        if (tickEvent?.closed) {
            await handleExchangeClosedEvent(sym, tickEvent, feature);
            return true;
        }
    }

    if (!activeTrades.has(sym)) return true;

    const holdMs = nowMs() - safeNumber(trade.openTime, nowMs());
    const currentRoi = getCurrentRoiPercent(trade, evalPrice);

    trade.bestRoi = Math.max(safeNumber(trade.bestRoi, 0), currentRoi);
    trade.worstRoi = Math.min(safeNumber(trade.worstRoi, 0), currentRoi);

    if (trade.type === 'LONG' && evalPrice > safeNumber(trade.highestPrice, 0)) {
        trade.highestPrice = evalPrice;
    }

    if (trade.type === 'SHORT' && (!trade.lowestPrice || evalPrice < trade.lowestPrice)) {
        trade.lowestPrice = evalPrice;
    }

    if (holdMs < MIN_HOLD_BEFORE_MODEL_EXIT_MS) {
        trade.lifecycle = 'PROTECTION';
    } else {
        trade.lifecycle = 'MODEL_MANAGED';
    }

    // 1. Emergency hard stop luôn được phép.
    if (currentRoi <= EMERGENCY_STOP_ROI) {
        await closeActiveTrade(
            sym,
            exitReferencePrice,
            `EMERGENCY_STOP ROI=${currentRoi.toFixed(2)}% <= ${EMERGENCY_STOP_ROI}%`,
            feature
        );
        return true;
    }

    // 2. Không cho exit quá sớm trừ emergency/liquidation.
    if (holdMs < MIN_HOLD_BEFORE_ANY_EXIT_MS) {
        return true;
    }

    // 3. Model exit.
    let exitDecision = null;
    let parsedExit = null;

    try {
        if (BRAIN_MODE === 'DUAL_BRAIN') {
            parsedExit = await runExitBrain(feature, trade);
            exitDecision = decideExitFromExitBrain(parsedExit);
        } else if (BRAIN_MODE === 'ACTION_4_CLASS') {
            parsedExit = await runActionBrain(feature, trade);
            exitDecision = decideExitFromAction4(parsedExit);
        }
    } catch (e) {
        console.error(`❌ [EXIT BRAIN] Error ${sym}:`, e.message);
    }

    if (exitDecision) {
        logDecision(sym, 'EXIT_DECISION', {
            decision: exitDecision,
            trade: {
                type: trade.type,
                entryPrice: trade.entryPrice,
                roi: currentRoi,
                bestRoi: trade.bestRoi,
                worstRoi: trade.worstRoi,
                holdMs
            }
        });

        if (exitDecision.shouldExit && holdMs >= MIN_HOLD_BEFORE_MODEL_EXIT_MS) {
            await closeActiveTrade(
                sym,
                exitReferencePrice,
                exitDecision.reason,
                feature,
                {
                    exitDecision
                }
            );
            return true;
        }
    }

    // 4. Optional rule backup.
    // Mặc định tắt để ONNX học exit.
    if (RULE_EXIT_BACKUP_ENABLED && holdMs >= MIN_HOLD_BEFORE_MODEL_EXIT_MS) {
        if (currentRoi >= TAKE_PROFIT_ROI) {
            await closeActiveTrade(
                sym,
                exitReferencePrice,
                `RULE_TAKE_PROFIT ROI=${currentRoi.toFixed(2)}%`,
                feature
            );
            return true;
        }

        if (currentRoi <= SOFT_STOP_ROI) {
            await closeActiveTrade(
                sym,
                exitReferencePrice,
                `RULE_SOFT_STOP ROI=${currentRoi.toFixed(2)}%`,
                feature
            );
            return true;
        }

        if (trade.bestRoi >= TRAILING_ACTIVATE_ROI) {
            const giveback = trade.bestRoi - currentRoi;

            if (giveback >= TRAILING_GIVEBACK_ROI) {
                await closeActiveTrade(
                    sym,
                    exitReferencePrice,
                    `RULE_TRAILING best=${trade.bestRoi.toFixed(2)}% now=${currentRoi.toFixed(2)}% giveback=${giveback.toFixed(2)}%`,
                    feature
                );
                return true;
            }
        }
    }

    // 5. Max hold vẫn giữ để tránh treo lệnh vô hạn.
    if (holdMs >= MAX_HOLD_MS) {
        await closeActiveTrade(
            sym,
            exitReferencePrice,
            `MAX_HOLD ${Math.round(holdMs / 1000)}s ROI=${currentRoi.toFixed(2)}%`,
            feature
        );
        return true;
    }

    return true;
}


// ============================================================
// 18. EV SWITCH
// ============================================================
async function tryEvSwitchForSlot(newSymbol, finalProb, currentFeature) {
    if (!EV_SWITCH_ENABLED) return activeTrades.size < MAX_ACTIVE_TRADES;
    if (activeTrades.size < MAX_ACTIVE_TRADES) return true;

    let worstTradeSymbol = null;
    let worstTradeProb = 1.0;

    for (const [sym, activeT] of activeTrades.entries()) {
        if (safeNumber(activeT.prob, 0) < worstTradeProb) {
            worstTradeProb = safeNumber(activeT.prob, 0);
            worstTradeSymbol = sym;
        }
    }

    if (!worstTradeSymbol) return false;

    const delta = finalProb - worstTradeProb;
    if (delta < EV_SWITCH_MIN_PROB_DELTA) return false;

    const tradeToKill = activeTrades.get(worstTradeSymbol);
    const killFeature = latestFeatures.get(worstTradeSymbol);
    const killEvalPrice = getEvalPrice(killFeature || {});
    const killExitPrice = getExitReferencePrice(killFeature || {}, tradeToKill.type) || killEvalPrice || tradeToKill.entryPrice;
    const currentRoi = getCurrentRoiPercent(tradeToKill, killEvalPrice || killExitPrice);

    if (currentRoi > EV_SWITCH_PROTECT_PROFIT_ROI) {
        logThought(
            newSymbol,
            `🧯 [EV SWITCH BỎ QUA] ${worstTradeSymbol} đang lời ROI ${currentRoi.toFixed(2)}%`
        );
        return false;
    }

    logThought(
        newSymbol,
        `🔥 [EV SWITCH] Cắt ${worstTradeSymbol} để lấy slot cho ${newSymbol}`
    );

    await closeActiveTrade(
        worstTradeSymbol,
        killExitPrice,
        'EV_SWITCH',
        killFeature || currentFeature,
        {
            force: true
        }
    );

    return activeTrades.size < MAX_ACTIVE_TRADES;
}


// ============================================================
// 19. ENTRY EXECUTION
// ============================================================
async function openNewTrade(symbol, entryDecision, macroScore, feature) {
    const sym = normalizeSymbol(symbol);

    if (!entryDecision || entryDecision.action !== 'ENTER') return;
    if (isExecuting.has(sym)) return;
    if (activeTrades.has(sym)) return;

    const side = entryDecision.side;
    const finalProb = safeNumber(entryDecision.prob, 0);
    const entryPrice = getEntryReferencePrice(feature, side);

    if (entryPrice <= 0) {
        logThought(sym, '❌ [ENTRY] Không có entry price hợp lệ');
        return;
    }

    isExecuting.add(sym);

    try {
        const safety = checkEntrySafety(sym, entryDecision, feature);
        if (!safety.ok) {
            omStats.entryRejected += 1;
            logThought(sym, `🧊 [ENTRY SAFETY] ${safety.reason}`);
            setCooldown(sym, Math.min(COOLDOWN_AFTER_FAIL_MS, 30 * 1000), 'ENTRY_SAFETY');
            return;
        }

        const confirmation = checkEntryConfirmation(sym, side, finalProb);
        if (!confirmation.ok) {
            omStats.entryRejected += 1;
            logThought(sym, `⏳ [ENTRY CONFIRM] ${confirmation.reason}`);
            return;
        }

        const finalLev = calculateLeverageFromProb(finalProb);

        const riskDecision = await checkRiskGuard(sym, side, feature, {
            finalProb,
            macroScore,
            activeTrades: activeTrades.size,
            maxActiveTrades: MAX_ACTIVE_TRADES,
            leverage: finalLev,
            exchangeMode: EXCHANGE_MODE,
            brainMode: BRAIN_MODE,
            entryDecision
        });

        if (riskDecision && riskDecision.allowed === false) {
            omStats.riskBlocked += 1;
            logThought(sym, `🛡️ [RISK BLOCK] ${riskDecision.reason}`);
            setCooldown(sym, COOLDOWN_AFTER_FAIL_MS, 'RISK_BLOCK');
            return;
        }

        const slotAvailable = await tryEvSwitchForSlot(sym, finalProb, feature);
        if (!slotAvailable) {
            logThought(sym, '🚫 [NO SLOT] Max active trades reached');
            return;
        }

        logThought(
            sym,
            `🧠 [BRAIN ENTRY] ${side} Prob=${(finalProb * 100).toFixed(1)}% Lev=${finalLev}x Macro=${macroScore.toFixed(3)} Mode=${EXCHANGE_MODE} Brain=${BRAIN_MODE} Reason=${entryDecision.reason}`
        );

        const success = await exchange.openTrade(
            sym,
            'MARKET',
            entryPrice,
            finalLev,
            finalProb,
            side,
            feature,
            {
                macroScore,
                entrySignal: `OM_V21_${BRAIN_MODE}`,
                modelVersion: MODEL_VERSION,
                brainMode: BRAIN_MODE,
                entryDecision
            }
        );

        if (success) {
            const evalPrice = getEvalPrice(feature) || entryPrice;

            activeTrades.set(sym, {
                entryPrice,
                highestPrice: evalPrice,
                lowestPrice: evalPrice,
                type: side,
                prob: finalProb,
                macroScore,
                leverage: finalLev,
                features: feature,
                openTime: nowMs(),
                bestRoi: 0,
                worstRoi: 0,
                lifecycle: 'PROTECTION',
                brainMode: BRAIN_MODE,
                entryDecision
            });

            entryConfirmState.delete(sym);
            syncStream(sym, 'ENTER_TRADE');
            await riskRecordOpened(sym);

            omStats.entriesOpened += 1;

            logDecision(sym, 'ENTRY_OPENED', {
                side,
                entryPrice,
                leverage: finalLev,
                macroScore,
                entryDecision,
                feature_ts: feature.feature_ts || null
            });

            logThought(sym, `✅ [VÀO LỆNH THÀNH CÔNG] ${side} ${sym} @ ${entryPrice.toFixed(6)}`);
        } else {
            setCooldown(sym, COOLDOWN_AFTER_FAIL_MS, 'OPEN_TRADE_FAILED');
            logThought(sym, '❌ [TỪ CHỐI] Mở lệnh thất bại. Block 1 phút.');
        }
    } catch (err) {
        console.error(`❌ [ENTRY] Error ${sym}:`, err.message);
        setCooldown(sym, COOLDOWN_AFTER_FAIL_MS, 'ENTRY_EXCEPTION');
    } finally {
        isExecuting.delete(sym);
    }
}


// ============================================================
// 20. MAIN FEATURE HANDLER
// ============================================================
async function getEntryDecision(feature) {
    // 1. Dual brain ưu tiên EntryBrain.
    if (BRAIN_MODE === 'DUAL_BRAIN') {
        const entry = await runEntryBrain(feature);

        if (entry) {
            return decideEntryFromEntryBrain(entry);
        }

        if (ALLOW_ENTRY_WITH_LEGACY_IN_DUAL_MODE || FALLBACK_TO_LEGACY_IF_NEW_BRAIN_MISSING) {
            const legacy = await runLegacyBrain(feature);
            return decideEntryFromLegacy(legacy);
        }

        return makeFlatDecision('DUAL_NO_ENTRY_MODEL');
    }

    // 2. Action 4.
    if (BRAIN_MODE === 'ACTION_4_CLASS') {
        const action4 = await runActionBrain(feature, null);

        if (action4) {
            return decideEntryFromAction4(action4);
        }

        if (FALLBACK_TO_LEGACY_IF_NEW_BRAIN_MISSING) {
            const legacy = await runLegacyBrain(feature);
            return decideEntryFromLegacy(legacy);
        }

        return makeFlatDecision('ACTION4_NO_MODEL');
    }

    // 3. Legacy.
    const legacy = await runLegacyBrain(feature);
    return decideEntryFromLegacy(legacy);
}

async function handleFeatureMessage(messageBuffer) {
    let feature;

    try {
        feature = decode(messageBuffer);
    } catch (e) {
        return;
    }

    omStats.features += 1;

    const validation = isFeatureValid(feature);
    if (!validation.ok) {
        omStats.invalidFeatures += 1;
        return;
    }

    const symbol = normalizeSymbol(feature.symbol);
    latestFeatures.set(symbol, feature);

    const currentTrade = activeTrades.get(symbol);

    // Emit prediction dashboard nhẹ.
    try {
        if (BRAIN_MODE === 'LEGACY_2_CLASS') {
            const legacy = await runLegacyBrain(feature);
            if (legacy) {
                emitPrediction(symbol, {
                    brainMode: BRAIN_MODE,
                    legacy
                });
            }
        } else if (BRAIN_MODE === 'DUAL_BRAIN') {
            const entry = currentTrade ? null : await runEntryBrain(feature);
            const exit = currentTrade ? await runExitBrain(feature, currentTrade) : null;

            emitPrediction(symbol, {
                brainMode: BRAIN_MODE,
                entry,
                exit
            });
        } else if (BRAIN_MODE === 'ACTION_4_CLASS') {
            const action4 = await runActionBrain(feature, currentTrade || null);

            emitPrediction(symbol, {
                brainMode: BRAIN_MODE,
                action4
            });
        }
    } catch (e) {
        // Prediction dashboard lỗi thì không ảnh hưởng trading.
    }

    // Nếu đang có lệnh: quản trị lệnh, không mở thêm cùng symbol.
    if (currentTrade) {
        await manageActiveTrade(symbol, feature);
        logStatsIfNeeded();
        return;
    }

    if (tradingPaused) return;
    if (isExecuting.has(symbol)) return;
    if (isRecentlyClosed(symbol)) return;
    if (cooldowns.has(symbol) && nowMs() < cooldowns.get(symbol)) return;

    const macroScore = safeNumber(macroCache.get(symbol), 0.5);
    if (macroScore < MIN_MACRO_SCORE) return;

    let entryDecision;

    try {
        entryDecision = await getEntryDecision(feature);
    } catch (err) {
        console.error(`❌ [ENTRY BRAIN] Error ${symbol}:`, err.message);
        return;
    }

    logDecision(symbol, 'ENTRY_DECISION', {
        decision: entryDecision,
        macroScore,
        brainMode: BRAIN_MODE,
        feature_ts: feature.feature_ts || null,
        schema_version: feature.schema_version || null
    });

    if (!entryDecision || entryDecision.action !== 'ENTER') {
        logStatsIfNeeded();
        return;
    }

    await openNewTrade(symbol, entryDecision, macroScore, feature);
    logStatsIfNeeded();
}


// ============================================================
// 21. SYSTEM COMMANDS
// ============================================================
async function closeAll(reason = 'MANUAL_CLOSE_ALL') {
    const symbols = [...activeTrades.keys()];

    for (const symbol of symbols) {
        const trade = activeTrades.get(symbol);
        const feature = latestFeatures.get(symbol);
        const price = getExitReferencePrice(feature || {}, trade?.type) || safeNumber(trade?.entryPrice, 0);

        if (price > 0) {
            await closeActiveTrade(symbol, price, reason, feature, {
                force: true
            });
        }
    }
}

async function handleSystemCommand(message) {
    try {
        const cmd = JSON.parse(message);

        if (cmd.action === 'RELOAD_AI') {
            console.log('🔄 [SYSTEM] RELOAD_AI');
            await initAI();
            return;
        }

        if (cmd.action === 'PAUSE_TRADING') {
            await riskPause(cmd.reason || 'manual');
            logThought('SYSTEM', `⏸️ Trading paused: ${pauseReason}`);
            return;
        }

        if (cmd.action === 'RESUME_TRADING') {
            await riskResume();
            logThought('SYSTEM', '▶️ Trading resumed');
            return;
        }

        if (cmd.action === 'CLOSE_ALL') {
            await riskPause(cmd.reason || 'close_all');
            await closeAll(cmd.reason || 'MANUAL_CLOSE_ALL');
            return;
        }

        if (cmd.action === 'RESET_PAPER_WALLET' && typeof exchange.resetPaperWallet === 'function') {
            const amount = safeNumber(cmd.amount, undefined);
            await exchange.resetPaperWallet(amount);
            logThought('SYSTEM', `🔄 Reset paper wallet to ${amount}`);
            return;
        }

        if (cmd.action === 'FORCE_CLOSE' && cmd.symbol) {
            const symbol = normalizeSymbol(cmd.symbol);
            const trade = activeTrades.get(symbol);
            const feature = latestFeatures.get(symbol);
            const price = safeNumber(
                cmd.price,
                getExitReferencePrice(feature || {}, trade?.type) || trade?.entryPrice
            );

            if (activeTrades.has(symbol) && price > 0) {
                await closeActiveTrade(
                    symbol,
                    price,
                    cmd.reason || 'MANUAL_FORCE_CLOSE',
                    feature,
                    {
                        force: true
                    }
                );
            }
            return;
        }

        if (cmd.action === 'STATUS') {
            logThought(
                'SYSTEM',
                `STATUS mode=${EXCHANGE_MODE} brain=${BRAIN_MODE} active=${activeTrades.size}/${MAX_ACTIVE_TRADES} paused=${tradingPaused}`
            );
        }
    } catch (e) {
        console.error('⚠️ [SYSTEM COMMAND] Invalid command:', e.message);
    }
}


// ============================================================
// 22. SUBSCRIPTIONS
// ============================================================
function startFeatureSubscription() {
    subClient.psubscribe(CHANNELS.FEATURES, (err) => {
        if (err) {
            console.error('❌ [REDIS] psubscribe features error:', err.message);
            return;
        }

        console.log(`📡 [REDIS] Listening ${CHANNELS.FEATURES}`);
    });

    subClient.on('pmessageBuffer', async (pattern, channel, messageBuffer) => {
        try {
            await handleFeatureMessage(messageBuffer);
        } catch (error) {
            console.error('❌ [FEATURE] Handler error:', error.message);
        }
    });
}

function startCommandSubscription() {
    cmdClient.subscribe(CHANNELS.COMMANDS, (err) => {
        if (err) {
            console.error('❌ [REDIS] subscribe commands error:', err.message);
            return;
        }

        console.log(`📡 [REDIS] Listening ${CHANNELS.COMMANDS}`);
    });

    cmdClient.on('message', async (channel, message) => {
        if (channel === CHANNELS.COMMANDS) {
            await handleSystemCommand(message);
        }
    });
}


// ============================================================
// 23. HOUSEKEEPING
// ============================================================
function startHousekeeping() {
    setInterval(() => {
        const now = nowMs();

        for (const [sym, expireTime] of cooldowns.entries()) {
            if (now > expireTime) cooldowns.delete(sym);
        }

        cleanupRecentClosed();

        for (const [sym, state] of entryConfirmState.entries()) {
            if (now - state.lastSeenAt > ENTRY_CONFIRM_TTL_MS) {
                entryConfirmState.delete(sym);
            }
        }
    }, COOLDOWN_CLEAN_INTERVAL_MS);

    setInterval(syncMacroScores, MACRO_SYNC_INTERVAL_MS);
}


// ============================================================
// 24. STARTUP CHECKS
// ============================================================
function validateModeSafety() {
    if (EXCHANGE_MODE === 'LIVE') {
        const dry = String(process.env.LIVE_DRY_RUN || 'true') === 'true';

        if (dry) {
            throw new Error(
                'EXCHANGE_MODE=LIVE nhưng LIVE_DRY_RUN vẫn true. Hãy dùng EXCHANGE_MODE=LIVE_DRY_RUN hoặc set LIVE_DRY_RUN=false rõ ràng.'
            );
        }
    }

    if (EXCHANGE_MODE === 'LIVE_DRY_RUN') {
        process.env.LIVE_DRY_RUN = 'true';
    }

    if (!['LEGACY_2_CLASS', 'ACTION_4_CLASS', 'DUAL_BRAIN'].includes(BRAIN_MODE)) {
        throw new Error(`BRAIN_MODE không hợp lệ: ${BRAIN_MODE}`);
    }
}


// ============================================================
// 25. MAIN
// ============================================================
async function main() {
    console.log('🚀 [ORDER MANAGER V21 - DUAL BRAIN POLICY EXECUTOR] Starting...');
    console.log(`⚙️ [CONFIG] mode=${EXCHANGE_MODE}, brain=${BRAIN_MODE}, maxTrades=${MAX_ACTIVE_TRADES}, macro>=${MIN_MACRO_SCORE}`);
    console.log(`⚙️ [MODELS] legacy=${LEGACY_MODEL_PATH}`);
    console.log(`⚙️ [MODELS] entry=${ENTRY_MODEL_PATH}`);
    console.log(`⚙️ [MODELS] exit=${EXIT_MODEL_PATH}`);
    console.log(`⚙️ [MODELS] action4=${ACTION_MODEL_PATH}`);
    console.log(`⚙️ [ENTRY] confirm=${ENTRY_CONFIRM_TICKS}, gap>=${MIN_ENTRY_ACTION_GAP}, long>=${ENTRY_LONG_THRESHOLD}, short>=${ENTRY_SHORT_THRESHOLD}`);
    console.log(`⚙️ [EXIT] modelExit>=${EXIT_THRESHOLD}, emergencySL=${EMERGENCY_STOP_ROI}%, maxHold=${MAX_HOLD_MS}ms, ruleBackup=${RULE_EXIT_BACKUP_ENABLED}`);

    validateModeSafety();

    await initAI();
    await syncMacroScores();

    if (typeof exchange.ensureReady === 'function') {
        await exchange.ensureReady();
    }

    await restoreStateFromExchange();

    startFeatureSubscription();
    startCommandSubscription();
    startHousekeeping();

    console.log('✅ [ORDER MANAGER V21] Ready');
}


// ============================================================
// 26. SHUTDOWN
// ============================================================
async function shutdown() {
    console.log('\n🛑 [ORDER MANAGER V21] Shutting down...');

    try { await subClient.quit(); } catch (e) {}
    try { await cmdClient.quit(); } catch (e) {}
    try { await pubClient.quit(); } catch (e) {}
    try { await dataClient.quit(); } catch (e) {}

    try {
        if (exchange.userStream && typeof exchange.userStream.stop === 'function') {
            await exchange.userStream.stop();
        }
    } catch (e) {}

    try {
        if (mongoose.connection.readyState !== 0) {
            await mongoose.disconnect();
        }
    } catch (e) {}

    process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().catch(err => {
    console.error('❌ [FATAL] Startup error:', err.message);
    process.exit(1);
});