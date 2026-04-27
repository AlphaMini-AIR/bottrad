const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const fs = require('fs');
const path = require('path');
const ort = require('onnxruntime-node');
const mongoose = require('mongoose');

require('dotenv').config({ path: path.join(__dirname, '../../../.env') });

// ============================================================
// ORDER MANAGER - PROFESSIONAL TRADER FINAL
// ------------------------------------------------------------
// Mục tiêu của bản này:
// 1. Không vào lệnh kiểu FOMO cuối nhịp.
// 2. Không đóng lệnh quá nhanh vì nhiễu vài cây nến nhỏ.
// 3. Cho lệnh sống đủ lâu để tạo dữ liệu học chất lượng cho ONNX.
// 4. Quản trị thoát lệnh theo lifecycle giống trader chuyên nghiệp:
//    - Protection Mode
//    - Let It Work
//    - Protect Profit
//    - Confirmed Reversal
// 5. Vẫn giữ hard stop để bảo vệ ví paper/live.
// 6. Giai đoạn test giữ threshold 0.6.
//
// Lưu ý:
// - File này thay thế src/services/execution/OrderManager.js
// - Tương thích PaperExchange_Fixed_SafeMongoFirst
// - Tương thích ScoutTrade_Model_NoMiddleware_Final
// ============================================================

// ============================================================
// 1. LOAD CONFIG
// ============================================================
const configPath = path.join(__dirname, '../../../system_config.json');
const modelPath = path.join(__dirname, '../../../Universal_Scout.onnx');

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

const EXCHANGE_MODE = String(process.env.EXCHANGE_MODE || TRADING.EXCHANGE_MODE || 'PAPER').toUpperCase();
const MAX_ACTIVE_TRADES = Number(TRADING.MAX_ACTIVE_TRADES || 3);

// Test threshold: theo yêu cầu hiện tại hạ xuống 0.6.
const AI_LONG_THRESHOLD = Number(TRADING.AI_LONG_THRESHOLD || 0.70);
const AI_SHORT_THRESHOLD = Number(TRADING.AI_SHORT_THRESHOLD || 0.70);
const MIN_MACRO_SCORE = Number(TRADING.MIN_MACRO_SCORE || 0.40);

// Feature validation.
const MAX_SPREAD_CLOSE = Number(TRADING.MAX_SPREAD_CLOSE || 0.0025);
const MAX_ATR_PERCENT = Number(TRADING.MAX_ATR_PERCENT || 0.20);
const MIN_ATR_PERCENT = Number(TRADING.MIN_ATR_PERCENT || 0.00001);
const DEFAULT_ATR_PERCENT = Number(TRADING.DEFAULT_ATR_PERCENT || 0.003);

// Cooldowns.
const COOLDOWN_AFTER_CLOSE_MS = Number(TRADING.COOLDOWN_AFTER_CLOSE_MS || 15 * 60 * 1000);
const COOLDOWN_AFTER_FAIL_MS = Number(TRADING.COOLDOWN_AFTER_FAIL_MS || 60 * 1000);
const COOLDOWN_AFTER_LIQUIDATION_MS = Number(TRADING.COOLDOWN_AFTER_LIQUIDATION_MS || 30 * 60 * 1000);
const COOLDOWN_CLEAN_INTERVAL_MS = Number(TRADING.COOLDOWN_CLEAN_INTERVAL_MS || 60 * 1000);
const PREDICTION_EMIT_INTERVAL_MS = Number(TRADING.PREDICTION_EMIT_INTERVAL_MS || 1000);
const MACRO_SYNC_INTERVAL_MS = Number(TRADING.MACRO_SYNC_INTERVAL_MS || 5000);

// EV switch.
const EV_SWITCH_MIN_PROB_DELTA = Number(TRADING.EV_SWITCH_MIN_PROB_DELTA || 0.14);
const EV_SWITCH_PROTECT_PROFIT_ROI = Number(TRADING.EV_SWITCH_PROTECT_PROFIT_ROI || 6);

// ============================================================
// PROFESSIONAL ENTRY FILTERS
// ------------------------------------------------------------
// Anti-chase: không đuổi theo cây nến đã chạy quá xa.
// Counter-radar: nếu đánh ngược radar, AI phải rất chắc.
// Confirm ticks: tín hiệu phải tồn tại vài nhịp trước khi mở.
// ============================================================
const ENTRY_MFA_CHASE_BLOCK = Number(TRADING.ENTRY_MFA_CHASE_BLOCK || 6.0);
const ENTRY_BODY_CHASE_BLOCK = Number(TRADING.ENTRY_BODY_CHASE_BLOCK || 0.012); // 1.2% body tương đối là rất lớn với 1 nhịp scalp.
const COUNTER_RADAR_MIN_PROB = Number(TRADING.COUNTER_RADAR_MIN_PROB || 0.72);
const ENTRY_CONFIRM_TICKS = Number(TRADING.ENTRY_CONFIRM_TICKS || 2);
const ENTRY_CONFIRM_TTL_MS = Number(TRADING.ENTRY_CONFIRM_TTL_MS || 15 * 1000);
const MIN_ENTRY_PROB_GAP = Number(TRADING.MIN_ENTRY_PROB_GAP || 0.12);
const MIN_SECONDS_AFTER_SUBSCRIBE = Number(TRADING.MIN_SECONDS_AFTER_SUBSCRIBE || 5);

// ============================================================
// PROFESSIONAL EXIT LIFECYCLE
// ============================================================
const MIN_HOLD_BEFORE_ANY_EXIT_MS = Number(TRADING.MIN_HOLD_BEFORE_ANY_EXIT_MS || 15 * 1000);
const MIN_HOLD_BEFORE_NORMAL_EXIT_MS = Number(TRADING.MIN_HOLD_BEFORE_NORMAL_EXIT_MS || 60 * 1000);
const MIN_HOLD_BEFORE_TRAILING_MS = Number(TRADING.MIN_HOLD_BEFORE_TRAILING_MS || 90 * 1000);

const EMERGENCY_STOP_ROI = Number(TRADING.EMERGENCY_STOP_ROI || -10); // ROI % theo margin.
const SOFT_STOP_ROI = Number(TRADING.SOFT_STOP_ROI || -6);
const TAKE_PROFIT_ROI = Number(TRADING.TAKE_PROFIT_ROI || 12);
const TRAILING_ACTIVATE_ROI = Number(TRADING.TRAILING_ACTIVATE_ROI || 6);
const TRAILING_GIVEBACK_ROI = Number(TRADING.TRAILING_GIVEBACK_ROI || 3);
const MAX_HOLD_MS = Number(TRADING.MAX_HOLD_MS || 15 * 60 * 1000);

// Reversal.
const REVERSAL_EXIT_ENABLED = TRADING.REVERSAL_EXIT_ENABLED !== false;
const REVERSAL_OPPOSITE_PROB = Number(TRADING.REVERSAL_OPPOSITE_PROB || 0.70);
const REVERSAL_MIN_PROB_GAP = Number(TRADING.REVERSAL_MIN_PROB_GAP || 0.12);
const REVERSAL_CONFIRM_TICKS = Number(TRADING.REVERSAL_CONFIRM_TICKS || 3);
const REVERSAL_MIN_HOLD_MS = Number(TRADING.REVERSAL_MIN_HOLD_MS || 60 * 1000);

// MFA chỉ là xác nhận phụ, không tự đóng lệnh đơn độc.
const MFA_PANIC_THRESHOLD = Number(TRADING.MFA_PANIC_THRESHOLD || 10.0);

const MODEL_VERSION = process.env.MODEL_VERSION || TRADING.MODEL_VERSION || 'Universal_Scout.onnx';

// ============================================================
// 2. EXCHANGE SELECTOR
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
// 5. AI STATE
// ============================================================
let aiSession = null;
let aiInputName = '';
let aiOutputName = '';
let aiReady = false;
let aiLoading = false;

async function initAI() {
    if (aiLoading) return;
    aiLoading = true;
    aiReady = false;

    try {
        aiSession = await ort.InferenceSession.create(modelPath);
        aiInputName = aiSession.inputNames[0];
        aiOutputName = aiSession.outputNames[aiSession.outputNames.length - 1];
        if (!aiInputName || !aiOutputName) throw new Error('Không tìm thấy input/output name trong ONNX model');
        aiReady = true;
        console.log(`🧠 [AI] Online. Input=${aiInputName} Output=${aiOutputName}`);
    } catch (e) {
        console.error(`❌ [AI] Load ONNX error: ${e.message}`);
    } finally {
        aiLoading = false;
    }
}

// ============================================================
// 6. MEMORY STATE
// ============================================================
const activeTrades = new Map();
const latestFeatures = new Map();
const macroCache = new Map();
const cooldowns = new Map();
const isExecuting = new Set();
const predictionEmitTimestamps = new Map();
const recentlyClosed = new Map();
const reversalState = new Map();
const entryConfirmState = new Map();

const RECENT_CLOSE_TTL_MS = 10 * 1000;
let tradingPaused = false;
let pauseReason = '';

// Simple AI statistics to detect bias / wrong ONNX mapping.
const aiStats = {
    longSignals: 0,
    shortSignals: 0,
    counterRadarSignals: 0,
    entryRejectedChase: 0,
    entryRejectedConfirm: 0,
    entryRejectedCounterRadar: 0,
    startedAt: Date.now()
};

// ============================================================
// 7. UTILS
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

function logThought(symbol, message) {
    const ts = nowMs();
    const sym = normalizeSymbol(symbol) || 'SYSTEM';
    console.log(`[${sym}] ${message}`);
    try {
        pubClient.publish('dashboard:logs', JSON.stringify({ symbol: sym, msg: message, ts }));
    } catch (e) {}
}

function syncStream(symbol, action) {
    const sym = normalizeSymbol(symbol);
    if (!sym || !action) return;
    try {
        pubClient.publish(CHANNELS.SUBSCRIPTIONS, JSON.stringify({ action, symbol: sym, client: 'oms' }));
    } catch (e) {}
}

function setCooldown(symbol, durationMs, reason = '') {
    const sym = normalizeSymbol(symbol);
    if (!sym || durationMs <= 0) return;
    cooldowns.set(sym, nowMs() + durationMs);
    if (reason) logThought(sym, `⏳ [COOLDOWN] ${Math.round(durationMs / 1000)}s | ${reason}`);
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

function getRadarDirection(feature) {
    // Nếu sau này RadarManager/FeedHandler truyền field hướng radar thì dùng.
    // Hiện tại không chắc field nào tồn tại, nên support nhiều tên phổ biến.
    const raw = feature?.radarDirection || feature?.radar_direction || feature?.side || feature?.signal_side || feature?.impulseSide || feature?.impulse_side;
    const v = String(raw || '').toUpperCase();
    if (v === 'LONG' || v === 'SHORT') return v;
    return null;
}

function getFeatureAgeSeconds(feature) {
    const ts = safeNumber(feature?.ts || feature?.timestamp || feature?.event_time || feature?.last_update_ts, 0);
    if (ts <= 0) return 999;
    const normalized = ts < 1e12 ? ts * 1000 : ts;
    return Math.max(0, (Date.now() - normalized) / 1000);
}

// ============================================================
// 8. FEATURE VECTOR / VALIDATION
// ============================================================
function buildFeatureVector(feature) {
    return Float32Array.from([
        safeNumber(feature.ob_imb_top20, 0),
        safeNumber(feature.spread_close, 0),
        safeNumber(feature.bid_vol_1pct, 0),
        safeNumber(feature.ask_vol_1pct, 0),
        safeNumber(feature.max_buy_trade, 0),
        safeNumber(feature.max_sell_trade, 0),
        safeNumber(feature.liq_long_vol, 0),
        safeNumber(feature.liq_short_vol, 0),
        safeNumber(feature.funding_rate, 0),
        safeNumber(feature.taker_buy_ratio, 0),
        safeNumber(feature.body_size, 0),
        safeNumber(feature.wick_size, 0),
        safeNumber(feature.btc_relative_strength, 0)
    ]);
}

function isFeatureValid(feature) {
    if (!feature || typeof feature !== 'object') return { ok: false, reason: 'EMPTY_FEATURE' };
    if (!feature.is_warm) return { ok: false, reason: 'NOT_WARM' };

    const symbol = normalizeSymbol(feature.symbol);
    if (!symbol) return { ok: false, reason: 'NO_SYMBOL' };

    const price = safeNumber(feature.last_price, 0);
    if (price <= 0) return { ok: false, reason: 'BAD_PRICE' };

    const bid = safeNumber(feature.best_bid, 0);
    const ask = safeNumber(feature.best_ask, 0);
    if (bid <= 0 || ask <= 0 || ask <= bid) return { ok: false, reason: 'BAD_BOOK' };

    const spread = safeNumber(feature.spread_close, 0);
    if (spread < 0 || spread > MAX_SPREAD_CLOSE) return { ok: false, reason: `SPREAD_TOO_WIDE_${spread}` };

    const atr = safeNumber(feature.ATR14, DEFAULT_ATR_PERCENT);
    if (atr < MIN_ATR_PERCENT || atr > MAX_ATR_PERCENT) return { ok: false, reason: `BAD_ATR_${atr}` };

    const takerBuyRatio = safeNumber(feature.taker_buy_ratio, 0.5);
    if (takerBuyRatio < 0 || takerBuyRatio > 1) return { ok: false, reason: 'BAD_TAKER_RATIO' };

    const vector = buildFeatureVector(feature);
    for (const value of vector) {
        if (!Number.isFinite(value)) return { ok: false, reason: 'NON_FINITE_VECTOR' };
    }

    return { ok: true };
}

function parseOnnxOutput(output) {
    if (!output || !aiOutputName || !output[aiOutputName]) return null;
    const probs = output[aiOutputName].data;
    if (!probs || probs.length < 2) return null;

    // Giữ mapping hiện tại: [SHORT, LONG].
    // Nếu sau thống kê thấy model bias ngược, kiểm tra lại mapping này.
    const probShort = safeNumber(probs[0], NaN);
    const probLong = safeNumber(probs[1], NaN);
    if (!Number.isFinite(probShort) || !Number.isFinite(probLong)) return null;
    if (probShort < 0 || probLong < 0) return null;

    return { probShort, probLong };
}

async function runAiInference(feature) {
    if (!aiReady || !aiSession) return null;
    const inputData = buildFeatureVector(feature);
    const tensor = new ort.Tensor('float32', inputData, [1, 13]);
    const feeds = {};
    feeds[aiInputName] = tensor;
    const output = await aiSession.run(feeds);
    return parseOnnxOutput(output);
}

// ============================================================
// 9. DASHBOARD / EXPERIENCE
// ============================================================
function logExperience(symbol, tradeType, entryPrice, exitPrice, prob, features, reason) {
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
            features_at_entry: features
        }));
    } catch (e) {}
}

function emitPrediction(symbol, probLong, probShort) {
    const sym = normalizeSymbol(symbol);
    const now = nowMs();
    const lastEmit = predictionEmitTimestamps.get(sym) || 0;
    if (now - lastEmit < PREDICTION_EMIT_INTERVAL_MS) return;

    try {
        pubClient.publish('dashboard:predictions', JSON.stringify({ symbol: sym, long: probLong, short: probShort, ts: now }));
        predictionEmitTimestamps.set(sym, now);
    } catch (e) {}
}

function logAiStatsIfNeeded() {
    const total = aiStats.longSignals + aiStats.shortSignals;
    if (total > 0 && total % 250 === 0) {
        const longPct = (aiStats.longSignals / total * 100).toFixed(1);
        const shortPct = (aiStats.shortSignals / total * 100).toFixed(1);
        logThought('SYSTEM', `📊 [AI STATS] total=${total} LONG=${longPct}% SHORT=${shortPct}% counterRadar=${aiStats.counterRadarSignals} chaseBlock=${aiStats.entryRejectedChase}`);
    }
}

// ============================================================
// 10. MACRO SCORE SYNC
// ============================================================
async function syncMacroScores() {
    try {
        const scores = await dataClient.hgetall(CHANNELS.MACRO_SCORES);
        for (const [sym, score] of Object.entries(scores)) {
            const parsed = parseFloat(score);
            if (Number.isFinite(parsed)) macroCache.set(normalizeSymbol(sym), parsed);
        }
    } catch (e) {
        console.error('⚠️ [MACRO] Sync error:', e.message);
    }
}

// ============================================================
// 11. RESTORE ACTIVE TRADES
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
                restored: true
            });
            syncStream(symbol, 'ENTER_TRADE');
            logThought(symbol, `♻️ [RESTORE] ${position.type} @ ${safeNumber(position.entryPrice).toFixed(6)}`);
        }
    } catch (error) {
        console.error('❌ [RESTORE] Error:', error.message);
    }
}

// ============================================================
// 12. RISK GUARD WRAPPERS
// ============================================================
async function checkRiskGuard(symbol, action, feature, context) {
    if (!riskGuard) return { allowed: true };
    try {
        if (typeof riskGuard.canOpenTrade === 'function') return await riskGuard.canOpenTrade(symbol, action, feature, context);
        if (typeof riskGuard.check === 'function') return await riskGuard.check(symbol, action, feature, context);
    } catch (error) {
        console.error(`⚠️ [RISK] Error ${symbol}:`, error.message);
        return { allowed: false, reason: 'RISK_GUARD_ERROR' };
    }
    return { allowed: true };
}

async function riskRecordOpened(symbol) {
    if (riskGuard && typeof riskGuard.recordTradeOpened === 'function') {
        try { await riskGuard.recordTradeOpened(symbol); } catch (e) {}
    }
}

async function riskRecordClosed(symbol, pnl) {
    if (riskGuard && typeof riskGuard.recordTradeClosed === 'function') {
        try { await riskGuard.recordTradeClosed(symbol, pnl); } catch (e) {}
    }
}

async function riskPause(reason) {
    tradingPaused = true;
    pauseReason = reason || 'manual';
    if (riskGuard && typeof riskGuard.pauseTrading === 'function') {
        try { await riskGuard.pauseTrading(pauseReason); } catch (e) {}
    }
}

async function riskResume() {
    tradingPaused = false;
    pauseReason = '';
    if (riskGuard && typeof riskGuard.resumeTrading === 'function') {
        try { await riskGuard.resumeTrading(); } catch (e) {}
    }
}

// ============================================================
// 13. ENTRY QUALITY FILTER
// ============================================================
function checkEntryQuality(symbol, tradeAction, finalProb, probLong, probShort, feature) {
    const sym = normalizeSymbol(symbol);
    const mfa = safeNumber(feature.MFA, 0);
    const bodySize = Math.abs(safeNumber(feature.body_size, 0));
    const radarDirection = getRadarDirection(feature);
    const ageSec = getFeatureAgeSeconds(feature);

    // Không dùng feature quá mới/chưa ổn định sau khi subscribe, nếu timestamp có hỗ trợ.
    if (ageSec < MIN_SECONDS_AFTER_SUBSCRIBE) {
        return { ok: false, reason: `FEATURE_TOO_FRESH_${ageSec.toFixed(1)}s` };
    }

    // Anti-chase:
    // SHORT không đuổi theo khi lực dump đã quá sâu.
    // LONG không đuổi theo khi lực pump đã quá sâu.
    if (tradeAction === 'SHORT' && mfa < -ENTRY_MFA_CHASE_BLOCK) {
        aiStats.entryRejectedChase += 1;
        return { ok: false, reason: `ANTI_CHASE_SHORT_MFA_${mfa.toFixed(2)}` };
    }

    if (tradeAction === 'LONG' && mfa > ENTRY_MFA_CHASE_BLOCK) {
        aiStats.entryRejectedChase += 1;
        return { ok: false, reason: `ANTI_CHASE_LONG_MFA_${mfa.toFixed(2)}` };
    }

    if (bodySize > ENTRY_BODY_CHASE_BLOCK && finalProb < COUNTER_RADAR_MIN_PROB) {
        aiStats.entryRejectedChase += 1;
        return { ok: false, reason: `ANTI_CHASE_BIG_BODY_${bodySize.toFixed(5)}` };
    }

    // AI phải có gap rõ giữa hai hướng.
    const gap = Math.abs(probLong - probShort);
    if (gap < MIN_ENTRY_PROB_GAP) {
        return { ok: false, reason: `PROB_GAP_TOO_SMALL_${gap.toFixed(3)}` };
    }

    // Nếu đánh ngược hướng radar, AI phải rất chắc.
    if (radarDirection && radarDirection !== tradeAction) {
        aiStats.counterRadarSignals += 1;
        if (finalProb < COUNTER_RADAR_MIN_PROB) {
            aiStats.entryRejectedCounterRadar += 1;
            return { ok: false, reason: `COUNTER_RADAR_NEED_${COUNTER_RADAR_MIN_PROB}_got_${finalProb.toFixed(3)} radar=${radarDirection}` };
        }
    }

    return { ok: true };
}

function checkEntryConfirmation(symbol, tradeAction, finalProb) {
    const sym = normalizeSymbol(symbol);
    const now = nowMs();
    const prev = entryConfirmState.get(sym);

    if (!prev || prev.side !== tradeAction || now - prev.firstSeenAt > ENTRY_CONFIRM_TTL_MS) {
        entryConfirmState.set(sym, {
            side: tradeAction,
            count: 1,
            firstSeenAt: now,
            lastSeenAt: now,
            maxProb: finalProb
        });

        if (ENTRY_CONFIRM_TICKS > 1) {
            aiStats.entryRejectedConfirm += 1;
            return { ok: false, reason: `ENTRY_CONFIRM_1/${ENTRY_CONFIRM_TICKS}` };
        }

        return { ok: true };
    }

    prev.count += 1;
    prev.lastSeenAt = now;
    prev.maxProb = Math.max(prev.maxProb, finalProb);
    entryConfirmState.set(sym, prev);

    if (prev.count < ENTRY_CONFIRM_TICKS) {
        aiStats.entryRejectedConfirm += 1;
        return { ok: false, reason: `ENTRY_CONFIRM_${prev.count}/${ENTRY_CONFIRM_TICKS}` };
    }

    entryConfirmState.delete(sym);
    return { ok: true };
}

// ============================================================
// 14. CONFIRMED REVERSAL
// ============================================================
function checkConfirmedReversal(symbol, trade, probLong, probShort) {
    const sym = normalizeSymbol(symbol);
    if (!REVERSAL_EXIT_ENABLED) {
        reversalState.delete(sym);
        return { shouldClose: false };
    }

    const now = nowMs();
    const holdMs = now - safeNumber(trade.openTime, now);
    if (holdMs < REVERSAL_MIN_HOLD_MS) {
        reversalState.delete(sym);
        return { shouldClose: false, reason: 'REVERSAL_MIN_HOLD_NOT_MET' };
    }

    let oppositeProb = 0;
    let currentSideProb = 0;
    let reversalDirection = '';

    if (trade.type === 'SHORT') {
        oppositeProb = safeNumber(probLong, 0);
        currentSideProb = safeNumber(probShort, 0);
        reversalDirection = 'LONG';
    } else {
        oppositeProb = safeNumber(probShort, 0);
        currentSideProb = safeNumber(probLong, 0);
        reversalDirection = 'SHORT';
    }

    const gap = oppositeProb - currentSideProb;
    const isClearReversal = oppositeProb >= REVERSAL_OPPOSITE_PROB && gap >= REVERSAL_MIN_PROB_GAP;

    if (!isClearReversal) {
        reversalState.delete(sym);
        return { shouldClose: false, reason: 'NO_CLEAR_REVERSAL' };
    }

    const prev = reversalState.get(sym) || { direction: reversalDirection, count: 0, firstSeenAt: now };
    if (prev.direction !== reversalDirection) {
        prev.direction = reversalDirection;
        prev.count = 0;
        prev.firstSeenAt = now;
    }

    prev.count += 1;
    prev.lastSeenAt = now;
    prev.oppositeProb = oppositeProb;
    prev.currentSideProb = currentSideProb;
    prev.gap = gap;
    reversalState.set(sym, prev);

    if (prev.count >= REVERSAL_CONFIRM_TICKS) {
        reversalState.delete(sym);
        return {
            shouldClose: true,
            reason: `${trade.type}: AI đảo chiều rõ sang ${reversalDirection} | opposite=${(oppositeProb * 100).toFixed(1)}% current=${(currentSideProb * 100).toFixed(1)}% gap=${(gap * 100).toFixed(1)}% confirm=${prev.count}`
        };
    }

    return { shouldClose: false, reason: `REVERSAL_CONFIRMING_${prev.count}/${REVERSAL_CONFIRM_TICKS}` };
}

// ============================================================
// 15. CLOSE TRADE HELPER
// ============================================================
async function closeActiveTrade(symbol, currentPrice, reason, exitFeature = null, options = {}) {
    const sym = normalizeSymbol(symbol);
    const trade = activeTrades.get(sym);
    if (!trade) return null;

    if (isExecuting.has(sym) && !options.force) return null;
    isExecuting.add(sym);

    try {
        logThought(sym, `🛑 [ĐÓNG LỆNH] ${reason} @ ${safeNumber(currentPrice).toFixed(6)}`);
        const result = await exchange.closeTrade(sym, currentPrice, reason, exitFeature);

        if (!result) {
            logThought(sym, '⛔ [CLOSE FAILED] Exchange không xác nhận đóng. Giữ active trade để tránh lệch.');
            return null;
        }

        activeTrades.delete(sym);
        reversalState.delete(sym);
        syncStream(sym, 'EXIT_TRADE');

        logExperience(sym, trade.type, trade.entryPrice, currentPrice, trade.prob, trade.features, reason);
        await riskRecordClosed(sym, safeNumber(result?.netPnL ?? result?.netPnl ?? result?.netPnL ?? 0));

        const cooldown = reason === 'LIQUIDATED' ? COOLDOWN_AFTER_LIQUIDATION_MS : (options.cooldownMs ?? COOLDOWN_AFTER_CLOSE_MS);
        setCooldown(sym, cooldown, reason);
        markRecentlyClosed(sym);

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
    reversalState.delete(sym);
    syncStream(sym, 'EXIT_TRADE');

    const closePrice = safeNumber(tickEvent.closePrice || tickEvent.result?.closePrice || feature.last_price);
    const reason = tickEvent.reason || 'EXCHANGE_CLOSED';

    logThought(sym, `💀 [EXCHANGE CLOSED] ${reason} @ ${closePrice.toFixed(6)}`);
    logExperience(sym, trade.type, trade.entryPrice, closePrice, trade.prob, trade.features, reason);
    await riskRecordClosed(sym, safeNumber(tickEvent.result?.netPnL || 0));

    const cooldownMs = reason === 'LIQUIDATED' ? COOLDOWN_AFTER_LIQUIDATION_MS : COOLDOWN_AFTER_CLOSE_MS;
    setCooldown(sym, cooldownMs, reason);
    markRecentlyClosed(sym);
}

// ============================================================
// 16. PROFESSIONAL TRADE MANAGEMENT
// ============================================================
async function manageActiveTrade(symbol, feature, aiProb = null) {
    const sym = normalizeSymbol(symbol);
    const trade = activeTrades.get(sym);
    if (!trade) return false;

    const currentPrice = safeNumber(feature.last_price);
    if (currentPrice <= 0) return true;

    if (typeof exchange.updateTick === 'function') {
        const tickEvent = await exchange.updateTick(sym, currentPrice, feature);
        if (tickEvent?.closed) {
            await handleExchangeClosedEvent(sym, tickEvent, feature);
            return true;
        }
    }

    if (!activeTrades.has(sym)) return true;

    const evalPrice = safeNumber(feature.mark_price, currentPrice);
    const holdMs = nowMs() - safeNumber(trade.openTime, nowMs());
    const currentRoi = getCurrentRoiPercent(trade, evalPrice);

    trade.bestRoi = Math.max(safeNumber(trade.bestRoi, 0), currentRoi);
    trade.worstRoi = Math.min(safeNumber(trade.worstRoi, 0), currentRoi);

    if (trade.type === 'LONG' && evalPrice > trade.highestPrice) trade.highestPrice = evalPrice;
    if (trade.type === 'SHORT' && (!trade.lowestPrice || evalPrice < trade.lowestPrice)) trade.lowestPrice = evalPrice;

    // Lifecycle label for debugging/dashboard.
    if (holdMs < MIN_HOLD_BEFORE_NORMAL_EXIT_MS) {
        trade.lifecycle = 'PROTECTION';
    } else if (trade.bestRoi < TRAILING_ACTIVATE_ROI) {
        trade.lifecycle = 'LET_IT_WORK';
    } else {
        trade.lifecycle = 'PROTECT_PROFIT';
    }

    // 1. Emergency hard stop: luôn được phép cắt.
    if (currentRoi <= EMERGENCY_STOP_ROI) {
        await closeActiveTrade(sym, currentPrice, `EMERGENCY_STOP ROI=${currentRoi.toFixed(2)}% <= ${EMERGENCY_STOP_ROI}%`, feature);
        return true;
    }

    // 2. Trong vài giây đầu, không thoát trừ emergency.
    if (holdMs < MIN_HOLD_BEFORE_ANY_EXIT_MS) {
        return true;
    }

    // 3. AI reversal cực rõ sau min hold.
    if (aiProb) {
        const reversal = checkConfirmedReversal(sym, trade, aiProb.probLong, aiProb.probShort);
        if (reversal.shouldClose) {
            await closeActiveTrade(sym, currentPrice, reversal.reason, feature);
            return true;
        }
    }

    // 4. MFA panic chỉ đóng khi AI cũng xác nhận hướng ngược rõ.
    const mfa = safeNumber(feature.MFA, 0);
    if (aiProb && holdMs >= REVERSAL_MIN_HOLD_MS) {
        if (
            trade.type === 'LONG' &&
            mfa < -MFA_PANIC_THRESHOLD &&
            safeNumber(aiProb.probShort, 0) >= REVERSAL_OPPOSITE_PROB &&
            safeNumber(aiProb.probShort, 0) - safeNumber(aiProb.probLong, 0) >= REVERSAL_MIN_PROB_GAP
        ) {
            await closeActiveTrade(sym, currentPrice, `LONG: MFA dump cực mạnh + AI SHORT ${(aiProb.probShort * 100).toFixed(1)}%`, feature);
            return true;
        }

        if (
            trade.type === 'SHORT' &&
            mfa > MFA_PANIC_THRESHOLD &&
            safeNumber(aiProb.probLong, 0) >= REVERSAL_OPPOSITE_PROB &&
            safeNumber(aiProb.probLong, 0) - safeNumber(aiProb.probShort, 0) >= REVERSAL_MIN_PROB_GAP
        ) {
            await closeActiveTrade(sym, currentPrice, `SHORT: MFA pump cực mạnh + AI LONG ${(aiProb.probLong * 100).toFixed(1)}%`, feature);
            return true;
        }
    }

    // 5. Trước 60s: cho lệnh sống, không trailing/chốt non.
    if (holdMs < MIN_HOLD_BEFORE_NORMAL_EXIT_MS) {
        return true;
    }

    // 6. Soft stop: sau 60s nếu vẫn lỗ đáng kể và AI không còn ủng hộ hướng cũ.
    if (currentRoi <= SOFT_STOP_ROI && aiProb) {
        const currentSideProb = trade.type === 'LONG' ? safeNumber(aiProb.probLong, 0) : safeNumber(aiProb.probShort, 0);
        if (currentSideProb < 0.50) {
            await closeActiveTrade(sym, currentPrice, `SOFT_STOP ROI=${currentRoi.toFixed(2)}% currentSideProb=${(currentSideProb * 100).toFixed(1)}%`, feature);
            return true;
        }
    }

    // 7. Take profit rõ ràng.
    if (currentRoi >= TAKE_PROFIT_ROI) {
        await closeActiveTrade(sym, currentPrice, `TAKE_PROFIT ROI=${currentRoi.toFixed(2)}% >= ${TAKE_PROFIT_ROI}%`, feature);
        return true;
    }

    // 8. Profit trailing chỉ bật khi lệnh từng lời rõ và đã giữ đủ lâu.
    if (holdMs >= MIN_HOLD_BEFORE_TRAILING_MS && trade.bestRoi >= TRAILING_ACTIVATE_ROI) {
        const giveback = trade.bestRoi - currentRoi;
        if (giveback >= TRAILING_GIVEBACK_ROI) {
            await closeActiveTrade(sym, currentPrice, `PROFIT_TRAILING best=${trade.bestRoi.toFixed(2)}% now=${currentRoi.toFixed(2)}% giveback=${giveback.toFixed(2)}%`, feature);
            return true;
        }
    }

    // 9. Max hold để tránh lệnh treo quá lâu không còn giá trị học.
    if (holdMs >= MAX_HOLD_MS) {
        await closeActiveTrade(sym, currentPrice, `MAX_HOLD ${Math.round(holdMs / 1000)}s ROI=${currentRoi.toFixed(2)}%`, feature);
        return true;
    }

    return true;
}

// ============================================================
// 17. EV SWITCH
// ============================================================
async function tryEvSwitchForSlot(newSymbol, finalProb, currentFeature) {
    if (activeTrades.size < MAX_ACTIVE_TRADES) return true;

    let worstTradeSymbol = null;
    let worstTradeProb = 1.0;

    for (const [sym, activeT] of activeTrades.entries()) {
        if (activeT.prob < worstTradeProb) {
            worstTradeProb = activeT.prob;
            worstTradeSymbol = sym;
        }
    }

    if (!worstTradeSymbol) return false;
    const delta = finalProb - worstTradeProb;
    if (delta < EV_SWITCH_MIN_PROB_DELTA) return false;

    const tradeToKill = activeTrades.get(worstTradeSymbol);
    const killFeature = latestFeatures.get(worstTradeSymbol);
    const killPrice = safeNumber(killFeature?.last_price, tradeToKill.entryPrice);
    const currentRoi = getCurrentRoiPercent(tradeToKill, killPrice);

    if (currentRoi > EV_SWITCH_PROTECT_PROFIT_ROI) {
        logThought(newSymbol, `🧯 [EV SWITCH BỎ QUA] ${worstTradeSymbol} đang lời ROI ${currentRoi.toFixed(2)}%`);
        return false;
    }

    logThought(newSymbol, `🔥 [EV SWITCH] Cắt ${worstTradeSymbol} để lấy slot cho ${newSymbol}`);
    await closeActiveTrade(worstTradeSymbol, killPrice, 'EV Switch', killFeature || currentFeature, { force: true });
    return activeTrades.size < MAX_ACTIVE_TRADES;
}

// ============================================================
// 18. ENTRY EXECUTION
// ============================================================
async function openNewTrade(symbol, tradeAction, currentPrice, finalProb, probLong, probShort, macroScore, feature) {
    const sym = normalizeSymbol(symbol);
    if (isExecuting.has(sym)) return;
    if (activeTrades.has(sym)) return;

    isExecuting.add(sym);

    try {
        const entryQuality = checkEntryQuality(sym, tradeAction, finalProb, probLong, probShort, feature);
        if (!entryQuality.ok) {
            logThought(sym, `🧊 [ENTRY FILTER] ${entryQuality.reason}`);
            setCooldown(sym, Math.min(COOLDOWN_AFTER_FAIL_MS, 30 * 1000), 'ENTRY_FILTER');
            return;
        }

        const confirmation = checkEntryConfirmation(sym, tradeAction, finalProb);
        if (!confirmation.ok) {
            logThought(sym, `⏳ [ENTRY CONFIRM] ${confirmation.reason}`);
            return;
        }

        // Kelly đơn giản, clamp 5x - 20x như bản cũ.
        const kelly = (finalProb * 2 - (1 - finalProb)) / 2;
        let finalLev = Math.floor(kelly * 20);
        finalLev = Math.max(5, Math.min(finalLev, 20));

        const riskDecision = await checkRiskGuard(sym, tradeAction, feature, {
            finalProb,
            probLong,
            probShort,
            macroScore,
            activeTrades: activeTrades.size,
            maxActiveTrades: MAX_ACTIVE_TRADES,
            leverage: finalLev,
            exchangeMode: EXCHANGE_MODE,
            aiThresholdLong: AI_LONG_THRESHOLD,
            aiThresholdShort: AI_SHORT_THRESHOLD
        });

        if (riskDecision && riskDecision.allowed === false) {
            logThought(sym, `🛡️ [RISK BLOCK] ${riskDecision.reason}`);
            setCooldown(sym, COOLDOWN_AFTER_FAIL_MS, 'RISK_BLOCK');
            return;
        }

        logThought(sym, `🧠 [AI BÓP CÒ] ${tradeAction} Prob=${(finalProb * 100).toFixed(1)}% L=${(probLong * 100).toFixed(1)}% S=${(probShort * 100).toFixed(1)}% Lev=${finalLev}x Macro=${macroScore.toFixed(3)} Mode=${EXCHANGE_MODE}`);

        const success = await exchange.openTrade(
            sym,
            'MARKET',
            currentPrice,
            finalLev,
            finalProb,
            tradeAction,
            feature,
            {
                macroScore,
                probLong,
                probShort,
                entrySignal: 'AI_PRO_TRADER_TEST_06',
                modelVersion: MODEL_VERSION
            }
        );

        if (success) {
            activeTrades.set(sym, {
                entryPrice: currentPrice,
                highestPrice: safeNumber(feature.mark_price, currentPrice),
                lowestPrice: safeNumber(feature.mark_price, currentPrice),
                type: tradeAction,
                prob: finalProb,
                probLong,
                probShort,
                macroScore,
                leverage: finalLev,
                features: feature,
                openTime: nowMs(),
                bestRoi: 0,
                worstRoi: 0,
                lifecycle: 'PROTECTION'
            });

            reversalState.delete(sym);
            entryConfirmState.delete(sym);
            syncStream(sym, 'ENTER_TRADE');
            await riskRecordOpened(sym);
            logThought(sym, `✅ [VÀO LỆNH THÀNH CÔNG] ${tradeAction} ${sym}`);
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
// 19. MAIN FEATURE HANDLER
// ============================================================
async function handleFeatureMessage(messageBuffer) {
    let feature;
    try {
        feature = decode(messageBuffer);
    } catch (e) {
        return;
    }

    const validation = isFeatureValid(feature);
    if (!validation.ok) return;
    if (!aiReady || !aiSession) return;

    const symbol = normalizeSymbol(feature.symbol);
    const currentPrice = safeNumber(feature.last_price, 0);
    latestFeatures.set(symbol, feature);

    let parsed;
    try {
        parsed = await runAiInference(feature);
    } catch (err) {
        console.error(`❌ [AI] Inference error ${symbol}:`, err.message);
        return;
    }

    if (!parsed) return;

    const { probShort, probLong } = parsed;
    emitPrediction(symbol, probLong, probShort);

    if (probLong >= AI_LONG_THRESHOLD && probLong >= probShort) aiStats.longSignals += 1;
    if (probShort >= AI_SHORT_THRESHOLD && probShort > probLong) aiStats.shortSignals += 1;
    logAiStatsIfNeeded();

    // Nếu đang có lệnh: chỉ quản trị, không mở thêm cùng symbol.
    if (activeTrades.has(symbol)) {
        await manageActiveTrade(symbol, feature, { probLong, probShort });
        return;
    }

    if (tradingPaused) return;
    if (isExecuting.has(symbol)) return;
    if (isRecentlyClosed(symbol)) return;
    if (cooldowns.has(symbol) && nowMs() < cooldowns.get(symbol)) return;

    const macroScore = safeNumber(macroCache.get(symbol), 0.5);
    if (macroScore < MIN_MACRO_SCORE) return;

    let tradeAction = null;
    let finalProb = 0;

    if (probLong >= AI_LONG_THRESHOLD && probLong >= probShort) {
        tradeAction = 'LONG';
        finalProb = probLong;
    } else if (probShort >= AI_SHORT_THRESHOLD && probShort > probLong) {
        tradeAction = 'SHORT';
        finalProb = probShort;
    }

    if (!tradeAction) return;

    const slotAvailable = await tryEvSwitchForSlot(symbol, finalProb, feature);
    if (!slotAvailable) return;

    await openNewTrade(symbol, tradeAction, currentPrice, finalProb, probLong, probShort, macroScore, feature);
}

// ============================================================
// 20. SYSTEM COMMANDS
// ============================================================
async function closeAll(reason = 'MANUAL_CLOSE_ALL') {
    const symbols = [...activeTrades.keys()];
    for (const symbol of symbols) {
        const feature = latestFeatures.get(symbol);
        const price = safeNumber(feature?.last_price, activeTrades.get(symbol)?.entryPrice);
        if (price > 0) await closeActiveTrade(symbol, price, reason, feature, { force: true });
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
            const feature = latestFeatures.get(symbol);
            const price = safeNumber(cmd.price, feature?.last_price || activeTrades.get(symbol)?.entryPrice);
            if (activeTrades.has(symbol) && price > 0) {
                await closeActiveTrade(symbol, price, cmd.reason || 'MANUAL_FORCE_CLOSE', feature, { force: true });
            }
        }
    } catch (e) {
        console.error('⚠️ [SYSTEM COMMAND] Invalid command:', e.message);
    }
}

// ============================================================
// 21. SUBSCRIPTIONS
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
        if (channel === CHANNELS.COMMANDS) await handleSystemCommand(message);
    });
}

// ============================================================
// 22. HOUSEKEEPING
// ============================================================
function startHousekeeping() {
    setInterval(() => {
        const now = nowMs();
        for (const [sym, expireTime] of cooldowns.entries()) {
            if (now > expireTime) cooldowns.delete(sym);
        }
        cleanupRecentClosed();

        for (const [sym, state] of entryConfirmState.entries()) {
            if (now - state.lastSeenAt > ENTRY_CONFIRM_TTL_MS) entryConfirmState.delete(sym);
        }
    }, COOLDOWN_CLEAN_INTERVAL_MS);

    setInterval(syncMacroScores, MACRO_SYNC_INTERVAL_MS);
}

// ============================================================
// 23. STARTUP CHECKS
// ============================================================
function validateModeSafety() {
    if (EXCHANGE_MODE === 'LIVE') {
        const dry = String(process.env.LIVE_DRY_RUN || 'true') === 'true';
        if (dry) throw new Error('EXCHANGE_MODE=LIVE nhưng LIVE_DRY_RUN vẫn true. Hãy dùng EXCHANGE_MODE=LIVE_DRY_RUN hoặc set LIVE_DRY_RUN=false rõ ràng.');
    }
    if (EXCHANGE_MODE === 'LIVE_DRY_RUN') process.env.LIVE_DRY_RUN = 'true';
}

// ============================================================
// 24. MAIN
// ============================================================
async function main() {
    console.log('🚀 [ORDER MANAGER PROFESSIONAL FINAL] Starting...');
    console.log(`⚙️ [CONFIG] mode=${EXCHANGE_MODE}, maxTrades=${MAX_ACTIVE_TRADES}, long>=${AI_LONG_THRESHOLD}, short>=${AI_SHORT_THRESHOLD}, macro>=${MIN_MACRO_SCORE}`);
    console.log(`⚙️ [ENTRY] confirm=${ENTRY_CONFIRM_TICKS}, gap>=${MIN_ENTRY_PROB_GAP}, antiChaseMFA=${ENTRY_MFA_CHASE_BLOCK}, counterRadar>=${COUNTER_RADAR_MIN_PROB}`);
    console.log(`⚙️ [EXIT] minHold=${MIN_HOLD_BEFORE_NORMAL_EXIT_MS}ms, hardSL=${EMERGENCY_STOP_ROI}%, TP=${TAKE_PROFIT_ROI}%, trailAct=${TRAILING_ACTIVATE_ROI}%, giveback=${TRAILING_GIVEBACK_ROI}%`);

    validateModeSafety();
    await initAI();
    await syncMacroScores();

    if (typeof exchange.ensureReady === 'function') await exchange.ensureReady();
    await restoreStateFromExchange();

    startFeatureSubscription();
    startCommandSubscription();
    startHousekeeping();

    console.log('✅ [ORDER MANAGER PROFESSIONAL FINAL] Ready');
}

// ============================================================
// 25. SHUTDOWN
// ============================================================
async function shutdown() {
    console.log('\n🛑 [ORDER MANAGER PROFESSIONAL FINAL] Shutting down...');
    try { await subClient.quit(); } catch (e) {}
    try { await cmdClient.quit(); } catch (e) {}
    try { await pubClient.quit(); } catch (e) {}
    try { await dataClient.quit(); } catch (e) {}
    try {
        if (exchange.userStream && typeof exchange.userStream.stop === 'function') await exchange.userStream.stop();
    } catch (e) {}
    try {
        if (mongoose.connection.readyState !== 0) await mongoose.disconnect();
    } catch (e) {}
    process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

main().catch(err => {
    console.error('❌ [FATAL] Startup error:', err.message);
    process.exit(1);
});
