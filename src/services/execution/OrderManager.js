const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const fs = require('fs');
const path = require('path');
const ort = require('onnxruntime-node');
const mongoose = require('mongoose');

require('dotenv').config({ path: path.join(__dirname, '../../../.env') });

// ============================================================
// ORDER MANAGER FINAL - LIVE READY MASTER EXECUTION BRAIN
// ------------------------------------------------------------
// Vai trò:
// 1. Nhận feature realtime từ FeedHandler V19.
// 2. Validate feature trước khi chạy ONNX.
// 3. Chạy Universal_Scout.onnx với đúng 13 input cũ.
// 4. Gọi RiskGuard trước khi mở lệnh.
// 5. Chọn PaperExchange hoặc LiveExchange bằng EXCHANGE_MODE.
// 6. Đồng bộ ENTER_TRADE / EXIT_TRADE với FeedHandler.
// 7. Hỗ trợ PAUSE_TRADING / RESUME_TRADING / CLOSE_ALL / FORCE_CLOSE.
// 8. Ghi accounting cho RiskGuard sau khi mở/đóng lệnh.
// 9. Restore lệnh PAPER/LIVE nếu exchange hỗ trợ.
//
// EXCHANGE_MODE:
// - PAPER        : dùng PaperExchange.js
// - LIVE_DRY_RUN : dùng LiveExchange.js nhưng LIVE_DRY_RUN=true
// - LIVE         : dùng LiveExchange.js, yêu cầu LIVE_DRY_RUN=false
//
// CẢNH BÁO:
// - Không bật LIVE nếu chưa chạy LIVE_DRY_RUN đủ lâu.
// - Không đổi thứ tự 13 input ONNX.
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
    SUBSCRIPTIONS: config.CHANNELS?.SUBSCRIPTIONS || 'system:subscriptions'
};

const TRADING = config.TRADING || {};

const EXCHANGE_MODE = String(process.env.EXCHANGE_MODE || TRADING.EXCHANGE_MODE || 'PAPER').toUpperCase();
const MAX_ACTIVE_TRADES = Number(TRADING.MAX_ACTIVE_TRADES || 3);
const AI_LONG_THRESHOLD = Number(TRADING.AI_LONG_THRESHOLD || 0.70);
const AI_SHORT_THRESHOLD = Number(TRADING.AI_SHORT_THRESHOLD || 0.70);
const MIN_MACRO_SCORE = Number(TRADING.MIN_MACRO_SCORE || 0.40);
const MAX_SPREAD_CLOSE = Number(TRADING.MAX_SPREAD_CLOSE || 0.002);
const MAX_ATR_PERCENT = Number(TRADING.MAX_ATR_PERCENT || 0.20);
const MIN_ATR_PERCENT = Number(TRADING.MIN_ATR_PERCENT || 0.00001);
const COOLDOWN_AFTER_CLOSE_MS = Number(TRADING.COOLDOWN_AFTER_CLOSE_MS || 15 * 60 * 1000);
const COOLDOWN_AFTER_FAIL_MS = Number(TRADING.COOLDOWN_AFTER_FAIL_MS || 60 * 1000);
const COOLDOWN_AFTER_LIQUIDATION_MS = Number(TRADING.COOLDOWN_AFTER_LIQUIDATION_MS || 30 * 60 * 1000);
const PREDICTION_EMIT_INTERVAL_MS = Number(TRADING.PREDICTION_EMIT_INTERVAL_MS || 1000);
const MACRO_SYNC_INTERVAL_MS = Number(TRADING.MACRO_SYNC_INTERVAL_MS || 5000);
const COOLDOWN_CLEAN_INTERVAL_MS = Number(TRADING.COOLDOWN_CLEAN_INTERVAL_MS || 60000);
const EV_SWITCH_MIN_PROB_DELTA = Number(TRADING.EV_SWITCH_MIN_PROB_DELTA || 0.10);
const EV_SWITCH_PROTECT_PROFIT_ROI = Number(TRADING.EV_SWITCH_PROTECT_PROFIT_ROI || 10);
const DEFAULT_ATR_PERCENT = Number(TRADING.DEFAULT_ATR_PERCENT || 0.003);
const MFA_PANIC_THRESHOLD = Number(TRADING.MFA_PANIC_THRESHOLD || 5.0);
const MFA_TIGHTEN_THRESHOLD = Number(TRADING.MFA_TIGHTEN_THRESHOLD || 4.0);
const TRAILING_MULTIPLIER_NORMAL = Number(TRADING.TRAILING_MULTIPLIER_NORMAL || 3.0);
const TRAILING_MULTIPLIER_TIGHT = Number(TRADING.TRAILING_MULTIPLIER_TIGHT || 1.5);
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
        .catch(err => {
            console.error('❌ [MONGO] Connect error:', err.message);
        });
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

        if (!aiInputName || !aiOutputName) {
            throw new Error('Không tìm thấy input/output name trong ONNX model');
        }

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
const RECENT_CLOSE_TTL_MS = 10 * 1000;

let tradingPaused = false;
let pauseReason = '';

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

// ============================================================
// 8. FEATURE VECTOR / VALIDATION
// ============================================================
function buildFeatureVector(feature) {
    // THỨ TỰ 13 INPUT TUYỆT ĐỐI KHÔNG ĐƯỢC ĐỔI
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

    const probShort = safeNumber(probs[0], NaN);
    const probLong = safeNumber(probs[1], NaN);
    if (!Number.isFinite(probShort) || !Number.isFinite(probLong)) return null;
    if (probShort < 0 || probLong < 0) return null;

    return { probShort, probLong };
}

// ============================================================
// 9. EXPERIENCE / DASHBOARD
// ============================================================
function logExperience(symbol, tradeType, entryPrice, exitPrice, prob, features, reason) {
    const sym = normalizeSymbol(symbol);
    if (!sym || !entryPrice || !exitPrice) return;

    const pnlPct = tradeType === 'LONG'
        ? ((exitPrice - entryPrice) / entryPrice) * 100
        : ((entryPrice - exitPrice) / entryPrice) * 100;

    const payload = {
        symbol: sym,
        ts_exit: nowMs(),
        trade_type: tradeType,
        prob_score: prob,
        pnl_pct: parseFloat(pnlPct.toFixed(4)),
        reason,
        features_at_entry: features
    };

    try {
        pubClient.publish('experience:raw', JSON.stringify(payload));
    } catch (e) {}
}

function emitPrediction(symbol, probLong, probShort) {
    const sym = normalizeSymbol(symbol);
    const now = nowMs();
    const lastEmit = predictionEmitTimestamps.get(sym) || 0;
    if (now - lastEmit < PREDICTION_EMIT_INTERVAL_MS) return;

    try {
        pubClient.publish('dashboard:predictions', JSON.stringify({
            symbol: sym,
            long: probLong,
            short: probShort,
            ts: now
        }));
        predictionEmitTimestamps.set(sym, now);
    } catch (e) {}
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
                leverage: safeNumber(position.leverage, 1),
                features: position.features || null,
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
// 13. CLOSE TRADE HELPER
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

        activeTrades.delete(sym);
        syncStream(sym, 'EXIT_TRADE');

        logExperience(sym, trade.type, trade.entryPrice, currentPrice, trade.prob, trade.features, reason);
        await riskRecordClosed(sym, safeNumber(result?.netPnL || result?.netPnl || 0));

        setCooldown(sym, options.cooldownMs ?? COOLDOWN_AFTER_CLOSE_MS, reason);
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
// 14. ACTIVE TRADE MANAGEMENT
// ============================================================
async function manageActiveTrade(symbol, feature) {
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
    if (trade.type === 'LONG' && evalPrice > trade.highestPrice) trade.highestPrice = evalPrice;
    if (trade.type === 'SHORT' && (!trade.lowestPrice || evalPrice < trade.lowestPrice)) trade.lowestPrice = evalPrice;

    const atrPercent = safeNumber(feature.ATR14, DEFAULT_ATR_PERCENT);
    const mfa = safeNumber(feature.MFA, 0);
    const stopMultiplier = Math.abs(mfa) > MFA_TIGHTEN_THRESHOLD ? TRAILING_MULTIPLIER_TIGHT : TRAILING_MULTIPLIER_NORMAL;
    const stopDistance = atrPercent * stopMultiplier;

    let shouldClose = false;
    let closeReason = '';

    if (trade.type === 'LONG') {
        const slPrice = trade.highestPrice * (1 - stopDistance);
        if (evalPrice <= slPrice) {
            shouldClose = true;
            closeReason = 'LONG: Trailing Stop Hit';
        } else if (mfa < -MFA_PANIC_THRESHOLD) {
            shouldClose = true;
            closeReason = 'LONG: Dump mạnh (MFA âm sâu)';
        }
    } else {
        const slPrice = trade.lowestPrice * (1 + stopDistance);
        if (evalPrice >= slPrice) {
            shouldClose = true;
            closeReason = 'SHORT: Trailing Stop Hit';
        } else if (mfa > MFA_PANIC_THRESHOLD) {
            shouldClose = true;
            closeReason = 'SHORT: Pump mạnh (MFA dương gắt)';
        }
    }

    if (shouldClose) await closeActiveTrade(sym, currentPrice, closeReason, feature);
    return true;
}

// ============================================================
// 15. EV SWITCH
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

    if (currentRoi > EV_SWITCH_PROTECT_PROFIT_ROI && delta < EV_SWITCH_MIN_PROB_DELTA * 2) {
        logThought(newSymbol, `🧯 [EV SWITCH BỎ QUA] ${worstTradeSymbol} đang lời ROI ${currentRoi.toFixed(2)}%`);
        return false;
    }

    logThought(newSymbol, `🔥 [EV SWITCH] Cắt ${worstTradeSymbol} để lấy slot cho ${newSymbol}`);
    await closeActiveTrade(worstTradeSymbol, killPrice, 'EV Switch', killFeature || currentFeature, { force: true });
    return activeTrades.size < MAX_ACTIVE_TRADES;
}

// ============================================================
// 16. ENTRY EXECUTION
// ============================================================
async function openNewTrade(symbol, tradeAction, currentPrice, finalProb, probLong, probShort, macroScore, feature) {
    const sym = normalizeSymbol(symbol);
    if (isExecuting.has(sym)) return;
    if (activeTrades.has(sym)) return;

    isExecuting.add(sym);

    try {
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
            exchangeMode: EXCHANGE_MODE
        });

        if (riskDecision && riskDecision.allowed === false) {
            logThought(sym, `🛡️ [RISK BLOCK] ${riskDecision.reason}`);
            setCooldown(sym, COOLDOWN_AFTER_FAIL_MS, 'RISK_BLOCK');
            return;
        }

        logThought(sym, `🧠 [AI BÓP CÒ] ${tradeAction} Prob=${(finalProb * 100).toFixed(1)}% Lev=${finalLev}x Macro=${macroScore.toFixed(3)} Mode=${EXCHANGE_MODE}`);

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
                entrySignal: 'AI_PROB_THRESHOLD',
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
                openTime: nowMs()
            });

            syncStream(sym, 'ENTER_TRADE');
            await riskRecordOpened(sym);
            logThought(sym, `✅ [VÀO LỆNH THÀNH CÔNG] ${tradeAction} ${sym}`);
        } else {
            setCooldown(sym, COOLDOWN_AFTER_FAIL_MS, 'OPEN_TRADE_FAILED');
            logThought(sym, `❌ [TỪ CHỐI] Mở lệnh thất bại. Block 1 phút.`);
        }
    } catch (err) {
        console.error(`❌ [ENTRY] Error ${sym}:`, err.message);
        setCooldown(sym, COOLDOWN_AFTER_FAIL_MS, 'ENTRY_EXCEPTION');
    } finally {
        isExecuting.delete(sym);
    }
}

// ============================================================
// 17. MAIN FEATURE HANDLER
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

    if (activeTrades.has(symbol)) {
        await manageActiveTrade(symbol, feature);
        return;
    }

    if (tradingPaused) return;
    if (isExecuting.has(symbol)) return;
    if (isRecentlyClosed(symbol)) return;
    if (cooldowns.has(symbol) && nowMs() < cooldowns.get(symbol)) return;

    const macroScore = safeNumber(macroCache.get(symbol), 0.5);
    if (macroScore < MIN_MACRO_SCORE) return;

    try {
        const inputData = buildFeatureVector(feature);
        const tensor = new ort.Tensor('float32', inputData, [1, 13]);
        const feeds = {};
        feeds[aiInputName] = tensor;

        const output = await aiSession.run(feeds);
        const parsed = parseOnnxOutput(output);
        if (!parsed) return;

        const { probShort, probLong } = parsed;
        emitPrediction(symbol, probLong, probShort);

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
    } catch (err) {
        console.error(`❌ [AI] Inference error ${symbol}:`, err.message);
    }
}

// ============================================================
// 18. SYSTEM COMMANDS
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
    } catch (e) {}
}

// ============================================================
// 19. SUBSCRIPTIONS INIT
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
    cmdClient.subscribe('system:commands', (err) => {
        if (err) {
            console.error('❌ [REDIS] subscribe commands error:', err.message);
            return;
        }
        console.log('📡 [REDIS] Listening system:commands');
    });

    cmdClient.on('message', async (channel, message) => {
        if (channel === 'system:commands') await handleSystemCommand(message);
    });
}

// ============================================================
// 20. HOUSEKEEPING
// ============================================================
function startHousekeeping() {
    setInterval(() => {
        const now = nowMs();
        for (const [sym, expireTime] of cooldowns.entries()) {
            if (now > expireTime) cooldowns.delete(sym);
        }
        cleanupRecentClosed();
    }, COOLDOWN_CLEAN_INTERVAL_MS);

    setInterval(syncMacroScores, MACRO_SYNC_INTERVAL_MS);
}

// ============================================================
// 21. STARTUP CHECKS
// ============================================================
function validateModeSafety() {
    if (EXCHANGE_MODE === 'LIVE') {
        const dry = String(process.env.LIVE_DRY_RUN || 'true') === 'true';
        if (dry) {
            throw new Error('EXCHANGE_MODE=LIVE nhưng LIVE_DRY_RUN vẫn true. Hãy dùng EXCHANGE_MODE=LIVE_DRY_RUN hoặc set LIVE_DRY_RUN=false rõ ràng.');
        }
    }

    if (EXCHANGE_MODE === 'LIVE_DRY_RUN') {
        process.env.LIVE_DRY_RUN = 'true';
    }
}

// ============================================================
// 22. MAIN
// ============================================================
async function main() {
    console.log('🚀 [ORDER MANAGER FINAL] Starting...');
    console.log(`⚙️ [CONFIG] exchangeMode=${EXCHANGE_MODE}, maxTrades=${MAX_ACTIVE_TRADES}, long>=${AI_LONG_THRESHOLD}, short>=${AI_SHORT_THRESHOLD}, macro>=${MIN_MACRO_SCORE}`);

    validateModeSafety();

    await initAI();
    await syncMacroScores();

    // Khởi tạo exchange trước khi subscribe feature, để lệnh đầu không bị race.
    if (typeof exchange.ensureReady === 'function') {
        await exchange.ensureReady();
    }

    await restoreStateFromExchange();

    startFeatureSubscription();
    startCommandSubscription();
    startHousekeeping();

    console.log('✅ [ORDER MANAGER FINAL] Ready');
}

// ============================================================
// 23. SHUTDOWN
// ============================================================
async function shutdown() {
    console.log('\n🛑 [ORDER MANAGER FINAL] Shutting down...');

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
