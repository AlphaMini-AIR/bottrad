const express = require('express');
const http = require('http');
const path = require('path');
const fs = require('fs');
const Redis = require('ioredis');
const mongoose = require('mongoose');
const { decode } = require('@msgpack/msgpack');
require('dotenv').config({ path: path.join(__dirname, '../../../.env') });

const ScoutTrade = require('../../models/ScoutTrade');

// ============================================================
// DASHBOARD SERVER V2 - TRADING CONTROL ROOM BACKEND
// ------------------------------------------------------------
// Mục tiêu bản V2:
// 1. Không hiển thị coin LOST trong watchlist.
// 2. Tự dọn symbol/candle/AI/log quá cũ để UI không phình.
// 3. Chuẩn hóa nến liên tục trong 1 giờ gần nhất để chart không bị đứt.
// 4. Gắn thêm thông tin tooltip vào từng nến.
// 5. Tạo marker IN/OUT từ lệnh OPEN/CLOSED cho frontend vẽ chart.
// 6. Giữ backend chỉ quan sát, không tự đặt lệnh.
// ============================================================

const configPath = path.join(__dirname, '../../../system_config.json');
let config;
try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    config = { REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379', CHANNELS: {} };
}

const REDIS_URL = config.REDIS_URL || process.env.REDIS_URL || 'redis://localhost:6379';
const MONGO_URI = process.env.MONGO_URI_SCOUT;
const PORT = Number(process.env.DASHBOARD_PORT || 3010);
const HOST = process.env.DASHBOARD_HOST || '0.0.0.0';

const FEATURE_PATTERN = config.CHANNELS?.FEATURES || process.env.FEATURE_PATTERN || 'market:features:*';
const PREDICTION_CHANNEL = process.env.DASHBOARD_PREDICTION_CHANNEL || 'dashboard:predictions';
const TRADES_CHANNEL = process.env.DASHBOARD_TRADES_CHANNEL || 'dashboard:trades';
const LOGS_CHANNEL = process.env.DASHBOARD_LOGS_CHANNEL || 'dashboard:logs';
const RISK_CHANNEL = process.env.DASHBOARD_RISK_CHANNEL || 'dashboard:risk';

const PAPER_WALLET_KEY = process.env.PAPER_WALLET_REDIS_KEY || 'paper:wallet:balance:v19';
const ACCOUNT_STATE_PREFIX = process.env.ACCOUNT_STATE_REDIS_PREFIX || 'account:state';

const WATCHLIST_EMIT_MS = Number(process.env.DASHBOARD_WATCHLIST_EMIT_MS || 1000);
const ACCOUNT_EMIT_MS = Number(process.env.DASHBOARD_ACCOUNT_EMIT_MS || 2000);
const HEALTH_EMIT_MS = Number(process.env.DASHBOARD_HEALTH_EMIT_MS || 3000);
const TRADE_REFRESH_MS = Number(process.env.DASHBOARD_TRADE_REFRESH_MS || 5000);
const CLEANUP_MS = Number(process.env.DASHBOARD_CLEANUP_MS || 30000);

const CANDLE_INTERVAL_SEC = Number(process.env.DASHBOARD_CANDLE_INTERVAL_SEC || 5);
const MAX_CANDLES_PER_SYMBOL = Number(process.env.DASHBOARD_MAX_CANDLES || 720); // 720 * 5s = 60 phút
const MAX_AI_POINTS_PER_SYMBOL = Number(process.env.DASHBOARD_MAX_AI_POINTS || 1200);
const MAX_LOGS = Number(process.env.DASHBOARD_MAX_LOGS || 300);
const MAX_WATCHLIST_SYMBOLS = Number(process.env.DASHBOARD_MAX_WATCHLIST_SYMBOLS || 60);

const FEATURE_LOST_MS = Number(process.env.DASHBOARD_FEATURE_LOST_MS || 10000);
const FEATURE_STALE_MS = Number(process.env.DASHBOARD_FEATURE_STALE_MS || 3000);
const KEEP_HISTORY_MS = Number(process.env.DASHBOARD_KEEP_HISTORY_MS || 60 * 60 * 1000);
const REMOVE_LOST_SYMBOL_MS = Number(process.env.DASHBOARD_REMOVE_LOST_SYMBOL_MS || 90 * 1000);

class DashboardServer {
    constructor() {
        this.app = express();
        this.server = http.createServer(this.app);
        this.io = require('socket.io')(this.server, {
            cors: { origin: '*' },
            pingTimeout: 30000,
            pingInterval: 10000
        });

        this.subClient = new Redis(REDIS_URL);
        this.dataClient = new Redis(REDIS_URL);
        this.pubClient = new Redis(REDIS_URL);

        this.symbolState = new Map();       // symbol -> latest state
        this.candles = new Map();           // symbol -> candle array, raw + synthetic on payload build
        this.aiTimeline = new Map();        // symbol -> prediction array
        this.logs = [];
        this.riskEvents = [];
        this.tradesCache = { open: [], closed: [], updatedAt: 0 };
        this.accountCache = { updatedAt: 0 };

        this.lastFeatureAt = 0;
        this.featureCount = 0;
        this.predictionCount = 0;
        this.connectedClients = 0;

        this.subClient.on('error', err => console.error('❌ [Dashboard subClient]', err.message));
        this.dataClient.on('error', err => console.error('❌ [Dashboard dataClient]', err.message));
        this.pubClient.on('error', err => console.error('❌ [Dashboard pubClient]', err.message));
    }

    // ========================================================
    // 1. UTILS
    // ========================================================
    normalizeSymbol(symbol) {
        return String(symbol || '').toUpperCase().trim();
    }

    safeNumber(value, fallback = 0) {
        if (value === null || value === undefined || value === '') return fallback;
        const n = Number(value);
        return Number.isFinite(n) ? n : fallback;
    }

    now() {
        return Date.now();
    }

    clampArray(arr, max) {
        if (!Array.isArray(arr)) return [];
        if (arr.length <= max) return arr;
        return arr.slice(arr.length - max);
    }

    getField(obj, keys, fallback = 0) {
        if (!obj) return fallback;
        for (const key of keys) {
            if (obj[key] !== undefined && obj[key] !== null) return obj[key];
        }
        return fallback;
    }

    statusFromLastSeen(lastSeen) {
        const age = this.now() - (lastSeen || 0);
        if (age > FEATURE_LOST_MS) return 'LOST';
        if (age > FEATURE_STALE_MS) return 'STALE';
        return 'LIVE';
    }

    toMs(value) {
        if (!value) return null;
        if (value instanceof Date) return value.getTime();
        const n = Number(value);
        if (Number.isFinite(n)) return n < 1e12 ? n * 1000 : n;
        const parsed = Date.parse(value);
        return Number.isFinite(parsed) ? parsed : null;
    }

    isClosedStatus(status) {
        const s = String(status || '').toUpperCase();
        return ['CLOSED', 'LIQUIDATED', 'FAILED', 'CANCELLED', 'LOST', 'STOP_LOSS', 'TAKE_PROFIT'].includes(s);
    }

    isOpenStatus(status) {
        const s = String(status || '').toUpperCase();
        return ['OPEN', 'ACTIVE', 'IN_TRADE'].includes(s);
    }

    // ========================================================
    // 2. EXPRESS / SOCKET SETUP
    // ========================================================
    setupHttp() {
        const publicDir = path.join(__dirname, 'public');
        this.app.use(express.json());
        this.app.use(express.static(publicDir));

        this.app.get('/api/health', (req, res) => {
            res.json(this.buildHealthPayload());
        });

        this.app.get('/api/watchlist', (req, res) => {
            res.json(this.buildWatchlistPayload());
        });

        this.app.get('/api/symbol/:symbol', (req, res) => {
            const symbol = this.normalizeSymbol(req.params.symbol);
            res.json(this.buildSymbolPayload(symbol));
        });

        this.app.get('/api/trades', async (req, res) => {
            await this.refreshTrades();
            res.json(this.tradesCache);
        });

        this.app.get('/api/account', async (req, res) => {
            res.json(await this.buildAccountPayload());
        });
    }

    setupSocket() {
        this.io.on('connection', socket => {
            this.connectedClients += 1;
            console.log(`🖥️ [Dashboard] Client connected: ${socket.id}`);

            socket.emit('watchlist:update', this.buildWatchlistPayload());
            socket.emit('logs:update', { logs: this.logs });
            socket.emit('trades:update', this.tradesCache);
            this.buildAccountPayload().then(payload => socket.emit('account:update', payload)).catch(() => {});
            socket.emit('system:health', this.buildHealthPayload());

            socket.on('symbol:select', symbolRaw => {
                const symbol = this.normalizeSymbol(symbolRaw);
                socket.emit('symbol:snapshot', this.buildSymbolPayload(symbol));
            });

            socket.on('command', async cmd => {
                const allowed = ['PAUSE_TRADING', 'RESUME_TRADING', 'CLOSE_ALL', 'FORCE_CLOSE', 'RELOAD_AI'];
                if (!cmd || !allowed.includes(cmd.action)) return;
                await this.pubClient.publish('system:commands', JSON.stringify(cmd));
                this.addLog({ symbol: 'DASHBOARD', msg: `Sent command ${cmd.action}`, ts: this.now() });
            });

            socket.on('disconnect', () => {
                this.connectedClients = Math.max(0, this.connectedClients - 1);
                console.log(`🖥️ [Dashboard] Client disconnected: ${socket.id}`);
            });
        });
    }

    // ========================================================
    // 3. REDIS SUBSCRIPTIONS
    // ========================================================
    async startRedisSubscriptions() {
        await this.subClient.psubscribe(FEATURE_PATTERN);
        await this.subClient.subscribe(PREDICTION_CHANNEL, TRADES_CHANNEL, LOGS_CHANNEL, RISK_CHANNEL);

        this.subClient.on('pmessageBuffer', async (pattern, channel, messageBuffer) => {
            try {
                await this.handleFeatureMessage(channel.toString(), messageBuffer);
            } catch (error) {
                console.error('❌ [Dashboard] feature handler error:', error.message);
            }
        });

        this.subClient.on('message', async (channel, message) => {
            try {
                if (channel === PREDICTION_CHANNEL) await this.handlePrediction(message);
                else if (channel === TRADES_CHANNEL) await this.handleTradesSignal(message);
                else if (channel === LOGS_CHANNEL) this.handleLog(message);
                else if (channel === RISK_CHANNEL) this.handleRisk(message);
            } catch (error) {
                console.error('❌ [Dashboard] redis message error:', error.message);
            }
        });

        console.log(`📡 [Dashboard] Listening ${FEATURE_PATTERN}, ${PREDICTION_CHANNEL}, ${TRADES_CHANNEL}, ${LOGS_CHANNEL}`);
    }

    decodeFeatureMessage(messageBuffer) {
        try {
            return decode(messageBuffer);
        } catch (msgpackError) {
            try {
                return JSON.parse(messageBuffer.toString());
            } catch (jsonError) {
                return null;
            }
        }
    }

    async handleFeatureMessage(channel, messageBuffer) {
        const feature = this.decodeFeatureMessage(messageBuffer);
        if (!feature) return;

        const symbol = this.normalizeSymbol(feature.symbol || channel.split(':').pop());
        if (!symbol) return;

        const ts = this.now();
        this.lastFeatureAt = ts;
        this.featureCount += 1;

        const prev = this.symbolState.get(symbol) || {};
        const prediction = prev.prediction || { long: null, short: null, ts: null };
        const activeTrade = this.findOpenTrade(symbol);

        const state = {
            symbol,
            feature,
            prediction,
            lastSeen: ts,
            status: activeTrade ? 'IN_TRADE' : this.statusFromLastSeen(ts),
            activeTrade
        };

        this.symbolState.set(symbol, state);
        const candle = this.updateCandle(symbol, feature, ts, prediction);

        this.io.emit('symbol:update', {
            symbol,
            feature,
            prediction,
            candle,
            status: state.status,
            activeTrade
        });
    }

    async handlePrediction(message) {
        let data;
        try { data = JSON.parse(message); } catch (e) { return; }
        const symbol = this.normalizeSymbol(data.symbol);
        if (!symbol) return;

        this.predictionCount += 1;
        const ts = this.safeNumber(data.ts, this.now());
        const point = {
            ts,
            time: Math.floor(ts / 1000),
            long: this.safeNumber(data.long ?? data.probLong ?? data.LONG, null),
            short: this.safeNumber(data.short ?? data.probShort ?? data.SHORT, null),
            side: data.side || data.bias || null,
            prob: this.safeNumber(data.prob, null),
            leverage: this.safeNumber(data.leverage, null),
            macroScore: this.safeNumber(data.macroScore ?? data.macro, null)
        };

        const arr = this.aiTimeline.get(symbol) || [];
        arr.push(point);
        this.aiTimeline.set(symbol, this.clampArray(arr, MAX_AI_POINTS_PER_SYMBOL));

        const prev = this.symbolState.get(symbol) || { symbol, feature: null, lastSeen: ts };
        prev.prediction = point;
        this.symbolState.set(symbol, prev);
        this.attachPredictionToCandle(symbol, point);

        this.io.emit('prediction:update', { symbol, prediction: point });
    }

    async handleTradesSignal(message) {
        await this.refreshTrades();
        this.syncOpenTradeStatusToSymbols();
        this.io.emit('trades:update', this.tradesCache);
        this.io.emit('account:update', await this.buildAccountPayload());
        this.io.emit('watchlist:update', this.buildWatchlistPayload());
    }

    handleLog(message) {
        let data;
        try { data = JSON.parse(message); } catch (e) { data = { symbol: 'SYSTEM', msg: String(message), ts: this.now() }; }
        this.addLog(data);
    }

    handleRisk(message) {
        let data;
        try { data = JSON.parse(message); } catch (e) { data = { symbol: 'RISK', reason: String(message), ts: this.now() }; }
        data.ts = data.ts || this.now();
        this.riskEvents.push(data);
        this.riskEvents = this.clampArray(this.trimByTime(this.riskEvents, 'ts', KEEP_HISTORY_MS), MAX_LOGS);
        this.io.emit('risk:update', { events: this.riskEvents });
    }

    addLog(data) {
        const log = {
            symbol: this.normalizeSymbol(data.symbol || 'SYSTEM'),
            msg: data.msg || data.message || JSON.stringify(data),
            ts: data.ts || this.now()
        };
        this.logs.push(log);
        this.logs = this.clampArray(this.trimByTime(this.logs, 'ts', KEEP_HISTORY_MS), MAX_LOGS);
        this.io.emit('logs:update', { logs: this.logs });
    }

    // ========================================================
    // 4. CANDLE BUILDER
    // ========================================================
    getFeaturePrice(feature) {
        return this.safeNumber(
            this.getField(feature, ['last_price', 'lastPrice', 'price', 'close', 'mark_price', 'markPrice'], 0),
            0
        );
    }

    getFeatureTooltip(feature, prediction) {
        return {
            spread: this.safeNumber(this.getField(feature, ['spread_close', 'spread'], 0), 0),
            fundingRate: this.safeNumber(this.getField(feature, ['funding_rate', 'fundingRate'], 0), 0),
            atr14: this.safeNumber(this.getField(feature, ['ATR14', 'atr14'], 0), 0),
            mfa: this.safeNumber(this.getField(feature, ['MFA', 'mfa'], 0), 0),
            ofi: this.safeNumber(this.getField(feature, ['OFI', 'ofi'], 0), 0),
            vpin: this.safeNumber(this.getField(feature, ['VPIN', 'vpin'], 0), 0),
            whaleNet: this.safeNumber(this.getField(feature, ['WHALE_NET', 'whaleNet'], 0), 0),
            takerBuyRatio: this.safeNumber(this.getField(feature, ['taker_buy_ratio', 'takerBuyRatio'], 0), 0),
            btcRelativeStrength: this.safeNumber(this.getField(feature, ['btc_relative_strength', 'btcRelativeStrength'], 0), 0),
            aiLong: prediction?.long ?? null,
            aiShort: prediction?.short ?? null,
            aiSide: prediction?.side ?? null,
            aiProb: prediction?.prob ?? null
        };
    }

    updateCandle(symbol, feature, tsMs, prediction = null) {
        const price = this.getFeaturePrice(feature);
        if (price <= 0) return null;

        const bucket = Math.floor(tsMs / 1000 / CANDLE_INTERVAL_SEC) * CANDLE_INTERVAL_SEC;
        const bucketMs = bucket * 1000;
        const arr = this.candles.get(symbol) || [];
        let last = arr[arr.length - 1];

        if (!last || last.time !== bucket) {
            last = {
                time: bucket,              // seconds, dùng cho lightweight-charts
                timeMs: bucketMs,
                open: price,
                high: price,
                low: price,
                close: price,
                markPrice: this.safeNumber(this.getField(feature, ['mark_price', 'markPrice'], price), price),
                volumeProxy: 1,
                tickCount: 1,
                isSynthetic: false,
                tooltip: this.getFeatureTooltip(feature, prediction)
            };
            arr.push(last);
        } else {
            last.high = Math.max(last.high, price);
            last.low = Math.min(last.low, price);
            last.close = price;
            last.markPrice = this.safeNumber(this.getField(feature, ['mark_price', 'markPrice'], last.markPrice || price), last.markPrice || price);
            last.volumeProxy += 1;
            last.tickCount += 1;
            last.tooltip = this.getFeatureTooltip(feature, prediction);
        }

        const trimmed = this.trimCandles(arr);
        this.candles.set(symbol, this.clampArray(trimmed, MAX_CANDLES_PER_SYMBOL));
        return last;
    }

    attachPredictionToCandle(symbol, prediction) {
        const arr = this.candles.get(symbol);
        if (!arr || !arr.length || !prediction?.ts) return;
        const bucket = Math.floor(prediction.ts / 1000 / CANDLE_INTERVAL_SEC) * CANDLE_INTERVAL_SEC;
        const candle = arr.find(c => c.time === bucket) || arr[arr.length - 1];
        if (!candle) return;
        candle.tooltip = candle.tooltip || {};
        candle.tooltip.aiLong = prediction.long;
        candle.tooltip.aiShort = prediction.short;
        candle.tooltip.aiSide = prediction.side;
        candle.tooltip.aiProb = prediction.prob;
        candle.tooltip.leverage = prediction.leverage;
        candle.tooltip.macroScore = prediction.macroScore;
    }

    trimCandles(arr) {
        const cutoffSec = Math.floor((this.now() - KEEP_HISTORY_MS) / 1000);
        return arr.filter(c => Number(c.time) >= cutoffSec);
    }

    buildContinuousCandles(symbol) {
        const raw = this.trimCandles(this.candles.get(symbol) || [])
            .filter(c => c && Number.isFinite(Number(c.time)) && Number(c.close) > 0)
            .sort((a, b) => a.time - b.time);

        if (!raw.length) return [];

        const byTime = new Map();
        for (const c of raw) byTime.set(Number(c.time), c);

        const first = raw[0];
        const last = raw[raw.length - 1];
        const nowBucket = Math.floor(this.now() / 1000 / CANDLE_INTERVAL_SEC) * CANDLE_INTERVAL_SEC;
        const end = Math.min(nowBucket, last.time + Math.floor(FEATURE_LOST_MS / 1000));

        const result = [];
        let prevClose = first.open || first.close;

        for (let t = first.time; t <= end; t += CANDLE_INTERVAL_SEC) {
            const real = byTime.get(t);
            if (real) {
                prevClose = real.close;
                result.push(real);
            } else {
                result.push({
                    time: t,
                    timeMs: t * 1000,
                    open: prevClose,
                    high: prevClose,
                    low: prevClose,
                    close: prevClose,
                    markPrice: prevClose,
                    volumeProxy: 0,
                    tickCount: 0,
                    isSynthetic: true,
                    tooltip: {
                        synthetic: true,
                        note: 'Nến nội suy do không có tick trong bucket này'
                    }
                });
            }
        }

        return this.clampArray(result, MAX_CANDLES_PER_SYMBOL);
    }

    buildTradeMarkers(symbol) {
        const cutoff = this.now() - KEEP_HISTORY_MS;
        const trades = [
            ...(this.tradesCache.open || []),
            ...(this.tradesCache.closed || [])
        ].filter(t => t.symbol === symbol);

        const markers = [];
        for (const t of trades) {
            const side = String(t.type || t.side || '').toUpperCase();
            const isLong = side === 'LONG';
            const openMs = this.toMs(t.openTime || t.openTs);
            const closeMs = this.toMs(t.closeTime || t.closeTs);

            if (openMs && openMs >= cutoff) {
                markers.push({
                    id: `${t.id}:IN`,
                    time: Math.floor(openMs / 1000),
                    type: 'ENTRY',
                    side,
                    position: isLong ? 'belowBar' : 'aboveBar',
                    shape: isLong ? 'arrowUp' : 'arrowDown',
                    text: `IN ${side}`,
                    price: this.safeNumber(t.executedEntryPrice ?? t.entryPrice, null),
                    trade: t
                });
            }

            if (closeMs && closeMs >= cutoff) {
                const pnl = this.safeNumber(t.pnl ?? t.netPnl, 0);
                markers.push({
                    id: `${t.id}:OUT`,
                    time: Math.floor(closeMs / 1000),
                    type: 'EXIT',
                    side,
                    position: isLong ? 'aboveBar' : 'belowBar',
                    shape: 'circle',
                    text: `OUT ${pnl.toFixed(3)}`,
                    price: this.safeNumber(t.executedClosePrice ?? t.closePrice, null),
                    pnl,
                    reason: t.reason,
                    trade: t
                });
            }
        }

        markers.sort((a, b) => a.time - b.time);
        return markers;
    }

    // ========================================================
    // 5. MONGO TRADES
    // ========================================================
    async connectMongo() {
        if (!MONGO_URI) {
            console.warn('⚠️ [Dashboard] Missing MONGO_URI_SCOUT');
            return;
        }
        if (mongoose.connection.readyState === 1) return;
        await mongoose.connect(MONGO_URI);
        console.log('📦 [Dashboard] Mongo connected');
    }

    async refreshTrades() {
        try {
            if (mongoose.connection.readyState !== 1) return this.tradesCache;

            const open = await ScoutTrade.find({ status: 'OPEN' })
                .sort({ openTime: -1 })
                .limit(50)
                .lean();

            const closed = await ScoutTrade.find({ status: { $in: ['CLOSED', 'LIQUIDATED', 'FAILED', 'CANCELLED'] } })
                .sort({ closeTime: -1, updatedAt: -1 })
                .limit(120)
                .lean();

            this.tradesCache = {
                open: open.map(t => this.compactTrade(t)).filter(t => this.isOpenStatus(t.status)),
                closed: closed.map(t => this.compactTrade(t)).filter(t => this.isClosedStatus(t.status)),
                updatedAt: this.now()
            };

            this.syncOpenTradeStatusToSymbols();
        } catch (error) {
            console.error('❌ [Dashboard] refreshTrades error:', error.message);
        }
        return this.tradesCache;
    }

    compactTrade(t) {
        return {
            id: String(t._id),
            symbol: this.normalizeSymbol(t.symbol),
            type: t.type || t.side,
            mode: t.mode,
            status: t.status,
            orderType: t.orderType,
            leverage: t.leverage,
            margin: t.margin,
            size: t.size,
            notionalValue: t.notionalValue,
            entryPrice: t.entryPrice,
            closePrice: t.closePrice,
            executedEntryPrice: t.executedEntryPrice,
            executedClosePrice: t.executedClosePrice,
            liquidationPrice: t.liquidationPrice,
            prob: t.prob,
            probLong: t.probLong,
            probShort: t.probShort,
            macroScore: t.macroScore,
            pnl: t.pnl,
            roi: t.roi,
            grossPnl: t.grossPnl,
            netPnl: t.netPnl,
            mae: t.mae,
            mfe: t.mfe,
            reason: t.reason,
            durationMs: t.durationMs,
            openTime: t.openTime,
            closeTime: t.closeTime,
            openTs: t.openTs,
            closeTs: t.closeTs,
            modelVersion: t.modelVersion
        };
    }

    findOpenTrade(symbol) {
        symbol = this.normalizeSymbol(symbol);
        return (this.tradesCache.open || []).find(t => t.symbol === symbol && this.isOpenStatus(t.status)) || null;
    }

    syncOpenTradeStatusToSymbols() {
        for (const [symbol, state] of this.symbolState.entries()) {
            const openTrade = this.findOpenTrade(symbol);
            state.activeTrade = openTrade;
            if (openTrade) state.status = 'IN_TRADE';
            else state.status = this.statusFromLastSeen(state.lastSeen || 0);
            this.symbolState.set(symbol, state);
        }
    }

    // ========================================================
    // 6. PAYLOAD BUILDERS
    // ========================================================
    buildWatchlistPayload() {
        const symbols = [];
        const now = this.now();

        for (const [symbol, state] of this.symbolState.entries()) {
            const f = state.feature || {};
            const pred = state.prediction || {};
            const openTrade = this.findOpenTrade(symbol);
            const status = openTrade ? 'IN_TRADE' : this.statusFromLastSeen(state.lastSeen || 0);

            // QUAN TRỌNG: coin LOST không hiển thị trong danh sách đang live.
            if (status === 'LOST' && !openTrade) continue;

            const aiLong = pred.long;
            const aiShort = pred.short;
            const score = Math.max(this.safeNumber(aiLong, 0), this.safeNumber(aiShort, 0));
            const bias = aiLong == null || aiShort == null ? 'WAIT' : (aiLong >= aiShort ? 'LONG' : 'SHORT');

            symbols.push({
                symbol,
                status,
                price: this.safeNumber(this.getField(f, ['last_price', 'lastPrice', 'price', 'close'], 0), 0),
                markPrice: this.safeNumber(this.getField(f, ['mark_price', 'markPrice'], this.getFeaturePrice(f)), 0),
                spread: this.safeNumber(this.getField(f, ['spread_close', 'spread'], 0), 0),
                fundingRate: this.safeNumber(this.getField(f, ['funding_rate', 'fundingRate'], 0), 0),
                aiLong,
                aiShort,
                score,
                bias,
                mfa: this.safeNumber(this.getField(f, ['MFA', 'mfa'], 0), 0),
                ofi: this.safeNumber(this.getField(f, ['OFI', 'ofi'], 0), 0),
                vpin: this.safeNumber(this.getField(f, ['VPIN', 'vpin'], 0), 0),
                whaleNet: this.safeNumber(this.getField(f, ['WHALE_NET', 'whaleNet'], 0), 0),
                atr14: this.safeNumber(this.getField(f, ['ATR14', 'atr14'], 0), 0),
                btcRelativeStrength: this.safeNumber(this.getField(f, ['btc_relative_strength', 'btcRelativeStrength'], 0), 0),
                lastSeen: state.lastSeen,
                ageMs: now - (state.lastSeen || 0),
                activeTrade: openTrade
            });
        }

        symbols.sort((a, b) => {
            if (a.status === 'IN_TRADE' && b.status !== 'IN_TRADE') return -1;
            if (b.status === 'IN_TRADE' && a.status !== 'IN_TRADE') return 1;
            if (a.status === 'LIVE' && b.status !== 'LIVE') return -1;
            if (b.status === 'LIVE' && a.status !== 'LIVE') return 1;
            return b.score - a.score;
        });

        return { symbols: symbols.slice(0, MAX_WATCHLIST_SYMBOLS), updatedAt: now };
    }

    buildSymbolPayload(symbol) {
        symbol = this.normalizeSymbol(symbol);
        const state = this.symbolState.get(symbol) || null;
        const aiTimeline = this.trimByTime(this.aiTimeline.get(symbol) || [], 'ts', KEEP_HISTORY_MS);
        const closedTrades = (this.tradesCache.closed || []).filter(t => t.symbol === symbol).slice(0, 30);

        return {
            symbol,
            state,
            candles: this.buildContinuousCandles(symbol),
            aiTimeline,
            markers: this.buildTradeMarkers(symbol),
            openTrade: this.findOpenTrade(symbol),
            closedTrades
        };
    }

    async buildAccountPayload() {
        const now = this.now();
        let paperBalance = null;
        let liveAsset = null;

        try {
            const raw = await this.dataClient.get(PAPER_WALLET_KEY);
            paperBalance = raw != null ? this.safeNumber(raw, null) : null;
        } catch (e) {}

        try {
            const rawAsset = await this.dataClient.get(`${ACCOUNT_STATE_PREFIX}:asset:USDT`);
            liveAsset = rawAsset ? JSON.parse(rawAsset) : null;
        } catch (e) {}

        const todayStart = new Date();
        todayStart.setHours(0, 0, 0, 0);

        let todayStats = { pnl: 0, trades: 0, wins: 0, losses: 0, winrate: 0 };
        try {
            if (mongoose.connection.readyState === 1) {
                const todayClosed = await ScoutTrade.find({
                    status: { $in: ['CLOSED', 'LIQUIDATED'] },
                    closeTime: { $gte: todayStart }
                }).lean();

                const pnl = todayClosed.reduce((sum, t) => sum + this.safeNumber(t.pnl ?? t.netPnl, 0), 0);
                const wins = todayClosed.filter(t => this.safeNumber(t.pnl ?? t.netPnl, 0) > 0).length;
                const losses = todayClosed.filter(t => this.safeNumber(t.pnl ?? t.netPnl, 0) < 0).length;
                todayStats = {
                    pnl,
                    trades: todayClosed.length,
                    wins,
                    losses,
                    winrate: todayClosed.length > 0 ? (wins / todayClosed.length) * 100 : 0
                };
            }
        } catch (e) {}

        this.accountCache = {
            mode: process.env.EXCHANGE_MODE || 'PAPER',
            paperBalance,
            liveAsset,
            openTrades: this.tradesCache.open.length,
            closedTradesToday: todayStats.trades,
            todayPnl: todayStats.pnl,
            todayWins: todayStats.wins,
            todayLosses: todayStats.losses,
            winrateToday: todayStats.winrate,
            updatedAt: now
        };

        return this.accountCache;
    }

    buildHealthPayload() {
        const now = this.now();
        return {
            redis: this.dataClient.status,
            mongo: mongoose.connection.readyState === 1 ? 'OK' : 'NOT_CONNECTED',
            feed: this.lastFeatureAt > 0 && now - this.lastFeatureAt < FEATURE_LOST_MS ? 'OK' : 'NO_RECENT_FEATURE',
            lastFeatureAgeMs: this.lastFeatureAt ? now - this.lastFeatureAt : null,
            featureCount: this.featureCount,
            predictionCount: this.predictionCount,
            watchedSymbols: this.symbolState.size,
            visibleSymbols: this.buildWatchlistPayload().symbols.length,
            connectedClients: this.connectedClients,
            updatedAt: now
        };
    }

    // ========================================================
    // 7. CLEANUP / PERIODIC EMITTERS
    // ========================================================
    trimByTime(items, key, keepMs) {
        const cutoff = this.now() - keepMs;
        return (items || []).filter(item => this.safeNumber(item?.[key], 0) >= cutoff);
    }

    cleanupMemory() {
        const now = this.now();

        for (const [symbol, state] of this.symbolState.entries()) {
            const openTrade = this.findOpenTrade(symbol);
            const age = now - (state.lastSeen || 0);

            // Không xóa symbol đang có lệnh mở.
            if (!openTrade && age > REMOVE_LOST_SYMBOL_MS) {
                this.symbolState.delete(symbol);
                this.candles.delete(symbol);
                this.aiTimeline.delete(symbol);
                continue;
            }

            this.candles.set(symbol, this.clampArray(this.trimCandles(this.candles.get(symbol) || []), MAX_CANDLES_PER_SYMBOL));
            this.aiTimeline.set(symbol, this.clampArray(this.trimByTime(this.aiTimeline.get(symbol) || [], 'ts', KEEP_HISTORY_MS), MAX_AI_POINTS_PER_SYMBOL));
        }

        this.logs = this.clampArray(this.trimByTime(this.logs, 'ts', KEEP_HISTORY_MS), MAX_LOGS);
        this.riskEvents = this.clampArray(this.trimByTime(this.riskEvents, 'ts', KEEP_HISTORY_MS), MAX_LOGS);
    }

    startEmitLoops() {
        setInterval(() => {
            this.io.emit('watchlist:update', this.buildWatchlistPayload());
        }, WATCHLIST_EMIT_MS);

        setInterval(async () => {
            this.io.emit('account:update', await this.buildAccountPayload());
        }, ACCOUNT_EMIT_MS);

        setInterval(() => {
            this.io.emit('system:health', this.buildHealthPayload());
        }, HEALTH_EMIT_MS);

        setInterval(async () => {
            await this.refreshTrades();
            this.io.emit('trades:update', this.tradesCache);
            this.io.emit('watchlist:update', this.buildWatchlistPayload());
        }, TRADE_REFRESH_MS);

        setInterval(() => {
            this.cleanupMemory();
        }, CLEANUP_MS);
    }

    // ========================================================
    // 8. STARTUP
    // ========================================================
    async start() {
        console.log('🚀 [Dashboard] Starting Trading Control Room server...');

        this.setupHttp();
        this.setupSocket();

        await this.connectMongo();
        await this.refreshTrades();
        await this.startRedisSubscriptions();
        this.startEmitLoops();

        this.server.listen(PORT, HOST, () => {
            console.log(`✅ [Dashboard] Listening on http://${HOST}:${PORT}`);
            console.log(`📊 [Dashboard] Candle interval=${CANDLE_INTERVAL_SEC}s | keep=${Math.round(KEEP_HISTORY_MS / 60000)}m`);
        });
    }

    async stop() {
        try { await this.subClient.quit(); } catch (e) {}
        try { await this.dataClient.quit(); } catch (e) {}
        try { await this.pubClient.quit(); } catch (e) {}
        try { await mongoose.disconnect(); } catch (e) {}
        try { this.server.close(); } catch (e) {}
    }
}

if (require.main === module) {
    const dashboard = new DashboardServer();
    dashboard.start().catch(err => {
        console.error('❌ [Dashboard FATAL]', err.message);
        process.exit(1);
    });

    process.on('SIGINT', async () => {
        await dashboard.stop();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        await dashboard.stop();
        process.exit(0);
    });
}

module.exports = DashboardServer;
