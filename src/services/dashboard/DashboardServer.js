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
// DASHBOARD SERVER V1 - TRADING CONTROL ROOM BACKEND
// ------------------------------------------------------------
// Vai trò:
// 1. Nghe Redis market:features:* từ FeedHandler.
// 2. Nghe dashboard:predictions từ OrderManager.
// 3. Nghe dashboard:logs / dashboard:trades.
// 4. Tự build nến live từ last_price để giao diện vẽ chart.
// 5. Query MongoDB ScoutTrade để hiển thị OPEN/CLOSED trades.
// 6. Đọc ví PAPER từ Redis key paper:wallet:balance:v19.
// 7. Đẩy realtime ra browser bằng Socket.IO.
//
// File này chỉ quan sát, không gửi lệnh trading.
// Các lệnh control sau này nên đi qua system:commands riêng.
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
const CANDLE_INTERVAL_SEC = Number(process.env.DASHBOARD_CANDLE_INTERVAL_SEC || 5);
const MAX_CANDLES_PER_SYMBOL = Number(process.env.DASHBOARD_MAX_CANDLES || 500);
const MAX_AI_POINTS_PER_SYMBOL = Number(process.env.DASHBOARD_MAX_AI_POINTS || 1000);
const MAX_LOGS = Number(process.env.DASHBOARD_MAX_LOGS || 300);
const FEATURE_LOST_MS = Number(process.env.DASHBOARD_FEATURE_LOST_MS || 10000);
const FEATURE_STALE_MS = Number(process.env.DASHBOARD_FEATURE_STALE_MS || 3000);

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
        this.candles = new Map();           // symbol -> candle array
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
        const n = Number(value);
        return Number.isFinite(n) ? n : fallback;
    }

    now() {
        return Date.now();
    }

    clampArray(arr, max) {
        if (arr.length <= max) return arr;
        return arr.slice(arr.length - max);
    }

    statusFromLastSeen(lastSeen) {
        const age = this.now() - lastSeen;
        if (age > FEATURE_LOST_MS) return 'LOST';
        if (age > FEATURE_STALE_MS) return 'STALE';
        return 'LIVE';
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
                // Chỉ hỗ trợ command an toàn qua dashboard.
                // Nếu muốn khóa lại bằng auth, thêm middleware sau.
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

    async handleFeatureMessage(channel, messageBuffer) {
        let feature;
        try {
            feature = decode(messageBuffer);
        } catch (error) {
            return;
        }

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
        const candle = this.updateCandle(symbol, feature, ts);

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
        const ts = data.ts || this.now();
        const point = {
            ts,
            long: this.safeNumber(data.long, null),
            short: this.safeNumber(data.short, null)
        };

        const arr = this.aiTimeline.get(symbol) || [];
        arr.push(point);
        this.aiTimeline.set(symbol, this.clampArray(arr, MAX_AI_POINTS_PER_SYMBOL));

        const prev = this.symbolState.get(symbol) || { symbol, feature: null, lastSeen: ts };
        prev.prediction = point;
        this.symbolState.set(symbol, prev);

        this.io.emit('prediction:update', { symbol, prediction: point });
    }

    async handleTradesSignal(message) {
        await this.refreshTrades();
        this.io.emit('trades:update', this.tradesCache);
        this.io.emit('account:update', await this.buildAccountPayload());
    }

    handleLog(message) {
        let data;
        try { data = JSON.parse(message); } catch (e) { data = { symbol: 'SYSTEM', msg: String(message), ts: this.now() }; }
        this.addLog(data);
    }

    handleRisk(message) {
        let data;
        try { data = JSON.parse(message); } catch (e) { data = { symbol: 'RISK', reason: String(message), ts: this.now() }; }
        this.riskEvents.push(data);
        this.riskEvents = this.clampArray(this.riskEvents, MAX_LOGS);
        this.io.emit('risk:update', { events: this.riskEvents });
    }

    addLog(data) {
        const log = {
            symbol: this.normalizeSymbol(data.symbol || 'SYSTEM'),
            msg: data.msg || data.message || JSON.stringify(data),
            ts: data.ts || this.now()
        };
        this.logs.push(log);
        this.logs = this.clampArray(this.logs, MAX_LOGS);
        this.io.emit('logs:update', { logs: this.logs });
    }

    // ========================================================
    // 4. CANDLE BUILDER
    // ========================================================
    updateCandle(symbol, feature, tsMs) {
        const price = this.safeNumber(feature.last_price, 0);
        if (price <= 0) return null;

        const bucket = Math.floor(tsMs / 1000 / CANDLE_INTERVAL_SEC) * CANDLE_INTERVAL_SEC;
        const arr = this.candles.get(symbol) || [];
        let last = arr[arr.length - 1];

        if (!last || last.time !== bucket) {
            last = {
                time: bucket,
                open: price,
                high: price,
                low: price,
                close: price,
                markPrice: this.safeNumber(feature.mark_price, price),
                volumeProxy: 1
            };
            arr.push(last);
        } else {
            last.high = Math.max(last.high, price);
            last.low = Math.min(last.low, price);
            last.close = price;
            last.markPrice = this.safeNumber(feature.mark_price, last.markPrice || price);
            last.volumeProxy += 1;
        }

        this.candles.set(symbol, this.clampArray(arr, MAX_CANDLES_PER_SYMBOL));
        return last;
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
                .limit(100)
                .lean();

            this.tradesCache = {
                open: open.map(t => this.compactTrade(t)),
                closed: closed.map(t => this.compactTrade(t)),
                updatedAt: this.now()
            };
        } catch (error) {
            console.error('❌ [Dashboard] refreshTrades error:', error.message);
        }
        return this.tradesCache;
    }

    compactTrade(t) {
        return {
            id: String(t._id),
            symbol: t.symbol,
            type: t.type,
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
        return this.tradesCache.open.find(t => t.symbol === symbol) || null;
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
            const aiLong = pred.long;
            const aiShort = pred.short;
            const score = Math.max(this.safeNumber(aiLong, 0), this.safeNumber(aiShort, 0));
            const bias = aiLong == null || aiShort == null ? 'WAIT' : (aiLong >= aiShort ? 'LONG' : 'SHORT');

            symbols.push({
                symbol,
                status,
                price: this.safeNumber(f.last_price, 0),
                markPrice: this.safeNumber(f.mark_price, f.last_price || 0),
                spread: this.safeNumber(f.spread_close, 0),
                fundingRate: this.safeNumber(f.funding_rate, 0),
                aiLong,
                aiShort,
                score,
                bias,
                mfa: this.safeNumber(f.MFA, 0),
                ofi: this.safeNumber(f.OFI, 0),
                vpin: this.safeNumber(f.VPIN, 0),
                whaleNet: this.safeNumber(f.WHALE_NET, 0),
                atr14: this.safeNumber(f.ATR14, 0),
                btcRelativeStrength: this.safeNumber(f.btc_relative_strength, 0),
                lastSeen: state.lastSeen,
                ageMs: now - (state.lastSeen || 0),
                activeTrade: openTrade
            });
        }

        symbols.sort((a, b) => {
            if (a.status === 'IN_TRADE' && b.status !== 'IN_TRADE') return -1;
            if (b.status === 'IN_TRADE' && a.status !== 'IN_TRADE') return 1;
            return b.score - a.score;
        });

        return { symbols, updatedAt: now };
    }

    buildSymbolPayload(symbol) {
        symbol = this.normalizeSymbol(symbol);
        return {
            symbol,
            state: this.symbolState.get(symbol) || null,
            candles: this.candles.get(symbol) || [],
            aiTimeline: this.aiTimeline.get(symbol) || [],
            openTrade: this.findOpenTrade(symbol),
            closedTrades: this.tradesCache.closed.filter(t => t.symbol === symbol).slice(0, 30)
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
            connectedClients: this.connectedClients,
            updatedAt: now
        };
    }

    // ========================================================
    // 7. PERIODIC EMITTERS
    // ========================================================
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
        }, TRADE_REFRESH_MS);
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
            console.log(`📊 [Dashboard] Candle interval=${CANDLE_INTERVAL_SEC}s`);
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
