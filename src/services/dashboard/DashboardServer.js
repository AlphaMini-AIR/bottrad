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
// DASHBOARD SERVER V3 - TRADING CONTROL ROOM BACKEND + TRADE AUDIT
// ------------------------------------------------------------
// Mục tiêu:
// 1. Không hiển thị coin LOST trong watchlist.
// 2. Tự dọn symbol/candle/AI/log quá cũ để UI không phình.
// 3. Chuẩn hóa nến liên tục trong 1 giờ gần nhất để chart không bị đứt.
// 4. Gắn thêm thông tin tooltip vào từng nến.
// 5. Tạo marker IN/OUT từ lệnh OPEN/CLOSED cho frontend vẽ chart.
// 6. Giữ backend chỉ quan sát, không tự đặt lệnh.
// 7. Thêm trade audit: bấm vào lệnh xem timeline, bộ não, lỗi, feature vào/ra.
// ============================================================

const configPath = path.join(__dirname, '../../../system_config.json');
let config;

try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    config = {
        REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379',
        CHANNELS: {}
    };
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
const MAX_CANDLES_PER_SYMBOL = Number(process.env.DASHBOARD_MAX_CANDLES || 720);
const MAX_AI_POINTS_PER_SYMBOL = Number(process.env.DASHBOARD_MAX_AI_POINTS || 1200);
const MAX_LOGS = Number(process.env.DASHBOARD_MAX_LOGS || 300);
const MAX_WATCHLIST_SYMBOLS = Number(process.env.DASHBOARD_MAX_WATCHLIST_SYMBOLS || 60);

const FEATURE_LOST_MS = Number(process.env.DASHBOARD_FEATURE_LOST_MS || 10000);
const FEATURE_STALE_MS = Number(process.env.DASHBOARD_FEATURE_STALE_MS || 3000);
const KEEP_HISTORY_MS = Number(process.env.DASHBOARD_KEEP_HISTORY_MS || 60 * 60 * 1000);
const REMOVE_LOST_SYMBOL_MS = Number(process.env.DASHBOARD_REMOVE_LOST_SYMBOL_MS || 90 * 1000);

const TRADE_CONTEXT_BEFORE_MS = Number(process.env.DASHBOARD_TRADE_CONTEXT_BEFORE_MS || 3 * 60 * 1000);
const TRADE_CONTEXT_AFTER_MS = Number(process.env.DASHBOARD_TRADE_CONTEXT_AFTER_MS || 3 * 60 * 1000);

const ENTRY_BRAIN_MODEL = process.env.ENTRY_BRAIN_MODEL || 'EntryBrain.onnx';
const EXIT_BRAIN_MODEL = process.env.EXIT_BRAIN_MODEL || 'ExitBrain.onnx';
const BRAIN_VERSION = process.env.BRAIN_VERSION || process.env.MODEL_VERSION || 'TRAIN_DUAL_BRAIN_V3_1_4';

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

        this.symbolState = new Map();
        this.candles = new Map();
        this.aiTimeline = new Map();
        this.logs = [];
        this.riskEvents = [];

        this.tradesCache = {
            open: [],
            closed: [],
            updatedAt: 0
        };

        this.accountCache = {
            updatedAt: 0
        };

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
            if (obj[key] !== undefined && obj[key] !== null) {
                return obj[key];
            }
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

        if (value instanceof Date) {
            return value.getTime();
        }

        const n = Number(value);
        if (Number.isFinite(n)) {
            return n < 1e12 ? n * 1000 : n;
        }

        const parsed = Date.parse(value);
        return Number.isFinite(parsed) ? parsed : null;
    }

    isClosedStatus(status) {
        const s = String(status || '').toUpperCase();

        return [
            'CLOSED',
            'LIQUIDATED',
            'FAILED',
            'CANCELLED',
            'LOST',
            'STOP_LOSS',
            'TAKE_PROFIT'
        ].includes(s);
    }

    isOpenStatus(status) {
        const s = String(status || '').toUpperCase();

        return [
            'OPEN',
            'ACTIVE',
            'IN_TRADE'
        ].includes(s);
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

        this.app.get('/api/brain/status', (req, res) => {
            res.json(this.buildBrainStatusPayload());
        });

        this.app.get('/api/trade/:id', async (req, res) => {
            try {
                const payload = await this.buildTradeDetailPayload(req.params.id);

                if (!payload) {
                    return res.status(404).json({
                        error: 'TRADE_NOT_FOUND'
                    });
                }

                return res.json(payload);
            } catch (error) {
                console.error('❌ [Dashboard] trade detail error:', error.message);

                return res.status(500).json({
                    error: 'TRADE_DETAIL_ERROR',
                    message: error.message
                });
            }
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
            socket.emit('system:health', this.buildHealthPayload());

            this.buildAccountPayload()
                .then(payload => socket.emit('account:update', payload))
                .catch(() => {});

            socket.on('symbol:select', symbolRaw => {
                const symbol = this.normalizeSymbol(symbolRaw);
                socket.emit('symbol:snapshot', this.buildSymbolPayload(symbol));
            });

            socket.on('command', async cmd => {
                const allowed = [
                    'PAUSE_TRADING',
                    'RESUME_TRADING',
                    'CLOSE_ALL',
                    'FORCE_CLOSE',
                    'RELOAD_AI'
                ];

                if (!cmd || !allowed.includes(cmd.action)) return;

                await this.pubClient.publish('system:commands', JSON.stringify(cmd));

                this.addLog({
                    symbol: 'DASHBOARD',
                    msg: `Sent command ${cmd.action}`,
                    ts: this.now()
                });
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
        await this.subClient.subscribe(
            PREDICTION_CHANNEL,
            TRADES_CHANNEL,
            LOGS_CHANNEL,
            RISK_CHANNEL
        );

        this.subClient.on('pmessageBuffer', async (pattern, channel, messageBuffer) => {
            try {
                await this.handleFeatureMessage(channel.toString(), messageBuffer);
            } catch (error) {
                console.error('❌ [Dashboard] feature handler error:', error.message);
            }
        });

        this.subClient.on('message', async (channel, message) => {
            try {
                if (channel === PREDICTION_CHANNEL) {
                    await this.handlePrediction(message);
                } else if (channel === TRADES_CHANNEL) {
                    await this.handleTradesSignal(message);
                } else if (channel === LOGS_CHANNEL) {
                    this.handleLog(message);
                } else if (channel === RISK_CHANNEL) {
                    this.handleRisk(message);
                }
            } catch (error) {
                console.error('❌ [Dashboard] redis message error:', error.message);
            }
        });

        console.log('✅ [Dashboard] Redis subscriptions ready');
        console.log(`📡 FEATURE_PATTERN=${FEATURE_PATTERN}`);
        console.log(`📡 PREDICTION_CHANNEL=${PREDICTION_CHANNEL}`);
        console.log(`📡 TRADES_CHANNEL=${TRADES_CHANNEL}`);
        console.log(`📡 LOGS_CHANNEL=${LOGS_CHANNEL}`);
        console.log(`📡 RISK_CHANNEL=${RISK_CHANNEL}`);
    }

    decodeMessage(messageBuffer) {
        try {
            return decode(messageBuffer);
        } catch (e) {
            try {
                return JSON.parse(messageBuffer.toString());
            } catch (err) {
                return {};
            }
        }
    }

    async handleFeatureMessage(channel, messageBuffer) {
        const feature = this.decodeMessage(messageBuffer);
        const symbol = this.normalizeSymbol(
            feature.symbol || channel.split(':').pop()
        );

        if (!symbol) return;

        const ts = this.safeNumber(
            feature._ts ||
            feature.feature_ts ||
            feature.timestamp ||
            feature.ts ||
            Date.now(),
            Date.now()
        );

        const price = this.safeNumber(
            feature.mark_price ||
            feature.last_price ||
            feature.price ||
            feature.close ||
            feature.index_price,
            0
        );

        if (price <= 0) return;

        this.lastFeatureAt = this.now();
        this.featureCount += 1;

        const previous = this.symbolState.get(symbol) || {};

        const state = {
            ...previous,
            ...feature,
            symbol,
            ts,
            price,
            lastPrice: price,
            markPrice: this.safeNumber(feature.mark_price, price),
            indexPrice: this.safeNumber(feature.index_price, price),
            spreadBps: this.safeNumber(feature.spread_bps, 0),
            featureReadyScore: this.safeNumber(feature.feature_ready_score, 1),
            tradeFlowImbalance: this.safeNumber(feature.trade_flow_imbalance, 0),
            micropriceBias: this.safeNumber(feature.microprice_bias, 0),
            topBookImbalance: this.safeNumber(feature.top_book_imbalance, 0),
            k1mTakerBuyRatio: this.safeNumber(feature.k1m_taker_buy_ratio, this.safeNumber(feature.taker_buy_ratio, 0.5)),
            k1mRangePct: this.safeNumber(feature.k1m_range_pct, 0),
            k1mBodyPct: this.safeNumber(feature.k1m_body_pct, 0),
            k1mWickPct: this.safeNumber(feature.k1m_wick_pct, 0),
            k1mClosePosition: this.safeNumber(feature.k1m_close_position, 0.5),
            liqNetQuote: this.safeNumber(feature.liq_net_quote, 0),
            maxTradeQuoteImbalance: this.safeNumber(feature.max_trade_quote_imbalance, 0),
            localQuoteVolume: this.safeNumber(feature.local_quote_volume, 0),
            localBuyQuote: this.safeNumber(feature.local_buy_quote, 0),
            localSellQuote: this.safeNumber(feature.local_sell_quote, 0),
            localTradeCount: this.safeNumber(feature.local_trade_count, 0),
            localTakerBuyRatio: this.safeNumber(feature.local_taker_buy_ratio, 0.5),
            priceAgeMs: this.safeNumber(feature.price_age_ms, 0),
            depthAgeMs: this.safeNumber(feature.depth_age_ms, 0),
            tradeAgeMs: this.safeNumber(feature.trade_age_ms, 0),
            markPriceAgeMs: this.safeNumber(feature.mark_price_age_ms, 0),
            bookTickerAgeMs: this.safeNumber(feature.book_ticker_age_ms, 0),
            klineAgeMs: this.safeNumber(feature.kline_age_ms, 0),
            radarAgeMs: this.safeNumber(feature.radar_age_ms, 0),
            isDataStale: Boolean(feature.is_data_stale),
            rawFeature: feature,
            lastSeen: this.now()
        };

        state.status = this.statusFromLastSeen(state.lastSeen);

        this.symbolState.set(symbol, state);
        this.updateCandle(symbol, state);
    }

    async handlePrediction(message) {
        let payload;

        try {
            payload = typeof message === 'string' ? JSON.parse(message) : message;
        } catch (e) {
            return;
        }

        const symbol = this.normalizeSymbol(payload.symbol);
        if (!symbol) return;

        const ts = this.safeNumber(payload.ts || payload.timestamp || Date.now(), Date.now());

        const point = {
            ...payload,
            symbol,
            ts,
            entry: payload.entry || payload.entryBrain || payload.EntryBrain || null,
            exit: payload.exit || payload.exitBrain || payload.ExitBrain || null,
            flat: this.safeNumber(payload.flat ?? payload.p_flat, null),
            long: this.safeNumber(payload.long ?? payload.p_long, null),
            short: this.safeNumber(payload.short ?? payload.p_short, null),
            hold: this.safeNumber(payload.hold ?? payload.p_hold, null),
            exitProb: this.safeNumber(payload.exitProb ?? payload.p_exit, null),
            confidence: this.safeNumber(payload.confidence, null),
            raw: payload
        };

        const arr = this.aiTimeline.get(symbol) || [];
        arr.push(point);
        this.aiTimeline.set(symbol, this.clampArray(arr, MAX_AI_POINTS_PER_SYMBOL));

        this.predictionCount += 1;

        this.io.emit('ai:update', point);
    }

    async handleTradesSignal(message) {
        this.handleLog(JSON.stringify({
            symbol: 'TRADE',
            msg: `Trade signal: ${String(message).slice(0, 300)}`,
            ts: this.now()
        }));

        await this.refreshTrades();

        this.io.emit('trades:update', this.tradesCache);
        this.io.emit('watchlist:update', this.buildWatchlistPayload());
    }

    handleLog(message) {
        let payload;

        try {
            payload = typeof message === 'string' ? JSON.parse(message) : message;
        } catch (e) {
            payload = {
                msg: String(message),
                ts: this.now()
            };
        }

        this.addLog(payload);
    }

    handleRisk(message) {
        let payload;

        try {
            payload = typeof message === 'string' ? JSON.parse(message) : message;
        } catch (e) {
            payload = {
                msg: String(message),
                ts: this.now()
            };
        }

        const event = {
            ...payload,
            symbol: this.normalizeSymbol(payload.symbol || 'RISK'),
            ts: this.safeNumber(payload.ts || payload.timestamp || Date.now(), Date.now())
        };

        this.riskEvents.push(event);
        this.riskEvents = this.clampArray(this.riskEvents, MAX_LOGS);

        this.io.emit('risk:update', event);
    }

    addLog(payload) {
        const log = {
            symbol: this.normalizeSymbol(payload.symbol || 'SYSTEM'),
            msg: payload.msg || payload.message || JSON.stringify(payload),
            level: payload.level || payload.type || 'info',
            ts: this.safeNumber(payload.ts || payload.timestamp || Date.now(), Date.now()),
            raw: payload
        };

        this.logs.push(log);
        this.logs = this.clampArray(this.logs, MAX_LOGS);

        this.io.emit('logs:update', { logs: this.logs });
    }

    // ========================================================
    // 4. CANDLES
    // ========================================================
    candleBucket(ts) {
        return Math.floor(ts / 1000 / CANDLE_INTERVAL_SEC) * CANDLE_INTERVAL_SEC;
    }

    updateCandle(symbol, state) {
        const bucket = this.candleBucket(state.ts);
        const arr = this.candles.get(symbol) || [];
        const last = arr[arr.length - 1];
        const price = state.price;

        const tooltip = this.buildCandleTooltip(state);

        if (!last || last.time !== bucket) {
            arr.push({
                time: bucket,
                open: price,
                high: price,
                low: price,
                close: price,
                volume: this.safeNumber(state.localQuoteVolume, 0),
                tooltip,
                raw: {
                    price: state.price,
                    featureReadyScore: state.featureReadyScore,
                    spreadBps: state.spreadBps,
                    tradeFlowImbalance: state.tradeFlowImbalance,
                    micropriceBias: state.micropriceBias,
                    topBookImbalance: state.topBookImbalance,
                    k1mTakerBuyRatio: state.k1mTakerBuyRatio,
                    k1mRangePct: state.k1mRangePct,
                    liqNetQuote: state.liqNetQuote,
                    ts: state.ts
                }
            });
        } else {
            last.high = Math.max(last.high, price);
            last.low = Math.min(last.low, price);
            last.close = price;
            last.volume += this.safeNumber(state.localQuoteVolume, 0);
            last.tooltip = tooltip;
            last.raw = {
                price: state.price,
                featureReadyScore: state.featureReadyScore,
                spreadBps: state.spreadBps,
                tradeFlowImbalance: state.tradeFlowImbalance,
                micropriceBias: state.micropriceBias,
                topBookImbalance: state.topBookImbalance,
                k1mTakerBuyRatio: state.k1mTakerBuyRatio,
                k1mRangePct: state.k1mRangePct,
                liqNetQuote: state.liqNetQuote,
                ts: state.ts
            };
        }

        this.candles.set(symbol, this.clampArray(arr, MAX_CANDLES_PER_SYMBOL));
    }

    buildCandleTooltip(state) {
        return {
            symbol: state.symbol,
            price: state.price,
            markPrice: state.markPrice,
            spreadBps: state.spreadBps,
            readyScore: state.featureReadyScore,
            flow: state.tradeFlowImbalance,
            microBias: state.micropriceBias,
            topBook: state.topBookImbalance,
            taker1m: state.k1mTakerBuyRatio,
            range1m: state.k1mRangePct,
            liqNet: state.liqNetQuote,
            age: {
                price: state.priceAgeMs,
                depth: state.depthAgeMs,
                trade: state.tradeAgeMs,
                mark: state.markPriceAgeMs,
                bookTicker: state.bookTickerAgeMs,
                kline: state.klineAgeMs,
                radar: state.radarAgeMs
            }
        };
    }

    trimCandles(arr) {
        const cutoffSec = Math.floor((this.now() - KEEP_HISTORY_MS) / 1000);
        return (arr || []).filter(c => this.safeNumber(c.time, 0) >= cutoffSec);
    }

    buildContinuousCandles(symbol) {
        const raw = this.trimCandles(this.candles.get(symbol) || []);
        if (!raw.length) return [];

        const sorted = raw.sort((a, b) => a.time - b.time);
        const out = [];
        let prev = sorted[0];

        out.push(prev);

        for (let i = 1; i < sorted.length; i += 1) {
            const cur = sorted[i];
            let t = prev.time + CANDLE_INTERVAL_SEC;

            while (t < cur.time) {
                out.push({
                    time: t,
                    open: prev.close,
                    high: prev.close,
                    low: prev.close,
                    close: prev.close,
                    volume: 0,
                    synthetic: true,
                    tooltip: {
                        synthetic: true,
                        symbol,
                        price: prev.close
                    }
                });

                t += CANDLE_INTERVAL_SEC;
            }

            out.push(cur);
            prev = cur;
        }

        return this.clampArray(out, MAX_CANDLES_PER_SYMBOL);
    }

    // ========================================================
    // 5. TRADE / MONGO
    // ========================================================
    async connectMongo() {
        if (!MONGO_URI) {
            console.warn('⚠️ [Dashboard] MONGO_URI_SCOUT missing, trade/account DB features limited');
            return;
        }

        try {
            await mongoose.connect(MONGO_URI, {
                maxPoolSize: 5,
                serverSelectionTimeoutMS: 5000
            });

            console.log('✅ [Dashboard] Mongo connected');
        } catch (error) {
            console.error('❌ [Dashboard] Mongo connect failed:', error.message);
        }
    }

    compactTrade(t) {
        const id = String(t._id || t.id || t.tradeId || '');

        const openMs = this.toMs(t.openTime || t.entryTime || t.createdAt || t.openTs || t.entryTs);
        const closeMs = this.toMs(t.closeTime || t.exitTime || t.updatedAt || t.closeTs || t.exitTs);

        const entryBrain = t.entryBrain || t.entryDecision || t.brainEntry || t.entry || null;
        const exitBrain = t.exitBrain || t.exitDecision || t.brainExit || t.exit || null;

        return {
            id,
            tradeId: t.tradeId || t.clientTradeId || id,
            symbol: this.normalizeSymbol(t.symbol),
            type: String(t.type || t.side || '').toUpperCase(),
            status: String(t.status || '').toUpperCase(),
            margin: this.safeNumber(t.margin, 0),
            leverage: this.safeNumber(t.leverage, 0),
            quantity: this.safeNumber(t.quantity || t.qty, 0),
            entryPrice: this.safeNumber(t.entryPrice || t.openPrice, 0),
            closePrice: this.safeNumber(t.closePrice || t.exitPrice, 0),
            executedEntryPrice: this.safeNumber(t.executedEntryPrice || t.fillEntryPrice, null),
            executedClosePrice: this.safeNumber(t.executedClosePrice || t.fillClosePrice, null),
            pnl: this.safeNumber(t.pnl ?? t.netPnl, 0),
            pnlPct: this.safeNumber(t.pnlPct ?? t.roi ?? t.netRoi, 0),
            fee: this.safeNumber(t.fee ?? t.totalFee, 0),
            reason: t.closeReason || t.reason || t.exitReason || '',
            openTime: openMs,
            closeTime: closeMs,
            holdMs: openMs && closeMs ? Math.max(0, closeMs - openMs) : null,
            entryBrain,
            exitBrain,
            hasAudit: true,
            rawStatus: t.status
        };
    }

    async refreshTrades() {
        if (mongoose.connection.readyState !== 1) {
            return this.tradesCache;
        }

        try {
            const openRaw = await ScoutTrade.find({
                status: { $in: ['OPEN', 'ACTIVE', 'IN_TRADE'] }
            })
                .sort({ openTime: -1, createdAt: -1 })
                .limit(100)
                .lean();

            const closedRaw = await ScoutTrade.find({
                status: { $in: ['CLOSED', 'LIQUIDATED', 'FAILED', 'CANCELLED', 'STOP_LOSS', 'TAKE_PROFIT'] }
            })
                .sort({ closeTime: -1, updatedAt: -1 })
                .limit(200)
                .lean();

            this.tradesCache = {
                open: openRaw.map(t => this.compactTrade(t)),
                closed: closedRaw.map(t => this.compactTrade(t)),
                updatedAt: this.now()
            };
        } catch (error) {
            console.error('❌ [Dashboard] refreshTrades failed:', error.message);
        }

        return this.tradesCache;
    }

    findOpenTrade(symbol) {
        return this.tradesCache.open.find(t => t.symbol === symbol);
    }

    findTradeInCache(id) {
        const text = String(id || '');

        return [
            ...this.tradesCache.open,
            ...this.tradesCache.closed
        ].find(t => String(t.id) === text || String(t.tradeId) === text);
    }

    async findTradeRaw(id) {
        if (mongoose.connection.readyState !== 1) return null;

        const text = String(id || '');

        let trade = null;

        if (mongoose.Types.ObjectId.isValid(text)) {
            trade = await ScoutTrade.findById(text).lean();
            if (trade) return trade;
        }

        trade = await ScoutTrade.findOne({
            $or: [
                { tradeId: text },
                { clientTradeId: text },
                { id: text }
            ]
        }).lean();

        return trade;
    }

    findNearestFeature(symbol, tsMs) {
        const candles = this.candles.get(symbol) || [];

        if (!candles.length || !tsMs) return null;

        let best = null;
        let bestDistance = Infinity;

        for (const candle of candles) {
            const candleMs = this.safeNumber(candle.time, 0) * 1000;
            const dist = Math.abs(candleMs - tsMs);

            if (dist < bestDistance) {
                best = candle;
                bestDistance = dist;
            }
        }

        return best ? best.raw || best.tooltip || best : null;
    }

    getContextRange(compact) {
        const openMs = this.toMs(compact.openTime);
        const closeMs = this.toMs(compact.closeTime);
        const baseStart = openMs || closeMs || this.now();
        const baseEnd = closeMs || openMs || this.now();

        return {
            start: Math.max(0, baseStart - TRADE_CONTEXT_BEFORE_MS),
            end: baseEnd + TRADE_CONTEXT_AFTER_MS
        };
    }

    filterContextItems(items, symbol, start, end) {
        return (items || []).filter(item => {
            const itemSymbol = this.normalizeSymbol(item.symbol || item.raw?.symbol);
            const ts = this.safeNumber(item.ts || item.timestamp, 0);

            const symbolOk = !itemSymbol || itemSymbol === symbol || itemSymbol === 'SYSTEM' || itemSymbol === 'TRADE' || itemSymbol === 'RISK';

            return symbolOk && ts >= start && ts <= end;
        });
    }

    buildBrainStatusPayload() {
        const now = this.now();

        return {
            version: BRAIN_VERSION,
            entry: {
                name: 'EntryBrain',
                model: ENTRY_BRAIN_MODEL,
                output: ['FLAT', 'LONG', 'SHORT'],
                status: 'READY',
                lastDecisionAt: this.getLastBrainDecisionAt('entry')
            },
            exit: {
                name: 'ExitBrain',
                model: EXIT_BRAIN_MODEL,
                output: ['HOLD', 'EXIT'],
                status: 'READY',
                lastDecisionAt: this.getLastBrainDecisionAt('exit')
            },
            updatedAt: now
        };
    }

    getLastBrainDecisionAt(type) {
        let latest = 0;

        for (const arr of this.aiTimeline.values()) {
            for (const point of arr || []) {
                const isExit = point.exit != null || point.hold != null || String(point.brain || '').toLowerCase().includes('exit');
                const matched = type === 'exit' ? isExit : !isExit;
                if (matched) latest = Math.max(latest, this.safeNumber(point.ts, 0));
            }
        }

        return latest || null;
    }

    buildTradeBrainSnapshot(compact, rawTrade, relatedAi) {
        const entryAi = [];
        const exitAi = [];

        for (const point of relatedAi || []) {
            const isExit = point.exit != null || point.hold != null || String(point.brain || '').toLowerCase().includes('exit');

            if (isExit) {
                exitAi.push(point);
            } else {
                entryAi.push(point);
            }
        }

        return {
            version: BRAIN_VERSION,
            entryBrain: compact.entryBrain || rawTrade?.entryBrain || rawTrade?.entryDecision || entryAi[entryAi.length - 1] || null,
            exitBrain: compact.exitBrain || rawTrade?.exitBrain || rawTrade?.exitDecision || exitAi[exitAi.length - 1] || null,
            entryAiTimeline: entryAi,
            exitAiTimeline: exitAi,
            summary: {
                lastEntryDecision: entryAi[entryAi.length - 1] || null,
                lastExitDecision: exitAi[exitAi.length - 1] || null,
                entryDecisionCount: entryAi.length,
                exitDecisionCount: exitAi.length
            }
        };
    }

    buildTradeMarkers(compact) {
        const markers = [];

        if (compact.openTime && compact.entryPrice) {
            markers.push({
                id: `${compact.id}:open`,
                type: 'IN',
                side: compact.type,
                time: Math.floor(compact.openTime / 1000),
                price: this.safeNumber(compact.executedEntryPrice ?? compact.entryPrice, compact.entryPrice),
                tradeId: compact.tradeId,
                label: `${compact.type} IN`
            });
        }

        if (compact.closeTime && compact.closePrice) {
            markers.push({
                id: `${compact.id}:close`,
                type: 'OUT',
                side: compact.type,
                time: Math.floor(compact.closeTime / 1000),
                price: this.safeNumber(compact.executedClosePrice ?? compact.closePrice, compact.closePrice),
                tradeId: compact.tradeId,
                pnl: compact.pnl,
                reason: compact.reason,
                label: `${compact.type} OUT`
            });
        }

        return markers.sort((a, b) => a.time - b.time);
    }

    buildTradeAuditTimeline(compact, rawTrade, aiTimeline, relatedLogs, relatedRisk) {
        const timeline = [];
        const openMs = this.toMs(compact.openTime || compact.openTs);
        const closeMs = this.toMs(compact.closeTime || compact.closeTs);
        const side = String(compact.type || '').toUpperCase();

        if (Array.isArray(rawTrade?.timeline)) {
            for (const item of rawTrade.timeline) {
                timeline.push({
                    ts: this.toMs(item.ts || item.time || item.createdAt) || this.now(),
                    type: item.event || item.type || 'TRADE_EVENT',
                    title: item.title || item.event || item.type || 'Trade event',
                    detail: item
                });
            }
        }

        if (openMs) {
            timeline.push({
                ts: openMs,
                type: 'ORDER_OPEN',
                title: `OPEN ${side}`,
                brain: compact.entryBrain,
                price: compact.executedEntryPrice ?? compact.entryPrice,
                detail: compact
            });
        }

        for (const p of aiTimeline || []) {
            const brain = p.brain || (p.exit != null || p.hold != null ? 'ExitBrain' : 'EntryBrain');

            timeline.push({
                ts: this.safeNumber(p.ts, 0),
                type: String(brain).toUpperCase().includes('EXIT') ? 'EXIT_BRAIN_DECISION' : 'ENTRY_BRAIN_DECISION',
                title: `${brain} decision`,
                brain,
                detail: p
            });
        }

        for (const risk of relatedRisk || []) {
            timeline.push({
                ts: this.safeNumber(risk.ts, 0),
                type: 'RISK_EVENT',
                title: risk.reason || risk.msg || 'Risk event',
                detail: risk
            });
        }

        for (const log of relatedLogs || []) {
            timeline.push({
                ts: this.safeNumber(log.ts, 0),
                type: 'LOG',
                title: log.msg || 'Log',
                detail: log
            });
        }

        if (closeMs) {
            timeline.push({
                ts: closeMs,
                type: 'ORDER_CLOSE',
                title: `CLOSE ${side}`,
                brain: compact.exitBrain,
                price: compact.executedClosePrice ?? compact.closePrice,
                pnl: compact.pnl ?? compact.netPnl,
                reason: compact.reason,
                detail: compact
            });
        }

        return timeline
            .filter(e => this.safeNumber(e.ts, 0) > 0)
            .sort((a, b) => this.safeNumber(a.ts, 0) - this.safeNumber(b.ts, 0));
    }

    extractOrderErrors(rawTrade, relatedLogs) {
        const errors = [];

        if (Array.isArray(rawTrade?.errors)) {
            errors.push(...rawTrade.errors);
        }

        for (const log of relatedLogs || []) {
            const msg = String(log.msg || '');

            if (/error|lỗi|fail|failed|reject|mongo|wallet|binance/i.test(msg)) {
                errors.push({
                    ts: log.ts,
                    stage: 'LOG',
                    message: msg,
                    source: log.symbol || 'SYSTEM'
                });
            }
        }

        return errors;
    }

    async buildTradeDetailPayload(id) {
        await this.refreshTrades();

        const cached = this.findTradeInCache(id);
        const rawTrade = await this.findTradeRaw(id);

        if (!cached && !rawTrade) return null;

        const compact = cached || this.compactTrade(rawTrade);
        const symbol = this.normalizeSymbol(compact.symbol);

        const { start, end } = this.getContextRange(compact);

        const ai = this.filterContextItems(this.aiTimeline.get(symbol) || [], symbol, start, end);
        const logs = this.filterContextItems(this.logs, symbol, start, end);
        const risk = this.filterContextItems(this.riskEvents, symbol, start, end);

        const entryFeature = this.findNearestFeature(symbol, compact.openTime);
        const exitFeature = this.findNearestFeature(symbol, compact.closeTime);

        return {
            trade: compact,
            rawTrade,
            symbol,
            context: {
                start,
                end,
                beforeMs: TRADE_CONTEXT_BEFORE_MS,
                afterMs: TRADE_CONTEXT_AFTER_MS
            },
            brainSnapshot: this.buildTradeBrainSnapshot(compact, rawTrade, ai),
            decisionTimeline: this.buildTradeAuditTimeline(compact, rawTrade, ai, logs, risk),
            orderErrors: this.extractOrderErrors(rawTrade, logs),
            features: {
                entry: entryFeature,
                exit: exitFeature
            },
            chart: {
                candles: this.buildContinuousCandles(symbol),
                markers: this.buildTradeMarkers(compact)
            },
            logs,
            risk,
            ai,
            updatedAt: this.now()
        };
    }

    // ========================================================
    // 6. PAYLOAD BUILDERS
    // ========================================================
    buildWatchlistPayload() {
        const rows = [];

        for (const [symbol, state] of this.symbolState.entries()) {
            const status = this.statusFromLastSeen(state.lastSeen);

            if (status === 'LOST') {
                const openTrade = this.findOpenTrade(symbol);
                if (!openTrade) continue;
            }

            const openTrade = this.findOpenTrade(symbol);

            rows.push({
                symbol,
                price: state.price,
                markPrice: state.markPrice,
                indexPrice: state.indexPrice,
                spreadBps: state.spreadBps,
                readyScore: state.featureReadyScore,
                flow: state.tradeFlowImbalance,
                microBias: state.micropriceBias,
                topBook: state.topBookImbalance,
                taker1m: state.k1mTakerBuyRatio,
                volume: state.localQuoteVolume,
                status,
                isDataStale: state.isDataStale,
                openTrade: openTrade || null,
                lastSeen: state.lastSeen,
                ageMs: this.now() - state.lastSeen
            });
        }

        rows.sort((a, b) => {
            const aTrade = a.openTrade ? 1 : 0;
            const bTrade = b.openTrade ? 1 : 0;

            if (aTrade !== bTrade) return bTrade - aTrade;

            const aReady = this.safeNumber(a.readyScore, 0);
            const bReady = this.safeNumber(b.readyScore, 0);

            if (aReady !== bReady) return bReady - aReady;

            return this.safeNumber(b.volume, 0) - this.safeNumber(a.volume, 0);
        });

        return {
            symbols: rows.slice(0, MAX_WATCHLIST_SYMBOLS),
            totalVisible: rows.length,
            totalTracked: this.symbolState.size,
            updatedAt: this.now()
        };
    }

    buildSymbolPayload(symbol) {
        const state = this.symbolState.get(symbol) || null;

        const ai = this.aiTimeline.get(symbol) || [];
        const candles = this.buildContinuousCandles(symbol);

        return {
            symbol,
            state,
            candles,
            ai,
            markers: this.buildAllTradeMarkers(symbol),
            openTrade: this.findOpenTrade(symbol) || null,
            logs: this.logs.filter(l => l.symbol === symbol).slice(-100),
            updatedAt: this.now()
        };
    }

    buildAllTradeMarkers(symbol) {
        const markers = [];

        const trades = [
            ...this.tradesCache.open,
            ...this.tradesCache.closed
        ].filter(t => t.symbol === symbol);

        for (const trade of trades) {
            markers.push(...this.buildTradeMarkers(trade));
        }

        return markers.sort((a, b) => a.time - b.time);
    }

    async buildAccountPayload() {
        const now = this.now();

        let paperBalance = null;
        let liveAsset = null;

        try {
            const raw = await this.dataClient.get(PAPER_WALLET_KEY);
            paperBalance = raw !== null ? this.safeNumber(raw, null) : null;
        } catch (e) {}

        try {
            const rawAsset = await this.dataClient.get(`${ACCOUNT_STATE_PREFIX}:asset:USDT`);
            liveAsset = rawAsset ? JSON.parse(rawAsset) : null;
        } catch (e) {}

        const todayStart = new Date();
        todayStart.setHours(0, 0, 0, 0);

        let todayStats = {
            pnl: 0,
            trades: 0,
            wins: 0,
            losses: 0,
            winrate: 0
        };

        try {
            if (mongoose.connection.readyState === 1) {
                const todayClosed = await ScoutTrade.find({
                    status: { $in: ['CLOSED', 'LIQUIDATED'] },
                    closeTime: { $gte: todayStart }
                }).lean();

                const pnl = todayClosed.reduce((sum, t) => {
                    return sum + this.safeNumber(t.pnl ?? t.netPnl, 0);
                }, 0);

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
            brain: this.buildBrainStatusPayload(),
            updatedAt: now
        };
    }

    // ========================================================
    // 7. CLEANUP / PERIODIC EMITTERS
    // ========================================================
    trimByTime(items, key, keepMs) {
        const cutoff = this.now() - keepMs;

        return (items || []).filter(item => {
            return this.safeNumber(item?.[key], 0) >= cutoff;
        });
    }

    cleanupMemory() {
        const now = this.now();

        for (const [symbol, state] of this.symbolState.entries()) {
            const openTrade = this.findOpenTrade(symbol);
            const age = now - (state.lastSeen || 0);

            if (!openTrade && age > REMOVE_LOST_SYMBOL_MS) {
                this.symbolState.delete(symbol);
                this.candles.delete(symbol);
                this.aiTimeline.delete(symbol);
                continue;
            }

            this.candles.set(
                symbol,
                this.clampArray(this.trimCandles(this.candles.get(symbol) || []), MAX_CANDLES_PER_SYMBOL)
            );

            this.aiTimeline.set(
                symbol,
                this.clampArray(this.trimByTime(this.aiTimeline.get(symbol) || [], 'ts', KEEP_HISTORY_MS), MAX_AI_POINTS_PER_SYMBOL)
            );
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
        try {
            await this.subClient.quit();
        } catch (e) {}

        try {
            await this.dataClient.quit();
        } catch (e) {}

        try {
            await this.pubClient.quit();
        } catch (e) {}

        try {
            await mongoose.disconnect();
        } catch (e) {}

        try {
            this.server.close();
        } catch (e) {}
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