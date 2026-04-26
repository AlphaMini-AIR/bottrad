const axios = require('axios');
const crypto = require('crypto');
const EventEmitter = require('events');
const WebSocket = require('ws');
const Redis = require('ioredis');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../../.env') });

// ============================================================
// BINANCE USER DATA STREAM V1 - PRIVATE ACCOUNT STATE ENGINE
// ------------------------------------------------------------
// Vai trò:
// 1. Tạo listenKey cho Binance USDⓈ-M Futures.
// 2. Kết nối private WebSocket endpoint mới: /private/ws/<listenKey>.
// 3. Keepalive listenKey trước khi hết hạn 60 phút.
// 4. Chủ động rotate WebSocket trước 24h.
// 5. Nhận và chuẩn hóa các event quan trọng:
//    - ACCOUNT_UPDATE
//    - ORDER_TRADE_UPDATE
//    - MARGIN_CALL
//    - listenKeyExpired
// 6. Lưu account/position/order state vào memory + Redis.
// 7. Emit event cho LiveExchange / OrderManager dùng để reconcile trạng thái thật.
//
// Lưu ý live-ready:
// - Không được trade live nếu không có UserDataStream hoặc AccountState sync.
// - REST chỉ dùng để gửi lệnh/khởi tạo; trạng thái thật nên bám ORDER_TRADE_UPDATE.
// ============================================================

const configPath = path.join(__dirname, '../../../system_config.json');
let config;
try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    config = { REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379' };
}

const REDIS_URL = config.REDIS_URL || process.env.REDIS_URL || 'redis://localhost:6379';

const BINANCE_FAPI_BASE = process.env.BINANCE_FAPI_BASE || 'https://fapi.binance.com';
const BINANCE_PRIVATE_WS_BASE = process.env.BINANCE_PRIVATE_WS_BASE || 'wss://fstream.binance.com/private';
const API_KEY = process.env.BINANCE_API_KEY || '';
const API_SECRET = process.env.BINANCE_API_SECRET || '';

const REQUEST_TIMEOUT_MS = Number(process.env.BINANCE_USER_STREAM_TIMEOUT_MS || 10000);
const LISTEN_KEY_KEEPALIVE_MS = Number(process.env.LISTEN_KEY_KEEPALIVE_MS || 45 * 60 * 1000);
const WS_ROTATE_MS = Number(process.env.USER_STREAM_WS_ROTATE_MS || 23.5 * 60 * 60 * 1000);
const RECONNECT_BASE_DELAY_MS = Number(process.env.USER_STREAM_RECONNECT_BASE_MS || 1000);
const RECONNECT_MAX_DELAY_MS = Number(process.env.USER_STREAM_RECONNECT_MAX_MS || 60000);
const STATE_REDIS_PREFIX = process.env.ACCOUNT_STATE_REDIS_PREFIX || 'account:state';

class BinanceUserDataStream extends EventEmitter {
    constructor() {
        super();

        this.redis = new Redis(REDIS_URL);
        this.redis.on('error', err => console.error('❌ [UserDataStream Redis]', err.message));

        this.listenKey = null;
        this.ws = null;
        this.wsConnectedAt = 0;
        this.reconnectAttempts = 0;

        this.keepAliveTimer = null;
        this.rotateTimer = null;
        this.reconnectTimer = null;

        this.isRunning = false;
        this.isConnecting = false;
        this.lastEventTime = 0;

        // Account state in memory.
        this.assets = new Map();      // asset -> balance object
        this.positions = new Map();   // symbol -> position object
        this.orders = new Map();      // clientOrderId/orderId -> order object
    }

    // ========================================================
    // 1. HELPERS
    // ========================================================
    _requireKeys() {
        if (!API_KEY || !API_SECRET) {
            throw new Error('Missing BINANCE_API_KEY or BINANCE_API_SECRET');
        }
    }

    _sign(queryString) {
        return crypto
            .createHmac('sha256', API_SECRET)
            .update(queryString)
            .digest('hex');
    }

    _safeNumber(value, fallback = 0) {
        const n = Number(value);
        return Number.isFinite(n) ? n : fallback;
    }

    _normalizeSymbol(symbol) {
        return String(symbol || '').toUpperCase().trim();
    }

    _jitter(ms, ratio = 0.25) {
        const spread = ms * ratio;
        return Math.max(250, Math.round(ms - spread + Math.random() * spread * 2));
    }

    _signedQuery(params = {}) {
        const payload = {
            ...params,
            timestamp: Date.now()
        };
        const query = new URLSearchParams(payload).toString();
        const signature = this._sign(query);
        return `${query}&signature=${signature}`;
    }

    async _publicUserStreamRequest(method, endpoint) {
        this._requireKeys();

        const response = await axios({
            method,
            url: `${BINANCE_FAPI_BASE}${endpoint}`,
            headers: { 'X-MBX-APIKEY': API_KEY },
            timeout: REQUEST_TIMEOUT_MS
        });

        return response.data;
    }

    async _signedGet(endpoint, params = {}) {
        this._requireKeys();
        const query = this._signedQuery(params);

        const response = await axios.get(`${BINANCE_FAPI_BASE}${endpoint}?${query}`, {
            headers: { 'X-MBX-APIKEY': API_KEY },
            timeout: REQUEST_TIMEOUT_MS
        });

        return response.data;
    }

    // ========================================================
    // 2. LISTEN KEY LIFECYCLE
    // ========================================================
    async createListenKey() {
        const data = await this._publicUserStreamRequest('POST', '/fapi/v1/listenKey');
        if (!data || !data.listenKey) {
            throw new Error('Binance did not return listenKey');
        }

        this.listenKey = data.listenKey;
        await this.redis.set(`${STATE_REDIS_PREFIX}:listenKey`, this.listenKey, 'EX', 60 * 60);
        console.log('🔑 [UserDataStream] listenKey created/refreshed');
        return this.listenKey;
    }

    async keepAliveListenKey() {
        if (!this.listenKey) {
            await this.createListenKey();
            return;
        }

        try {
            await this._publicUserStreamRequest('PUT', '/fapi/v1/listenKey');
            await this.redis.expire(`${STATE_REDIS_PREFIX}:listenKey`, 60 * 60);
            console.log('💓 [UserDataStream] listenKey keepalive OK');
        } catch (error) {
            const code = error.response?.data?.code;
            const msg = error.response?.data?.msg || error.message;
            console.warn(`⚠️ [UserDataStream] keepalive failed: ${code || ''} ${msg}`);

            // -1125: This listenKey does not exist. Recreate.
            if (code === -1125 || String(msg).toLowerCase().includes('listenkey')) {
                await this.createListenKey();
                await this.reconnect('LISTEN_KEY_RECREATED');
            }
        }
    }

    startKeepAliveLoop() {
        if (this.keepAliveTimer) clearInterval(this.keepAliveTimer);
        this.keepAliveTimer = setInterval(() => {
            this.keepAliveListenKey().catch(err => {
                console.error('❌ [UserDataStream] keepalive loop error:', err.message);
            });
        }, LISTEN_KEY_KEEPALIVE_MS);
    }

    async closeListenKey() {
        if (!this.listenKey) return;

        try {
            await this._publicUserStreamRequest('DELETE', '/fapi/v1/listenKey');
            console.log('🗑️ [UserDataStream] listenKey closed');
        } catch (error) {
            console.warn('⚠️ [UserDataStream] close listenKey failed:', error.message);
        }
    }

    // ========================================================
    // 3. REST SNAPSHOT FOR INITIAL SYNC / RECONCILE
    // ========================================================
    async syncAccountSnapshot() {
        try {
            const data = await this._signedGet('/fapi/v2/account', { recvWindow: 5000 });

            if (Array.isArray(data.assets)) {
                for (const asset of data.assets) {
                    this.assets.set(asset.asset, {
                        asset: asset.asset,
                        walletBalance: this._safeNumber(asset.walletBalance),
                        unrealizedProfit: this._safeNumber(asset.unrealizedProfit),
                        marginBalance: this._safeNumber(asset.marginBalance),
                        maintMargin: this._safeNumber(asset.maintMargin),
                        initialMargin: this._safeNumber(asset.initialMargin),
                        positionInitialMargin: this._safeNumber(asset.positionInitialMargin),
                        openOrderInitialMargin: this._safeNumber(asset.openOrderInitialMargin),
                        availableBalance: this._safeNumber(asset.availableBalance),
                        crossWalletBalance: this._safeNumber(asset.crossWalletBalance),
                        updateTime: Date.now(),
                        source: 'REST_ACCOUNT_SNAPSHOT'
                    });
                }
            }

            if (Array.isArray(data.positions)) {
                for (const pos of data.positions) {
                    const symbol = this._normalizeSymbol(pos.symbol);
                    const amt = this._safeNumber(pos.positionAmt);

                    this.positions.set(symbol, {
                        symbol,
                        positionAmt: amt,
                        entryPrice: this._safeNumber(pos.entryPrice),
                        breakEvenPrice: this._safeNumber(pos.breakEvenPrice),
                        markPrice: this._safeNumber(pos.markPrice),
                        unRealizedProfit: this._safeNumber(pos.unRealizedProfit),
                        liquidationPrice: this._safeNumber(pos.liquidationPrice),
                        leverage: this._safeNumber(pos.leverage),
                        maxNotionalValue: this._safeNumber(pos.maxNotionalValue),
                        marginType: pos.marginType,
                        isolatedMargin: this._safeNumber(pos.isolatedMargin),
                        isAutoAddMargin: pos.isAutoAddMargin,
                        positionSide: pos.positionSide || 'BOTH',
                        notional: this._safeNumber(pos.notional),
                        isolatedWallet: this._safeNumber(pos.isolatedWallet),
                        updateTime: Date.now(),
                        source: 'REST_ACCOUNT_SNAPSHOT'
                    });
                }
            }

            await this.persistState();
            this.emit('accountSnapshot', this.getState());
            console.log('📸 [UserDataStream] Account snapshot synced');
        } catch (error) {
            console.error('❌ [UserDataStream] syncAccountSnapshot error:', error.response?.data || error.message);
            throw error;
        }
    }

    // ========================================================
    // 4. WEBSOCKET CONNECTION
    // ========================================================
    async connectWebSocket() {
        if (!this.listenKey) await this.createListenKey();
        if (this.isConnecting) return;

        this.isConnecting = true;

        const url = `${BINANCE_PRIVATE_WS_BASE}/ws/${this.listenKey}`;
        console.log(`🔌 [UserDataStream] Connecting private WS...`);

        return new Promise((resolve, reject) => {
            const ws = new WebSocket(url, {
                perMessageDeflate: false,
                handshakeTimeout: REQUEST_TIMEOUT_MS
            });

            let resolved = false;

            ws.on('open', () => {
                this.ws = ws;
                this.wsConnectedAt = Date.now();
                this.reconnectAttempts = 0;
                this.isConnecting = false;
                console.log('✅ [UserDataStream] Private WS connected');

                this.scheduleRotate();
                this.emit('connected');

                resolved = true;
                resolve();
            });

            ws.on('message', raw => {
                this.handleMessage(raw).catch(err => {
                    console.error('❌ [UserDataStream] handleMessage error:', err.message);
                });
            });

            ws.on('close', (code, reason) => {
                console.warn(`⚠️ [UserDataStream] WS closed code=${code} reason=${reason}`);
                this.ws = null;
                this.emit('disconnected', { code, reason: String(reason || '') });

                if (this.isRunning) {
                    this.scheduleReconnect('WS_CLOSED');
                }
            });

            ws.on('error', err => {
                console.error('❌ [UserDataStream] WS error:', err.message);
                this.emit('error', err);

                if (!resolved) {
                    this.isConnecting = false;
                    reject(err);
                }
            });
        });
    }

    scheduleRotate() {
        if (this.rotateTimer) clearTimeout(this.rotateTimer);

        this.rotateTimer = setTimeout(() => {
            console.log('♻️ [UserDataStream] Rotating private WS before 24h limit');
            this.reconnect('ROTATE_24H').catch(err => {
                console.error('❌ [UserDataStream] rotate error:', err.message);
            });
        }, WS_ROTATE_MS);
    }

    scheduleReconnect(reason = 'UNKNOWN') {
        if (this.reconnectTimer) return;

        const attempt = this.reconnectAttempts++;
        const delay = this._jitter(Math.min(RECONNECT_MAX_DELAY_MS, RECONNECT_BASE_DELAY_MS * (2 ** attempt)));

        console.log(`⏳ [UserDataStream] Reconnect scheduled in ${delay}ms | reason=${reason}`);

        this.reconnectTimer = setTimeout(async () => {
            this.reconnectTimer = null;
            try {
                await this.reconnect(reason);
            } catch (error) {
                console.error('❌ [UserDataStream] reconnect error:', error.message);
                this.scheduleReconnect('RECONNECT_FAILED');
            }
        }, delay);
    }

    async reconnect(reason = 'MANUAL') {
        console.log(`🔄 [UserDataStream] Reconnecting... reason=${reason}`);

        if (this.ws) {
            try { this.ws.terminate(); } catch (e) {}
            this.ws = null;
        }

        await this.keepAliveListenKey();
        await this.connectWebSocket();

        // Sau reconnect nên sync snapshot vì có thể miss event khi disconnected.
        await this.syncAccountSnapshot();
        this.emit('reconnected', { reason });
    }

    // ========================================================
    // 5. EVENT HANDLERS
    // ========================================================
    async handleMessage(raw) {
        let event;
        try {
            event = JSON.parse(raw.toString());
        } catch (e) {
            return;
        }

        this.lastEventTime = Date.now();

        const eventType = event.e;
        if (!eventType) return;

        switch (eventType) {
            case 'ACCOUNT_UPDATE':
                await this.handleAccountUpdate(event);
                break;
            case 'ORDER_TRADE_UPDATE':
                await this.handleOrderTradeUpdate(event);
                break;
            case 'MARGIN_CALL':
                await this.handleMarginCall(event);
                break;
            case 'listenKeyExpired':
                await this.handleListenKeyExpired(event);
                break;
            default:
                this.emit('rawEvent', event);
                break;
        }
    }

    async handleAccountUpdate(event) {
        const update = event.a || {};
        const eventTime = event.E || Date.now();

        // Balance updates.
        for (const b of update.B || []) {
            const asset = b.a;
            if (!asset) continue;

            this.assets.set(asset, {
                asset,
                walletBalance: this._safeNumber(b.wb),
                crossWalletBalance: this._safeNumber(b.cw),
                balanceChange: this._safeNumber(b.bc),
                updateTime: eventTime,
                source: 'ACCOUNT_UPDATE'
            });
        }

        // Position updates.
        for (const p of update.P || []) {
            const symbol = this._normalizeSymbol(p.s);
            if (!symbol) continue;

            this.positions.set(symbol, {
                symbol,
                positionAmt: this._safeNumber(p.pa),
                entryPrice: this._safeNumber(p.ep),
                breakEvenPrice: this._safeNumber(p.bep),
                accumulatedRealized: this._safeNumber(p.cr),
                unrealizedPnl: this._safeNumber(p.up),
                marginType: p.mt,
                isolatedWallet: this._safeNumber(p.iw),
                positionSide: p.ps || 'BOTH',
                updateTime: eventTime,
                source: 'ACCOUNT_UPDATE'
            });
        }

        await this.persistState();
        this.emit('accountUpdate', event);
    }

    async handleOrderTradeUpdate(event) {
        const order = event.o || {};
        const symbol = this._normalizeSymbol(order.s);
        const eventTime = event.E || Date.now();

        const normalized = {
            symbol,
            clientOrderId: order.c,
            side: order.S,
            orderType: order.o,
            timeInForce: order.f,
            originalQty: this._safeNumber(order.q),
            originalPrice: this._safeNumber(order.p),
            averagePrice: this._safeNumber(order.ap),
            stopPrice: this._safeNumber(order.sp),
            executionType: order.x,
            orderStatus: order.X,
            orderId: order.i,
            lastFilledQty: this._safeNumber(order.l),
            accumulatedFilledQty: this._safeNumber(order.z),
            lastFilledPrice: this._safeNumber(order.L),
            commissionAsset: order.N,
            commission: this._safeNumber(order.n),
            orderTradeTime: order.T,
            tradeId: order.t,
            bidsNotional: this._safeNumber(order.b),
            asksNotional: this._safeNumber(order.a),
            isMaker: order.m,
            isReduceOnly: order.R,
            workingType: order.wt,
            originalOrderType: order.ot,
            positionSide: order.ps || 'BOTH',
            closeAll: order.cp,
            activationPrice: this._safeNumber(order.AP),
            callbackRate: this._safeNumber(order.cr),
            realizedProfit: this._safeNumber(order.rp),
            eventTime,
            raw: order
        };

        if (normalized.clientOrderId) this.orders.set(normalized.clientOrderId, normalized);
        if (normalized.orderId) this.orders.set(String(normalized.orderId), normalized);

        await this.persistOrder(normalized);
        this.emit('orderUpdate', normalized);

        if (normalized.orderStatus === 'FILLED') {
            this.emit('orderFilled', normalized);
        }

        if (['CANCELED', 'EXPIRED', 'REJECTED'].includes(normalized.orderStatus)) {
            this.emit('orderRejectedOrCanceled', normalized);
        }
    }

    async handleMarginCall(event) {
        console.error('🚨 [UserDataStream] MARGIN_CALL received:', JSON.stringify(event));
        await this.redis.set(`${STATE_REDIS_PREFIX}:margin_call:last`, JSON.stringify(event), 'EX', 24 * 60 * 60);
        this.emit('marginCall', event);
    }

    async handleListenKeyExpired(event) {
        console.warn('⚠️ [UserDataStream] listenKeyExpired event received');
        this.emit('listenKeyExpired', event);
        await this.createListenKey();
        await this.reconnect('LISTEN_KEY_EXPIRED');
    }

    // ========================================================
    // 6. STATE PERSISTENCE
    // ========================================================
    getState() {
        return {
            ts: Date.now(),
            lastEventTime: this.lastEventTime,
            listenKey: this.listenKey,
            assets: Object.fromEntries(this.assets.entries()),
            positions: Object.fromEntries(this.positions.entries())
        };
    }

    getPosition(symbol) {
        return this.positions.get(this._normalizeSymbol(symbol)) || null;
    }

    getAsset(asset = 'USDT') {
        return this.assets.get(asset) || null;
    }

    getOrder(id) {
        return this.orders.get(String(id)) || null;
    }

    hasOpenPosition(symbol) {
        const pos = this.getPosition(symbol);
        return Boolean(pos && Math.abs(this._safeNumber(pos.positionAmt)) > 0);
    }

    async persistState() {
        try {
            const state = this.getState();
            await this.redis.set(`${STATE_REDIS_PREFIX}:snapshot`, JSON.stringify(state), 'EX', 24 * 60 * 60);

            for (const [symbol, pos] of this.positions.entries()) {
                await this.redis.set(`${STATE_REDIS_PREFIX}:position:${symbol}`, JSON.stringify(pos), 'EX', 24 * 60 * 60);
            }

            for (const [asset, balance] of this.assets.entries()) {
                await this.redis.set(`${STATE_REDIS_PREFIX}:asset:${asset}`, JSON.stringify(balance), 'EX', 24 * 60 * 60);
            }
        } catch (error) {
            console.error('⚠️ [UserDataStream] persistState error:', error.message);
        }
    }

    async persistOrder(order) {
        try {
            if (order.clientOrderId) {
                await this.redis.set(`${STATE_REDIS_PREFIX}:order:client:${order.clientOrderId}`, JSON.stringify(order), 'EX', 24 * 60 * 60);
            }
            if (order.orderId) {
                await this.redis.set(`${STATE_REDIS_PREFIX}:order:id:${order.orderId}`, JSON.stringify(order), 'EX', 24 * 60 * 60);
            }
            await this.redis.publish('account:orders:update', JSON.stringify(order));
        } catch (error) {
            console.error('⚠️ [UserDataStream] persistOrder error:', error.message);
        }
    }

    // ========================================================
    // 7. PUBLIC START / STOP
    // ========================================================
    async start() {
        if (this.isRunning) return;
        this._requireKeys();
        this.isRunning = true;

        console.log('🧩 [UserDataStream] Starting Binance private account stream...');

        await this.createListenKey();
        this.startKeepAliveLoop();
        await this.syncAccountSnapshot();
        await this.connectWebSocket();

        console.log('✅ [UserDataStream] Ready');
    }

    async stop() {
        this.isRunning = false;

        if (this.keepAliveTimer) clearInterval(this.keepAliveTimer);
        if (this.rotateTimer) clearTimeout(this.rotateTimer);
        if (this.reconnectTimer) clearTimeout(this.reconnectTimer);

        if (this.ws) {
            try { this.ws.close(); } catch (e) {}
            this.ws = null;
        }

        await this.closeListenKey();
        try { await this.redis.quit(); } catch (e) {}
    }
}

if (require.main === module) {
    const stream = new BinanceUserDataStream();

    stream.on('orderFilled', order => {
        console.log(`✅ [ORDER FILLED] ${order.symbol} ${order.side} qty=${order.accumulatedFilledQty} avg=${order.averagePrice}`);
    });

    stream.on('marginCall', event => {
        console.error('🚨 [MARGIN CALL EVENT]', event);
    });

    stream.start().catch(err => {
        console.error('❌ [UserDataStream FATAL]', err.message);
        process.exit(1);
    });

    process.on('SIGINT', async () => {
        await stream.stop();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        await stream.stop();
        process.exit(0);
    });
}

module.exports = BinanceUserDataStream;
