const IExchange = require('./IExchange');
const ScoutTrade = require('../../models/ScoutTrade');
const exchangeRules = require('../binance/ExchangeRules');
const BinanceUserDataStream = require('../binance/BinanceUserDataStream');

const axios = require('axios');
const crypto = require('crypto');
const Redis = require('ioredis');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../../.env') });

// ============================================================
// LIVE EXCHANGE V19 - BINANCE USDⓈ-M FUTURES PRODUCTION ADAPTER
// ------------------------------------------------------------
// Vai trò:
// 1. Thực thi lệnh thật trên Binance USDⓈ-M Futures.
// 2. Tất cả order phải qua ExchangeRules trước khi gửi.
// 3. Không coi lệnh thành công cho đến khi UserDataStream xác nhận FILLED.
// 4. Đóng lệnh dùng reduceOnly để tránh mở ngược vị thế.
// 5. Có clientOrderId idempotent để tránh bắn trùng.
// 6. Có REST fallback query order nếu UserDataStream chậm.
// 7. Có chế độ LIVE_DRY_RUN để test rule/account mà không gửi order thật.
//
// Interface giữ gần giống PaperExchange:
// - openTrade(symbol, orderType, currentPrice, leverage, prob, type, features, meta)
// - closeTrade(symbol, closePrice, reason, exitFeatures)
// - updateTick(symbol, currentPrice, exitFeatures)
//
// CẢNH BÁO:
// - Chỉ dùng live thật khi đã chạy DRY_RUN đủ lâu.
// - Cần bật đúng mode tài khoản One-way/Hedge. Bản này mặc định One-way mode (positionSide=BOTH).
// ============================================================

const configPath = path.join(__dirname, '../../../system_config.json');
let config;
try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    config = { REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379' };
}

const REDIS_URL = config.REDIS_URL || process.env.REDIS_URL || 'redis://localhost:6379';
const LIVE = config.LIVE || {};

const BINANCE_FAPI_BASE = process.env.BINANCE_FAPI_BASE || 'https://fapi.binance.com';
const API_KEY = process.env.BINANCE_API_KEY || '';
const API_SECRET = process.env.BINANCE_API_SECRET || '';
const BOT_ID = process.env.BOT_ID || 'scout-main-live';
const MODEL_VERSION = process.env.MODEL_VERSION || 'Universal_Scout.onnx';

const LIVE_DRY_RUN = String(process.env.LIVE_DRY_RUN || LIVE.DRY_RUN || 'true') === 'true';
const LIVE_MODE = LIVE_DRY_RUN ? 'BACKTEST' : 'LIVE';
const DEFAULT_FIXED_MARGIN = Number(process.env.LIVE_FIXED_MARGIN || LIVE.FIXED_MARGIN || 2.0);
const DEFAULT_MARGIN_TYPE = process.env.LIVE_MARGIN_TYPE || LIVE.MARGIN_TYPE || 'ISOLATED'; // ISOLATED/CROSSED
const POSITION_MODE = process.env.LIVE_POSITION_MODE || LIVE.POSITION_MODE || 'ONE_WAY'; // ONE_WAY/HEDGE
const DEFAULT_FEE_TAKER = Number(process.env.LIVE_FEE_TAKER || LIVE.FEE_TAKER || 0.0004);
const REQUEST_TIMEOUT_MS = Number(process.env.LIVE_REQUEST_TIMEOUT_MS || 10000);
const DEFAULT_RECV_WINDOW = Number(process.env.BINANCE_RECV_WINDOW || 5000);
const ORDER_FILL_TIMEOUT_MS = Number(process.env.LIVE_ORDER_FILL_TIMEOUT_MS || 15000);
const ORDER_POLL_INTERVAL_MS = Number(process.env.LIVE_ORDER_POLL_INTERVAL_MS || 750);
const MAX_RETRIES = Number(process.env.LIVE_MAX_RETRIES || 2);
const CLIENT_ID_PREFIX = process.env.LIVE_CLIENT_ID_PREFIX || 'scout';

class LiveExchange extends IExchange {
    constructor() {
        super();

        this.pubClient = new Redis(REDIS_URL);
        this.dataClient = new Redis(REDIS_URL);
        this.pubClient.on('error', err => console.error('❌ [LIVE REDIS pubClient]', err.message));
        this.dataClient.on('error', err => console.error('❌ [LIVE REDIS dataClient]', err.message));

        this.userStream = new BinanceUserDataStream();
        this.activePositions = new Map();
        this.pendingOrders = new Map();
        this.symbolSetupCache = new Map();

        this.FIXED_MARGIN = DEFAULT_FIXED_MARGIN;
        this.FEE_TAKER = DEFAULT_FEE_TAKER;
        this.ready = false;

        this._wireUserStreamEvents();
    }

    // ========================================================
    // 1. SETUP / UTILS
    // ========================================================
    _requireKeys() {
        if (!API_KEY || !API_SECRET) {
            throw new Error('Missing BINANCE_API_KEY or BINANCE_API_SECRET');
        }
    }

    _safeNumber(value, fallback = 0) {
        const n = Number(value);
        return Number.isFinite(n) ? n : fallback;
    }

    _normalizeSymbol(symbol) {
        return String(symbol || '').toUpperCase().trim();
    }

    _normalizeType(type) {
        return type === 'SHORT' ? 'SHORT' : 'LONG';
    }

    _sideFromType(type) {
        return type === 'LONG' ? 'BUY' : 'SELL';
    }

    _exitSideFromType(type) {
        return type === 'LONG' ? 'SELL' : 'BUY';
    }

    _positionSideFromType(type) {
        if (POSITION_MODE === 'HEDGE') return type === 'LONG' ? 'LONG' : 'SHORT';
        return 'BOTH';
    }

    _sign(queryString) {
        return crypto.createHmac('sha256', API_SECRET).update(queryString).digest('hex');
    }

    _query(params = {}) {
        const payload = {
            ...params,
            recvWindow: params.recvWindow || DEFAULT_RECV_WINDOW,
            timestamp: Date.now()
        };
        const query = new URLSearchParams(payload).toString();
        const signature = this._sign(query);
        return `${query}&signature=${signature}`;
    }

    _clientOrderId(symbol, purpose) {
        const ts = Date.now().toString(36);
        const rand = Math.random().toString(36).slice(2, 8);
        // Binance newClientOrderId có giới hạn pattern và độ dài; giữ ngắn, không dùng dấu lạ.
        return `${CLIENT_ID_PREFIX}_${purpose}_${symbol}_${ts}_${rand}`.slice(0, 36);
    }

    _buildFeatureVector(features = {}) {
        if (!features || typeof features !== 'object') return undefined;
        return [
            Number(features.ob_imb_top20 || 0),
            Number(features.spread_close || 0),
            Number(features.bid_vol_1pct || 0),
            Number(features.ask_vol_1pct || 0),
            Number(features.max_buy_trade || 0),
            Number(features.max_sell_trade || 0),
            Number(features.liq_long_vol || 0),
            Number(features.liq_short_vol || 0),
            Number(features.funding_rate || 0),
            Number(features.taker_buy_ratio || 0),
            Number(features.body_size || 0),
            Number(features.wick_size || 0),
            Number(features.btc_relative_strength || 0)
        ];
    }

    async ensureReady() {
        if (this.ready) return;
        this._requireKeys();
        await exchangeRules.init();
        if (!LIVE_DRY_RUN) {
            await this.userStream.start();
        } else {
            // DRY_RUN vẫn cố sync snapshot nếu có thể, nhưng không bắt buộc chạy WS.
            try {
                await this.userStream.start();
            } catch (e) {
                console.warn('⚠️ [LIVE DRY RUN] UserDataStream không khởi động được, tiếp tục dry-run rule validation:', e.message);
            }
        }
        this.ready = true;
        console.log(`✅ [LiveExchange] Ready | mode=${LIVE_MODE} dryRun=${LIVE_DRY_RUN}`);
    }

    _wireUserStreamEvents() {
        this.userStream.on('orderUpdate', order => {
            if (order.clientOrderId) {
                this.pendingOrders.set(order.clientOrderId, order);
            }
        });

        this.userStream.on('orderFilled', order => {
            if (order.clientOrderId) {
                this.pendingOrders.set(order.clientOrderId, order);
            }
        });

        this.userStream.on('marginCall', event => {
            console.error('🚨 [LIVE] MARGIN CALL - trading should be paused immediately', event);
            this.pubClient.publish('system:commands', JSON.stringify({ action: 'PAUSE_TRADING', reason: 'MARGIN_CALL' })).catch(() => {});
        });
    }

    // ========================================================
    // 2. HTTP REQUESTS
    // ========================================================
    async _request(method, endpoint, params = {}, options = {}) {
        this._requireKeys();
        const query = this._query(params);
        const url = `${BINANCE_FAPI_BASE}${endpoint}?${query}`;

        let lastError;
        const maxRetries = options.maxRetries ?? MAX_RETRIES;

        for (let attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                const response = await axios({
                    method,
                    url,
                    headers: { 'X-MBX-APIKEY': API_KEY },
                    timeout: REQUEST_TIMEOUT_MS
                });
                return response.data;
            } catch (error) {
                lastError = error;
                const code = error.response?.data?.code;
                const msg = error.response?.data?.msg || error.message;

                // Idempotent harmless errors.
                if (endpoint === '/fapi/v1/marginType' && code === -4046) return { ok: true, ignored: true, code, msg };
                if (endpoint === '/fapi/v1/positionSide/dual' && code === -4059) return { ok: true, ignored: true, code, msg };

                if (attempt === maxRetries) {
                    await exchangeRules.refreshAfterReject(msg);
                    throw error;
                }

                await new Promise(resolve => setTimeout(resolve, 500 * (attempt + 1)));
            }
        }

        throw lastError;
    }

    async _post(endpoint, params = {}, options = {}) {
        return this._request('POST', endpoint, params, options);
    }

    async _get(endpoint, params = {}, options = {}) {
        return this._request('GET', endpoint, params, options);
    }

    async _delete(endpoint, params = {}, options = {}) {
        return this._request('DELETE', endpoint, params, options);
    }

    // ========================================================
    // 3. ACCOUNT / SYMBOL SETUP
    // ========================================================
    async setPositionModeIfNeeded() {
        // Position mode là cấp toàn account. Chỉ set nếu người dùng bật env rõ ràng.
        const shouldSet = String(process.env.LIVE_FORCE_SET_POSITION_MODE || 'false') === 'true';
        if (!shouldSet || LIVE_DRY_RUN) return;

        const dualSidePosition = POSITION_MODE === 'HEDGE' ? 'true' : 'false';
        await this._post('/fapi/v1/positionSide/dual', { dualSidePosition });
        console.log(`⚙️ [LIVE] Position mode set: ${POSITION_MODE}`);
    }

    async setupSymbol(symbol, leverage) {
        symbol = this._normalizeSymbol(symbol);
        const key = `${symbol}:${leverage}:${DEFAULT_MARGIN_TYPE}:${POSITION_MODE}`;
        if (this.symbolSetupCache.has(key)) return;

        if (LIVE_DRY_RUN) {
            this.symbolSetupCache.set(key, true);
            return;
        }

        await this.setPositionModeIfNeeded();

        try {
            await this._post('/fapi/v1/marginType', { symbol, marginType: DEFAULT_MARGIN_TYPE });
        } catch (e) {
            const code = e.response?.data?.code;
            if (code !== -4046) throw e; // No need to change margin type.
        }

        await this._post('/fapi/v1/leverage', { symbol, leverage });
        this.symbolSetupCache.set(key, true);
        console.log(`⚙️ [LIVE] Setup ${symbol}: margin=${DEFAULT_MARGIN_TYPE}, leverage=${leverage}x`);
    }

    // ========================================================
    // 4. ORDER SEND / WAIT FILL
    // ========================================================
    async placeOrder(params) {
        if (LIVE_DRY_RUN) {
            console.log('🧪 [LIVE DRY RUN] Would place order:', params);
            return {
                dryRun: true,
                clientOrderId: params.newClientOrderId,
                orderId: `DRY_${Date.now()}`,
                status: 'FILLED',
                avgPrice: params.currentPrice || params.price || params.stopPrice || 0,
                executedQty: params.quantity || 0
            };
        }

        return this._post('/fapi/v1/order', params);
    }

    async queryOrder(symbol, clientOrderId) {
        if (LIVE_DRY_RUN) return null;
        return this._get('/fapi/v1/order', { symbol, origClientOrderId: clientOrderId }, { maxRetries: 1 });
    }

    async waitForFill(symbol, clientOrderId, timeoutMs = ORDER_FILL_TIMEOUT_MS) {
        if (LIVE_DRY_RUN) {
            return this.pendingOrders.get(clientOrderId) || null;
        }

        const started = Date.now();

        while (Date.now() - started < timeoutMs) {
            const cached = this.pendingOrders.get(clientOrderId);
            if (cached && cached.orderStatus === 'FILLED') return cached;
            if (cached && ['REJECTED', 'CANCELED', 'EXPIRED'].includes(cached.orderStatus)) {
                throw new Error(`Order ${clientOrderId} ${cached.orderStatus}`);
            }

            // REST fallback mỗi vòng để không phụ thuộc hoàn toàn vào WS.
            try {
                const rest = await this.queryOrder(symbol, clientOrderId);
                if (rest && rest.status === 'FILLED') {
                    return {
                        symbol,
                        clientOrderId,
                        orderId: rest.orderId,
                        orderStatus: rest.status,
                        averagePrice: this._safeNumber(rest.avgPrice),
                        accumulatedFilledQty: this._safeNumber(rest.executedQty),
                        raw: rest
                    };
                }
                if (rest && ['REJECTED', 'CANCELED', 'EXPIRED'].includes(rest.status)) {
                    throw new Error(`Order ${clientOrderId} ${rest.status}`);
                }
            } catch (e) {
                // query order có thể chưa thấy ngay; tiếp tục chờ nếu chưa timeout.
            }

            await new Promise(resolve => setTimeout(resolve, ORDER_POLL_INTERVAL_MS));
        }

        throw new Error(`ORDER_FILL_TIMEOUT ${symbol} ${clientOrderId}`);
    }

    async cancelOrder(symbol, clientOrderId) {
        if (LIVE_DRY_RUN) return { dryRun: true };
        return this._delete('/fapi/v1/order', { symbol, origClientOrderId: clientOrderId });
    }

    // ========================================================
    // 5. OPEN TRADE
    // ========================================================
    async openTrade(symbol, orderType, currentPrice, leverage, prob, type = 'LONG', features = null, meta = {}) {
        await this.ensureReady();

        symbol = this._normalizeSymbol(symbol);
        type = this._normalizeType(type);
        currentPrice = this._safeNumber(currentPrice);
        leverage = Math.floor(this._safeNumber(leverage));
        prob = this._safeNumber(prob);

        if (!symbol || currentPrice <= 0 || leverage <= 0) return false;
        if (this.activePositions.has(symbol)) {
            console.warn(`⚠️ [LIVE] ${symbol} đã có active position trong LiveExchange`);
            return false;
        }

        const side = this._sideFromType(type);
        const positionSide = this._positionSideFromType(type);
        const referencePrice = side === 'BUY'
            ? this._safeNumber(features?.best_ask, currentPrice)
            : this._safeNumber(features?.best_bid, currentPrice);

        const targetNotional = this.FIXED_MARGIN * leverage;
        const rawQty = targetNotional / referencePrice;
        const quantity = exchangeRules.normalizeQuantity(symbol, rawQty, 'MARKET');

        const validation = exchangeRules.validateMarketEntry({
            symbol,
            side,
            quantity,
            currentPrice: referencePrice,
            leverage
        });

        if (!validation.ok) {
            console.warn(`⚠️ [LIVE] Reject open ${symbol}: ${validation.reason} ${validation.detail || ''}`);
            return false;
        }

        const normalizedQty = validation.normalized.quantity;
        await this.setupSymbol(symbol, leverage);

        const clientOrderId = this._clientOrderId(symbol, type === 'LONG' ? 'LE' : 'SE');
        const params = {
            symbol,
            side,
            type: 'MARKET',
            quantity: normalizedQty,
            newClientOrderId: clientOrderId,
            newOrderRespType: 'RESULT'
        };

        if (POSITION_MODE === 'HEDGE') params.positionSide = positionSide;

        // Không dùng reduceOnly cho entry.
        const openTs = Date.now();
        const featureVectorAtEntry = this._buildFeatureVector(features);

        try {
            const sent = await this.placeOrder({ ...params, currentPrice: referencePrice });
            const fill = LIVE_DRY_RUN
                ? {
                    clientOrderId,
                    orderId: sent.orderId,
                    orderStatus: 'FILLED',
                    averagePrice: referencePrice,
                    accumulatedFilledQty: normalizedQty
                }
                : await this.waitForFill(symbol, clientOrderId);

            const entryPrice = this._safeNumber(fill.averagePrice || sent.avgPrice || referencePrice, referencePrice);
            const executedQty = this._safeNumber(fill.accumulatedFilledQty || sent.executedQty || normalizedQty, normalizedQty);
            const notionalValue = entryPrice * executedQty;
            const entryFee = notionalValue * this.FEE_TAKER;
            const maintenanceMarginRate = exchangeRules.getMaintenanceMarginRate(symbol, notionalValue);
            const liquidationPrice = this._estimateLiquidationPrice(type, entryPrice, executedQty, this.FIXED_MARGIN, notionalValue, maintenanceMarginRate);

            const position = {
                symbol,
                type,
                side,
                positionSide,
                entryPrice,
                size: executedQty,
                leverage,
                margin: this.FIXED_MARGIN,
                notionalValue,
                entryFee,
                liquidationPrice,
                highestPrice: entryPrice,
                lowestPrice: entryPrice,
                openTime: openTs,
                prob,
                probLong: meta.probLong,
                probShort: meta.probShort,
                macroScore: meta.macroScore,
                features,
                featureVectorAtEntry,
                entryOrderId: String(fill.orderId || sent.orderId || ''),
                entryClientOrderId: clientOrderId
            };

            this.activePositions.set(symbol, position);

            const dbRecord = await ScoutTrade.create({
                symbol,
                type,
                mode: LIVE_MODE,
                orderType: 'MARKET',
                status: 'OPEN',
                leverage,
                margin: this.FIXED_MARGIN,
                size: executedQty,
                notionalValue,
                liquidationPrice,
                entryPrice,
                executedEntryPrice: entryPrice,
                entryFee,
                openTime: new Date(openTs),
                openTs,
                prob,
                probLong: meta.probLong,
                probShort: meta.probShort,
                macroScore: meta.macroScore,
                entrySignal: meta.entrySignal || 'AI_PROB_THRESHOLD',
                modelVersion: meta.modelVersion || MODEL_VERSION,
                features,
                featuresAtEntry: features,
                featureVectorAtEntry,
                highestPrice: entryPrice,
                lowestPrice: entryPrice,
                entryOrderId: position.entryOrderId,
                botId: BOT_ID,
                note: JSON.stringify({ liveVersion: 'V19', dryRun: LIVE_DRY_RUN, sent, fill })
            });

            position.dbId = dbRecord._id;

            console.log(`✅ [LIVE${LIVE_DRY_RUN ? ' DRY' : ''}] Open ${type} ${symbol} qty=${executedQty} entry=${entryPrice}`);
            this._publishDashboardUpdate();
            return true;
        } catch (error) {
            console.error(`❌ [LIVE] openTrade failed ${symbol}:`, error.response?.data || error.message);
            await exchangeRules.refreshAfterReject(error.response?.data?.msg || error.message);
            return false;
        }
    }

    _estimateLiquidationPrice(type, entryPrice, size, margin, notionalValue, maintenanceMarginRate) {
        if (size <= 0 || entryPrice <= 0) return 0;
        const maintMargin = notionalValue * maintenanceMarginRate;
        const price = type === 'LONG'
            ? entryPrice - (margin - maintMargin) / size
            : entryPrice + (margin - maintMargin) / size;
        return price > 0 ? price : 0;
    }

    // ========================================================
    // 6. CLOSE TRADE
    // ========================================================
    async closeTrade(symbol, closePrice, reason, exitFeatures = null) {
        await this.ensureReady();

        symbol = this._normalizeSymbol(symbol);
        closePrice = this._safeNumber(closePrice);
        reason = reason || 'UNKNOWN';

        const position = this.activePositions.get(symbol);
        if (!position) {
            console.warn(`⚠️ [LIVE] Không có active position để đóng ${symbol}`);
            return null;
        }

        const side = this._exitSideFromType(position.type);
        const positionSide = this._positionSideFromType(position.type);
        const quantity = exchangeRules.normalizeQuantity(symbol, position.size, 'MARKET');

        const params = {
            symbol,
            side,
            type: 'MARKET',
            quantity,
            reduceOnly: 'true',
            newClientOrderId: this._clientOrderId(symbol, position.type === 'LONG' ? 'LX' : 'SX'),
            newOrderRespType: 'RESULT'
        };

        // Trong Hedge Mode, Binance không cho reduceOnly, dùng positionSide thay thế.
        if (POSITION_MODE === 'HEDGE') {
            delete params.reduceOnly;
            params.positionSide = positionSide;
        }

        const closeTs = Date.now();
        const featureVectorAtExit = this._buildFeatureVector(exitFeatures);

        try {
            const sent = await this.placeOrder({ ...params, currentPrice: closePrice });
            const fill = LIVE_DRY_RUN
                ? {
                    clientOrderId: params.newClientOrderId,
                    orderId: sent.orderId,
                    orderStatus: 'FILLED',
                    averagePrice: closePrice,
                    accumulatedFilledQty: quantity
                }
                : await this.waitForFill(symbol, params.newClientOrderId);

            const executedClosePrice = this._safeNumber(fill.averagePrice || sent.avgPrice || closePrice, closePrice);
            const exitQty = this._safeNumber(fill.accumulatedFilledQty || sent.executedQty || quantity, quantity);
            const exitNotional = exitQty * executedClosePrice;
            const exitFee = exitNotional * this.FEE_TAKER;
            const grossPnl = position.type === 'LONG'
                ? (executedClosePrice - position.entryPrice) * exitQty
                : (position.entryPrice - executedClosePrice) * exitQty;
            const netPnl = grossPnl - position.entryFee - exitFee;
            const roiPercent = position.margin > 0 ? (netPnl / position.margin) * 100 : 0;
            const durationMs = Math.max(0, closeTs - position.openTime);
            const { mfePercent, maePercent } = this._calculateMaeMfe(position);

            const finalStatus = reason === 'LIQUIDATED' ? 'LIQUIDATED' : 'CLOSED';
            const updatePayload = {
                status: finalStatus,
                closePrice: executedClosePrice,
                executedClosePrice,
                exitFee,
                closeTime: new Date(closeTs),
                closeTs,
                durationMs,
                reason,
                pnl: netPnl,
                roi: roiPercent,
                grossPnl,
                netPnl,
                mae: maePercent,
                mfe: mfePercent,
                highestPrice: position.highestPrice,
                lowestPrice: position.lowestPrice,
                features: position.features,
                featuresAtEntry: position.features,
                featuresAtExit: exitFeatures,
                featureVectorAtEntry: position.featureVectorAtEntry,
                featureVectorAtExit,
                closeOrderId: String(fill.orderId || sent.orderId || ''),
                note: JSON.stringify({ liveVersion: 'V19', dryRun: LIVE_DRY_RUN, closeSent: sent, closeFill: fill })
            };

            if (position.dbId) {
                await ScoutTrade.findByIdAndUpdate(position.dbId, updatePayload, { new: false });
            }

            this.activePositions.delete(symbol);
            this._publishDashboardUpdate();

            console.log(`✅ [LIVE${LIVE_DRY_RUN ? ' DRY' : ''}] Close ${position.type} ${symbol} price=${executedClosePrice} net=${netPnl}`);

            return {
                symbol,
                type: position.type,
                closed: true,
                reason,
                closePrice: executedClosePrice,
                grossPnL: grossPnl,
                netPnL: netPnl,
                roiPercent,
                maePercent,
                mfePercent,
                durationMs
            };
        } catch (error) {
            console.error(`❌ [LIVE] closeTrade failed ${symbol}:`, error.response?.data || error.message);
            await exchangeRules.refreshAfterReject(error.response?.data?.msg || error.message);
            return null;
        }
    }

    _calculateMaeMfe(position) {
        let mfePercent = 0;
        let maePercent = 0;
        if (position.type === 'LONG') {
            mfePercent = ((position.highestPrice - position.entryPrice) / position.entryPrice) * 100;
            maePercent = ((position.lowestPrice - position.entryPrice) / position.entryPrice) * 100;
        } else {
            mfePercent = ((position.entryPrice - position.lowestPrice) / position.entryPrice) * 100;
            maePercent = ((position.entryPrice - position.highestPrice) / position.entryPrice) * 100;
        }
        return { mfePercent, maePercent };
    }

    // ========================================================
    // 7. UPDATE TICK / RECONCILE
    // ========================================================
    async updateTick(symbol, currentPrice, exitFeatures = null) {
        symbol = this._normalizeSymbol(symbol);
        currentPrice = this._safeNumber(currentPrice);
        const position = this.activePositions.get(symbol);
        if (!position || currentPrice <= 0) return null;

        const markPrice = this._safeNumber(exitFeatures?.mark_price, currentPrice);
        if (markPrice > position.highestPrice) position.highestPrice = markPrice;
        if (markPrice < position.lowestPrice) position.lowestPrice = markPrice;

        // Reconcile với account state nếu user stream đang chạy.
        const livePos = this.userStream.getPosition(symbol);
        if (!LIVE_DRY_RUN && livePos && Math.abs(this._safeNumber(livePos.positionAmt)) === 0) {
            // Position đã biến mất ngoài ý muốn: coi như exchange closed.
            this.activePositions.delete(symbol);
            return {
                closed: true,
                reason: 'EXCHANGE_POSITION_ZERO',
                symbol,
                closePrice: currentPrice,
                result: null
            };
        }

        return {
            closed: false,
            symbol,
            highestPrice: position.highestPrice,
            lowestPrice: position.lowestPrice
        };
    }

    // ========================================================
    // 8. OPTIONAL PROTECTIVE ORDERS
    // ========================================================
    async placeReduceOnlyStopMarket({ symbol, type, stopPrice }) {
        await this.ensureReady();
        symbol = this._normalizeSymbol(symbol);
        type = this._normalizeType(type);

        const side = this._exitSideFromType(type);
        const positionSide = this._positionSideFromType(type);
        const normalizedStop = exchangeRules.normalizeStopPrice(symbol, stopPrice, side, 'STOP_LOSS');

        const validation = exchangeRules.validateClosePositionStop({
            symbol,
            side,
            stopPrice: normalizedStop,
            orderType: 'STOP_MARKET'
        });

        if (!validation.ok) {
            console.warn(`⚠️ [LIVE] Reject SL ${symbol}: ${validation.reason} ${validation.detail || ''}`);
            return null;
        }

        const params = {
            symbol,
            side,
            type: 'STOP_MARKET',
            stopPrice: normalizedStop,
            closePosition: 'true',
            workingType: 'MARK_PRICE',
            priceProtect: 'true',
            newClientOrderId: this._clientOrderId(symbol, 'SL')
        };

        if (POSITION_MODE === 'HEDGE') params.positionSide = positionSide;

        return this.placeOrder(params);
    }

    // ========================================================
    // 9. GETTERS / DASHBOARD
    // ========================================================
    getActivePositions() {
        return new Map(this.activePositions);
    }

    getPosition(symbol) {
        return this.activePositions.get(this._normalizeSymbol(symbol)) || null;
    }

    hasActivePosition(symbol) {
        return this.activePositions.has(this._normalizeSymbol(symbol));
    }

    _publishDashboardUpdate() {
        try {
            this.pubClient.publish('dashboard:trades', JSON.stringify({ action: 'UPDATE' }));
        } catch (e) {}
    }
}

module.exports = new LiveExchange();
