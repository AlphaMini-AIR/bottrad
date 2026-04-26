const axios = require('axios');
const crypto = require('crypto');
require('dotenv').config();

// ============================================================
// EXCHANGE RULES V1 - BINANCE USDⓈ-M FUTURES RULE VALIDATOR
// ------------------------------------------------------------
// Vai trò:
// 1. Là lớp luật sàn trung tâm cho PaperExchange và LiveExchange.
// 2. Tự sync exchangeInfo từ Binance Futures, không hard-code tick/step/minNotional.
// 3. Tự sync leverageBracket nếu có API key/secret.
// 4. Chuẩn hóa price theo tickSize.
// 5. Chuẩn hóa quantity theo stepSize.
// 6. Validate lệnh trước khi Paper/Live gửi đi.
//
// Nguyên tắc live-ready:
// - OrderManager không tự đoán rule.
// - PaperExchange không tự đoán rule.
// - LiveExchange không tự đoán rule.
// - Mọi lệnh đều phải đi qua ExchangeRules trước.
// ============================================================

const BINANCE_FAPI_BASE = process.env.BINANCE_FAPI_BASE || 'https://fapi.binance.com';
const API_KEY = process.env.BINANCE_API_KEY || '';
const API_SECRET = process.env.BINANCE_API_SECRET || '';

const EXCHANGE_INFO_REFRESH_MS = Number(process.env.EXCHANGE_INFO_REFRESH_MS || 15 * 60 * 1000);
const LEVERAGE_BRACKET_REFRESH_MS = Number(process.env.LEVERAGE_BRACKET_REFRESH_MS || 30 * 60 * 1000);
const REQUEST_TIMEOUT_MS = Number(process.env.BINANCE_RULES_TIMEOUT_MS || 10000);
const DEFAULT_RECV_WINDOW = Number(process.env.BINANCE_RECV_WINDOW || 5000);

class ExchangeRules {
    constructor() {
        this.symbolRules = new Map();
        this.leverageBrackets = new Map();

        this.exchangeInfoUpdatedAt = 0;
        this.leverageBracketUpdatedAt = 0;

        this.isSyncingExchangeInfo = false;
        this.isSyncingLeverageBrackets = false;

        this.ready = false;
    }

    // ========================================================
    // 1. HTTP HELPERS
    // ========================================================
    _sign(queryString) {
        return crypto
            .createHmac('sha256', API_SECRET)
            .update(queryString)
            .digest('hex');
    }

    async _publicGet(endpoint, params = {}) {
        const response = await axios.get(`${BINANCE_FAPI_BASE}${endpoint}`, {
            params,
            timeout: REQUEST_TIMEOUT_MS
        });
        return response.data;
    }

    async _signedGet(endpoint, params = {}) {
        if (!API_KEY || !API_SECRET) {
            throw new Error('Missing BINANCE_API_KEY or BINANCE_API_SECRET');
        }

        const payload = {
            ...params,
            timestamp: Date.now(),
            recvWindow: DEFAULT_RECV_WINDOW
        };

        const queryString = new URLSearchParams(payload).toString();
        const signature = this._sign(queryString);
        const url = `${BINANCE_FAPI_BASE}${endpoint}?${queryString}&signature=${signature}`;

        const response = await axios.get(url, {
            headers: { 'X-MBX-APIKEY': API_KEY },
            timeout: REQUEST_TIMEOUT_MS
        });

        return response.data;
    }

    // ========================================================
    // 2. DECIMAL / ROUNDING HELPERS
    // --------------------------------------------------------
    // Dùng integer scaling để hạn chế lỗi floating point.
    // ========================================================
    _decimalPlaces(value) {
        const text = String(value);
        if (text.includes('e-')) {
            const [, exp] = text.split('e-');
            return Number(exp);
        }
        const dot = text.indexOf('.');
        if (dot === -1) return 0;
        return text.length - dot - 1;
    }

    _floorToStep(value, step) {
        value = Number(value);
        step = Number(step);
        if (!Number.isFinite(value) || !Number.isFinite(step) || step <= 0) return 0;

        const precision = this._decimalPlaces(step);
        const factor = 10 ** precision;
        const valueInt = Math.floor(value * factor + 1e-9);
        const stepInt = Math.round(step * factor);
        const resultInt = Math.floor(valueInt / stepInt) * stepInt;
        return Number((resultInt / factor).toFixed(precision));
    }

    _ceilToStep(value, step) {
        value = Number(value);
        step = Number(step);
        if (!Number.isFinite(value) || !Number.isFinite(step) || step <= 0) return 0;

        const precision = this._decimalPlaces(step);
        const factor = 10 ** precision;
        const valueInt = Math.ceil(value * factor - 1e-9);
        const stepInt = Math.round(step * factor);
        const resultInt = Math.ceil(valueInt / stepInt) * stepInt;
        return Number((resultInt / factor).toFixed(precision));
    }

    _roundToStep(value, step) {
        value = Number(value);
        step = Number(step);
        if (!Number.isFinite(value) || !Number.isFinite(step) || step <= 0) return 0;

        const precision = this._decimalPlaces(step);
        const factor = 10 ** precision;
        const valueInt = Math.round(value * factor);
        const stepInt = Math.round(step * factor);
        const resultInt = Math.round(valueInt / stepInt) * stepInt;
        return Number((resultInt / factor).toFixed(precision));
    }

    _safeNumber(value, fallback = 0) {
        const n = Number(value);
        return Number.isFinite(n) ? n : fallback;
    }

    _normalizeSymbol(symbol) {
        return String(symbol || '').toUpperCase().trim();
    }

    // ========================================================
    // 3. PARSE EXCHANGE INFO
    // ========================================================
    _parseFilters(filters = []) {
        const parsed = {
            tickSize: 0,
            minPrice: 0,
            maxPrice: 0,
            stepSize: 0,
            minQty: 0,
            maxQty: 0,
            marketStepSize: 0,
            marketMinQty: 0,
            marketMaxQty: 0,
            minNotional: 0,
            maxNumOrders: 0,
            maxNumAlgoOrders: 0,
            multiplierUp: 0,
            multiplierDown: 0,
            multiplierDecimal: 0
        };

        for (const filter of filters) {
            switch (filter.filterType) {
                case 'PRICE_FILTER':
                    parsed.tickSize = this._safeNumber(filter.tickSize);
                    parsed.minPrice = this._safeNumber(filter.minPrice);
                    parsed.maxPrice = this._safeNumber(filter.maxPrice);
                    break;

                case 'LOT_SIZE':
                    parsed.stepSize = this._safeNumber(filter.stepSize);
                    parsed.minQty = this._safeNumber(filter.minQty);
                    parsed.maxQty = this._safeNumber(filter.maxQty);
                    break;

                case 'MARKET_LOT_SIZE':
                    parsed.marketStepSize = this._safeNumber(filter.stepSize);
                    parsed.marketMinQty = this._safeNumber(filter.minQty);
                    parsed.marketMaxQty = this._safeNumber(filter.maxQty);
                    break;

                case 'MIN_NOTIONAL':
                    parsed.minNotional = this._safeNumber(filter.notional);
                    break;

                case 'MAX_NUM_ORDERS':
                    parsed.maxNumOrders = this._safeNumber(filter.limit);
                    break;

                case 'MAX_NUM_ALGO_ORDERS':
                    parsed.maxNumAlgoOrders = this._safeNumber(filter.limit);
                    break;

                case 'PERCENT_PRICE':
                    parsed.multiplierUp = this._safeNumber(filter.multiplierUp);
                    parsed.multiplierDown = this._safeNumber(filter.multiplierDown);
                    parsed.multiplierDecimal = this._safeNumber(filter.multiplierDecimal);
                    break;

                default:
                    break;
            }
        }

        return parsed;
    }

    _parseSymbolInfo(item) {
        const filters = this._parseFilters(item.filters || []);

        return {
            symbol: item.symbol,
            pair: item.pair,
            contractType: item.contractType,
            status: item.status,
            baseAsset: item.baseAsset,
            quoteAsset: item.quoteAsset,
            marginAsset: item.marginAsset,
            pricePrecision: this._safeNumber(item.pricePrecision),
            quantityPrecision: this._safeNumber(item.quantityPrecision),
            baseAssetPrecision: this._safeNumber(item.baseAssetPrecision),
            quotePrecision: this._safeNumber(item.quotePrecision),
            underlyingType: item.underlyingType,
            underlyingSubType: item.underlyingSubType || [],
            triggerProtect: this._safeNumber(item.triggerProtect),

            // Filters normalized.
            tickSize: filters.tickSize,
            minPrice: filters.minPrice,
            maxPrice: filters.maxPrice,
            stepSize: filters.stepSize,
            minQty: filters.minQty,
            maxQty: filters.maxQty,
            marketStepSize: filters.marketStepSize || filters.stepSize,
            marketMinQty: filters.marketMinQty || filters.minQty,
            marketMaxQty: filters.marketMaxQty || filters.maxQty,
            minNotional: filters.minNotional,
            maxNumOrders: filters.maxNumOrders,
            maxNumAlgoOrders: filters.maxNumAlgoOrders,
            multiplierUp: filters.multiplierUp,
            multiplierDown: filters.multiplierDown,
            multiplierDecimal: filters.multiplierDecimal,

            // Leverage bracket sẽ được merge sau nếu có API key.
            maxLeverage: 20,
            maintenanceMarginRate: 0.005,
            leverageBrackets: []
        };
    }

    // ========================================================
    // 4. SYNC EXCHANGE INFO / LEVERAGE BRACKETS
    // ========================================================
    async syncExchangeInfo(force = false) {
        const now = Date.now();
        if (!force && this.symbolRules.size > 0 && now - this.exchangeInfoUpdatedAt < EXCHANGE_INFO_REFRESH_MS) {
            return;
        }

        if (this.isSyncingExchangeInfo) return;
        this.isSyncingExchangeInfo = true;

        try {
            const data = await this._publicGet('/fapi/v1/exchangeInfo');
            const nextRules = new Map();

            for (const item of data.symbols || []) {
                if (item.contractType !== 'PERPETUAL') continue;
                if (item.quoteAsset !== 'USDT') continue;

                const rule = this._parseSymbolInfo(item);
                const existingBracket = this.leverageBrackets.get(rule.symbol);
                if (existingBracket) this._mergeBracket(rule, existingBracket);

                nextRules.set(rule.symbol, rule);
            }

            this.symbolRules = nextRules;
            this.exchangeInfoUpdatedAt = now;
            this.ready = true;

            console.log(`✅ [ExchangeRules] Synced exchangeInfo: ${this.symbolRules.size} USDT-M perpetual symbols`);
        } catch (error) {
            console.error('❌ [ExchangeRules] syncExchangeInfo error:', error.response?.data || error.message);
            if (this.symbolRules.size === 0) throw error;
        } finally {
            this.isSyncingExchangeInfo = false;
        }
    }

    async syncLeverageBrackets(force = false) {
        const now = Date.now();
        if (!API_KEY || !API_SECRET) {
            console.warn('⚠️ [ExchangeRules] Skip leverageBracket sync: missing API key/secret');
            return;
        }

        if (!force && this.leverageBrackets.size > 0 && now - this.leverageBracketUpdatedAt < LEVERAGE_BRACKET_REFRESH_MS) {
            return;
        }

        if (this.isSyncingLeverageBrackets) return;
        this.isSyncingLeverageBrackets = true;

        try {
            const data = await this._signedGet('/fapi/v1/leverageBracket');
            const next = new Map();

            for (const item of data || []) {
                const symbol = this._normalizeSymbol(item.symbol);
                if (!symbol) continue;
                next.set(symbol, item.brackets || []);
            }

            this.leverageBrackets = next;
            this.leverageBracketUpdatedAt = now;

            for (const [symbol, brackets] of next.entries()) {
                const rule = this.symbolRules.get(symbol);
                if (rule) this._mergeBracket(rule, brackets);
            }

            console.log(`✅ [ExchangeRules] Synced leverage brackets: ${this.leverageBrackets.size} symbols`);
        } catch (error) {
            console.error('❌ [ExchangeRules] syncLeverageBrackets error:', error.response?.data || error.message);
        } finally {
            this.isSyncingLeverageBrackets = false;
        }
    }

    _mergeBracket(rule, brackets = []) {
        if (!rule || !Array.isArray(brackets) || brackets.length === 0) return;

        rule.leverageBrackets = brackets.map(b => ({
            bracket: this._safeNumber(b.bracket),
            initialLeverage: this._safeNumber(b.initialLeverage),
            notionalCap: this._safeNumber(b.notionalCap),
            notionalFloor: this._safeNumber(b.notionalFloor),
            maintMarginRatio: this._safeNumber(b.maintMarginRatio),
            cum: this._safeNumber(b.cum)
        }));

        // Bracket đầu thường là mức leverage cao nhất cho notional thấp nhất.
        const first = rule.leverageBrackets[0];
        if (first) {
            rule.maxLeverage = first.initialLeverage || rule.maxLeverage;
            rule.maintenanceMarginRate = first.maintMarginRatio || rule.maintenanceMarginRate;
        }
    }

    async init() {
        await this.syncExchangeInfo(true);
        await this.syncLeverageBrackets(false);
        this.startAutoRefresh();
        return this;
    }

    startAutoRefresh() {
        if (this._autoRefreshStarted) return;
        this._autoRefreshStarted = true;

        setInterval(() => {
            this.syncExchangeInfo(false).catch(() => {});
        }, EXCHANGE_INFO_REFRESH_MS);

        setInterval(() => {
            this.syncLeverageBrackets(false).catch(() => {});
        }, LEVERAGE_BRACKET_REFRESH_MS);
    }

    async ensureReady() {
        if (!this.ready || this.symbolRules.size === 0) {
            await this.syncExchangeInfo(true);
        }
    }

    // ========================================================
    // 5. GETTERS
    // ========================================================
    getSymbolRules(symbol) {
        symbol = this._normalizeSymbol(symbol);
        return this.symbolRules.get(symbol) || null;
    }

    isTradable(symbol) {
        const rule = this.getSymbolRules(symbol);
        return Boolean(rule && rule.status === 'TRADING' && rule.contractType === 'PERPETUAL' && rule.quoteAsset === 'USDT');
    }

    getAllTradableSymbols() {
        return [...this.symbolRules.values()]
            .filter(rule => rule.status === 'TRADING' && rule.contractType === 'PERPETUAL' && rule.quoteAsset === 'USDT')
            .map(rule => rule.symbol);
    }

    getBracketForNotional(symbol, notionalValue) {
        const rule = this.getSymbolRules(symbol);
        if (!rule || !Array.isArray(rule.leverageBrackets) || rule.leverageBrackets.length === 0) {
            return null;
        }

        const notional = this._safeNumber(notionalValue);
        return rule.leverageBrackets.find(b => notional >= b.notionalFloor && notional < b.notionalCap) || rule.leverageBrackets[rule.leverageBrackets.length - 1];
    }

    getMaxLeverage(symbol, notionalValue = 0) {
        const bracket = this.getBracketForNotional(symbol, notionalValue);
        if (bracket && bracket.initialLeverage) return bracket.initialLeverage;

        const rule = this.getSymbolRules(symbol);
        return rule?.maxLeverage || 20;
    }

    getMaintenanceMarginRate(symbol, notionalValue = 0) {
        const bracket = this.getBracketForNotional(symbol, notionalValue);
        if (bracket && bracket.maintMarginRatio) return bracket.maintMarginRatio;

        const rule = this.getSymbolRules(symbol);
        return rule?.maintenanceMarginRate || 0.005;
    }

    // ========================================================
    // 6. NORMALIZATION
    // ========================================================
    normalizePrice(symbol, price, mode = 'nearest') {
        const rule = this.getSymbolRules(symbol);
        if (!rule) return null;

        const tickSize = rule.tickSize;
        if (!tickSize || tickSize <= 0) return Number(price);

        if (mode === 'floor') return this._floorToStep(price, tickSize);
        if (mode === 'ceil') return this._ceilToStep(price, tickSize);
        return this._roundToStep(price, tickSize);
    }

    normalizeQuantity(symbol, quantity, orderType = 'MARKET') {
        const rule = this.getSymbolRules(symbol);
        if (!rule) return null;

        const stepSize = orderType === 'MARKET' ? rule.marketStepSize : rule.stepSize;
        if (!stepSize || stepSize <= 0) return Number(quantity);

        // Quantity nên floor để không vượt quá size tính toán/risk dự kiến.
        return this._floorToStep(quantity, stepSize);
    }

    normalizeStopPrice(symbol, stopPrice, side, intent = 'STOP_LOSS') {
        // Đây là normalize thận trọng để giảm reject do tickSize.
        // LONG stop-loss đóng bằng SELL: floor stopPrice.
        // SHORT stop-loss đóng bằng BUY: ceil stopPrice.
        const upperSide = String(side || '').toUpperCase();

        if (intent === 'STOP_LOSS') {
            if (upperSide === 'SELL') return this.normalizePrice(symbol, stopPrice, 'floor');
            if (upperSide === 'BUY') return this.normalizePrice(symbol, stopPrice, 'ceil');
        }

        if (intent === 'TAKE_PROFIT') {
            if (upperSide === 'SELL') return this.normalizePrice(symbol, stopPrice, 'floor');
            if (upperSide === 'BUY') return this.normalizePrice(symbol, stopPrice, 'ceil');
        }

        return this.normalizePrice(symbol, stopPrice, 'nearest');
    }

    // ========================================================
    // 7. VALIDATION
    // ========================================================
    validateOrder(params = {}) {
        const symbol = this._normalizeSymbol(params.symbol);
        const side = String(params.side || '').toUpperCase();
        const type = String(params.type || 'MARKET').toUpperCase();
        const leverage = this._safeNumber(params.leverage, 1);
        const currentPrice = this._safeNumber(params.currentPrice || params.price || params.stopPrice, 0);
        const reduceOnly = Boolean(params.reduceOnly);
        const closePosition = params.closePosition === true || params.closePosition === 'true';

        const rule = this.getSymbolRules(symbol);
        if (!rule) return this._reject('NO_SYMBOL_RULES', `Không có rules cho ${symbol}`);
        if (!this.isTradable(symbol)) return this._reject('SYMBOL_NOT_TRADING', `${symbol} status=${rule.status}`);
        if (!['BUY', 'SELL'].includes(side)) return this._reject('BAD_SIDE', `side=${side}`);

        const isMarket = type === 'MARKET';
        const isStopMarket = type === 'STOP_MARKET' || type === 'TAKE_PROFIT_MARKET';
        const isLimitLike = type.includes('LIMIT') || type === 'LIMIT';

        let quantity = this._safeNumber(params.quantity, 0);
        let price = this._safeNumber(params.price, 0);
        let stopPrice = this._safeNumber(params.stopPrice, 0);

        if (!closePosition) {
            quantity = this.normalizeQuantity(symbol, quantity, isMarket ? 'MARKET' : type);
            if (!quantity || quantity <= 0) return this._reject('BAD_QUANTITY', `quantity=${params.quantity}`);
        }

        if (isLimitLike) {
            price = this.normalizePrice(symbol, price, 'nearest');
            if (!price || price <= 0) return this._reject('BAD_PRICE', `price=${params.price}`);
        }

        if (isStopMarket) {
            stopPrice = this.normalizeStopPrice(symbol, stopPrice, side, type === 'STOP_MARKET' ? 'STOP_LOSS' : 'TAKE_PROFIT');
            if (!stopPrice || stopPrice <= 0) return this._reject('BAD_STOP_PRICE', `stopPrice=${params.stopPrice}`);
        }

        // Notional check chỉ áp dụng khi có quantity.
        // closePosition=true không truyền quantity nên không kiểm minNotional ở đây.
        const referencePrice = price || currentPrice || stopPrice;
        const notional = closePosition ? 0 : quantity * referencePrice;

        if (!closePosition && referencePrice <= 0) {
            return this._reject('BAD_REFERENCE_PRICE', `currentPrice/price invalid`);
        }

        if (!closePosition && notional < rule.minNotional) {
            return this._reject('MIN_NOTIONAL', `notional ${notional} < minNotional ${rule.minNotional}`);
        }

        const minQty = isMarket ? rule.marketMinQty : rule.minQty;
        const maxQty = isMarket ? rule.marketMaxQty : rule.maxQty;

        if (!closePosition && minQty && quantity < minQty) {
            return this._reject('MIN_QTY', `quantity ${quantity} < minQty ${minQty}`);
        }

        if (!closePosition && maxQty && quantity > maxQty) {
            return this._reject('MAX_QTY', `quantity ${quantity} > maxQty ${maxQty}`);
        }

        const maxLev = this.getMaxLeverage(symbol, notional);
        if (leverage > maxLev) {
            return this._reject('MAX_LEVERAGE', `leverage ${leverage} > maxLeverage ${maxLev}`);
        }

        return {
            ok: true,
            reason: 'OK',
            rule,
            normalized: {
                symbol,
                side,
                type,
                quantity,
                price,
                stopPrice,
                leverage,
                reduceOnly,
                closePosition,
                notional,
                minNotional: rule.minNotional,
                maxLeverage: maxLev,
                tickSize: rule.tickSize,
                stepSize: isMarket ? rule.marketStepSize : rule.stepSize
            }
        };
    }

    validateMarketEntry({ symbol, side, quantity, currentPrice, leverage }) {
        return this.validateOrder({
            symbol,
            side,
            type: 'MARKET',
            quantity,
            currentPrice,
            leverage
        });
    }

    validateClosePositionStop({ symbol, side, stopPrice, orderType = 'STOP_MARKET' }) {
        return this.validateOrder({
            symbol,
            side,
            type: orderType,
            stopPrice,
            closePosition: true,
            leverage: 1
        });
    }

    _reject(reason, detail = '') {
        return {
            ok: false,
            reason,
            detail
        };
    }

    // ========================================================
    // 8. FORCE REFRESH KHI BINANCE REJECT DO FILTER
    // ========================================================
    async refreshAfterReject(errorMessage = '') {
        const msg = String(errorMessage || '').toLowerCase();
        const shouldRefresh =
            msg.includes('precision') ||
            msg.includes('filter') ||
            msg.includes('tick') ||
            msg.includes('lot_size') ||
            msg.includes('min_notional') ||
            msg.includes('notional') ||
            msg.includes('leverage');

        if (shouldRefresh) {
            await this.syncExchangeInfo(true);
            await this.syncLeverageBrackets(true);
        }
    }
}

module.exports = new ExchangeRules();
