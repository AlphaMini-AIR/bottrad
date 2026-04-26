const IExchange = require('./IExchange');
const ScoutTrade = require('../../models/ScoutTrade');
const Redis = require('ioredis');
const fs = require('fs');
const path = require('path');
const exchangeRules = require('../binance/ExchangeRules');

// ============================================================
// PAPER EXCHANGE V19 - LIVE PARITY PAPER ENGINE
// ------------------------------------------------------------
// Mục tiêu:
// 1. PaperExchange phải mô phỏng gần LiveExchange nhất có thể.
// 2. Mọi lệnh phải qua ExchangeRules: tickSize, stepSize, minNotional, leverage bracket.
// 3. Không dùng last_price để khớp lệnh một cách ảo tưởng.
// 4. Entry/exit MARKET được mô phỏng bằng bid/ask + slippage.
// 5. Liquidation check ưu tiên mark_price nếu FeedHandler cung cấp.
// 6. Wallet accounting đúng: mở lệnh khóa margin + trừ entry fee, đóng lệnh hoàn margin + PnL - exit fee - funding.
// 7. Lưu đầy đủ ScoutTrade_Model_V18/V19 fields để training và audit live-ready.
//
// Interface giữ tương thích OrderManager_V18:
// - openTrade(symbol, orderType, currentPrice, leverage, prob, type, features, meta)
// - closeTrade(symbol, closePrice, reason, exitFeatures)
// - updateTick(symbol, currentPrice, exitFeatures)
// ============================================================

const configPath = path.join(__dirname, '../../../system_config.json');
let config;

try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    config = {
        REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379'
    };
}

const PAPER_MODE = 'PAPER';
const DEFAULT_INITIAL_CAPITAL = Number(process.env.PAPER_INITIAL_CAPITAL || 200);
const DEFAULT_FIXED_MARGIN = Number(process.env.PAPER_FIXED_MARGIN || 2.0);
const DEFAULT_FEE_MAKER = Number(process.env.PAPER_FEE_MAKER || 0.0002);
const DEFAULT_FEE_TAKER = Number(process.env.PAPER_FEE_TAKER || 0.0004);
const DEFAULT_MOCK_FUNDING_RATE = Number(process.env.PAPER_MOCK_FUNDING_RATE || 0.0001);
const DEFAULT_SLIPPAGE_BPS = Number(process.env.PAPER_SLIPPAGE_BPS || 2); // 2 bps = 0.02%
const MAX_ACCEPTABLE_SPREAD = Number(process.env.PAPER_MAX_ACCEPTABLE_SPREAD || 0.003); // 0.3%
const WALLET_REDIS_KEY = process.env.PAPER_WALLET_REDIS_KEY || 'paper:wallet:balance:v19';
const BOT_ID = process.env.BOT_ID || 'scout-main';
const MODEL_VERSION = process.env.MODEL_VERSION || 'Universal_Scout.onnx';

class PaperExchange extends IExchange {
    constructor(initialCapital = DEFAULT_INITIAL_CAPITAL) {
        super();

        this.initialCapital = initialCapital;
        this.walletBalance = initialCapital;
        this.activePositions = new Map();

        this.pubClient = new Redis(config.REDIS_URL);
        this.dataClient = new Redis(config.REDIS_URL);

        this.pubClient.on('error', err => console.error('❌ [PAPER REDIS pubClient]', err.message));
        this.dataClient.on('error', err => console.error('❌ [PAPER REDIS dataClient]', err.message));

        this.FIXED_MARGIN = DEFAULT_FIXED_MARGIN;
        this.FEE_MAKER = DEFAULT_FEE_MAKER;
        this.FEE_TAKER = DEFAULT_FEE_TAKER;
        this.MOCK_FUNDING_RATE = DEFAULT_MOCK_FUNDING_RATE;
        this.DEFAULT_SLIPPAGE_BPS = DEFAULT_SLIPPAGE_BPS;

        this._walletLoaded = false;
        this._rulesReady = false;
        this._restoreDone = false;
    }

    // ========================================================
    // 1. UTILS
    // ========================================================
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

    _now() {
        return Date.now();
    }

    _isTakerOrder(orderType) {
        return orderType === 'MARKET' || orderType === 'LIMIT_FOK';
    }

    _getFeeRate(orderType) {
        return this._isTakerOrder(orderType) ? this.FEE_TAKER : this.FEE_MAKER;
    }

    _sideFromType(type) {
        return type === 'LONG' ? 'BUY' : 'SELL';
    }

    _exitSideFromType(type) {
        return type === 'LONG' ? 'SELL' : 'BUY';
    }

    _buildFeatureVector(features = {}) {
        if (!features || typeof features !== 'object') return undefined;

        // Thứ tự phải khớp 13 input ONNX trong OrderManager.
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
        await this.ensureWalletLoaded();

        if (!this._rulesReady) {
            await exchangeRules.ensureReady();
            // Nếu có API key thì ExchangeRules tự sync leverageBracket trong init; ensureReady chỉ cần exchangeInfo.
            if (typeof exchangeRules.syncLeverageBrackets === 'function') {
                await exchangeRules.syncLeverageBrackets(false);
            }
            this._rulesReady = true;
        }
    }

    // ========================================================
    // 2. WALLET PERSISTENCE
    // ========================================================
    async ensureWalletLoaded() {
        if (this._walletLoaded) return;

        try {
            const saved = await this.dataClient.get(WALLET_REDIS_KEY);
            const savedBalance = this._safeNumber(saved, NaN);

            if (Number.isFinite(savedBalance) && savedBalance >= 0) {
                this.walletBalance = savedBalance;
                console.log(`👛 [SÀN ẢO V19] Khôi phục ví từ Redis: ${this.walletBalance.toFixed(4)} USDT`);
            } else {
                await this.persistWalletBalance();
                console.log(`👛 [SÀN ẢO V19] Khởi tạo ví: ${this.walletBalance.toFixed(4)} USDT`);
            }
        } catch (error) {
            console.error('⚠️ [SÀN ẢO V19] Không thể load ví Redis, dùng RAM:', error.message);
        } finally {
            this._walletLoaded = true;
        }
    }

    async persistWalletBalance() {
        try {
            await this.dataClient.set(WALLET_REDIS_KEY, String(this.walletBalance));
        } catch (error) {
            console.error('⚠️ [SÀN ẢO V19] Không thể lưu ví Redis:', error.message);
        }
    }

    // ========================================================
    // 3. LIVE-LIKE FILL SIMULATOR
    // ========================================================
    _getBookFromFeature(features = {}, fallbackPrice = 0) {
        const last = this._safeNumber(features?.last_price, fallbackPrice);
        const bid = this._safeNumber(features?.best_bid, 0);
        const ask = this._safeNumber(features?.best_ask, 0);
        const mark = this._safeNumber(features?.mark_price, last);

        if (bid > 0 && ask > 0 && ask > bid) {
            const spread = last > 0 ? (ask - bid) / last : 0;
            return { bid, ask, last: last || (bid + ask) / 2, mark: mark || last || (bid + ask) / 2, spread, hasBook: true };
        }

        // Fallback bất lợi nếu chưa có book: tự tạo synthetic spread nhẹ quanh current price.
        const synthetic = last || fallbackPrice;
        if (synthetic <= 0) {
            return { bid: 0, ask: 0, last: 0, mark: 0, spread: 1, hasBook: false };
        }

        const syntheticSpread = 0.0005; // 0.05% fallback
        return {
            bid: synthetic * (1 - syntheticSpread / 2),
            ask: synthetic * (1 + syntheticSpread / 2),
            last: synthetic,
            mark: mark || synthetic,
            spread: syntheticSpread,
            hasBook: false
        };
    }

    _slippageRate(features = {}, side = 'BUY') {
        const base = this.DEFAULT_SLIPPAGE_BPS / 10000;
        const spread = this._safeNumber(features?.spread_close, 0);

        // Nếu spread đã cao, paper phải phạt thêm để không quá lạc quan.
        const spreadPenalty = Math.max(0, spread) * 0.25;

        // Nếu có orderbook imbalance nghiêng ngược hướng thì phạt nhẹ.
        const obImb = this._safeNumber(features?.ob_imb_top20, 0);
        let imbalancePenalty = 0;
        if (side === 'BUY' && obImb < -0.3) imbalancePenalty = 0.0002;
        if (side === 'SELL' && obImb > 0.3) imbalancePenalty = 0.0002;

        return Math.max(0, base + spreadPenalty + imbalancePenalty);
    }

    _simulateMarketFill({ symbol, side, quantity, fallbackPrice, features, purpose }) {
        const book = this._getBookFromFeature(features, fallbackPrice);
        if (book.last <= 0 || book.bid <= 0 || book.ask <= 0) {
            return { ok: false, reason: 'NO_VALID_PRICE' };
        }

        if (book.spread > MAX_ACCEPTABLE_SPREAD) {
            return { ok: false, reason: 'SPREAD_TOO_WIDE', detail: `spread=${book.spread}` };
        }

        const slip = this._slippageRate(features, side);
        let rawFillPrice;

        if (side === 'BUY') {
            rawFillPrice = book.ask * (1 + slip);
        } else {
            rawFillPrice = book.bid * (1 - slip);
        }

        const normalizedPrice = exchangeRules.normalizePrice(symbol, rawFillPrice, side === 'BUY' ? 'ceil' : 'floor');
        if (!normalizedPrice || normalizedPrice <= 0) {
            return { ok: false, reason: 'BAD_NORMALIZED_FILL_PRICE' };
        }

        return {
            ok: true,
            purpose,
            side,
            quantity,
            fillPrice: normalizedPrice,
            rawFillPrice,
            slippageRate: slip,
            spread: book.spread,
            usedSyntheticBook: !book.hasBook,
            markPrice: book.mark
        };
    }

    _calculateLiquidationPrice(type, entryPrice, positionSize, margin, notionalValue, maintenanceMarginRate) {
        if (positionSize <= 0 || entryPrice <= 0) return 0;

        const maintMargin = notionalValue * maintenanceMarginRate;
        const liquidationPrice = type === 'LONG'
            ? entryPrice - (margin - maintMargin) / positionSize
            : entryPrice + (margin - maintMargin) / positionSize;

        return liquidationPrice > 0 ? liquidationPrice : 0;
    }

    _calculateGrossPnl(position, closePrice) {
        if (position.type === 'LONG') {
            return (closePrice - position.entryPrice) * position.size;
        }
        return (position.entryPrice - closePrice) * position.size;
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

    _publishDashboardUpdate() {
        try {
            this.pubClient.publish('dashboard:trades', JSON.stringify({ action: 'UPDATE' }));
        } catch (error) {
            console.error('⚠️ [PAPER V19] Lỗi publish dashboard:trades:', error.message);
        }
    }

    _logReject(symbol, reason, extra = '') {
        console.log(`⚠️ [SÀN ẢO V19] TỪ CHỐI ${symbol}: ${reason}${extra ? ` | ${extra}` : ''}`);
    }

    // ========================================================
    // 4. RESTORE OPEN TRADES
    // ========================================================
    async restoreOpenTrades() {
        await this.ensureReady();
        if (this._restoreDone) return this.getActivePositions();

        try {
            const openTrades = await ScoutTrade.find({
                status: 'OPEN',
                mode: PAPER_MODE,
                botId: BOT_ID
            }).sort({ openTime: 1 }).lean();

            for (const trade of openTrades) {
                const symbol = this._normalizeSymbol(trade.symbol);
                if (!symbol || this.activePositions.has(symbol)) continue;

                const position = {
                    dbId: trade._id,
                    entryPrice: this._safeNumber(trade.entryPrice),
                    size: this._safeNumber(trade.size),
                    leverage: this._safeNumber(trade.leverage, 1),
                    margin: this._safeNumber(trade.margin, this.FIXED_MARGIN),
                    notionalValue: this._safeNumber(trade.notionalValue, this._safeNumber(trade.size) * this._safeNumber(trade.entryPrice)),
                    orderType: trade.orderType || 'MARKET',
                    entryFee: this._safeNumber(trade.entryFee),
                    type: this._normalizeType(trade.type),
                    features: trade.featuresAtEntry || trade.features || null,
                    featureVectorAtEntry: trade.featureVectorAtEntry,
                    liquidationPrice: this._safeNumber(trade.liquidationPrice),
                    highestPrice: this._safeNumber(trade.highestPrice, trade.entryPrice),
                    lowestPrice: this._safeNumber(trade.lowestPrice, trade.entryPrice),
                    openTime: trade.openTs || new Date(trade.openTime).getTime(),
                    prob: this._safeNumber(trade.prob),
                    probLong: this._safeNumber(trade.probLong, undefined),
                    probShort: this._safeNumber(trade.probShort, undefined),
                    macroScore: this._safeNumber(trade.macroScore, undefined)
                };

                this.activePositions.set(symbol, position);
            }

            if (openTrades.length > 0) {
                console.log(`♻️ [SÀN ẢO V19] Restore ${this.activePositions.size} lệnh OPEN từ MongoDB`);
            }
        } catch (error) {
            console.error('❌ [SÀN ẢO V19] Lỗi restoreOpenTrades:', error.message);
        } finally {
            this._restoreDone = true;
        }

        return this.getActivePositions();
    }

    getActivePositions() {
        return new Map(this.activePositions);
    }

    // ========================================================
    // 5. OPEN TRADE
    // ========================================================
    async openTrade(symbol, orderType, currentPrice, leverage, prob, type = 'LONG', features = null, meta = {}) {
        await this.ensureReady();

        symbol = this._normalizeSymbol(symbol);
        type = this._normalizeType(type);
        orderType = orderType || 'MARKET';
        currentPrice = this._safeNumber(currentPrice);
        leverage = Math.floor(this._safeNumber(leverage));
        prob = this._safeNumber(prob);

        if (!symbol) return false;
        if (this.activePositions.has(symbol)) {
            this._logReject(symbol, 'đã có vị thế PAPER đang mở');
            return false;
        }
        if (currentPrice <= 0) {
            this._logReject(symbol, 'currentPrice không hợp lệ', `currentPrice=${currentPrice}`);
            return false;
        }
        if (leverage <= 0) {
            this._logReject(symbol, 'leverage không hợp lệ', `leverage=${leverage}`);
            return false;
        }

        const rule = exchangeRules.getSymbolRules(symbol);
        if (!rule || !exchangeRules.isTradable(symbol)) {
            this._logReject(symbol, 'symbol không tradable theo ExchangeRules');
            return false;
        }

        const side = this._sideFromType(type);
        const targetNotional = this.FIXED_MARGIN * leverage;
        const book = this._getBookFromFeature(features, currentPrice);
        const referenceEntryPrice = side === 'BUY' ? book.ask : book.bid;

        if (!referenceEntryPrice || referenceEntryPrice <= 0) {
            this._logReject(symbol, 'không có referenceEntryPrice hợp lệ');
            return false;
        }

        const rawQuantity = targetNotional / referenceEntryPrice;
        const quantity = exchangeRules.normalizeQuantity(symbol, rawQuantity, orderType);

        const validation = exchangeRules.validateMarketEntry({
            symbol,
            side,
            quantity,
            currentPrice: referenceEntryPrice,
            leverage
        });

        if (!validation.ok) {
            this._logReject(symbol, validation.reason, validation.detail || '');
            return false;
        }

        const normalizedQty = validation.normalized.quantity;
        const fill = this._simulateMarketFill({
            symbol,
            side,
            quantity: normalizedQty,
            fallbackPrice: currentPrice,
            features,
            purpose: 'ENTRY'
        });

        if (!fill.ok) {
            this._logReject(symbol, fill.reason, fill.detail || '');
            return false;
        }

        const entryPrice = fill.fillPrice;
        const notionalValue = normalizedQty * entryPrice;
        const finalValidation = exchangeRules.validateMarketEntry({
            symbol,
            side,
            quantity: normalizedQty,
            currentPrice: entryPrice,
            leverage
        });

        if (!finalValidation.ok) {
            this._logReject(symbol, finalValidation.reason, finalValidation.detail || '');
            return false;
        }

        const maintenanceMarginRate = exchangeRules.getMaintenanceMarginRate(symbol, notionalValue);
        const entryFee = notionalValue * this._getFeeRate(orderType);
        const requiredWallet = this.FIXED_MARGIN + entryFee;

        if (this.walletBalance < requiredWallet) {
            this._logReject(symbol, 'ví PAPER không đủ margin + fee', `wallet=${this.walletBalance.toFixed(4)} required=${requiredWallet.toFixed(4)}`);
            return false;
        }

        const openTs = this._now();
        const liquidationPrice = this._calculateLiquidationPrice(
            type,
            entryPrice,
            normalizedQty,
            this.FIXED_MARGIN,
            notionalValue,
            maintenanceMarginRate
        );

        this.walletBalance -= requiredWallet;
        await this.persistWalletBalance();

        const featureVectorAtEntry = this._buildFeatureVector(features);

        const newPosition = {
            entryPrice,
            size: normalizedQty,
            leverage,
            margin: this.FIXED_MARGIN,
            notionalValue,
            orderType,
            entryFee,
            type,
            side,
            features,
            featureVectorAtEntry,
            liquidationPrice,
            highestPrice: entryPrice,
            lowestPrice: entryPrice,
            openTime: openTs,
            prob,
            probLong: meta.probLong,
            probShort: meta.probShort,
            macroScore: meta.macroScore,
            entrySignal: meta.entrySignal || 'AI_PROB_THRESHOLD',
            modelVersion: meta.modelVersion || MODEL_VERSION,
            entryFill: fill
        };

        this.activePositions.set(symbol, newPosition);

        console.log(
            `💰 [SÀN ẢO V19] Mở ${type} ${symbol} | Fill=${entryPrice.toFixed(8)} | Qty=${normalizedQty} | Lev=${leverage}x | Notional=${notionalValue.toFixed(4)}$ | Fee=${entryFee.toFixed(5)}$ | Slip=${(fill.slippageRate * 100).toFixed(4)}%`
        );

        try {
            const dbRecord = await ScoutTrade.create({
                symbol,
                type,
                mode: PAPER_MODE,
                orderType,
                status: 'OPEN',
                leverage,
                margin: this.FIXED_MARGIN,
                size: normalizedQty,
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
                botId: BOT_ID,
                note: JSON.stringify({
                    paperVersion: 'V19_LIVE_PARITY',
                    entryFill: fill,
                    walletAfterOpen: this.walletBalance
                })
            });

            newPosition.dbId = dbRecord._id;
        } catch (err) {
            console.error('❌ [MONGO] Lỗi lưu lệnh OPEN:', err.message);
            newPosition.executionError = `MONGO_OPEN_ERROR: ${err.message}`;
        }

        this._publishDashboardUpdate();
        return true;
    }

    // ========================================================
    // 6. CLOSE TRADE
    // ========================================================
    async closeTrade(symbol, closePrice, reason, exitFeatures = null) {
        await this.ensureReady();

        symbol = this._normalizeSymbol(symbol);
        closePrice = this._safeNumber(closePrice);
        reason = reason || 'UNKNOWN';

        if (!this.activePositions.has(symbol)) {
            console.warn(`⚠️ [SÀN ẢO V19] Không thể đóng ${symbol}: không có vị thế active`);
            return null;
        }

        const position = this.activePositions.get(symbol);
        if (closePrice <= 0) {
            console.warn(`⚠️ [SÀN ẢO V19] Không thể đóng ${symbol}: closePrice không hợp lệ ${closePrice}`);
            return null;
        }

        const exitSide = this._exitSideFromType(position.type);
        const fill = this._simulateMarketFill({
            symbol,
            side: exitSide,
            quantity: position.size,
            fallbackPrice: closePrice,
            features: exitFeatures,
            purpose: 'EXIT'
        });

        if (!fill.ok) {
            console.warn(`⚠️ [SÀN ẢO V19] Exit fill lỗi ${symbol}: ${fill.reason}. Fallback closePrice=${closePrice}`);
        }

        const executedClosePrice = fill.ok ? fill.fillPrice : exchangeRules.normalizePrice(symbol, closePrice, 'nearest');
        const closeTs = this._now();
        const exitNotional = position.size * executedClosePrice;
        const exitFee = exitNotional * this.FEE_TAKER;
        const grossPnL = this._calculateGrossPnl(position, executedClosePrice);

        let fundingFeeDeducted = 0;
        const hoursHeld = (closeTs - position.openTime) / (1000 * 60 * 60);
        if (hoursHeld >= 8) {
            // V19 vẫn dùng funding mock đơn giản. FundingEngine thật sẽ làm ở bước sau.
            fundingFeeDeducted = (exitNotional * this.MOCK_FUNDING_RATE) * Math.floor(hoursHeld / 8);
        }

        const netPnL = grossPnL - position.entryFee - exitFee - fundingFeeDeducted;

        // Vì margin + entryFee đã bị trừ khi mở lệnh,
        // khi đóng chỉ hoàn margin + grossPnL - exitFee - funding.
        const walletCredit = position.margin + grossPnL - exitFee - fundingFeeDeducted;
        this.walletBalance += walletCredit;
        this.walletBalance = Math.max(0, this.walletBalance);
        await this.persistWalletBalance();

        const roiPercent = position.margin > 0 ? (netPnL / position.margin) * 100 : 0;
        const { mfePercent, maePercent } = this._calculateMaeMfe(position);
        const durationMs = Math.max(0, closeTs - position.openTime);
        const featureVectorAtExit = this._buildFeatureVector(exitFeatures);
        const finalStatus = reason === 'LIQUIDATED' ? 'LIQUIDATED' : 'CLOSED';

        console.log(
            `💳 [SÀN ẢO V19] Đóng ${position.type} ${symbol} (${reason}) | Fill=${executedClosePrice.toFixed(8)} | Gross=${grossPnL.toFixed(5)}$ | Net=${netPnL.toFixed(5)}$ | ROI=${roiPercent.toFixed(2)}% | Ví=${this.walletBalance.toFixed(2)}$`
        );

        try {
            const updatePayload = {
                status: finalStatus,
                closePrice: executedClosePrice,
                executedClosePrice,
                exitFee,
                fundingFee: fundingFeeDeducted,
                closeTime: new Date(closeTs),
                closeTs,
                durationMs,
                reason,
                pnl: netPnL,
                roi: roiPercent,
                grossPnl: grossPnL,
                netPnl: netPnL,
                mae: maePercent,
                mfe: mfePercent,
                highestPrice: position.highestPrice,
                lowestPrice: position.lowestPrice,
                features: position.features,
                featuresAtEntry: position.features,
                featuresAtExit: exitFeatures,
                featureVectorAtEntry: position.featureVectorAtEntry,
                featureVectorAtExit,
                note: JSON.stringify({
                    paperVersion: 'V19_LIVE_PARITY',
                    entryFill: position.entryFill,
                    exitFill: fill,
                    walletAfterClose: this.walletBalance
                })
            };

            if (position.dbId) {
                await ScoutTrade.findByIdAndUpdate(position.dbId, updatePayload, { new: false });
            } else {
                await ScoutTrade.findOneAndUpdate(
                    { symbol, status: 'OPEN', mode: PAPER_MODE, botId: BOT_ID },
                    updatePayload,
                    { sort: { openTime: -1 }, new: false }
                );
            }
        } catch (err) {
            console.error('❌ [MONGO] Lỗi update lệnh CLOSED:', err.message);
        }

        this.activePositions.delete(symbol);
        this._publishDashboardUpdate();

        return {
            symbol,
            type: position.type,
            closed: true,
            reason,
            closePrice: executedClosePrice,
            grossPnL,
            netPnL,
            roiPercent,
            maePercent,
            mfePercent,
            durationMs,
            walletBalance: this.walletBalance
        };
    }

    // ========================================================
    // 7. UPDATE TICK / LIQUIDATION CHECK
    // ========================================================
    async updateTick(symbol, currentPrice, exitFeatures = null) {
        await this.ensureReady();

        symbol = this._normalizeSymbol(symbol);
        currentPrice = this._safeNumber(currentPrice);

        if (!this.activePositions.has(symbol) || currentPrice <= 0) {
            return null;
        }

        const position = this.activePositions.get(symbol);
        const book = this._getBookFromFeature(exitFeatures, currentPrice);
        const markPrice = this._safeNumber(exitFeatures?.mark_price, book.mark || currentPrice);

        // Dùng markPrice để cập nhật MFE/MAE gần với cơ chế Futures hơn.
        const evaluationPrice = markPrice > 0 ? markPrice : currentPrice;

        if (evaluationPrice > position.highestPrice) position.highestPrice = evaluationPrice;
        if (evaluationPrice < position.lowestPrice) position.lowestPrice = evaluationPrice;

        if (position.liquidationPrice <= 0) {
            return { closed: false, symbol, highestPrice: position.highestPrice, lowestPrice: position.lowestPrice };
        }

        const longLiquidated = position.type === 'LONG' && evaluationPrice <= position.liquidationPrice;
        const shortLiquidated = position.type === 'SHORT' && evaluationPrice >= position.liquidationPrice;

        if (longLiquidated || shortLiquidated) {
            console.log(`💀 [LIQUIDATED V19] ${symbol} mark=${evaluationPrice.toFixed(8)} chạm liquidation=${position.liquidationPrice.toFixed(8)}`);
            const result = await this.closeTrade(symbol, currentPrice, 'LIQUIDATED', exitFeatures);
            return {
                closed: true,
                reason: 'LIQUIDATED',
                symbol,
                closePrice: result?.closePrice || currentPrice,
                result
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
    // 8. GETTERS / COMMANDS
    // ========================================================
    getWalletBalance() {
        return this.walletBalance;
    }

    hasActivePosition(symbol) {
        symbol = this._normalizeSymbol(symbol);
        return this.activePositions.has(symbol);
    }

    getPosition(symbol) {
        symbol = this._normalizeSymbol(symbol);
        return this.activePositions.get(symbol) || null;
    }

    async resetPaperWallet(amount = this.initialCapital) {
        this.walletBalance = this._safeNumber(amount, this.initialCapital);
        await this.persistWalletBalance();
        console.log(`♻️ [SÀN ẢO V19] Reset ví PAPER về ${this.walletBalance.toFixed(4)} USDT`);
        return this.walletBalance;
    }
}

module.exports = new PaperExchange(DEFAULT_INITIAL_CAPITAL);
