const IExchange = require('./IExchange');
const ScoutTrade = require('../../models/ScoutTrade');

const Redis = require('ioredis');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '../../../.env') });

// ============================================================
// PAPER EXCHANGE - FIXED SAFE MONGO-FIRST VERSION
// ------------------------------------------------------------
// Mục tiêu sửa lỗi lần này:
// 1. Tuyệt đối KHÔNG trừ ví paper nếu Mongo lưu OPEN thất bại.
// 2. Tuyệt đối KHÔNG set activePositions nếu Mongo lưu OPEN thất bại.
// 3. Khi closeTrade, nếu update Mongo thất bại thì vẫn giữ position trong RAM
//    và KHÔNG cộng/trừ ví để tránh lệch trạng thái.
// 4. Có restoreOpenTrades() để PM2 restart vẫn khôi phục lệnh OPEN từ Mongo.
// 5. Ví paper mặc định 200 USDT, có thể reset qua system command.
// 6. Không mở trùng symbol nếu lệnh cũ còn mở.
// 7. Giữ interface tương thích OrderManager_Final:
//    - openTrade(...)
//    - closeTrade(...)
//    - updateTick(...)
//    - restoreOpenTrades()
//    - resetPaperWallet(...)
//    - getActivePositions()
//
// Lý do phải Mongo-first:
// Nếu bot dùng để học mỗi đêm, một lệnh không lưu được Mongo là lệnh vô nghĩa.
// Không được để ví paper thay đổi nhưng không có lịch sử lệnh/dataset.
// ============================================================

const configPath = path.join(__dirname, '../../../system_config.json');
let config;
try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    config = { REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379' };
}

const REDIS_URL = config.REDIS_URL || process.env.REDIS_URL || 'redis://localhost:6379';
const PAPER = config.PAPER || {};
const TRADING = config.TRADING || {};

const INITIAL_CAPITAL = Number(process.env.PAPER_INITIAL_CAPITAL || PAPER.INITIAL_CAPITAL || 200);
const FIXED_MARGIN = Number(process.env.PAPER_FIXED_MARGIN || PAPER.FIXED_MARGIN || TRADING.FIXED_MARGIN || 2);
const FEE_TAKER = Number(process.env.PAPER_FEE_TAKER || PAPER.FEE_TAKER || 0.0004);
const SLIPPAGE_BPS = Number(process.env.PAPER_SLIPPAGE_BPS || PAPER.SLIPPAGE_BPS || 2);
const MAX_ACCEPTABLE_SPREAD = Number(process.env.PAPER_MAX_ACCEPTABLE_SPREAD || PAPER.MAX_ACCEPTABLE_SPREAD || 0.003);
const WALLET_KEY = process.env.PAPER_WALLET_REDIS_KEY || 'paper:wallet:balance:v19';
const BOT_ID = process.env.BOT_ID || 'scout-paper-v19';
const MODEL_VERSION = process.env.MODEL_VERSION || TRADING.MODEL_VERSION || 'Universal_Scout.onnx';

class PaperExchange extends IExchange {
    constructor() {
        super();

        this.redis = new Redis(REDIS_URL);
        this.redis.on('error', err => console.error('❌ [PAPER REDIS]', err.message));

        this.activePositions = new Map();
        this.wallet = {
            balance: INITIAL_CAPITAL,
            initialCapital: INITIAL_CAPITAL,
            updatedAt: Date.now()
        };

        this.ready = false;
        this.FIXED_MARGIN = FIXED_MARGIN;
        this.FEE_TAKER = FEE_TAKER;
        this.SLIPPAGE_BPS = SLIPPAGE_BPS;
        this.MAX_ACCEPTABLE_SPREAD = MAX_ACCEPTABLE_SPREAD;
    }

    // ========================================================
    // 1. INIT / WALLET
    // ========================================================
    async ensureReady() {
        if (this.ready) return;
        await this.loadWallet();
        await this.restoreOpenTrades();
        this.ready = true;
        console.log(`✅ [SÀN ẢO FIXED] Ready | balance=${this.wallet.balance.toFixed(4)} USDT | active=${this.activePositions.size}`);
    }

    async loadWallet() {
        try {
            const raw = await this.redis.get(WALLET_KEY);
            const parsed = raw != null ? Number(raw) : NaN;

            if (Number.isFinite(parsed)) {
                this.wallet.balance = parsed;
                console.log(`👛 [SÀN ẢO FIXED] Khôi phục ví từ Redis: ${this.wallet.balance.toFixed(4)} USDT`);
            } else {
                this.wallet.balance = INITIAL_CAPITAL;
                await this.persistWallet();
                console.log(`👛 [SÀN ẢO FIXED] Tạo ví mới: ${this.wallet.balance.toFixed(4)} USDT`);
            }
        } catch (error) {
            console.error('❌ [SÀN ẢO FIXED] Lỗi load ví Redis:', error.message);
            this.wallet.balance = INITIAL_CAPITAL;
        }
    }

    async persistWallet() {
        this.wallet.updatedAt = Date.now();
        await this.redis.set(WALLET_KEY, String(this.wallet.balance));
    }

    async resetPaperWallet(amount = INITIAL_CAPITAL) {
        const value = Number(amount);
        if (!Number.isFinite(value) || value < 0) {
            throw new Error(`Invalid paper wallet reset amount: ${amount}`);
        }

        this.wallet.balance = value;
        this.wallet.initialCapital = value;
        await this.persistWallet();
        console.log(`🔄 [SÀN ẢO FIXED] Reset ví paper: ${value.toFixed(4)} USDT`);
        return this.wallet;
    }

    getWallet() {
        return { ...this.wallet };
    }

    // ========================================================
    // 2. BASIC UTILS
    // ========================================================
    normalizeSymbol(symbol) {
        return String(symbol || '').toUpperCase().trim();
    }

    normalizeType(type) {
        return type === 'SHORT' ? 'SHORT' : 'LONG';
    }

    safeNumber(value, fallback = 0) {
        const n = Number(value);
        return Number.isFinite(n) ? n : fallback;
    }

    buildFeatureVector(features = {}) {
        if (!features || typeof features !== 'object') return undefined;

        // THỨ TỰ 13 INPUT phải khớp OrderManager/ONNX.
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

    getExecutionPrice(type, currentPrice, features = null, purpose = 'ENTRY') {
        const price = this.safeNumber(currentPrice, 0);
        if (price <= 0) return 0;

        const bestBid = this.safeNumber(features?.best_bid, 0);
        const bestAsk = this.safeNumber(features?.best_ask, 0);

        // Nếu có bid/ask thật thì mô phỏng market fill sát hơn.
        if (bestBid > 0 && bestAsk > 0 && bestAsk > bestBid) {
            if (purpose === 'ENTRY') {
                return type === 'LONG' ? bestAsk : bestBid;
            }
            return type === 'LONG' ? bestBid : bestAsk;
        }

        // Fallback slippage theo bps.
        const slip = this.SLIPPAGE_BPS / 10000;
        if (purpose === 'ENTRY') {
            return type === 'LONG' ? price * (1 + slip) : price * (1 - slip);
        }
        return type === 'LONG' ? price * (1 - slip) : price * (1 + slip);
    }

    validateSpread(features = null) {
        if (!features) return { ok: true };
        const spread = this.safeNumber(features.spread_close, 0);
        if (spread < 0) return { ok: false, reason: 'BAD_SPREAD' };
        if (spread > this.MAX_ACCEPTABLE_SPREAD) {
            return { ok: false, reason: `SPREAD_TOO_WIDE_${spread}` };
        }
        return { ok: true };
    }

    estimateLiquidationPrice(type, entryPrice, size, margin, notionalValue) {
        if (size <= 0 || entryPrice <= 0 || margin <= 0) return 0;

        // Mô phỏng đơn giản cho paper, không thay thế công thức Binance thật.
        // Dùng maintenance giả định 0.5% notional để dashboard có mốc tham khảo.
        const maintenanceMargin = notionalValue * 0.005;
        const effectiveMargin = Math.max(0, margin - maintenanceMargin);

        if (type === 'LONG') {
            return Math.max(0, entryPrice - effectiveMargin / size);
        }
        return entryPrice + effectiveMargin / size;
    }

    calculateMaeMfe(position) {
        if (!position || !position.entryPrice) return { maePercent: 0, mfePercent: 0 };

        let mfePercent = 0;
        let maePercent = 0;

        if (position.type === 'LONG') {
            mfePercent = ((position.highestPrice - position.entryPrice) / position.entryPrice) * 100;
            maePercent = ((position.lowestPrice - position.entryPrice) / position.entryPrice) * 100;
        } else {
            mfePercent = ((position.entryPrice - position.lowestPrice) / position.entryPrice) * 100;
            maePercent = ((position.entryPrice - position.highestPrice) / position.entryPrice) * 100;
        }

        return { maePercent, mfePercent };
    }

    // ========================================================
    // 3. RESTORE OPEN TRADES FROM MONGO
    // ========================================================
    async restoreOpenTrades() {
        try {
            const openTrades = await ScoutTrade.find({ status: 'OPEN', mode: { $in: ['PAPER', 'BACKTEST'] } })
                .sort({ openTime: -1 })
                .lean();

            this.activePositions.clear();

            for (const t of openTrades) {
                const symbol = this.normalizeSymbol(t.symbol);
                if (!symbol) continue;

                const entryPrice = this.safeNumber(t.executedEntryPrice || t.entryPrice, 0);
                const size = this.safeNumber(t.size, 0);
                if (entryPrice <= 0 || size <= 0) continue;

                this.activePositions.set(symbol, {
                    dbId: t._id,
                    symbol,
                    type: this.normalizeType(t.type),
                    entryPrice,
                    size,
                    leverage: this.safeNumber(t.leverage, 1),
                    margin: this.safeNumber(t.margin, this.FIXED_MARGIN),
                    notionalValue: this.safeNumber(t.notionalValue, entryPrice * size),
                    entryFee: this.safeNumber(t.entryFee, 0),
                    liquidationPrice: this.safeNumber(t.liquidationPrice, 0),
                    highestPrice: this.safeNumber(t.highestPrice, entryPrice),
                    lowestPrice: this.safeNumber(t.lowestPrice, entryPrice),
                    openTime: this.safeNumber(t.openTs, t.openTime ? new Date(t.openTime).getTime() : Date.now()),
                    prob: this.safeNumber(t.prob, 0),
                    probLong: t.probLong,
                    probShort: t.probShort,
                    macroScore: t.macroScore,
                    features: t.featuresAtEntry || t.features || null,
                    featureVectorAtEntry: t.featureVectorAtEntry || undefined,
                    restored: true
                });
            }

            if (openTrades.length > 0) {
                console.log(`♻️ [SÀN ẢO FIXED] Restore ${this.activePositions.size}/${openTrades.length} OPEN trades từ Mongo`);
            }
        } catch (error) {
            console.error('❌ [SÀN ẢO FIXED] Lỗi restore OPEN trades:', error.message);
        }
    }

    // ========================================================
    // 4. OPEN TRADE - MONGO FIRST, WALLET SECOND
    // ========================================================
    async openTrade(symbol, orderType, currentPrice, leverage, prob, type = 'LONG', features = null, meta = {}) {
        await this.ensureReady();

        symbol = this.normalizeSymbol(symbol);
        type = this.normalizeType(type);
        currentPrice = this.safeNumber(currentPrice, 0);
        leverage = Math.floor(this.safeNumber(leverage, 1));
        prob = this.safeNumber(prob, 0);

        if (!symbol || currentPrice <= 0 || leverage <= 0) {
            console.warn(`⚠️ [SÀN ẢO FIXED] Reject open: BAD_INPUT symbol=${symbol} price=${currentPrice} lev=${leverage}`);
            return false;
        }

        if (this.activePositions.has(symbol)) {
            console.warn(`⚠️ [SÀN ẢO FIXED] Reject open ${symbol}: ACTIVE_POSITION_EXISTS`);
            return false;
        }

        const spreadCheck = this.validateSpread(features);
        if (!spreadCheck.ok) {
            console.warn(`⚠️ [SÀN ẢO FIXED] Reject open ${symbol}: ${spreadCheck.reason}`);
            return false;
        }

        const margin = this.FIXED_MARGIN;
        if (this.wallet.balance < margin) {
            console.warn(`⚠️ [SÀN ẢO FIXED] Reject open ${symbol}: INSUFFICIENT_BALANCE balance=${this.wallet.balance.toFixed(4)} margin=${margin}`);
            return false;
        }

        const entryPrice = this.getExecutionPrice(type, currentPrice, features, 'ENTRY');
        if (entryPrice <= 0) {
            console.warn(`⚠️ [SÀN ẢO FIXED] Reject open ${symbol}: BAD_EXECUTION_PRICE`);
            return false;
        }

        const notionalValue = margin * leverage;
        const size = notionalValue / entryPrice;
        const entryFee = notionalValue * this.FEE_TAKER;
        const openTs = Date.now();
        const liquidationPrice = this.estimateLiquidationPrice(type, entryPrice, size, margin, notionalValue);
        const featureVectorAtEntry = this.buildFeatureVector(features);

        const position = {
            symbol,
            type,
            entryPrice,
            size,
            leverage,
            margin,
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
            featureVectorAtEntry
        };

        const payload = {
            symbol,
            type,
            mode: 'PAPER',
            orderType: orderType || 'MARKET',
            status: 'OPEN',
            leverage,
            margin,
            size,
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
                paperVersion: 'FIXED_MONGO_FIRST',
                currentPrice,
                entryPrice,
                slippageBps: this.SLIPPAGE_BPS,
                spread: features?.spread_close
            })
        };

        let dbRecord;

        try {
            // QUAN TRỌNG: Mongo phải lưu OPEN thành công trước.
            dbRecord = await ScoutTrade.create(payload);
        } catch (error) {
            console.error(`❌ [MONGO] Lỗi lưu lệnh OPEN ${symbol}:`, error.message);
            console.error('⛔ [SÀN ẢO FIXED] Không trừ ví, không mở position vì Mongo lưu thất bại.');
            return false;
        }

        try {
            // Chỉ sau khi Mongo OK mới trừ entry fee và set active position.
            this.wallet.balance -= entryFee;
            await this.persistWallet();

            position.dbId = dbRecord._id;
            this.activePositions.set(symbol, position);

            console.log(`✅ [SÀN ẢO FIXED] Open ${type} ${symbol} entry=${entryPrice} size=${size} lev=${leverage} fee=${entryFee.toFixed(6)} wallet=${this.wallet.balance.toFixed(4)}`);
            return true;
        } catch (error) {
            // Nếu Redis/wallet fail sau Mongo OK, đánh dấu trade FAILED để không treo trạng thái.
            console.error(`❌ [SÀN ẢO FIXED] Lỗi sau khi Mongo đã lưu OPEN ${symbol}:`, error.message);

            try {
                await ScoutTrade.findByIdAndUpdate(dbRecord._id, {
                    status: 'FAILED',
                    reason: 'POST_MONGO_OPEN_FAILED',
                    error: error.message,
                    closeTime: new Date(),
                    closeTs: Date.now()
                }, { returnDocument: 'before' });
            } catch (updateError) {
                console.error(`❌ [MONGO] Không đánh dấu FAILED được ${symbol}:`, updateError.message);
            }

            return false;
        }
    }

    // ========================================================
    // 5. CLOSE TRADE - MONGO UPDATE FIRST, WALLET SECOND
    // ========================================================
    async closeTrade(symbol, closePrice, reason = 'UNKNOWN', exitFeatures = null) {
        await this.ensureReady();

        symbol = this.normalizeSymbol(symbol);
        closePrice = this.safeNumber(closePrice, 0);
        reason = reason || 'UNKNOWN';

        const position = this.activePositions.get(symbol);
        if (!position) {
            console.warn(`⚠️ [SÀN ẢO FIXED] Không có active position để đóng ${symbol}`);
            return null;
        }

        if (closePrice <= 0) {
            console.warn(`⚠️ [SÀN ẢO FIXED] Reject close ${symbol}: BAD_CLOSE_PRICE`);
            return null;
        }

        const executedClosePrice = this.getExecutionPrice(position.type, closePrice, exitFeatures, 'EXIT');
        const exitNotional = executedClosePrice * position.size;
        const exitFee = exitNotional * this.FEE_TAKER;

        const grossPnl = position.type === 'LONG'
            ? (executedClosePrice - position.entryPrice) * position.size
            : (position.entryPrice - executedClosePrice) * position.size;

        const netPnl = grossPnl - position.entryFee - exitFee;
        const roiPercent = position.margin > 0 ? (netPnl / position.margin) * 100 : 0;
        const closeTs = Date.now();
        const durationMs = Math.max(0, closeTs - position.openTime);
        const { maePercent, mfePercent } = this.calculateMaeMfe(position);
        const featureVectorAtExit = this.buildFeatureVector(exitFeatures);
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
            note: JSON.stringify({
                paperVersion: 'FIXED_MONGO_FIRST',
                reason,
                closePrice,
                executedClosePrice,
                exitFee,
                walletBefore: this.wallet.balance
            })
        };

        try {
            // QUAN TRỌNG: Mongo update CLOSED phải thành công trước khi cộng/trừ ví.
            if (!position.dbId) {
                throw new Error('Missing dbId on active position. Refuse to close to avoid wallet drift.');
            }

            await ScoutTrade.findByIdAndUpdate(position.dbId, updatePayload, { returnDocument: 'before' });
        } catch (error) {
            console.error(`❌ [MONGO] Lỗi update lệnh CLOSED ${symbol}:`, error.message);
            console.error('⛔ [SÀN ẢO FIXED] Không cập nhật ví, không xóa active position để tránh lệch trạng thái.');
            return null;
        }

        try {
            // Paper wallet: khi mở đã chỉ trừ entryFee, margin không bị khóa thật trong ví này.
            // Khi đóng cộng netPnl đã bao gồm entryFee và exitFee.
            // Vì entryFee đã trừ trước đó, để không trừ entryFee hai lần, cộng grossPnl - exitFee.
            const walletDelta = grossPnl - exitFee;
            this.wallet.balance += walletDelta;
            await this.persistWallet();

            this.activePositions.delete(symbol);

            console.log(`✅ [SÀN ẢO FIXED] Close ${position.type} ${symbol} exit=${executedClosePrice} gross=${grossPnl.toFixed(6)} net=${netPnl.toFixed(6)} roi=${roiPercent.toFixed(2)}% wallet=${this.wallet.balance.toFixed(4)}`);

            return {
                symbol,
                type: position.type,
                closed: true,
                reason,
                closePrice: executedClosePrice,
                grossPnL: grossPnl,
                netPnL: netPnl,
                grossPnl,
                netPnl,
                roiPercent,
                roi: roiPercent,
                maePercent,
                mfePercent,
                durationMs
            };
        } catch (error) {
            console.error(`❌ [SÀN ẢO FIXED] Lỗi cập nhật ví sau khi close Mongo OK ${symbol}:`, error.message);
            // Lúc này Mongo đã CLOSED nhưng ví chưa cập nhật được. Đây là lỗi cần can thiệp.
            // Không restore lại Mongo vì CLOSED là sự kiện thật trong paper execution.
            this.activePositions.delete(symbol);
            return null;
        }
    }

    // ========================================================
    // 6. UPDATE TICK
    // ========================================================
    async updateTick(symbol, currentPrice, exitFeatures = null) {
        await this.ensureReady();

        symbol = this.normalizeSymbol(symbol);
        currentPrice = this.safeNumber(currentPrice, 0);

        const position = this.activePositions.get(symbol);
        if (!position || currentPrice <= 0) return null;

        const markOrLast = this.safeNumber(exitFeatures?.mark_price, currentPrice);

        if (markOrLast > position.highestPrice) {
            position.highestPrice = markOrLast;
        }

        if (markOrLast < position.lowestPrice) {
            position.lowestPrice = markOrLast;
        }

        // PaperExchange không tự quyết định SL/TP ở đây.
        // OrderManager quản trị trailing/reversal rồi gọi closeTrade().
        return {
            closed: false,
            symbol,
            highestPrice: position.highestPrice,
            lowestPrice: position.lowestPrice
        };
    }

    // ========================================================
    // 7. GETTERS
    // ========================================================
    getActivePositions() {
        return new Map(this.activePositions);
    }

    getPosition(symbol) {
        return this.activePositions.get(this.normalizeSymbol(symbol)) || null;
    }

    hasActivePosition(symbol) {
        return this.activePositions.has(this.normalizeSymbol(symbol));
    }
}

module.exports = new PaperExchange();
