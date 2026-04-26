const Redis = require('ioredis');
const fs = require('fs');
const path = require('path');

// ============================================================
// RISK GUARD V1 - CENTRAL PRE-TRADE SAFETY LAYER
// ------------------------------------------------------------
// Vai trò:
// 1. Là lớp chặn rủi ro trước khi OrderManager mở lệnh.
// 2. Không thay thế ONNX; chỉ chặn những tình huống nguy hiểm.
// 3. Dùng được cho PAPER, DRY_RUN và LIVE.
// 4. Fail-safe: nếu dữ liệu quan trọng lỗi/stale, không cho vào lệnh.
//
// OrderManager_V18 đã gọi optional:
// riskGuard.canOpenTrade(symbol, action, feature, context)
//
// Return chuẩn:
// { allowed: true }
// hoặc
// { allowed: false, reason: '...' }
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

const REDIS_URL = config.REDIS_URL || process.env.REDIS_URL || 'redis://localhost:6379';
const RISK = config.RISK || {};

// ============================================================
// DEFAULT RISK CONFIG
// ============================================================
const DEFAULTS = {
    ENABLED: true,

    // Spread/data health.
    MAX_SPREAD_CLOSE: 0.0025,             // 0.25%
    MAX_DEPTH_AGE_MS: 2500,
    MAX_PRICE_AGE_MS: 8000,
    MAX_MARK_PRICE_AGE_MS: 10000,
    REQUIRE_MARK_PRICE: false,            // paper/live-parity nên bật true khi FeedHandler V19 ổn định.

    // Funding/market stress.
    MAX_ABS_FUNDING_RATE: 0.005,          // 0.5% funding cực đoan thì tránh.
    MAX_ATR14: 0.20,                      // guard data lỗi, ATR 20% là rất bất thường.
    MIN_ATR14: 0.00001,

    // Position/exposure.
    MAX_ACTIVE_TRADES: 3,
    MAX_TOTAL_LEVERAGE_NOTIONAL: 200,     // tổng notional PAPER/LIVE guard mềm.
    MAX_LEVERAGE_PER_TRADE: 20,

    // Frequency guard.
    MAX_TRADES_PER_HOUR: 20,
    MAX_TRADES_PER_SYMBOL_PER_HOUR: 3,
    SYMBOL_COOLDOWN_AFTER_BLOCK_MS: 60 * 1000,

    // Loss guard.
    MAX_DAILY_LOSS_USDT: 10,
    MAX_DAILY_LOSS_PCT: 5,
    MAX_CONSECUTIVE_LOSSES: 5,

    // ONNX confidence sanity.
    MIN_ENTRY_PROB: 0.70,
    MIN_PROB_GAP: 0.02,                   // tránh long/short quá sát nhau.

    // Symbol blacklist/whitelist.
    BLACKLIST: [],
    WHITELIST: [],                        // nếu có whitelist thì chỉ trade symbol trong whitelist.

    // System kill switch.
    PAUSE_REDIS_KEY: 'risk:trading:paused',
    DAILY_PNL_REDIS_KEY: 'risk:daily:pnl',
    CONSECUTIVE_LOSS_REDIS_KEY: 'risk:consecutive_losses',
    TRADE_LOG_REDIS_KEY: 'risk:trade:log',
    SYMBOL_TRADE_LOG_PREFIX: 'risk:symbol:trade:log',
    BLOCKED_SYMBOL_PREFIX: 'risk:symbol:blocked_until'
};

function pick(key) {
    return RISK[key] !== undefined ? RISK[key] : DEFAULTS[key];
}

class RiskGuard {
    constructor() {
        this.redis = new Redis(REDIS_URL);
        this.redis.on('error', err => {
            console.error('❌ [RiskGuard Redis]', err.message);
        });

        this.enabled = pick('ENABLED');
        this.blacklist = new Set((pick('BLACKLIST') || []).map(s => String(s).toUpperCase()));
        this.whitelist = new Set((pick('WHITELIST') || []).map(s => String(s).toUpperCase()));
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

    reject(reason, detail = {}) {
        return { allowed: false, reason, detail };
    }

    allow() {
        return { allowed: true };
    }

    async isPaused() {
        try {
            const val = await this.redis.get(pick('PAUSE_REDIS_KEY'));
            return val === '1' || val === 'true' || val === 'PAUSED';
        } catch (e) {
            // Fail-safe: nếu không đọc được Redis pause key thì không tự pause,
            // vì OrderManager vẫn cần hoạt động trong PAPER. Live mode sau này có thể đổi thành true.
            return false;
        }
    }

    async getDailyPnl() {
        try {
            const raw = await this.redis.get(pick('DAILY_PNL_REDIS_KEY'));
            return this.safeNumber(raw, 0);
        } catch (e) {
            return 0;
        }
    }

    async getConsecutiveLosses() {
        try {
            const raw = await this.redis.get(pick('CONSECUTIVE_LOSS_REDIS_KEY'));
            return Math.floor(this.safeNumber(raw, 0));
        } catch (e) {
            return 0;
        }
    }

    async countTradesInWindow(key, windowMs) {
        const now = Date.now();
        const min = now - windowMs;

        try {
            await this.redis.zremrangebyscore(key, 0, min);
            return await this.redis.zcard(key);
        } catch (e) {
            // Nếu không đọc được trade frequency, fail-safe không block ở PAPER.
            return 0;
        }
    }

    async isSymbolBlocked(symbol) {
        const key = `${pick('BLOCKED_SYMBOL_PREFIX')}:${symbol}`;
        try {
            const until = this.safeNumber(await this.redis.get(key), 0);
            return until > Date.now();
        } catch (e) {
            return false;
        }
    }

    async blockSymbol(symbol, ms, reason = '') {
        const key = `${pick('BLOCKED_SYMBOL_PREFIX')}:${symbol}`;
        const until = Date.now() + ms;
        try {
            await this.redis.set(key, String(until), 'PX', ms);
        } catch (e) {}
        if (reason) console.log(`⛔ [RiskGuard] Block ${symbol} ${Math.round(ms / 1000)}s | ${reason}`);
    }

    // ========================================================
    // 2. VALIDATION GROUPS
    // ========================================================
    checkSymbolPolicy(symbol) {
        if (this.blacklist.has(symbol)) return this.reject('SYMBOL_BLACKLISTED');

        if (this.whitelist.size > 0 && !this.whitelist.has(symbol)) {
            return this.reject('SYMBOL_NOT_IN_WHITELIST');
        }

        return this.allow();
    }

    checkFeatureHealth(feature) {
        if (!feature || typeof feature !== 'object') return this.reject('EMPTY_FEATURE');
        if (!feature.is_warm) return this.reject('FEATURE_NOT_WARM');

        const lastPrice = this.safeNumber(feature.last_price, 0);
        const bestBid = this.safeNumber(feature.best_bid, 0);
        const bestAsk = this.safeNumber(feature.best_ask, 0);
        if (lastPrice <= 0) return this.reject('BAD_LAST_PRICE');
        if (bestBid <= 0 || bestAsk <= 0 || bestAsk <= bestBid) return this.reject('BAD_ORDER_BOOK');

        const spread = this.safeNumber(feature.spread_close, 0);
        if (spread < 0 || spread > pick('MAX_SPREAD_CLOSE')) {
            return this.reject('SPREAD_TOO_WIDE', { spread, max: pick('MAX_SPREAD_CLOSE') });
        }

        const depthAge = feature.depth_age_ms;
        if (depthAge !== null && depthAge !== undefined && depthAge > pick('MAX_DEPTH_AGE_MS')) {
            return this.reject('DEPTH_STALE', { depthAge, max: pick('MAX_DEPTH_AGE_MS') });
        }

        const priceAge = feature.price_age_ms;
        if (priceAge !== null && priceAge !== undefined && priceAge > pick('MAX_PRICE_AGE_MS')) {
            return this.reject('PRICE_STALE', { priceAge, max: pick('MAX_PRICE_AGE_MS') });
        }

        if (pick('REQUIRE_MARK_PRICE')) {
            const markPrice = this.safeNumber(feature.mark_price, 0);
            if (markPrice <= 0) return this.reject('NO_MARK_PRICE');

            const markAge = feature.mark_price_age_ms;
            if (markAge !== null && markAge !== undefined && markAge > pick('MAX_MARK_PRICE_AGE_MS')) {
                return this.reject('MARK_PRICE_STALE', { markAge, max: pick('MAX_MARK_PRICE_AGE_MS') });
            }
        }

        const funding = Math.abs(this.safeNumber(feature.funding_rate, 0));
        if (funding > pick('MAX_ABS_FUNDING_RATE')) {
            return this.reject('FUNDING_TOO_EXTREME', { funding, max: pick('MAX_ABS_FUNDING_RATE') });
        }

        const atr = this.safeNumber(feature.ATR14, 0);
        if (atr < pick('MIN_ATR14') || atr > pick('MAX_ATR14')) {
            return this.reject('BAD_ATR', { atr });
        }

        return this.allow();
    }

    checkModelConfidence(action, context = {}) {
        const finalProb = this.safeNumber(context.finalProb, 0);
        const probLong = this.safeNumber(context.probLong, 0);
        const probShort = this.safeNumber(context.probShort, 0);

        if (finalProb < pick('MIN_ENTRY_PROB')) {
            return this.reject('PROB_TOO_LOW', { finalProb, min: pick('MIN_ENTRY_PROB') });
        }

        const gap = Math.abs(probLong - probShort);
        if (gap < pick('MIN_PROB_GAP')) {
            return this.reject('PROB_GAP_TOO_SMALL', { probLong, probShort, gap });
        }

        if (action === 'LONG' && probLong < probShort) {
            return this.reject('LONG_NOT_DOMINANT', { probLong, probShort });
        }

        if (action === 'SHORT' && probShort < probLong) {
            return this.reject('SHORT_NOT_DOMINANT', { probLong, probShort });
        }

        return this.allow();
    }

    checkExposure(context = {}) {
        const activeTrades = this.safeNumber(context.activeTrades, 0);
        const maxActiveTrades = this.safeNumber(context.maxActiveTrades, pick('MAX_ACTIVE_TRADES'));
        const leverage = this.safeNumber(context.leverage, 1);

        if (activeTrades >= Math.min(maxActiveTrades, pick('MAX_ACTIVE_TRADES'))) {
            return this.reject('MAX_ACTIVE_TRADES_REACHED', { activeTrades, maxActiveTrades });
        }

        if (leverage > pick('MAX_LEVERAGE_PER_TRADE')) {
            return this.reject('LEVERAGE_TOO_HIGH', { leverage, max: pick('MAX_LEVERAGE_PER_TRADE') });
        }

        return this.allow();
    }

    async checkFrequency(symbol) {
        const hourMs = 60 * 60 * 1000;
        const totalKey = pick('TRADE_LOG_REDIS_KEY');
        const symbolKey = `${pick('SYMBOL_TRADE_LOG_PREFIX')}:${symbol}`;

        const totalCount = await this.countTradesInWindow(totalKey, hourMs);
        if (totalCount >= pick('MAX_TRADES_PER_HOUR')) {
            return this.reject('MAX_TRADES_PER_HOUR_REACHED', { totalCount, max: pick('MAX_TRADES_PER_HOUR') });
        }

        const symbolCount = await this.countTradesInWindow(symbolKey, hourMs);
        if (symbolCount >= pick('MAX_TRADES_PER_SYMBOL_PER_HOUR')) {
            return this.reject('MAX_TRADES_PER_SYMBOL_PER_HOUR_REACHED', { symbolCount, max: pick('MAX_TRADES_PER_SYMBOL_PER_HOUR') });
        }

        return this.allow();
    }

    async checkLossLimits() {
        const dailyPnl = await this.getDailyPnl();
        if (dailyPnl <= -Math.abs(pick('MAX_DAILY_LOSS_USDT'))) {
            return this.reject('MAX_DAILY_LOSS_USDT_REACHED', { dailyPnl, maxLoss: pick('MAX_DAILY_LOSS_USDT') });
        }

        const consecutiveLosses = await this.getConsecutiveLosses();
        if (consecutiveLosses >= pick('MAX_CONSECUTIVE_LOSSES')) {
            return this.reject('MAX_CONSECUTIVE_LOSSES_REACHED', { consecutiveLosses, max: pick('MAX_CONSECUTIVE_LOSSES') });
        }

        return this.allow();
    }

    // ========================================================
    // 3. PUBLIC PRE-TRADE CHECK
    // ========================================================
    async canOpenTrade(symbol, action, feature, context = {}) {
        if (!this.enabled) return this.allow();

        symbol = this.normalizeSymbol(symbol);
        action = String(action || '').toUpperCase();

        if (!symbol) return this.reject('NO_SYMBOL');
        if (!['LONG', 'SHORT'].includes(action)) return this.reject('BAD_ACTION', { action });

        if (await this.isPaused()) return this.reject('TRADING_PAUSED');
        if (await this.isSymbolBlocked(symbol)) return this.reject('SYMBOL_TEMP_BLOCKED');

        const checks = [
            this.checkSymbolPolicy(symbol),
            this.checkFeatureHealth(feature),
            this.checkModelConfidence(action, context),
            this.checkExposure(context)
        ];

        for (const result of checks) {
            if (!result.allowed) {
                await this.blockSymbol(symbol, pick('SYMBOL_COOLDOWN_AFTER_BLOCK_MS'), result.reason);
                return result;
            }
        }

        const frequency = await this.checkFrequency(symbol);
        if (!frequency.allowed) return frequency;

        const lossLimits = await this.checkLossLimits();
        if (!lossLimits.allowed) return lossLimits;

        return this.allow();
    }

    // Alias để tương thích nếu OrderManager gọi check().
    async check(symbol, action, feature, context = {}) {
        return this.canOpenTrade(symbol, action, feature, context);
    }

    // ========================================================
    // 4. POST-TRADE ACCOUNTING HOOKS
    // --------------------------------------------------------
    // Các hàm này nên được gọi bởi OrderManager/PaperExchange/LiveExchange
    // sau khi lệnh mở/đóng để RiskGuard có dữ liệu trade frequency và loss.
    // Nếu chưa gọi, RiskGuard vẫn chặn bằng dữ liệu realtime nhưng thiếu daily loss.
    // ========================================================
    async recordTradeOpened(symbol) {
        symbol = this.normalizeSymbol(symbol);
        if (!symbol) return;

        const now = Date.now();
        const totalKey = pick('TRADE_LOG_REDIS_KEY');
        const symbolKey = `${pick('SYMBOL_TRADE_LOG_PREFIX')}:${symbol}`;
        const ttlSec = 2 * 60 * 60;

        try {
            await this.redis.multi()
                .zadd(totalKey, now, `${symbol}:${now}`)
                .expire(totalKey, ttlSec)
                .zadd(symbolKey, now, `${symbol}:${now}`)
                .expire(symbolKey, ttlSec)
                .exec();
        } catch (e) {}
    }

    async recordTradeClosed(symbol, pnl = 0) {
        symbol = this.normalizeSymbol(symbol);
        const pnlValue = this.safeNumber(pnl, 0);

        try {
            await this.redis.incrbyfloat(pick('DAILY_PNL_REDIS_KEY'), pnlValue);

            // Daily key nên expire hơn 1 ngày để tự reset gần đúng.
            await this.redis.expire(pick('DAILY_PNL_REDIS_KEY'), 30 * 60 * 60);

            if (pnlValue < 0) {
                await this.redis.incr(pick('CONSECUTIVE_LOSS_REDIS_KEY'));
            } else if (pnlValue > 0) {
                await this.redis.set(pick('CONSECUTIVE_LOSS_REDIS_KEY'), '0');
            }

            await this.redis.expire(pick('CONSECUTIVE_LOSS_REDIS_KEY'), 30 * 60 * 60);
        } catch (e) {}
    }

    async pauseTrading(reason = 'manual') {
        await this.redis.set(pick('PAUSE_REDIS_KEY'), 'PAUSED');
        console.log(`⏸️ [RiskGuard] Trading paused | ${reason}`);
    }

    async resumeTrading() {
        await this.redis.del(pick('PAUSE_REDIS_KEY'));
        console.log('▶️ [RiskGuard] Trading resumed');
    }
}

module.exports = new RiskGuard();
