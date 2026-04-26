const fs = require('fs');
const path = require('path');
const Redis = require('ioredis');
const mongoose = require('mongoose');
const ort = require('onnxruntime-node');
require('dotenv').config({ path: path.join(__dirname, '../../../.env') });

const exchangeRules = require('../binance/ExchangeRules');

// ============================================================
// SYSTEM HEALTH CHECK / PRE-LIVE CHECKLIST V1
// ------------------------------------------------------------
// Vai trò:
// 1. Chạy trước khi bật bot paper/live.
// 2. Kiểm tra cấu hình hệ thống có đủ để chạy không.
// 3. Chặn live thật nếu thiếu Redis/Mongo/API/ONNX/UserDataStream/ExchangeRules.
// 4. Kiểm tra dry-run/live mode có mâu thuẫn không.
// 5. Kiểm tra FeatureRecorder có đang lưu dữ liệu không.
// 6. Trả exit code:
//    - 0: PASS
//    - 1: FAIL critical
//
// Cách chạy:
// node src/services/ops/SystemHealthCheck.js
//
// Gợi ý chạy trước bot:
// node src/services/ops/SystemHealthCheck.js && node src/services/execution/OrderManager.js
// ============================================================

const configPath = path.join(__dirname, '../../../system_config.json');
const modelPath = path.join(__dirname, '../../../Universal_Scout.onnx');

let config = {};
try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    // Để checkConfig báo lỗi rõ hơn.
}

const REDIS_URL = config.REDIS_URL || process.env.REDIS_URL || 'redis://localhost:6379';
const MONGO_URI = process.env.MONGO_URI_SCOUT;
const EXCHANGE_MODE = String(process.env.EXCHANGE_MODE || config.TRADING?.EXCHANGE_MODE || 'PAPER').toUpperCase();
const LIVE_DRY_RUN = String(process.env.LIVE_DRY_RUN || 'true') === 'true';
const FEATURE_ARCHIVE_PREFIX = process.env.FEATURE_ARCHIVE_PREFIX || 'features:archive';

const CHECK_FEATURE_ARCHIVE = process.env.HEALTH_CHECK_FEATURE_ARCHIVE !== 'false';
const CHECK_USER_STREAM = process.env.HEALTH_CHECK_USER_STREAM === 'true';
const CHECK_SAMPLE_SYMBOL = process.env.HEALTH_CHECK_SAMPLE_SYMBOL || 'BTCUSDT';
const MIN_ARCHIVE_ENTRIES = Number(process.env.HEALTH_MIN_ARCHIVE_ENTRIES || 1);

class SystemHealthCheck {
    constructor() {
        this.results = [];
        this.redis = null;
    }

    pass(name, detail = '') {
        this.results.push({ level: 'PASS', name, detail });
        console.log(`✅ [PASS] ${name}${detail ? ` | ${detail}` : ''}`);
    }

    warn(name, detail = '') {
        this.results.push({ level: 'WARN', name, detail });
        console.warn(`⚠️ [WARN] ${name}${detail ? ` | ${detail}` : ''}`);
    }

    fail(name, detail = '') {
        this.results.push({ level: 'FAIL', name, detail });
        console.error(`❌ [FAIL] ${name}${detail ? ` | ${detail}` : ''}`);
    }

    hasFail() {
        return this.results.some(r => r.level === 'FAIL');
    }

    // ========================================================
    // 1. CONFIG CHECKS
    // ========================================================
    async checkConfig() {
        if (!fs.existsSync(configPath)) {
            this.fail('system_config.json exists', `Missing: ${configPath}`);
            return;
        }
        this.pass('system_config.json exists');

        if (!config.REDIS_URL && !process.env.REDIS_URL) {
            this.warn('REDIS_URL configured', 'Missing REDIS_URL, fallback redis://localhost:6379');
        } else {
            this.pass('REDIS_URL configured');
        }

        if (!MONGO_URI) {
            this.fail('MONGO_URI_SCOUT configured', 'Missing MONGO_URI_SCOUT in .env');
        } else {
            this.pass('MONGO_URI_SCOUT configured');
        }

        if (!['PAPER', 'LIVE_DRY_RUN', 'LIVE'].includes(EXCHANGE_MODE)) {
            this.fail('EXCHANGE_MODE valid', `EXCHANGE_MODE=${EXCHANGE_MODE}`);
        } else {
            this.pass('EXCHANGE_MODE valid', EXCHANGE_MODE);
        }

        if (EXCHANGE_MODE === 'LIVE' && LIVE_DRY_RUN) {
            this.fail('LIVE mode safety', 'EXCHANGE_MODE=LIVE but LIVE_DRY_RUN=true. Set LIVE_DRY_RUN=false explicitly only when ready.');
        } else if (EXCHANGE_MODE === 'LIVE_DRY_RUN' && !LIVE_DRY_RUN) {
            this.fail('LIVE_DRY_RUN mode safety', 'EXCHANGE_MODE=LIVE_DRY_RUN but LIVE_DRY_RUN=false');
        } else {
            this.pass('Live/dry-run mode consistency', `EXCHANGE_MODE=${EXCHANGE_MODE}, LIVE_DRY_RUN=${LIVE_DRY_RUN}`);
        }

        if (EXCHANGE_MODE === 'LIVE' || EXCHANGE_MODE === 'LIVE_DRY_RUN') {
            if (!process.env.BINANCE_API_KEY || !process.env.BINANCE_API_SECRET) {
                this.fail('Binance API keys configured', 'Required for LIVE/LIVE_DRY_RUN');
            } else {
                this.pass('Binance API keys configured');
            }
        } else {
            this.pass('Binance API keys optional in PAPER');
        }
    }

    // ========================================================
    // 2. REDIS
    // ========================================================
    async checkRedis() {
        try {
            this.redis = new Redis(REDIS_URL, { lazyConnect: true });
            await this.redis.connect();
            const pong = await this.redis.ping();
            if (pong === 'PONG') this.pass('Redis connection', REDIS_URL);
            else this.fail('Redis connection', `Unexpected ping response: ${pong}`);
        } catch (error) {
            this.fail('Redis connection', error.message);
        }
    }

    // ========================================================
    // 3. MONGO
    // ========================================================
    async checkMongo() {
        if (!MONGO_URI) return;

        try {
            if (mongoose.connection.readyState !== 1) {
                await mongoose.connect(MONGO_URI, { serverSelectionTimeoutMS: 10000 });
            }
            const state = mongoose.connection.readyState;
            if (state === 1) this.pass('MongoDB connection');
            else this.fail('MongoDB connection', `readyState=${state}`);
        } catch (error) {
            this.fail('MongoDB connection', error.message);
        }
    }

    // ========================================================
    // 4. ONNX MODEL
    // ========================================================
    async checkOnnx() {
        if (!fs.existsSync(modelPath)) {
            this.fail('Universal_Scout.onnx exists', `Missing: ${modelPath}`);
            return;
        }

        this.pass('Universal_Scout.onnx exists');

        try {
            const session = await ort.InferenceSession.create(modelPath);
            const inputNames = session.inputNames || [];
            const outputNames = session.outputNames || [];

            if (inputNames.length === 0 || outputNames.length === 0) {
                this.fail('ONNX input/output names', `inputs=${inputNames.length}, outputs=${outputNames.length}`);
            } else {
                this.pass('ONNX loadable', `input=${inputNames[0]}, output=${outputNames[outputNames.length - 1]}`);
            }
        } catch (error) {
            this.fail('ONNX loadable', error.message);
        }
    }

    // ========================================================
    // 5. BINANCE EXCHANGE RULES
    // ========================================================
    async checkExchangeRules() {
        try {
            await exchangeRules.ensureReady();
            const symbols = exchangeRules.getAllTradableSymbols();
            if (!symbols || symbols.length === 0) {
                this.fail('ExchangeRules exchangeInfo', 'No tradable symbols loaded');
            } else {
                this.pass('ExchangeRules exchangeInfo', `${symbols.length} tradable symbols`);
            }

            const btc = exchangeRules.getSymbolRules(CHECK_SAMPLE_SYMBOL);
            if (!btc) {
                this.warn('Sample symbol rules', `No rules for ${CHECK_SAMPLE_SYMBOL}`);
            } else {
                this.pass('Sample symbol rules', `${CHECK_SAMPLE_SYMBOL} tick=${btc.tickSize}, step=${btc.stepSize}, minNotional=${btc.minNotional}`);
            }

            if (EXCHANGE_MODE === 'LIVE' || EXCHANGE_MODE === 'LIVE_DRY_RUN') {
                try {
                    await exchangeRules.syncLeverageBrackets(true);
                    const maxLev = exchangeRules.getMaxLeverage(CHECK_SAMPLE_SYMBOL, 10);
                    this.pass('Leverage brackets sync', `${CHECK_SAMPLE_SYMBOL} maxLev=${maxLev}`);
                } catch (error) {
                    this.fail('Leverage brackets sync', error.message);
                }
            }
        } catch (error) {
            this.fail('ExchangeRules exchangeInfo', error.message);
        }
    }

    // ========================================================
    // 6. FEATURE ARCHIVE / RECORDER
    // ========================================================
    async checkFeatureArchive() {
        if (!CHECK_FEATURE_ARCHIVE) {
            this.warn('Feature archive check skipped');
            return;
        }

        if (!this.redis) {
            this.warn('Feature archive check', 'Redis unavailable');
            return;
        }

        try {
            const key = `${FEATURE_ARCHIVE_PREFIX}:${CHECK_SAMPLE_SYMBOL}`;
            const len = await this.redis.xlen(key);
            if (len >= MIN_ARCHIVE_ENTRIES) {
                this.pass('Feature archive stream', `${key} entries=${len}`);
            } else {
                this.warn('Feature archive stream', `${key} entries=${len}. FeatureRecorder may not be running yet.`);
            }
        } catch (error) {
            this.warn('Feature archive stream', error.message);
        }
    }

    // ========================================================
    // 7. USER DATA STREAM OPTIONAL CHECK
    // ========================================================
    async checkUserDataStream() {
        if (!CHECK_USER_STREAM) {
            this.warn('UserDataStream live check skipped', 'Set HEALTH_CHECK_USER_STREAM=true to test private stream');
            return;
        }

        if (!(EXCHANGE_MODE === 'LIVE' || EXCHANGE_MODE === 'LIVE_DRY_RUN')) {
            this.warn('UserDataStream check skipped', 'Not in LIVE/LIVE_DRY_RUN');
            return;
        }

        try {
            const BinanceUserDataStream = require('../binance/BinanceUserDataStream');
            const stream = new BinanceUserDataStream();
            await stream.start();
            const state = stream.getState();
            if (state && state.assets) {
                this.pass('UserDataStream start', `assets=${Object.keys(state.assets).length}, positions=${Object.keys(state.positions || {}).length}`);
            } else {
                this.fail('UserDataStream start', 'No account state');
            }
            await stream.stop();
        } catch (error) {
            this.fail('UserDataStream start', error.message);
        }
    }

    // ========================================================
    // 8. RISK CONFIG
    // ========================================================
    async checkRiskConfig() {
        const risk = config.RISK || {};
        const trading = config.TRADING || {};

        const maxActive = Number(trading.MAX_ACTIVE_TRADES || 3);
        if (EXCHANGE_MODE === 'LIVE' && maxActive > 1) {
            this.warn('Live max active trades', `MAX_ACTIVE_TRADES=${maxActive}. For first live run, recommend 1.`);
        } else {
            this.pass('Max active trades config', `MAX_ACTIVE_TRADES=${maxActive}`);
        }

        const dailyLoss = Number(risk.MAX_DAILY_LOSS_USDT || 10);
        if (EXCHANGE_MODE === 'LIVE' && dailyLoss > 5) {
            this.warn('Live daily loss limit', `MAX_DAILY_LOSS_USDT=${dailyLoss}. For first live run, recommend <= 3-5 USDT.`);
        } else {
            this.pass('Daily loss limit config', `MAX_DAILY_LOSS_USDT=${dailyLoss}`);
        }
    }

    // ========================================================
    // 9. RUN ALL
    // ========================================================
    async run() {
        console.log('🧪 [SystemHealthCheck] Starting pre-live checklist...');
        console.log(`⚙️ mode=${EXCHANGE_MODE}, dryRun=${LIVE_DRY_RUN}, sample=${CHECK_SAMPLE_SYMBOL}`);

        await this.checkConfig();
        await this.checkRedis();
        await this.checkMongo();
        await this.checkOnnx();
        await this.checkExchangeRules();
        await this.checkFeatureArchive();
        await this.checkUserDataStream();
        await this.checkRiskConfig();

        await this.close();

        const fails = this.results.filter(r => r.level === 'FAIL').length;
        const warns = this.results.filter(r => r.level === 'WARN').length;
        const passes = this.results.filter(r => r.level === 'PASS').length;

        console.log('\n============================================================');
        console.log(`📊 [SystemHealthCheck] PASS=${passes} WARN=${warns} FAIL=${fails}`);
        console.log('============================================================');

        if (fails > 0) {
            console.error('❌ [SystemHealthCheck] FAILED. Không nên chạy bot/live cho đến khi sửa lỗi FAIL.');
            return false;
        }

        if (warns > 0) {
            console.warn('⚠️ [SystemHealthCheck] PASSED WITH WARNINGS. Có thể chạy PAPER/DRY_RUN, nhưng xem lại warning trước LIVE thật.');
        } else {
            console.log('✅ [SystemHealthCheck] ALL CLEAR.');
        }

        return true;
    }

    async close() {
        try { if (this.redis) await this.redis.quit(); } catch (e) {}
        try { if (mongoose.connection.readyState !== 0) await mongoose.disconnect(); } catch (e) {}
    }
}

if (require.main === module) {
    const checker = new SystemHealthCheck();
    checker.run()
        .then(ok => process.exit(ok ? 0 : 1))
        .catch(async err => {
            console.error('❌ [SystemHealthCheck FATAL]', err.message);
            await checker.close();
            process.exit(1);
        });
}

module.exports = SystemHealthCheck;
