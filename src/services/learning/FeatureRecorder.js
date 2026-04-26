const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const fs = require('fs');
const path = require('path');

// ============================================================
// FEATURE RECORDER V1 - LEARNING BUFFER FOR NIGHTLY ONNX TRAINING
// ------------------------------------------------------------
// Vai trò:
// 1. Nghe toàn bộ market:features:* từ FeedHandler.
// 2. Lưu snapshot feature theo timestamp vào Redis Stream.
// 3. Giữ lại dữ liệu đủ lâu để mỗi đêm lấy cửa sổ:
//    - 60 giây trước khi vào lệnh
//    - thời gian trong lệnh
//    - 60 giây sau khi thoát lệnh
// 4. Không ảnh hưởng OrderManager vì chỉ nghe thụ động.
// 5. Giữ schema compact để training nightly đọc nhanh.
//
// Vì sao cần file này?
// - ScoutTrade chỉ lưu featuresAtEntry/featuresAtExit.
// - Nhưng để bot tự học tốt hơn, cần chuỗi diễn biến trước/sau lệnh.
// - Nếu không lưu realtime stream, ban đêm không thể khôi phục chính xác dữ liệu trước lệnh.
//
// Redis Stream key:
// features:archive:BTCUSDT
// features:archive:PEPEUSDT
//
// Stream ID dùng dạng timestamp thật:
// 1712345678901-0
//
// Query nightly ví dụ:
// XRANGE features:archive:PEPEUSDT 1712345618901-0 1712345738901-999
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
const FEATURE_PATTERN = config.CHANNELS?.FEATURES || process.env.FEATURE_PATTERN || 'market:features:*';

// Mặc định lưu 3 ngày để mỗi đêm train vẫn có buffer nếu job lỗi 1 ngày.
const ARCHIVE_TTL_SEC = Number(process.env.FEATURE_ARCHIVE_TTL_SEC || 3 * 24 * 60 * 60);

// Lấy mẫu 1 giây/lần/symbol để không làm Redis phình quá mạnh.
// Nếu muốn train HFT dày hơn, có thể giảm xuống 250ms.
const SAMPLE_INTERVAL_MS = Number(process.env.FEATURE_ARCHIVE_SAMPLE_INTERVAL_MS || 1000);

// Max entries mỗi symbol. 3 ngày ở 1s ≈ 259200 entry/symbol.
// Dùng MAXLEN ~ để Redis tự trim gần đúng, tránh stream tăng vô hạn.
const STREAM_MAXLEN = Number(process.env.FEATURE_ARCHIVE_MAXLEN || 300000);

// Prefix Redis Stream.
const ARCHIVE_PREFIX = process.env.FEATURE_ARCHIVE_PREFIX || 'features:archive';

// Có lưu đủ feature object hay chỉ compact fields.
const STORE_FULL_FEATURE = process.env.FEATURE_ARCHIVE_STORE_FULL === 'true';

class FeatureRecorder {
    constructor() {
        this.subClient = new Redis(REDIS_URL);
        this.dataClient = new Redis(REDIS_URL);

        this.lastSavedAt = new Map();
        this.savedCount = 0;
        this.skippedCount = 0;
        this.errorCount = 0;

        this.subClient.on('error', err => {
            console.error('❌ [FeatureRecorder subClient]', err.message);
        });

        this.dataClient.on('error', err => {
            console.error('❌ [FeatureRecorder dataClient]', err.message);
        });
    }

    normalizeSymbol(symbol) {
        return String(symbol || '').toUpperCase().trim();
    }

    safeNumber(value, fallback = 0) {
        const n = Number(value);
        return Number.isFinite(n) ? n : fallback;
    }

    shouldSave(symbol, ts) {
        const last = this.lastSavedAt.get(symbol) || 0;
        if (ts - last < SAMPLE_INTERVAL_MS) {
            this.skippedCount += 1;
            return false;
        }
        this.lastSavedAt.set(symbol, ts);
        return true;
    }

    buildCompactFeature(feature, ts) {
        const symbol = this.normalizeSymbol(feature.symbol);

        // Giữ các field cần cho training và audit.
        // 13 ONNX input + các field live-parity + indicator quản lý lệnh.
        return {
            ts,
            symbol,
            is_warm: Boolean(feature.is_warm),

            best_ask: this.safeNumber(feature.best_ask),
            best_bid: this.safeNumber(feature.best_bid),
            last_price: this.safeNumber(feature.last_price),
            mark_price: this.safeNumber(feature.mark_price, feature.last_price),
            index_price: this.safeNumber(feature.index_price, feature.mark_price || feature.last_price),
            next_funding_time: Number(feature.next_funding_time || 0),

            // 13 input ONNX cố định.
            ob_imb_top20: this.safeNumber(feature.ob_imb_top20),
            spread_close: this.safeNumber(feature.spread_close),
            bid_vol_1pct: this.safeNumber(feature.bid_vol_1pct),
            ask_vol_1pct: this.safeNumber(feature.ask_vol_1pct),
            max_buy_trade: this.safeNumber(feature.max_buy_trade),
            max_sell_trade: this.safeNumber(feature.max_sell_trade),
            liq_long_vol: this.safeNumber(feature.liq_long_vol),
            liq_short_vol: this.safeNumber(feature.liq_short_vol),
            funding_rate: this.safeNumber(feature.funding_rate),
            taker_buy_ratio: this.safeNumber(feature.taker_buy_ratio, 0.5),
            body_size: this.safeNumber(feature.body_size),
            wick_size: this.safeNumber(feature.wick_size),
            btc_relative_strength: this.safeNumber(feature.btc_relative_strength),

            // Các field quản trị lệnh / risk.
            ATR14: this.safeNumber(feature.ATR14),
            VPIN: this.safeNumber(feature.VPIN),
            OFI: this.safeNumber(feature.OFI),
            MFA: this.safeNumber(feature.MFA),
            WHALE_NET: this.safeNumber(feature.WHALE_NET),

            // Data age audit.
            mark_price_age_ms: feature.mark_price_age_ms ?? null,
            depth_age_ms: feature.depth_age_ms ?? null,
            price_age_ms: feature.price_age_ms ?? null
        };
    }

    async saveFeature(feature) {
        const ts = Date.now();
        const symbol = this.normalizeSymbol(feature.symbol);
        if (!symbol) return;

        if (!this.shouldSave(symbol, ts)) return;

        const streamKey = `${ARCHIVE_PREFIX}:${symbol}`;
        const payload = STORE_FULL_FEATURE
            ? { ...feature, ts, symbol }
            : this.buildCompactFeature(feature, ts);

        const json = JSON.stringify(payload);
        const streamId = `${ts}-0`;

        try {
            await this.dataClient
                .multi()
                .xadd(streamKey, 'MAXLEN', '~', STREAM_MAXLEN, streamId, 'data', json)
                .expire(streamKey, ARCHIVE_TTL_SEC)
                .exec();

            this.savedCount += 1;
        } catch (error) {
            // Nếu stream ID trùng trong cùng 1ms, retry với * để Redis tự cấp sequence.
            try {
                await this.dataClient
                    .multi()
                    .xadd(streamKey, 'MAXLEN', '~', STREAM_MAXLEN, '*', 'data', json)
                    .expire(streamKey, ARCHIVE_TTL_SEC)
                    .exec();

                this.savedCount += 1;
            } catch (retryError) {
                this.errorCount += 1;
                console.error(`❌ [FeatureRecorder] Lỗi lưu ${symbol}:`, retryError.message);
            }
        }
    }

    async handleMessage(messageBuffer) {
        let feature;

        try {
            feature = decode(messageBuffer);
        } catch (error) {
            this.errorCount += 1;
            return;
        }

        if (!feature || !feature.symbol) return;
        await this.saveFeature(feature);
    }

    startStatsLogger() {
        setInterval(() => {
            console.log(
                `📦 [FeatureRecorder] saved=${this.savedCount} skipped=${this.skippedCount} errors=${this.errorCount} sample=${SAMPLE_INTERVAL_MS}ms ttl=${ARCHIVE_TTL_SEC}s`
            );
        }, 60 * 1000);
    }

    async run() {
        console.log('🧠 [FeatureRecorder] Khởi động learning buffer...');
        console.log(`⚙️ [FeatureRecorder] pattern=${FEATURE_PATTERN}`);
        console.log(`⚙️ [FeatureRecorder] prefix=${ARCHIVE_PREFIX}, sample=${SAMPLE_INTERVAL_MS}ms, ttl=${ARCHIVE_TTL_SEC}s, maxLen=${STREAM_MAXLEN}`);

        await this.subClient.psubscribe(FEATURE_PATTERN);
        console.log(`✅ [FeatureRecorder] Đang nghe ${FEATURE_PATTERN}`);

        this.startStatsLogger();

        this.subClient.on('pmessageBuffer', async (pattern, channel, messageBuffer) => {
            try {
                await this.handleMessage(messageBuffer);
            } catch (error) {
                this.errorCount += 1;
                console.error('❌ [FeatureRecorder] handleMessage error:', error.message);
            }
        });
    }
}

if (require.main === module) {
    const recorder = new FeatureRecorder();
    recorder.run().catch(err => {
        console.error('❌ [FeatureRecorder FATAL]', err.message);
        process.exit(1);
    });
}

module.exports = FeatureRecorder;
