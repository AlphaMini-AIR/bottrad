const fs = require('fs');
const path = require('path');
const Redis = require('ioredis');
const mongoose = require('mongoose');
require('dotenv').config({ path: path.join(__dirname, '../../../.env') });

const ScoutTrade = require('../../models/ScoutTrade');

// ============================================================
// NIGHTLY TRAINING DATASET BUILDER V1
// ------------------------------------------------------------
// Vai trò:
// 1. Chạy mỗi đêm hoặc chạy thủ công sau phiên paper/live.
// 2. Lấy các lệnh đã CLOSED/LIQUIDATED từ MongoDB.
// 3. Với mỗi lệnh, lấy feature timeline từ Redis Stream:
//    features:archive:SYMBOL
//    trong cửa sổ: openTs - 60s  →  closeTs + 60s
// 4. Tạo sample học gồm:
//    - trade metadata
//    - featureAtEntry / featureAtExit
//    - featureWindowBeforeEntry
//    - featureWindowDuringTrade
//    - featureWindowAfterExit
//    - label gợi ý cho ONNX
// 5. Xuất ra file JSONL để pipeline train Python đọc.
//
// File output mặc định:
// data/training/nightly/scout_dataset_YYYY-MM-DD.jsonl
//
// Lý do cần file này:
// - ScoutTrade chỉ lưu snapshot entry/exit.
// - FeatureRecorder lưu chuỗi feature realtime.
// - Builder này ghép 2 nguồn lại thành dữ liệu học sâu hơn.
// ============================================================

const configPath = path.join(__dirname, '../../../system_config.json');
let config;
try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    config = { REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379' };
}

const REDIS_URL = config.REDIS_URL || process.env.REDIS_URL || 'redis://localhost:6379';
const MONGO_URI = process.env.MONGO_URI_SCOUT;

const ARCHIVE_PREFIX = process.env.FEATURE_ARCHIVE_PREFIX || 'features:archive';
const OUTPUT_DIR = process.env.TRAINING_DATASET_OUTPUT_DIR || path.join(__dirname, '../../../data/training/nightly');

// Cửa sổ mặc định: lấy trước entry 60s, sau exit 60s.
const PRE_ENTRY_MS = Number(process.env.TRAINING_PRE_ENTRY_MS || 60 * 1000);
const POST_EXIT_MS = Number(process.env.TRAINING_POST_EXIT_MS || 60 * 1000);

// Số trade tối đa mỗi lần build để tránh chạy quá nặng.
const MAX_TRADES_PER_RUN = Number(process.env.TRAINING_MAX_TRADES_PER_RUN || 2000);

// Chỉ lấy lệnh đóng gần đây nếu muốn.
// Mặc định 36h để bao phủ phiên đêm trước và tránh miss nếu job chạy lệch giờ.
const LOOKBACK_HOURS = Number(process.env.TRAINING_LOOKBACK_HOURS || 36);

// Nếu true, chỉ build trade chưa trained.
const ONLY_UNTRAINED = process.env.TRAINING_ONLY_UNTRAINED !== 'false';

// Label thresholds.
const WIN_ROI_THRESHOLD = Number(process.env.TRAINING_WIN_ROI_THRESHOLD || 0.2); // ROI > 0.2% xem là win tối thiểu
const LOSS_ROI_THRESHOLD = Number(process.env.TRAINING_LOSS_ROI_THRESHOLD || -0.2);
const GOOD_MFE_THRESHOLD = Number(process.env.TRAINING_GOOD_MFE_THRESHOLD || 0.5);
const BAD_MAE_THRESHOLD = Number(process.env.TRAINING_BAD_MAE_THRESHOLD || -0.5);

class NightlyTrainingDatasetBuilder {
    constructor() {
        this.redis = new Redis(REDIS_URL);
        this.outputDir = OUTPUT_DIR;
        this.stats = {
            tradesLoaded: 0,
            samplesWritten: 0,
            skippedNoWindow: 0,
            skippedBadTrade: 0,
            errors: 0
        };
    }

    normalizeSymbol(symbol) {
        return String(symbol || '').toUpperCase().trim();
    }

    safeNumber(value, fallback = 0) {
        const n = Number(value);
        return Number.isFinite(n) ? n : fallback;
    }

    ensureOutputDir() {
        fs.mkdirSync(this.outputDir, { recursive: true });
    }

    getOutputFilePath() {
        const d = new Date();
        const yyyy = d.getFullYear();
        const mm = String(d.getMonth() + 1).padStart(2, '0');
        const dd = String(d.getDate()).padStart(2, '0');
        return path.join(this.outputDir, `scout_dataset_${yyyy}-${mm}-${dd}.jsonl`);
    }

    async connectMongo() {
        if (!MONGO_URI) {
            throw new Error('Missing MONGO_URI_SCOUT in .env');
        }

        if (mongoose.connection.readyState === 1) return;

        await mongoose.connect(MONGO_URI);
        console.log('📦 [DatasetBuilder] Mongo connected');
    }

    buildTradeQuery() {
        const since = new Date(Date.now() - LOOKBACK_HOURS * 60 * 60 * 1000);

        const query = {
            status: { $in: ['CLOSED', 'LIQUIDATED'] },
            closeTime: { $gte: since },
            openTs: { $exists: true, $gt: 0 },
            closeTs: { $exists: true, $gt: 0 }
        };

        if (ONLY_UNTRAINED) {
            query.trained = { $ne: true };
        }

        return query;
    }

    async loadTrades() {
        const query = this.buildTradeQuery();

        const trades = await ScoutTrade.find(query)
            .sort({ closeTime: 1 })
            .limit(MAX_TRADES_PER_RUN)
            .lean();

        this.stats.tradesLoaded = trades.length;
        console.log(`📚 [DatasetBuilder] Loaded ${trades.length} trades`);
        return trades;
    }

    parseStreamEntry(entry) {
        // ioredis XRANGE trả dạng: [id, [field, value, field, value]]
        const [id, fields] = entry;
        let raw = null;

        for (let i = 0; i < fields.length; i += 2) {
            const key = fields[i];
            const value = fields[i + 1];
            if (key === 'data') {
                raw = value;
                break;
            }
        }

        if (!raw) return null;

        try {
            const parsed = JSON.parse(raw);
            if (!parsed.ts) {
                const idTs = Number(String(id).split('-')[0]);
                parsed.ts = idTs;
            }
            return parsed;
        } catch (error) {
            return null;
        }
    }

    async loadFeatureWindow(symbol, startTs, endTs) {
        const streamKey = `${ARCHIVE_PREFIX}:${symbol}`;
        const start = `${startTs}-0`;
        const end = `${endTs}-999999`;

        const entries = await this.redis.xrange(streamKey, start, end);
        const features = [];

        for (const entry of entries) {
            const parsed = this.parseStreamEntry(entry);
            if (parsed) features.push(parsed);
        }

        features.sort((a, b) => a.ts - b.ts);
        return features;
    }

    splitWindow(features, openTs, closeTs) {
        const before = [];
        const during = [];
        const after = [];

        for (const f of features) {
            if (f.ts < openTs) before.push(f);
            else if (f.ts <= closeTs) during.push(f);
            else after.push(f);
        }

        return { before, during, after };
    }

    // ========================================================
    // LABELING LOGIC V1
    // --------------------------------------------------------
    // Đây là label gợi ý ban đầu, không ép bạn train y chang.
    // Có thể thay đổi sau khi quan sát dữ liệu.
    //
    // Output:
    // - label_binary: WIN / LOSS / NEUTRAL
    // - label_direction: LONG_WIN / SHORT_WIN / LONG_LOSS / SHORT_LOSS / NEUTRAL
    // - label_numeric:
    //    LONG win  = 1
    //    SHORT win = 0
    //    neutral   = 0.5
    //
    // Nếu sau này model 3-class:
    // - NO_TRADE = 0
    // - SHORT    = 1
    // - LONG     = 2
    // ========================================================
    buildLabels(trade) {
        const roi = this.safeNumber(trade.roi, 0);
        const pnl = this.safeNumber(trade.pnl ?? trade.netPnl, 0);
        const mfe = this.safeNumber(trade.mfe, 0);
        const mae = this.safeNumber(trade.mae, 0);
        const type = trade.type === 'SHORT' ? 'SHORT' : 'LONG';

        let labelBinary = 'NEUTRAL';

        if (roi >= WIN_ROI_THRESHOLD && mfe >= GOOD_MFE_THRESHOLD) {
            labelBinary = 'WIN';
        } else if (roi <= LOSS_ROI_THRESHOLD || mae <= BAD_MAE_THRESHOLD) {
            labelBinary = 'LOSS';
        }

        let labelDirection = 'NEUTRAL';
        if (labelBinary === 'WIN') labelDirection = `${type}_WIN`;
        if (labelBinary === 'LOSS') labelDirection = `${type}_LOSS`;

        let labelNumeric = 0.5;
        if (labelBinary === 'WIN') {
            labelNumeric = type === 'LONG' ? 1 : 0;
        }

        return {
            label_binary: labelBinary,
            label_direction: labelDirection,
            label_numeric: labelNumeric,
            label_quality: {
                roi,
                pnl,
                mfe,
                mae,
                durationMs: this.safeNumber(trade.durationMs, 0)
            }
        };
    }

    compactTrade(trade) {
        return {
            id: String(trade._id),
            symbol: this.normalizeSymbol(trade.symbol),
            type: trade.type,
            mode: trade.mode,
            status: trade.status,
            openTs: trade.openTs,
            closeTs: trade.closeTs,
            durationMs: trade.durationMs,
            entryPrice: trade.entryPrice,
            closePrice: trade.closePrice,
            executedEntryPrice: trade.executedEntryPrice,
            executedClosePrice: trade.executedClosePrice,
            leverage: trade.leverage,
            margin: trade.margin,
            size: trade.size,
            notionalValue: trade.notionalValue,
            prob: trade.prob,
            probLong: trade.probLong,
            probShort: trade.probShort,
            macroScore: trade.macroScore,
            pnl: trade.pnl,
            roi: trade.roi,
            grossPnl: trade.grossPnl,
            netPnl: trade.netPnl,
            mae: trade.mae,
            mfe: trade.mfe,
            reason: trade.reason,
            modelVersion: trade.modelVersion,
            featuresAtEntry: trade.featuresAtEntry || trade.features || null,
            featuresAtExit: trade.featuresAtExit || null,
            featureVectorAtEntry: trade.featureVectorAtEntry || null,
            featureVectorAtExit: trade.featureVectorAtExit || null
        };
    }

    buildSample(trade, fullWindow) {
        const symbol = this.normalizeSymbol(trade.symbol);
        const openTs = Number(trade.openTs);
        const closeTs = Number(trade.closeTs);

        if (!symbol || !openTs || !closeTs || closeTs < openTs) {
            this.stats.skippedBadTrade += 1;
            return null;
        }

        const { before, during, after } = this.splitWindow(fullWindow, openTs, closeTs);
        const labels = this.buildLabels(trade);

        return {
            schemaVersion: 'nightly_dataset_v1',
            generatedAt: Date.now(),
            trade: this.compactTrade(trade),
            window: {
                startTs: openTs - PRE_ENTRY_MS,
                openTs,
                closeTs,
                endTs: closeTs + POST_EXIT_MS,
                beforeCount: before.length,
                duringCount: during.length,
                afterCount: after.length
            },
            features: {
                beforeEntry: before,
                duringTrade: during,
                afterExit: after
            },
            labels
        };
    }

    async markTradeTrained(tradeId, label) {
        try {
            await ScoutTrade.findByIdAndUpdate(tradeId, {
                trained: true,
                trainedAt: new Date(),
                trainingLabel: label
            });
        } catch (error) {
            console.error(`⚠️ [DatasetBuilder] Không thể mark trained ${tradeId}:`, error.message);
        }
    }

    async build() {
        this.ensureOutputDir();
        await this.connectMongo();

        const trades = await this.loadTrades();
        const outputFile = this.getOutputFilePath();
        const out = fs.createWriteStream(outputFile, { flags: 'a' });

        for (const trade of trades) {
            const symbol = this.normalizeSymbol(trade.symbol);
            const openTs = Number(trade.openTs);
            const closeTs = Number(trade.closeTs);

            if (!symbol || !openTs || !closeTs || closeTs < openTs) {
                this.stats.skippedBadTrade += 1;
                continue;
            }

            try {
                const startTs = openTs - PRE_ENTRY_MS;
                const endTs = closeTs + POST_EXIT_MS;
                const fullWindow = await this.loadFeatureWindow(symbol, startTs, endTs);

                if (!fullWindow || fullWindow.length === 0) {
                    this.stats.skippedNoWindow += 1;
                    continue;
                }

                const sample = this.buildSample(trade, fullWindow);
                if (!sample) continue;

                out.write(JSON.stringify(sample) + '\n');
                this.stats.samplesWritten += 1;

                await this.markTradeTrained(trade._id, sample.labels.label_direction);
            } catch (error) {
                this.stats.errors += 1;
                console.error(`❌ [DatasetBuilder] Lỗi build sample ${symbol}:`, error.message);
            }
        }

        await new Promise(resolve => out.end(resolve));

        console.log('✅ [DatasetBuilder] Done');
        console.log(JSON.stringify({ outputFile, stats: this.stats }, null, 2));

        return { outputFile, stats: this.stats };
    }

    async close() {
        try { await this.redis.quit(); } catch (e) {}
        try { await mongoose.disconnect(); } catch (e) {}
    }
}

if (require.main === module) {
    const builder = new NightlyTrainingDatasetBuilder();
    builder.build()
        .then(() => builder.close())
        .then(() => process.exit(0))
        .catch(async err => {
            console.error('❌ [DatasetBuilder FATAL]', err.message);
            await builder.close();
            process.exit(1);
        });
}

module.exports = NightlyTrainingDatasetBuilder;
