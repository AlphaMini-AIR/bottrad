const mongoose = require('mongoose');

// ============================================================
// SCOUT TRADE MODEL - NO MIDDLEWARE FINAL
// ------------------------------------------------------------
// Lý do viết lại bản này:
// Log VPS vẫn báo: "next is not a function" khi ScoutTrade.create().
// Để loại bỏ tận gốc, file này KHÔNG dùng bất kỳ middleware pre/post nào,
// KHÔNG có next(), KHÔNG có async hook.
//
// Tất cả field thời gian/pnl/feature được PaperExchange.js và OrderManager.js
// truyền trực tiếp khi OPEN/CLOSED, nên model chỉ cần lưu dữ liệu ổn định.
//
// Mục tiêu:
// 1. Mongo create OPEN không còn lỗi middleware.
// 2. Mongo update CLOSED không còn lỗi middleware.
// 3. Giữ đủ field cho PaperExchange_Fixed, OrderManager_Final_Fixed,
//    DashboardServer và NightlyTrainingDatasetBuilder.
// 4. strict:false để không vỡ khi có field mới.
// ============================================================

const ScoutTradeSchema = new mongoose.Schema(
    {
        // ====================================================
        // 1. BASIC TRADE INFO
        // ====================================================
        symbol: {
            type: String,
            required: true,
            index: true,
            trim: true
        },

        type: {
            type: String,
            enum: ['LONG', 'SHORT'],
            required: true,
            index: true
        },

        mode: {
            type: String,
            enum: ['PAPER', 'LIVE', 'BACKTEST', 'LIVE_DRY_RUN'],
            default: 'PAPER',
            index: true
        },

        orderType: {
            type: String,
            default: 'MARKET'
        },

        status: {
            type: String,
            enum: ['OPEN', 'CLOSED', 'LIQUIDATED', 'FAILED', 'CANCELLED', 'CANCELED'],
            default: 'OPEN',
            index: true
        },

        // ====================================================
        // 2. POSITION SIZE
        // ====================================================
        leverage: {
            type: Number,
            default: 1
        },

        margin: {
            type: Number,
            default: 0
        },

        size: {
            type: Number,
            default: 0
        },

        notionalValue: {
            type: Number,
            default: 0
        },

        liquidationPrice: {
            type: Number,
            default: 0
        },

        // ====================================================
        // 3. ENTRY / EXIT PRICE
        // ====================================================
        entryPrice: {
            type: Number,
            required: true
        },

        closePrice: {
            type: Number,
            default: null
        },

        executedEntryPrice: {
            type: Number,
            default: null
        },

        executedClosePrice: {
            type: Number,
            default: null
        },

        highestPrice: {
            type: Number,
            default: null
        },

        lowestPrice: {
            type: Number,
            default: null
        },

        // ====================================================
        // 4. FEE / PNL / ROI
        // ====================================================
        entryFee: {
            type: Number,
            default: 0
        },

        exitFee: {
            type: Number,
            default: 0
        },

        grossPnl: {
            type: Number,
            default: 0
        },

        netPnl: {
            type: Number,
            default: 0
        },

        pnl: {
            type: Number,
            default: 0,
            index: true
        },

        roi: {
            type: Number,
            default: 0
        },

        mae: {
            type: Number,
            default: 0
        },

        mfe: {
            type: Number,
            default: 0
        },

        // ====================================================
        // 5. TIME
        // ====================================================
        openTime: {
            type: Date,
            default: Date.now,
            index: true
        },

        closeTime: {
            type: Date,
            default: null,
            index: true
        },

        openTs: {
            type: Number,
            default: null,
            index: true
        },

        closeTs: {
            type: Number,
            default: null,
            index: true
        },

        durationMs: {
            type: Number,
            default: 0
        },

        // ====================================================
        // 6. AI / MODEL / SIGNAL
        // ====================================================
        prob: {
            type: Number,
            default: 0,
            index: true
        },

        probLong: {
            type: Number,
            default: null
        },

        probShort: {
            type: Number,
            default: null
        },

        macroScore: {
            type: Number,
            default: null
        },

        entrySignal: {
            type: String,
            default: 'AI_PROB_THRESHOLD'
        },

        modelVersion: {
            type: String,
            default: 'Universal_Scout.onnx',
            index: true
        },

        botId: {
            type: String,
            default: 'scout-main',
            index: true
        },

        // ====================================================
        // 7. FEATURES / TRAINING
        // ====================================================
        features: {
            type: mongoose.Schema.Types.Mixed,
            default: null
        },

        featuresAtEntry: {
            type: mongoose.Schema.Types.Mixed,
            default: null
        },

        featuresAtExit: {
            type: mongoose.Schema.Types.Mixed,
            default: null
        },

        featureVectorAtEntry: {
            type: [Number],
            default: undefined
        },

        featureVectorAtExit: {
            type: [Number],
            default: undefined
        },

        trainingUsed: {
            type: Boolean,
            default: false,
            index: true
        },

        trainingDatasetId: {
            type: String,
            default: null,
            index: true
        },

        trainingLabel: {
            type: String,
            default: null,
            index: true
        },

        // ====================================================
        // 8. EXCHANGE ORDER IDS
        // ====================================================
        entryOrderId: {
            type: String,
            default: null,
            index: true
        },

        closeOrderId: {
            type: String,
            default: null,
            index: true
        },

        entryClientOrderId: {
            type: String,
            default: null,
            index: true
        },

        closeClientOrderId: {
            type: String,
            default: null,
            index: true
        },

        // ====================================================
        // 9. REASON / ERROR / NOTE
        // ====================================================
        reason: {
            type: String,
            default: null,
            index: true
        },

        note: {
            type: String,
            default: null
        },

        error: {
            type: String,
            default: null
        }
    },
    {
        timestamps: true,
        strict: false,
        minimize: false
    }
);

// ============================================================
// INDEXES
// ============================================================
ScoutTradeSchema.index({ status: 1, openTime: -1 });
ScoutTradeSchema.index({ status: 1, closeTime: -1 });
ScoutTradeSchema.index({ symbol: 1, status: 1, openTime: -1 });
ScoutTradeSchema.index({ symbol: 1, closeTime: -1 });
ScoutTradeSchema.index({ mode: 1, status: 1, closeTime: -1 });
ScoutTradeSchema.index({ trainingUsed: 1, closeTime: -1 });
ScoutTradeSchema.index({ botId: 1, modelVersion: 1, closeTime: -1 });
ScoutTradeSchema.index({ openTs: -1 });
ScoutTradeSchema.index({ closeTs: -1 });

// ============================================================
// STATIC HELPERS
// ============================================================
ScoutTradeSchema.statics.findOpenTrades = function () {
    return this.find({ status: 'OPEN' }).sort({ openTime: -1 });
};

ScoutTradeSchema.statics.findRecentClosedTrades = function (limit = 100) {
    return this.find({
        status: { $in: ['CLOSED', 'LIQUIDATED', 'FAILED', 'CANCELLED', 'CANCELED'] }
    })
        .sort({ closeTime: -1, updatedAt: -1 })
        .limit(limit);
};

// ============================================================
// EXPORT
// ============================================================
module.exports = mongoose.models.ScoutTrade || mongoose.model('ScoutTrade', ScoutTradeSchema);
