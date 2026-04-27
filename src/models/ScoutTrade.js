const mongoose = require('mongoose');

// ============================================================
// SCOUT TRADE MODEL - FIXED LIVE/PAPER READY
// ------------------------------------------------------------
// Mục tiêu của file này:
// 1. Lưu toàn bộ lệnh PAPER / LIVE / BACKTEST của luồng Scout.
// 2. Không dùng middleware kiểu async function(next) để tránh lỗi:
//    "next is not a function" trên Mongoose bản mới.
// 3. Chấp nhận đầy đủ field mới từ PaperExchange_V19, LiveExchange_V19,
//    OrderManager_Final và NightlyTrainingDatasetBuilder.
// 4. Cho phép thêm field mới trong tương lai bằng strict:false,
//    nhưng vẫn khai báo các field quan trọng để query/dashboard/training ổn định.
// 5. Tạo index cho dashboard, nightly training và phân tích hiệu suất.
// ============================================================

const ScoutTradeSchema = new mongoose.Schema(
    {
        // ====================================================
        // 1. THÔNG TIN LỆNH CƠ BẢN
        // ====================================================
        symbol: {
            type: String,
            required: true,
            index: true,
            uppercase: true,
            trim: true
        },

        // LONG / SHORT
        type: {
            type: String,
            enum: ['LONG', 'SHORT'],
            required: true,
            index: true
        },

        // PAPER / LIVE / BACKTEST
        mode: {
            type: String,
            enum: ['PAPER', 'LIVE', 'BACKTEST', 'LIVE_DRY_RUN'],
            default: 'PAPER',
            index: true
        },

        // MARKET / LIMIT / STOP_MARKET...
        orderType: {
            type: String,
            default: 'MARKET'
        },

        // OPEN / CLOSED / LIQUIDATED / FAILED / CANCELLED
        status: {
            type: String,
            enum: ['OPEN', 'CLOSED', 'LIQUIDATED', 'FAILED', 'CANCELLED', 'CANCELED'],
            default: 'OPEN',
            index: true
        },

        // ====================================================
        // 2. POSITION / SIZE / LEVERAGE
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

        // Giá thực thi sau slippage/fill thực tế.
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
        // 4. FEES / PNL / ROI
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

        // Giữ lại alias pnl để dashboard/query cũ vẫn đọc được.
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
        // 5. THỜI GIAN
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

        // Timestamp dạng number để training lấy window nhanh.
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
        // 7. FEATURE SNAPSHOT / TRAINING DATA
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

        // NightlyTrainingDatasetBuilder có thể đánh dấu đã dùng để build dataset.
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
        // 9. CLOSE REASON / NOTE
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
        // timestamps tự tạo createdAt / updatedAt.
        timestamps: true,

        // Cho phép field mới phát sinh từ các module mới mà không làm vỡ schema.
        // Điều này quan trọng vì đây là bot đang tiến hóa liên tục.
        strict: false,

        // Giảm overhead nếu không cần virtual trong JSON.
        minimize: false
    }
);

// ============================================================
// INDEXES
// ------------------------------------------------------------
// Các index này phục vụ:
// - Dashboard query OPEN/CLOSED nhanh.
// - Nightly training query trade vừa đóng.
// - Phân tích theo symbol/model/bot.
// ============================================================

ScoutTradeSchema.index({ status: 1, openTime: -1 });
ScoutTradeSchema.index({ status: 1, closeTime: -1 });
ScoutTradeSchema.index({ symbol: 1, status: 1, openTime: -1 });
ScoutTradeSchema.index({ symbol: 1, closeTime: -1 });
ScoutTradeSchema.index({ mode: 1, status: 1, closeTime: -1 });
ScoutTradeSchema.index({ trainingUsed: 1, closeTime: -1 });
ScoutTradeSchema.index({ botId: 1, modelVersion: 1, closeTime: -1 });

// ============================================================
// SAFE MIDDLEWARE
// ------------------------------------------------------------
// Tuyệt đối KHÔNG dùng async function(next) trong file này.
// Lỗi trước đó "next is not a function" nhiều khả năng đến từ middleware
// trộn async + next ở Mongoose bản mới.
//
// Các middleware dưới đây dùng style sync + next đúng chuẩn.
// ============================================================

ScoutTradeSchema.pre('validate', function (next) {
    try {
        // Chuẩn hóa symbol.
        if (this.symbol) {
            this.symbol = String(this.symbol).toUpperCase().trim();
        }

        // Đồng bộ openTs/openTime.
        if (!this.openTime) {
            this.openTime = new Date();
        }

        if (!this.openTs && this.openTime) {
            this.openTs = new Date(this.openTime).getTime();
        }

        // Nếu có closeTime nhưng chưa có closeTs.
        if (this.closeTime && !this.closeTs) {
            this.closeTs = new Date(this.closeTime).getTime();
        }

        // Đồng bộ executed price nếu chưa truyền.
        if (this.executedEntryPrice == null && this.entryPrice != null) {
            this.executedEntryPrice = this.entryPrice;
        }

        if (this.closePrice != null && this.executedClosePrice == null) {
            this.executedClosePrice = this.closePrice;
        }

        // Đồng bộ pnl/netPnl hai chiều để dashboard và query cũ không vỡ.
        if (this.netPnl == null && this.pnl != null) {
            this.netPnl = this.pnl;
        }

        if (this.pnl == null && this.netPnl != null) {
            this.pnl = this.netPnl;
        }

        // highest/lowest mặc định bằng entry.
        if (this.highestPrice == null && this.entryPrice != null) {
            this.highestPrice = this.entryPrice;
        }

        if (this.lowestPrice == null && this.entryPrice != null) {
            this.lowestPrice = this.entryPrice;
        }

        next();
    } catch (error) {
        next(error);
    }
});

ScoutTradeSchema.pre('findOneAndUpdate', function (next) {
    try {
        const update = this.getUpdate() || {};
        const set = update.$set || update;

        // Nếu update closeTime mà chưa có closeTs thì tự thêm.
        if (set.closeTime && !set.closeTs) {
            set.closeTs = new Date(set.closeTime).getTime();
        }

        // Nếu update closePrice mà chưa có executedClosePrice thì tự thêm.
        if (set.closePrice != null && set.executedClosePrice == null) {
            set.executedClosePrice = set.closePrice;
        }

        // Đồng bộ pnl/netPnl trong update.
        if (set.netPnl == null && set.pnl != null) {
            set.netPnl = set.pnl;
        }

        if (set.pnl == null && set.netPnl != null) {
            set.pnl = set.netPnl;
        }

        // Mongoose timestamps sẽ tự set updatedAt, nhưng giữ lại để chắc chắn.
        set.updatedAt = new Date();

        if (update.$set) {
            update.$set = set;
            this.setUpdate(update);
        } else {
            this.setUpdate(set);
        }

        next();
    } catch (error) {
        next(error);
    }
});

ScoutTradeSchema.pre('updateOne', function (next) {
    try {
        const update = this.getUpdate() || {};
        const set = update.$set || update;
        set.updatedAt = new Date();

        if (update.$set) {
            update.$set = set;
            this.setUpdate(update);
        } else {
            this.setUpdate(set);
        }

        next();
    } catch (error) {
        next(error);
    }
});

// ============================================================
// STATIC HELPERS
// ============================================================

ScoutTradeSchema.statics.findOpenTrades = function () {
    return this.find({ status: 'OPEN' }).sort({ openTime: -1 });
};

ScoutTradeSchema.statics.findRecentClosedTrades = function (limit = 100) {
    return this.find({ status: { $in: ['CLOSED', 'LIQUIDATED', 'FAILED', 'CANCELLED', 'CANCELED'] } })
        .sort({ closeTime: -1, updatedAt: -1 })
        .limit(limit);
};

// ============================================================
// EXPORT MODEL
// ------------------------------------------------------------
// Dùng mongoose.models để tránh OverwriteModelError khi PM2/dev reload.
// ============================================================

module.exports = mongoose.models.ScoutTrade || mongoose.model('ScoutTrade', ScoutTradeSchema);
