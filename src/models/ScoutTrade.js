const mongoose = require('mongoose');

// ============================================================
// SCOUT TRADE MODEL V18 - TRADE MEMORY 10/10
// ------------------------------------------------------------
// Vai trò:
// 1. Lưu toàn bộ vòng đời một lệnh giao dịch PAPER / LIVE / BACKTEST.
// 2. Giữ tương thích với code cũ đang dùng field `features`.
// 3. Bổ sung dữ liệu cần thiết cho dashboard, audit, training lại ONNX.
// 4. Tách rõ snapshot feature lúc vào lệnh và lúc thoát lệnh.
// 5. Hỗ trợ truy vấn nhanh theo status, mode, symbol, type, closeTime.
//
// Lưu ý tương thích:
// - PaperExchange cũ đang lưu `features`, nên field này vẫn được giữ.
// - Các field mới không required để dữ liệu cũ trong MongoDB không bị lỗi.
// ============================================================

const scoutTradeSchema = new mongoose.Schema(
    {
        // ====================================================
        // 1. THÔNG TIN ĐỊNH DANH LỆNH
        // ====================================================
        symbol: {
            type: String,
            required: true,
            uppercase: true,
            trim: true,
            index: true
        },

        // Hướng đánh do OrderManager quyết định sau khi ONNX inference.
        type: {
            type: String,
            required: true,
            enum: ['LONG', 'SHORT'],
            index: true
        },

        // Môi trường giao dịch.
        // PAPER: sàn ảo
        // LIVE: sàn thật
        // BACKTEST: dữ liệu backtest/offline
        mode: {
            type: String,
            enum: ['PAPER', 'LIVE', 'BACKTEST'],
            default: 'PAPER',
            index: true
        },

        // MARKET, LIMIT_MAKER, LIMIT_FOK...
        orderType: {
            type: String,
            required: true,
            trim: true
        },

        // Trạng thái vòng đời lệnh.
        status: {
            type: String,
            required: true,
            enum: ['OPEN', 'CLOSED', 'CANCELLED', 'FAILED', 'LIQUIDATED'],
            default: 'OPEN',
            index: true
        },

        // ====================================================
        // 2. THÔNG TIN AI / RADAR TẠI THỜI ĐIỂM VÀO LỆNH
        // ====================================================

        // Xác suất cuối cùng được dùng để bóp cò.
        // Ví dụ LONG 0.78 hoặc SHORT 0.74.
        prob: {
            type: Number,
            min: 0,
            max: 1
        },

        // Nếu OrderManager sau này muốn lưu cả 2 output của ONNX.
        probLong: {
            type: Number,
            min: 0,
            max: 1
        },

        probShort: {
            type: Number,
            min: 0,
            max: 1
        },

        // Điểm radar / macro score tại thời điểm vào lệnh.
        // Dùng để phân tích sau này: macroScore cao có thật sự hiệu quả không.
        macroScore: {
            type: Number,
            min: 0,
            max: 1
        },

        // Lý do/tín hiệu đầu vào nếu có.
        // Ví dụ: AI_PROB_THRESHOLD, EV_SWITCH_ENTRY, MANUAL_ENTRY...
        entrySignal: {
            type: String,
            trim: true
        },

        // Phiên bản model ONNX dùng để ra quyết định.
        // Rất quan trọng khi bạn tự học/reload model nhiều lần.
        modelVersion: {
            type: String,
            trim: true
        },

        // ====================================================
        // 3. THÔNG SỐ VỐN / KHỐI LƯỢNG
        // ====================================================
        leverage: {
            type: Number,
            required: true,
            min: 1
        },

        margin: {
            type: Number,
            required: true,
            min: 0
        },

        // Số lượng coin/base asset.
        size: {
            type: Number,
            required: true,
            min: 0
        },

        // Giá trị danh nghĩa vị thế = size * entryPrice.
        notionalValue: {
            type: Number,
            min: 0
        },

        // Giá thanh lý ước tính, đặc biệt hữu ích cho dashboard/risk audit.
        liquidationPrice: {
            type: Number,
            min: 0
        },

        // ====================================================
        // 4. THÔNG SỐ VÀO LỆNH
        // ====================================================
        entryPrice: {
            type: Number,
            required: true,
            min: 0
        },

        entryFee: {
            type: Number,
            default: 0
        },

        openTime: {
            type: Date,
            required: true,
            index: true
        },

        // Dạng number để Python/training xử lý nhanh hơn.
        openTs: {
            type: Number,
            index: true
        },

        // ====================================================
        // 5. THÔNG SỐ RA LỆNH
        // ====================================================
        closePrice: {
            type: Number,
            min: 0
        },

        exitFee: {
            type: Number,
            default: 0
        },

        fundingFee: {
            type: Number,
            default: 0
        },

        closeTime: {
            type: Date,
            index: true
        },

        closeTs: {
            type: Number,
            index: true
        },

        // Thời gian giữ lệnh, tính bằng millisecond.
        durationMs: {
            type: Number,
            min: 0
        },

        // Lý do đóng lệnh.
        // Ví dụ: Trailing Stop, EV Switch, LIQUIDATED, PANIC_SELL...
        reason: {
            type: String,
            trim: true
        },

        // ====================================================
        // 6. SNAPSHOT FEATURE CHO AI / TRAINING
        // ====================================================

        // Field cũ: giữ nguyên để không phá code/dashboard/training cũ.
        // Quy ước mới: `features` vẫn nên hiểu là snapshot tại entry.
        features: {
            type: Object,
            required: false
        },

        // Field mới rõ nghĩa hơn.
        featuresAtEntry: {
            type: Object,
            required: false
        },

        featuresAtExit: {
            type: Object,
            required: false
        },

        // Có thể lưu thêm feature đã chuẩn hóa đúng thứ tự 13 input của ONNX.
        // Dùng cho audit/training để tránh lệch schema khi feature object thay đổi.
        featureVectorAtEntry: {
            type: [Number],
            default: undefined
        },

        featureVectorAtExit: {
            type: [Number],
            default: undefined
        },

        // ====================================================
        // 7. KPI HIỆU QUẢ
        // ====================================================
        pnl: {
            type: Number
        },

        // ROI tính theo %, ví dụ 5.2 nghĩa là +5.2% trên margin.
        roi: {
            type: Number
        },

        // PnL thô trước phí/funding nếu muốn audit chi tiết.
        grossPnl: {
            type: Number
        },

        // PnL sau phí/funding.
        netPnl: {
            type: Number
        },

        // Maximum Adverse Excursion (%).
        mae: {
            type: Number
        },

        // Maximum Favorable Excursion (%).
        mfe: {
            type: Number
        },

        // Giá cao nhất/thấp nhất trong vòng đời lệnh.
        highestPrice: {
            type: Number,
            min: 0
        },

        lowestPrice: {
            type: Number,
            min: 0
        },

        // ====================================================
        // 8. AUDIT EXECUTION CHO LIVE/PAPER
        // ====================================================

        // ID lệnh thật từ Binance nếu chạy LIVE.
        entryOrderId: {
            type: String,
            trim: true
        },

        closeOrderId: {
            type: String,
            trim: true
        },

        slOrderId: {
            type: String,
            trim: true
        },

        tpOrderId: {
            type: String,
            trim: true
        },

        // Giá khớp thực tế nếu live exchange trả về khác giá dự kiến.
        executedEntryPrice: {
            type: Number,
            min: 0
        },

        executedClosePrice: {
            type: Number,
            min: 0
        },

        // Dùng để lưu lỗi execution nếu lệnh bị reject/failed.
        executionError: {
            type: String,
            trim: true
        },

        // Số lần retry khi bắn lệnh live.
        executionRetries: {
            type: Number,
            default: 0,
            min: 0
        },

        // ====================================================
        // 9. AUDIT HỆ THỐNG / DASHBOARD
        // ====================================================

        // Bot/session ID nếu sau này chạy nhiều bot song song.
        botId: {
            type: String,
            default: 'scout-main',
            trim: true,
            index: true
        },

        // Ghi chú tùy chọn cho dashboard/manual review.
        note: {
            type: String,
            trim: true
        },

        // Đánh dấu record đã được dùng để train hay chưa.
        trained: {
            type: Boolean,
            default: false,
            index: true
        },

        trainedAt: {
            type: Date
        },

        // Label training nếu pipeline tự học muốn ghi lại.
        // Ví dụ: WIN, LOSS, BREAKEVEN, BAD_ENTRY, GOOD_EXIT...
        trainingLabel: {
            type: String,
            trim: true,
            index: true
        }
    },
    {
        timestamps: true,
        minimize: false,
        versionKey: false
    }
);

// ============================================================
// 10. INDEX TỐI ƯU TRUY VẤN DASHBOARD / TRAINING
// ============================================================

// Query lệnh đang mở/đã đóng mới nhất.
scoutTradeSchema.index({ status: 1, closeTime: -1 });

// Query theo symbol và trạng thái.
scoutTradeSchema.index({ symbol: 1, status: 1, openTime: -1 });

// Query theo môi trường PAPER/LIVE/BACKTEST.
scoutTradeSchema.index({ mode: 1, status: 1, closeTime: -1 });

// Query hiệu quả LONG/SHORT.
scoutTradeSchema.index({ type: 1, status: 1, closeTime: -1 });

// Query dữ liệu chưa train.
scoutTradeSchema.index({ trained: 1, status: 1, closeTime: -1 });

// Query theo bot nếu chạy nhiều bot/process.
scoutTradeSchema.index({ botId: 1, status: 1, closeTime: -1 });

// ============================================================
// 11. HOOK TỰ ĐỘNG CHUẨN HÓA DỮ LIỆU TRƯỚC KHI LƯU
// ============================================================
scoutTradeSchema.pre('validate', function preValidate(next) {
    // Luôn uppercase symbol để tránh BTCUSDT/btcusdt bị tách record.
    if (this.symbol) {
        this.symbol = String(this.symbol).toUpperCase().trim();
    }

    // Tự sinh timestamp number nếu chưa có.
    if (this.openTime && !this.openTs) {
        this.openTs = new Date(this.openTime).getTime();
    }

    if (this.closeTime && !this.closeTs) {
        this.closeTs = new Date(this.closeTime).getTime();
    }

    // Tự tính duration nếu đủ open/close.
    if (this.openTs && this.closeTs && !this.durationMs) {
        this.durationMs = Math.max(0, this.closeTs - this.openTs);
    }

    // Nếu code cũ chỉ truyền `features`, tự copy sang featuresAtEntry.
    if (this.features && !this.featuresAtEntry) {
        this.featuresAtEntry = this.features;
    }

    // Nếu code mới chỉ truyền featuresAtEntry, vẫn copy ngược về features để tương thích code cũ.
    if (this.featuresAtEntry && !this.features) {
        this.features = this.featuresAtEntry;
    }

    // Tự tính notional nếu chưa có.
    if (!this.notionalValue && this.size && this.entryPrice) {
        this.notionalValue = this.size * this.entryPrice;
    }

    // Nếu status LIQUIDATED thì vẫn xem là CLOSED về mặt vòng đời.
    // Không ép đổi status để giữ ngữ nghĩa, nhưng closeTime nên có ở tầng Exchange.
    next();
});

// ============================================================
// 12. STATIC HELPERS CHO TRAINING/DASHBOARD
// ============================================================
scoutTradeSchema.statics.findClosedForTraining = function findClosedForTraining(limit = 1000, mode = 'PAPER') {
    return this.find({
        status: { $in: ['CLOSED', 'LIQUIDATED'] },
        mode,
        featuresAtEntry: { $exists: true, $ne: null }
    })
        .sort({ closeTime: -1 })
        .limit(limit)
        .lean();
};

scoutTradeSchema.statics.findOpenTrades = function findOpenTrades(mode = 'PAPER') {
    return this.find({
        status: 'OPEN',
        mode
    })
        .sort({ openTime: -1 })
        .lean();
};

// ============================================================
// 13. INSTANCE HELPER
// ============================================================
scoutTradeSchema.methods.markTrained = async function markTrained(label = null) {
    this.trained = true;
    this.trainedAt = new Date();
    if (label) this.trainingLabel = label;
    return this.save();
};

module.exports = mongoose.model('ScoutTrade', scoutTradeSchema);
