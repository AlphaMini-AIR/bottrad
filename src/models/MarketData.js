/**
 * src/models/MarketData.js
 */
const mongoose = require('mongoose');

const marketDataSchema = new mongoose.Schema({
    symbol: {
        type: String,
        required: true,
        index: true // Đánh index để truy vấn nhanh theo coin
    },
    timestamp: {
        type: Number,
        required: true,
        index: true // Đánh index để truy vấn time-series
    },

    // --- VI MÔ (Micro - Nến 1 phút) ---
    open: { type: Number, required: true },
    high: { type: Number, required: true },
    low: { type: Number, required: true },
    close: { type: Number, required: true },
    volume: { type: Number, required: true },
    taker_buy_base: { type: Number, required: true }, // Lực mua chủ động

    // --- VĨ MÔ (Macro - Cập nhật từ nến 5m/Funding) ---
    open_interest: { type: Number, default: null },
    long_short_ratio: { type: Number, default: null },
    funding_rate: { type: Number, default: null },
    next_funding_time: { type: Number, default: null },

    // --- BỔ SUNG VI CẤU TRÚC (V5.3) ---
    vpin: { type: Number, default: 0 },
    ob_imb: { type: Number, default: 1.0 },
    liq_long: { type: Number, default: 0 },
    liq_short: { type: Number, default: 0 },
    mark_close: { type: Number, default: 0 },

    // --- CỜ BẢO VỆ (DevOps / Ops) ---
    // Đánh dấu true nếu phút này bị rớt mạng và phải dùng Forward-fill đắp nến
    isStaleData: { type: Boolean, default: false }
});

// Tạo compound index để đảm bảo không lưu trùng 1 phút của 1 coin
marketDataSchema.index({ symbol: 1, timestamp: 1 }, { unique: true });

module.exports = mongoose.model('MarketData', marketDataSchema);