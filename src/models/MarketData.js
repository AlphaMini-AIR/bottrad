/**
 * src/models/MarketData.js - V15 (Ultimate Data Schema)
 */
const mongoose = require('mongoose');

const marketDataSchema = new mongoose.Schema({
    symbol: {
        type: String,
        required: true,
        index: true // Đánh index để truy vấn nhanh theo coin
    },
    openTime: { // Đổi từ timestamp sang openTime để khớp với Binance API
        type: Number,
        required: true,
        index: true // Đánh index để truy vấn time-series
    },

    // --- 1. NHÓM NẾN CƠ BẢN (Chuẩn 11 trường Binance) ---
    ohlcv: {
        open: { type: Number, required: true },
        high: { type: Number, required: true },
        low: { type: Number, required: true },
        close: { type: Number, required: true },
        volume: { type: Number, required: true },
        quoteVolume: { type: Number, required: true },
        trades: { type: Number, required: true },
        takerBuyBase: { type: Number, required: true },
        takerBuyQuote: { type: Number, required: true }
    },

    // --- 2. NHÓM VI CẤU TRÚC (Dữ liệu Live độc quyền) ---
    micro: {
        ob_imb_top20: { type: Number, default: 0.5 },
        spread_close: { type: Number, default: 0 },
        bid_vol_1pct: { type: Number, default: 0 },
        ask_vol_1pct: { type: Number, default: 0 },
        max_buy_trade: { type: Number, default: 0 },
        max_sell_trade: { type: Number, default: 0 },
        liq_long_vol: { type: Number, default: 0 },
        liq_short_vol: { type: Number, default: 0 }
    },

    // --- 3. NHÓM VĨ MÔ ---
    macro: {
        funding_rate: { type: Number, default: 0 },
        open_interest: { type: Number, default: 0 }
    },

    // --- CỜ BẢO VỆ (Cơ chế tự chữa lành) ---
    // Đánh dấu true nếu nến này được kéo bù từ API do VPS sập (thiếu dữ liệu micro thật)
    isStaleData: { type: Boolean, default: false }
}, { collection: 'market_data_clean' });

// Tạo compound index để đảm bảo không lưu trùng 1 phút của 1 coin
marketDataSchema.index({ symbol: 1, openTime: 1 }, { unique: true });

// LƯU Ý QUAN TRỌNG: Ép Mongoose lưu chính xác vào collection 'market_data_live' 
// để khớp với luồng của syncToMongo.js
module.exports = marketDataSchema