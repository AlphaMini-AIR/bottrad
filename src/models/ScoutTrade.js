const mongoose = require('mongoose');

// Kết nối với MongoDB Tier 3 (Sử dụng chung kết nối hoặc cấu hình riêng)
// Giả định hệ thống đã gọi mongoose.connect() ở file db.js tổng.

const scoutTradeSchema = new mongoose.Schema({
    symbol: { type: String, required: true, index: true },
    orderType: { type: String, required: true }, // MARKET, LIMIT_MAKER, LIMIT_FOK
    leverage: { type: Number, required: true },
    margin: { type: Number, required: true },
    size: { type: Number, required: true }, // Số lượng coin
    
    // THÔNG SỐ VÀO LỆNH
    entryPrice: { type: Number, required: true },
    entryFee: { type: Number, default: 0 },
    openTime: { type: Date, required: true },

    // THÔNG SỐ RA LỆNH
    closePrice: { type: Number, required: true },
    exitFee: { type: Number, default: 0 },
    fundingFee: { type: Number, default: 0 },
    closeTime: { type: Date, default: Date.now },
    reason: { type: String, required: true }, // LIQUIDATED, Trailing Stop, PANIC_SELL...

    // CHỈ SỐ HIỆU QUẢ (KPI)
    pnl: { type: Number, required: true },
    roi: { type: Number, required: true }, // Tính theo %

    // CHỈ SỐ KIỂM TOÁN AI (Mức gồng lỗ/lãi lớn nhất)
    mae: { type: Number }, // Maximum Adverse Excursion (%)
    mfe: { type: Number }  // Maximum Favorable Excursion (%)
});

module.exports = mongoose.model('ScoutTrade', scoutTradeSchema);