const mongoose = require('mongoose');

// Kết nối với MongoDB Tier 3 (Sử dụng chung kết nối hoặc cấu hình riêng)
// Giả định hệ thống đã gọi mongoose.connect() ở file db.js tổng.

const scoutTradeSchema = new mongoose.Schema({
    symbol: { type: String, required: true, index: true },
    
    // [BỔ SUNG QUAN TRỌNG 1]: Hướng đánh (LONG/SHORT) để Python biết đường gán nhãn
    type: { type: String, required: true, enum: ['LONG', 'SHORT'] }, 
    
    orderType: { type: String, required: true }, // MARKET, LIMIT_MAKER, LIMIT_FOK
    
    // [BỔ SUNG QUAN TRỌNG 2]: Trạng thái lệnh để Python query {"status": "CLOSED"}
    status: { type: String, required: true, enum: ['OPEN', 'CLOSED'], default: 'OPEN', index: true },

    leverage: { type: Number, required: true },
    margin: { type: Number, required: true },
    size: { type: Number, required: true }, // Số lượng coin

    // THÔNG SỐ VÀO LỆNH
    entryPrice: { type: Number, required: true },
    entryFee: { type: Number, default: 0 },
    openTime: { type: Date, required: true },

    // THÔNG SỐ RA LỆNH
    closePrice: { type: Number }, // Để trống lúc OPEN, cập nhật khi CLOSED
    exitFee: { type: Number, default: 0 },
    fundingFee: { type: Number, default: 0 },
    closeTime: { type: Date },
    reason: { type: String }, // LIQUIDATED, Trailing Stop, PANIC_SELL...
    
    // [CHỤP ẢNH KÝ ỨC DÀNH CHO AI]
    features: {
        type: Object,
        required: false // Lệnh cũ không có thì không sao, lệnh mới sẽ tự động điền
    },

    // CHỈ SỐ HIỆU QUẢ (KPI)
    pnl: { type: Number }, 
    roi: { type: Number }, // Tính theo %

    // CHỈ SỐ KIỂM TOÁN AI (Mức gồng lỗ/lãi lớn nhất)
    mae: { type: Number }, // Maximum Adverse Excursion (%)
    mfe: { type: Number }  // Maximum Favorable Excursion (%)
});

// Đánh Index kép để Dashboard hoặc Python truy vấn nhanh tốc độ cao
scoutTradeSchema.index({ status: 1, closeTime: -1 });

module.exports = mongoose.model('ScoutTrade', scoutTradeSchema);