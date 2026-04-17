const mongoose = require('mongoose');

const paperTradeSchema = new mongoose.Schema({
    symbol: { type: String, required: true, index: true },
    side: { type: String, enum: ['LONG', 'SHORT'], required: true },
    entryPrice: { type: Number, required: true },
    entryTime: { type: Date, default: Date.now },

    // Các thông số tại thời điểm vào lệnh
    winProb: { type: Number },
    slPrice: { type: Number },
    tpPrice: { type: Number },
    riskRatio: { type: Number }, // Suggested Risk Ratio
    reason: { type: String },
    featuresAtEntry: [Number], // Lưu lại 11 features để nghiên cứu tại sao AI vào lệnh này

    // Kết quả (Sẽ cập nhật khi đóng lệnh)
    status: { type: String, enum: ['OPEN', 'CLOSED'], default: 'OPEN' },
    exitPrice: { type: Number },
    exitTime: { type: Date },
    pnlPercent: { type: Number, default: 0 }, // % ROI
    pnlUsdt: { type: Number, default: 0 },    // Lợi nhuận giả định (ví dụ tính trên 100$ vốn)
    outcome: { type: String, enum: ['WIN', 'LOSS', 'BREAKEVEN', 'PENDING'], default: 'PENDING' }
});

module.exports = mongoose.model('PaperTrade', paperTradeSchema);