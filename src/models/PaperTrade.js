/**
 * src/models/PaperTrade.js - V17.0 (Trajectory Edition)
 * Lưu trữ Lịch sử giao dịch để thống kê Win Rate và Phân tích AI.
 * Tích hợp Quỹ đạo giá (Price Trajectory) phục vụ Dynamic Re-labeling.
 */
const mongoose = require('mongoose');

const paperTradeSchema = new mongoose.Schema({
    symbol: {
        type: String,
        required: true,
        index: true // Đánh index để truy vấn nhanh sau này
    },
    side: {
        type: String,
        required: true,
        enum: ['LONG', 'SHORT']
    },
    entryPrice: {
        type: Number,
        required: true
    },
    closePrice: {
        type: Number,
        required: true
    },
    margin: {
        type: Number,
        required: true // Số vốn đi lệnh (Ví dụ: 2$)
    },
    netPnl: {
        type: Number,
        required: true // Lợi nhuận sau khi đã trừ phí sàn
    },
    outcome: {
        type: String,
        required: true,
        enum: ['WIN', 'LOSS']
    },
    closeReason: {
        type: String,
        required: true // HARD_SL, HARD_TP, hoặc TRAILING_STOP
    },

    // ==========================================================
    // 🟢 DỮ LIỆU SỐNG CÒN CHO PYTHON GÁN NHÃN LẠI (RE-LABELING)
    // ==========================================================
    highestDuringTrade: { 
        type: Number, 
        required: false // Để false để tương thích ngược với các lệnh cũ chưa có tính năng này
    },
    lowestDuringTrade: { 
        type: Number, 
        required: false 
    },

    openTime: {
        type: Date,
        required: true
    },
    closeTime: {
        type: Date,
        default: Date.now
    },
    slPrice: { type: Number }, // Lưu mốc Stoploss ban đầu
    tpPrice: { type: Number }, // Lưu mốc Take Profit ban đầu
    winProb: { type: Number }, // Xác suất thắng mà AI dự đoán lúc vào lệnh
    
    // ==========================================================
    // 🧠 LƯU TRẠNG THÁI NÃO BỘ (FEATURES SNAPSHOT)
    // ==========================================================
    aiFeatures: { 
        type: [Number], // Chuyển từ Object sang [Number] để lưu chuẩn mảng 13 thông số Float32
        default: [] 
    } 
});

module.exports = paperTradeSchema;