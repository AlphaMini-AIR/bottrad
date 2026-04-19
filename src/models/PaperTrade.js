/**
 * src/models/PaperTrade.js
 * Lưu trữ Lịch sử giao dịch để thống kê Win Rate và Phân tích AI
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
    openTime: { 
        type: Date, 
        required: true 
    },
    closeTime: { 
        type: Date, 
        default: Date.now 
    }
});

module.exports = paperTradeSchema