const mongoose = require('mongoose');

const paperAccountSchema = new mongoose.Schema({
    account_id: { type: String, default: 'main_paper', unique: true },
    balance: { type: Number, default: 100.00 },
    open_positions: [{
        symbol: String,
        side: { type: String, enum: ['LONG', 'SHORT'] },
        entry_price: Number,
        margin: Number,
        leverage: Number,
        size: Number,
        hard_sl: Number,
        entry_time: { type: Date, default: Date.now }
    }],
    last_funding_charged: { type: Date, default: Date.now }
});

module.exports = mongoose.model('PaperAccount', paperAccountSchema);