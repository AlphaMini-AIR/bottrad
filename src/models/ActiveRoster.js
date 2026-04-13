const mongoose = require('mongoose');

const activeRosterSchema = new mongoose.Schema({
    batch_id: { type: String, default: () => new Date().toISOString().split('T')[0] },
    coins: [{
        symbol: String,
        sharpe_ratio: Number,
        volatility: Number,
        is_active: { type: Boolean, default: true },
        mode: { type: String, default: 'STANDARD' }
    }],
    created_at: { type: Date, default: Date.now }
});

module.exports = mongoose.model('ActiveRoster', activeRosterSchema);