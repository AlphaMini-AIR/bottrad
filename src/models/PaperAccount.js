const mongoose = require('mongoose');
const PaperAccountSchema = new mongoose.Schema({
    balance: { type: Number, default: 1000 },
    equity: { type: Number, default: 1000 },
    lastUpdate: { type: Date, default: Date.now }
});
module.exports = PaperAccountSchema;