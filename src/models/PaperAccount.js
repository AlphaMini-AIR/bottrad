const mongoose = require('mongoose');

const paperAccountSchema = new mongoose.Schema({
    accountId: { type: String, required: true, unique: true },
    balance: { type: Number, required: true },
    initialBalance: { type: Number, required: true }
}, { timestamps: true }); // Mongoose tự động quản lý thời gian cập nhật

module.exports = mongoose.model('PaperAccount', paperAccountSchema);