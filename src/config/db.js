const mongoose = require('mongoose');
require('dotenv').config();

const connectDB = async () => {
    try {
        await mongoose.connect('mongodb+srv://assistantsupdev_db_user:rCp0BrUushhwIKR8@binance.bjmukc0.mongodb.net/?appName=Binance');
        console.log('✅ [MongoDB] Kết nối thành công tới Cluster!');
    } catch (err) {
        console.error('❌ [MongoDB] Lỗi kết nối:', err.message);
        process.exit(1);
    }
};

module.exports = connectDB;