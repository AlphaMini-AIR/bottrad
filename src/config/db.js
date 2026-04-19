const mongoose = require('mongoose');
require('dotenv').config();

// Đối tượng lưu trữ các luồng kết nối độc lập tới 3 Cluster khác nhau
const connections = {
    tier1: null,
    tier2: null,
    scout: null
};

const connectDB = async () => {
    try {
        const options = {
            maxPoolSize: 15,          
            serverSelectionTimeoutMS: 5000, 
            socketTimeoutMS: 45000,   
            family: 4                 
        };

        console.log('⏳ [DB_MANAGER] Đang khởi tạo Cổng kết nối Đa Luồng (3 CLUSTERS ĐỘC LẬP 512MB x 3)...');

        // Bắt buộc phải có đủ 3 link trong file .env
        if (!process.env.MONGO_URI_TIER1 || !process.env.MONGO_URI_TIER2 || !process.env.MONGO_URI_SCOUT) {
            throw new Error("Thiếu cấu hình URI trong file .env cho Tier1, Tier2 hoặc Scout!");
        }

        // Kết nối đồng thời tới 3 dự án MongoDB riêng biệt
        [connections.tier1, connections.tier2, connections.scout] = await Promise.all([
            mongoose.createConnection(process.env.MONGO_URI_TIER1, options).asPromise(),
            mongoose.createConnection(process.env.MONGO_URI_TIER2, options).asPromise(),
            mongoose.createConnection(process.env.MONGO_URI_SCOUT, options).asPromise()
        ]);

        console.log('✅ [MongoDB] TIER-1 DB (Cụm Server 1) -> Sẵn sàng!');
        console.log('✅ [MongoDB] TIER-2 DB (Cụm Server 2) -> Sẵn sàng!');
        console.log('✅ [MongoDB] SCOUT DB  (Cụm Server 3) -> Sẵn sàng!');

        // Lắng nghe sự cố rớt mạng cục bộ sau khi đã kết nối
        Object.keys(connections).forEach(tierName => {
            connections[tierName].on('disconnected', () => {
                console.warn(`⚠️ [MongoDB ${tierName.toUpperCase()}] Bị mất kết nối! Mongoose đang thử lại...`);
            });
            connections[tierName].on('error', (err) => {
                console.error(`❌ [MongoDB ${tierName.toUpperCase()}] Lỗi nghiêm trọng:`, err.message);
            });
        });

    } catch (err) {
        console.error('❌ [DB_MANAGER] Lỗi khởi tạo hệ thống DB:', err.message);
        process.exit(1); 
    }
};

/**
 * Hàm Helper để các Model (Candle, Trade, Paper) gọi ra kết nối tương ứng
 * @param {string} tier - 'tier1', 'tier2', hoặc 'scout'
 */
const getDbConnection = (tier) => {
    if (!connections[tier]) {
        throw new Error(`[DB_MANAGER] Kết nối cho phân vùng '${tier}' chưa được thiết lập!`);
    }
    return connections[tier];
};

module.exports = { connectDB, getDbConnection };