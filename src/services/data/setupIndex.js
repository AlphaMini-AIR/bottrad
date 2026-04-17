/**
 * src/services/data/setupIndex.js
 * Nhiệm vụ: Chạy 1 lần duy nhất để thiết lập luật chống trùng lặp cho Database
 */
require('dotenv').config();
const { MongoClient } = require('mongodb');

// Lấy kết nối từ file .env (Giống hệt các file khác)
const MONGO_URI = 'mongodb+srv://assistantsupdev_db_user:rCp0BrUushhwIKR8@binance.bjmukc0.mongodb.net/?appName=Binance';
const DB_NAME = 'Binance';
const COLLECTION_NAME = 'market_data_live';

async function createUniqueIndex() {
    const client = new MongoClient(MONGO_URI);

    try {
        console.log("⏳ Đang kết nối tới MongoDB...");
        await client.connect();
        const db = client.db(DB_NAME);
        const collection = db.collection(COLLECTION_NAME);

        console.log(`⏳ Đang tạo Unique Index cho collection '${COLLECTION_NAME}'...`);

        // Lệnh cốt lõi: Tạo Index gộp (symbol + openTime) và ép cờ Unique = true
        const indexName = await collection.createIndex(
            { symbol: 1, openTime: 1 },
            { unique: true }
        );

        console.log(`✅ THÀNH CÔNG! Đã tạo Lá chắn thép chống trùng lặp.`);
        console.log(`Tên Index trên DB: ${indexName}`);

    } catch (err) {
        console.error("❌ Lỗi khi tạo Index:", err.message);
    } finally {
        await client.close();
        console.log("🔴 Đã đóng kết nối.");
    }
}

// Khởi chạy
createUniqueIndex();