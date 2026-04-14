/**
 * prepare_dataset.js - Version 4.2 (Production Data Bridge)
 * Nhiệm vụ: Trích xuất dữ liệu từ MongoDB ra dataset_multi_3months.csv cho AI học.
 */
const mongoose = require('mongoose');
const fs = require('fs');
const connectDB = require('./src/config/db');
const MarketData = require('./src/models/MarketData');

async function exportMongoToCSV() {
    const SEVEN_DAYS_MS = 7 * 24 * 60 * 60 * 1000;
    const cutoffTime = Date.now() - SEVEN_DAYS_MS;
    try {
        await connectDB();
        const fileName = 'dataset_multi_3months.csv';

        console.log(`⏳ Đang bắt đầu trích xuất dữ liệu từ MongoDB...`);

        // 1. Tạo luồng ghi file (Write Stream) để tiết kiệm RAM
        const writeStream = fs.createWriteStream(fileName);

        // 2. Ghi dòng tiêu đề (Headers) chuẩn cho train_master.py
        const headers = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'funding_rate'];
        writeStream.write(headers.join(',') + '\n');

        // 3. Truy vấn dữ liệu từ MongoDB dưới dạng Cursor (để không load hết 1.3 triệu bản ghi vào RAM một lúc)
        const cursor = MarketData.find({ timestamp: { $lt: cutoffTime } }) // CHỈ LẤY DỮ LIỆU TRƯỚC 7 NGÀY GẦN ĐÂY
            .sort({ symbol: 1, timestamp: 1 })
            .lean()
            .cursor();
            
        let count = 0;
        for (let doc = await cursor.next(); doc != null; doc = await cursor.next()) {
            const row = [
                doc.symbol,
                doc.timestamp,
                doc.open,
                doc.high,
                doc.low,
                doc.close,
                doc.volume,
                doc.funding_rate || 0.0001 // Fallback nếu thiếu funding
            ];

            writeStream.write(row.join(',') + '\n');
            count++;

            if (count % 100000 === 0) {
                console.log(`   📊 Đã trích xuất: ${count} bản ghi...`);
            }
        }

        writeStream.end();

        writeStream.on('finish', () => {
            console.log(`\n✅ THÀNH CÔNG! Đã tạo file ${fileName} với ${count} dòng.`);
            console.log(`👉 Bây giờ bạn có thể chạy: python train_master.py`);
            mongoose.connection.close();
            process.exit();
        });

    } catch (error) {
        console.error('❌ Lỗi trích xuất dữ liệu:', error.message);
        process.exit(1);
    }
}

exportMongoToCSV();