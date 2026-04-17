/**
 * src/services/data/syncToMongo.js
 * Nhiệm vụ: Đẩy file JSONL tạm lên MongoDB mỗi giờ và chống trùng lặp.
 */
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const cron = require('node-cron');
const { MongoClient } = require('mongodb');

const MONGO_URI = 'mongodb+srv://assistantsupdev_db_user:rCp0BrUushhwIKR8@binance.bjmukc0.mongodb.net/?appName=Binance';
const DB_NAME = 'Binance';
const COLLECTION_NAME = 'market_data_live';

const LIVE_DATA_DIR = path.join(__dirname, '../../../data/live_buffer');
const SYNCING_FILE = path.join(LIVE_DATA_DIR, 'syncing.jsonl');
const PROCESSING_FILE = path.join(LIVE_DATA_DIR, 'processing.jsonl');

async function syncDataToMongo() {
    // 1. Kiểm tra xem có dữ liệu để đồng bộ không
    if (!fs.existsSync(SYNCING_FILE)) {
        console.log(`[SYNC] Không tìm thấy file syncing.jsonl. Bỏ qua.`);
        return;
    }

    // 2. Đổi tên file để tránh xung đột ghi với StreamAggregator.js
    // StreamAggregator sẽ tự động tạo file syncing.jsonl mới ngay lập tức
    try {
        fs.renameSync(SYNCING_FILE, PROCESSING_FILE);
    } catch (err) {
        console.error(`[SYNC] Lỗi đổi tên file:`, err.message);
        return;
    }

    const client = new MongoClient(MONGO_URI);
    try {
        await client.connect();
        const db = client.db(DB_NAME);
        const collection = db.collection(COLLECTION_NAME);

        const records = [];
        const fileStream = fs.createReadStream(PROCESSING_FILE);
        const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

        // 3. Đọc từng dòng JSONL đưa vào mảng
        for await (const line of rl) {
            if (line.trim()) {
                try {
                    records.push(JSON.parse(line));
                } catch (e) {
                    console.error("[SYNC] Lỗi Parse JSON ở dòng:", line);
                }
            }
        }

        if (records.length === 0) {
            fs.unlinkSync(PROCESSING_FILE);
            return;
        }

        console.log(`[SYNC] Đang đẩy ${records.length} nến vi cấu trúc lên MongoDB...`);

        // 4. KỸ THUẬT FIRST-WRITER-WINS (Chống trùng lặp tuyệt đối)
        const bulkOps = records.map(record => ({
            updateOne: {
                filter: {
                    symbol: record.symbol,
                    openTime: record.openTime
                },
                update: {
                    // CHỈ thêm mới nếu DB chưa có dòng này.
                    // Nếu DB ĐÃ CÓ (Do VPS khác đẩy rồi), nó sẽ BỎ QUA hoàn toàn.
                    $setOnInsert: record
                },
                upsert: true // Bật cờ bù đắp
            }
        }));

        const result = await collection.bulkWrite(bulkOps, { ordered: false });

        console.log(`✅ [SYNC] Hoàn tất! Đã chèn mới: ${result.upsertedCount}. Bỏ qua trùng lặp: ${records.length - result.upsertedCount}`);

        // 5. Đẩy thành công -> Xóa file rác
        fs.unlinkSync(PROCESSING_FILE);

    } catch (err) {
        console.error("❌ [SYNC] Lỗi kết nối hoặc đẩy dữ liệu lên Mongo:", err.message);
        // Nếu lỗi mạng, ĐỔI TÊN LẠI để giữ dữ liệu cho vòng sau, không xóa file!
        if (fs.existsSync(PROCESSING_FILE)) {
            // Nối dữ liệu trả về file syncing
            const processingData = fs.readFileSync(PROCESSING_FILE);
            fs.appendFileSync(SYNCING_FILE, processingData);
            fs.unlinkSync(PROCESSING_FILE);
            console.log("♻️ [SYNC] Đã hoàn trả dữ liệu về syncing.jsonl để chờ lần sau.");
        }
    } finally {
        await client.close();
    }
}

// ================= CẤU HÌNH CRONJOB LỆCH GIỜ =================
// Đọc biến môi trường xem đây là máy chính hay máy phụ
const IS_BACKUP_NODE = process.env.IS_BACKUP_NODE === 'true';

// Máy chính: Chạy đúng phút 00 của mỗi giờ (VD: 01:00, 02:00)
// Máy phụ (Local): Chạy phút 05 của mỗi giờ (VD: 01:05, 02:05) để trám chỗ nếu máy chính sập
const cronSchedule = IS_BACKUP_NODE ? '5 * * * *' : '0 * * * *';

console.log(`🕒 [CRON] Chế độ máy: ${IS_BACKUP_NODE ? 'BACKUP (Phút 05)' : 'MASTER (Phút 00)'}. Đang chờ đồng bộ...`);

cron.schedule(cronSchedule, () => {
    syncDataToMongo();
});

// Hàm export để main.js có thể gọi nếu cần thiết (Khi tắt bot đột ngột)
module.exports = { syncDataToMongo };