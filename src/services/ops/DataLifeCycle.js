/**
 * src/services/ops/DataLifeCycle.js
 * Nhiệm vụ: Tự động backup data cũ ra ổ cứng cục bộ và xóa khỏi MongoDB
 * Chạy tự động vào 2h sáng mỗi ngày.
 */
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const cron = require('node-cron');
const { getDbConnection } = require('../../config/db');
const MarketDataSchema = require('../../models/MarketData');

// Cấu hình thư mục lưu trữ cứng
const ARCHIVE_DIR = path.join(__dirname, '../../../data/archive');
const TIER_LIST_FILE = path.join(__dirname, '../../../tier_list.json');

// Đảm bảo thư mục archive luôn tồn tại
if (!fs.existsSync(ARCHIVE_DIR)) {
    fs.mkdirSync(ARCHIVE_DIR, { recursive: true });
}

async function runDataCleanup() {
    console.log('🧹 [LifeCycle] Bắt đầu tiến trình Dọn dẹp & Lưu trữ Lạnh (2:00 AM)...');

    // 1. Nạp danh sách Coin và Nơi lưu trữ
    let tierConfig = { storage_map: {} };
    try {
        if (fs.existsSync(TIER_LIST_FILE)) {
            tierConfig = JSON.parse(fs.readFileSync(TIER_LIST_FILE, 'utf-8'));
        } else {
            console.error('❌ [LifeCycle] Không tìm thấy tier_list.json, hủy dọn dẹp để bảo toàn data!');
            return;
        }
    } catch (e) {
        console.error('❌ [LifeCycle] Lỗi đọc tier_list.json:', e.message);
        return;
    }

    const coins = Object.keys(tierConfig.storage_map);

    // 2. Duyệt qua từng đồng Coin để xử lý
    for (const symbol of coins) {
        const storageNode = tierConfig.storage_map[symbol];

        // Thiết lập tuổi thọ: Tầng Scout chỉ sống 7 ngày, Tầng 1&2 sống 60 ngày
        const retentionDays = storageNode === 'scout' ? 7 : 60;
        const cutoffTime = Date.now() - (retentionDays * 24 * 60 * 60 * 1000);

        try {
            const connection = getDbConnection(storageNode);
            const MarketDataModel = connection.model('MarketData', MarketDataSchema);

            // Tìm đếm xem có bao nhiêu nến quá hạn
            const oldRecordsCount = await MarketDataModel.countDocuments({
                symbol: symbol,
                openTime: { $lt: cutoffTime }
            });

            if (oldRecordsCount === 0) {
                console.log(`ℹ️ [LifeCycle] ${symbol}: Dữ liệu chưa vượt quá ${retentionDays} ngày. Bỏ qua.`);
                continue;
            }

            console.log(`📦 [LifeCycle] ${symbol}: Phát hiện ${oldRecordsCount} nến quá hạn. Đang tiến hành Export...`);

            // Tạo tên file Cold Storage theo định dạng: BTCUSDT_archive.jsonl
            const archiveFilePath = path.join(ARCHIVE_DIR, `${symbol}_archive.jsonl`);
            const writeStream = fs.createWriteStream(archiveFilePath, { flags: 'a' }); // 'a' là append (ghi nối tiếp)

            // 3. Sử dụng Cursor để stream data (Cực kỳ tiết kiệm RAM)
            const cursor = MarketDataModel.find({
                symbol: symbol,
                openTime: { $lt: cutoffTime }
            }).lean().cursor();

            for await (const doc of cursor) {
                // Xóa trường _id của Mongo để file gọn nhẹ hơn
                delete doc._id;
                delete doc.__v;
                writeStream.write(JSON.stringify(doc) + '\n');
            }

            writeStream.end();

            // 4. Chờ file ghi xong vật lý trên ổ cứng mới được lệnh xóa Mongo
            await new Promise(resolve => writeStream.on('finish', resolve));

            // 5. Purge (Xóa vĩnh viễn khỏi MongoDB)
            const deleteResult = await MarketDataModel.deleteMany({
                symbol: symbol,
                openTime: { $lt: cutoffTime }
            });

            console.log(`✅ [LifeCycle] ${symbol}: Đã nén thành công và xóa ${deleteResult.deletedCount} nến khỏi Cluster ${storageNode.toUpperCase()}.`);

        } catch (error) {
            console.error(`❌ [LifeCycle] Lỗi khi xử lý ${symbol}:`, error.message);
        }
    }

    console.log('🏁 [LifeCycle] Hoàn tất toàn bộ chu trình dọn dẹp hệ thống!');
}

// ================= CẤU HÌNH CRONJOB =================
// Chạy vào 2:00 sáng mỗi ngày (Thời điểm thị trường ít biến động nhất)
const IS_BACKUP_NODE = process.env.IS_BACKUP_NODE === 'true';

if (!IS_BACKUP_NODE) {
    console.log('🕒 [CRON] Module LifeCycle đã kích hoạt (Chạy ngầm lúc 02:00 AM mỗi ngày).');
    cron.schedule('0 2 * * *', () => {
        runDataCleanup();
    });
} else {
    console.log('🕒 [CRON] LifeCycle TẮT trên máy Backup để tránh ghi đè file lưu trữ chéo nhau.');
}

module.exports = { runDataCleanup };