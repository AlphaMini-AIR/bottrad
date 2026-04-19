/**
 * src/services/data/syncToMongo.js
 * Nhiệm vụ: Đẩy file JSONL tạm lên MongoDB mỗi giờ, chống trùng lặp 
 * VÀ Tự động phân luồng theo tier_list.json (Không cần module trung gian)
 */
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const readline = require('readline');
const cron = require('node-cron');
const { getDbConnection } = require('../../config/db'); // Cổng kết nối Đa luồng
const MarketDataSchema = require('../../models/MarketData');

const LIVE_DATA_DIR = path.join(__dirname, '../../../data/live_buffer');
const SYNCING_FILE = path.join(LIVE_DATA_DIR, 'syncing.jsonl');
const PROCESSING_FILE = path.join(LIVE_DATA_DIR, 'processing.jsonl');

// Đường dẫn file cấu hình Tier
const TIER_LIST_FILE = path.join(__dirname, '../../../tier_list.json');

async function syncDataToMongo() {
    // 1. Kiểm tra file dữ liệu
    if (!fs.existsSync(SYNCING_FILE)) {
        console.log(`[SYNC] Không tìm thấy file syncing.jsonl. Bỏ qua.`);
        return;
    }

    // 2. Khóa file (Tránh xung đột luồng ghi)
    try {
        fs.renameSync(SYNCING_FILE, PROCESSING_FILE);
    } catch (err) {
        console.error(`[SYNC] Lỗi đổi tên file (Khóa Stream):`, err.message);
        return;
    }

    console.log(`[SYNC] Đang xử lý file buffer và phân luồng lên 3 Cụm MongoDB...`);

    // 3. Nạp cấu hình từ tier_list.json TRỰC TIẾP TẠI ĐÂY
    let tierConfig = { ranks: { tier_1: [], tier_2: [], tier_3: [] }, storage_map: {} };
    try {
        if (fs.existsSync(TIER_LIST_FILE)) {
            const rawData = fs.readFileSync(TIER_LIST_FILE, 'utf-8');
            tierConfig = JSON.parse(rawData);
        } else {
            console.warn(`⚠️ [SYNC] Cảnh báo: Không tìm thấy tier_list.json. Mặc định đẩy hết vào Scout.`);
        }
    } catch (error) {
        console.error(`❌ [SYNC] Lỗi đọc tier_list.json:`, error.message);
    }

    // Hàm nội bộ xác định thứ hạng cho Giao diện UI
    const getLogicalTier = (symbol) => {
        if (tierConfig.ranks.tier_1.includes(symbol)) return 1;
        if (tierConfig.ranks.tier_2.includes(symbol)) return 2;
        if (tierConfig.ranks.tier_3.includes(symbol)) return 3;
        return 99;
    };

    // Tạo 3 thùng chứa Batch
    const dbBatches = { tier1: [], tier2: [], scout: [] };
    let totalRecords = 0;

    try {
        const fileStream = fs.createReadStream(PROCESSING_FILE);
        const rl = readline.createInterface({ input: fileStream, crlfDelay: Infinity });

        // 4. Đọc từng dòng và chia mâm
        for await (const line of rl) {
            if (line.trim()) {
                try {
                    const record = JSON.parse(line);
                    
                    // Thêm metadata để hiển thị UI sau này
                    record.logicalTier = getLogicalTier(record.symbol);
                    record.createdAt = new Date();

                    // Xác định nơi lưu trữ dựa vào storage_map (Nếu không có thì tống vào scout)
                    const storageNode = tierConfig.storage_map[record.symbol] || 'scout';
                    
                    if (dbBatches[storageNode]) {
                        dbBatches[storageNode].push({
                            updateOne: {
                                filter: { symbol: record.symbol, openTime: record.openTime },
                                update: { $setOnInsert: record },
                                upsert: true
                            }
                        });
                        totalRecords++;
                    }
                } catch (e) {
                    console.error("[SYNC] Lỗi Parse JSON ở dòng:", line);
                }
            }
        }

        if (totalRecords === 0) {
            fs.unlinkSync(PROCESSING_FILE);
            return;
        }

        // 5. Ghi BulkWrite song song lên các Database
        const syncPromises = Object.entries(dbBatches).map(async ([nodeName, opsArray]) => {
            if (opsArray.length > 0) {
                const connection = getDbConnection(nodeName);
                const MarketDataModel = connection.model('MarketData', MarketDataSchema);
                
                const result = await MarketDataModel.bulkWrite(opsArray, { ordered: false });
                console.log(`✅ [SYNC ${nodeName.toUpperCase()}] Đã duyệt: ${opsArray.length} | Ghi mới: ${result.upsertedCount} | Trùng lặp: ${opsArray.length - result.upsertedCount}`);
            }
        });

        await Promise.all(syncPromises);

        // Đẩy thành công -> Xóa file rác
        fs.unlinkSync(PROCESSING_FILE);
        console.log(`🎯 [SYNC] Đã hoàn tất đồng bộ toàn bộ hệ thống!`);

    } catch (err) {
        console.error("❌ [SYNC] Lỗi kết nối hoặc đẩy dữ liệu lên Mongo:", err.message);
        // Fallback: Nối lại dữ liệu nếu thất bại
        if (fs.existsSync(PROCESSING_FILE)) {
            const processingData = fs.readFileSync(PROCESSING_FILE);
            fs.appendFileSync(SYNCING_FILE, processingData);
            fs.unlinkSync(PROCESSING_FILE);
            console.log("♻️ [SYNC] Đã hoàn trả dữ liệu về syncing.jsonl để chờ lần sau.");
        }
    }
}

// ================= CẤU HÌNH CRONJOB LỆCH GIỜ =================
const IS_BACKUP_NODE = process.env.IS_BACKUP_NODE === 'true';
const cronSchedule = IS_BACKUP_NODE ? '5 * * * *' : '0 * * * *';

console.log(`🕒 [CRON] Chế độ máy: ${IS_BACKUP_NODE ? 'BACKUP (Phút 05)' : 'MASTER (Phút 00)'}. Đang chờ đồng bộ...`);

cron.schedule(cronSchedule, () => {
    syncDataToMongo();
});

module.exports = { syncDataToMongo };