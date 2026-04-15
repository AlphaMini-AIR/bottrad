/**
 * src/services/ops/AutoRetrainScheduler.js
 */
const cron = require('node-cron');
const { exec } = require('child_process');
const path = require('path');
const fs = require('fs');

class AutoRetrainScheduler {
    init() {
        console.log('📅 [SCHEDULER] Đã kích hoạt bộ lập lịch huấn luyện tự động.');

        cron.schedule('0 2 * * 0', async () => {
            console.log('\n🌅 [WEEKEND UPDATE] Bắt đầu quy trình huấn luyện và tuyển quân...');
            this.runTrainingPipeline();
        });
    }

    // [THÊM MỚI]: Hàm hứng dữ liệu 11 biến và ghi vào file JSON
    logCompletedTrade(symbol, side, entryPrice, closePrice, netPnl, features) {
        try {
            const today = new Date().toISOString().split('T')[0];
            const fileName = `exp_${today}.json`;
            const filePath = path.join(__dirname, '../../../', fileName);

            // Nhãn (Label) cho AI học: Lãi > 0 là 1 (Thắng), ngược lại là 0 (Thua)
            const tradeData = {
                timestamp: Date.now(),
                symbol: symbol,
                side: side,
                pnl: netPnl,
                label: netPnl > 0 ? 1 : 0,
                features: Array.from(features) // Chuyển Float32Array thành mảng Array thường để lưu JSON
            };

            let dataset = [];
            if (fs.existsSync(filePath)) {
                const fileContent = fs.readFileSync(filePath, 'utf8');
                if (fileContent) dataset = JSON.parse(fileContent);
            }

            dataset.push(tradeData);
            fs.writeFileSync(filePath, JSON.stringify(dataset, null, 2));
            console.log(`💾 [DATA HARVEST] Đã ghi nhận 11 Đặc trưng của ${symbol} vào ${fileName}`);
        } catch (error) {
            console.error(`❌ [DATA HARVEST ERROR] Lỗi ghi file JSON:`, error.message);
        }
    }

    runTrainingPipeline() {
        console.log('⏳ Bước 1: Đang làm mới dữ liệu...');
        exec('node prepare_dataset.js', (err, stdout, stderr) => {
            if (err) return console.error(`❌ Lỗi prepare_dataset: ${err.message}`);
            console.log('⏳ Bước 2: Đang kích hoạt Python...');
            exec('python train_master.py', (pyErr, pyStdout, pyStderr) => {
                if (pyErr) return console.error(`❌ Lỗi Python: ${pyErr.message}`);
                console.log(pyStdout);
            });
        });
    }
}

module.exports = new AutoRetrainScheduler();