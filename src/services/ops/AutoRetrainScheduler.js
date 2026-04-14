/**
 * src/services/ops/AutoRetrainScheduler.js
 */
const cron = require('node-cron');
const { exec } = require('child_process');
const path = require('path');

class AutoRetrainScheduler {
    init() {
        console.log('📅 [SCHEDULER] Đã kích hoạt bộ lập lịch huấn luyện tự động.');

        // Cấu hình chạy vào 02:00 sáng Chủ Nhật hàng tuần (0 2 * * 0)
        // Để test ngay bây giờ, bạn có thể đổi thành '*/5 * * * *' (5 phút chạy 1 lần)
        cron.schedule('0 2 * * 0', async () => {
            console.log('\n🌅 [WEEKEND UPDATE] Bắt đầu quy trình huấn luyện và tuyển quân...');
            this.runTrainingPipeline();
        });
    }

    runTrainingPipeline() {
        console.log('⏳ Bước 1: Đang làm mới dữ liệu huấn luyện...');

        // Gọi script chuẩn bị dữ liệu (Task 1.5 chúng ta đã làm)
        exec('node prepare_dataset.js', (err, stdout, stderr) => {
            if (err) {
                console.error(`❌ [ERROR] Lỗi prepare_dataset: ${err.message}`);
                return;
            }
            console.log('✅ Bước 1: Dữ liệu đã sẵn sàng.');

            console.log('⏳ Bước 2: Đang kích hoạt Python huấn luyện AI Quant Sniper...');

            // Gọi Python thực hiện Task 2.1, 2.2, 2.3
            exec('python train_master.py', (pyErr, pyStdout, pyStderr) => {
                if (pyErr) {
                    console.error(`❌ [ERROR] Lỗi Python: ${pyErr.message}`);
                    return;
                }
                console.log(pyStdout);
                console.log('✅ Bước 2: Huấn luyện hoàn tất. white_list.json đã được cập nhật.');
                console.log('🚀 [SYSTEM] Hệ thống sẽ tự động nhận diện danh sách coin mới trong phút tiếp theo.');
            });
        });
    }
}

module.exports = new AutoRetrainScheduler();