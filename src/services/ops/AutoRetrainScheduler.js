/**
 * src/services/ops/AutoRetrainScheduler.js
 * Quản lý tiến trình Tự Học tự động (Daily Retrain)
 */
const { spawn } = require('child_process');
const cron = require('node-cron');
const path = require('path');

class AutoRetrainScheduler {
    constructor() {
        this.isTraining = false;
    }

    init() {
        // Lên lịch chạy vào 2:00 Sáng mỗi ngày (Giờ hệ thống/VPS)
        // Cú pháp cron: Phút Giờ Ngày Tháng Thứ ('0 2 * * *' = 2:00 AM)
        cron.schedule('0 2 * * *', () => {
            console.log('\n⏰ [CRON] Kích hoạt tiến trình Tự Học (Retrain) hàng ngày...');
            this.runPythonTrainer();
        });
        
        console.log('✅ [SCHEDULER] Đã lên lịch tự động Retrain AI vào lúc 2:00 AM mỗi ngày.');
    }

    runPythonTrainer() {
        // Tránh tình trạng file cũ chưa chạy xong đã gọi file mới
        if (this.isTraining) {
            console.log('⚠️ [SCHEDULER] Luồng AI đang bận học, bỏ qua lượt này.');
            return;
        }

        this.isTraining = true;
        
        // Trỏ đường dẫn chính xác ra file train_master.py nằm ở thư mục gốc
        // Tùy vào tên file bạn lưu là gì (train_master.py hoặc train_master_v16.py)
        const pythonScriptPath = path.join(__dirname, '../../../train_master.py'); 

        console.log(`🚀 [SCHEDULER] Node.js đang gọi luồng Python: ${pythonScriptPath}`);

        // Dùng 'spawn' để Node.js có thể đọc log của Python theo thời gian thực
        const pythonProcess = spawn('python', [pythonScriptPath]);

        // In các dòng chữ màu trắng (Log bình thường của Python)
        pythonProcess.stdout.on('data', (data) => {
            console.log(`[AI LEARNING]: ${data.toString().trim()}`);
        });

        // In các dòng chữ màu đỏ (Lỗi của Python)
        pythonProcess.stderr.on('data', (data) => {
            console.error(`[AI ERROR]: ${data.toString().trim()}`);
        });

        // Lắng nghe sự kiện khi Python chạy xong
        pythonProcess.on('close', (code) => {
            this.isTraining = false;
            if (code === 0) {
                console.log('🎉 [SCHEDULER] Tiến trình nâng cấp não bộ (Retrain) hoàn tất! Các file ONNX đã được cập nhật.');
                // Lưu ý: Không cần viết code reload lại model ở đây.
                // Bởi vì AiEngine.js đã có hàm fs.watch(), nó sẽ tự động phát hiện file ONNX bị thay đổi và load lại nóng!
            } else {
                console.log(`❌ [SCHEDULER] Tiến trình Retrain thất bại với mã lỗi: ${code}`);
            }
        });
    }
}

module.exports = new AutoRetrainScheduler();