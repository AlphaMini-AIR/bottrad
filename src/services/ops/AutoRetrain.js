const { exec } = require('child_process');
const Redis = require('ioredis');
const path = require('path');

// [FIX 1]: Nạp file .env để lấy đúng REDIS_URL
require('dotenv').config({ path: path.join(__dirname, '../../../.env') });

let config;
try {
    config = JSON.parse(require('fs').readFileSync(path.join(__dirname, '../../../system_config.json'), 'utf8'));
} catch (e) {
    config = { REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379' };
}

const pubClient = new Redis(config.REDIS_URL);

// [FIX 2]: Bắt lỗi Redis để Terminal không bị spam khi Server Redis chưa bật
pubClient.on('error', (err) => {
    // Chỉ in ra 1 dòng cảnh báo ngắn gọn
    // console.error(`[Redis] Chờ kết nối: ${err.message}`); 
});

const trainerPath = path.join(__dirname, '../../../universal_trainer.py');

function runIncrementalTraining() {
    console.log(`\n[${new Date().toLocaleString()}] 🌙 BẮT ĐẦU CHẾ ĐỘ TIẾN HÓA BAN ĐÊM...`);
    
    // [FIX 3]: Ép môi trường xuất log theo chuẩn UTF-8 để Windows không bị crash vì Emoji
    const pythonProcess = exec(`python ${trainerPath} incremental`, {
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });

    pythonProcess.stdout.on('data', (data) => process.stdout.write(data));
    pythonProcess.stderr.on('data', (data) => process.stderr.write(data));

    pythonProcess.on('close', (code) => {
        if (code === 0) {
            console.log(`\n✅ [${new Date().toLocaleString()}] TIẾN HÓA HOÀN TẤT! Đang nạp lại não cho OrderManager...`);
            
            // Chỉ publish lệnh nếu Redis đang hoạt động
            if (pubClient.status === 'ready') {
                pubClient.publish('system:commands', JSON.stringify({ action: 'RELOAD_AI' }));
            } else {
                console.log(`⚠️ Không thể báo OrderManager Reload vì Redis đang ngắt kết nối.`);
            }
        } else {
            console.error(`\n❌ [${new Date().toLocaleString()}] Tiến hóa thất bại. Mã lỗi: ${code}`);
        }
    });
}

// Chạy 1 lần ngay khi khởi động để test
runIncrementalTraining();

function scheduleDaily() {
    const now = new Date();
    const night = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1, 0, 0, 0); 
    const msToMidnight = night.getTime() - now.getTime();

    console.log(`⏳ Lần tiến hóa tiếp theo sẽ diễn ra sau ${Math.floor(msToMidnight/1000/60/60)} giờ...`);

    setTimeout(() => {
        runIncrementalTraining();
        setInterval(runIncrementalTraining, 24 * 60 * 60 * 1000); 
    }, msToMidnight);
}

scheduleDaily();