/**
 * src/dashboard/server.js
 * Watchtower UI Server - Trạm kiểm soát trung tâm AI Quant Sniper V17
 */
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const Redis = require('ioredis');
const path = require('path');
const fs = require('fs');

// 1. NẠP CẤU HÌNH HỆ THỐNG
require('dotenv').config({ path: path.join(__dirname, '../../.env') });

let config;
try {
    const configPath = path.join(__dirname, '../../system_config.json');
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    config = { 
        REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379',
        CHANNELS: {
            LOGS: 'dashboard:logs',
            EXPERIENCE: 'experience:raw'
        }
    };
}

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

// 2. KẾT NỐI REDIS ĐỂ LẮNG NGHE SỰ KIỆN
const subClient = new Redis(config.REDIS_URL);

// Đăng ký các kênh dữ liệu từ OrderManager và FeedHandler
const channelsToSub = [
    config.CHANNELS?.LOGS || 'dashboard:logs',
    config.CHANNELS?.EXPERIENCE || 'experience:raw'
];

subClient.subscribe(...channelsToSub, (err, count) => {
    if (err) {
        console.error("❌ [REDIS] Lỗi đăng ký kênh dashboard:", err.message);
    } else {
        console.log(`📡 [WATCHTOWER] Đang nghe ${count} kênh dữ liệu từ hệ thống...`);
    }
});

// 3. CHUYỂN TIẾP DỮ LIỆU SANG GIAO DIỆN WEB (SOCKET.IO)
subClient.on('message', (channel, message) => {
    try {
        const data = JSON.parse(message);
        
        // Phân loại kênh để gửi đúng sự kiện về trình duyệt
        if (channel === (config.CHANNELS?.LOGS || 'dashboard:logs')) {
            io.emit('log', data);
        } 
        else if (channel === (config.CHANNELS?.EXPERIENCE || 'experience:raw')) {
            io.emit('experience', data);
        }
    } catch (e) {
        // Nếu message không phải JSON (có thể là log thuần văn bản)
        if (channel === 'dashboard:logs') {
            io.emit('log', { symbol: 'SYS', msg: message, ts: Date.now() });
        }
    }
});

// 4. CẤU HÌNH WEB SERVER
app.use(express.static(path.join(__dirname, 'public')));

// Route mặc định
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// 5. QUẢN LÝ KẾT NỐI NGƯỜI DÙNG
io.on('connection', (socket) => {
    console.log('👤 [UI] Một người dùng đã kết nối vào Dashboard.');
    
    socket.on('disconnect', () => {
        console.log('👤 [UI] Một người dùng đã rời Dashboard.');
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`
    =================================================
    🚀 AI QUANT SNIPER V17 - WATCHTOWER ONLINE
    🌐 Giao diện: http://localhost:${PORT}
    📡 Trạng thái: Đang kết nối luồng Redis...
    =================================================
    `);
});