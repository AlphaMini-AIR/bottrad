const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const fs = require('fs');
const path = require('path');

// 1. Đọc cấu hình
const config = JSON.parse(fs.readFileSync(path.join(__dirname, '../../system_config.json'), 'utf8'));

// 2. Khởi tạo Web Server
const app = express();
const server = http.createServer(app);
const io = new Server(server);

// Phục vụ file giao diện HTML
app.use(express.static(path.join(__dirname, 'public')));

// 3. Kết nối Redis (Chỉ Đọc - Không ảnh hưởng đến luồng chính)
const subClient = new Redis(config.REDIS_URL);

// Đăng ký nghe tất cả các kênh
subClient.psubscribe(config.CHANNELS.FEATURES);
subClient.subscribe(config.CHANNELS.CANDIDATES);
subClient.subscribe('dashboard:logs'); // Kênh mới để nhận suy nghĩ của OrderManager

console.log("👁️ Watchtower Backend đang lắng nghe Redis...");

// 4. Định tuyến dữ liệu từ Redis ném thẳng ra Frontend (Client)
subClient.on('message', (channel, message) => {
    if (channel === config.CHANNELS.CANDIDATES) {
        io.emit('radar_update', JSON.parse(message));
    }
    if (channel === 'dashboard:logs') {
        io.emit('ai_thoughts', JSON.parse(message));
    }
});

// Xử lý dữ liệu nén nhị phân từ Python (Mili-giây)
subClient.on('pmessageBuffer', (pattern, channel, messageBuffer) => {
    try {
        const feature = decode(messageBuffer);
        // Ném dữ liệu tươi sống ra giao diện web
        io.emit('live_features', feature);
    } catch (e) {}
});

// Khởi chạy Server ở Port 3000
const PORT = 3000;
server.listen(PORT, () => {
    console.log(`🌐 Dashboard đang chạy tại: http://<IP_VPS_CUA_BAN>:${PORT}`);
});