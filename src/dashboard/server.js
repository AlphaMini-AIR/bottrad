const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const mongoose = require('mongoose');
const fs = require('fs');
const path = require('path');
const ScoutTrade = require('../models/ScoutTrade');

// 1. NẠP BIẾN MÔI TRƯỜNG & CẤU HÌNH
require('dotenv').config({ path: path.join(__dirname, '../../.env') });

const configPath = path.join(__dirname, '../../system_config.json');
let config;
try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    console.error("❌ [WATCHTOWER] Lỗi đọc system_config.json, sử dụng mặc định.");
    config = { 
        REDIS_URL: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
        CHANNELS: { FEATURES: 'market:features:*' } 
    };
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(express.static(path.join(__dirname, 'public')));

// 2. BỘ NHỚ ĐỆM TRÊN RAM (QUẢN LÝ TRẠNG THÁI)
let aiLogHistory = {};      // Lưu 50 dòng suy nghĩ gần nhất cho mỗi coin
let lastTickData = new Map(); // Lưu dữ liệu giá/feature mới nhất của mỗi coin
let lastEmitTime = {};      // [CỰC QUAN TRỌNG] Dùng để lọc tốc độ gửi dữ liệu lên Web

// 3. KẾT NỐI REDIS & MONGODB
const subClient = new Redis(config.REDIS_URL);
// Lắng nghe đa kênh: Features (Ticker), Logs (AI suy nghĩ), Trades (Lệnh mới)
subClient.psubscribe(config.CHANNELS.FEATURES);
subClient.subscribe('dashboard:logs', 'dashboard:trades');

if (process.env.MONGO_URI_SCOUT) {
    mongoose.connect(process.env.MONGO_URI_SCOUT)
        .then(() => console.log("📦 [WATCHTOWER] Đã kết nối MongoDB Scout thành công."))
        .catch(err => console.error("❌ [WATCHTOWER] Lỗi kết nối MongoDB:", err.message));
}

// 4. API ENDPOINTS (DÀNH CHO GIAO DIỆN)

// Lấy danh sách lệnh để hiển thị bảng
app.get('/api/trades', async (req, res) => {
    try {
        const trades = await ScoutTrade.find().sort({ openTime: -1 }).limit(100);
        res.json(trades);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// Thống kê Winrate, PnL và Ví (Gốc $200)
app.get('/api/stats', async (req, res) => {
    try {
        const trades = await ScoutTrade.find({ status: 'CLOSED' });
        const openTrades = await ScoutTrade.find({ status: 'OPEN' });

        const totalPnL = trades.reduce((sum, t) => sum + (t.pnl || 0), 0);
        const winningTrades = trades.filter(t => t.pnl > 0).length;
        const winRate = trades.length > 0 ? (winningTrades / trades.length * 100) : 0;

        res.json({
            totalTrades: trades.length + openTrades.length,
            winningTrades,
            winRate: winRate.toFixed(2) + '%',
            netPnL: totalPnL.toFixed(2) + ' USDT',
            openPositions: openTrades.length,
            currentWalletEstimation: (200 + totalPnL).toFixed(2) + ' USDT'
        });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// Dữ liệu cho biểu đồ tăng trưởng vốn
app.get('/api/equity-curve', async (req, res) => {
    try {
        const trades = await ScoutTrade.find({ status: 'CLOSED' }).sort({ closeTime: 1 });
        let balance = 200;
        let curve = [{ time: 'Start', balance: 200 }];
        trades.forEach(t => {
            balance += (t.pnl || 0);
            curve.push({ time: t.closeTime, balance: parseFloat(balance.toFixed(2)), symbol: t.symbol });
        });
        res.json(curve);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// 5. XỬ LÝ SOCKET.IO (GIAO TIẾP VỚI TRÌNH DUYỆT)

io.on('connection', async (socket) => {
    try {
        // Khi người dùng vào web, lấy ngay 50 lệnh mới nhất từ DB gửi cho họ
        const initialTrades = await ScoutTrade.find().sort({ openTime: -1 }).limit(50);
        socket.emit('full_state', {
            logs: aiLogHistory,
            trades: initialTrades,
            ticks: Array.from(lastTickData.values())
        });
    } catch (e) { console.error("Socket connection error:", e); }
});

// 6. XỬ LÝ DỮ LIỆU TỪ REDIS (TRÁI TIM CỦA HỆ THỐNG)

subClient.on('message', (channel, message) => {
    // A. Xử lý Log "Suy nghĩ" của AI
    if (channel === 'dashboard:logs') {
        try {
            const logData = JSON.parse(message);
            const sym = logData.symbol;
            if (!sym || sym === 'undefined') return;

            if (!aiLogHistory[sym]) aiLogHistory[sym] = [];
            aiLogHistory[sym].push(logData);
            
            // Giữ RAM sạch: Chỉ lưu 50 log gần nhất
            if (aiLogHistory[sym].length > 50) aiLogHistory[sym].shift();

            io.emit('ai_thoughts', logData);
        } catch (e) {}
    }
    // B. Xử lý cập nhật Lệnh (khi AI bóp cò hoặc chốt lời)
    else if (channel === 'dashboard:trades') {
        io.emit('trade_update', { ts: Date.now() });
    }
});

// C. Xử lý luồng Tick dữ liệu (Cực nặng - Cần tối ưu)
subClient.on('pmessageBuffer', (pattern, channel, messageBuffer) => {
    let feature;
    // GIẢI MÃ: Thử MsgPack trước, nếu lỗi thử JSON
    try { 
        feature = decode(messageBuffer); 
    } catch (e) {
        try { feature = JSON.parse(messageBuffer.toString()); } catch (err) { return; }
    }

    // Kiểm tra tính hợp lệ
    if (!feature || !feature.symbol || feature.symbol === 'undefined') return;

    const symbol = feature.symbol;
    const now = Date.now();

    // THROTTLING: Chỉ cho phép gửi dữ liệu lên Web mỗi 500ms/coin
    // Việc này giúp Dashboard không bị "đơ" khi có quá nhiều coin cùng lúc
    if (!lastEmitTime[symbol] || (now - lastEmitTime[symbol]) > 500) {
        lastTickData.set(symbol, feature);
        io.emit('live_features', feature);
        lastEmitTime[symbol] = now;
    }
});

// 7. KHỞI CHẠY
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`\n🚀 [WATCHTOWER V17] CHẾ ĐỘ GIÁM SÁT ĐÃ BẬT`);
    console.log(`🌐 Dashboard: http://localhost:${PORT}`);
});