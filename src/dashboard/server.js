const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const mongoose = require('mongoose');
const fs = require('fs');
const path = require('path');
const ScoutTrade = require('../models/ScoutTrade');

// Nạp biến môi trường
require('dotenv').config({ path: path.join(__dirname, '../../.env') });

// ==========================================
// 1. CẤU HÌNH HỆ THỐNG
// ==========================================
const configPath = path.join(__dirname, '../../system_config.json');
let config;
try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    console.warn("⚠️ Không tìm thấy system_config.json, dùng cấu hình mặc định.");
    config = { 
        REDIS_URL: process.env.REDIS_URL || 'redis://127.0.0.1:6379', 
        CHANNELS: { FEATURES: 'market:features:*' } 
    };
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(express.static(path.join(__dirname, 'public')));

// ==========================================
// 2. KHỞI TẠO BỘ NHỚ ĐỆM (RAM)
// ==========================================
let aiLogHistory = {};      // Lưu log "suy nghĩ" AI
let lastTickData = new Map(); // Lưu dữ liệu giá mới nhất
let lastEmitTime = {};      // [FIX] Khởi tạo biến để lọc tần suất gửi lên Web

// ==========================================
// 3. KẾT NỐI DỮ LIỆU (REDIS & MONGO)
// ==========================================
const subClient = new Redis(config.REDIS_URL);
subClient.psubscribe(config.CHANNELS.FEATURES);
subClient.subscribe('dashboard:logs', 'dashboard:trades');

if (process.env.MONGO_URI_SCOUT) {
    mongoose.connect(process.env.MONGO_URI_SCOUT)
        .then(() => console.log("📦 [WATCHTOWER] Đã kết nối MongoDB Scout."))
        .catch(err => console.error("❌ [WATCHTOWER] Lỗi kết nối MongoDB:", err.message));
}

// ==========================================
// 4. API ENDPOINTS (LỊCH SỬ TRADING)
// ==========================================

// Lấy danh sách 100 lệnh gần nhất
app.get('/api/trades', async (req, res) => {
    try {
        const trades = await ScoutTrade.find().sort({ openTime: -1 }).limit(100);
        res.json(trades);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// Tính toán thống kê hiệu suất (Vốn gốc $200)
app.get('/api/stats', async (req, res) => {
    try {
        const trades = await ScoutTrade.find({ status: 'CLOSED' });
        const openTrades = await ScoutTrade.find({ status: 'OPEN' });

        let totalPnL = trades.reduce((sum, t) => sum + (t.pnl || 0), 0);
        let winningTrades = trades.filter(t => t.pnl > 0).length;
        let winRate = trades.length > 0 ? (winningTrades / trades.length) * 100 : 0;

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

// Dữ liệu vẽ biểu đồ Equity
app.get('/api/equity-curve', async (req, res) => {
    try {
        const trades = await ScoutTrade.find({ status: 'CLOSED' }).sort({ closeTime: 1 });
        let balance = 200;
        let data = [{ time: 'Start', balance: 200 }];
        trades.forEach(t => {
            balance += (t.pnl || 0);
            data.push({ time: t.closeTime, balance: parseFloat(balance.toFixed(2)), symbol: t.symbol });
        });
        res.json(data);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// ==========================================
// 5. XỬ LÝ REAL-TIME (SOCKET.IO & REDIS)
// ==========================================

io.on('connection', async (socket) => {
    // Khi có người mở Web, bơm ngay dữ liệu cũ vào cho họ xem
    const trades = await ScoutTrade.find().sort({ openTime: -1 }).limit(50);
    socket.emit('full_state', {
        logs: aiLogHistory,
        trades: trades,
        ticks: Array.from(lastTickData.values())
    });
});

subClient.on('message', (channel, message) => {
    if (channel === 'dashboard:logs') {
        try {
            const logData = JSON.parse(message);
            const sym = logData.symbol;
            if (!sym || sym === 'undefined') return;

            if (!aiLogHistory[sym]) aiLogHistory[sym] = [];
            aiLogHistory[sym].push(logData);
            if (aiLogHistory[sym].length > 50) aiLogHistory[sym].shift();

            io.emit('ai_thoughts', logData);
        } catch (e) { }
    }
    else if (channel === 'dashboard:trades') {
        io.emit('trade_update', { ts: Date.now() });
    }
});

subClient.on('pmessageBuffer', (pattern, channel, messageBuffer) => {
    let feature;
    // [FIX]: Giải mã dữ liệu TRƯỚC khi sử dụng
    try { 
        feature = decode(messageBuffer); 
    } catch (e) {
        try { feature = JSON.parse(messageBuffer.toString()); } catch (err) { return; }
    }

    // Kiểm tra dữ liệu hợp lệ và lọc coin 'undefined'
    if (!feature || !feature.symbol || feature.symbol === 'undefined') return;

    const symbol = feature.symbol;
    const now = Date.now();

    // [FIX]: Cơ chế lọc 500ms chuẩn xác
    if (!lastEmitTime[symbol] || (now - lastEmitTime[symbol]) > 500) {
        lastTickData.set(symbol, feature);
        io.emit('live_features', feature);
        lastEmitTime[symbol] = now;
    }
});

// ==========================================
// 6. KHỞI CHẠY SERVER
// ==========================================
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`\n🚀 [WATCHTOWER V17] ONLINE AT PORT ${PORT}`);
    console.log(`🔗 TRUY CẬP: http://localhost:${PORT}`);
});