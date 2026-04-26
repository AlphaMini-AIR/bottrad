const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const mongoose = require('mongoose');
const fs = require('fs');
const path = require('path');
const ScoutTrade = require('../models/ScoutTrade');

require('dotenv').config({ path: path.join(__dirname, '../../.env') });

const configPath = path.join(__dirname, '../../system_config.json');
let config;
try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    config = { 
        REDIS_URL: process.env.REDIS_URL || 'redis://127.0.0.1:6379',
        CHANNELS: { FEATURES: 'market:features:*', MACRO_SCORES: 'macro:scores' } 
    };
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

// KẾT NỐI REDIS: 1 luồng nghe (Sub), 1 luồng đọc/ghi (Data)
const subClient = new Redis(config.REDIS_URL);
const dataClient = new Redis(config.REDIS_URL);

// Đăng ký các kênh dữ liệu
subClient.psubscribe(config.CHANNELS?.FEATURES || 'market:features:*');
subClient.subscribe('dashboard:logs', 'experience:raw', 'dashboard:trades');

if (process.env.MONGO_URI_SCOUT) {
    mongoose.connect(process.env.MONGO_URI_SCOUT).catch(e => console.error("❌ MongoDB Error:", e.message));
}

app.use(express.static(path.join(__dirname, 'public')));

// BỘ NHỚ TẠM TRÊN RAM SERVER
let aiLogHistory = [];
let lastTickCache = new Map();

// --- XỬ LÝ KHI NGƯỜI DÙNG KẾT NỐI ---
io.on('connection', async (socket) => {
    console.log("👤 [DASHBOARD] Người dùng đã kết nối.");

    try {
        // 1. Đọc "Toàn bộ" danh sách coin và điểm vĩ mô từ Redis
        const macroScores = await dataClient.hgetall(config.CHANNELS?.MACRO_SCORES || 'macro:scores');
        
        // 2. Lấy 50 lệnh gần nhất từ MongoDB
        const initialTrades = await ScoutTrade.find().sort({ openTime: -1 }).limit(50);

        // 3. Gửi toàn bộ trạng thái hiện tại cho UI ngay lập tức
        socket.emit('full_state_sync', {
            macroScores: macroScores,
            logs: aiLogHistory,
            trades: initialTrades,
            ticks: Array.from(lastTickCache.values())
        });
    } catch (e) {
        console.error("❌ [SYNC ERROR]:", e.message);
    }
});

// --- LẮNG NGHE DỮ LIỆU THỜI GIAN THỰC ---

// A. Log suy nghĩ và Kinh nghiệm học tập
subClient.on('message', (channel, message) => {
    try {
        const data = JSON.parse(message);
        if (channel === 'dashboard:logs') {
            aiLogHistory.push(data);
            if (aiLogHistory.length > 100) aiLogHistory.shift();
            io.emit('ai_thoughts', data);
        } 
        else if (channel === 'experience:raw') {
            io.emit('experience', data);
        }
        else if (channel === 'dashboard:trades') {
            io.emit('trade_update', { ts: Date.now() });
        }
    } catch (e) {}
});

// B. Luồng dữ liệu Features (Tick-by-tick)
let lastEmitTime = {};
subClient.on('pmessageBuffer', (pattern, channel, messageBuffer) => {
    let feature;
    try { 
        feature = decode(messageBuffer); 
    } catch (e) {
        try { feature = JSON.parse(messageBuffer.toString()); } catch (err) { return; }
    }

    if (!feature || !feature.symbol) return;
    const symbol = feature.symbol.toUpperCase();

    // Lưu vào cache để người dùng vào sau có cái xem ngay
    lastTickCache.set(symbol, feature);

    // Throttling 500ms để tránh treo trình duyệt
    const now = Date.now();
    if (!lastEmitTime[symbol] || (now - lastEmitTime[symbol]) > 500) {
        io.emit('live_features', feature);
        lastEmitTime[symbol] = now;
    }
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 Watchtower V17 Ready on Port ${PORT}`));