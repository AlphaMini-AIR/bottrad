/**
 * src/dashboard/server.js
 * Watchtower Server V17 - Trung tâm đồng bộ Redis & MongoDB
 */
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const mongoose = require('mongoose');
const fs = require('fs');
const path = require('path');

// Nạp Model Lịch sử giao dịch (Đảm bảo đường dẫn đúng với dự án của bạn)
const ScoutTrade = require('../models/ScoutTrade');

// 1. NẠP CẤU HÌNH & MÔI TRƯỜNG
require('dotenv').config({ path: path.join(__dirname, '../../.env') });

let config;
try {
    const configPath = path.join(__dirname, '../../system_config.json');
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

// 2. KẾT NỐI DATABASE (MONGODB & REDIS)
if (process.env.MONGO_URI_SCOUT) {
    mongoose.connect(process.env.MONGO_URI_SCOUT)
        .then(() => console.log("📦 [WATCHTOWER] Đã kết nối MongoDB thành công."))
        .catch(err => console.error("❌ [WATCHTOWER] Lỗi kết nối MongoDB:", err.message));
}

const subClient = new Redis(config.REDIS_URL);
const dataClient = new Redis(config.REDIS_URL);

subClient.psubscribe(config.CHANNELS?.FEATURES || 'market:features:*');
subClient.subscribe('dashboard:logs', 'experience:raw', 'system:subscriptions', 'dashboard:predictions');

// 3. BỘ NHỚ ĐỆM SERVER (RAM CACHE)
let aiLogHistory = [];
let lastTickCache = new Map();
let lastEmitTime = {};

app.use(express.static(path.join(__dirname, 'public')));

// ==========================================
// 4. API ENDPOINTS CHO GIAO DIỆN (MONGODB)
// ==========================================

// API: Lấy 50 lệnh gần nhất
app.get('/api/trades', async (req, res) => {
    try {
        const trades = await ScoutTrade.find().sort({ openTime: -1 }).limit(50);
        res.json(trades);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// API: Thống kê PnL và Winrate thực tế
app.get('/api/stats', async (req, res) => {
    try {
        const closedTrades = await ScoutTrade.find({ status: 'CLOSED' });
        const openTrades = await ScoutTrade.find({ status: 'OPEN' });

        const totalPnL = closedTrades.reduce((sum, t) => sum + (t.pnl || 0), 0);
        const winningTrades = closedTrades.filter(t => t.pnl > 0).length;
        const winRate = closedTrades.length > 0 ? (winningTrades / closedTrades.length * 100) : 0;

        res.json({
            totalTrades: closedTrades.length + openTrades.length,
            winningTrades,
            winRate: winRate.toFixed(2) + '%',
            netPnL: totalPnL.toFixed(2) + ' USDT',
            openPositions: openTrades.length,
            currentWalletEstimation: (200 + totalPnL).toFixed(2) + ' USDT' // Giả sử vốn gốc 200$
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ==========================================
// 5. XỬ LÝ WEBSOCKET (REAL-TIME SYNC)
// ==========================================

io.on('connection', async (socket) => {
    console.log("👤 [DASHBOARD] Client connected.");
    try {
        // Đồng bộ dữ liệu tĩnh từ Redis & MongoDB ngay khi vào trang
        const macroScores = await dataClient.hgetall(config.CHANNELS?.MACRO_SCORES || 'macro:scores');
        const initialTrades = await ScoutTrade.find().sort({ openTime: -1 }).limit(20);

        socket.emit('full_state_sync', {
            macroScores: macroScores,
            logs: aiLogHistory,
            trades: initialTrades,
            ticks: Array.from(lastTickCache.values())
        });
    } catch (e) {
        console.error("❌ Sync Error:", e.message);
    }
});

// A. Lắng nghe Log & Experience từ OrderManager
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
            io.emit('trade_update');
        }
        else if (channel === 'system:subscriptions' && (data.action === 'ENTER_TRADE' || data.action === 'EXIT_TRADE')) {
            io.emit('trade_update');
        }
        // 🟢 [THÊM MỚI] Chuyển tiếp dự đoán AI xuống giao diện web
        else if (channel === 'dashboard:predictions') {
            io.emit('ai_prediction', data);
        }
    } catch (e) { }
});

// B. Lắng nghe dòng dữ liệu HFT (Tick by Tick)
subClient.on('pmessageBuffer', (pattern, channel, messageBuffer) => {
    let feature;
    // Hybrid Decode: Hỗ trợ cả MsgPack (Python) và JSON
    try {
        feature = decode(messageBuffer);
    } catch (e) {
        try { feature = JSON.parse(messageBuffer.toString()); } catch (err) { return; }
    }

    if (!feature || !feature.symbol) return;
    const symbol = feature.symbol.toUpperCase();

    // Cache lại tick cuối cùng để sync cho người vào sau
    lastTickCache.set(symbol, feature);

    // BỘ LỌC CHỐNG TREO GIAO DIỆN (Throttling 500ms)
    const now = Date.now();
    if (!lastEmitTime[symbol] || (now - lastEmitTime[symbol]) > 500) {
        io.emit('live_features', feature);
        lastEmitTime[symbol] = now;
    }
});

// 6. KHỞI CHẠY SERVER
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`
    =================================================
    🚀 AI QUANT SNIPER V17 - COMMAND CENTER ONLINE
    🌐 Giao diện: http://localhost:${PORT}
    📡 Đã kết nối Redis Pub/Sub & MongoDB API
    =================================================
    `);
});