const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const mongoose = require('mongoose');
const fs = require('fs');
const path = require('path');
const ScoutTrade = require('../models/ScoutTrade');

// ==========================================
// 1. KHỞI TẠO VÀ ĐỌC CẤU HÌNH
// ==========================================
const configPath = path.join(__dirname, '../../system_config.json');
let config;
try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    console.error("❌ [WATCHTOWER] Lỗi đọc system_config.json:", e.message);
    process.exit(1);
}
// Đọc biến môi trường (Lấy MONGO_URI)
require('dotenv').config({ path: path.join(__dirname, '../../.env') });

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

// Phục vụ giao diện Frontend (index.html)
app.use(express.static(path.join(__dirname, 'public')));

// Bộ nhớ đệm (RAM) cho Dashboard
let fullTradeHistory = []; 
let aiLogHistory = {}; 
let lastTickData = new Map(); 

// Kết nối Redis
const subClient = new Redis(config.REDIS_URL);
subClient.psubscribe(config.CHANNELS.FEATURES);
subClient.subscribe('dashboard:logs', 'dashboard:trades');

// ==========================================
// 2. KẾT NỐI MONGODB (ĐỒNG BỘ LỊCH SỬ)
// ==========================================
if (process.env.MONGO_URI_SCOUT) {
    mongoose.createConnection(process.env.MONGO_URI_SCOUT).asPromise().then(async (db) => {
        console.log("📦 [WATCHTOWER] Đã kết nối MongoDB.");
        try {
            // Load 200 lệnh cũ nhất để bơm vào Dashboard khi vừa mở web
            const trades = await db.collection('scouttrades').find().sort({ts: -1}).limit(200).toArray();
            fullTradeHistory = trades;
        } catch (e) {
            console.log("⚠️ [WATCHTOWER] Chưa có lịch sử lệnh trong MongoDB:", e.message);
        }
    }).catch(err => console.error("❌ [WATCHTOWER] Lỗi kết nối MongoDB:", err.message));
} else {
    console.warn("⚠️ [WATCHTOWER] Thiếu MONGO_URI_SCOUT trong file .env!");
}

// ==========================================
// 3. REST API CHO BIỂU ĐỒ & KIỂM TOÁN
// ==========================================
app.get('/api/trades', async (req, res) => {
    try {
        const trades = await ScoutTrade.find().sort({ closeTime: -1 }).limit(100);
        res.json(trades);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/stats', async (req, res) => {
    try {
        const trades = await ScoutTrade.find();
        let totalTrades = trades.length;
        let winningTrades = trades.filter(t => t.pnl > 0).length;
        let winRate = totalTrades > 0 ? (winningTrades / totalTrades) * 100 : 0;
        let totalPnL = trades.reduce((sum, t) => sum + t.pnl, 0);
        let totalFees = trades.reduce((sum, t) => sum + (t.entryFee || 0) + (t.exitFee || 0) + (t.fundingFee || 0), 0);

        res.json({
            totalTrades, 
            winningTrades, 
            winRate: winRate.toFixed(2) + '%',
            netPnL: totalPnL.toFixed(2) + ' USDT',
            totalFeesPaidToBinance: totalFees.toFixed(4) + ' USDT',
            currentWalletEstimation: (200 + totalPnL).toFixed(2) + ' USDT'
        });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/equity-curve', async (req, res) => {
    try {
        const trades = await ScoutTrade.find().sort({ closeTime: 1 });
        let currentBalance = 200; 
        let equityData = [{ time: 'Start', balance: currentBalance }];

        trades.forEach(t => {
            currentBalance += t.pnl;
            equityData.push({
                time: t.closeTime || t.ts,
                balance: parseFloat(currentBalance.toFixed(2)),
                symbol: t.symbol
            });
        });
        res.json(equityData);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// ==========================================
// 4. WEBSOCKET & XỬ LÝ DỮ LIỆU REDIS (ROBUST DECODER)
// ==========================================
io.on('connection', (socket) => {
    // Bơm trạng thái ngay khi có người mở trình duyệt
    socket.emit('full_state', {
        logs: aiLogHistory, 
        trades: fullTradeHistory, 
        ticks: Array.from(lastTickData.values())
    });
});

subClient.on('message', (channel, message) => {
    if (channel === 'dashboard:logs') {
        try {
            const logData = JSON.parse(message);
            const sym = logData.symbol;
            if (!aiLogHistory[sym]) aiLogHistory[sym] = [];
            
            aiLogHistory[sym].push(logData);
            
            // Xóa log cũ hơn 15 phút để nhẹ RAM
            const fifteenMinsAgo = Date.now() - 900000;
            aiLogHistory[sym] = aiLogHistory[sym].filter(l => l.ts >= fifteenMinsAgo);
            
            io.emit('ai_thoughts', logData); 
        } catch (e) {
            console.error("❌ [WATCHTOWER] Lỗi parse JSON từ dashboard:logs :", e.message);
        }
    }
});

subClient.on('pmessageBuffer', (pattern, channel, messageBuffer) => {
    let feature;
    try {
        // [CỨU CÁNH 1]: Giải mã nhị phân MsgPack
        feature = decode(messageBuffer); 
    } catch (msgpackError) {
        try {
            // [CỨU CÁNH 2]: Giải mã chuỗi JSON
            feature = JSON.parse(messageBuffer.toString()); 
        } catch (jsonError) {
            // IN RA MÀN HÌNH NẾU DỮ LIỆU RÁC BAY VÀO
            console.error("❌ [WATCHTOWER] Lỗi giải mã Tick từ Python (Data hỏng).");
            return;
        }
    }

    if (!feature || !feature.symbol) return;
    
    // Nếu Python chưa gom đủ 100 nến, bỏ qua
    if (!feature.is_warm) return; 

    // Cập nhật tick mới nhất và bắn ra giao diện web
    lastTickData.set(feature.symbol, feature);
    io.emit('live_features', feature);
});

// ==========================================
// 5. KHỞI CHẠY SERVER
// ==========================================
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🌐 [WATCHTOWER] Dashboard V17 đang trực tại Port ${PORT}`);
});