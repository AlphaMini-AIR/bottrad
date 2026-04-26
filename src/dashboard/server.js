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
require('dotenv').config({ path: path.join(__dirname, '../../.env') });
const configPath = path.join(__dirname, '../../system_config.json');

let config;
try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    console.error("❌ [WATCHTOWER] Không tìm thấy system_config.json");
    process.exit(1);
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(express.static(path.join(__dirname, 'public')));

// Bộ nhớ đệm RAM
let fullTradeHistory = []; 
let aiLogHistory = {}; 
let lastTickData = new Map(); 

// Biến kiểm soát tốc độ (Throttling) để tránh làm nghẽn UI
let lastTickEmitTime = 0;

const subClient = new Redis(config.REDIS_URL);

// ==========================================
// 2. KẾT NỐI MONGODB TOÀN CỤC
// ==========================================
const mongoUri = process.env.MONGO_URI_SCOUT || process.env.MONGO_URI;

if (mongoUri) {
    mongoose.connect(mongoUri)
        .then(async () => {
            console.log("📦 [WATCHTOWER] Đã kết nối MongoDB thành công.");
            try {
                // Chỉ lấy 50 lệnh gần nhất để nhẹ UI
                fullTradeHistory = await ScoutTrade.find().sort({ ts: -1 }).limit(50);
            } catch (e) {
                console.warn("⚠️ [DB] Chưa có dữ liệu lệnh cũ.");
            }
        })
        .catch(err => console.error("❌ [WATCHTOWER] Lỗi MongoDB:", err.message));
} else {
    console.warn("⚠️ [WATCHTOWER] Thiếu cấu hình MONGO_URI trong .env");
}

// ==========================================
// 3. API REST
// ==========================================

app.get('/api/stats', async (req, res) => {
    try {
        const trades = await ScoutTrade.find();
        if (!trades || trades.length === 0) {
            return res.json({
                totalTrades: 0, winningTrades: 0, winRate: '0.00%',
                netPnL: '0.00 USDT', totalFeesPaidToBinance: '0.0000 USDT',
                currentWalletEstimation: '200.00 USDT'
            });
        }

        let totalTrades = trades.length;
        let winningTrades = trades.filter(t => t.pnl > 0).length;
        let winRate = (winningTrades / totalTrades) * 100;
        let totalPnL = trades.reduce((sum, t) => sum + (t.pnl || 0), 0);
        let totalFees = trades.reduce((sum, t) => sum + (t.entryFee || 0) + (t.exitFee || 0), 0);

        res.json({
            totalTrades, 
            winningTrades, 
            winRate: winRate.toFixed(2) + '%',
            netPnL: totalPnL.toFixed(2) + ' USDT',
            totalFeesPaidToBinance: totalFees.toFixed(4) + ' USDT',
            currentWalletEstimation: (200 + totalPnL).toFixed(2) + ' USDT'
        });
    } catch (err) {
        res.json({ totalTrades: 0, netPnL: '0.00 USDT', winRate: '0.00%' });
    }
});

app.get('/api/equity-curve', async (req, res) => {
    try {
        const trades = await ScoutTrade.find().sort({ closeTime: 1 });
        let balance = 200;
        let curve = [{ time: Date.now(), balance: 200 }];
        
        trades.forEach(t => {
            balance += (t.pnl || 0);
            curve.push({ time: t.closeTime || t.ts, balance: parseFloat(balance.toFixed(2)) });
        });
        res.json(curve);
    } catch (err) { res.json([]); }
});

// ==========================================
// 4. XỬ LÝ REDIS PUB/SUB & SOCKET.IO
// ==========================================

subClient.psubscribe('market:features*');
subClient.subscribe('dashboard:logs', 'dashboard:trades');

subClient.on('pmessageBuffer', (pattern, channel, messageBuffer) => {
    try {
        const feature = decode(messageBuffer);
        if (feature && feature.symbol) {
            lastTickData.set(feature.symbol, feature);
            
            const now = Date.now();
            // TỐI ƯU 1: Chỉ phát sự kiện tick mỗi 500ms (giảm 80% tải cho UI)
            if (now - lastTickEmitTime > 500) {
                // Đóng gói tất cả ticks hiện tại thành 1 mảng để gửi 1 lần
                const allTicks = Array.from(lastTickData.values());
                io.emit('live_features_batch', allTicks);
                lastTickEmitTime = now;
            }
        }
    } catch (e) {}
});

subClient.on('message', (channel, message) => {
    try {
        const data = JSON.parse(message);
        if (channel === 'dashboard:logs') {
            const sym = data.symbol || 'SYSTEM';
            if (!aiLogHistory[sym]) aiLogHistory[sym] = [];
            
            aiLogHistory[sym].push(data);
            
            // TỐI ƯU 2: Giữ tối đa 30 log mỗi coin trên RAM server
            if (aiLogHistory[sym].length > 30) aiLogHistory[sym].shift();
            
            io.emit('ai_thoughts', data); 
        } 
        else if (channel === 'dashboard:trades') {
            io.emit('new_trade', data); 
        }
    } catch (e) {}
});

io.on('connection', (socket) => {
    console.log("👤 [DASHBOARD] Một người dùng đã kết nối.");
    socket.emit('full_state', {
        logs: aiLogHistory, 
        trades: fullTradeHistory, 
        ticks: Array.from(lastTickData.values())
    });
});

// ==========================================
// 5. START SERVER
// ==========================================
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`\n🚀 [WATCHTOWER V17] Giao diện đang trực tại cổng ${PORT}`);
});