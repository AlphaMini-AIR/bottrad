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

// Bộ nhớ đệm RAM để tăng tốc hiển thị cho người dùng mới
let fullTradeHistory = []; 
let aiLogHistory = {}; 
let lastTickData = new Map(); 

const subClient = new Redis(config.REDIS_URL);

// ==========================================
// 2. KẾT NỐI MONGODB TOÀN CỤC (FIX LỖI UNDEFINED)
// ==========================================
const mongoUri = process.env.MONGO_URI_SCOUT || process.env.MONGO_URI;

if (mongoUri) {
    mongoose.connect(mongoUri)
        .then(async () => {
            console.log("📦 [WATCHTOWER] Đã kết nối MongoDB thành công.");
            // Load lịch sử 100 lệnh gần nhất
            try {
                fullTradeHistory = await ScoutTrade.find().sort({ ts: -1 }).limit(100);
            } catch (e) {
                console.warn("⚠️ [DB] Chưa có dữ liệu lệnh cũ.");
            }
        })
        .catch(err => console.error("❌ [WATCHTOWER] Lỗi MongoDB:", err.message));
} else {
    console.warn("⚠️ [WATCHTOWER] Thiếu cấu hình MONGO_URI trong .env");
}

// ==========================================
// 3. API REST - CUNG CẤP DỮ LIỆU KIỂM TOÁN
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
        res.json({ totalTrades: 0, netPnL: '0.00 USDT', winRate: '0.00%' }); // Trả về mặc định khi lỗi
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

// Đăng ký nghe các kênh dữ liệu từ Python Feed và OrderManager
subClient.psubscribe('market:features*');
subClient.subscribe('dashboard:logs', 'dashboard:trades');

subClient.on('pmessageBuffer', (pattern, channel, messageBuffer) => {
    try {
        const feature = decode(messageBuffer); // Giải nén MsgPack từ Python
        if (feature && feature.symbol) {
            lastTickData.set(feature.symbol, feature);
            io.emit('live_features', feature); // Gửi dữ liệu tick lên UI
        }
    } catch (e) {
        // console.error("❌ Lỗi giải mã Tick:", e.message);
    }
});

subClient.on('message', (channel, message) => {
    try {
        const data = JSON.parse(message);
        if (channel === 'dashboard:logs') {
            const sym = data.symbol || 'SYSTEM';
            if (!aiLogHistory[sym]) aiLogHistory[sym] = [];
            aiLogHistory[sym].push(data);
            if (aiLogHistory[sym].length > 100) aiLogHistory[sym].shift();
            io.emit('ai_thoughts', data); // Gửi log suy nghĩ của AI lên UI
        } 
        else if (channel === 'dashboard:trades') {
            io.emit('new_trade', data); // Báo có lệnh mới để UI cập nhật bảng
        }
    } catch (e) {}
});

io.on('connection', (socket) => {
    console.log("👤 [DASHBOARD] Người dùng đã kết nối.");
    // Gửi toàn bộ trạng thái hiện tại ngay khi user mở web
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
    console.log(`🔗 Truy cập: http://localhost:${PORT} hoặc IP-VPS:${PORT}\n`);
});