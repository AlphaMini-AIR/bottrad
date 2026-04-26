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
    config = { REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379', CHANNELS: { FEATURES: 'market:features:*' } };
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(express.static(path.join(__dirname, 'public')));

// Bộ nhớ đệm (RAM)
let aiLogHistory = {}; 
let lastTickData = new Map(); 

const subClient = new Redis(config.REDIS_URL);
subClient.psubscribe(config.CHANNELS.FEATURES);
subClient.subscribe('dashboard:logs', 'dashboard:trades'); // Nghe thêm kênh trades

// Kết nối MongoDB
if (process.env.MONGO_URI_SCOUT) {
    mongoose.connect(process.env.MONGO_URI_SCOUT).then(() => {
        console.log("📦 [WATCHTOWER] Đã kết nối MongoDB Scout.");
    }).catch(err => console.error("❌ [WATCHTOWER] Lỗi kết nối MongoDB:", err.message));
}

// REST API
app.get('/api/trades', async (req, res) => {
    try {
        // Lấy full cả lệnh OPEN và CLOSED, xếp lệnh mới nhất lên đầu
        const trades = await ScoutTrade.find().sort({ openTime: -1 }).limit(100);
        res.json(trades);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/stats', async (req, res) => {
    try {
        const trades = await ScoutTrade.find({ status: 'CLOSED' });
        const openTrades = await ScoutTrade.find({ status: 'OPEN' });
        
        let totalTrades = trades.length;
        let winningTrades = trades.filter(t => t.pnl > 0).length;
        let winRate = totalTrades > 0 ? (winningTrades / totalTrades) * 100 : 0;
        let totalPnL = trades.reduce((sum, t) => sum + (t.pnl || 0), 0);
        
        // Vốn gốc $200
        const STARTING_BALANCE = 200;
        let currentBalance = STARTING_BALANCE + totalPnL;

        res.json({
            totalTrades: totalTrades + openTrades.length, 
            winningTrades, 
            winRate: winRate.toFixed(2) + '%',
            netPnL: totalPnL.toFixed(2) + ' USDT',
            openPositions: openTrades.length,
            currentWalletEstimation: currentBalance.toFixed(2) + ' USDT'
        });
    } catch (err) { res.status(500).json({ error: err.message }); }
});

app.get('/api/equity-curve', async (req, res) => {
    try {
        const trades = await ScoutTrade.find({ status: 'CLOSED' }).sort({ closeTime: 1 });
        let currentBalance = 200; // Vốn gốc
        let equityData = [{ time: 'Start', balance: currentBalance }];

        trades.forEach(t => {
            currentBalance += (t.pnl || 0);
            equityData.push({
                time: t.closeTime,
                balance: parseFloat(currentBalance.toFixed(2)),
                symbol: t.symbol
            });
        });
        res.json(equityData);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// WEBSOCKET
io.on('connection', async (socket) => {
    const initialTrades = await ScoutTrade.find().sort({ openTime: -1 }).limit(100);
    socket.emit('full_state', {
        logs: aiLogHistory, 
        trades: initialTrades, 
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
            
            // Tối ưu: Chỉ giữ tối đa 50 log gần nhất mỗi coin (Khoảng 10-15s tùy tần suất)
            if (aiLogHistory[sym].length > 50) {
                aiLogHistory[sym].shift();
            }
            
            io.emit('ai_thoughts', logData); 
        } catch (e) {}
    } 
    else if (channel === 'dashboard:trades') {
        // Nhận tín hiệu Real-time khi OrderManager bóp cò hoặc chốt lời
        try {
            const tradeUpdate = JSON.parse(message);
            io.emit('trade_update', tradeUpdate);
        } catch (e) {}
    }
});

subClient.on('pmessageBuffer', (pattern, channel, messageBuffer) => {
    let feature;
    try { feature = decode(messageBuffer); } catch (e) {
        try { feature = JSON.parse(messageBuffer.toString()); } catch (err) { return; }
    }
    if (!feature || !feature.symbol || !feature.is_warm) return; 

    lastTickData.set(feature.symbol, feature);
    io.emit('live_features', feature);
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🌐 [WATCHTOWER] Dashboard V17 đang trực tại Port ${PORT}`));