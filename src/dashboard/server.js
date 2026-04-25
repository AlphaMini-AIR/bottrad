const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const mongoose = require('mongoose');
const fs = require('fs');
const path = require('path');
const ScoutTrade = require('../models/ScoutTrade');

const config = JSON.parse(fs.readFileSync(path.join(__dirname, '../../system_config.json'), 'utf8'));
require('dotenv').config({ path: path.join(__dirname, '../../.env') }); 

const app = express();
const server = http.createServer(app);
const io = new Server(server);
app.use(express.static(path.join(__dirname, 'public')));

let fullTradeHistory = []; 
let aiLogHistory = {}; 
let lastTickData = new Map(); 

const subClient = new Redis(config.REDIS_URL);
subClient.psubscribe(config.CHANNELS.FEATURES);
subClient.subscribe('dashboard:logs', 'dashboard:trades');

mongoose.createConnection(process.env.MONGO_URI_SCOUT).asPromise().then(async (db) => {
    console.log("📦 Dashboard đã kết nối MongoDB.");
    // Cache lịch sử khi khởi động
    try {
        const trades = await db.collection('scouttrades').find().sort({ts: -1}).limit(200).toArray();
        fullTradeHistory = trades;
    } catch (e) {}
});

// ==========================================
// REST API CHO BÁO CÁO VÀ BIỂU ĐỒ (SPRINT 4)
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
        let totalFees = trades.reduce((sum, t) => sum + t.entryFee + t.exitFee + t.fundingFee, 0);

        res.json({
            totalTrades, winningTrades, winRate: winRate.toFixed(2) + '%',
            netPnL: totalPnL.toFixed(2) + ' USDT',
            totalFeesPaidToBinance: totalFees.toFixed(2) + ' USDT',
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
                time: t.closeTime,
                balance: parseFloat(currentBalance.toFixed(2)),
                symbol: t.symbol
            });
        });
        res.json(equityData);
    } catch (err) { res.status(500).json({ error: err.message }); }
});

// ==========================================
// THÔNG TIN THỜI GIAN THỰC (WEBSOCKETS)
// ==========================================
io.on('connection', (socket) => {
    socket.emit('full_state', {
        logs: aiLogHistory, trades: fullTradeHistory, ticks: Array.from(lastTickData.values())
    });
});

subClient.on('message', (channel, message) => {
    if (channel === 'dashboard:logs') {
        const logData = JSON.parse(message);
        const sym = logData.symbol;
        if (!aiLogHistory[sym]) aiLogHistory[sym] = [];
        
        aiLogHistory[sym].push(logData);
        const fifteenMinsAgo = Date.now() - 900000;
        aiLogHistory[sym] = aiLogHistory[sym].filter(l => l.ts >= fifteenMinsAgo);
        
        io.emit('ai_thoughts', logData); 
    }
});

subClient.on('pmessageBuffer', (pattern, channel, messageBuffer) => {
    try {
        const feature = decode(messageBuffer);
        if (feature.is_warm) {
            lastTickData.set(feature.symbol, feature);
            io.emit('live_features', feature);
        }
    } catch (e) {}
});

server.listen(3000, () => console.log(`🌐 Stateful Watchtower đang chạy ở Port 3000`));