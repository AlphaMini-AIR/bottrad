/**
 * src/api/server.js
 * Nhiệm vụ: Cổng giao tiếp REST API phục vụ cho Dashboard Next.js 16.
 * Tính năng: Ranks, Charts, Account, Active Trades, Trade History & Stats.
 */
const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const { getDbConnection } = require('../config/db');
const MarketDataSchema = require('../models/MarketData');
const PaperAccountSchema = require('../models/PaperAccount');
const PaperTradeSchema = require('../models/PaperTrade');
const paperExchange = require('../services/execution/PaperExchange'); // Lấy dữ liệu lệnh từ RAM

const app = express();
const PORT = process.env.API_PORT || 3001; 
const TIER_LIST_PATH = path.join(__dirname, '../../tier_list.json');

app.use(cors());
app.use(express.json());

// ---------------------------------------------------------
// API 1: Lấy danh sách phân Tầng & Thứ hạng 
// ---------------------------------------------------------
app.get('/api/ranks', (req, res) => {
    try {
        if (!fs.existsSync(TIER_LIST_PATH)) {
            return res.status(404).json({ error: "Không tìm thấy tier_list.json" });
        }
        const tierData = JSON.parse(fs.readFileSync(TIER_LIST_PATH, 'utf-8'));

        res.json({
            last_update: tierData.last_update,
            ranks: tierData.ranks,
            scout_tracking: tierData.scout_tracking || {}
        });
    } catch (err) {
        res.status(500).json({ error: "Lỗi hệ thống khi đọc Ranks" });
    }
});

// ---------------------------------------------------------
// API 2: Lấy dữ liệu Biểu đồ nến Lịch sử 
// ---------------------------------------------------------
app.get('/api/charts/:symbol', async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();
    try {
        const tierData = JSON.parse(fs.readFileSync(TIER_LIST_PATH, 'utf-8'));
        const storageNode = tierData.storage_map[symbol];

        if (!storageNode) {
            return res.status(404).json({ error: `Coin ${symbol} không có trong hệ thống.` });
        }

        const connection = getDbConnection(storageNode);
        const MarketDataModel = connection.model('MarketData', MarketDataSchema);

        const candles = await MarketDataModel.find({ symbol: symbol })
            .sort({ openTime: -1 }) 
            .limit(1440) // 1 ngày nến 1 phút
            .lean();

        if (!candles || candles.length === 0) return res.json([]);

        candles.sort((a, b) => a.openTime - b.openTime);

        const formattedData = candles.map(c => ({
            time: Math.floor(c.openTime / 1000), 
            open: c.ohlcv.open,
            high: c.ohlcv.high,
            low: c.ohlcv.low,
            close: c.ohlcv.close,
            volume: c.ohlcv.volume,
            isStaleData: c.isStaleData || false 
        }));

        res.setHeader('Cache-Control', 'public, s-maxage=10, stale-while-revalidate=59');
        res.json(formattedData);
    } catch (err) {
        console.error(`❌ [API] Lỗi khi lấy biểu đồ ${symbol}:`, err.message);
        res.status(500).json({ error: "Lỗi nội bộ máy chủ" });
    }
});

// ---------------------------------------------------------
// API 3: Lấy thông tin Tài khoản (Ví Quỹ)
// ---------------------------------------------------------
app.get('/api/account', async (req, res) => {
    try {
        // Tài khoản luôn lưu ở Tầng 1 (tier1)
        const connection = getDbConnection('tier1');
        const AccountModel = connection.model('PaperAccount', PaperAccountSchema);
        
        let account = await AccountModel.findOne({ accountId: 'MAIN_PAPER' }).lean();
        if (!account) {
            account = { balance: 1000, initialBalance: 1000 };
        }
        res.json(account);
    } catch (err) {
        res.status(500).json({ error: "Lỗi khi lấy dữ liệu tài khoản" });
    }
});

// ---------------------------------------------------------
// API 4: Lấy các lệnh ĐANG MỞ (Realtime từ RAM)
// ---------------------------------------------------------
app.get('/api/trades/active', (req, res) => {
    try {
        // Lấy Map activeTrades từ PaperExchange và chuyển thành Array
        const activeTrades = Array.from(paperExchange.activeTrades.values());
        res.json(activeTrades);
    } catch (err) {
        res.status(500).json({ error: "Lỗi khi lấy lệnh đang mở" });
    }
});

// ---------------------------------------------------------
// API 5: Lấy Lịch sử giao dịch & Thống kê (Truy vấn 3 Cụm DB)
// ---------------------------------------------------------
app.get('/api/trades/history', async (req, res) => {
    const { symbol, limit = 50 } = req.query;

    try {
        const tierData = JSON.parse(fs.readFileSync(TIER_LIST_PATH, 'utf-8'));
        let trades = [];

        // Nếu Next.js yêu cầu xem lịch sử của 1 coin cụ thể (Khi người dùng click vào coin đó)
        if (symbol) {
            const storageNode = tierData.storage_map[symbol.toUpperCase()] || 'scout';
            const connection = getDbConnection(storageNode);
            const TradeModel = connection.model('PaperTrade', PaperTradeSchema);
            
            trades = await TradeModel.find({ symbol: symbol.toUpperCase() })
                .sort({ closeTime: -1 })
                .limit(parseInt(limit))
                .lean();
        } 
        // Nếu Next.js yêu cầu danh sách tổng (Bảng Leaderboard)
        else {
            // Quét song song cả 3 database để gom lịch sử lệnh
            const nodes = ['tier1', 'tier2', 'scout'];
            const allPromises = nodes.map(node => {
                const connection = getDbConnection(node);
                const TradeModel = connection.model('PaperTrade', PaperTradeSchema);
                return TradeModel.find().sort({ closeTime: -1 }).limit(30).lean();
            });
            
            const results = await Promise.all(allPromises);
            // Gộp data từ 3 DB, sắp xếp lại theo thời gian và cắt lấy 50 lệnh mới nhất
            trades = results.flat()
                .sort((a, b) => b.closeTime - a.closeTime)
                .slice(0, parseInt(limit));
        }

        res.json(trades);
    } catch (err) {
        console.error(`❌ [API] Lỗi khi lấy lịch sử lệnh:`, err.message);
        res.status(500).json({ error: "Lỗi nội bộ máy chủ" });
    }
});

// ---------------------------------------------------------
// Khởi động Express Server
// ---------------------------------------------------------
const startApiServer = () => {
    app.listen(PORT, () => {
        console.log(`🌐 [API SERVER] Đã mở cổng ${PORT} phục vụ Dashboard Next.js!`);
    });
};

module.exports = { startApiServer };