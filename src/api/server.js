/**
 * src/api/server.js
 * Nhiệm vụ: Cổng giao tiếp REST API phục vụ cho Dashboard Next.js 16.
 * Tính năng: Trả về Bảng xếp hạng (Ranks) và Dữ liệu biểu đồ (Charts).
 */
const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const { getDbConnection } = require('../config/db');
const MarketDataSchema = require('../models/MarketData');

const app = express();
const PORT = process.env.API_PORT || 3001; // Chạy ở port 3001 để không đụng port web
const TIER_LIST_PATH = path.join(__dirname, '../../tier_list.json');

// Cấu hình CORS để Vercel (Next.js) có thể gọi vào VPS của bạn mà không bị block
app.use(cors());
app.use(express.json());

// ---------------------------------------------------------
// API 1: Lấy danh sách phân Tầng & Thứ hạng (Cho Leaderboard)
// ---------------------------------------------------------
app.get('/api/ranks', (req, res) => {
    try {
        if (!fs.existsSync(TIER_LIST_PATH)) {
            return res.status(404).json({ error: "Không tìm thấy tier_list.json" });
        }
        const tierData = JSON.parse(fs.readFileSync(TIER_LIST_PATH, 'utf-8'));

        // Chỉ trả về Ranks và Theo dõi Tầng 3 (Giấu Storage Map đi cho bảo mật)
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
// API 2: Lấy dữ liệu Biểu đồ nến Lịch sử (Cho Lightweight Charts)
// ---------------------------------------------------------
app.get('/api/charts/:symbol', async (req, res) => {
    const symbol = req.params.symbol.toUpperCase();

    try {
        // 1. Đọc Storage Map để biết rút data từ DB nào
        const tierData = JSON.parse(fs.readFileSync(TIER_LIST_PATH, 'utf-8'));
        const storageNode = tierData.storage_map[symbol];

        if (!storageNode) {
            return res.status(404).json({ error: `Coin ${symbol} không nằm trong hệ thống thi đấu.` });
        }

        // 2. Kết nối đúng Cluster
        const connection = getDbConnection(storageNode);
        const MarketDataModel = connection.model('MarketData', MarketDataSchema);

        // 3. Truy vấn giới hạn 1440 nến (1 ngày gần nhất)
        const candles = await MarketDataModel.find({ symbol: symbol })
            .sort({ openTime: -1 }) // Sắp xếp giảm dần để lấy mới nhất
            .limit(1440)
            .lean();

        if (!candles || candles.length === 0) {
            return res.json([]);
        }

        // 4. Sắp xếp lại theo chiều tăng dần cho TradingView dễ vẽ
        candles.sort((a, b) => a.openTime - b.openTime);

        // 5. Chuẩn hóa format trả về cho Lightweight Charts
        const formattedData = candles.map(c => ({
            time: Math.floor(c.openTime / 1000), // TradingView dùng UNIX Timestamp (giây)
            open: c.ohlcv.open,
            high: c.ohlcv.high,
            low: c.ohlcv.low,
            close: c.ohlcv.close,
            volume: c.ohlcv.volume,
            isStaleData: c.isStaleData || false // Chuyển cờ rác lên UI để tô màu xám
        }));

        // Trả về kèm header Cache-Control để chống spam request (Giữ cache 10 giây)
        res.setHeader('Cache-Control', 'public, s-maxage=10, stale-while-revalidate=59');
        res.json(formattedData);

    } catch (err) {
        console.error(`❌ [API] Lỗi khi lấy biểu đồ ${symbol}:`, err.message);
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