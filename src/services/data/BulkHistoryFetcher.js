/**
 * src/services/data/BulkHistoryFetcher.js
 */
const axios = require('axios');
const MarketData = require('../../models/MarketData'); // Đảm bảo dùng Model MarketData mới
const connectDB = require('../../config/db');

const BASE_URL = 'https://fapi.binance.com';
const SIX_MONTHS_MS = 180 * 24 * 60 * 60 * 1000;

class BulkHistoryFetcher {
    async fetchHistory(symbol) {
        console.log(`\n📂 [DATABASE-BACKFILL] Bắt đầu tải 6 tháng dữ liệu cho ${symbol}...`);

        const endTime = Date.now();
        const startTime = endTime - SIX_MONTHS_MS;
        let currentPointer = startTime;

        while (currentPointer < endTime) {
            try {
                // Binance chỉ cho phép tối đa 1500 nến/lần gọi
                const res = await axios.get(`${BASE_URL}/fapi/v1/klines`, {
                    params: {
                        symbol: symbol.toUpperCase(),
                        interval: '1m',
                        startTime: currentPointer,
                        limit: 1500
                    }
                });

                if (res.data.length === 0) break;

                const batch = res.data.map(k => ({
                    symbol: symbol.toUpperCase(),
                    timestamp: k[0],
                    open: parseFloat(k[1]),
                    high: parseFloat(k[2]),
                    low: parseFloat(k[3]),
                    close: parseFloat(k[4]),
                    volume: parseFloat(k[5]),
                    taker_buy_base: parseFloat(k[9]), // Lực mua chủ động
                    // Các thông số vĩ mô > 30 ngày sẽ để null, AI sẽ tự xử lý
                    open_interest: null,
                    long_short_ratio: null,
                    funding_rate: 0.0001,
                    isStaleData: false
                }));

                // Lưu hàng loạt vào DB để tiết kiệm tài nguyên
                const ops = batch.map(doc => ({
                    updateOne: {
                        filter: { symbol: doc.symbol, timestamp: doc.timestamp },
                        update: { $set: doc },
                        upsert: true
                    }
                }));

                await MarketData.bulkWrite(ops);

                const lastTimestamp = batch[batch.length - 1].timestamp;
                currentPointer = lastTimestamp + 1;

                console.log(`   ⏳ Đã lưu đến: ${new Date(lastTimestamp).toLocaleString()} | Tiến độ: ${(((lastTimestamp - startTime) / SIX_MONTHS_MS) * 100).toFixed(2)}%`);

                // Nghỉ ngắn để tránh bị Binance chặn (Rate Limit)
                await new Promise(r => setTimeout(r, 300));

            } catch (error) {
                console.error(`❌ Lỗi tại mốc thời gian ${currentPointer}:`, error.message);
                await new Promise(r => setTimeout(r, 1000));
            }
        }
    }

    async run() {
        await connectDB();
        const coins = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT'];
        for (const coin of coins) {
            await this.fetchHistory(coin);
        }
        console.log('\n✅ [DATABASE] Đã nạp đầy dữ liệu 6 tháng cho 3 coin!');
        process.exit();
    }
}

new BulkHistoryFetcher().run();