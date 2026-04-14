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
        console.log(`\n📂 [BACKFILL] Bắt đầu hút dữ liệu cho ${symbol}...`);

        const endTime = Date.now();
        const startTime = endTime - SIX_MONTHS_MS;
        let currentPointer = startTime;

        while (currentPointer < endTime) {
            try {
                const res = await axios.get(`${BASE_URL}/fapi/v1/klines`, {
                    params: {
                        symbol: symbol.toUpperCase(),
                        interval: '1m',
                        startTime: currentPointer,
                        limit: 1500
                    }
                });

                if (!res.data || res.data.length === 0) {
                    // Nếu không có dữ liệu, tự động nhảy 1500 phút (khoảng 25 giờ) để tìm tiếp
                    currentPointer += 1500 * 60000;
                    continue;
                }

                const batch = res.data.map(k => ({
                    symbol: symbol.toUpperCase(),
                    timestamp: k[0],
                    open: parseFloat(k[1]),
                    high: parseFloat(k[2]),
                    low: parseFloat(k[3]),
                    close: parseFloat(k[4]),
                    volume: parseFloat(k[5]),
                    funding_rate: 0.0001,
                    isStaleData: false
                }));

                const ops = batch.map(doc => ({
                    updateOne: {
                        filter: { symbol: doc.symbol, timestamp: doc.timestamp },
                        update: { $set: doc },
                        upsert: true
                    }
                }));

                await MarketData.bulkWrite(ops);

                // Cập nhật Pointer đến nến cuối cùng vừa tải được
                currentPointer = batch[batch.length - 1].timestamp + 60000;

                console.log(`   ⏳ ${symbol}: Đến ngày ${new Date(currentPointer).toLocaleDateString()}`);
                await new Promise(res => setTimeout(res, 300));

            } catch (error) {
                console.error(`❌ Lỗi tại mốc ${currentPointer}: ${error.message}`);

                // QUAN TRỌNG: Nếu lỗi (như 400), ta phải nhảy Pointer lên để không bị treo vòng lặp
                currentPointer += 1500 * 60000;
                await new Promise(res => setTimeout(res, 1000));
            }
        }
    }

    async run() {
        await connectDB();
        const coins = ['SOLUSDT'];
        for (const coin of coins) {
            await this.fetchHistory(coin);
        }
        console.log('\n✅ [DATABASE] Đã nạp đầy dữ liệu 6 tháng cho 1 coin!');
        process.exit();
    }
}

new BulkHistoryFetcher().run();