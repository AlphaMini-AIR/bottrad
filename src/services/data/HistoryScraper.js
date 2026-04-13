/**
 * src/services/data/HistoryScraper.js
 */
const axios = require('axios');
const Candle1m = require('../../models/Candle1m');

class HistoryScraper {
    async fetch30Days(symbol) {
        console.log(`📥 [SCRAPER] Bắt đầu hút 30 ngày dữ liệu cho ${symbol.toUpperCase()}...`);
        const limit = 1500;
        const totalNeeded = 43200;
        let allKlines = [];
        let endTime = Date.now();

        while (allKlines.length < totalNeeded) {
            try {
                const res = await axios.get('https://fapi.binance.com/fapi/v1/klines', {
                    params: { symbol: symbol.toUpperCase(), interval: '1m', limit, endTime }
                });

                if (res.data.length === 0) break;

                allKlines.push(...res.data);
                endTime = res.data[0][0] - 1; // Lùi thời gian về trước cây nến cũ nhất vừa lấy
                console.log(`   - Đã hút: ${allKlines.length}/${totalNeeded} nến...`);

                // Tránh bị Binance khóa IP
                await new Promise(r => setTimeout(r, 300));
            } catch (err) {
                console.error('❌ Lỗi hút data:', err.message);
                break;
            }
        }

        // Lưu vào MongoDB (Sử dụng bulkWrite để tối ưu tốc độ)
        const ops = allKlines.map(k => ({
            insertOne: {
                document: {
                    timestamp: new Date(k[0]),
                    symbol: symbol.toUpperCase(),
                    open: parseFloat(k[1]),
                    high: parseFloat(k[2]),
                    low: parseFloat(k[3]),
                    close: parseFloat(k[4]),
                    volume: parseFloat(k[5]),
                    decayed_obi: 0, // Dữ liệu lịch sử không có OBI real-time, ta sẽ giả lập trung bình
                    is_closed: true
                }
            }
        }));

        await Candle1m.bulkWrite(ops);
        console.log(`✅ [SCRAPER] Đã lưu 30 ngày nến cho ${symbol.toUpperCase()}.\n`);
    }
}

module.exports = new HistoryScraper();