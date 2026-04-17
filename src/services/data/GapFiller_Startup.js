/**
 * src/services/data/GapFiller_Startup.js
 * Nhiệm vụ: Tự động vá lỗ hổng dữ liệu khi bot bị sập mạng/restart.
 */
require('dotenv').config();
const { MongoClient } = require('mongodb');
const axios = require('axios');

const MONGO_URI =  'mongodb+srv://assistantsupdev_db_user:rCp0BrUushhwIKR8@binance.bjmukc0.mongodb.net/?appName=Binance';
const DB_NAME = 'Binance';
const COLLECTION_NAME = 'market_data_live';

async function healDataGaps(symbols) {
    console.log(`\n🩺 [SELF-HEALING] Khởi động Cơ chế tự chữa lành cho ${symbols.length} coins...`);
    const client = new MongoClient(MONGO_URI);

    try {
        await client.connect();
        const db = client.db(DB_NAME);
        const collection = db.collection(COLLECTION_NAME);

        for (const symbol of symbols) {
            // 1. Tìm cây nến cuối cùng của coin này trong DB
            const lastCandle = await collection.findOne(
                { symbol: symbol },
                { sort: { openTime: -1 } } // Sắp xếp giảm dần để lấy cái mới nhất
            );

            let startTime = 0;
            const now = Date.now();

            if (!lastCandle) {
                console.log(`[${symbol}] DB trống. Kéo 60 phút gần nhất làm vốn...`);
                startTime = now - (60 * 60 * 1000); // Lùi lại 60 phút
            } else {
                startTime = lastCandle.openTime + 60000; // Bắt đầu từ cây nến TIẾP THEO
            }

            // Nếu khoảng cách chưa tới 1 phút -> Bot vẫn đang mượt, không cần vá
            if (startTime > now - 60000) {
                console.log(`✅ [${symbol}] Dữ liệu liền mạch, không bị mất nến nào.`);
                continue;
            }

            console.log(`⚠️ [${symbol}] Phát hiện rớt mạng! Đang vá dữ liệu từ ${new Date(startTime).toLocaleString()}...`);

            // 2. Kéo dữ liệu API thô từ Binance
            const res = await axios.get('https://fapi.binance.com/fapi/v1/klines', {
                params: {
                    symbol: symbol,
                    interval: '1m',
                    startTime: startTime,
                    endTime: now,
                    limit: 1000 // Tối đa 1000 nến 1 lần
                }
            });

            const klines = res.data;
            if (klines.length === 0) {
                console.log(`[${symbol}] Không có dữ liệu khuyết nào cần vá.`);
                continue;
            }

            // 3. Nhào nặn dữ liệu thô thành form CHUẨN của hệ thống (có cờ isStaleData)
            const records = klines.map(k => ({
                symbol: symbol,
                openTime: k[0],
                ohlcv: {
                    open: parseFloat(k[1]),
                    high: parseFloat(k[2]),
                    low: parseFloat(k[3]),
                    close: parseFloat(k[4]),
                    volume: parseFloat(k[5]),
                    quoteVolume: parseFloat(k[7]),
                    trades: k[8],
                    takerBuyBase: parseFloat(k[9]),
                    takerBuyQuote: parseFloat(k[10])
                },
                micro: {
                    ob_imb_top20: 0.5,  // Không có Sổ lệnh thật -> Để 0.5 (Cân bằng)
                    spread_close: 0,
                    bid_vol_1pct: 0,
                    ask_vol_1pct: 0,
                    max_buy_trade: 0,   // Không có tick data thật -> Để 0
                    max_sell_trade: 0,
                    liq_long_vol: 0,
                    liq_short_vol: 0
                },
                macro: {
                    funding_rate: 0,
                    open_interest: 0
                },
                // BẬT CỜ CẢNH BÁO: Đây là nến chắp vá, AI cẩn thận khi học!
                isStaleData: true
            }));

            // 4. Nhét vào MongoDB an toàn (Vẫn dùng khiên bảo vệ)
            const bulkOps = records.map(record => ({
                updateOne: {
                    filter: { symbol: record.symbol, openTime: record.openTime },
                    update: { $setOnInsert: record },
                    upsert: true
                }
            }));

            const result = await collection.bulkWrite(bulkOps, { ordered: false });
            console.log(`🛠️ [${symbol}] Đã vá xong ${result.upsertedCount} nến bị khuyết!`);

            // Dừng 0.5s để tránh bị Binance ban IP khi vòng lặp chạy quá nhanh
            await new Promise(resolve => setTimeout(resolve, 500));
        }

        console.log(`🏁 [SELF-HEALING] Quá trình tự chữa lành hoàn tất.\n`);

    } catch (err) {
        console.error("❌ [SELF-HEALING] Lỗi:", err.message);
    } finally {
        await client.close();
    }
}

module.exports = { healDataGaps };