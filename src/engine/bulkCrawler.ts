import mongoose from 'mongoose';
import axios from 'axios';

const MONGODB_URI = "mongodb+srv://assistantsupdev_db_user:rCp0BrUushhwIKR8@binance.bjmukc0.mongodb.net/?appName=Binance";

const HistoricalKline = mongoose.models.HistoricalKline || mongoose.model('HistoricalKline', new mongoose.Schema({
    s: { type: String, index: true }, i: { type: String, index: true },
    t: { type: Number, index: true }, o: Number, h: Number, l: Number, c: Number, v: Number
}));

const SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BNBUSDT', 'XRPUSDT', 'ADAUSDT', 'FETUSDT']; // Hưng có thể thêm đủ 100 con vào đây
const INTERVAL = '15m';
const START_TIME = new Date('2025-01-01').getTime();
const END_TIME = Date.now();

async function crawlSymbolHistory(symbol: string) {
    let currentStart = START_TIME;
    console.log(`\n[${symbol}] 🚀 Bắt đầu hành trình cào nến từ 2025...`);

    while (currentStart < END_TIME) {
        try {
            // Cào 1000 nến mỗi lần (Binance limit tối đa)
            const url = `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=${INTERVAL}&startTime=${currentStart}&limit=1000`;
            const { data } = await axios.get(url);

            if (!data || data.length === 0) break;

            const klines = data.map((k: any) => ({
                s: symbol, i: INTERVAL, t: k[0],
                o: parseFloat(k[1]), h: parseFloat(k[2]), l: parseFloat(k[3]), c: parseFloat(k[4]), v: parseFloat(k[5])
            }));

            // Lưu vào DB - Dùng bulkWrite để tránh treo máy
            const ops = klines.map((k: any) => ({
                updateOne: {
                    filter: { s: k.s, i: k.i, t: k.t },
                    update: { $set: k },
                    upsert: true
                }
            }));
            await HistoricalKline.bulkWrite(ops);

            // Cập nhật mốc thời gian tiếp theo (thời gian nến cuối + 1ms)
            currentStart = data[data.length - 1][0] + 1;

            const progress = ((currentStart - START_TIME) / (END_TIME - START_TIME) * 100).toFixed(1);
            process.stdout.write(`\r  📊 Tiến độ ${symbol}: ${progress}% `);

            // Nghỉ 200ms để tránh bị Binance khóa IP (Rate limit)
            await new Promise(r => setTimeout(r, 200));

        } catch (error: any) {
            console.error(`\n  ❌ Lỗi tại mốc ${currentStart}: ${error.message}`);
            await new Promise(r => setTimeout(r, 5000)); // Lỗi thì nghỉ 5s rồi thử lại
        }
    }
    console.log(`\n[${symbol}] ✅ Hoàn tất!`);
}

async function main() {
    await mongoose.connect(MONGODB_URI);
    console.log('--- 🛡️ SNIPER BULK CRAWLER v1 ---');

    for (const sym of SYMBOLS) {
        await crawlSymbolHistory(sym);
    }

    console.log('\n🔥 TOÀN BỘ DỮ LIỆU ĐÃ SẴN SÀNG TRONG DB!');
    await mongoose.disconnect();
}

main();