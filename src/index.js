const connectDB = require('./config/db');
const HistoryScraper = require('./services/data/HistoryScraper');
const LearningEngine = require('./services/ai/LearningEngine');
const StreamAggregator = require('./services/binance/StreamAggregator');
const { startServer } = require('./api/server');

async function main() {
    await connectDB();

    const testCoins = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT'];

    // 1. HÚT DỮ LIỆU (Chỉ chạy 1 lần nếu DB trống)
    for (const coin of testCoins) {
        const count = await require('./models/Candle1m').countDocuments({ symbol: coin });
        if (count < 40000) {
            await HistoryScraper.fetch30Days(coin);
        }
    }

    // 2. HUẤN LUYỆN AI
    for (const coin of testCoins) {
        await LearningEngine.train(coin);
    }

    // 3. CHẠY HỆ THỐNG
    startServer();
    StreamAggregator.start();

    console.log('🚀 [SYSTEM] AI Quant Sniper đã nạp đầy tri thức và bắt đầu săn mồi!');
}

main();