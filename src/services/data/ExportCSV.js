/**
 * src/services/data/ExportCSV.js - Bản gộp dữ liệu 3 Coin
 */
const fs = require('fs');
const path = require('path');
const connectDB = require('../../config/db');
const Candle1m = require('../../models/Candle1m');

async function exportMultiToCSV() {
    await connectDB();
    const symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT'];
    const csvPath = path.join(__dirname, `../../../dataset_multi_3months.csv`);
    const writeStream = fs.createWriteStream(csvPath);

    writeStream.write('timestamp,symbol,open,high,low,close,volume\n');

    for (const symbol of symbols) {
        console.log(`📥 Đang trích xuất ${symbol}...`);
        const data = await Candle1m.find({ symbol }).sort({ timestamp: 1 }).lean();

        data.forEach(d => {
            writeStream.write(`${d.timestamp.getTime()},${symbol},${d.open},${d.high},${d.low},${d.close},${d.volume}\n`);
        });
        console.log(`   ✅ Đã nạp ${data.length} nến ${symbol}`);
    }

    writeStream.end();
    writeStream.on('finish', () => {
        console.log(`\n🚀 HOÀN TẤT! File gộp đã sẵn sàng: ${csvPath}`);
        process.exit(0);
    });
}

exportMultiToCSV();