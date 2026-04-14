/**
 * prepare_dataset.js - Script hỗ trợ test AI
 * Kéo nhanh 1500 nến lịch sử từ Binance để Python có data huấn luyện.
 */
const axios = require('axios');
const fs = require('fs');

async function generateDataset() {
    try {
        console.log("⏳ Đang kéo 1500 nến lịch sử từ Binance...");
        
        // Kéo klines limit tối đa của Binance là 1500
        const res = await axios.get('https://fapi.binance.com/fapi/v1/klines', {
            params: { symbol: 'TAOUSDT', interval: '1m', limit: 2500 }
        });

        // Định nghĩa Headers chuẩn cho file CSV
        const headers = ['symbol', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'funding_rate'];
        let csvContent = headers.join(',') + '\n';

        // Lặp qua dữ liệu Binance và chuyển thành CSV
        res.data.forEach(kline => {
            // Funding_rate fake 0.0001 để test logic tính Delta của AI
            csvContent += `BTCUSDT,${kline[0]},${kline[1]},${kline[2]},${kline[3]},${kline[4]},${kline[5]},0.0001\n`;
        });

        // Ghi ra file CSV
        fs.writeFileSync('dataset_multi_3months.csv', csvContent);
        console.log(`✅ Đã tạo thành công dataset_multi_3months.csv với ${res.data.length} nến!`);
        console.log(`👉 BẠN ĐÃ CÓ THỂ CHẠY LẠI: python train_master.py`);

    } catch (error) {
        console.error("❌ Lỗi kéo dữ liệu:", error.message);
    }
}

generateDataset();