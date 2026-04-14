/**
 * src/services/execution/LiveExchange.js
 */
const axios = require('axios');
const crypto = require('crypto');
require('dotenv').config();

class LiveExchange {
    constructor() {
        this.apiKey = process.env.BINANCE_API_KEY || 'DEMO_KEY';
        this.apiSecret = process.env.BINANCE_API_SECRET || 'DEMO_SECRET';
        this.baseURL = 'https://fapi.binance.com';
    }

    // Ký xác thực API Binance
    signRequest(queryString) {
        return crypto.createHmac('sha256', this.apiSecret).update(queryString).digest('hex');
    }

    // Gửi Request POST lên Binance
    async sendPostRequest(endpoint, params) {
        const queryString = new URLSearchParams(params).toString();
        const signature = this.signRequest(queryString);
        const url = `${this.baseURL}${endpoint}?${queryString}&signature=${signature}`;

        // Cấu hình Header
        const headers = { 'X-MBX-APIKEY': this.apiKey };

        try {
            const response = await axios.post(url, null, { headers });
            return response.data;
        } catch (error) {
            const msg = error.response ? error.response.data.msg : error.message;
            throw new Error(`[BINANCE API ERROR] ${msg}`);
        }
    }

    // Chỉnh Đòn bẩy (Leverage) tự động
    async setLeverage(symbol, leverage) {
        try {
            const params = {
                symbol: symbol.toUpperCase(),
                leverage: leverage,
                timestamp: Date.now()
            };
            await this.sendPostRequest('/fapi/v1/leverage', params);
            console.log(`   ⚙️ Đã set Đòn bẩy ${symbol} lên x${leverage}`);
        } catch (error) {
            console.error(error.message);
        }
    }

    /**
     * Bắn Chuỗi Lệnh: Entry Market + Cắt Lỗ + Chốt Lời (Chuẩn closePosition của V4.1)
     */
    async executeTrade(symbol, side, quantity, currentPrice, slPrice, tpPrice) {
        console.log(`\n🔥 [LIVE TRADE] BẮT ĐẦU CHUỖI LỆNH ${side} ${symbol.toUpperCase()}`);

        // --- BƯỚC 1: LÁCH LUẬT MINIMUM NOTIONAL ---
        const notionalValue = quantity * currentPrice;
        let requiredLeverage = 1;

        // Nếu tổng giá trị lệnh < 55 USDT, ta cần đẩy đòn bẩy lên để lách luật sàn
        if (notionalValue < 55) {
            requiredLeverage = Math.ceil(55 / notionalValue) + 1; // Cộng thêm 1 cho chắc chắn
            if (requiredLeverage > 125) requiredLeverage = 125; // Max đòn bẩy Binance
            console.log(`   ⚠️ Giá trị lệnh (${notionalValue.toFixed(2)}$) < 55$. Kích hoạt Auto-Leverage x${requiredLeverage}.`);
            await this.setLeverage(symbol, requiredLeverage);
        } else {
            // Set mặc định đòn bẩy an toàn
            await this.setLeverage(symbol, 10);
        }

        // --- BƯỚC 2: BẮN LỆNH MỞ VỊ THẾ (ENTRY) ---
        try {
            const entryParams = {
                symbol: symbol.toUpperCase(),
                side: side,
                type: 'MARKET',
                quantity: quantity,
                timestamp: Date.now()
            };
            // UNCOMMENT DÒNG DƯỚI ĐỂ CHẠY THẬT (Hiện tại log ra để Test an toàn)
            // await this.sendPostRequest('/fapi/v1/order', entryParams);
            console.log(`   ✅ Bắn Lệnh ENTRY: MARKET ${side} | Size: ${quantity}`);

            // --- BƯỚC 3: BẮN LỆNH CẮT LỖ VÀ CHỐT LỜI (Cờ closePosition = true) ---
            // Lệnh Đóng phải ngược chiều với lệnh Mở
            const exitSide = side === 'LONG' ? 'SELL' : 'BUY';

            const slParams = {
                symbol: symbol.toUpperCase(),
                side: exitSide,
                type: 'STOP_MARKET',
                stopPrice: slPrice,
                closePosition: 'true', // CHUẨN PSD V4.1: Đóng toàn bộ, tuyệt đối không truyền quantity
                timestamp: Date.now()
            };

            const tpParams = {
                symbol: symbol.toUpperCase(),
                side: exitSide,
                type: 'TAKE_PROFIT_MARKET',
                stopPrice: tpPrice,
                closePosition: 'true', // CHUẨN PSD V4.1
                timestamp: Date.now()
            };

            // UNCOMMENT ĐỂ CHẠY THẬT
            // await this.sendPostRequest('/fapi/v1/order', slParams);
            // await this.sendPostRequest('/fapi/v1/order', tpParams);

            console.log(`   🛡️ Bắn Lệnh STOP LOSS: ${exitSide} tại giá ${slPrice} (Cờ: closePosition)`);
            console.log(`   🎯 Bắn Lệnh TAKE PROFIT: ${exitSide} tại giá ${tpPrice} (Cờ: closePosition)`);
            console.log(`🎉 CHUỖI LỆNH ĐÃ THIẾT LẬP THÀNH CÔNG!\n`);

            return true;

        } catch (error) {
            console.error('❌ [LIVE TRADE] Thất bại ở luồng Order:', error.message);
            return false;
        }
    }
}

module.exports = new LiveExchange();