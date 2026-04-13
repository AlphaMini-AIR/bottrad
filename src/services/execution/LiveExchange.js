/**
 * src/services/execution/LiveExchange.js
 */
const axios = require('axios');
const crypto = require('crypto');
require('dotenv').config();

class LiveExchange {
    constructor() {
        this.apiKey = process.env.BINANCE_API_KEY;
        this.apiSecret = process.env.BINANCE_API_SECRET;
        this.baseURL = 'https://fapi.binance.com';
    }

    // Hàm tạo chữ ký bảo mật cho API Binance
    signRequest(queryString) {
        return crypto.createHmac('sha256', this.apiSecret).update(queryString).digest('hex');
    }

    async placeOrder(symbol, side, type, quantity, extraParams = {}) {
        const timestamp = Date.now();
        let queryString = `symbol=${symbol.toUpperCase()}&side=${side}&type=${type}&quantity=${quantity}&timestamp=${timestamp}`;

        // Thêm các tham số phụ (như stopPrice, reduceOnly)
        for (const [key, value] of Object.entries(extraParams)) {
            queryString += `&${key}=${value}`;
        }

        const signature = this.signRequest(queryString);
        queryString += `&signature=${signature}`;

        try {
            const response = await axios.post(`${this.baseURL}/fapi/v1/order?${queryString}`, null, {
                headers: { 'X-MBX-APIKEY': this.apiKey }
            });
            return response.data;
        } catch (error) {
            console.error(`❌ [LIVE API] Lỗi đặt lệnh ${type}:`, error.response ? error.response.data.msg : error.message);
            throw error;
        }
    }

    // Hàm thực thi bộ 3 lệnh: Entry + Cắt Lỗ + Chốt Lời
    async executeTrade(symbol, side, quantity, currentPrice, mode, hardSl, takeProfit) {
        try {
            if (!this.apiKey || !this.apiSecret) {
                console.log('⚠️ [LIVE] Chưa cấu hình API Key. Bỏ qua đặt lệnh thật.');
                return false;
            }

            console.log(`\n🔥 [LIVE TRADE] BẮT ĐẦU CHUỖI LỆNH ${side} ${symbol.toUpperCase()}`);

            // 1. Bắn lệnh Mở Vị thế (Lệnh Market để khớp ngay lập tức)
            await this.placeOrder(symbol, side, 'MARKET', quantity);
            console.log(`   ✅ Đã khớp lệnh Mở (Entry)`);

            // 2. Bắn lệnh Cắt Lỗ Cứng (Stop Market)
            // Phải ngược chiều với lệnh Entry. Vd: Mở LONG thì SL phải là SELL
            const exitSide = side === 'LONG' ? 'SELL' : 'BUY';

            // LƯU Ý THI CÔNG V8.0: Bắt buộc có reduceOnly=true
            await this.placeOrder(symbol, exitSide, 'STOP_MARKET', quantity, {
                stopPrice: hardSl.toFixed(2), // Sàn yêu cầu làm tròn giá
                reduceOnly: 'true'
            });
            console.log(`   🛡️ Đã giăng lưới Cắt Lỗ tại: ${hardSl.toFixed(2)}`);

            // 3. Bắn lệnh Chốt Lời (Take Profit Market)
            await this.placeOrder(symbol, exitSide, 'TAKE_PROFIT_MARKET', quantity, {
                stopPrice: takeProfit.toFixed(2),
                reduceOnly: 'true'
            });
            console.log(`   🎯 Đã đặt mục tiêu Chốt Lời tại: ${takeProfit.toFixed(2)}\n`);

            return true;
        } catch (error) {
            console.error('❌ [LIVE TRADE] Chuỗi lệnh thất bại. Cần kiểm tra lại tài khoản.');
            return false;
        }
    }
}

module.exports = new LiveExchange();