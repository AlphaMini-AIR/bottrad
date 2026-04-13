/**
 * src/services/ai/TheGreatSifter.js (Bản Lite - Test 3 Coin)
 */
const axios = require('axios');
const ActiveRoster = require('../../models/ActiveRoster');

class TheGreatSifter {
    constructor() {
        // CHỈ ĐỊNH ĐÍCH DANH 3 COIN ĐỂ TEST NHANH
        this.targetCoins = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT'];
        this.requiredDays = 30; // Rút xuống 30 ngày để test cực nhanh
    }

    async evaluateCoin(symbol) {
        try {
            const res = await axios.get('https://fapi.binance.com/fapi/v1/klines', {
                params: { symbol: symbol, interval: '1d', limit: this.requiredDays }
            });

            const klines = res.data;
            if (klines.length < this.requiredDays) {
                console.log(`⚠️ [SIFTER] ${symbol} không đủ dữ liệu.`);
                return null;
            }

            let returns = [];
            for (let i = 1; i < klines.length; i++) {
                const prevClose = parseFloat(klines[i - 1][4]);
                const close = parseFloat(klines[i][4]);
                returns.push((close - prevClose) / prevClose);
            }

            const meanReturn = returns.reduce((a, b) => a + b, 0) / returns.length;
            const volatility = Math.sqrt(returns.reduce((sq, val) => sq + Math.pow(val - meanReturn, 2), 0) / returns.length);
            const sharpeRatio = volatility === 0 ? 0 : (meanReturn / volatility) * Math.sqrt(365);

            return { symbol, sharpeRatio, volatility };
        } catch (error) {
            console.error(`❌ [SIFTER] Lỗi khi kéo dữ liệu ${symbol}:`, error.message);
            return null;
        }
    }

    async run() {
        console.log(`🔥 [SIFTER] KHỞI ĐỘNG LÒ LUYỆN ĐƠN (TEST MODE - ${this.targetCoins.length} COINS)`);
        let results = [];

        for (const sym of this.targetCoins) {
            const evalData = await this.evaluateCoin(sym);
            if (evalData) {
                results.push(evalData);
            }
            // Delay 100ms tránh Rate Limit
            await new Promise(r => setTimeout(r, 100));
        }

        console.log('\n🏆 [SIFTER] KẾT QUẢ ĐÁNH GIÁ 3 COIN:');
        results.forEach((c, i) => {
            console.log(`  ${i + 1}. ${c.symbol} | Sharpe: ${c.sharpeRatio.toFixed(2)} | Volatility: ${(c.volatility * 100).toFixed(2)}%`);
        });

        // Xóa danh sách cũ nếu có (để dọn dẹp lúc test lại nhiều lần)
        await ActiveRoster.deleteMany({});

        // Lưu vào DB
        const roster = new ActiveRoster({
            coins: results.map(c => ({
                symbol: c.symbol,
                sharpe_ratio: c.sharpeRatio,
                volatility: c.volatility,
                is_active: true,
                mode: 'STANDARD'
            }))
        });

        await roster.save();
        console.log('💾 [DB] Đã lưu danh sách Active Roster (3 Coins) vào hệ thống.\n');

        return results;
    }
}

module.exports = new TheGreatSifter();