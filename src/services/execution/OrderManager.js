/**
 * src/services/execution/OrderManager.js
 */
const PaperExchange = require('./PaperExchange');
const LiveExchange = require('./LiveExchange');
const CircuitBreaker = require('../ops/CircuitBreaker');
const PaperAccount = require('../../models/PaperAccount');
require('dotenv').config();

class OrderManager {
    constructor() {
        this.tradeMode = process.env.TRADE_MODE || 'PAPER';
        this.MAX_RISK_PER_TRADE = 0.01; // Quy tắc 1%
    }

    async getAccountBalance() {
        if (this.tradeMode === 'PAPER') {
            const acc = await PaperAccount.findOne({ account_id: 'main_paper' });
            return acc ? acc.balance : 0;
        } else {
            return 0; // Sẽ xử lý ở Task API thật
        }
    }

    async executeSignal(decision, currentPrice) {
        // 1. KHÓA CÒ: Bỏ qua ngay lập tức nếu đang có lệnh đồng coin này
        if (this.tradeMode === 'PAPER') {
            const acc = await PaperAccount.findOne({ account_id: 'main_paper' });
            if (acc && acc.open_positions.some(p => p.symbol === decision.symbol)) {
                return; // Im lặng rút lui
            }
        }

        // 2. KIỂM TRA CẦU DAO
        const isSystemSafe = await CircuitBreaker.checkSafety();
        const isCoinSafe = await CircuitBreaker.isCoinSafe(decision.symbol);
        if (!isSystemSafe || !isCoinSafe) return;

        // 3. TÍNH TOÁN RỦI RO (SL/TP)
        const riskPercent = decision.mode === 'HIT_AND_RUN' ? 0.005 : 0.01;
        let hardSl = decision.side === 'LONG' ? currentPrice * (1 - riskPercent) : currentPrice * (1 + riskPercent);
        let takeProfit = decision.side === 'LONG' ? currentPrice * (1 + riskPercent * 1.5) : currentPrice * (1 - riskPercent * 1.5);

        // 4. QUẢN LÝ VỐN TỰ ĐỘNG
        const currentBalance = await this.getAccountBalance();
        if (currentBalance <= 0) return;

        const amountAtRisk = currentBalance * this.MAX_RISK_PER_TRADE;
        const priceDistanceToSl = Math.abs(currentPrice - hardSl);
        let calculatedSize = amountAtRisk / priceDistanceToSl;

        if (decision.symbol === 'btcusdt') calculatedSize = Math.round(calculatedSize * 1000) / 1000;
        if (decision.symbol === 'ethusdt') calculatedSize = Math.round(calculatedSize * 100) / 100;
        if (decision.symbol === 'bnbusdt') calculatedSize = Math.round(calculatedSize * 100) / 100;

        if (calculatedSize <= 0) return;

        console.log(`\n🛡️ [RISK MANAGEMENT] Vốn: ${currentBalance.toFixed(2)} | Chịu rủi ro: ${amountAtRisk.toFixed(2)} USDT`);
        console.log(`📏 [RISK MANAGEMENT] Đã tính toán Size an toàn: ${calculatedSize} ${decision.symbol.replace('usdt', '').toUpperCase()}`);

        // 5. BÓP CÒ
        if (this.tradeMode === 'LIVE') {
            await LiveExchange.executeTrade(decision.symbol, decision.side, calculatedSize, currentPrice, decision.mode, hardSl, takeProfit);
        } else {
            await PaperExchange.openPosition(decision.symbol, decision.side, currentPrice, calculatedSize, decision.mode, hardSl, takeProfit);
        }
    }
}

module.exports = new OrderManager();