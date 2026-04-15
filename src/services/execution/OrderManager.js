/**
 * src/services/execution/OrderManager.js
 * Phiên bản V4.5: Bơm Toán học từ DeepThinker & Chống Spam/Min Notional LIVE
 */
const PaperExchange = require('./PaperExchange');
const LiveExchange = require('./LiveExchange');
const CircuitBreaker = require('../ops/CircuitBreaker');
const PaperAccount = require('../../models/PaperAccount');
const ExchangeInfo = require('../binance/ExchangeInfo');
require('dotenv').config();

class OrderManager {
    constructor() {
        this.tradeMode = process.env.TRADE_MODE || 'PAPER';
    }

    async getAccountBalance() {
        if (this.tradeMode === 'PAPER') {
            const acc = await PaperAccount.findOne({ account_id: 'main_paper' });
            return acc ? acc.balance : 0;
        } else {
            // Cần gọi API Binance để lấy số dư khả dụng (Available Balance) thực tế
            // Tạm thời giả định 1000 để an toàn trong lúc test logic
            return 1000; 
        }
    }

    async executeSignal(decision, currentPrice) {
        // 1. KHÓA CÒ: Bỏ qua nếu đang có lệnh đồng coin này (CẢ PAPER LẪN LIVE)
        if (this.tradeMode === 'PAPER') {
            const acc = await PaperAccount.findOne({ account_id: 'main_paper' });
            if (acc && acc.open_positions.some(p => p.symbol === decision.symbol)) {
                return; // Đang ôm lệnh giả lập thì bỏ qua
            }
        } else {
            // [BẢN VÁ LIVE]: Kiểm tra vị thế thực tế trên sàn (Tránh spam nhồi lệnh)
            // Yêu cầu: Bạn cần đảm bảo đã viết hàm getActivePositions() trong LiveExchange.js
            try {
                const activePositions = await LiveExchange.getActivePositions(); 
                if (activePositions.includes(decision.symbol)) {
                    return; // Đang ôm lệnh LIVE rồi thì im lặng rút lui
                }
            } catch (error) {
                console.error(`❌ [LIVE LOCK] Lỗi khi check vị thế ${decision.symbol}:`, error.message);
                return; // Lỗi mạng thì thà bỏ lỡ lệnh còn hơn spam nhầm
            }
        }

        // 2. KIỂM TRA CẦU DAO (Bảo vệ cháy nổ)
        const isSystemSafe = await CircuitBreaker.checkSafety();
        const isCoinSafe = await CircuitBreaker.isCoinSafe(decision.symbol);
        if (!isSystemSafe || !isCoinSafe) return;

        // 3. ĐIỀU PHỐI VỐN BẰNG KELLY
        const currentBalance = await this.getAccountBalance();
        if (currentBalance <= 0) return;

        const amountAtRisk = currentBalance * decision.suggestedRiskRatio;

        // 4. TÍNH TOÁN QUY MÔ LỆNH & SL/TP
        let hardSl = decision.side === 'LONG' ? currentPrice - decision.slDistance : currentPrice + decision.slDistance;
        let takeProfit = decision.side === 'LONG' ? currentPrice + decision.tpDistance : currentPrice - decision.tpDistance;

        let rawSize = amountAtRisk / decision.slDistance;
        
        // Làm tròn Size theo bước nhảy của sàn
        const precision = ExchangeInfo.getPrecision(decision.symbol);
        let sizeDecimals = precision && precision.stepSize < 1 ? precision.stepSize.toString().split('.')[1].length : 3;
        let calculatedSize = parseFloat((Math.floor(rawSize / (precision?.stepSize || 0.001)) * (precision?.stepSize || 0.001)).toFixed(sizeDecimals));

        // [BẢN VÁ MIN NOTIONAL]: Kiểm tra quy mô lệnh tối thiểu của Binance
        const notionalValue = calculatedSize * currentPrice;
        if (calculatedSize <= 0 || notionalValue < 5.1) {
            console.log(`⚠️ [REJECTED] Lệnh quá bé (Notional: ${notionalValue.toFixed(2)}$ < 5$). Bỏ qua ${decision.symbol} để tránh lỗi sàn.`);
            return;
        }

     console.log(`\n🎯 [DEEP THINKER -> EXECUTION] Bóp cò ${decision.side} ${decision.symbol}!`); console.log(`🛡️ Rủi ro: ${(decision.suggestedRiskRatio * 100).toFixed(2)}% vốn (~${amountAtRisk.toFixed(2)} USDT)`); console.log(`📏 Size: ${calculatedSize} | Entry: ${currentPrice} | SL: ${hardSl.toFixed(4)} | TP: ${takeProfit.toFixed(4)}`); // 5. CHUYỂN CHO BỘ PHẬN ĐẶT LỆNH THỤ ĐỘNG (LIMIT MAKER) // [THÊM BIẾN decision.features] if (this.tradeMode === 'LIVE') { await LiveExchange.executeTrade( decision.symbol, decision.side, calculatedSize, currentPrice, decision.mode, hardSl, takeProfit, decision.features ); } else { await PaperExchange.openPosition( decision.symbol, decision.side, currentPrice, calculatedSize, decision.mode, hardSl, takeProfit, decision.features ); }
    }
}

module.exports = new OrderManager();

