/**
 * src/services/execution/OrderManager.js
 * Phiên bản V6.0: Rủi ro Fixed USD + Dynamic Leverage + Min Notional Check
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
            // Cần gọi API: await LiveExchange.getAvailableBalance();
            return 1000; 
        }
    }

    async executeSignal(decision, currentPrice) {
        // 1. KHÓA CÒ: Chống Spam lệnh
        if (this.tradeMode === 'PAPER') {
            const acc = await PaperAccount.findOne({ account_id: 'main_paper' });
            if (acc && acc.open_positions.some(p => p.symbol === decision.symbol)) return;
        } else {
            try {
                const activePositions = await LiveExchange.getActivePositions(); 
                if (activePositions.includes(decision.symbol)) return;
            } catch (error) {
                console.error(`❌ [LIVE LOCK] Lỗi check vị thế ${decision.symbol}:`, error.message);
                return; 
            }
        }

        // 2. KIỂM TRA CẦU DAO
        const isSystemSafe = await CircuitBreaker.checkSafety();
        const isCoinSafe = await CircuitBreaker.isCoinSafe(decision.symbol);
        if (!isSystemSafe || !isCoinSafe) return;

        // 3. ĐIỀU PHỐI VỐN & ĐÒN BẨY (Nhận từ DeepThinker V6)
        const currentBalance = await this.getAccountBalance();
        if (currentBalance <= 0) return;

        const amountAtRisk = decision.suggestedRiskUSD; // Vd: 1.5$ đến 5$
        const leverage = decision.suggestedLeverage || 10; // Mặc định 10x nếu thiếu
        
        // 4. TÍNH TOÁN QUY MÔ LỆNH & SL/TP
        let hardSl = decision.side === 'LONG' ? currentPrice - decision.slDistance : currentPrice + decision.slDistance;
        let takeProfit = decision.side === 'LONG' ? currentPrice + decision.tpDistance : currentPrice - decision.tpDistance;

        let rawSize = amountAtRisk / decision.slDistance;

        // Làm tròn Size theo bước nhảy của sàn
        const precision = ExchangeInfo.getPrecision(decision.symbol);
        let sizeDecimals = precision && precision.stepSize < 1 ? precision.stepSize.toString().split('.')[1].length : 3;
        let calculatedSize = parseFloat((Math.floor(rawSize / (precision?.stepSize || 0.001)) * (precision?.stepSize || 0.001)).toFixed(sizeDecimals));

        // 5. KIỂM TRA MIN NOTIONAL VÀ KÝ QUỸ (MARGIN)
        const notionalValue = calculatedSize * currentPrice; // Giá trị thực tế của lệnh
        const marginRequired = notionalValue / leverage;     // Số tiền thực tế phải bỏ ra (Ký quỹ)

        if (calculatedSize <= 0 || notionalValue < 5.1) {
            console.log(`⚠️ [REJECTED] Lệnh quá bé (Notional: ${notionalValue.toFixed(2)}$ < 5$). Bỏ qua ${decision.symbol}.`);
            return;
        }

        if (marginRequired > currentBalance) {
            console.log(`⚠️ [REJECTED] Thiếu số dư. Cần ${marginRequired.toFixed(2)}$, đang có ${currentBalance.toFixed(2)}$`);
            return;
        }

        // 6. IN LOG CHUẨN XÁC
        console.log(`\n🎯 [EXECUTION] Bóp cò ${decision.side} ${decision.symbol}!`); 
        console.log(`🛡️ Rủi ro cố định: ${amountAtRisk.toFixed(2)} USDT | Ký quỹ (Margin): ${marginRequired.toFixed(2)} USDT`); 
        console.log(`⚖️ Đòn bẩy: ${leverage}x | Size: ${calculatedSize} | Entry: ${currentPrice}`);
        console.log(`📏 SL: ${hardSl.toFixed(4)} | TP: ${takeProfit.toFixed(4)}`); 

        // 7. THỰC THI LỆNH
        if (this.tradeMode === 'LIVE') { 
            // Bạn cần cập nhật LiveExchange.executeTrade để nó nhận biến 'leverage' và gọi API đổi đòn bẩy trên Binance trước khi vào lệnh
            await LiveExchange.executeTrade( 
                decision.symbol, decision.side, calculatedSize, currentPrice, 
                decision.mode, hardSl, takeProfit, decision.features, leverage 
            ); 
        } else { 
            await PaperExchange.openPosition( 
                decision.symbol, decision.side, currentPrice, calculatedSize, 
                decision.mode, hardSl, takeProfit, decision.features, leverage 
            ); 
        }
    }
}

module.exports = new OrderManager();