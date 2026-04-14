/**
 * src/services/execution/risk_manager.js
 */
const ExchangeInfo = require('../binance/ExchangeInfo');

class RiskManager {
    constructor() {
        this.MAX_RISK_PER_TRADE = 0.02; // Rủi ro cứng: 2% vốn/lệnh
        this.HARD_CAP_RISK = 0.10;      // Cầu dao tổng: Không vượt quá 10%
    }

    // Tính Average True Range (ATR) 14 chu kỳ để đo độ giật của giá
    calculateATR(historyCandles, period = 14) {
        if (historyCandles.length < period + 1) return historyCandles[historyCandles.length - 1].close * 0.01; 
        
        let sumTR = 0;
        for (let i = historyCandles.length - period; i < historyCandles.length; i++) {
            const current = historyCandles[i];
            const prev = historyCandles[i - 1];
            const tr = Math.max(
                current.high - current.low,
                Math.abs(current.high - prev.close),
                Math.abs(current.low - prev.close)
            );
            sumTR += tr;
        }
        return sumTR / period;
    }

    /**
     * Tính toán Quy mô lệnh (Size) dựa trên ATR và Precision của sàn
     */
    async processSignal(decision, historyCandles, mockAccount = null) {
        // Trong thực tế sẽ lấy từ DB, ở đây dùng mock để test độc lập
        const account = mockAccount || { balance: 1000, current_total_risk: 0.04 }; // Đang gánh rủi ro 4% của các lệnh khác

        console.log(`\n🏦 [RISK MANAGER] Nhận lệnh ${decision.side} ${decision.symbol}. Tiến hành thẩm định rủi ro...`);

        // 1. KIỂM TRA CẦU DAO TỔNG (Hard Cap 10%)
        if (account.current_total_risk >= this.HARD_CAP_RISK) {
            console.log(`⛔ [RISK MANAGER] TỪ CHỐI: Kích hoạt cầu dao tổng! Đang chịu rủi ro ${(account.current_total_risk * 100).toFixed(1)}% >= 10%.`);
            return false;
        }

        // 2. TÍNH KHOẢNG CÁCH CẮT LỖ BẰNG ATR
        // Swing đánh theo trend nên SL rộng hơn (1.5 ATR). Range bắt đảo chiều nên SL chặt hơn (1.0 ATR).
        const atr = this.calculateATR(historyCandles);
        const multiplier = decision.mode === 'SWING' ? 1.5 : 1.0;
        const slDistance = atr * multiplier;

        // 3. TÍNH TOÁN QUY MÔ VỊ THẾ (SIZE LỆNH)
        const riskAmount = account.balance * this.MAX_RISK_PER_TRADE; // Vốn 1000$ -> Rủi ro 20$
        const rawSize = riskAmount / slDistance;

        // 4. ÉP KIỂU SỐ THẬP PHÂN CHUẨN SÀN BINANCE (Tránh lỗi LOT_SIZE / PRICE_FILTER)
        const precision = ExchangeInfo.getPrecision(decision.symbol);
        
        // Cắt số thập phân cho Size lệnh
        let sizeDecimals = precision.stepSize < 1 ? precision.stepSize.toString().split('.')[1].length : 0;
        const calculatedSize = parseFloat((Math.floor(rawSize / precision.stepSize) * precision.stepSize).toFixed(sizeDecimals));

        if (calculatedSize <= 0) {
            console.log(`⚠️ [RISK MANAGER] Size quá nhỏ, không đủ đáp ứng Min LOT_SIZE của sàn.`);
            return false;
        }

        // Cắt số thập phân cho Giá SL/TP
        let priceDecimals = precision.tickSize < 1 ? precision.tickSize.toString().split('.')[1].length : 0;
        let slPrice = decision.side === 'LONG' ? decision.price - slDistance : decision.price + slDistance;
        let tpPrice = decision.side === 'LONG' ? decision.price + (slDistance * 2) : decision.price - (slDistance * 3); // R:R 1:3

        slPrice = parseFloat(slPrice.toFixed(priceDecimals));
        tpPrice = parseFloat(tpPrice.toFixed(priceDecimals));

        console.log(`📏 Khoảng cách SL (ATR x${multiplier}): $${slDistance.toFixed(2)}`);
        console.log(`💰 Kế hoạch: Vốn $${account.balance.toFixed(2)} | Chịu lỗ tối đa: $${riskAmount.toFixed(2)}`);
        console.log(`✅ Chốt Size lệnh: ${calculatedSize} ${decision.symbol.replace('USDT', '')} - Giá Entry: $${decision.price}`);
        console.log(`🎯 StopLoss: $${slPrice} | TakeProfit: $${tpPrice}`);

        return {
            ...decision,
            size: calculatedSize,
            slPrice,
            tpPrice
        };
    }
}

module.exports = new RiskManager();