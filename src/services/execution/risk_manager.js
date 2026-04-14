/**
 * src/services/execution/risk_manager.js - Version 6.0 (AI-Driven Risk & Defensive Armor)
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
     * Tính toán Quy mô lệnh (Size) dựa trên ATR, thông số AI và Precision của sàn
     * @param {Object} decision - Quyết định từ Router (chứa symbol, side, mode, price, atr_multiplier, tp_multiplier)
     * @param {Array} historyCandles - Nến lịch sử
     * @param {Object} mockAccount - Tài khoản giả lập (Tùy chọn)
     * @param {Number} customSlMultiplier - Hệ số SL truyền trực tiếp từ main (Tùy chọn)
     */
    async processSignal(decision, historyCandles, mockAccount = null, customSlMultiplier = null) {
        const account = mockAccount || { balance: 1000, current_total_risk: 0.04 };

        if (account.current_total_risk >= this.HARD_CAP_RISK) return false;

        const atr = this.calculateATR(historyCandles);
        const ai_sl_mult = customSlMultiplier || decision.atr_multiplier || (decision.mode === 'SWING' ? 1.5 : 1.0);

        // 1. TĂNG LỚP GIÁP LÊN 1.0 ATR (Thay vì 0.5)
        const defensive_sl_mult = ai_sl_mult + 1.0;
        let slDistance = atr * defensive_sl_mult;

        // 🌟 FIX TỬ HUYỆT: ÉP BUỘC SL CÁCH ÍT NHẤT 0.3% GIÁ TRỊ COIN 
        // Đảm bảo tiền phí sàn không bao giờ được phép lớn hơn tiền Risk
        const minDistance = decision.price * 0.003;
        if (slDistance < minDistance) {
            slDistance = minDistance;
        }

        // 🌟 FIX TỬ HUYỆT 2: TÍNH CẢ PHÍ SÀN VÀO KÍCH THƯỚC LỆNH (0.1% Round trip)
        const riskAmount = account.balance * this.MAX_RISK_PER_TRADE;
        const estimatedFeePerCoin = decision.price * 0.001;
        const rawSize = riskAmount / (slDistance + estimatedFeePerCoin);

        const precision = ExchangeInfo.getPrecision(decision.symbol);
        let sizeDecimals = precision.stepSize < 1 ? precision.stepSize.toString().split('.')[1].length : 0;
        const calculatedSize = parseFloat((Math.floor(rawSize / precision.stepSize) * precision.stepSize).toFixed(sizeDecimals));

        if (calculatedSize <= 0) return false;

        let priceDecimals = precision.tickSize < 1 ? precision.tickSize.toString().split('.')[1].length : 0;
        let slPrice = decision.side === 'LONG' ? decision.price - slDistance : decision.price + slDistance;

        // Nới TP ra tỷ lệ 1:2.5 để bù đắp phí
        const tpDistance = slDistance * 2.5;
        let tpPrice = decision.side === 'LONG' ? decision.price + tpDistance : decision.price - tpDistance;

        slPrice = parseFloat(slPrice.toFixed(priceDecimals));
        tpPrice = parseFloat(tpPrice.toFixed(priceDecimals));

        return { ...decision, size: calculatedSize, slPrice, tpPrice };
    }
}

module.exports = new RiskManager();