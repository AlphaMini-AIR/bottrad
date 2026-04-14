/**
 * src/services/ai/StrategyRouter.js - Version 4.2 (Dynamic Logic)
 */
class StrategyRouter {
    constructor() {
        this.lastTradeTime = {};
        this.highVolatilityMode = {};
    }

    /**
     * @param {string} symbol - Tên coin
     * @param {Array} history - Dữ liệu nến lịch sử
     * @param {number} aiWinProb - Xác suất từ AI (0-1)
     * @param {Object} meta - Thông tin chiến thuật học được từ white_list.json
     */
    async evaluate(symbol, history, aiWinProb, meta) {
        const current = history[history.length - 1];
        
        // 1. CHỐNG LOOK-AHEAD BIAS: Nếu dữ liệu bị lỗi/stale, từ chối ngay
        if (current.isStaleData) return { approved: false, reason: "STALE_DATA" };

        // 2. XÁC ĐỊNH CHIỀU VÀO LỆNH & NGƯỠNG TỰ TIN (Học từ AI)
        // Thay vì fix cứng 0.5, ta dùng ngưỡng meta.min_prob (ví dụ 0.7) mà AI đã tính toán là hiệu quả nhất
        let intendedSide = null;
        const threshold = meta.min_prob || 0.6; // Mặc định 0.6 nếu chưa học được

        if (aiWinProb >= threshold) intendedSide = "LONG";
        else if (aiWinProb <= (1 - threshold)) intendedSide = "SHORT";
        else return { approved: false, reason: "LOW_CONFIDENCE" };

        // 3. KIỂM TRA COOL-DOWN (Hồi chiêu)
        const now = current.timestamp;
        const cooldown = this.highVolatilityMode[symbol] ? 60 * 60 * 1000 : 15 * 60 * 1000;
        if (now - (this.lastTradeTime[symbol] || 0) < cooldown) {
            return { approved: false, reason: "COOLDOWN" };
        }

        // 4. THỰC THI CHIẾN THUẬT DỰA TRÊN "SỞ TRƯỜNG" (Không check tên coin)
        let strategyResult = { approved: false };

        switch (meta.strategy) {
            case 'SWING':
                strategyResult = this.execSwingLogic(aiWinProb, current, meta);
                break;
            case 'RANGE':
                strategyResult = this.execRangeLogic(aiWinProb, history, meta);
                break;
            case 'SCALP':
                strategyResult = this.execScalpLogic(aiWinProb, history, meta);
                break;
            default:
                return { approved: false, reason: "UNKNOWN_STRATEGY" };
        }

        if (strategyResult.approved) {
            this.lastTradeTime[symbol] = now;
            console.log(`\n🔥 [ROUTER] DUYỆT LỆNH: ${symbol} | Phương pháp: ${meta.strategy} | Side: ${intendedSide}`);
            return {
                ...strategyResult,
                symbol,
                side: intendedSide,
                mode: meta.strategy,
                price: current.close,
                atr_multiplier: meta.atr_multiplier // Truyền xuống RiskManager
            };
        }

        return strategyResult;
    }

    // --- LOGIC CHI TIẾT CHO TỪNG PHƯƠNG PHÁP ---

    execSwingLogic(prob, current, meta) {
        // Swing: Quan trọng nhất là phí Funding. 
        // Nếu phí phạt quá nặng chiều định đánh, ép xác suất phải cực cao (> 80%)
        const isCloseToFunding = (current.next_funding_time - current.timestamp) < 2 * 60 * 60 * 1000;
        const isPaying = (current.funding_rate > 0 && prob > 0.5) || (current.funding_rate < 0 && prob < 0.5);

        if (isCloseToFunding && isPaying && Math.abs(prob - 0.5) < 0.3) {
            return { approved: false, reason: "SWING_FUNDING_PROTECTION" };
        }
        return { approved: true };
    }

    execRangeLogic(prob, history, meta) {
        const current = history[history.length - 1];
        // Range: Cần cảm biến Râu nến (Wick) và Gia tốc Volume
        const body = Math.abs(current.close - current.open) + 1e-8;
        const wick = current.high - current.low;
        const wickToBody = wick / body;

        let avgVol = 0;
        for (let i = history.length - 15; i < history.length; i++) avgVol += history[i].volume;
        const volAccel = current.volume / (avgVol / 15 + 1e-8);

        // Các thông số 2.5 và 3.0 này sau này có thể đưa vào meta nếu AI học được
        if (wickToBody > 2.5 && volAccel > 3.0) {
            return { approved: true };
        }
        return { approved: false, reason: "RANGE_NO_LIQUIDATION_SIGN" };
    }

    execScalpLogic(prob, history, meta) {
        const current = history[history.length - 1];
        // Scalp: Cần biến động cực mạnh (Momentum)
        let sumVol = 0;
        for (let i = history.length - 5; i < history.length; i++) sumVol += history[i].volume;
        const momentum = current.volume / (sumVol / 5 + 1e-8);

        if (momentum > (meta.min_momentum || 4.0)) {
            return { approved: true };
        }
        return { approved: false, reason: "SCALP_NOT_ENOUGH_MOMENTUM" };
    }
}

module.exports = new StrategyRouter();