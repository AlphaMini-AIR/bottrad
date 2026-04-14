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

        // 1. Tính ADX để xác định thị trường có đang Sideway không
        const adx = this.calculateADXProxy(history);

        // Nếu ADX > 25, thị trường đang Trend mạnh -> KHÔNG đánh Range
        if (adx > 25) {
            return { approved: false, reason: `RANGE_REJECTED_TRENDING (ADX: ${adx.toFixed(1)})` };
        }

        // 2. Cảm biến Râu nến (Wick) và Gia tốc Volume
        const body = Math.abs(current.close - current.open) + 1e-8;
        const wick = current.high - current.low;
        const wickToBody = wick / body;

        let avgVol = 0;
        for (let i = history.length - 15; i < history.length; i++) avgVol += history[i].volume;
        const volAccel = current.volume / (avgVol / 15 + 1e-8);

        if (wickToBody > 2.5 && volAccel > 3.0) {
            return { approved: true };
        }

        return { approved: false, reason: "RANGE_NO_LIQUIDATION_SIGN" };
    }

    execScalpLogic(prob, history, meta) {
        const current = history[history.length - 1];

        // Tính trung bình volume của 5 nến TRƯỚC ĐÓ (không bao gồm nến hiện tại)
        let sumVol = 0;
        for (let i = history.length - 6; i < history.length - 1; i++) {
            sumVol += history[i].volume;
        }
        const avgVol = (sumVol / 5) + 1e-8;

        // Gia tốc volume của nến hiện tại
        const momentum = current.volume / avgVol;

        // VÁ LỖI: 
        // 1. Hạ ngưỡng vào lệnh xuống 1.5 (Volume tăng 50% là đủ để xác nhận có dòng tiền)
        // 2. Chặn các nến có volume x3 (Climax/Kiệt sức) để tránh bị cắn Stoploss ngược
        const min_mom = meta.min_momentum || 1.5;
        const max_mom = 3.0;

        if (momentum >= min_mom && momentum <= max_mom) {
            return { approved: true };
        }

        return {
            approved: false,
            reason: `SCALP_REJECTED (Momentum: ${momentum.toFixed(2)} - Nằm ngoài vùng an toàn 1.5 -> 3.0)`
        };
    }
    calculateADXProxy(history, period = 14) {
        if (history.length < period + 1) return 20; // Mặc định sideway nếu thiếu nến

        let trSum = 0;
        let plusDmSum = 0;
        let minusDmSum = 0;

        for (let i = history.length - period; i < history.length; i++) {
            const current = history[i];
            const prev = history[i - 1];

            const tr = Math.max(
                current.high - current.low,
                Math.abs(current.high - prev.close),
                Math.abs(current.low - prev.close)
            );
            trSum += tr;

            const upMove = current.high - prev.high;
            const downMove = prev.low - current.low;

            let plusDm = 0;
            let minusDm = 0;

            if (upMove > downMove && upMove > 0) plusDm = upMove;
            if (downMove > upMove && downMove > 0) minusDm = downMove;

            plusDmSum += plusDm;
            minusDmSum += minusDm;
        }

        if (trSum === 0) return 0;

        const plusDI = (plusDmSum / trSum) * 100;
        const minusDI = (minusDmSum / trSum) * 100;

        // Tính DX (Directional Index) làm proxy cho ADX giúp giảm tải cho CPU
        const dx = (Math.abs(plusDI - minusDI) / (plusDI + minusDI + 1e-8)) * 100;
        return dx;
    }
}

module.exports = new StrategyRouter();