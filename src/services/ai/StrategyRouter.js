/**
 * src/services/ai/StrategyRouter.js - Version 6.5 (Advanced Intelligent Routing)
 * Tích hợp lọc xu hướng (ADX), Momentum và AI Reversal
 */

class StrategyRouter {
    constructor() {
        this.lastTradeTime = {};
        // Ngưỡng AI Win Probability
        this.LONG_THRESHOLD = 0.70;
        this.SHORT_THRESHOLD = 0.30;
        this.SUPER_SIGNAL_LONG = 0.85;
        this.SUPER_SIGNAL_SHORT = 0.15;
    }

    /**
     * Đánh giá lệnh mới dựa trên AI và bộ lọc kỹ thuật
     */
    async evaluate(symbol, history, aiWinProb, meta) {
        const current = history[history.length - 1];
        if (current.isStaleData) return { approved: false, reason: "STALE_DATA" };

        let intendedSide = null;
        let isSuperSignal = false;

        // 1. PHÂN LOẠI TÍN HIỆU AI
        if (aiWinProb >= this.LONG_THRESHOLD) {
            intendedSide = "LONG";
            if (aiWinProb >= this.SUPER_SIGNAL_LONG) isSuperSignal = true;
        } else if (aiWinProb <= this.SHORT_THRESHOLD) {
            intendedSide = "SHORT";
            if (aiWinProb <= this.SUPER_SIGNAL_SHORT) isSuperSignal = true;
        } else {
            return { approved: false, reason: `NEUTRAL_ZONE (${(aiWinProb * 100).toFixed(1)}%)` };
        }

        // 2. KIỂM TRA COOLDOWN (Nghỉ ngơi giữa các lệnh)
        const now = Date.now();
        const baseCooldown = 30 * 60 * 1000; // 30 phút
        const timeSinceLast = now - (this.lastTradeTime[symbol] || 0);

        if (!isSuperSignal && timeSinceLast < baseCooldown) {
            return { approved: false, reason: "SMART_COOLDOWN_ACTIVE" };
        }

        // 3. ĐIỀU PHỐI CHIẾN THUẬT (ROUTING)
        let strategyResult = { approved: false };
        const adx = this.calculateADXProxy(history);

        switch (meta.strategy) {
            case 'SWING': 
                strategyResult = this.execSwingLogic(aiWinProb, adx, isSuperSignal); 
                break;
            case 'RANGE': 
                strategyResult = this.execRangeLogic(aiWinProb, adx, history); 
                break;
            case 'SCALP': 
                strategyResult = this.execScalpLogic(aiWinProb, history, isSuperSignal); 
                break;
            default: 
                return { approved: false, reason: "UNKNOWN_STRATEGY" };
        }

        if (strategyResult.approved) {
            this.lastTradeTime[symbol] = now;
            console.log(`🎯 [ENTRY] ${symbol} | ${intendedSide} | AI: ${(aiWinProb * 100).toFixed(1)}% | Mode: ${meta.strategy} | ADX: ${adx.toFixed(1)}`);
            return {
                ...strategyResult,
                symbol,
                side: intendedSide,
                mode: meta.strategy,
                price: current.close,
                isSuperSignal
            };
        }
        
        return strategyResult;
    }

    /**
     * LOGIC SWING: Ưu tiên bám đuổi xu hướng mạnh
     */
    execSwingLogic(prob, adx, isSuper) {
        // Swing cần xu hướng (ADX > 25) hoặc AI cực mạnh
        if (adx > 25 || isSuper) {
            return { approved: true };
        }
        return { approved: false, reason: "SWING_WAITING_FOR_TREND" };
    }

    /**
     * LOGIC RANGE: Ưu tiên thị trường đi ngang, biến động thấp
     */
    execRangeLogic(prob, adx, history) {
        // Range cần thị trường không có trend (ADX < 20)
        if (adx < 22) {
            return { approved: true };
        }
        return { approved: false, reason: "RANGE_REJECT_TRENDING" };
    }

    /**
     * LOGIC SCALP: Ưu tiên bùng nổ khối lượng (Momentum)
     */
    execScalpLogic(prob, history, isSuper) {
        const current = history[history.length - 1];
        let sumVol = 0;
        const period = 10;
        for (let i = history.length - (period + 1); i < history.length - 1; i++) {
            sumVol += history[i].volume;
        }
        const avgVol = sumVol / period;
        const momentum = current.volume / (avgVol + 1e-8);

        // Scalp cần sự đột biến volume nhưng không được quá Climax (dễ đảo chiều)
        const min_mom = 1.3;
        const max_mom = isSuper ? 4.5 : 3.0;

        if (momentum >= min_mom && momentum <= max_mom) {
            return { approved: true };
        }
        return { approved: false, reason: `SCALP_MOM_FAIL (${momentum.toFixed(1)}x)` };
    }

    /**
     * CẮT LỆNH CHỦ ĐỘNG (ACTIVE EXIT)
     * Ngăn chặn việc gồng lỗ khi AI đổi ý hoặc thị trường đảo chiều gấp
     */
    evaluateEarlyExit(position, history, aiWinProb) {
        const current = history[history.length - 1];

        // 1. EXIT THEO AI REVERSAL (AI quay xe)
        if (position.side === 'LONG' && aiWinProb < 0.40) {
            return { shouldExit: true, reason: "AI_REVERSAL_BEARISH" };
        }
        if (position.side === 'SHORT' && aiWinProb > 0.60) {
            return { shouldExit: true, reason: "AI_REVERSAL_BULLISH" };
        }

        // 2. EXIT THEO VOLUME CLIMAX (Xả hàng hoặc FOMO quá mức)
        let sumVol = 0;
        for (let i = history.length - 6; i < history.length - 1; i++) sumVol += history[i].volume;
        const momentum = current.volume / ((sumVol / 5) + 1e-8);

        // Nếu volume vọt lên gấp 5 lần trung bình nến trước -> Dễ là đỉnh/đáy tạm thời
        if (momentum > 5.0) {
            return { shouldExit: true, reason: "VOLUME_CLIMAX_DETECTED" };
        }

        return { shouldExit: false };
    }

    /**
     * Tính toán ADX Proxy để xác định sức mạnh xu hướng
     */
    calculateADXProxy(history, period = 14) {
        if (history.length < period * 2) return 20;

        let plusDM = 0;
        let minusDM = 0;
        let trSum = 0;

        for (let i = history.length - period; i < history.length; i++) {
            const curr = history[i];
            const prev = history[i - 1];

            const moveUp = curr.high - prev.high;
            const moveDown = prev.low - curr.low;

            if (moveUp > 0 && moveUp > moveDown) plusDM += moveUp;
            if (moveDown > 0 && moveDown > moveUp) minusDM += moveDown;

            const tr = Math.max(
                curr.high - curr.low,
                Math.abs(curr.high - prev.close),
                Math.abs(curr.low - prev.close)
            );
            trSum += tr;
        }

        const plusDI = 100 * (plusDM / (trSum + 1e-8));
        const minusDI = 100 * (minusDM / (trSum + 1e-8));
        
        // Trả về giá trị đơn giản đại diện cho sức mạnh xu hướng
        return Math.abs(plusDI - minusDI) / (plusDI + minusDI + 1e-8) * 100;
    }
}

module.exports = new StrategyRouter();