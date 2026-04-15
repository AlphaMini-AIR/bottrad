/**
 * src/services/ai/DeepThinker.js 
 */
class DeepThinker {
    constructor() {
        this.MAX_HARD_RISK = 0.015; // Rủi ro tối đa 1.5% tài khoản
        this.STREAK_PENALTY_THRESHOLD = 3;
        this.consecutiveLosses = {};
    }

    preTradeValidate(signal, features) {
        const ob_imb_norm = features[8] !== undefined ? features[8] : 0;
        const liq_press = features[9] !== undefined ? features[9] : 0;
        const prem_idx = features[10] !== undefined ? features[10] : 0;

        if (signal === 'LONG') {
            if (prem_idx < -0.005) return { allowed: false, reason: 'REJECT: Premium < -0.5%' };
            if (ob_imb_norm < -0.2) return { allowed: false, reason: 'REJECT: OB < -0.2' };
            if (liq_press > 0.6) return { allowed: false, reason: 'REJECT: Liq Cascade' };
        } else if (signal === 'SHORT') {
            if (prem_idx > 0.005) return { allowed: false, reason: 'REJECT: Premium > 0.5%' };
            if (ob_imb_norm > 0.2) return { allowed: false, reason: 'REJECT: OB > 0.2' };
            if (liq_press < -0.6) return { allowed: false, reason: 'REJECT: Liq Cascade' };
        }
        return { allowed: true };
    }

    evaluateLogic(symbol, history, winProb, currentPrice, currentObi = 0, features = [], meta = null) {
        let side = null;

        // 1. CHẤP NHẬN SỐ LƯỢNG LỆNH NHIỀU HƠN MỘT CHÚT ĐỂ BÙ CHO BỘ LỌC PHÍ DÀY
        if (winProb > 0.65) side = 'LONG';
        else if (winProb < 0.35) side = 'SHORT';

        if (!side) return { approved: false, reason: `AI_UNCERTAIN (${(winProb * 100).toFixed(1)}%)` };

        // 2. LỌC VI CẤU TRÚC
        const validation = this.preTradeValidate(side, features);
        if (!validation.allowed) return { approved: false, reason: validation.reason };

        let sumTr = 0;
        for (let i = history.length - 14; i < history.length; i++) {
            let h = history[i].high, l = history[i].low, pc = history[i - 1].close;
            sumTr += Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
        }
        const atr = sumTr / 14;

        // 3. THÔNG SỐ SWING GỐC
        const sl_mult = 3.0;
        const tp_mult = 7.5;

        const dynamicSL = atr * sl_mult;
        const dynamicTP = atr * tp_mult;

        // ========================================================
        // 🚀 4. BỘ LỌC CHỐNG BẪY PHÍ SÀN (NET R:R FILTER)
        // ========================================================
        const roundTripFee = currentPrice * 0.001; // Phí 2 chiều (0.1%)

        // Rủi ro ròng = SL + Phí | Lợi nhuận ròng = TP - Phí
        const netSL = dynamicSL + roundTripFee;
        const netTP = dynamicTP - roundTripFee;

        // Nếu phí lớn hơn cả TP thì vứt luôn
        if (netTP <= 0) return { approved: false, reason: 'REJECT: Phí sàn lớn hơn cả tiền lãi dự kiến' };

        // Tính Tỷ lệ Cược Thực tế (True Net R:R)
        const trueRR = netTP / netSL;

        // LUẬT THÉP: Nếu Lãi thực không lớn hơn 1.8 lần Lỗ thực -> TỪ CHỐI BẮN!
        if (trueRR < 1.8) {
            return { approved: false, reason: `REJECT: Net R:R quá thấp (${trueRR.toFixed(2)}) do nhiễu sóng ngắn` };
        }

        // ========================================================
        // 5. TÍNH TOÁN KELLY DỰA TRÊN TỶ LỆ THỰC (TRUE R:R)
        // ========================================================
        const realTimeEdge = side === 'LONG' ? winProb : (1 - winProb);
        const q = 1 - realTimeEdge;
        let kellyFraction = (realTimeEdge * trueRR - q) / trueRR; // Đã đổi sang trueRR

        if (kellyFraction <= 0.02) return { approved: false, reason: `KELLY_TOO_LOW` };

        let safeKelly = kellyFraction * 0.25;
        if ((this.consecutiveLosses[symbol] || 0) >= this.STREAK_PENALTY_THRESHOLD) safeKelly *= 0.5;

        return {
            approved: true, symbol, side,
            suggestedRiskRatio: Math.min(safeKelly, this.MAX_HARD_RISK),
            slDistance: dynamicSL, tpDistance: dynamicTP,
            reason: `EDGE_${(realTimeEdge * 100).toFixed(1)}%_NET-RR_${trueRR.toFixed(1)}`,
            features: features
        };
    }
    evaluateEarlyExit(position, history, aiWinProb) {
        return { shouldExit: false }; // Tắt Early Exit trong Backtest
    }

    reportTradeResult(symbol, isWin) {
        if (!this.consecutiveLosses[symbol]) this.consecutiveLosses[symbol] = 0;
        if (isWin) this.consecutiveLosses[symbol] = 0;
        else this.consecutiveLosses[symbol] += 1;
    }
}
module.exports = new DeepThinker();