/**
 * src/services/ai/DeepThinker.js - Version 5.3 (Hard-Rules & Pre-Trade Validator)
 */

class DeepThinker {
    constructor() {
        this.MAX_HARD_RISK = 0.015; // Tối đa rủi ro 1.5% vốn
        this.STREAK_PENALTY_THRESHOLD = 3; // Thua 3 lệnh liên tiếp sẽ bị phạt Vol
        this.consecutiveLosses = {};
    }

    // 🛡️ BỘ LỌC CỨNG (PRE-TRADE VALIDATOR)
    preTradeValidate(signal, features) {
        const ob_imb_norm = features[8];  // > 0 là Tường Mua, < 0 là Tường Bán
        const liq_press = features[9];    // > 0 là Long đang chết, < 0 là Short đang chết
        const prem_idx = features[10];    // < 0 là Giá bị bơm ảo cao hơn Mark Price

        if (signal === 'LONG') {
            if (prem_idx < -0.005) return { allowed: false, reason: 'REJECT: Bẫy thanh khoản (Premium < -0.5%)' };
            if (ob_imb_norm < -0.2) return { allowed: false, reason: 'REJECT: Sổ lệnh đè Bán mạnh (OB < -0.2)' };
            if (liq_press > 0.6) return { allowed: false, reason: 'REJECT: Bão thanh lý Long (Liq Cascade)' };
        } 
        else if (signal === 'SHORT') {
            if (prem_idx > 0.005) return { allowed: false, reason: 'REJECT: Giá bị đạp ảo (Premium > 0.5%)' };
            if (ob_imb_norm > 0.2) return { allowed: false, reason: 'REJECT: Sổ lệnh kê Mua mạnh (OB > 0.2)' };
            if (liq_press < -0.6) return { allowed: false, reason: 'REJECT: Bão thanh lý Short (Liq Cascade)' };
        }

        return { allowed: true };
    }

    evaluateLogic(symbol, history, winProb, currentPrice, currentObi = 0, features = []) {
        let side = null;
        // Tín hiệu từ AI
        if (winProb > 0.65) side = 'LONG';
        else if (winProb < 0.35) side = 'SHORT';

        if (!side) return { approved: false, reason: 'NO_EDGE' };

        // 1. KIỂM DUYỆT BẰNG LUẬT CỨNG (MICROSTRUCTURE FILTERS)
        const validation = this.preTradeValidate(side, features);
        if (!validation.allowed) {
            console.log(`🛡️ [DEEP THINKER] ${symbol} ${side} bị từ chối: ${validation.reason}`);
            return { approved: false, reason: validation.reason };
        }

        // --- CÁC BƯỚC TOÁN HỌC PHÍA SAU GIỮ NGUYÊN NHƯ V5.2 ---
        
        // 2. Tính ATR để đặt SL/TP
        let sumTr = 0;
        for (let i = history.length - 14; i < history.length; i++) {
            let h = history[i].high, l = history[i].low, pc = history[i - 1].close;
            sumTr += Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
        }
        const atr = sumTr / 14;
        const dynamicSL = atr * 1.5;
        const dynamicTP = atr * 2.0;
        const avgRewardRiskRatio = dynamicTP / dynamicSL;

        // 3. Tính Kelly Fractional
        const realTimeEdge = side === 'LONG' ? winProb : (1 - winProb);
        const q = 1 - realTimeEdge;
        let kellyFraction = (realTimeEdge * avgRewardRiskRatio - q) / avgRewardRiskRatio;

        if (kellyFraction <= 0) return { approved: false, reason: 'KELLY_NEGATIVE' };

        // 4. Kỷ luật chuỗi thua
        let safeKelly = kellyFraction * 0.25; 
        const losses = this.consecutiveLosses[symbol] || 0;
        if (losses >= this.STREAK_PENALTY_THRESHOLD) {
            safeKelly = safeKelly * 0.5; // Phạt chia đôi Vol
        }

        return {
            approved: true,
            symbol,
            side,
            suggestedRiskRatio: Math.min(safeKelly, this.MAX_HARD_RISK),
            slDistance: dynamicSL,
            tpDistance: dynamicTP,
            reason: `EDGE_${(realTimeEdge*100).toFixed(1)}%_KELLY_${safeKelly.toFixed(3)}`,
            features: features // Giao mảng 11 biến cho hệ thống JSON
        };
    }

    reportTradeResult(symbol, isWin) {
        if (!this.consecutiveLosses[symbol]) this.consecutiveLosses[symbol] = 0;
        if (isWin) this.consecutiveLosses[symbol] = 0;
        else this.consecutiveLosses[symbol] += 1;
    }
}

module.exports = new DeepThinker();


