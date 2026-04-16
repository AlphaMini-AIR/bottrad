/**
 * src/services/ai/DeepThinker.js - V12 (Volatility Mismatch Fix)
 * Áp dụng "Khoảng Thở Cá Voi": Ép SL tối thiểu 0.5% để ôm lệnh 4H thực thụ.
 */
class DeepThinker {
    constructor() {
        this.BASE_RISK_USD = 1.5; 
        this.MAX_RISK_USD = 4.0;  
        this.MIN_RISK_USD = 0.5;  
        this.STREAK_PENALTY_THRESHOLD = 3;
        this.consecutiveLosses = {};
    }

    evaluateLogic(symbol, history, winProb, currentPrice, currentObi = 0, features = [], meta = null) {
        let side = null;

        if (winProb > 0.70) side = 'LONG';
        else if (winProb < 0.30) side = 'SHORT';

        if (!side) return { approved: false, reason: `AI_UNCERTAIN` };

        const trend_4h = features[8];
        const is_dead_zone = features[10];
        const is_london_open = features[11];
        const is_ny_open = features[12];
        const exhaustion_ratio = features[13];
        const spoofing_index = features[14];

        if (side === 'LONG' && trend_4h === -1) return { approved: false, reason: `REJECT: Ngược Trend 4H` };
        if (side === 'SHORT' && trend_4h === 1) return { approved: false, reason: `REJECT: Ngược Trend 4H` };
        if (exhaustion_ratio > 2.0) return { approved: false, reason: `REJECT: Kiệt sức Volume` };
        if (spoofing_index > 0.5) return { approved: false, reason: `REJECT: Bị Spoofing Sổ lệnh` };

        const sweepCandle = history[history.length - 1];
        let limitEntryPrice, dynamicSL, dynamicTP;

        let sumTr = 0;
        for (let i = history.length - 14; i < history.length; i++) {
            let h = history[i].high, l = history[i].low, pc = history[i - 1].close;
            sumTr += Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
        }
        const atr = sumTr / 14;

        const recent20 = history.slice(-21, -1);
        const swingHigh = Math.max(...recent20.map(c => c.high));
        const swingLow = Math.min(...recent20.map(c => c.low));

        // BẢN VÁ V12: KHOẢNG THỞ TỐI THIỂU 0.5% (Tránh bị nhiễu 1m quét)
        const minSLDistance = currentPrice * 0.005; // Mức SL nhỏ nhất là 0.5% giá

        if (side === 'LONG') {
            const wickTop = Math.min(sweepCandle.open, sweepCandle.close);
            const wickBot = sweepCandle.low;
            limitEntryPrice = wickBot + (wickTop - wickBot) * 0.4;
            limitEntryPrice = Math.min(limitEntryPrice, currentPrice - (atr * 0.2));

            const hardSLPrice = Math.min(swingLow, sweepCandle.low) - (atr * 0.3);
            dynamicSL = limitEntryPrice - hardSLPrice; 

            // XÓA BỎ GIỚI HẠN ATR NHỎ, ÉP SL PHẢI ĐỦ RỘNG
            dynamicSL = Math.max(minSLDistance, dynamicSL); 

            // TP linh động, tối thiểu bằng 2 lần SL
            dynamicTP = Math.max((swingHigh - limitEntryPrice) * 1.1, dynamicSL * 2.0); 

        } else {
            const wickBot = Math.max(sweepCandle.open, sweepCandle.close);
            const wickTop = sweepCandle.high;
            limitEntryPrice = wickBot + (wickTop - wickBot) * 0.6;
            limitEntryPrice = Math.max(limitEntryPrice, currentPrice + (atr * 0.2));

            const hardSLPrice = Math.max(swingHigh, sweepCandle.high) + (atr * 0.3);
            dynamicSL = hardSLPrice - limitEntryPrice;

            // XÓA BỎ GIỚI HẠN ATR NHỎ, ÉP SL PHẢI ĐỦ RỘNG
            dynamicSL = Math.max(minSLDistance, dynamicSL);

            // TP linh động, tối thiểu bằng 2 lần SL
            dynamicTP = Math.max((limitEntryPrice - swingLow) * 1.1, dynamicSL * 2.0);
        }

        if (dynamicTP / dynamicSL < 2.0) {
            return { approved: false, reason: `REJECT: R:R Không đạt 1:2.0` };
        }

        let riskUsd = this.BASE_RISK_USD;
        if (is_dead_zone === 1) riskUsd = this.MIN_RISK_USD;
        if (is_london_open === 1 || is_ny_open === 1) riskUsd = this.MAX_RISK_USD;
        if ((this.consecutiveLosses[symbol] || 0) >= this.STREAK_PENALTY_THRESHOLD) {
            riskUsd = Math.max(this.MIN_RISK_USD, riskUsd * 0.5);
        }

        const slPercent = dynamicSL / limitEntryPrice; 
        let maxSafeLeverage = Math.floor(1 / (slPercent * 1.2));

        return {
            approved: true, 
            symbol, side,
            suggestedRiskUSD: riskUsd, 
            suggestedLeverage: Math.max(3, Math.min(maxSafeLeverage, 20)), 
            limitEntryPrice: limitEntryPrice, 
            slDistance: dynamicSL, 
            tpDistance: dynamicTP,
            reason: `SWING_${side}_Vol$${riskUsd}`,
            features: features
        };
    }

    evaluateEarlyExit() { return { shouldExit: false }; }
    reportTradeResult(symbol, isWin) {
        if (!this.consecutiveLosses[symbol]) this.consecutiveLosses[symbol] = 0;
        if (isWin) this.consecutiveLosses[symbol] = 0;
        else this.consecutiveLosses[symbol] += 1;
    }
}
module.exports = new DeepThinker();

