/**
 * src/services/ai/ScoringEngine.js
 */
const Resampler = require('../data/Resampler');
const LearningEngine = require('./LearningEngine');

class ScoringEngine {
    async evaluate(symbol, currentObi) {
        const klines4h = await Resampler.getKlines(symbol, 240, 2);
        if (klines4h.length < 2) return { action: 'WAIT', score: 0 };
        const isMacroUp = klines4h[0].close > klines4h[1].close;

        const klines15m = await Resampler.getKlines(symbol, 15, 2);
        if (klines15m.length < 2) return { action: 'WAIT', score: 0 };
        const isLocalUp = klines15m[0].close > klines15m[1].close;

        // HỎI Ý KIẾN BỘ NÃO KINH NGHIỆM
        const historicalWinRate = LearningEngine.getHistoricalWinRate(symbol);

        let score = 0;
        if (currentObi > 0.5) score += 0.4;
        if (isMacroUp) score += 0.3;
        if (isLocalUp) score += 0.3;

        // Thưởng điểm nếu trong quá khứ mô hình này thắng > 55%
        if (historicalWinRate > 0.55) score += 0.2;

        const side = currentObi > 0 ? 'LONG' : 'SHORT';

        if (score >= 0.7) {
            const mode = (side === 'LONG' && !isMacroUp) || (side === 'SHORT' && isMacroUp) ? 'HIT_AND_RUN' : 'STANDARD';
            return { action: 'EXECUTE', symbol, side, score, mode };
        }

        return { action: 'WAIT', score };
    }
}

module.exports = new ScoringEngine();