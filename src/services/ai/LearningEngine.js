/**
 * src/services/ai/LearningEngine.js
 */
const Candle1m = require('../../models/Candle1m');

class LearningEngine {
    constructor() {
        this.knowledgeBase = {};
    }

    async train(symbol) {
        console.log(`🧠 [LEARNING] Bot đang học từ dữ liệu quá khứ của ${symbol}...`);
        const data = await Candle1m.find({ symbol }).sort({ timestamp: 1 }).lean();

        if (data.length < 250) {
            console.log(`⚠️ Dữ liệu quá ít để học tập.`);
            return;
        }

        let winCount = 0;
        let totalSignals = 0;

        for (let i = 240; i < data.length - 15; i++) {
            const current = data[i];
            const prev4h = data[i - 240];
            const future15m = data[i + 15];

            const isMacroUp = current.close > prev4h.close;
            const isLocalUp = current.close > data[i - 15].close;
            const mockObi = current.close > data[i - 1].close ? 0.6 : -0.6;

            if (isMacroUp && isLocalUp && mockObi > 0.5) {
                totalSignals++;
                if (future15m.close > current.close) {
                    winCount++;
                }
            }
        }

        const winRate = totalSignals > 0 ? (winCount / totalSignals) : 0;
        this.knowledgeBase[symbol] = { winRate, totalSignals };

        console.log(`   🎯 Đã đúc kết kinh nghiệm: Tìm thấy ${totalSignals} kịch bản tương đồng.`);
        console.log(`   🎯 Tỉ lệ chiến thắng (Win-rate) thực tế: ${(winRate * 100).toFixed(2)}%\n`);
    }

    getHistoricalWinRate(symbol) {
        return this.knowledgeBase[symbol] ? this.knowledgeBase[symbol].winRate : 0.5;
    }
}

module.exports = new LearningEngine();