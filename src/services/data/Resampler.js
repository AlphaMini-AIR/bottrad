/**
 * src/services/data/Resampler.js
 */
const Candle1m = require('../../models/Candle1m');

class Resampler {
    // Gom nến 1m thành nến khung lớn hơn (15m, 1h, 4h)
    async getKlines(symbol, timeframeMinutes, limit = 100) {
        const candles = await Candle1m.find({ symbol: symbol.toUpperCase() })
            .sort({ timestamp: -1 })
            .limit(limit * timeframeMinutes); // Lấy đủ số nến 1m để gộp

        let resampled = [];
        for (let i = 0; i < candles.length; i += timeframeMinutes) {
            const chunk = candles.slice(i, i + timeframeMinutes);
            if (chunk.length === 0) continue;

            resampled.push({
                timestamp: chunk[0].timestamp,
                open: chunk[chunk.length - 1].open,
                high: Math.max(...chunk.map(c => c.high)),
                low: Math.min(...chunk.map(c => c.low)),
                close: chunk[0].close,
                volume: chunk.reduce((sum, c) => sum + c.volume, 0)
            });
        }
        return resampled; // Trả về danh sách nến khung lớn
    }
}

module.exports = new Resampler();