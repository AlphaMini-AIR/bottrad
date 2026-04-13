const mongoose = require('mongoose');

const candleSchema = new mongoose.Schema({
    timestamp: { type: Date, required: true },
    symbol: { type: String, required: true }, // MetaField cho TimeSeries
    open: Number,
    high: Number,
    low: Number,
    close: Number,
    volume: Number,
    decayed_obi: Number,   // Dòng tiền ròng đã lọc
    funding_rate: Number,  // Phí funding thu thập theo batch
    is_closed: { type: Boolean, default: true }
}, {
    // Cấu hình TimeSeries cho MongoDB 5.0+
    timeseries: {
        timeField: 'timestamp',
        metaField: 'symbol',
        granularity: 'minutes'
    }
});

// Cài đặt TTL Index: Tự động xóa sau 90 ngày (7,776,000 giây)
// Ghi chú: Với bản Free 512MB, nếu cậu test nhiều coin, hãy giảm xuống 604800 (7 ngày)
candleSchema.index({ timestamp: 1 }, { expireAfterSeconds: 7776000 });

async function getMacroTrend(symbol, timeframeMinutes = 240) { // Mặc định 4h (240p)
    const lookback = new Date(Date.now() - timeframeMinutes * 60 * 1000);
    const candles = await Candle1m.find({
        symbol: symbol.toUpperCase(),
        timestamp: { $gte: lookback }
    }).sort({ timestamp: 1 });

    if (candles.length === 0) return 'SIDEWAY';

    const firstPrice = candles[0].open;
    const lastPrice = candles[candles.length - 1].close;
    const diff = (lastPrice - firstPrice) / firstPrice;

    if (diff > 0.005) return 'TREND_UP';
    if (diff < -0.005) return 'TREND_DOWN';
    return 'SIDEWAY';
}

module.exports = mongoose.model('Candle1m', candleSchema);