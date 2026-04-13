/**
 * src/services/data/Ingestor.js
 */
const Candle1m = require('../../models/Candle1m');
const RestClient = require('../binance/RestClient');

class Ingestor {
    constructor() {
        this.fundingCache = {};
        this.lastFundingFetch = 0;
    }

    // Cache Funding Rate 30s để 100 coin đóng nến cùng lúc chỉ tốn 1 request API
    async getCachedFunding(symbol) {
        const now = Date.now();
        if (now - this.lastFundingFetch > 30000) {
            this.fundingCache = await RestClient.getFundingRates();
            this.lastFundingFetch = now;
        }
        return this.fundingCache[symbol] || 0;
    }

    async saveCandle(klineData, obi) {
        const symbol = klineData.s.toLowerCase();
        const fundingRate = await this.getCachedFunding(symbol);

        const candle = new Candle1m({
            timestamp: new Date(klineData.k.T), // k.T là thời gian ĐÓNG nến
            symbol: symbol.toUpperCase(),
            open: parseFloat(klineData.k.o),
            high: parseFloat(klineData.k.h),
            low: parseFloat(klineData.k.l),
            close: parseFloat(klineData.k.c),
            volume: parseFloat(klineData.k.v),
            decayed_obi: obi,
            funding_rate: fundingRate,
            is_closed: true
        });

        try {
            await candle.save();
            // Chỉ log tối giản để Hưng dễ theo dõi luồng
            // console.log(`💾 [DB] Saved ${symbol.toUpperCase()} | Price: ${klineData.k.c}`);
        } catch (error) {
            console.error(`❌ [DB] Error saving ${symbol.toUpperCase()}:`, error.message);
        }
    }
}

module.exports = new Ingestor();