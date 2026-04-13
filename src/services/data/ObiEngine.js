/**
 * src/services/data/ObiEngine.js
 */
class ObiEngine {
    constructor(decayFactor = 0.5) {
        this.alpha = decayFactor;
    }

    /**
     * Tính toán Decayed OBI ròng
     * @param {Array} bids [[price, qty], ...]
     * @param {Array} asks [[price, qty], ...]
     */
    calculate(bids, asks) {
        const midPrice = (parseFloat(bids[0][0]) + parseFloat(asks[0][0])) / 2;

        const weightedBids = bids.reduce((acc, [price, qty]) => {
            const distance = Math.abs(parseFloat(price) - midPrice) / midPrice * 100;
            const weight = Math.exp(-this.alpha * distance);
            return acc + (parseFloat(qty) * weight);
        }, 0);

        const weightedAsks = asks.reduce((acc, [price, qty]) => {
            const distance = Math.abs(parseFloat(price) - midPrice) / midPrice * 100;
            const weight = Math.exp(-this.alpha * distance);
            return acc + (parseFloat(qty) * weight);
        }, 0);

        // OBI ròng từ -1 (Bán áp đảo) đến 1 (Mua áp đảo)
        return (weightedBids - weightedAsks) / (weightedBids + weightedAsks);
    }
}

module.exports = new ObiEngine();