/**
 * src/services/binance/StreamAggregator.js
 */
const WebSocket = require('ws');
const ObiEngine = require('../data/ObiEngine');
const Ingestor = require('../data/Ingestor');
const ScoringEngine = require('../ai/ScoringEngine');
const OrderManager = require('../execution/OrderManager');
const PaperExchange = require('../execution/PaperExchange');

class StreamAggregator {
    constructor(symbols = ['btcusdt', 'ethusdt', 'bnbusdt']) { // Cố định 3 coin test
        this.symbols = symbols;
        this.baseUrl = 'wss://fstream.binance.com/stream?streams=';
        this.ws = null;
        this.lastUpdateId = {};
    }

    start() {
        const streams = this.symbols.flatMap(s => [
            `${s}@kline_1m`,
            `${s}@depth20@100ms`
        ]).join('/');

        this.ws = new WebSocket(this.baseUrl + streams);

        this.ws.on('open', () => console.log('🌐 [Hệ thần kinh] Đã kết nối 3 coin mục tiêu'));

        this.ws.on('message', async (data) => {
            const payload = JSON.parse(data);
            const streamType = payload.stream.split('@')[1];

            if (streamType === 'depth20') {
                await this.handleDepth(payload.data);
            } else if (streamType === 'kline_1m') {
                this.handleKline(payload.data);
            }
        });

        this.ws.on('close', () => {
            setTimeout(() => this.start(), 5000);
        });
    }

    async handleDepth(data) {
        const symbol = data.s.toLowerCase();
        if (this.lastUpdateId[symbol] && data.u <= this.lastUpdateId[symbol]) return;
        this.lastUpdateId[symbol] = data.u;

        const obi = ObiEngine.calculate(data.b, data.a);
        global.latestObi = global.latestObi || {};
        global.latestObi[symbol] = obi;

        const decision = await ScoringEngine.evaluate(symbol, obi);
        if (decision.action === 'EXECUTE') {
            const currentPrice = (parseFloat(data.b[0][0]) + parseFloat(data.a[0][0])) / 2;
            await OrderManager.executeSignal(decision, currentPrice);
        }

        // --- BẢN VÁ: CẬP NHẬT RADAR GIÁ ---
        // Lấy giá hiện tại và gửi sang Sàn giả lập để dò Stoploss/TakeProfit
        const currentPrice = (parseFloat(data.b[0][0]) + parseFloat(data.a[0][0])) / 2;
        const livePrices = {};
        livePrices[symbol] = currentPrice;
        await PaperExchange.monitorPrices(livePrices);
    }

    handleKline(data) {
        if (data.k.x) {
            const symbol = data.s.toLowerCase();
            const obi = global.latestObi[symbol] || 0;
            Ingestor.saveCandle(data, obi);
            console.log(`⏱️ [NẾN ĐÓNG] ${symbol.toUpperCase()} - Dữ liệu đã lưu vào DB.`);
        }
    }
}

module.exports = new StreamAggregator();