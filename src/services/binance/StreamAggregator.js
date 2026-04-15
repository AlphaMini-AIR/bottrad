/**
 * src/services/binance/StreamAggregator.js - Version 5.3 (Microstructure Ears)
 */
const WebSocket = require('ws');
const InMemoryBuffer = require('../data/InMemoryBuffer'); 

class StreamAggregator {
    constructor() {
        this.ws = null;
        this.symbols = []; // Chứa danh sách coin dạng UPPERCASE (VD: 'BTCUSDT')
        this.vpinBuffer = {}; 
    }

    // Khởi tạo các biến Global để chứa dữ liệu tạm thời trên RAM
    initGlobals(symbol) {
        if (!global.orderbookImbalanceRaw) global.orderbookImbalanceRaw = {};
        if (!global.liqLong1mSum) global.liqLong1mSum = {};
        if (!global.liqShort1mSum) global.liqShort1mSum = {};
        if (!global.currentMarkPrice) global.currentMarkPrice = {};

        if (global.orderbookImbalanceRaw[symbol] === undefined) global.orderbookImbalanceRaw[symbol] = 1.0;
        if (global.liqLong1mSum[symbol] === undefined) global.liqLong1mSum[symbol] = 0;
        if (global.liqShort1mSum[symbol] === undefined) global.liqShort1mSum[symbol] = 0;
        if (global.currentMarkPrice[symbol] === undefined) global.currentMarkPrice[symbol] = null;
    }

    start() {
        if (this.symbols.length === 0) return;

        let streamPaths = [];
        this.symbols.forEach(sym => {
            const s = sym.toLowerCase();
            this.initGlobals(sym); // Khởi tạo biến RAM cho coin này
            
            // 4 luồng cục bộ cho từng đồng coin
            streamPaths.push(`${s}@kline_1m`);
            streamPaths.push(`${s}@aggTrade`);
            streamPaths.push(`${s}@depth20@1000ms`); // Sổ lệnh update 1 giây/lần
            streamPaths.push(`${s}@markPrice@1s`);    // Mark price update 1 giây/lần
        });

        // Thêm 1 luồng Global (Toàn thị trường) để bắt lệnh thanh lý
        streamPaths.push('!forceOrder@arr');

        const wsUrl = `wss://fstream.binance.com/stream?streams=${streamPaths.join('/')}`;
        
        console.log(`📡 [WEBSOCKET] Đang kết nối 5 luồng Vi Cấu Trúc cho ${this.symbols.length} coins...`);
        this.ws = new WebSocket(wsUrl);

        this.ws.on('open', () => {
            console.log('🟢 [WEBSOCKET] Đã kết nối thành công tới Binance Futures (Microstructure Enabled)!');
        });

        this.ws.on('message', (msg) => {
            try {
                const data = JSON.parse(msg);
                if (data.data) {
                    this.routeMessage(data.data);
                }
            } catch (err) {
                // Nuốt lỗi parse JSON để không crash bot
            }
        });

        this.ws.on('error', (err) => console.error('❌ [WEBSOCKET] Lỗi:', err.message));
        
        this.ws.on('close', () => {
            console.warn('⚠️ [WEBSOCKET] Bị ngắt kết nối. Đang thử lại sau 5s...');
            setTimeout(() => this.start(), 5000);
        });
    }

    // Phân luồng sự kiện siêu tốc
    routeMessage(data) {
        const e = data.e;
        if (e === 'kline') this.handleKline(data);
        else if (e === 'aggTrade') this.handleAggTrade(data);
        else if (e === 'depthUpdate') this.handleDepth(data);
        else if (e === 'markPriceUpdate') this.handleMarkPrice(data);
        else if (e === 'forceOrder') this.handleForceOrder(data);
    }

    // 1. TÍNH VPIN (Dòng tiền Tick)
    handleAggTrade(data) {
        const symbol = data.s.toUpperCase();
        if (!this.vpinBuffer[symbol]) this.vpinBuffer[symbol] = { buyVol: 0, sellVol: 0 };

        const qty = parseFloat(data.q);
        if (data.m) this.vpinBuffer[symbol].sellVol += qty; // Taker Sell
        else this.vpinBuffer[symbol].buyVol += qty;         // Taker Buy
    }

    // 2. TÍNH ORDERBOOK IMBALANCE (Cân bằng Sổ lệnh)
    handleDepth(data) {
        const symbol = data.s.toUpperCase();
        let bidVol = 0, askVol = 0;
        
        // Cộng dồn 20 mức giá đầu tiên. Chi phí vòng lặp: cực thấp (O(20))
        for (let i = 0; i < data.b.length; i++) bidVol += parseFloat(data.b[i][1]);
        for (let i = 0; i < data.a.length; i++) askVol += parseFloat(data.a[i][1]);
        
        // Lưu thẳng vào RAM Global
        global.orderbookImbalanceRaw[symbol] = bidVol / (askVol + 1e-8); 
    }

    // 3. THEO DÕI MARK PRICE
    handleMarkPrice(data) {
        const symbol = data.s.toUpperCase();
        global.currentMarkPrice[symbol] = parseFloat(data.p);
    }

    // 4. BẮT THANH LÝ (Cháy tài khoản)
    handleForceOrder(data) {
        const order = data.o;
        const symbol = order.s.toUpperCase();
        
        // Chỉ quan tâm các coin trong White-list của bot
        if (global.liqLong1mSum[symbol] !== undefined) {
            const value = parseFloat(order.p) * parseFloat(order.q); // Giá trị USD bị cháy
            
            // Nếu sàn BUY để đóng vị thế -> Lệnh Long đã bị cháy (Force Sell)
            if (order.S === 'BUY') {
                global.liqLong1mSum[symbol] += value;
                console.log(`🔥 [LIQUIDATION] ${symbol} Long Squeeze: ${value.toFixed(0)}$`);
            } else {
                global.liqShort1mSum[symbol] += value;
                console.log(`🔥 [LIQUIDATION] ${symbol} Short Squeeze: ${value.toFixed(0)}$`);
            }
        }
    }

    // 5. CHỐT SỔ (Khi Nến 1 phút đóng)
    handleKline(data) {
        if (data.k.x) { 
            const symbol = data.s.toUpperCase();
            
            // -- Lấy và tính VPIN --
            const vpinData = this.vpinBuffer[symbol] || { buyVol: 0, sellVol: 0 };
            const totalVol = vpinData.buyVol + vpinData.sellVol;
            const vpin = totalVol > 0 ? (vpinData.buyVol - vpinData.sellVol) / totalVol : 0; 
            
            // -- Đọc các chỉ số Vi Cấu Trúc từ RAM --
            const ob_imb = global.orderbookImbalanceRaw[symbol] || 1.0;
            const liq_long = global.liqLong1mSum[symbol] || 0;
            const liq_short = global.liqShort1mSum[symbol] || 0;
            const mark_close = global.currentMarkPrice[symbol] || data.k.c;

            // -- Đẩy toàn bộ 11 tham số vào Lõi Bộ Nhớ (InMemoryBuffer) --
            InMemoryBuffer.push(symbol, data.k, vpin, ob_imb, liq_long, liq_short, mark_close);
            
            // -- 🧹 RESET BỘ ĐẾM CHO PHÚT TIẾP THEO --
            this.vpinBuffer[symbol] = { buyVol: 0, sellVol: 0 };
            global.liqLong1mSum[symbol] = 0;
            global.liqShort1mSum[symbol] = 0;
            // (Không reset Orderbook và Mark Price vì nó là ảnh chụp Snapshot tĩnh)

            console.log(`⏱️ [NẾN ĐÓNG] ${symbol} | VPIN: ${vpin.toFixed(2)} | OB_IMB: ${ob_imb.toFixed(2)} | Liq(L-S): ${liq_long.toFixed(0)}$ - ${liq_short.toFixed(0)}$`);
        }
    }
}

module.exports = new StreamAggregator();


