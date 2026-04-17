/**
 * src/services/binance/StreamAggregator.js - Version 15.0 (Ultimate Data Harvester)
 */
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const InMemoryBuffer = require('../data/InMemoryBuffer'); // Buffer giữ data cho AI đánh Live

// Đảm bảo thư mục lưu dữ liệu jsonl tồn tại
const LIVE_DATA_DIR = path.join(__dirname, '../../../data/live_buffer');
if (!fs.existsSync(LIVE_DATA_DIR)) fs.mkdirSync(LIVE_DATA_DIR, { recursive: true });

class StreamAggregator {
    constructor() {
        this.ws = null;
        this.symbols = []; // VD: ['SOLUSDT', 'BTCUSDT']
    }

    // Khởi tạo các biến Global để chứa dữ liệu Vi cấu trúc tạm thời trong 60 giây
    initGlobals(symbol) {
        if (!global.liveMicroData) global.liveMicroData = {};
        
        // Cấu trúc tạm RAM chuẩn bị cho JSON cuối cùng
        if (!global.liveMicroData[symbol]) {
            global.liveMicroData[symbol] = {
                ob_imb_top20: 0.5,
                spread_close: 0,
                bid_vol_1pct: 0,
                ask_vol_1pct: 0,
                max_buy_trade: 0,
                max_sell_trade: 0,
                liq_long_vol: 0,
                liq_short_vol: 0,
                funding_rate: 0
            };
        }
    }

    start() {
        if (this.symbols.length === 0) return;

        let streamPaths = [];
        this.symbols.forEach(sym => {
            const s = sym.toLowerCase();
            this.initGlobals(sym);
            
            // Cắm 4 đầu dò cho từng coin
            streamPaths.push(`${s}@kline_1m`);         // Nến 1 phút
            streamPaths.push(`${s}@aggTrade`);         // Từng lệnh khớp (Bắt cá mập)
            streamPaths.push(`${s}@depth20@100ms`);    // Sổ lệnh ĐỘ TRỄ SIÊU THẤP 100ms
            streamPaths.push(`${s}@markPrice@1s`);     // Giá Mark & Funding Rate
        });

        // 1 đầu dò Global bắt toàn bộ lệnh cháy tài khoản của thị trường
        streamPaths.push('!forceOrder@arr');

        const wsUrl = `wss://fstream.binance.com/stream?streams=${streamPaths.join('/')}`;
        
        console.log(`📡 [WS] Đang thả lưới thu thập Vi Cấu Trúc cho ${this.symbols.length} coins...`);
        this.ws = new WebSocket(wsUrl);

        this.ws.on('open', () => console.log('🟢 [WS] Kết nối Binance Futures thành công (Ultimate Mode)!'));

        this.ws.on('message', (msg) => {
            try {
                const data = JSON.parse(msg);
                if (data.data) this.routeMessage(data.data);
            } catch (err) {} // Bỏ qua lỗi parse
        });

        this.ws.on('error', (err) => console.error('❌ [WS] Lỗi:', err.message));
        
        this.ws.on('close', () => {
            console.warn('⚠️ [WS] Đứt cáp Binance. Tự động nối lại sau 5s...');
            setTimeout(() => this.start(), 5000);
        });
    }

    // Phân luồng dữ liệu về đúng hàm xử lý
    routeMessage(data) {
        const e = data.e;
        if (e === 'kline') this.handleKline(data);
        else if (e === 'aggTrade') this.handleAggTrade(data);
        else if (e === 'depthUpdate') this.handleDepth(data);
        else if (e === 'markPriceUpdate') this.handleMarkPrice(data);
        else if (e === 'forceOrder') this.handleForceOrder(data);
    }

    // 1. TAI NGHE KHỚP LỆNH: Tìm lệnh Đơn Lẻ Lớn Nhất (Cá mập)
    handleAggTrade(data) {
        const symbol = data.s.toUpperCase();
        const tradeValue = parseFloat(data.q) * parseFloat(data.p); // Quy ra USDT

        if (data.m) { 
            // Taker Sell (Người bán chủ động)
            if (tradeValue > global.liveMicroData[symbol].max_sell_trade) {
                global.liveMicroData[symbol].max_sell_trade = tradeValue;
            }
        } else { 
            // Taker Buy (Người mua chủ động)
            if (tradeValue > global.liveMicroData[symbol].max_buy_trade) {
                global.liveMicroData[symbol].max_buy_trade = tradeValue;
            }
        }
    }

    // 2. TAI NGHE SỔ LỆNH: Tính Spread, Lệch Pha, và Tường Cản 1%
    handleDepth(data) {
        const symbol = data.s.toUpperCase();
        const bids = data.b; // [[price, qty], ...]
        const asks = data.a;
        if (!bids.length || !asks.length) return;

        const bestBid = parseFloat(bids[0][0]);
        const bestAsk = parseFloat(asks[0][0]);
        
        // Tính độ giãn chênh lệch (Spread)
        global.liveMicroData[symbol].spread_close = bestAsk - bestBid;

        let bidVolTop20 = 0, askVolTop20 = 0;
        let bidVol1Pct = 0, askVol1Pct = 0;

        // Quét phe Mua (Bids)
        for (let i = 0; i < bids.length; i++) {
            const p = parseFloat(bids[i][0]);
            const vol = p * parseFloat(bids[i][1]); // Tính bằng USDT
            bidVolTop20 += vol;
            if (p >= bestBid * 0.99) bidVol1Pct += vol; // Lệnh nằm trong vùng 1%
        }

        // Quét phe Bán (Asks)
        for (let i = 0; i < asks.length; i++) {
            const p = parseFloat(asks[i][0]);
            const vol = p * parseFloat(asks[i][1]); // Tính bằng USDT
            askVolTop20 += vol;
            if (p <= bestAsk * 1.01) askVol1Pct += vol; // Lệnh nằm trong vùng 1%
        }

        // Cập nhật Snapshot vào RAM
        global.liveMicroData[symbol].ob_imb_top20 = bidVolTop20 / ((bidVolTop20 + askVolTop20) || 1);
        global.liveMicroData[symbol].bid_vol_1pct = bidVol1Pct;
        global.liveMicroData[symbol].ask_vol_1pct = askVol1Pct;
    }

    // 3. TAI NGHE MARK PRICE: Cập nhật Funding Rate
    handleMarkPrice(data) {
        const symbol = data.s.toUpperCase();
        if (data.r) {
            global.liveMicroData[symbol].funding_rate = parseFloat(data.r);
        }
    }

    // 4. TAI NGHE THANH LÝ: Bắt tổng lượng cháy tài khoản
    handleForceOrder(data) {
        const order = data.o;
        const symbol = order.s.toUpperCase();
        
        if (global.liveMicroData[symbol]) {
            const value = parseFloat(order.p) * parseFloat(order.q); // Giá trị cháy (USDT)
            
            if (order.S === 'BUY') {
                // Sàn thanh lý bằng cách bắt BUY -> Tức là lệnh SHORT bị cháy
                global.liveMicroData[symbol].liq_short_vol += value;
            } else {
                // Sàn thanh lý bằng cách bắt SELL -> Tức là lệnh LONG bị cháy
                global.liveMicroData[symbol].liq_long_vol += value;
            }
        }
    }

    // 5. THỜI KHẮC ĐÓNG NẾN: Gói dữ liệu & Lưu Ổ Cứng
    handleKline(data) {
        if (data.k.x) { // k.x = true nghĩa là nến 1 phút đã đóng hoàn toàn
            const symbol = data.s.toUpperCase();
            
            // BƯỚC A: TẠO CẤU TRÚC JSON HOÀN CHỈNH (Như Master Plan)
            const payload = {
                symbol: symbol,
                openTime: data.k.t,
                ohlcv: {
                    open: parseFloat(data.k.o),
                    high: parseFloat(data.k.h),
                    low: parseFloat(data.k.l),
                    close: parseFloat(data.k.c),
                    volume: parseFloat(data.k.v),
                    quoteVolume: parseFloat(data.k.q),
                    trades: data.k.n,
                    takerBuyBase: parseFloat(data.k.V),
                    takerBuyQuote: parseFloat(data.k.Q)
                },
                micro: { ...global.liveMicroData[symbol] }, // Lấy bản sao từ RAM
                macro: {
                    funding_rate: global.liveMicroData[symbol].funding_rate,
                    open_interest: 0 // Lưu ý: Binance WS không hỗ trợ OI real-time chuẩn. Sẽ kéo bằng REST API ở tầng khác nếu cần, tạm để 0.
                },
                isStaleData: false
            };

            // BƯỚC B: LƯU VÀO FILE JSONL (Nạp đạn cho lò tự học ban đêm)
            const filePath = path.join(LIVE_DATA_DIR, 'syncing.jsonl');
            fs.appendFileSync(filePath, JSON.stringify(payload) + '\n');

            // BƯỚC C: GỬI LÊN BỘ NHỚ RAM ĐỂ BOT ĐÁNH LIVE
            // Vẫn duy trì truyền data sang InMemoryBuffer để AiEngine nhận diện
            const mData = payload.micro;
            // Tính VPIN tương đối từ ohlcv để truyền vào Buffer cũ
            const vpin = payload.ohlcv.quoteVolume > 0 ? ((payload.ohlcv.takerBuyQuote * 2) - payload.ohlcv.quoteVolume) / payload.ohlcv.quoteVolume : 0;
            
            InMemoryBuffer.push(symbol, data.k, vpin, mData.ob_imb_top20, mData.liq_long_vol, mData.liq_short_vol, data.k.c);

            // BƯỚC D: RESET BỘ ĐẾM CHO PHÚT TIẾP THEO
            global.liveMicroData[symbol].max_buy_trade = 0;
            global.liveMicroData[symbol].max_sell_trade = 0;
            global.liveMicroData[symbol].liq_long_vol = 0;
            global.liveMicroData[symbol].liq_short_vol = 0;
            // (Spread, Funding, Ob_imb giữ nguyên vì nó là snapshot liên tục)

            console.log(`✅ [DATA VAULT] Đã lưu nến ${symbol} | Mập Mua: $${mData.max_buy_trade.toFixed(0)} | Spread: ${mData.spread_close.toFixed(4)}`);
        }
    }
}

module.exports = new StreamAggregator();