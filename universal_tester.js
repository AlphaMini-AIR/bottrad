/**
 * universal_tester.js - Đấu trường Sinh tử (Sandbox Forward Testing)
 * Chạy độc lập hoàn toàn, không dùng MongoDB.
 * Vốn 100$, Ký quỹ 1$, Đòn bẩy Động từ x1 đến x20.
 */
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const ort = require('onnxruntime-node');

// --- CẤU HÌNH THỬ NGHIỆM ---
const TEST_COINS = ['NEARUSDT', 'FETUSDT', 'INJUSDT', 'RENDERUSDT', 'SUIUSDT', 'SEIUSDT', 'APTUSDT', 'ARBUSDT'];
const WALLET_FILE = path.join(__dirname, 'sandbox_wallet.json');
const TRADES_FILE = path.join(__dirname, 'sandbox_trades.json');
const MODEL_PATH = path.join(__dirname, 'model_universal_pro.onnx'); // Trỏ đến file não dùng chung của bạn

const FIXED_MARGIN = 1.0; // 1 USD mỗi lệnh
const TAKER_FEE = 0.0004; // 0.04% phí sàn Binance

class UniversalTester {
    constructor() {
        this.wallet = 100.0;
        this.activeTrades = new Map();
        this.marketBuffer = {}; // Lưu nến và micro data
        this.session = null;
        
        this.initStorage();
    }

    initStorage() {
        // Load hoặc tạo ví ảo 100$
        if (fs.existsSync(WALLET_FILE)) {
            this.wallet = JSON.parse(fs.readFileSync(WALLET_FILE)).balance;
        } else {
            fs.writeFileSync(WALLET_FILE, JSON.stringify({ balance: this.wallet }));
        }

        // Tạo file lịch sử nếu chưa có
        if (!fs.existsSync(TRADES_FILE)) {
            fs.writeFileSync(TRADES_FILE, '[]');
        }

        // Khởi tạo buffer cho các coin + mỏ neo BTCUSDT
        ['BTCUSDT', ...TEST_COINS].forEach(sym => {
            this.marketBuffer[sym] = {
                candles: [],
                micro: { ob_imb: 0.5, spread: 0, liq_l: 0, liq_s: 0, max_b: 0, max_s: 0 }
            };
        });
    }

    async loadModel() {
        if (!fs.existsSync(MODEL_PATH)) {
            console.error(`❌ Không tìm thấy bộ não dùng chung tại ${MODEL_PATH}`);
            process.exit(1);
        }
        this.session = await ort.InferenceSession.create(MODEL_PATH, { executionProviders: ['cpu'] });
        console.log(`🧠 [AI] Đã nạp thành công bộ não Universal! Vốn hiện tại: $${this.wallet.toFixed(2)}`);
    }

    startWebsocket() {
        const streams = [];
        ['BTCUSDT', ...TEST_COINS].forEach(sym => {
            const s = sym.toLowerCase();
            streams.push(`${s}@kline_1m`);
            streams.push(`${s}@aggTrade`);
            streams.push(`${s}@bookTicker`); // Lấy Spread và Orderbook Imbalance nhẹ
        });
        streams.push('!forceOrder@arr'); // Lấy thanh lý toàn thị trường

        const wsUrl = `wss://fstream.binance.com/stream?streams=${streams.join('/')}`;
        const ws = new WebSocket(wsUrl);

        console.log(`📡 [WS] Đang nghe lén ${TEST_COINS.length} coin (Sandbox Mode)...`);

        ws.on('message', (msg) => {
            const data = JSON.parse(msg).data;
            if (!data) return;

            const e = data.e;
            if (e === 'kline' && data.k.x) this.handleCandleClose(data);
            else if (e === 'aggTrade') this.handleAggTrade(data);
            else if (e === 'bookTicker') this.handleBookTicker(data);
            else if (e === 'forceOrder') this.handleForceOrder(data);

            // Mồi tick giá vào luồng giám sát lệnh
            if (e === 'aggTrade') this.monitorActiveTrades(data.s.toUpperCase(), parseFloat(data.p));
        });

        ws.on('close', () => {
            console.log('⚠️ Mất kết nối WS. Bật lại sau 3s...');
            setTimeout(() => this.startWebsocket(), 3000);
        });
    }

    // --- CÁC HÀM XỬ LÝ SỰ KIỆN DATA THU GỌN ---
    handleAggTrade(data) {
        const sym = data.s.toUpperCase();
        if (!this.marketBuffer[sym]) return;
        const val = parseFloat(data.p) * parseFloat(data.q);
        if (data.m && val > this.marketBuffer[sym].micro.max_s) this.marketBuffer[sym].micro.max_s = val;
        if (!data.m && val > this.marketBuffer[sym].micro.max_b) this.marketBuffer[sym].micro.max_b = val;
    }

    handleBookTicker(data) {
        const sym = data.s.toUpperCase();
        if (!this.marketBuffer[sym]) return;
        const bidP = parseFloat(data.b), bidQ = parseFloat(data.B);
        const askP = parseFloat(data.a), askQ = parseFloat(data.A);
        this.marketBuffer[sym].micro.spread = askP - bidP;
        this.marketBuffer[sym].micro.ob_imb = (bidP * bidQ) / ((bidP * bidQ) + (askP * askQ) + 0.0001);
    }

    handleForceOrder(data) {
        const sym = data.o.s.toUpperCase();
        if (!this.marketBuffer[sym]) return;
        const val = parseFloat(data.o.p) * parseFloat(data.o.q);
        if (data.o.S === 'BUY') this.marketBuffer[sym].micro.liq_s += val;
        else this.marketBuffer[sym].micro.liq_l += val;
    }

    // --- XỬ LÝ LÕI KHI ĐÓNG NẾN (BÓP CÒ) ---
    async handleCandleClose(data) {
        const sym = data.s.toUpperCase();
        const candle = {
            open: parseFloat(data.k.o), high: parseFloat(data.k.h),
            low: parseFloat(data.k.l), close: parseFloat(data.k.c),
            vol: parseFloat(data.k.v), quoteVol: parseFloat(data.k.q)
        };
        
        this.marketBuffer[sym].candles.push(candle);
        if (this.marketBuffer[sym].candles.length > 20) this.marketBuffer[sym].candles.shift();

        // 1. Chỉ phân tích nếu không phải BTC (BTC chỉ làm mỏ neo) và có đủ 20 nến tính ATR
        if (sym === 'BTCUSDT' || this.marketBuffer[sym].candles.length < 20) return;
        if (this.activeTrades.has(sym)) return; // Đang ôm lệnh thì không bóp cò thêm

        // 2. Tính toán Features nhanh
        const current = candle;
        const prev = this.marketBuffer[sym].candles[this.marketBuffer[sym].candles.length - 2];
        
        const btcCandles = this.marketBuffer['BTCUSDT'].candles;
        let btc_rs = 0;
        if (btcCandles.length >= 2) {
            const btcCurr = btcCandles[btcCandles.length - 1];
            const btcPrev = btcCandles[btcCandles.length - 2];
            const coin_pct = (current.close - prev.close) / prev.close;
            const btc_pct = (btcCurr.close - btcPrev.close) / btcPrev.close;
            btc_rs = (coin_pct - btc_pct) * 100;
        }

        const body_size = (Math.abs(current.close - current.open) / current.open) * 100;
        const wick_size = (((current.high - current.low) - Math.abs(current.close - current.open)) / current.open) * 100;
        const micro = this.marketBuffer[sym].micro;

        // Mảng 13 features chuẩn (Tương thích với train_master)
        const features = [
            body_size, wick_size, current.vol, 0, 0, // 0 = funding, 0 = vpin (Rút gọn cho Sandbox)
            micro.ob_imb, micro.liq_l, micro.liq_s,
            micro.max_b, micro.max_s, btc_rs, micro.spread, current.close
        ];

        // Reset Micro
        this.marketBuffer[sym].micro = { ob_imb: 0.5, spread: 0, liq_l: 0, liq_s: 0, max_b: 0, max_s: 0 };

        // 3. Đưa vào Lò AI dự đoán
        try {
            const tensor = new ort.Tensor('float32', Float32Array.from(features), [1, 13]);
            const results = await this.session.run({ float_input: tensor });
            
            const outName = this.session.outputNames[1] || 'probabilities';
            const probs = results[outName]?.data;
            if (!probs) return;

            const p_short = probs[0], p_long = probs[1], p_noise = probs[2];
            
            let side = null;
            let winProb = 0;

            if (p_long > 0.6 && p_long > p_noise) { side = 'LONG'; winProb = p_long; }
            else if (p_short > 0.6 && p_short > p_noise) { side = 'SHORT'; winProb = p_short; }

            if (!side) return;

            // 4. LOGIC ĐÒN BẨY ĐỘNG (DYNAMIC LEVERAGE)
            let lev = 5; 
            if (winProb >= 0.8) lev = 20;
            else if (winProb >= 0.7) lev = 15;
            else if (winProb >= 0.65) lev = 10;

            // 5. TÍNH ATR ĐỂ ĐẶT SL/TP
            let atrSum = 0;
            const history = this.marketBuffer[sym].candles;
            for(let i=0; i < history.length - 1; i++) {
                atrSum += (history[i].high - history[i].low);
            }
            const atr = atrSum / 19;
            
            const slDist = atr * 1.5;
            const tpDist = atr * 3.0;

            const entry = current.close;
            const sl = side === 'LONG' ? entry - slDist : entry + slDist;
            const tp = side === 'LONG' ? entry + tpDist : entry - tpDist;
            
            // Điểm kích hoạt Trailing Stop (Đi được 50% quãng đường)
            const trailingAct = side === 'LONG' ? entry + (tpDist * 0.5) : entry - (tpDist * 0.5);

            // 6. KHỚP LỆNH ẢO
            this.activeTrades.set(sym, {
                symbol: sym, side, entry, sl, tp, lev, 
                trailingAct, isTrailing: false, extremePrice: entry,
                margin: FIXED_MARGIN, size: (FIXED_MARGIN * lev) / entry,
                time: new Date().toISOString(), winProb
            });

            console.log(`\n🎯 [BÓP CÒ] ${side} ${sym} | Prob: ${(winProb*100).toFixed(1)}% | Đòn bẩy: x${lev} | Giá: ${entry}`);

        } catch (e) {
            console.error('Lỗi suy luận ONNX:', e.message);
        }
    }

    // --- LUỒNG GIÁM SÁT LỆNH ---
    monitorActiveTrades(symbol, currentPrice) {
        if (!this.activeTrades.has(symbol)) return;
        const trade = this.activeTrades.get(symbol);

        let isClosed = false;
        let closeReason = '';

        if (trade.side === 'LONG') {
            if (currentPrice > trade.extremePrice) trade.extremePrice = currentPrice;
            
            // Kích hoạt Trailing
            if (!trade.isTrailing && currentPrice >= trade.trailingAct) {
                trade.isTrailing = true;
                console.log(`🔥 [TRAILING] ${symbol} đã kích hoạt bảo vệ lãi!`);
            }

            // Check SL / TP
            if (currentPrice <= trade.sl) { isClosed = true; closeReason = 'SL'; }
            else if (currentPrice >= trade.tp && !trade.isTrailing) { isClosed = true; closeReason = 'TP'; }
            else if (trade.isTrailing) {
                const dynamicSl = trade.extremePrice * (1 - 0.005); // Trailing 0.5%
                if (currentPrice <= dynamicSl) { isClosed = true; closeReason = 'TRAILING_STOP'; }
            }
        } else { // SHORT
            if (currentPrice < trade.extremePrice) trade.extremePrice = currentPrice;
            
            if (!trade.isTrailing && currentPrice <= trade.trailingAct) {
                trade.isTrailing = true;
                console.log(`🔥 [TRAILING] ${symbol} đã kích hoạt bảo vệ lãi!`);
            }

            if (currentPrice >= trade.sl) { isClosed = true; closeReason = 'SL'; }
            else if (currentPrice <= trade.tp && !trade.isTrailing) { isClosed = true; closeReason = 'TP'; }
            else if (trade.isTrailing) {
                const dynamicSl = trade.extremePrice * (1 + 0.005);
                if (currentPrice >= dynamicSl) { isClosed = true; closeReason = 'TRAILING_STOP'; }
            }
        }

        if (isClosed) this.closeTrade(trade, currentPrice, closeReason);
    }

    closeTrade(trade, exitPrice, reason) {
        this.activeTrades.delete(trade.symbol);

        // Tính PnL thật (Quy ra USDT)
        let rawPnl = 0;
        if (trade.side === 'LONG') rawPnl = (exitPrice - trade.entry) * trade.size;
        else rawPnl = (trade.entry - exitPrice) * trade.size;

        // Tính phí sàn (Mở lệnh + Đóng lệnh) dựa trên Notional Size
        const notionalSize = trade.margin * trade.lev; 
        const fee = (notionalSize * TAKER_FEE) * 2; 

        const netPnl = rawPnl - fee;
        this.wallet += netPnl;

        // Ghi log
        const logData = {
            symbol: trade.symbol,
            side: trade.side,
            lev: trade.lev,
            prob: trade.winProb.toFixed(3),
            reason: reason,
            pnl: parseFloat(netPnl.toFixed(4)),
            wallet: parseFloat(this.wallet.toFixed(2)),
            time: new Date().toISOString()
        };

        // Ghi đè cập nhật Ví
        fs.writeFileSync(WALLET_FILE, JSON.stringify({ balance: this.wallet }));
        
        // Ghi tiếp vào Lịch sử lệnh
        const history = JSON.parse(fs.readFileSync(TRADES_FILE));
        history.push(logData);
        fs.writeFileSync(TRADES_FILE, JSON.stringify(history, null, 2));

        const icon = netPnl > 0 ? '✅' : '❌';
        console.log(`${icon} [ĐÓNG LỆNH] ${trade.symbol} | Lý do: ${reason} | PnL: $${netPnl.toFixed(4)} | Tổng vốn: $${this.wallet.toFixed(2)}`);
    }
}

// KHỞI ĐỘNG
const tester = new UniversalTester();
tester.loadModel().then(() => {
    tester.startWebsocket();
});