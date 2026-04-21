/**
 * universal_tester_pro.js - PRO RESEARCH EDITION (Z-Score & Correlation Sync)
 * Tái tạo chính xác 100% logic tiền xử lý của UniversalFurnacePro (Python).
 * Chạy độc lập, không cần MongoDB.
 */
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const ort = require('onnxruntime-node');

// --- CẤU HÌNH THỬ NGHIỆM ---
const TEST_COINS = ['NEARUSDT', 'FETUSDT', 'INJUSDT', 'RENDERUSDT', 'SUIUSDT', 'SEIUSDT', 'APTUSDT', 'ARBUSDT'];
const DATA_DIR = path.join(__dirname, 'data');
const WALLET_FILE = path.join(DATA_DIR, 'sandbox_wallet.json');
const TRADES_FILE = path.join(DATA_DIR, 'trade_performance.json');
const MARKET_LOG_FILE = path.join(DATA_DIR, 'market_experience.jsonl'); 
const MODEL_PATH = path.join(__dirname, 'model_universal_pro.onnx'); // File ONNX 11 Features

const FIXED_MARGIN = 1.0; 
const TAKER_FEE = 0.0004; // 0.04%

// --- HÀM TOÁN HỌC (Giả lập Pandas trong Python) ---
const getMean = (arr) => arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : 0;
const getStd = (arr, mean) => {
    if (arr.length < 2) return 1e-8; // Tránh chia cho 0
    const variance = arr.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / (arr.length - 1);
    return Math.sqrt(variance) + 1e-8;
};
const getPearsonCorr = (xs, ys) => {
    if (xs.length !== ys.length || xs.length === 0) return 0;
    const meanX = getMean(xs), meanY = getMean(ys);
    let num = 0, denX = 0, denY = 0;
    for (let i = 0; i < xs.length; i++) {
        const dx = xs[i] - meanX;
        const dy = ys[i] - meanY;
        num += dx * dy;
        denX += dx * dx;
        denY += dy * dy;
    }
    if (denX === 0 || denY === 0) return 0;
    return num / Math.sqrt(denX * denY);
};

class UniversalTesterProMax {
    constructor() {
        this.wallet = 100.0;
        this.activeTrades = new Map();
        this.marketBuffer = {};
        this.session = null;
        this.initStorage();
    }

    initStorage() {
        if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR);

        if (fs.existsSync(WALLET_FILE)) {
            this.wallet = JSON.parse(fs.readFileSync(WALLET_FILE)).balance;
        } else {
            fs.writeFileSync(WALLET_FILE, JSON.stringify({ balance: this.wallet }));
        }

        if (!fs.existsSync(TRADES_FILE)) fs.writeFileSync(TRADES_FILE, '[]');

        ['BTCUSDT', ...TEST_COINS].forEach(sym => {
            this.marketBuffer[sym] = {
                candles: [], // Lưu trữ lịch sử 25 nến để tính Rolling
                micro: { ob_imb_top20: 0.5, spread_close: 0, bid_vol_1pct: 0, ask_vol_1pct: 0, liq_long_vol: 0, liq_short_vol: 0 }
            };
        });
    }

    async loadModel() {
        if (!fs.existsSync(MODEL_PATH)) {
            console.error(`❌ Không tìm thấy model tại: ${MODEL_PATH}`);
            process.exit(1);
        }
        this.session = await ort.InferenceSession.create(MODEL_PATH);
        console.log(`🧠 [AI] Đã nạp Model Tương quan Động (11 Features). Vốn: $${this.wallet.toFixed(2)}`);
    }

    logMarketExperience(symbol, features, label = 2) {
        const entry = { timestamp: Date.now(), symbol, features, suggested_label: label };
        fs.appendFileSync(MARKET_LOG_FILE, JSON.stringify(entry) + '\n');
    }

    startWebsocket() {
        const streams = [];
        ['BTCUSDT', ...TEST_COINS].forEach(sym => {
            const s = sym.toLowerCase();
            streams.push(`${s}@kline_1m`, `${s}@aggTrade`, `${s}@depth20@100ms`);
        });
        streams.push('!forceOrder@arr');

        const wsUrl = `wss://fstream.binance.com/stream?streams=${streams.join('/')}`;
        const ws = new WebSocket(wsUrl);

        console.log(`📡 [WS-SYNC] Đang nghe lén FULL DATA của ${TEST_COINS.length} Altcoins + BTC...`);

        ws.on('message', (msg) => {
            const raw = JSON.parse(msg);
            const data = raw.data;
            if (!data) return;

            const stream = raw.stream;
            const e = data.e;

            if (e === 'kline' && data.k.x) this.handleCandleClose(data);
            else if (stream && stream.includes('@depth20')) this.handleDepth20(data);
            else if (e === 'forceOrder') this.handleForceOrder(data);

            if (e === 'aggTrade') this.monitorActiveTrades(data.s.toUpperCase(), parseFloat(data.p));
        });

        ws.on('close', () => setTimeout(() => this.startWebsocket(), 3000));
        ws.on('error', (err) => console.error('WS Error:', err.message));
    }

    // --- THU THẬP VI CẤU TRÚC ---
    handleDepth20(data) {
        const sym = data.s.toUpperCase();
        if (!this.marketBuffer[sym]) return;

        let bidVol = 0, askVol = 0;
        let bidVol1pct = 0, askVol1pct = 0;
        const bestBid = data.b[0] ? parseFloat(data.b[0][0]) : 0;
        const bestAsk = data.a[0] ? parseFloat(data.a[0][0]) : 0;

        if (data.b) data.b.forEach(l => {
            const vol = parseFloat(l[1]); bidVol += vol;
            if (bestBid > 0 && parseFloat(l[0]) >= bestBid * 0.99) bidVol1pct += vol;
        });
        if (data.a) data.a.forEach(l => {
            const vol = parseFloat(l[1]); askVol += vol;
            if (bestAsk > 0 && parseFloat(l[0]) <= bestAsk * 1.01) askVol1pct += vol;
        });

        const micro = this.marketBuffer[sym].micro;
        const totalVol = bidVol + askVol;
        if (totalVol > 0) micro.ob_imb_top20 = bidVol / totalVol;
        if (bestBid && bestAsk) micro.spread_close = bestAsk - bestBid;
        micro.bid_vol_1pct = bidVol1pct;
        micro.ask_vol_1pct = askVol1pct;
    }

    handleForceOrder(data) {
        const sym = data.o.s.toUpperCase();
        if (!this.marketBuffer[sym]) return;
        const val = parseFloat(data.o.p) * parseFloat(data.o.q);
        if (data.o.S === 'BUY') this.marketBuffer[sym].micro.liq_s += val;
        else this.marketBuffer[sym].micro.liq_l += val;
    }

    // --- LÕI TÍNH TOÁN & BÓP CÒ ---
    async handleCandleClose(data) {
        const sym = data.s.toUpperCase();
        const k = data.k;
        const currentOpen = parseFloat(k.o), currentClose = parseFloat(k.c);
        
        const candleData = {
            open: currentOpen, high: parseFloat(k.h), low: parseFloat(k.l), close: currentClose,
            quoteVol: parseFloat(k.q), takerBuyQuote: parseFloat(k.Q),
            coin_pct: 0, btc_pct: 0, liq_net: 0
        };

        const history = this.marketBuffer[sym].candles;
        if (history.length > 0) {
            const prevClose = history[history.length - 1].close;
            candleData.coin_pct = (currentClose - prevClose) / prevClose;
        }
        
        // Ghi nhận Liq Net của nến này
        candleData.liq_net = this.marketBuffer[sym].micro.liq_long_vol - this.marketBuffer[sym].micro.liq_short_vol;

        // Lưu nến vào mảng (giới hạn 25 nến)
        history.push(candleData);
        if (history.length > 25) history.shift();

        // 🟢 NẾU LÀ BTC: Cập nhật % thay đổi vào mảng để Altcoin tính toán
        if (sym === 'BTCUSDT') return;

        // Bỏ qua nếu chưa đủ 20 nến để tính Rolling
        if (history.length < 20 || this.marketBuffer['BTCUSDT'].candles.length < 20) return;

        // Lấy lịch sử 20 nến gần nhất để tính toán
        const rolling20 = history.slice(-20);
        const btcRolling20 = this.marketBuffer['BTCUSDT'].candles.slice(-20);

        // 1. Đồng bộ hóa btc_pct vào rolling20 của Altcoin
        const coinPctArr = [], btcPctArr = [], quoteVolArr = [], liqNetArr = [];
        let atrSum = 0;

        for (let i = 0; i < 20; i++) {
            coinPctArr.push(rolling20[i].coin_pct);
            btcPctArr.push(btcRolling20[i].coin_pct); // Lấy phần trăm của BTC
            quoteVolArr.push(rolling20[i].quoteVol);
            liqNetArr.push(rolling20[i].liq_net);
            atrSum += (rolling20[i].high - rolling20[i].low);
        }

        // --- BẮT ĐẦU CHUẨN HÓA DỮ LIỆU (Giống hệt Python) ---
        const micro = this.marketBuffer[sym].micro;
        const atr_20 = atrSum / 20;

        // Chuẩn hóa Nến
        const body_abs = Math.abs(currentClose - currentOpen);
        const wick_abs = (parseFloat(k.h) - parseFloat(k.l)) - body_abs;
        const body_to_atr = Math.min(body_abs / (atr_20 + 1e-8), 5.0);
        const wick_to_atr = Math.min(wick_abs / (atr_20 + 1e-8), 5.0);

        // Chuẩn hóa Z-Score Khối lượng
        const volMean = getMean(quoteVolArr);
        const volStd = getStd(quoteVolArr, volMean);
        const vol_zscore = (candleData.quoteVol - volMean) / volStd;

        // Chuẩn hóa Z-Score Thanh lý
        const liqMean = getMean(liqNetArr);
        const liqStd = getStd(liqNetArr, liqMean);
        const liq_imbalance_zscore = (candleData.liq_net - liqMean) / liqStd;

        // Tương quan & BTC
        const taker_buy_ratio = candleData.takerBuyQuote / (candleData.quoteVol + 1e-8);
        const btc_relative_strength = candleData.coin_pct - btcRolling20[19].coin_pct;
        const btc_corr_20 = getPearsonCorr(coinPctArr, btcPctArr);

        // MẢNG 11 FEATURES CHUẨN ĐỂ ĐƯA VÀO ONNX
        const features = [
            micro.ob_imb_top20,
            micro.spread_close,
            micro.bid_vol_1pct,
            micro.ask_vol_1pct,
            taker_buy_ratio,
            body_to_atr,
            wick_to_atr,
            vol_zscore,
            liq_imbalance_zscore,
            btc_relative_strength,
            btc_corr_20
        ];

        // Ghi Log phục vụ Train lại
        this.logMarketExperience(sym, features);

        // Reset Micro
        this.marketBuffer[sym].micro = { ob_imb_top20: 0.5, spread_close: 0, bid_vol_1pct: 0, ask_vol_1pct: 0, liq_long_vol: 0, liq_short_vol: 0 };

        if (this.activeTrades.has(sym)) return;

        // --- GỌI AI SUY LUẬN ---
        try {
            const tensor = new ort.Tensor('float32', Float32Array.from(features), [1, 11]);
            const results = await this.session.run({ float_input: tensor });
            const probs = results[this.session.outputNames[1]]?.data;
            if (!probs) return;

            let side = null, winProb = 0;
            if (probs[1] > 0.65) { side = 'LONG'; winProb = probs[1]; }
            else if (probs[0] > 0.65) { side = 'SHORT'; winProb = probs[0]; }

            if (side) {
                // Đòn bẩy động & ATR Limit
                let lev = 5;
                if (winProb >= 0.8) lev = 20;
                else if (winProb >= 0.75) lev = 15;
                else if (winProb >= 0.7) lev = 10;

                const slDist = atr_20 * 1.5;
                const tpDist = atr_20 * 3.0;
                const entry = currentClose;
                
                this.activeTrades.set(sym, {
                    symbol: sym, side, entry, lev, winProb,
                    sl: side === 'LONG' ? entry - slDist : entry + slDist,
                    tp: side === 'LONG' ? entry + tpDist : entry - tpDist,
                    trailingAct: side === 'LONG' ? entry + (tpDist * 0.5) : entry - (tpDist * 0.5),
                    isTrailing: false, extremePrice: entry,
                    margin: FIXED_MARGIN, size: (FIXED_MARGIN * lev) / entry,
                    features_at_entry: features,
                    time: new Date().toISOString()
                });

                console.log(`\n🎯 [AI FIRE] ${side} ${sym} | Độ tự tin: ${(winProb*100).toFixed(1)}% | Đòn bẩy: x${lev} | Giá: ${entry}`);
                console.log(`   └ Tương quan BTC: ${btc_corr_20.toFixed(2)} | Đột biến Vol: ${vol_zscore.toFixed(2)} Z`);
            }
        } catch (e) {
            console.error(`❌ Lỗi suy luận ONNX ${sym}:`, e.message);
        }
    }

    // --- GIÁM SÁT LỆNH ---
    monitorActiveTrades(symbol, currentPrice) {
        if (!this.activeTrades.has(symbol)) return;
        const trade = this.activeTrades.get(symbol);
        let isClosed = false, closeReason = '';

        if (trade.side === 'LONG') {
            if (currentPrice > trade.extremePrice) trade.extremePrice = currentPrice;
            if (!trade.isTrailing && currentPrice >= trade.trailingAct) { trade.isTrailing = true; console.log(`🔥 [TRAILING] ${symbol}`); }
            if (currentPrice <= trade.sl) { isClosed = true; closeReason = 'SL'; }
            else if (currentPrice >= trade.tp && !trade.isTrailing) { isClosed = true; closeReason = 'TP'; }
            else if (trade.isTrailing && currentPrice <= trade.extremePrice * 0.995) { isClosed = true; closeReason = 'TRAILING_STOP'; }
        } else {
            if (currentPrice < trade.extremePrice) trade.extremePrice = currentPrice;
            if (!trade.isTrailing && currentPrice <= trade.trailingAct) { trade.isTrailing = true; console.log(`🔥 [TRAILING] ${symbol}`); }
            if (currentPrice >= trade.sl) { isClosed = true; closeReason = 'SL'; }
            else if (currentPrice <= trade.tp && !trade.isTrailing) { isClosed = true; closeReason = 'TP'; }
            else if (trade.isTrailing && currentPrice >= trade.extremePrice * 1.005) { isClosed = true; closeReason = 'TRAILING_STOP'; }
        }

        if (isClosed) this.closeTrade(trade, currentPrice, closeReason);
    }

    closeTrade(trade, exitPrice, reason) {
        this.activeTrades.delete(trade.symbol);
        const rawPnl = trade.side === 'LONG' ? (exitPrice - trade.entry) * trade.size : (trade.entry - exitPrice) * trade.size;
        const fee = (trade.margin * trade.lev * TAKER_FEE) * 2;
        const netPnl = rawPnl - fee;
        this.wallet += netPnl;

        const logEntry = { ...trade, exit: exitPrice, close_reason: reason, fee: fee.toFixed(4), net_pnl: netPnl.toFixed(4), final_wallet: this.wallet.toFixed(2) };
        
        const history = JSON.parse(fs.readFileSync(TRADES_FILE));
        history.push(logEntry);
        fs.writeFileSync(TRADES_FILE, JSON.stringify(history, null, 2));
        fs.writeFileSync(WALLET_FILE, JSON.stringify({ balance: this.wallet }));

        console.log(`${netPnl > 0 ? '✅' : '❌'} [CLOSE] ${trade.symbol} | Lý do: ${reason} | PnL: $${netPnl.toFixed(4)} | Vốn: $${this.wallet.toFixed(2)}`);
    }
}

const tester = new UniversalTesterProMax();
tester.loadModel().then(() => tester.startWebsocket());