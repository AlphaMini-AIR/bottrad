const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const fs = require('fs');
const path = require('path');
const ort = require('onnxruntime-node');

const exchange = require('./PaperExchange'); 
const riskGuard = require('./RiskGuard');

// ==========================================
// 1. CẤU HÌNH & KHỞI TẠO NÃO AI
// ==========================================
let aiSession = null;
let aiInputName = '';
let aiOutputName = '';

const configPath = path.join(__dirname, '../../../system_config.json');
const modelPath = path.join(__dirname, '../../../../Universal_Scout.onnx');

let config;
try {
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    console.error("❌ [FATAL] Thiếu file system_config.json");
    process.exit(1);
}

async function initAI() {
    try {
        aiSession = await ort.InferenceSession.create(modelPath);
        aiInputName = aiSession.inputNames[0];
        // Giả định output cuối cùng là tensor xác suất (2 lớp: SHORT/LONG)
        aiOutputName = aiSession.outputNames[aiSession.outputNames.length - 1];
        console.log(`🧠 [AI ENGINE] Online. Input: "${aiInputName}", Output: "${aiOutputName}"`);
    } catch (e) {
        console.error(`❌ [AI FATAL] Lỗi nạp ONNX: ${e.message}`);
        process.exit(1);
    }
}
initAI();

const subClient = new Redis(config.REDIS_URL);
const pubClient = new Redis(config.REDIS_URL);
const dataClient = new Redis(config.REDIS_URL);

const activeTrades = new Map();
const pendingOrders = new Map();
const latestFeatures = new Map();
const macroCache = new Map(); // Lưu điểm xu hướng từ Radar
const MAX_PENDING_ORDERS = 3;

// ==========================================
// 2. ĐỒNG BỘ DỮ LIỆU NGOẠI VI
// ==========================================

// Đồng bộ Macro Scores từ Redis mỗi 5 giây
setInterval(async () => {
    try {
        const scores = await dataClient.hgetall(config.CHANNELS.MACRO_SCORES || 'macro:scores');
        for (const [sym, score] of Object.entries(scores)) {
            macroCache.set(sym.toUpperCase(), parseFloat(score)); // Chuẩn hóa key
        }
    } catch (e) {
        // Bỏ qua lỗi tạm thời
    }
}, 5000);

function logThought(symbol, message) {
    const ts = Date.now();
    console.log(`[${symbol}] ${message}`);
    try {
        pubClient.publish('dashboard:logs', JSON.stringify({ symbol, msg: message, ts }));
    } catch (e) {}
}

function syncStream(symbol, action) {
    try {
        pubClient.publish('system:keep_alive', JSON.stringify({ symbol, action }));
    } catch (e) {}
}

function simulateLatency(callback) {
    setTimeout(callback, Math.floor(Math.random() * 50) + 50);
}

// ==========================================
// 3. VÒNG LẶP HFT CHÍNH
// ==========================================
subClient.psubscribe(config.CHANNELS.FEATURES);
subClient.subscribe(config.CHANNELS.CANDIDATES);

subClient.on('pmessageBuffer', async (pattern, channel, messageBuffer) => {
    let feature;
    try { feature = decode(messageBuffer); } catch (e) { return; }

    if (!feature || !feature.is_warm || !aiSession) return;

    const symbol = feature.symbol;
    const currentPrice = feature.best_ask || feature.last_price || 0;
    if (currentPrice === 0) return;

    latestFeatures.set(symbol, feature);
    
    // Cập nhật sàn ảo và kiểm tra lệnh đã đóng
    exchange.updateTick(symbol, currentPrice);

    if (activeTrades.has(symbol) && !exchange.hasActivePosition(symbol)) {
        activeTrades.delete(symbol);
        syncStream(symbol, 'EXIT_TRADE');
        logThought(symbol, "📉 Lệnh đã đóng trên sàn ảo (SL/Liq). Ngắt stream.");
    }

    // Gác cổng rủi ro
    if (riskGuard.isHalted()) return;
    if (!riskGuard.checkDrawdown(exchange.getWalletBalance())) {
        for (const [s, t] of activeTrades.entries()) {
            await exchange.closeTrade(s, currentPrice, 'KILLSWITCH_DRAWDOWN');
            syncStream(s, 'EXIT_TRADE');
        }
        activeTrades.clear();
        return;
    }

    // ------------------------------------------
    // NHÁNH A: QUẢN LÝ LỆNH ĐANG CHẠY (TRAILING)
    // ------------------------------------------
    if (activeTrades.has(symbol)) {
        const trade = activeTrades.get(symbol);
        if (currentPrice > trade.highestPrice) trade.highestPrice = currentPrice;

        const atrPercent = feature.ATR14 || 0.002; // Fallback 0.2% nếu feed không gửi
        const stopMultiplier = (feature.MFA > 0 ? 3.5 : 1.2);
        const stopDistance = atrPercent * stopMultiplier;
        const slPrice = trade.highestPrice * (1 - stopDistance);

        if (currentPrice <= slPrice || (feature.MFA && feature.MFA < -1.8)) {
            const reason = (feature.MFA < -1.8) ? 'Gia tốc xả mạnh' : `Trailing Stop (${(stopDistance*100).toFixed(2)}%)`;
            logThought(symbol, `🛑 [CLOSE] ${reason} @ ${currentPrice.toFixed(4)}`);
            
            await exchange.closeTrade(symbol, currentPrice, reason);
            activeTrades.delete(symbol);
            syncStream(symbol, 'EXIT_TRADE');
        }
        return;
    }

    // ------------------------------------------
    // NHÁNH B: BÓP CÒ AI (INFERENCE)
    // ------------------------------------------
    
    // Fast Macro Check
    const macroScore = macroCache.get(symbol) || 0;
    if (macroScore < (config.MACRO_THRESHOLD || 0.6)) return;

    if (pendingOrders.has(symbol) || pendingOrders.size >= MAX_PENDING_ORDERS) return;
    if (!riskGuard.canOpenNewTrade(activeTrades.size, pendingOrders.size)) return;

    try {
        const inputData = Float32Array.from([
            feature.ob_imb_top20 || 0, feature.spread_close || 0,
            feature.bid_vol_1pct || 0, feature.ask_vol_1pct || 0,
            feature.max_buy_trade || 0, feature.max_sell_trade || 0,
            feature.liq_long_vol || 0, feature.liq_short_vol || 0,
            feature.funding_rate || 0, feature.taker_buy_ratio || 0,
            feature.body_size || 0, feature.wick_size || 0,
            feature.btc_relative_strength || 0
        ]);

        const tensor = new ort.Tensor('float32', inputData, [1, 13]);
        const feeds = {}; feeds[aiInputName] = tensor;
        const output = await aiSession.run(feeds);
        
        const probs = output[aiOutputName].data;
        const probLong = probs[1]; // Xác suất lớp LONG

        if (probLong >= 0.75) {
            logThought(symbol, `🧠 [AI] Tín hiệu LONG: ${(probLong*100).toFixed(1)}% (Macro: ${macroScore})`);

            const kelly = (probLong * (config.RISK_REWARD_RATIO || 2) - (1 - probLong)) / (config.RISK_REWARD_RATIO || 2);
            let finalLev = Math.floor(kelly * config.MAX_LEVERAGE * (config.KELLY_FRACTION || 0.5));
            finalLev = Math.max(1, Math.min(finalLev, 20));

            const vpin = feature.VPIN || 0;
            const ofi = feature.OFI || 0;
            const useMarket = (vpin > 0.8 || ofi > 15);

            simulateLatency(async () => {
                const freshAsk = latestFeatures.get(symbol)?.best_ask || currentPrice;

                if (useMarket) {
                    const success = await exchange.openTrade(symbol, 'MARKET', freshAsk, finalLev, probLong);
                    if (success) {
                        activeTrades.set(symbol, { highestPrice: freshAsk, type: 'LONG' });
                        syncStream(symbol, 'ENTER_TRADE');
                        logThought(symbol, `⚡ [EXEC] MARKET LONG @ ${freshAsk.toFixed(4)}`);
                    }
                } else {
                    pendingOrders.set(symbol, { targetPrice: freshAsk, leverage: finalLev, prob: probLong, ts: Date.now() });
                    logThought(symbol, `🛡️ [EXEC] Đặt LIMIT @ ${freshAsk.toFixed(4)}`);
                }
            });
        }
    } catch (err) { /* Bỏ qua lỗi inference để giữ luồng chạy */ }

    // ------------------------------------------
    // NHÁNH C: KHỚP LỆNH LIMIT CHỜ
    // ------------------------------------------
    if (pendingOrders.has(symbol)) {
        const order = pendingOrders.get(symbol);
        if (currentPrice <= order.targetPrice) {
            logThought(symbol, `🎯 [MATCH] Khớp vùng chờ ${order.targetPrice.toFixed(4)}`);
            
            const success = await exchange.openTrade(symbol, 'LIMIT_MAKER', order.targetPrice, order.leverage, order.prob);
            if (success) {
                activeTrades.set(symbol, { highestPrice: order.targetPrice, type: 'LONG' });
                syncStream(symbol, 'ENTER_TRADE');
            }
            pendingOrders.delete(symbol);
        }
    }
});

// Lắng nghe tín hiệu Candidates từ Radar
subClient.on('message', (channel, message) => {
    if (channel === config.CHANNELS.CANDIDATES) {
        try {
            const data = JSON.parse(message);
            logThought(data.symbol, `📡 [RADAR] Nhận ứng viên. Đang đợi dữ liệu Feed...`);
        } catch (e) {}
    }
});

// Tự động hủy lệnh Limit treo quá 10 giây
setInterval(() => {
    const now = Date.now();
    for (const [sym, order] of pendingOrders.entries()) {
        if (now - order.ts > 10000) {
            pendingOrders.delete(sym);
        }
    }
}, 1000);

console.log("🚀 [MASTER V17] Hệ thống Giao dịch AI đã sẵn sàng.");