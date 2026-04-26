const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const fs = require('fs');
const path = require('path');
const ort = require('onnxruntime-node');
const mongoose = require('mongoose');

require('dotenv').config({ path: path.join(__dirname, '../../../.env') });

if (process.env.MONGO_URI_SCOUT) {
    mongoose.connect(process.env.MONGO_URI_SCOUT)
        .then(() => console.log("📦 [MONGO] Order Manager đã kết nối DB thành công!"))
        .catch(err => console.error("❌ [MONGO] Lỗi kết nối DB:", err.message));
}

const exchange = require('./PaperExchange');
const riskGuard = require('./RiskGuard');

// ==========================================
// 1. CẤU HÌNH & KHỞI TẠO NÃO AI
// ==========================================
let aiSession = null;
let aiInputName = '';
let aiOutputName = '';

const configPath = path.join(__dirname, '../../../system_config.json');
const modelPath = path.join(__dirname, '../../../Universal_Scout.onnx');

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
        aiOutputName = aiSession.outputNames[aiSession.outputNames.length - 1];
        console.log(`🧠 [AI ENGINE] Online. Sẵn sàng bóp cò!`);
    } catch (e) {
        console.error(`❌ [AI FATAL] Lỗi nạp ONNX: ${e.message}`);
        process.exit(1);
    }
}
initAI();

const subClient = new Redis(config.REDIS_URL);
const pubClient = new Redis(config.REDIS_URL);
const dataClient = new Redis(config.REDIS_URL);

// --- BỘ NHỚ TRẠNG THÁI ---
const activeTrades = new Map();
const latestFeatures = new Map();
const macroCache = new Map(); 
const cooldowns = new Map(); 
const isExecuting = new Set(); 

const MAX_ACTIVE_TRADES = 3; 

// ==========================================
// 2. ĐỒNG BỘ DỮ LIỆU NGOẠI VI
// ==========================================
setInterval(async () => {
    try {
        const scores = await dataClient.hgetall(config.CHANNELS?.MACRO_SCORES || 'macro:scores');
        for (const [sym, score] of Object.entries(scores)) {
            macroCache.set(sym.toUpperCase(), parseFloat(score));
        }
    } catch (e) {}
}, 5000);

function logThought(symbol, message) {
    const ts = Date.now();
    console.log(`[${symbol}] ${message}`);
    try { pubClient.publish('dashboard:logs', JSON.stringify({ symbol, msg: message, ts })); } catch (e) { }
}

function syncStream(symbol, action) {
    try { 
        pubClient.publish(config.CHANNELS?.SUBSCRIPTIONS || 'system:subscriptions', JSON.stringify({ 
            action, 
            symbol, 
            client: "oms" 
        })); 
    } catch (e) { }
}

function logExperience(symbol, tradeType, entryPrice, exitPrice, prob, features, reason) {
    const pnlPct = tradeType === 'LONG' 
        ? ((exitPrice - entryPrice) / entryPrice) * 100 
        : ((entryPrice - exitPrice) / entryPrice) * 100;
        
    const payload = {
        symbol,
        ts_exit: Date.now(),
        trade_type: tradeType,
        prob_score: prob,
        pnl_pct: parseFloat(pnlPct.toFixed(2)),
        reason: reason,
        features_at_entry: features
    };
    pubClient.publish('experience:raw', JSON.stringify(payload));
}

// ==========================================
// 3. VÒNG LẶP HFT CHÍNH
// ==========================================
subClient.psubscribe(config.CHANNELS?.FEATURES || 'market:features:*');

subClient.on('pmessageBuffer', async (pattern, channel, messageBuffer) => {
    let feature;
    try { feature = decode(messageBuffer); } catch (e) { return; }

    if (!feature || !feature.is_warm || !aiSession) return;

    const symbol = feature.symbol.toUpperCase();
    const currentPrice = feature.last_price || 0; 
    if (currentPrice === 0) return;

    latestFeatures.set(symbol, feature);
    exchange.updateTick(symbol, currentPrice);

    // ------------------------------------------
    // NHÁNH A: QUẢN LÝ LỆNH ĐANG CHẠY (TRAILING STOP & CẮT LỖ)
    // ------------------------------------------
    if (activeTrades.has(symbol)) {
        const trade = activeTrades.get(symbol);
        
        if (trade.type === 'LONG' && currentPrice > trade.highestPrice) trade.highestPrice = currentPrice;
        if (trade.type === 'SHORT' && (!trade.lowestPrice || currentPrice < trade.lowestPrice)) trade.lowestPrice = currentPrice;

        const atrPercent = feature.ATR14 || 0.003;
        const stopMultiplier = (Math.abs(feature.MFA) > 4.0 ? 1.5 : 3.0); 
        const stopDistance = atrPercent * stopMultiplier;

        let shouldClose = false;
        let closeReason = "";

        if (trade.type === 'LONG') {
            const slPrice = trade.highestPrice * (1 - stopDistance);
            if (currentPrice <= slPrice) { shouldClose = true; closeReason = "LONG: Trailing Stop Hit"; }
            else if (feature.MFA < -5.0) { shouldClose = true; closeReason = "LONG: Dump mạnh (MFA âm sâu)"; }
        } else {
            const slPrice = trade.lowestPrice * (1 + stopDistance); 
            if (currentPrice >= slPrice) { shouldClose = true; closeReason = "SHORT: Trailing Stop Hit"; }
            else if (feature.MFA > 5.0) { shouldClose = true; closeReason = "SHORT: Pump mạnh (MFA dương gắt)"; }
        }

        if (shouldClose) {
            logThought(symbol, `🛑 [ĐÓNG LỆNH] ${closeReason} @ ${currentPrice.toFixed(4)}`);
            activeTrades.delete(symbol);
            syncStream(symbol, 'UNSUBSCRIBE'); // Báo FeedHandler có thể rút ống nếu Radar cũng nhả
            
            await exchange.closeTrade(symbol, currentPrice, closeReason, trade.features);
            logExperience(symbol, trade.type, trade.entryPrice, currentPrice, trade.prob, trade.features, closeReason);
            
            cooldowns.set(symbol, Date.now() + 15 * 60 * 1000); 
        }
        return; 
    }

    // ------------------------------------------
    // NHÁNH B: KIỂM TRA ĐIỀU KIỆN & Ổ KHÓA BẢO VỆ
    // ------------------------------------------
    if (isExecuting.has(symbol)) return;
    if (cooldowns.has(symbol) && Date.now() < cooldowns.get(symbol)) return;
    
    const macroScore = macroCache.get(symbol) || 0.5;
    if (macroScore < 0.4) return;

    // ------------------------------------------
    // NHÁNH C: AI INFERENCE
    // ------------------------------------------
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

        const probShort = probs[0];
        const probLong = probs[1];

        let tradeAction = null;
        let finalProb = 0;
        
        const now = Date.now();
        if (!latestFeatures.get(`ai_emit_${symbol}`) || now - latestFeatures.get(`ai_emit_${symbol}`) > 1000) {
            try {
                pubClient.publish('dashboard:predictions', JSON.stringify({
                    symbol: symbol,
                    long: probLong,
                    short: probShort
                }));
                latestFeatures.set(`ai_emit_${symbol}`, now);
            } catch (e) {}
        }

        if (probLong >= 0.70) { tradeAction = 'LONG'; finalProb = probLong; }
        else if (probShort >= 0.70) { tradeAction = 'SHORT'; finalProb = probShort; }

        if (!tradeAction) return; 

        // Tính Đòn bẩy
        const kelly = (finalProb * 2 - (1 - finalProb)) / 2;
        let finalLev = Math.floor(kelly * 20);
        finalLev = Math.max(5, Math.min(finalLev, 20));

        // ------------------------------------------
        // NHÁNH D: CƠ CHẾ NHẢY TÀU (EV SWITCH)
        // ------------------------------------------
        if (activeTrades.size >= MAX_ACTIVE_TRADES) {
            let worstTradeSymbol = null;
            let worstTradeProb = 1.0;
            
            for (const [sym, activeT] of activeTrades.entries()) {
                if (activeT.prob < worstTradeProb) {
                    worstTradeProb = activeT.prob;
                    worstTradeSymbol = sym;
                }
            }

            if (worstTradeSymbol && (finalProb - worstTradeProb >= 0.10)) {
                logThought(symbol, `🔥 [EV SWITCH] ${symbol} (${(finalProb*100).toFixed(0)}%) ngon hơn! Cắt ${worstTradeSymbol} (${(worstTradeProb*100).toFixed(0)}%) để lấy slot...`);
                
                const tradeToKill = activeTrades.get(worstTradeSymbol);
                const killPrice = latestFeatures.get(worstTradeSymbol)?.last_price || tradeToKill.entryPrice;
                
                activeTrades.delete(worstTradeSymbol);
                syncStream(worstTradeSymbol, 'UNSUBSCRIBE');
                
                await exchange.closeTrade(worstTradeSymbol, killPrice, "EV Switch", tradeToKill.features);
                logExperience(worstTradeSymbol, tradeToKill.type, tradeToKill.entryPrice, killPrice, tradeToKill.prob, tradeToKill.features, "EV Switch");
            } else {
                return; 
            }
        }

        // ------------------------------------------
        // NHÁNH E: THỰC THI LỆNH (MUTEX LOCKED)
        // ------------------------------------------
        isExecuting.add(symbol); 

        try {
            logThought(symbol, `🧠 [AI BÓP CÒ] Đang gửi lệnh ${tradeAction} sang Binance... (Prob: ${(finalProb * 100).toFixed(1)}%, Lev: ${finalLev}x)`);

            const success = await exchange.openTrade(symbol, 'MARKET', currentPrice, finalLev, finalProb, tradeAction, feature);
            
            if (success) {
                activeTrades.set(symbol, {
                    entryPrice: currentPrice,
                    highestPrice: currentPrice,
                    lowestPrice: currentPrice,
                    type: tradeAction,
                    prob: finalProb,
                    features: feature
                });
                syncStream(symbol, 'ENTER_TRADE');
                logThought(symbol, `✅ [VÀO LỆNH THÀNH CÔNG] Đã khớp ${tradeAction} ${symbol}.`);
            } else {
                cooldowns.set(symbol, Date.now() + 60000);
                logThought(symbol, `❌ [TỪ CHỐI] Lỗi mở lệnh. Block ${symbol} 1 phút.`);
            }
        } catch (err) {
            console.error(`❌ Lỗi thực thi lệnh ${symbol}:`, err.message);
        } finally {
            isExecuting.delete(symbol);
        }

    } catch (err) { 
        console.error(`❌ Inference error ${symbol}:`, err.message); 
    }
});

// ==========================================
// 4. LẮNG NGHE LỆNH HỆ THỐNG & DỌN DẸP
// ==========================================
subClient.subscribe('system:commands');
subClient.on('message', async (channel, message) => {
    if (channel === 'system:commands') {
        try {
            const cmd = JSON.parse(message);
            if (cmd.action === 'RELOAD_AI') {
                console.log("🔄 [HỆ THỐNG] Nhận lệnh RELOAD_AI từ mô-đun Tự học. Đang thay não mới...");
                await initAI(); 
            }
        } catch (e) { }
    }
});

setInterval(() => {
    const now = Date.now();
    for (const [sym, expireTime] of cooldowns.entries()) {
        if (now > expireTime) cooldowns.delete(sym);
    }
}, 60000);

console.log("🚀 [MASTER V17] Xạ Thủ Đã Lên Đạn (Chống Spam + EV Switch + Khóa Mutex + Tự Học)!");