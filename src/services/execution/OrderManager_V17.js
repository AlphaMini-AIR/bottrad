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

setInterval(async () => {
    try {
        const scores = await dataClient.hgetall(config.CHANNELS.MACRO_SCORES || 'macro:scores');
        for (const [sym, score] of Object.entries(scores)) {
            macroCache.set(sym.toUpperCase(), parseFloat(score));
        }
        console.log(`🔄 [MACRO CACHE] Đã cập nhật ${macroCache.size} coin`);
    } catch (e) {
        console.error("⚠️ [REDIS] Lỗi đồng bộ Macro Cache:", e.message);
    }
}, 5000);

function logThought(symbol, message) {
    const ts = Date.now();
    console.log(`[${symbol}] ${message}`);
    try {
        pubClient.publish('dashboard:logs', JSON.stringify({ symbol, msg: message, ts }));
    } catch (e) { }
}

function syncStream(symbol, action) {
    try {
        pubClient.publish('system:keep_alive', JSON.stringify({ symbol, action }));
    } catch (e) { }
}

function simulateLatency(callback) {
    setTimeout(callback, Math.floor(Math.random() * 50) + 50);
}

// ==========================================
// 3. VÒNG LẶP HFT CHÍNH
// ==========================================
subClient.psubscribe(config.CHANNELS.FEATURES);
subClient.subscribe(config.CHANNELS.CANDIDATES);

let featureCount = 0;

subClient.on('pmessageBuffer', async (pattern, channel, messageBuffer) => {
    let feature;
    try {
        feature = decode(messageBuffer);
    } catch (e) { return; }

    if (!feature || !feature.is_warm || !aiSession) return;

    const symbol = feature.symbol;
    const currentPrice = feature.last_price || 0; // Dùng last_price để tính toán đồng nhất
    if (currentPrice === 0) return;

    featureCount++;
    latestFeatures.set(symbol, feature);
    exchange.updateTick(symbol, currentPrice);

    // ------------------------------------------
    // NHÁNH A: QUẢN LÝ LỆNH ĐANG CHẠY (TRAILING STOP 2 CHIỀU)
    // ------------------------------------------
    if (activeTrades.has(symbol)) {
        const trade = activeTrades.get(symbol);
        if (!exchange.hasActivePosition(symbol)) {
            activeTrades.delete(symbol);
            syncStream(symbol, 'EXIT_TRADE');
            return;
        }

        const atrPercent = feature.ATR14 || 0.002;
        const stopMultiplier = (Math.abs(feature.MFA) > 0.5 ? 3.5 : 1.2);
        const stopDistance = atrPercent * stopMultiplier;

        if (trade.type === 'LONG') {
            // Đuổi theo Đỉnh
            if (currentPrice > trade.highestPrice) trade.highestPrice = currentPrice;
            const slPrice = trade.highestPrice * (1 - stopDistance);

            // Chốt nếu giá rơi sâu hoặc MFA báo hiệu đảo chiều giảm
            if (currentPrice <= slPrice || (feature.MFA && feature.MFA < -1.8)) {
                const reason = (feature.MFA < -1.8) ? 'LONG: Đảo chiều giảm mạnh' : `LONG: Trailing Stop`;
                logThought(symbol, `🛑 [CLOSE LONG] ${reason} @ ${currentPrice.toFixed(4)}`);
                await exchange.closeTrade(symbol, currentPrice, reason, trade.features);
                activeTrades.delete(symbol);
            }
        }
        else if (trade.type === 'SHORT') {
            // Đuổi theo Đáy (Sửa lại logic: giá thấp hơn thì cập nhật lowestPrice)
            if (!trade.lowestPrice || currentPrice < trade.lowestPrice) trade.lowestPrice = currentPrice;
            const slPrice = trade.lowestPrice * (1 + stopDistance); // Giá HỒI LÊN chạm slPrice là chốt

            // Chốt nếu giá hồi lên mạnh hoặc MFA báo hiệu đảo chiều tăng
            if (currentPrice >= slPrice || (feature.MFA && feature.MFA > 1.8)) {
                const reason = (feature.MFA > 1.8) ? 'SHORT: Đảo chiều tăng mạnh' : `SHORT: Trailing Stop`;
                logThought(symbol, `🛑 [CLOSE SHORT] ${reason} @ ${currentPrice.toFixed(4)}`);
                await exchange.closeTrade(symbol, currentPrice, reason, trade.features);
                activeTrades.delete(symbol);
            }
        }
        return;
    }

    // ------------------------------------------
    // NHÁNH B: BÓP CÒ AI (INFERENCE 2 CHIỀU)
    // ------------------------------------------
    const macroScore = macroCache.get(symbol);
    if (!macroScore || macroScore < (config.MACRO_THRESHOLD || 0.6)) return;

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

        const probShort = probs[0];
        const probLong = probs[1];

        // Báo cáo song song cả 2 hướng lên Dashboard
        if (featureCount % 10 === 0) {
            logThought(symbol, `🤖 AI: LONG ${(probLong * 100).toFixed(1)}% | SHORT ${(probShort * 100).toFixed(1)}% | Macro: ${macroScore.toFixed(2)}`);
        }

        // 🚀 ĐIỀU KIỆN VÀO LỆNH (Threshold 0.75)
        let tradeAction = null;
        if (probLong >= 0.75) tradeAction = 'LONG';
        else if (probShort >= 0.75) tradeAction = 'SHORT';

        if (tradeAction) {
            const prob = tradeAction === 'LONG' ? probLong : probShort;
            logThought(symbol, `🧠 [AI] Tín hiệu ${tradeAction}: ${(prob * 100).toFixed(1)}%`);

            // Tính Kelly & Leverage
            const kelly = (prob * 2 - (1 - prob)) / 2;
            let finalLev = Math.floor(kelly * config.MAX_LEVERAGE * (config.KELLY_FRACTION || 0.5));
            finalLev = Math.max(1, Math.min(finalLev, 20));

            const useMarket = (feature.VPIN > 0.8 || Math.abs(feature.OFI) > 15);

            simulateLatency(async () => {
                // LONG thì lấy giá Ask (mua), SHORT thì lấy giá Bid (bán)
                const targetPrice = tradeAction === 'LONG'
                    ? (latestFeatures.get(symbol)?.best_ask || currentPrice)
                    : (latestFeatures.get(symbol)?.best_bid || currentPrice);

                if (useMarket) {
                    const success = await exchange.openTrade(symbol, 'MARKET', targetPrice, finalLev, prob, tradeAction, feature);
                    if (success) {
                        activeTrades.set(symbol, {
                            highestPrice: targetPrice,
                            lowestPrice: targetPrice,
                            type: tradeAction,
                            features: feature
                        });
                        syncStream(symbol, 'ENTER_TRADE');
                    }
                } else {
                    pendingOrders.set(symbol, {
                        targetPrice, leverage: finalLev, prob,
                        ts: Date.now(), type: tradeAction, features: feature
                    });
                }
            });
        }
    } catch (err) { console.error(`❌ Inference error ${symbol}:`, err.message); }

    // ------------------------------------------
    // NHÁNH C: KHỚP LỆNH LIMIT CHỜ (2 CHIỀU)
    // ------------------------------------------
    if (pendingOrders.has(symbol)) {
        const order = pendingOrders.get(symbol);
        const canMatch = order.type === 'LONG'
            ? currentPrice <= order.targetPrice
            : currentPrice >= order.targetPrice;

        if (canMatch) {
            logThought(symbol, `🎯 [MATCH] Khớp vùng chờ ${order.type} @ ${order.targetPrice.toFixed(4)}`);
            const success = await exchange.openTrade(symbol, 'LIMIT_MAKER', order.targetPrice, order.leverage, order.prob, order.type, order.features);
            if (success) {
                activeTrades.set(symbol, {
                    highestPrice: order.targetPrice,
                    lowestPrice: order.targetPrice,
                    type: order.type,
                    features: order.features
                });
                syncStream(symbol, 'ENTER_TRADE');
            }
            pendingOrders.delete(symbol);
        }
    }
});

subClient.subscribe(config.CHANNELS.CANDIDATES, 'system:commands'); // Nghe thêm kênh commands

subClient.on('message', async (channel, message) => {
    if (channel === config.CHANNELS.CANDIDATES) {
        try {
            const data = JSON.parse(message);
            const symbol = data.symbol || 'UNKNOWN';
            logThought(symbol, `📡 [RADAR] Nhận ứng viên. Đang đợi dữ liệu Feed...`);
        } catch (e) { }
    }

    // [BỔ SUNG QUAN TRỌNG]: Lắng nghe lệnh tải lại não
    if (channel === 'system:commands') {
        try {
            const cmd = JSON.parse(message);
            if (cmd.action === 'RELOAD_AI') {
                console.log("🔄 [HỆ THỐNG] Nhận lệnh RELOAD_AI. Đang tải lại não bộ mới...");
                await initAI(); // Gọi lại hàm nạp ONNX
                console.log("✅ [HỆ THỐNG] Nạp não mới thành công! Sẵn sàng chiến đấu.");
            }
        } catch (e) { }
    }
});

setInterval(() => {
    const now = Date.now();
    for (const [sym, order] of pendingOrders.entries()) {
        if (now - order.ts > 10000) {
            pendingOrders.delete(sym);
        }
    }
}, 1000);

console.log("🚀 [MASTER V17] Hệ thống Giao dịch AI đã sẵn sàng.");