const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const fs = require('fs');

const exchange = require('./PaperExchange'); 
const riskGuard = require('./RiskGuard'); // [CẬP NHẬT TỪ SPRINT 3]

let config;
try {
    config = JSON.parse(fs.readFileSync('./system_config.json', 'utf8'));
} catch (e) {
    console.error("❌ Không tìm thấy system_config.json!");
    process.exit(1);
}

const subClient = new Redis(config.REDIS_URL);
const dataClient = new Redis(config.REDIS_URL);
const pubClient = new Redis(config.REDIS_URL); 

function logThought(symbol, message) {
    console.log(`[${symbol}] ${message}`);
    pubClient.publish('dashboard:logs', JSON.stringify({ symbol: symbol, msg: message, ts: Date.now() }));
}

const activeTrades = new Map();
const macroCache = new Map();
const pendingOrders = new Map();
const latestFeatures = new Map(); 
const MAX_PENDING_ORDERS = 3;

setInterval(async () => {
    try {
        const scores = await dataClient.hgetall(config.CHANNELS.MACRO_SCORES);
        for (const [sym, score] of Object.entries(scores)) macroCache.set(sym, parseFloat(score));
    } catch (e) {}
}, 5000);

function simulateLatency(callback) {
    const latency = Math.floor(Math.random() * 50) + 50; 
    setTimeout(callback, latency);
}

function calculateRealEntryPrice(symbol, orderType, bestAsk, size, feature) {
    if (orderType === 'LIMIT_MAKER') return bestAsk; 
    const notionalValue = size * bestAsk;
    const impactPenalty = (notionalValue / 1000) * 0.0001; 
    const volatilityPenalty = feature.MFA > 1.0 ? (feature.ATR14 || 0.001) * 0.1 : 0; 
    return bestAsk + (bestAsk * (impactPenalty + volatilityPenalty));
}

// [CẬP NHẬT TỪ SPRINT 3]: HÀM PANIC SELL
async function panicSellAll() {
    logThought('SYSTEM', '🚨 THỰC THI LỆNH PANIC SELL TOÀN BỘ THỊ TRƯỜNG 🚨');
    pendingOrders.clear(); 

    for (const [symbol, trade] of activeTrades.entries()) {
        const freshFeature = latestFeatures.get(symbol);
        const currentAsk = freshFeature?.best_ask || trade.highestPrice;
        await exchange.closeTrade(symbol, currentAsk, 'PANIC_SELL_KILLSWITCH');
    }
    
    activeTrades.clear();
    logThought('SYSTEM', '💀 ĐÃ ĐÓNG TẤT CẢ VỊ THẾ. BOT CHÍNH THỨC RÚT PHÍCH CẮM.');
}

setInterval(() => {
    const now = Date.now();
    for (const [symbol, order] of pendingOrders.entries()) {
        if (now - order.timestamp > 10000) { 
            logThought(symbol, `⏳ [HỦY LỆNH] Quá 10s không khớp LIMIT. Giải phóng vốn.`);
            pendingOrders.delete(symbol);
        }
    }
}, 1000);

function calculateSigmoidLeverage(confidenceScore) {
    const p0 = 0.75, k = 10, L_min = 1;
    return L_min + (config.MAX_LEVERAGE - L_min) / (1 + Math.exp(-k * (confidenceScore - p0)));
}
function calculateKellyLeverage(confidenceScore) {
    const kelly = (confidenceScore * config.RISK_REWARD_RATIO - (1 - confidenceScore)) / config.RISK_REWARD_RATIO;
    return config.KELLY_FRACTION * kelly * config.MAX_LEVERAGE;
}

subClient.psubscribe(config.CHANNELS.FEATURES);
subClient.subscribe(config.CHANNELS.CANDIDATES);

subClient.on('pmessageBuffer', (pattern, channel, messageBuffer) => {
    try {
        const feature = decode(messageBuffer);
        if (!feature.is_warm) return;
        const symbol = feature.symbol;

        const currentAsk = feature.best_ask || feature.last_price || 0;
        if (currentAsk === 0) return; 

        latestFeatures.set(symbol, feature);

        // [CẬP NHẬT TỪ SPRINT 3]: HỆ THỐNG PHÒNG THỦ VĨ MÔ
        if (symbol === 'BTCUSDT' && feature.ATR14) {
            riskGuard.checkMacroCircuitBreaker(feature.ATR14 * 100); 
        }

        if (riskGuard.isHalted()) return; 

        const isWalletSafe = riskGuard.checkDrawdown(exchange.getWalletBalance());
        if (!isWalletSafe) {
            panicSellAll(); 
            return; 
        }

        exchange.updateTick(symbol, currentAsk);

        if (activeTrades.has(symbol) && !exchange.activePositions.has(symbol)) {
            activeTrades.delete(symbol);
        }

        if (activeTrades.has(symbol)) {
            const trade = activeTrades.get(symbol);
            if (currentAsk > trade.highestPrice) trade.highestPrice = currentAsk;

            const stopDistance = (feature.ATR14 || 0.001) * (feature.MFA > 0 ? 3.0 : 0.5);
            const dynamicStopPrice = trade.highestPrice * (1 - stopDistance);

            if (currentAsk <= dynamicStopPrice || feature.MFA < -1.5) {
                const reason = feature.MFA < -1.5 ? 'Gia tốc xả mạnh' : 'Chạm Trailing Stop';
                logThought(symbol, `🛑 [CẮT LỆNH] ${reason} ở giá ${currentAsk.toFixed(4)}`);
                
                exchange.closeTrade(symbol, currentAsk, reason).then(() => {
                    activeTrades.delete(symbol);
                });
            }
            return;
        }

        const macroScore = macroCache.get(symbol) || 0;
        if (macroScore < config.MACRO_THRESHOLD) return;

        if (feature.OFI > 5 && feature.MFA > 0.5) {
            if (activeTrades.has(symbol) || pendingOrders.has(symbol)) return;
            
            // [CẬP NHẬT TỪ SPRINT 3]: CHẶN QUÁ TẢI LỆNH
            if (!riskGuard.canOpenNewTrade(activeTrades.size, pendingOrders.size)) return;
            if (pendingOrders.size >= MAX_PENDING_ORDERS) return;

            const mockConfidence = 0.88; 
            let finalLeverage = Math.floor(Math.min(calculateSigmoidLeverage(mockConfidence), calculateKellyLeverage(mockConfidence)));
            if (finalLeverage < 1) finalLeverage = 1;

            let orderType = (feature.VPIN > 0.7 && feature.OFI > 10) ? 'MARKET' : 'LIMIT_MAKER';
            const estimatedSize = (2.0 * finalLeverage) / currentAsk; 

            simulateLatency(async () => {
                const freshFeature = latestFeatures.get(symbol);
                const freshAsk = freshFeature?.best_ask || currentAsk;

                if (orderType === 'MARKET') {
                    const executionPrice = calculateRealEntryPrice(symbol, orderType, freshAsk, estimatedSize, freshFeature);
                    const success = await exchange.openTrade(symbol, orderType, executionPrice, finalLeverage, mockConfidence);
                    if (success) {
                        activeTrades.set(symbol, { highestPrice: executionPrice });
                        logThought(symbol, `⚡ [KHỚP MARKET] Khớp ở giá ${executionPrice.toFixed(4)} (Bị trượt giá).`);
                    }
                } else {
                    pendingOrders.set(symbol, { orderType, targetPrice: freshAsk, leverage: finalLeverage, timestamp: Date.now() });
                    logThought(symbol, `🛡️ [XẾP HÀNG] Rải Limit ở ${freshAsk.toFixed(4)}.`);
                }
            });
        }

        if (pendingOrders.has(symbol)) {
            const order = pendingOrders.get(symbol);
            if (currentAsk <= order.targetPrice) {
                logThought(symbol, `🎯 [KHỚP LIMIT] Quét qua ${order.targetPrice.toFixed(4)}...`);
                
                exchange.openTrade(symbol, order.orderType, order.targetPrice, order.leverage, 0.88)
                    .then(success => {
                        if (success) activeTrades.set(symbol, { highestPrice: order.targetPrice });
                    })
                    .catch(err => console.error(err))
                    .finally(() => pendingOrders.delete(symbol));
            }
        }

    } catch (error) {}
});

console.log("🚀 Lõi Giao Dịch HFT (Mô phỏng Thực tế + RiskGuard) Đã Khởi Động!");