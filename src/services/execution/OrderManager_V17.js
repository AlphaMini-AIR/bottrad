const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const fs = require('fs');
const path = require('path');

const exchange = require('./PaperExchange'); 
const riskGuard = require('./RiskGuard');

// ==========================================
// 1. KHỞI TẠO VÀ ĐỌC CẤU HÌNH
// ==========================================
let config;
try {
    const configPath = path.join(__dirname, '../../../system_config.json');
    config = JSON.parse(fs.readFileSync(configPath, 'utf8'));
} catch (e) {
    console.error("❌ [CRITICAL] Không tìm thấy system_config.json! Bot dừng hoạt động.");
    process.exit(1);
}

const subClient = new Redis(config.REDIS_URL);
const dataClient = new Redis(config.REDIS_URL);
const pubClient = new Redis(config.REDIS_URL); 

function logThought(symbol, message) {
    console.log(`[${symbol}] ${message}`);
    pubClient.publish('dashboard:logs', JSON.stringify({ symbol: symbol, msg: message, ts: Date.now() }));
}

// ==========================================
// 2. BỘ NHỚ L1 (RAM CACHE) VÀ THAM SỐ
// ==========================================
const activeTrades = new Map();
const macroCache = new Map();
const pendingOrders = new Map();
const latestFeatures = new Map(); 
const MAX_PENDING_ORDERS = 3;

// Đồng bộ điểm Vĩ mô (Macro Score) mỗi 5 giây
setInterval(async () => {
    try {
        const scores = await dataClient.hgetall(config.CHANNELS.MACRO_SCORES);
        for (const [sym, score] of Object.entries(scores)) macroCache.set(sym, parseFloat(score));
    } catch (e) {
        console.error("⚠️ [LỖI ĐỒNG BỘ MACRO]:", e.message);
    }
}, 5000);

// Dọn dẹp lệnh Limit quá hạn (Mỗi giây)
setInterval(() => {
    const now = Date.now();
    for (const [symbol, order] of pendingOrders.entries()) {
        if (now - order.timestamp > 10000) { // Quá 10 giây
            logThought(symbol, `⏳ [HỦY LỆNH] Lệnh Limit treo quá 10s không khớp. Đã hủy để giải phóng vốn.`);
            pendingOrders.delete(symbol);
        }
    }
}, 1000);

// ==========================================
// 3. HÀM TIỆN ÍCH (MÔ PHỎNG & TOÁN HỌC)
// ==========================================
function simulateLatency(callback) {
    const latency = Math.floor(Math.random() * 50) + 50; // Trễ mạng 50-100ms thật
    setTimeout(callback, latency);
}

function calculateRealEntryPrice(symbol, orderType, bestAsk, size, feature) {
    if (orderType === 'LIMIT_MAKER') return bestAsk; 
    const notionalValue = size * bestAsk;
    const impactPenalty = (notionalValue / 1000) * 0.0001; 
    const volatilityPenalty = feature.MFA > 1.0 ? (feature.ATR14 || 0.001) * 0.1 : 0; 
    return bestAsk + (bestAsk * (impactPenalty + volatilityPenalty));
}

async function panicSellAll() {
    logThought('SYSTEM', '🚨 [KILLSWITCH] THỰC THI PANIC SELL TOÀN BỘ THỊ TRƯỜNG!');
    pendingOrders.clear(); 

    for (const [symbol, trade] of activeTrades.entries()) {
        const freshFeature = latestFeatures.get(symbol);
        const currentAsk = freshFeature?.best_ask || trade.highestPrice;
        await exchange.closeTrade(symbol, currentAsk, 'PANIC_SELL_KILLSWITCH');
    }
    
    activeTrades.clear();
    logThought('SYSTEM', '💀 ĐÃ ĐÓNG TẤT CẢ VỊ THẾ. BOT RÚT PHÍCH CẮM.');
}

function calculateSigmoidLeverage(confidenceScore) {
    const p0 = 0.75, k = 10, L_min = 1;
    return L_min + (config.MAX_LEVERAGE - L_min) / (1 + Math.exp(-k * (confidenceScore - p0)));
}
function calculateKellyLeverage(confidenceScore) {
    const kelly = (confidenceScore * config.RISK_REWARD_RATIO - (1 - confidenceScore)) / config.RISK_REWARD_RATIO;
    return config.KELLY_FRACTION * kelly * config.MAX_LEVERAGE;
}

// ==========================================
// 4. LẮNG NGHE SỰ KIỆN TỪ RADAR VÀ PYTHON
// ==========================================
subClient.psubscribe(config.CHANNELS.FEATURES);
subClient.subscribe(config.CHANNELS.CANDIDATES);

subClient.on('message', (channel, message) => {
    if (channel === config.CHANNELS.CANDIDATES) {
        try {
            const data = JSON.parse(message);
            logThought(data.symbol, `📡 [RADAR] Đưa vào tầm ngắm. Chờ Python bơm dữ liệu Vi cấu trúc...`);
        } catch (e) {
            console.error("❌ [LỖI JSON RADAR]:", e.message);
        }
    }
});

subClient.on('pmessageBuffer', (pattern, channel, messageBuffer) => {
    let feature;
    try {
        // [CỨU CÁNH 1]: Cố gắng giải nén MsgPack trước
        feature = decode(messageBuffer);
    } catch (msgpackError) {
        try {
            // [CỨU CÁNH 2]: Nếu Python gửi JSON, tự động chuyển đổi
            feature = JSON.parse(messageBuffer.toString());
        } catch (jsonError) {
            console.error("❌ [LỖI GIẢI MÃ DỮ LIỆU TỪ PYTHON] Data không hợp lệ:", messageBuffer.toString().substring(0, 50));
            return;
        }
    }

    if (!feature || !feature.symbol) return;
    
    // Nếu Python báo chưa đủ 100 nến (chưa warm-up), bỏ qua
    if (!feature.is_warm) return; 

    const symbol = feature.symbol;
    const currentAsk = feature.best_ask || feature.last_price || 0;
    if (currentAsk === 0) return; // Bảo vệ chia cho 0

    latestFeatures.set(symbol, feature);

    // ==========================================
    // TẦNG 1: KIỂM SOÁT RỦI RO & THANH LÝ
    // ==========================================
    if (symbol === 'BTCUSDT' && feature.ATR14) {
        riskGuard.checkMacroCircuitBreaker(feature.ATR14 * 100); 
    }

    if (riskGuard.isHalted()) return; 

    const isWalletSafe = riskGuard.checkDrawdown(exchange.getWalletBalance());
    if (!isWalletSafe) {
        panicSellAll(); 
        return; 
    }

    // Nạp giá mới vào Sàn Ảo để kiểm tra Thanh lý (Liquidation) và tính MAE/MFE
    exchange.updateTick(symbol, currentAsk);

    // Đồng bộ nếu sàn ảo tự thanh lý lệnh
    if (activeTrades.has(symbol) && !exchange.activePositions.has(symbol)) {
        activeTrades.delete(symbol);
    }

    // ==========================================
    // TẦNG 2: QUẢN LÝ VỊ THẾ ĐANG CHẠY (TRAILING STOP)
    // ==========================================
    if (activeTrades.has(symbol)) {
        const trade = activeTrades.get(symbol);
        if (currentAsk > trade.highestPrice) trade.highestPrice = currentAsk;

        const stopDistance = (feature.ATR14 || 0.001) * (feature.MFA > 0 ? 3.0 : 0.5);
        const dynamicStopPrice = trade.highestPrice * (1 - stopDistance);

        if (currentAsk <= dynamicStopPrice || feature.MFA < -1.5) {
            const reason = feature.MFA < -1.5 ? 'Gia tốc xả mạnh (MFA < -1.5)' : 'Chạm Trailing Stop Động';
            logThought(symbol, `🛑 [CẮT LỆNH] ${reason} tại giá ${currentAsk.toFixed(4)}`);
            
            exchange.closeTrade(symbol, currentAsk, reason).then(() => {
                activeTrades.delete(symbol);
            });
        }
        return; // Đã xử lý lệnh cũ, không xét mở lệnh mới
    }

    // ==========================================
    // TẦNG 3: BÓP CÒ MỞ LỆNH MỚI
    // ==========================================
    const macroScore = macroCache.get(symbol) || 0;
    if (macroScore < config.MACRO_THRESHOLD) return;

    // ĐIỀU KIỆN KÍCH HOẠT: Áp lực mua (OFI) > 5 và Gia tốc (MFA) > 0.5
    if (feature.OFI > 5 && feature.MFA > 0.5) {
        if (activeTrades.has(symbol) || pendingOrders.has(symbol)) return;
        
        if (!riskGuard.canOpenNewTrade(activeTrades.size, pendingOrders.size)) return;
        if (pendingOrders.size >= MAX_PENDING_ORDERS) return;

        // Tính Độ tự tin AI linh động (Giả lập thay cho MicroBrain hiện tại)
        const dynamicConfidence = Math.min(0.95, 0.60 + (feature.OFI / 100) + (feature.MFA / 10)); 
        
        let finalLeverage = Math.floor(Math.min(calculateSigmoidLeverage(dynamicConfidence), calculateKellyLeverage(dynamicConfidence)));
        if (finalLeverage < 1) finalLeverage = 1;

        let orderType = (feature.VPIN > 0.7 && feature.OFI > 10) ? 'MARKET' : 'LIMIT_MAKER';
        const estimatedSize = (exchange.FIXED_MARGIN * finalLeverage) / currentAsk; 

        simulateLatency(async () => {
            const freshFeature = latestFeatures.get(symbol);
            const freshAsk = freshFeature?.best_ask || currentAsk;

            if (orderType === 'MARKET') {
                const executionPrice = calculateRealEntryPrice(symbol, orderType, freshAsk, estimatedSize, freshFeature);
                const success = await exchange.openTrade(symbol, orderType, executionPrice, finalLeverage, dynamicConfidence);
                if (success) {
                    activeTrades.set(symbol, { highestPrice: executionPrice });
                    logThought(symbol, `⚡ [KHỚP MARKET] Giá bóp cò: ${currentAsk.toFixed(4)} -> Trượt giá khớp: ${executionPrice.toFixed(4)}`);
                } else {
                    logThought(symbol, `❌ [TỪ CHỐI] Lệnh MARKET thất bại (Sàn từ chối do Min Notional/Leverage).`);
                }
            } else {
                pendingOrders.set(symbol, { orderType, targetPrice: freshAsk, leverage: finalLeverage, timestamp: Date.now() });
                logThought(symbol, `🛡️ [XẾP HÀNG LIMIT] Đặt mua ở mốc ${freshAsk.toFixed(4)} (OFI: ${feature.OFI.toFixed(1)}).`);
            }
        });
    }

    // ==========================================
    // TẦNG 4: HỆ THỐNG KHỚP LỆNH LIMIT
    // ==========================================
    if (pendingOrders.has(symbol)) {
        const order = pendingOrders.get(symbol);
        
        // Nếu giá quét xuống thấp hơn hoặc bằng giá ta đặt mua Limit
        if (currentAsk <= order.targetPrice) {
            logThought(symbol, `🎯 [KHỚP LIMIT] Giá thị trường chạm mốc chờ ${order.targetPrice.toFixed(4)}.`);
            
            exchange.openTrade(symbol, order.orderType, order.targetPrice, order.leverage, 0.88)
                .then(success => {
                    if (success) activeTrades.set(symbol, { highestPrice: order.targetPrice });
                    else logThought(symbol, `❌ [TỪ CHỐI] Sàn từ chối lệnh Limit lúc khớp (Lỗi Margin).`);
                })
                .catch(err => console.error(err))
                .finally(() => pendingOrders.delete(symbol));
        }
    }
});

console.log("🚀 Lõi Giao Dịch HFT [Phiên bản Hoàn hảo 100%] Đã Khởi Động!");