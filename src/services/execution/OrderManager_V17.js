const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack');
const fs = require('fs');

// 1. Đọc cấu hình hệ thống
let config;
try {
    config = JSON.parse(fs.readFileSync('./system_config.json', 'utf8'));
} catch (e) {
    console.warn("⚠️ Không tìm thấy system_config.json, dùng cấu hình mặc định.");
    config = {
        REDIS_URL: 'redis://localhost:6379',
        MAX_LEVERAGE: 20,
        KELLY_FRACTION: 0.2,
        RISK_REWARD_RATIO: 2.0,
        MACRO_THRESHOLD: 0.6,
        CHANNELS: {
            FEATURES: 'market:features:*',
            CANDIDATES: 'radar:candidates',
            MACRO_SCORES: 'macro:scores'
        }
    };
}

// 2. KHỞI TẠO TẤT CẢ REDIS CLIENT Ở ĐẦU FILE
const subClient = new Redis(config.REDIS_URL);
const dataClient = new Redis(config.REDIS_URL);
const pubClient = new Redis(config.REDIS_URL); // Đã dọn lên đây

// --- HÀM ĐẨY SUY NGHĨ LÊN DASHBOARD ---
function logThought(message) {
    console.log(message); // Vẫn in ra terminal PM2
    // Đẩy lên kênh dashboard để Web Frontend hứng
    pubClient.publish('dashboard:logs', JSON.stringify({ msg: message }));
}

// --- BỘ NHỚ RAM CỤC BỘ (L1 CACHE) ---
const activeTrades = new Map();
const macroCache = new Map();

// --- ĐỒNG BỘ MACRO SCORE ---
setInterval(async () => {
    try {
        const scores = await dataClient.hgetall(config.CHANNELS.MACRO_SCORES);
        for (const [sym, score] of Object.entries(scores)) {
            macroCache.set(sym, parseFloat(score));
        }
    } catch (e) {}
}, 5000);

// --- TOÁN HỌC ĐỊNH CỠ ĐÒN BẨY ---
function calculateSigmoidLeverage(confidenceScore) {
    const p0 = 0.75, k = 10, L_min = 1;
    return L_min + (config.MAX_LEVERAGE - L_min) / (1 + Math.exp(-k * (confidenceScore - p0)));
}

function calculateKellyLeverage(confidenceScore) {
    const kelly = (confidenceScore * config.RISK_REWARD_RATIO - (1 - confidenceScore)) / config.RISK_REWARD_RATIO;
    return config.KELLY_FRACTION * kelly * config.MAX_LEVERAGE;
}

// --- ĐĂNG KÝ LẮNG NGHE ĐA KÊNH ---
subClient.psubscribe(config.CHANNELS.FEATURES);
subClient.subscribe(config.CHANNELS.CANDIDATES);

subClient.on('message', (channel, message) => {
    if (channel === config.CHANNELS.CANDIDATES) {
        // Áp dụng logThought
        logThought(`🚨 [EV SWITCH] Radar báo có ứng viên mới: ${message}. Đang tính toán Chi phí Cơ hội...`);
    }
});

// --- VÒNG LẶP SỰ KIỆN CHÍNH (HOT-PATH HFT) ---
subClient.on('pmessageBuffer', (pattern, channel, messageBuffer) => {
    try {
        const feature = decode(messageBuffer);
        if (!feature.is_warm) return;

        const symbol = feature.symbol;
        
        // NHÁNH A: QUẢN LÝ VỊ THẾ ĐANG MỞ
        if (activeTrades.has(symbol)) {
            const trade = activeTrades.get(symbol);
            const currentPrice = feature.best_ask || feature.ATR_proxy;
            
            if (currentPrice > trade.highestPrice) trade.highestPrice = currentPrice;

            const mfaMultiplier = feature.MFA > 0 ? 3.0 : 0.5;
            const stopDistance = feature.ATR14 * mfaMultiplier;
            const dynamicStopPrice = trade.highestPrice * (1 - stopDistance);

            if (currentPrice <= dynamicStopPrice || feature.MFA < -1.5) {
                // Áp dụng logThought
                logThought(`🛑 [ĐÓNG LỆNH] ${symbol} | Giá: ${currentPrice} | Lý do: ${feature.MFA < -1.5 ? 'Gia tốc xả mạnh' : 'Chạm Trailing Stop'}`);
                
                activeTrades.delete(symbol);
            }
            return;
        }

        // NHÁNH B: TÌM KIẾM ĐIỂM BÓP CÒ MỚI
        const macroScore = macroCache.get(symbol) || 0;
        if (macroScore < config.MACRO_THRESHOLD) return;

        if (feature.OFI > 5 && feature.MFA > 0.5) {
            
            const mockConfidence = 0.88; 
            const L_sig = calculateSigmoidLeverage(mockConfidence);
            const L_kelly = calculateKellyLeverage(mockConfidence);
            let finalLeverage = Math.floor(Math.min(L_sig, L_kelly));
            if (finalLeverage < 1) finalLeverage = 1;

            let orderType = '';
            
            if (feature.VPIN > 0.7 && feature.OFI > 10) {
                orderType = 'LIMIT_FOK';
                // Áp dụng logThought
                logThought(`⚡ [ROUTING] ${symbol}: Bão thanh khoản (VPIN: ${feature.VPIN.toFixed(2)}). Đánh LIMIT_FOK!`);
            } else {
                orderType = 'LIMIT_MAKER';
                // Áp dụng logThought
                logThought(`🛡️ [ROUTING] ${symbol}: Thanh khoản ổn định. Rải LIMIT_MAKER.`);
            }

            // Áp dụng logThought
            logThought(`🔥 [MỞ LỆNH LONG] ${symbol} | Áp lực mua OFI: ${feature.OFI.toFixed(2)} | Đòn bẩy: ${finalLeverage}x`);
            
            activeTrades.set(symbol, {
                entryPrice: 100, 
                highestPrice: 100,
                leverage: finalLeverage,
                orderType: orderType,
                ts: Date.now()
            });
        }

    } catch (error) {
        console.error("❌ Lỗi giải nén hoặc xử lý HFT:", error);
    }
});

// Chốt hạ khởi động
logThought("🚀 OrderManager V17 HFT Engine đã khởi động và kết nối Watchtower!");