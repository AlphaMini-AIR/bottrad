const Redis = require('ioredis');
const { decode } = require('@msgpack/msgpack'); // 2.6: Dùng đúng thư viện đồng bộ với Python
const fs = require('fs');

// 2.5: Đọc file cấu hình hệ thống
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

// Cần 2 client Redis: 1 để Pub/Sub, 1 để truy vấn Data (Hash/Keys)
const subClient = new Redis(config.REDIS_URL);
const dataClient = new Redis(config.REDIS_URL);

// --- BỘ NHỚ RAM CỤC BỘ (L1 CACHE) ---
const activeTrades = new Map(); // 2.3: Quản lý vị thế đang mở
const macroCache = new Map();   // 2.1: Cache điểm Macro 4H (để Fast Check 0ms)

// --- ĐỒNG BỘ MACRO SCORE (Mỗi 5 giây) ---
// Tránh dùng await redis.hget() trong hot-path (pmessageBuffer) vì sẽ làm nghẽn Event Loop
setInterval(async () => {
    try {
        const scores = await dataClient.hgetall(config.CHANNELS.MACRO_SCORES);
        for (const [sym, score] of Object.entries(scores)) {
            macroCache.set(sym, parseFloat(score));
        }
    } catch (e) {
        console.error("Lỗi đồng bộ MacroCache:", e);
    }
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
subClient.psubscribe(config.CHANNELS.FEATURES); // Nghe tick vi cấu trúc
subClient.subscribe(config.CHANNELS.CANDIDATES); // 2.4: Nghe sự kiện EV Switch từ Radar

subClient.on('message', (channel, message) => {
    // 2.4: LOGIC EV SWITCH (Khi Radar báo có coin mới đẹp hơn)
    if (channel === config.CHANNELS.CANDIDATES) {
        console.log(`\n🚨 [EV SWITCH] Nhận được ứng viên mới: ${message}. Bắt đầu tính toán Chi phí Cơ hội...`);
        // TODO: Viết hàm tìm lệnh activeTrades xấu nhất -> Tính EV_switch -> Bán cắt lỗ nếu D > Threshold
    }
});

// --- VÒNG LẶP SỰ KIỆN CHÍNH (HOT-PATH HFT) ---
subClient.on('pmessageBuffer', (pattern, channel, messageBuffer) => {
    try {
        const feature = decode(messageBuffer); // Giải nén MsgPack
        if (!feature.is_warm) return;

        const symbol = feature.symbol;
        
        // =========================================================
        // NHÁNH A: QUẢN LÝ VỊ THẾ ĐANG MỞ (2.3 - DYNAMIC TRAILING STOP)
        // =========================================================
        if (activeTrades.has(symbol)) {
            const trade = activeTrades.get(symbol);
            const currentPrice = feature.best_ask || feature.ATR_proxy; // Tùy Python gửi sang giá nào làm mốc
            
            // Cập nhật giá cao nhất (Highest Price) để bám đuổi
            if (currentPrice > trade.highestPrice) trade.highestPrice = currentPrice;

            // Tính Khoảng cách Cắt lỗ Động (ATR * MFA_Multiplier)
            const mfaMultiplier = feature.MFA > 0 ? 3.0 : 0.5;
            const stopDistance = feature.ATR14 * mfaMultiplier;
            const dynamicStopPrice = trade.highestPrice * (1 - stopDistance);

            // Kiểm tra kích hoạt Stoploss/Trailing
            if (currentPrice <= dynamicStopPrice || feature.MFA < -1.5) {
                console.log(`\n🛑 [ĐÓNG LỆNH] ${symbol} | Giá: ${currentPrice} | Lý do: ${feature.MFA < -1.5 ? 'Gia tốc xả mạnh' : 'Chạm Trailing Stop'}`);
                
                // TODO: Gửi lệnh đóng xuống Binance/PaperExchange
                
                // TODO: Gửi JSON Experience Payload lên kênh experience:raw để học ban đêm
                activeTrades.delete(symbol);
            }
            return; // Đã là lệnh active thì không xét mở lệnh mới nữa
        }

        // =========================================================
        // NHÁNH B: TÌM KIẾM ĐIỂM BÓP CÒ MỚI
        // =========================================================
        
        // 2.1. FAST MACRO CHECK (Độ trễ 0ms vì đọc từ Cache cục bộ)
        const macroScore = macroCache.get(symbol) || 0;
        if (macroScore < config.MACRO_THRESHOLD) {
            // Uncomment dòng dưới nếu muốn xem log, nhưng sẽ spam màn hình
            // console.log(`[BULL TRAP AVOIDED] ${symbol} bị bỏ qua vì Macro Score = ${macroScore}`);
            return;
        }

        // Kiểm tra điều kiện Vi cấu trúc cơ bản
        if (feature.OFI > 5 && feature.MFA > 0.5) {
            
            // 2.7. TÍCH HỢP AI MICROBRAIN (Placeholder rõ ràng)
            // Lấy độ tự tin từ mô hình ONNX. Chỗ này sẽ gọi await MicroBrain.run(feature) 
            const mockConfidence = 0.88; 

            // Tính đòn bẩy
            const L_sig = calculateSigmoidLeverage(mockConfidence);
            const L_kelly = calculateKellyLeverage(mockConfidence);
            let finalLeverage = Math.floor(Math.min(L_sig, L_kelly));
            if (finalLeverage < 1) finalLeverage = 1;

            // 2.2. ORDER ROUTING: LIMIT MAKER vs LIMIT FOK
            let orderType = '';
            let targetPrice = 0; // Giả lập lấy giá
            
            if (feature.VPIN > 0.7 && feature.OFI > 10) {
                // Bão thanh khoản, cá mập gom hàng -> Phải Market/FOK tránh lỡ tàu
                orderType = 'LIMIT_FOK';
                // Đặt giá cao hơn Ask 0.5% để bảo vệ Slippage
                console.log(`⚡ [ROUTING] ${symbol}: Bão thanh khoản (VPIN: ${feature.VPIN.toFixed(2)}). Đánh LIMIT_FOK!`);
            } else {
                // Cân bằng, có thể chờ xếp hàng
                orderType = 'LIMIT_MAKER';
                console.log(`🛡️ [ROUTING] ${symbol}: Thanh khoản ổn định. Rải LIMIT_MAKER.`);
            }

            console.log(`🔥 [MỞ LỆNH LONG] ${symbol} | Đòn bẩy: ${finalLeverage}x | Lệnh: ${orderType}`);
            
            // Đưa vào Active Pool để nhánh A bắt đầu quản lý Trailing Stop
            activeTrades.set(symbol, {
                entryPrice: 100, // Thay bằng giá thực tế
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

console.log("🚀 OrderManager V17 HFT Engine đã khởi động!");