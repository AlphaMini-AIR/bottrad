const axios = require('axios');
const Redis = require('ioredis');
const fs = require('fs');

// --- 1. ĐỌC CẤU HÌNH ---
let config;
try {
    config = JSON.parse(fs.readFileSync('./system_config.json', 'utf8'));
} catch (e) {
    config = {
        REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379',
        MAX_RADAR_SLOTS: 30,
        MACRO_THRESHOLD: 0.6,
        VOLUME_SURGE_MULTIPLIER: 3.0, // Thanh khoản phải gấp 3 lần bình thường
        CHANNELS: {
            CANDIDATES: 'radar:candidates',
            MACRO_SCORES: 'macro:scores',
            SUBSCRIPTIONS: 'system:subscriptions'
        }
    };
}

const pubClient = new Redis(config.REDIS_URL);
const dataClient = new Redis(config.REDIS_URL);

// --- BỘ NHỚ LƯU TRỮ TRẠNG THÁI (L1 CACHE) ---
let currentRadarSlots = new Set(); 
const macroScoreCache = new Map();

// --- BỘ LỌC 1: ĐỘT BIẾN THANH KHOẢN (ĐÃ FIX THEO YÊU CẦU) ---
async function fetchVolumeMovers() {
    try {
        // 1. Lấy tất cả ticker 24h để lọc base liquidity (tránh coin chết)
        const response = await axios.get('https://fapi.binance.com/fapi/v1/ticker/24hr');
        let tickers = response.data
            .filter(t => t.symbol.endsWith('USDT') && !['BTCUSDT', 'ETHUSDT'].includes(t.symbol))
            .filter(t => parseFloat(t.quoteVolume) > 5000000) // Khối lượng 24h > 5M USDT
            .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume))
            .slice(0, 100); // Lấy top 100 coin thanh khoản tốt nhất để check đột biến

        const surgeCandidates = [];
        const BATCH_SIZE = 10; // Tránh Rate Limit của Binance
        
        // 2. Thuật toán tìm Đột biến (Current 1h Volume vs Average 1h Volume of 24h)
        for (let i = 0; i < tickers.length; i += BATCH_SIZE) {
            const batch = tickers.slice(i, i + BATCH_SIZE);
            const promises = batch.map(async (t) => {
                try {
                    // Lấy 24 nến 1H gần nhất
                    const klines = await axios.get(`https://fapi.binance.com/fapi/v1/klines?symbol=${t.symbol}&interval=1h&limit=24`);
                    const volumes = klines.data.map(k => parseFloat(k[5])); // K[5] là Taker base volume hoặc Quote Volume
                    
                    const current1hVol = volumes[volumes.length - 1];
                    const avg24hVol = volumes.slice(0, -1).reduce((a, b) => a + b, 0) / 23;

                    if (current1hVol >= avg24hVol * config.VOLUME_SURGE_MULTIPLIER) {
                        return t.symbol;
                    }
                } catch (e) { return null; }
            });
            
            const results = await Promise.all(promises);
            surgeCandidates.push(...results.filter(sym => sym !== null));
            await new Promise(r => setTimeout(r, 200)); // Nghỉ 200ms giữa các batch
        }
        
        return surgeCandidates;
    } catch (error) {
        console.error("❌ Lỗi check Volume Surge:", error.message);
        return [];
    }
}

// --- BỘ LỌC 2: ON-CHAIN THAO TÚNG ---
async function checkOnChainManipulation(symbol) {
    /*
     * TODO [PRODUCTION CRITICAL]: 
     * 1. Gọi API TokenUnlocks (https://api.tokenunlocks.com/...) để check lịch trả coin. Nếu có đợt trả > 5% cung lưu hành trong 24h tới -> LOẠI (Return true).
     * 2. Gọi API CoinMarketCap hoặc Glassnode lấy Top 10 Holders. Nếu > 60% tổng cung -> LOẠI.
     * 3. Gọi Binance API (Top Trader Long/Short Ratio). Nếu Ratio > 3.0 (Cá mập Long quá mức) hoặc < 0.3 -> Cực kỳ thao túng.
     */
    // Hiện tại giả lập (Bỏ qua thao túng để test luồng)
    return false; 
}

// --- BỘ LỌC 3: NÃO VĨ MÔ (MACRO AI) ---
async function inferMacroScore(symbol) {
    /*
     * TODO [PRODUCTION CRITICAL]:
     * 1. Dùng thư viện `onnxruntime-node`.
     * 2. Tải 120 nến Klines gần nhất (interval: 1m hoặc 5m tùy theo model 4H/8H quy đổi).
     * 3. Tiền xử lý thành mảng Float32Array (OHLCV).
     * 4. Chạy: const output = await macroSession.run({ input: tensor });
     * 5. Return giá trị sigmoid xu hướng từ output.
     */
    const mockScore = 0.3 + (Math.random() * 0.6); 
    return parseFloat(mockScore.toFixed(2));
}

// --- VÒNG LẶP SỰ KIỆN CHÍNH ---

async function syncMacroScores() {
    if (currentRadarSlots.size === 0) return;
    const pipeline = dataClient.pipeline();
    
    for (const symbol of currentRadarSlots) {
        const score = await inferMacroScore(symbol);
        macroScoreCache.set(symbol, score);
        pipeline.hset(config.CHANNELS.MACRO_SCORES, symbol, score);
    }
    
    await pipeline.exec();
}

async function scanRadar() {
    console.log(`\n📡 [RADAR] Đang quét thị trường tìm Volume Surge (x${config.VOLUME_SURGE_MULTIPLIER})...`);
    
    const volumeMovers = await fetchVolumeMovers();
    let candidates = [];

    for (const symbol of volumeMovers) {
        const isManipulated = await checkOnChainManipulation(symbol);
        if (isManipulated) continue;

        const score = await inferMacroScore(symbol);
        if (score >= config.MACRO_THRESHOLD) {
            candidates.push({ symbol, score });
        }
    }

    candidates.sort((a, b) => b.score - a.score);
    const topCandidates = candidates.slice(0, config.MAX_RADAR_SLOTS);

    const newSlotSet = new Set(topCandidates.map(c => c.symbol));
    const coinsToDrop = [...currentRadarSlots].filter(sym => !newSlotSet.has(sym));
    const coinsToAdd = [...newSlotSet].filter(sym => !currentRadarSlots.has(sym));

    // ==========================================
    // ĐÃ FIX: LỖI RÒ RỈ WEBSOCKET (REF COUNT)
    // ==========================================
    for (const sym of coinsToDrop) {
        // 1. Xóa điểm Macro để OrderManager không dùng nữa
        dataClient.hdel(config.CHANNELS.MACRO_SCORES, sym);
        
        // 2. Gửi lệnh UNSUBSCRIBE CÓ GẮN CLIENT ID. 
        // FeedHandler (Python) sẽ trừ ref count của "radar". Nếu OMS cũng không giữ lệnh này, ref count = 0 -> Đóng Stream vật lý.
        pubClient.publish(config.CHANNELS.SUBSCRIPTIONS, JSON.stringify({
            action: "UNSUBSCRIBE",
            symbol: sym,
            client: "radar" // Định danh rõ ràng
        }));
    }

    if (coinsToAdd.length > 0) {
        console.log(`✨ [RADAR] Thêm ${coinsToAdd.length} coin mới vào danh sách giám sát!`);
        
        for (const sym of coinsToAdd) {
            const candidateData = topCandidates.find(c => c.symbol === sym);
            
            // Báo FeedHandler MỞ luồng với Client ID
            pubClient.publish(config.CHANNELS.SUBSCRIPTIONS, JSON.stringify({
                action: "SUBSCRIBE",
                symbol: sym,
                client: "radar"
            }));

            // Báo OrderManager tính Toán EV_Switch
            pubClient.publish(config.CHANNELS.CANDIDATES, JSON.stringify(candidateData));
        }
    }

    currentRadarSlots = newSlotSet;
    await syncMacroScores();
}

console.log("👁️ RadarManager V17 HFT - Khởi động bộ quét Dòng tiền Vĩ mô!");
scanRadar();
setInterval(scanRadar, 3 * 60 * 1000); // 3 phút
setInterval(syncMacroScores, 15 * 60 * 1000); // 15 phút