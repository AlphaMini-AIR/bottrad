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
        MACRO_THRESHOLD: 0.5, // Hạ nhẹ threshold vì công thức điểm mới khắt khe hơn
        VOLUME_SURGE_MULTIPLIER: 1.5, // Mức x1.5 phù hợp cho lưới rộng 2 chiều
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
let currentRadarSlots = new Map(); // key: symbol, value: { addedAt: timestamp }
const MIN_HOLD_MS = 10 * 60 * 1000; // 10 phút giữ tối thiểu
const macroScoreCache = new Map();

// --- BỘ LỌC 1: ĐỘT BIẾN THANH KHOẢN & BIẾN ĐỘNG (LƯỚI 2 CHIỀU) ---
async function fetchVolumeMovers() {
    try {
        const response = await axios.get('https://fapi.binance.com/fapi/v1/ticker/24hr');
        let tickers = response.data
            .filter(t => t.symbol.endsWith('USDT') && !['BTCUSDT', 'ETHUSDT'].includes(t.symbol))
            .filter(t => parseFloat(t.quoteVolume) > 5000000)
            .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume))
            .slice(0, 100);

        const surgeCandidates = [];
        const BATCH_SIZE = 10;
        
        for (let i = 0; i < tickers.length; i += BATCH_SIZE) {
            const batch = tickers.slice(i, i + BATCH_SIZE);
            const promises = batch.map(async (t) => {
                try {
                    // Dùng nến 15m, quét 24 cây = 6 giờ qua (bắt sóng Day Trading)
                    const klines = await axios.get(`https://fapi.binance.com/fapi/v1/klines?symbol=${t.symbol}&interval=15m&limit=24`);
                    
                    const currentCandle = klines.data[klines.data.length - 1];
                    const currentVol = parseFloat(currentCandle[5]);
                    
                    // Tính Volume trung bình 23 cây nến trước
                    const prevVolumes = klines.data.slice(0, 23).map(k => parseFloat(k[5]));
                    const avgVol = prevVolumes.reduce((a, b) => a + b, 0) / 23;

                    // TÍNH TOÁN BIẾN ĐỘNG 2 CHIỀU (Tuyệt đối)
                    const openPrice = parseFloat(currentCandle[1]);
                    const highPrice = parseFloat(currentCandle[2]);
                    const lowPrice = parseFloat(currentCandle[3]);
                    const closePrice = parseFloat(currentCandle[4]);

                    // 1. Độ lệch giá đóng/mở (Body Change)
                    const bodyChangePct = Math.abs((closePrice - openPrice) / openPrice) * 100;

                    // 2. Tổng biên độ dao động của nến (Amplitude/Volatility)
                    const amplitudePct = Math.abs((highPrice - lowPrice) / lowPrice) * 100;

                    // ĐIỀU KIỆN CHỌN: Volume x1.5 VÀ (Thân nến >= 0.3% HOẶC Râu quét >= 0.6%)
                    if (currentVol >= avgVol * config.VOLUME_SURGE_MULTIPLIER && (bodyChangePct >= 0.3 || amplitudePct >= 0.6)) {
                        return t.symbol;
                    }
                } catch (e) { return null; }
            });
            
            const results = await Promise.all(promises);
            surgeCandidates.push(...results.filter(sym => sym !== null));
            await new Promise(r => setTimeout(r, 200)); // Rate limit protection
        }
        
        return surgeCandidates;
    } catch (error) {
        console.error("❌ Lỗi check Volume & Volatility:", error.message);
        return [];
    }
}

// --- BỘ LỌC 2: ON-CHAIN THAO TÚNG ---
async function checkOnChainManipulation(symbol) {
    // Tạm thời bỏ qua (trả về false) để tập trung đánh theo dòng tiền kỹ thuật
    return false; 
}

// --- BỘ LỌC 3: NÃO VĨ MÔ (ĐÃ LOẠI BỎ MATH.RANDOM) ---
async function inferMacroScore(symbol) {
    try {
        // Dùng nến 4H để tính trend lớn
        const klines = await axios.get(
            `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=4h&limit=20`
        );
        const closes = klines.data.map(k => parseFloat(k[4]));
        const sma20 = closes.reduce((a, b) => a + b, 0) / closes.length;
        const currentClose = closes[closes.length - 1];
        
        // Tính khoảng cách % từ giá hiện tại đến SMA20 (Đo lường độ nén/xung lực của Trend)
        const diffPct = Math.abs(currentClose - sma20) / sma20 * 100; 

        // Tính điểm: Giá càng xa SMA20 (Trend càng mạnh) -> Điểm càng cao
        // diffPct 0% -> Điểm 0.5. diffPct 5% -> Điểm 0.95
        let baseScore = 0.5 + (diffPct * 0.09); 
        
        // Giới hạn điểm trần là 0.98 để không bị lỗi tuyệt đối
        return parseFloat(Math.min(baseScore, 0.98).toFixed(2));
        
    } catch (e) {
        // Fallback an toàn, ổn định nếu lỗi API
        return 0.5;
    }
}

// --- VÒNG LẶP SỰ KIỆN CHÍNH ---

async function syncMacroScores() {
    if (currentRadarSlots.size === 0) return;
    const pipeline = dataClient.pipeline();
    
    for (const [symbol, slot] of currentRadarSlots.entries()) {
        const score = await inferMacroScore(symbol);
        macroScoreCache.set(symbol, score);
        pipeline.hset(config.CHANNELS.MACRO_SCORES, symbol, score);
    }
    
    await pipeline.exec();
}

async function scanRadar() {
    console.log(`\n📡 [RADAR] Đang quét lưới 2 chiều (Vol x${config.VOLUME_SURGE_MULTIPLIER} & Biến động >0.3%)...`);
    
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

    // Ưu tiên các coin có xung lực mạnh nhất đưa lên top
    candidates.sort((a, b) => b.score - a.score);
    const topCandidates = candidates.slice(0, config.MAX_RADAR_SLOTS);

    const newSlotSet = new Set(topCandidates.map(c => c.symbol));
    
    // --- Thêm coin mới ---
    const coinsToAdd = [...newSlotSet].filter(sym => !currentRadarSlots.has(sym));
    for (const sym of coinsToAdd) {
        currentRadarSlots.set(sym, { addedAt: Date.now() });
        
        pubClient.publish(config.CHANNELS.SUBSCRIPTIONS, JSON.stringify({
            action: "SUBSCRIBE",
            symbol: sym,
            client: "radar"
        }));
        
        const candidateData = topCandidates.find(c => c.symbol === sym);
        pubClient.publish(config.CHANNELS.CANDIDATES, JSON.stringify(candidateData));
        
        console.log(`🎯 [BẮT MỒI] Thêm ${sym} vào Radar, giữ tối thiểu ${MIN_HOLD_MS/60000} phút`);
    }
    
    // --- Xác định coin có thể loại bỏ ---
    const coinsToDrop = [];
    for (const [sym, slot] of currentRadarSlots.entries()) {
        if (!newSlotSet.has(sym)) {
            // Chỉ loại nếu đã giữ đủ thời gian tối thiểu
            if (Date.now() - slot.addedAt >= MIN_HOLD_MS) {
                coinsToDrop.push(sym);
            } else {
                console.log(`🔒 [GIỮ CHÂN] Đang theo dõi ${sym} (còn ${Math.ceil((MIN_HOLD_MS - (Date.now() - slot.addedAt))/60000)} phút)`);
            }
        } else {
            // Coin vẫn tiếp tục có biến động, reset thời gian giữ
            slot.addedAt = Date.now();
        }
    }
    
    // --- Thực sự loại bỏ những coin đã hết hạn giữ ---
    for (const sym of coinsToDrop) {
        dataClient.hdel(config.CHANNELS.MACRO_SCORES, sym);
        pubClient.publish(config.CHANNELS.SUBSCRIPTIONS, JSON.stringify({
            action: "UNSUBSCRIBE",
            symbol: sym,
            client: "radar"
        }));
        currentRadarSlots.delete(sym);
        console.log(`❌ [MẤT SÓNG] Loại bỏ ${sym} vì hết thanh khoản/biến động`);
    }
    
    await syncMacroScores();
    
    console.log(`📊 [RADAR] Tổng số coin đang trong tầm ngắm: ${currentRadarSlots.size}`);
}

console.log("👁️ RadarManager V17 HFT - Khởi động mạng lưới Quét 2 Chiều!");
scanRadar();

// Quét Radar mỗi 3 phút (Bắt nhịp nến 15m thực tế)
setInterval(scanRadar, 3 * 60 * 1000); 

// Đồng bộ Vĩ mô mỗi 15 phút
setInterval(syncMacroScores, 15 * 60 * 1000);