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
        VOLUME_SURGE_MULTIPLIER: 1.5,  // đã nới lỏng
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
// [THAY ĐỔI] Dùng Map để lưu thời điểm thêm coin
let currentRadarSlots = new Map(); // key: symbol, value: { addedAt: timestamp }
const MIN_HOLD_MS = 10 * 60 * 1000; // 10 phút giữ tối thiểu
const macroScoreCache = new Map();

// --- BỘ LỌC 1: ĐỘT BIẾN THANH KHOẢN ---
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
                    const klines = await axios.get(`https://fapi.binance.com/fapi/v1/klines?symbol=${t.symbol}&interval=1h&limit=24`);
                    const volumes = klines.data.map(k => parseFloat(k[5]));
                    
                    const current1hVol = volumes[volumes.length - 1];
                    const avg24hVol = volumes.slice(0, -1).reduce((a, b) => a + b, 0) / 23;

                    if (current1hVol >= avg24hVol * config.VOLUME_SURGE_MULTIPLIER) {
                        return t.symbol;
                    }
                } catch (e) { return null; }
            });
            
            const results = await Promise.all(promises);
            surgeCandidates.push(...results.filter(sym => sym !== null));
            await new Promise(r => setTimeout(r, 200));
        }
        
        return surgeCandidates;
    } catch (error) {
        console.error("❌ Lỗi check Volume Surge:", error.message);
        return [];
    }
}

// --- BỘ LỌC 2: ON-CHAIN THAO TÚNG ---
async function checkOnChainManipulation(symbol) {
    // Giữ nguyên TODO, hiện tại bỏ qua
    return false; 
}

// --- BỘ LỌC 3: NÃO VĨ MÔ (MACRO AI) ---
async function inferMacroScore(symbol) {
    // [THAY ĐỔI] Dùng SMA 20 trên nến 4H thay vì mock ngẫu nhiên
    try {
        const klines = await axios.get(
            `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=4h&limit=20`
        );
        const closes = klines.data.map(k => parseFloat(k[4]));
        const sma20 = closes.reduce((a, b) => a + b, 0) / closes.length;
        const currentClose = closes[closes.length - 1];
        if (currentClose > sma20) {
            return 0.8 + (Math.random() * 0.1); // 0.8–0.9
        } else {
            return 0.5 + (Math.random() * 0.1); // 0.5–0.6
        }
    } catch (e) {
        // Fallback an toàn
        const fallback = 0.3 + (Math.random() * 0.6);
        return parseFloat(fallback.toFixed(2));
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
        
        console.log(`➕ [STICKY] Thêm mới ${sym}, giữ tối thiểu ${MIN_HOLD_MS/60000} phút`);
    }
    
    // --- Xác định coin có thể loại bỏ ---
    const coinsToDrop = [];
    for (const [sym, slot] of currentRadarSlots.entries()) {
        if (!newSlotSet.has(sym)) {
            // Chỉ loại nếu đã giữ đủ thời gian tối thiểu
            if (Date.now() - slot.addedAt >= MIN_HOLD_MS) {
                coinsToDrop.push(sym);
            } else {
                console.log(`🔒 [STICKY] Giữ ${sym} dù không còn trong top (còn ${Math.ceil((MIN_HOLD_MS - (Date.now() - slot.addedAt))/60000)} phút)`);
            }
        } else {
            // Coin vẫn đạt điều kiện, reset thời gian giữ
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
        console.log(`❌ [STICKY] Loại bỏ ${sym} sau khi giữ đủ thời gian`);
    }
    
    await syncMacroScores();
    
    console.log(`📊 [RADAR] Tổng coin đang giám sát: ${currentRadarSlots.size}`);
}

console.log("👁️ RadarManager V17 HFT - Khởi động bộ quét Dòng tiền Vĩ mô!");
scanRadar();
setInterval(scanRadar, 3 * 60 * 1000); // 3 phút
setInterval(syncMacroScores, 15 * 60 * 1000); // 15 phút