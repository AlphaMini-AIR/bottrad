/**
 * src/services/ai/AiEngine.js - V17.0 (DUAL-LOOP ARCHITECTURE)
 * Hỗ trợ song song 2 luồng: Não Cụ Thể (Tầng 1,2) & Não Tập Trung (Tầng 3 Scout)
 * Tích hợp Sparsity-aware (khiên NaN chuẩn IEEE 754) và Hot-Reload.
 */
require('dotenv').config();
const ort = require('onnxruntime-node');
const fs = require('fs');
const path = require('path');
const InMemoryBuffer = require('../data/InMemoryBuffer');
const DeepThinker = require('./DeepThinker'); // KHÔI PHỤC DEEPTHINKER

class AiEngine {
    constructor() {
        this.sessions = {}; // Lưu model riêng từng coin
        this.watchers = {};
        this.isReloading = {};
        this.missingModels = new Set(); // Cache các coin chưa có model riêng

        // Cấu hình Universal Model
        this.UNIVERSAL_MODEL_PATH = path.resolve(__dirname, '../../../Universal_Scout.onnx');
        this.loadUniversalModel();
        this.watchUniversalModel();
    }

    // =========================================================================
    // 🧠 LUỒNG 1: QUẢN LÝ NÃO TẬP TRUNG (UNIVERSAL MODEL CHO SCOUT)
    // =========================================================================
    async loadUniversalModel() {
        try {
            if (fs.existsSync(this.UNIVERSAL_MODEL_PATH)) {
                const options = { executionProviders: ['cpu'], graphOptimizationLevel: 'all' };
                const session = await ort.InferenceSession.create(this.UNIVERSAL_MODEL_PATH, options);
                this.sessions['UNIVERSAL'] = session;
                console.log(`🌍 [AiEngine] Đã nạp Universal Model cho các coin Scout (Tầng 3)`);
            } else {
                console.warn(`⚠️ [AiEngine] Chưa có Universal_Scout.onnx. Đang chờ luồng Python...`);
            }
        } catch (error) {
            console.error(`❌ [AiEngine] Lỗi nạp Universal Model:`, error.message);
        }
    }

    watchUniversalModel() {
        const modelDir = path.dirname(this.UNIVERSAL_MODEL_PATH);
        if (!fs.existsSync(modelDir)) return;

        fs.watch(modelDir, (eventType, filename) => {
            if (filename === 'Universal_Scout.onnx' && !this.isReloading['UNIVERSAL']) {
                this.isReloading['UNIVERSAL'] = true;
                setTimeout(async () => {
                    console.log(`🔄 [AiEngine] Cập nhật Universal Model mới. Đang Hot-Reload...`);
                    await this.loadUniversalModel();
                    this.isReloading['UNIVERSAL'] = false;
                }, 1000); // Đợi 1s để Python Atomic Write ghi xong
            }
        });
    }

    // =========================================================================
    // 🧠 LUỒNG 2: QUẢN LÝ NÃO CỤ THỂ (SPECIFIC MODELS CHO TẦNG 1, 2)
    // =========================================================================
    async init(symbol) {
        if (this.sessions[symbol] || this.missingModels.has(symbol)) return;

        const modelPath = path.join(__dirname, `../../../model_v16_${symbol}.onnx`);
        const success = await this.loadModelSafe(symbol, modelPath);

        if (success) {
            this.setupAtomicWatcher(symbol, modelPath);
        } else {
            this.missingModels.add(symbol); // Đánh dấu để lần sau không quét ổ cứng nữa
        }
    }

    async loadModelSafe(symbol, modelPath) {
        if (this.isReloading[symbol]) return false;
        this.isReloading[symbol] = true;

        try {
            if (fs.existsSync(modelPath)) {
                const session = await ort.InferenceSession.create(modelPath, { executionProviders: ['cpu'] });
                this.sessions[symbol] = session;
                console.log(`🤖 [AiEngine] Đã nạp Model riêng cho ${symbol}`);
                return true;
            }
        } catch (err) {
            console.error(`❌ [AiEngine] Lỗi nạp Model ${symbol}:`, err.message);
        } finally {
            this.isReloading[symbol] = false;
        }
        return false;
    }

    setupAtomicWatcher(symbol, modelPath) {
        if (this.watchers[symbol]) return;
        let debounceTimer;
        this.watchers[symbol] = fs.watch(path.dirname(modelPath), (eventType, filename) => {
            if (filename === path.basename(modelPath)) {
                clearTimeout(debounceTimer);
                debounceTimer = setTimeout(async () => {
                    console.log(`🔄 [AiEngine] Hot-Reload Model riêng cho ${symbol}...`);
                    await this.loadModelSafe(symbol, modelPath);
                }, 1000);
            }
        });
    }

    // =========================================================================
    // 🎯 ĐỘNG CƠ DỰ ĐOÁN CHÍNH (PREDICTION ENGINE)
    // =========================================================================
    async predict(historyCandles, symbol) { // LƯU Ý: Khớp tham số với main.js gọi vào
        if (!historyCandles || historyCandles.length < 2) return null;

        // XÁC ĐỊNH NÃO: Ưu tiên Não Cụ Thể, nếu không có thì dùng Não Tập Trung (Universal)
        const session = this.sessions[symbol] || this.sessions['UNIVERSAL'];
        const isUniversal = !this.sessions[symbol];

        if (!session) return null; // AI chưa sẵn sàng

        try {
            const current = historyCandles[historyCandles.length - 1];
            const prev = historyCandles[historyCandles.length - 2];

            const closedData = InMemoryBuffer.getLatestClosedCandle(symbol);
            const micro = (closedData && closedData.micro) ? closedData.micro : {};
            const macro = (closedData && closedData.macro) ? closedData.macro : {};

            // 🟢 1. Feature Engineering: Tính toán Nến
            const body_size = current.open > 0 ? (Math.abs(current.close - current.open) / current.open) * 100 : 0;
            const wick_size = current.open > 0 ? (((current.high - current.low) - Math.abs(current.close - current.open)) / current.open) * 100 : 0;
            const taker_buy_ratio = (current.volume > 0 && current.takerBuyBase) ? (current.takerBuyBase / current.volume) : NaN;

            // 🟢 2. Feature Engineering: Khôi phục logic Mỏ neo BTCUSDT
            let btc_relative_strength = NaN;
            const btcData = InMemoryBuffer.getHistoryObjects('BTCUSDT');
            if (btcData && btcData.length >= 2) {
                const btcCurrent = btcData[btcData.length - 1];
                const btcPrev = btcData[btcData.length - 2];
                const timeDiff = Math.abs((current.openTime || current.t) - (btcCurrent.openTime || btcCurrent.t));
                
                if (timeDiff < 65000 && prev.close > 0 && btcPrev.close > 0) {
                    const altcoin_pct = ((current.close - prev.close) / prev.close) * 100;
                    const btc_pct = ((btcCurrent.close - btcPrev.close) / btcPrev.close) * 100;
                    btc_relative_strength = altcoin_pct - btc_pct;
                }
            }

            // 🟢 3. Xây dựng Mảng 13 Đặc trưng (Khớp Python `incremental_trainer.py`)
            const rawFeatures = [
                micro.ob_imb_top20 ?? NaN,
                micro.spread_close ?? NaN,
                micro.bid_vol_1pct ?? NaN,
                micro.ask_vol_1pct ?? NaN,
                micro.max_buy_trade ?? NaN,
                micro.max_sell_trade ?? NaN,
                micro.liq_long_vol ?? NaN,
                micro.liq_short_vol ?? NaN,
                macro.funding_rate ?? NaN,
                taker_buy_ratio,
                body_size,
                wick_size,
                btc_relative_strength
            ];

            // 🟢 4. Khiên NaN (Sparsity-aware Trigger)
            const sanitizedFeatures = rawFeatures.map(val => {
                if (val === undefined || val === null || Number.isNaN(val)) return Number.NaN;
                return parseFloat(val);
            });

            // 🟢 5. Chạy ONNX Model
            const tensor = new ort.Tensor('float32', Float32Array.from(sanitizedFeatures), [1, 13]);
            let feeds = {};
            feeds[session.inputNames[0]] = tensor;
            
            const results = await session.run(feeds);
            
            // Xử lý output (Đảm bảo tương thích cả model cũ và mới)
            const outputName = session.outputNames.includes('probabilities') ? 'probabilities' : session.outputNames[1] || session.outputNames[0];
            const probsObj = results[outputName]?.data || results.probabilities?.data || results[session.outputNames[0]].data;
            
            let winProb = 0.5; // Mặc định nhiễu
            if (probsObj && probsObj.length >= 3) {
                const prob_short = probsObj[0];
                const prob_long = probsObj[1];
                const prob_noise = probsObj[2];

                if (prob_long > prob_noise && prob_long > prob_short) {
                    winProb = prob_long; 
                } else if (prob_short > prob_noise && prob_short > prob_long) {
                    winProb = 1 - prob_short; // Ép về < 0.5 để kích hoạt Short
                }
            }

            // 🟢 6. KHÔI PHỤC: Gọi bộ não DeepThinker để quản lý rủi ro
            const thinkerResult = DeepThinker.evaluate(symbol, current.close, winProb, micro);

            // KHÔI PHỤC Return chuẩn để main.js không bị crash
            return {
                winProb: winProb,
                thinker: thinkerResult,
                features: sanitizedFeatures,
                isUniversal: isUniversal // Gửi kèm cờ hiệu để PaperExchange biết đang xài model nào
            };

        } catch (err) {
            console.error(`❌ [AiEngine] Lỗi suy luận ${symbol}:`, err.message);
            return null;
        }
    }
}

module.exports = new AiEngine();