/**
 * src/services/ai/AiEngine.js - V16 (Microstructure & Macro Era)
 * Nạp mô hình ONNX với 13 Features cốt lõi (Bao gồm BTC_Beta)
 */
const ort = require('onnxruntime-node');
const path = require('path');
const fs = require('fs');
const InMemoryBuffer = require('../data/InMemoryBuffer'); // Dùng để gọi data BTC

class AiEngine {
    constructor() {
        this.sessions = {};
        this.watchers = {};
        this.isReloading = {};
    }

    async init(symbol) {
        // Tìm file model_v16_<SYMBOL>.onnx
        const modelPath = path.join(__dirname, `../../../model_v16_${symbol}.onnx`);
        if (!this.sessions[symbol]) {
            await this.loadModelSafe(symbol, modelPath);
            this.setupAtomicWatcher(symbol, modelPath);
        }
    }

    async loadModelSafe(symbol, modelPath) {
        if (!fs.existsSync(modelPath)) return false;
        try {
            const newSession = await ort.InferenceSession.create(modelPath);
            this.sessions[symbol] = newSession;
            console.log(`🧠 [AI ENGINE V16] Nạp thành công bộ não Vi Cấu Trúc cho ${symbol}! (Input: 13 Features)`);
            return true;
        } catch (err) {
            console.error(`❌ [AI FATAL] File model lỗi! Giữ não cũ cho ${symbol}.`, err.message);
            return false;
        }
    }

    setupAtomicWatcher(symbol, modelPath) {
        if (this.watchers[symbol]) return;
        const dir = path.dirname(modelPath);
        const filename = path.basename(modelPath);
        try {
            this.watchers[symbol] = fs.watch(dir, async (eventType, triggerName) => {
                if (eventType === 'rename' && triggerName === filename) {
                    if (this.isReloading[symbol]) return;
                    this.isReloading[symbol] = true;
                    setTimeout(async () => {
                        await this.loadModelSafe(symbol, modelPath);
                        setTimeout(() => { this.isReloading[symbol] = false; }, 2000);
                    }, 100);
                }
            });
        } catch (error) { }
    }

    async predict(historyCandles, symbol) {
        await this.init(symbol);

        // Mặc định trả về 13 số 0 nếu chưa có não ONNX hoặc thiếu data
        const defaultFeatures = new Array(13).fill(0);
        if (!this.sessions[symbol] || historyCandles.length < 2) {
            return { winProb: 0.5, features: defaultFeatures };
        }

        // 1. LẤY DỮ LIỆU NẾN HIỆN TẠI (ALTCOIN)
        const current = historyCandles[historyCandles.length - 1];
        const prev = historyCandles[historyCandles.length - 2];

        // 2. KÉO DỮ LIỆU VI CẤU TRÚC (Từ StreamAggregator bắn sang RAM)
        const micro = global.liveMicroData?.[symbol] || {};
        const macro = global.liveMacroData?.[symbol] || {};

        // 3. TÍNH TOÁN CÁC FEATURE MỞ RỘNG (Giống hệt Python)
        const taker_buy_ratio = current.quoteVolume > 0 
            ? (current.takerBuyQuote / current.quoteVolume) 
            : 0.5;
        const body_size = Math.abs(current.close - current.open);
        const wick_size = (current.high - current.low) - body_size;
        
        const coin_pct_change = (current.close - prev.close) / (prev.close + 1e-8);

        // 4. KÉO DỮ LIỆU ÔNG TRÙM (BTCUSDT) ĐỂ SO SÁNH SỨC MẠNH
        let btc_pct_change = 0;
        if (symbol !== 'BTCUSDT') {
            const btcHistory = InMemoryBuffer.getHistoryObjects('BTCUSDT');
            if (btcHistory && btcHistory.length >= 2) {
                const btcCurrent = btcHistory[btcHistory.length - 1];
                const btcPrev = btcHistory[btcHistory.length - 2];
                btc_pct_change = (btcCurrent.close - btcPrev.close) / (btcPrev.close + 1e-8);
            }
        }
        const btc_relative_strength = coin_pct_change - btc_pct_change;

        // 5. ĐÓNG GÓI MẢNG 13 FEATURES (THỨ TỰ PHẢI KHỚP 100% VỚI PYTHON)
        const features13 = Float32Array.from([
            micro.ob_imb_top20 || 0,
            micro.spread_close || 0,
            micro.bid_vol_1pct || 0,
            micro.ask_vol_1pct || 0,
            micro.max_buy_trade || 0,
            micro.max_sell_trade || 0,
            micro.liq_long_vol || 0,
            micro.liq_short_vol || 0,
            macro.funding_rate || micro.funding_rate || 0,
            taker_buy_ratio,
            body_size,
            wick_size,
            btc_relative_strength
        ]);

        const tensor = new ort.Tensor('float32', features13, [1, 13]);

        let winProb = 0.5; // Mặc định là Nhãn 2 (Nhiễu)
        
        try {
            const results = await this.sessions[symbol].run({ float_input: tensor });
            let probs = results.probabilities?.data || results.output_probability?.data;

            if (probs && probs.length >= 3) {
                const prob_short = probs[0]; // Xác suất giảm (Short)
                const prob_long = probs[1];  // Xác suất tăng (Long)
                const prob_noise = probs[2]; // Xác suất nhiễu

                // Truyền thẳng xác suất Long hoặc Short nếu nó vượt trội hơn tỷ lệ Nhiễu
                if (prob_long > prob_noise && prob_long > prob_short) {
                    winProb = prob_long; // (VD: 0.65 -> Sẽ kích hoạt Long ở DeepThinker)
                } else if (prob_short > prob_noise && prob_short > prob_long) {
                    winProb = 1 - prob_short; // Đảo ngược để < 0.5 (VD: 1 - 0.65 = 0.35 -> Kích hoạt Short)
                }
            }
        } catch (err) {
            console.error(`❌ [AI ERROR] Lỗi dự đoán Tensor ${symbol}:`, err.message);
        }

        return { winProb: winProb, features: Array.from(features13) };
    }
}
module.exports = new AiEngine();