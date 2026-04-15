// /**
//  * src/services/ai/AiEngine.js - Version 5.3 (11 Features & Shadow Mode)
//  */
// const ort = require('onnxruntime-node');
// const path = require('path');
// const fs = require('fs');

// class AiEngine {
//     constructor() {
//         this.sessions = {};
//         this.watchers = {};
//         this.isReloading = {};
//     }

//     async init(symbol) {
//         const modelPath = path.join(__dirname, `../../../model_${symbol}.onnx`);
//         if (!this.sessions[symbol]) {
//             await this.loadModelSafe(symbol, modelPath);
//             this.setupAtomicWatcher(symbol, modelPath);
//         }
//     }

//     async loadModelSafe(symbol, modelPath) {
//         if (!fs.existsSync(modelPath)) return false;
//         try {
//             const newSession = await ort.InferenceSession.create(modelPath);
//             this.sessions[symbol] = newSession;
//             console.log(`🧠 [AI ENGINE] Nạp thành công bộ não cho ${symbol}! (Input Size: 7)`);
//             return true;
//         } catch (err) {
//             console.error(`❌ [AI FATAL] File model lỗi! Giữ não cũ.`, err.message);
//             return false;
//         }
//     }

//     setupAtomicWatcher(symbol, modelPath) {
//         if (this.watchers[symbol]) return;
//         const dir = path.dirname(modelPath);
//         const filename = path.basename(modelPath);
//         try {
//             this.watchers[symbol] = fs.watch(dir, async (eventType, triggerName) => {
//                 if (eventType === 'rename' && triggerName === filename) {
//                     if (this.isReloading[symbol]) return;
//                     this.isReloading[symbol] = true;
//                     setTimeout(async () => {
//                         await this.loadModelSafe(symbol, modelPath);
//                         setTimeout(() => { this.isReloading[symbol] = false; }, 2000);
//                     }, 100);
//                 }
//             });
//         } catch (error) { }
//     }

//     // Lấy Sentiment với thuật toán Time Decay
//     getTopTraderSentiment(symbol) {
//         if (!global.topTraderRatios) return 0;
//         const data = global.topTraderRatios[symbol];
//         if (!data) return 0;

//         const ageMinutes = (Date.now() - data.timestamp) / 60000;
//         let trust = 1.0;

//         if (data.isStale) {
//             trust = Math.max(0, 1 - (ageMinutes / 60)); // Giảm mạnh về 0 sau 1 tiếng nếu rớt mạng
//         } else {
//             trust = Math.max(0.5, 1 - (ageMinutes / 120)); // Giảm nhẹ nếu dữ liệu cũ
//         }

//         const rawRatio = data.longRatio / (data.shortRatio + 1e-12);
//         return (rawRatio - 1.0) * trust; // > 0 là Cá voi Long, < 0 là Cá voi Short
//     }

//     async predict(historyCandles, symbol) {
//         await this.init(symbol);

//         // Trả về 11 số 0 nếu chưa đủ nến
//         const defaultFeatures = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
//         if (!this.sessions[symbol] || historyCandles.length < 721) {
//             return { winProb: 0.5, features: defaultFeatures };
//         }

//         const current = historyCandles[historyCandles.length - 1];
//         const calcStd = (arr) => {
//             if (arr.length <= 1) return 0;
//             const mean = arr.reduce((a, b) => a + b) / arr.length;
//             const variance = arr.map(x => Math.pow(x - mean, 2)).reduce((a, b) => a + b) / (arr.length - 1);
//             return Math.sqrt(Math.max(0, variance));
//         };

//         // --- TÍNH TOÁN 11 ĐẶC TRƯNG ---
//         // 0. Hurst
//         let ret1m_arr = [], ret20m_arr = [];
//         for (let i = historyCandles.length - 20; i < historyCandles.length; i++) {
//             ret1m_arr.push((historyCandles[i].close - historyCandles[i - 1].close) / historyCandles[i - 1].close);
//             ret20m_arr.push((historyCandles[i].close - historyCandles[i - 20].close) / historyCandles[i - 20].close);
//         }
//         const hurst = (calcStd(ret20m_arr) || 0) / (Math.sqrt(20) * (calcStd(ret1m_arr) || 1e-8) + 1e-8);

//         // 1. VWAP (Sử dụng Mark Price để chống làm giá)
//         let sum_pv = 0, sum_vol = 0;
//         for (let i = historyCandles.length - 240; i < historyCandles.length; i++) {
//             const mPrice = historyCandles[i].mark_close || historyCandles[i].close;
//             sum_pv += mPrice * historyCandles[i].volume;
//             sum_vol += historyCandles[i].volume;
//         }
//         const currentMark = current.mark_close || current.close;
//         const vwap = (currentMark - (sum_pv / (sum_vol + 1e-8))) / (sum_pv / (sum_vol + 1e-8));

//         // 2 & 3. Wick/Body & Vol Accel
//         const wick_body = (current.high - current.low) / (Math.abs(current.close - current.open) + 1e-8);
//         let sum_vol_15 = 0;
//         for (let i = historyCandles.length - 15; i < historyCandles.length; i++) sum_vol_15 += historyCandles[i].volume;
//         const vol_accel = current.volume / ((sum_vol_15 / 15) + 1e-8);

//         // 4. Sentiment Divergence (Cá Voi vs Đám đông)
//         const sentiment = this.getTopTraderSentiment(symbol);

//         // 5 & 6. ATR Norm & RSI
//         let sumTr = 0, gains = 0, losses = 0;
//         for (let i = historyCandles.length - 14; i < historyCandles.length; i++) {
//             let h = historyCandles[i].high, l = historyCandles[i].low, pc = historyCandles[i - 1].close;
//             sumTr += Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
//             let diff = historyCandles[i].close - historyCandles[i - 1].close;
//             if (diff > 0) gains += diff; else losses -= diff;
//         }
//         const atr_norm = (sumTr / 14) / current.close;
//         const rsi = 100 - (100 / (1 + ((gains / 14) / ((losses / 14) + 1e-8))));

//         // --- NHÓM VI CẤU TRÚC (MICROSTRUCTURE) ---
//         // 7. VPIN
//         const vpin = current.vpin || 0;

//         // 8. Orderbook Imbalance (Chuẩn hóa về [-1, 1])
//         const ob_raw = current.ob_imb || 1.0;
//         const ob_imb_norm = (ob_raw - 1) / (ob_raw + 1);

//         // 9. Liquidation Pressure
//         const liq_l = current.liq_long || 0;
//         const liq_s = current.liq_short || 0;
//         const liq_press = (liq_l + liq_s) === 0 ? 0 : (liq_l - liq_s) / (liq_l + liq_s);

//         // 10. Premium Index
//         const prem_idx = currentMark === 0 ? 0 : (currentMark - current.close) / currentMark;

//         // ==========================================
//         // TẠO MẢNG 11 ĐẶC TRƯNG & SHADOW MODE
//         // ==========================================
//         const features11 = Float32Array.from([
//             hurst, vwap, wick_body, vol_accel, sentiment, atr_norm, rsi, // 7 Cũ
//             vpin, ob_imb_norm, liq_press, prem_idx                       // 4 Mới
//         ]);

//         // ⚔️ FEATURE MASKING: Dùng .subarray(0, 7) tránh GC, đánh lừa não cũ
//         const tensor = new ort.Tensor('float32', features11.subarray(0, 7), [1, 7]);

//         let winProb = 0.5;
//         try {
//             const results = await this.sessions[symbol].run({ float_input: tensor });
//             if (results.probabilities && results.probabilities.data) {
//                 winProb = results.probabilities.data[1] !== undefined ? results.probabilities.data[1] : 0;
//             } else if (results.output_probability && results.output_probability.data) {
//                 winProb = results.output_probability.data[1];
//             }
//         } catch (err) {
//             console.error(`❌ [AI ERROR] ${symbol} ONNX inference failed:`, err.message);
//         }

//         // Trả về full 11 biến để DeepThinker dùng và File JSON lưu lại
//         return { winProb: winProb, features: features11 };
//     }
// }
// module.exports = new AiEngine();


/**
 * src/services/ai/AiEngine.js - Version 5.3 (11 Features & Shadow Mode)
 */
const ort = require('onnxruntime-node');
const path = require('path');
const fs = require('fs');

class AiEngine {
    constructor() {
        this.sessions = {};
        this.watchers = {};
        this.isReloading = {};
    }

    async init(symbol) {
        const modelPath = path.join(__dirname, `../../../model_${symbol}.onnx`);
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
            // Đổi log thành 8 để đúng với mô hình thực tế
            console.log(`🧠 [AI ENGINE] Nạp thành công bộ não cho ${symbol}! (Input Size: 8)`);
            return true;
        } catch (err) {
            console.error(`❌ [AI FATAL] File model lỗi! Giữ não cũ.`, err.message);
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

    // Lấy Sentiment với thuật toán Time Decay
    getTopTraderSentiment(symbol) {
        if (!global.topTraderRatios) return 0;
        const data = global.topTraderRatios[symbol];
        if (!data) return 0;

        const ageMinutes = (Date.now() - data.timestamp) / 60000;
        let trust = 1.0;

        if (data.isStale) {
            trust = Math.max(0, 1 - (ageMinutes / 60)); // Giảm mạnh về 0 sau 1 tiếng nếu rớt mạng
        } else {
            trust = Math.max(0.5, 1 - (ageMinutes / 120)); // Giảm nhẹ nếu dữ liệu cũ
        }

        const rawRatio = data.longRatio / (data.shortRatio + 1e-12);
        return (rawRatio - 1.0) * trust; // > 0 là Cá voi Long, < 0 là Cá voi Short
    }

    async predict(historyCandles, symbol) {
        await this.init(symbol);
        
        // Trả về mặc định nếu chưa đủ nến (Yêu cầu 721 nến do tính VWAP và Funding)
        const defaultFeatures = [0,0,0,0,0,0,0,0,0,0,0]; 
        if (!this.sessions[symbol] || historyCandles.length < 721) {
            return { winProb: 0.5, features: defaultFeatures }; 
        }

        const current = historyCandles[historyCandles.length - 1];
        const calcStd = (arr) => {
            if (arr.length <= 1) return 0;
            const mean = arr.reduce((a, b) => a + b) / arr.length;
            const variance = arr.map(x => Math.pow(x - mean, 2)).reduce((a, b) => a + b) / (arr.length - 1);
            return Math.sqrt(Math.max(0, variance));
        };

        // --- TÍNH TOÁN CÁC ĐẶC TRƯNG ---
        
        // 0. Hurst
        let ret1m_arr = [], ret20m_arr = [];
        for (let i = historyCandles.length - 20; i < historyCandles.length; i++) {
            ret1m_arr.push((historyCandles[i].close - historyCandles[i - 1].close) / historyCandles[i - 1].close);
            ret20m_arr.push((historyCandles[i].close - historyCandles[i - 20].close) / historyCandles[i - 20].close);
        }
        const hurst = (calcStd(ret20m_arr) || 0) / (Math.sqrt(20) * (calcStd(ret1m_arr) || 1e-8) + 1e-8);

        // 1. VWAP (Sử dụng Mark Price để chống làm giá)
        let sum_pv = 0, sum_vol = 0;
        for (let i = historyCandles.length - 240; i < historyCandles.length; i++) {
            const mPrice = historyCandles[i].mark_close || historyCandles[i].close;
            sum_pv += mPrice * historyCandles[i].volume;
            sum_vol += historyCandles[i].volume;
        }
        const currentMark = current.mark_close || current.close;
        const vwap = (currentMark - (sum_pv / (sum_vol + 1e-8))) / (sum_pv / (sum_vol + 1e-8));

        // 2 & 3. Wick/Body & Vol Accel
        const wick_body = (current.high - current.low) / (Math.abs(current.close - current.open) + 1e-8);
        let sum_vol_15 = 0;
        for (let i = historyCandles.length - 15; i < historyCandles.length; i++) sum_vol_15 += historyCandles[i].volume;
        const vol_accel = current.volume / ((sum_vol_15 / 15) + 1e-8);

        // 4. Funding Delta 12h (MỚI - KHỚP VỚI PYTHON)
        let funding_delta = 0;
        if (historyCandles.length >= 720) {
            funding_delta = (current.funding_rate || 0) - (historyCandles[historyCandles.length - 720].funding_rate || 0);
        }

        // 5 & 6. ATR Norm & RSI
        let sumTr = 0, gains = 0, losses = 0;
        for (let i = historyCandles.length - 14; i < historyCandles.length; i++) {
            let h = historyCandles[i].high, l = historyCandles[i].low, pc = historyCandles[i - 1].close;
            sumTr += Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
            let diff = historyCandles[i].close - historyCandles[i - 1].close;
            if (diff > 0) gains += diff; else losses -= diff;
        }
        const atr_norm = (sumTr / 14) / current.close;
        const rsi = 100 - (100 / (1 + ((gains / 14) / ((losses / 14) + 1e-8))));

        // --- NHÓM VI CẤU TRÚC (MICROSTRUCTURE) ---
        // 7. VPIN
        const vpin = current.vpin || 0;
        
        // 8. Orderbook Imbalance (Chuẩn hóa về [-1, 1])
        const ob_raw = current.ob_imb || 1.0;
        const ob_imb_norm = (ob_raw - 1) / (ob_raw + 1);

        // 9. Liquidation Pressure
        const liq_l = current.liq_long || 0;
        const liq_s = current.liq_short || 0;
        const liq_press = (liq_l + liq_s) === 0 ? 0 : (liq_l - liq_s) / (liq_l + liq_s);

        // 10. Premium Index
        const prem_idx = currentMark === 0 ? 0 : (currentMark - current.close) / currentMark;

        // ==========================================
        // TẠO MẢNG ĐẶC TRƯNG & NẠP VÀO AI V5.3 (8 Biến)
        // ==========================================
        const features11 = Float32Array.from([
            hurst, vwap, wick_body, vol_accel, funding_delta, atr_norm, rsi, 
            vpin, ob_imb_norm, liq_press, prem_idx                       
        ]);
        
        // MỞ KHÓA 8 CỔNG TENSOR CHO MÔ HÌNH XGBoost MỚI
        const tensor = new ort.Tensor('float32', features11.subarray(0, 8), [1, 8]); 

        let winProb = 0.5;
        try {
            const results = await this.sessions[symbol].run({ float_input: tensor });
            if (results.probabilities && results.probabilities.data) {
                winProb = results.probabilities.data[1] !== undefined ? results.probabilities.data[1] : 0;
            } else if (results.output_probability && results.output_probability.data) {
                 winProb = results.output_probability.data[1]; 
            }
        } catch (err) {
            console.error(`[AI ERROR] Lỗi khi chạy mô hình cho ${symbol}:`, err.message);
        }

        // Trả về full biến để DeepThinker dùng và File JSON lưu lại
        return { winProb: winProb, features: features11 };
    }
}
module.exports = new AiEngine();