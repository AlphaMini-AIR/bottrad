/**
 * src/services/ai/AiEngine.js - V8.2 (20 Features & Multi-Class Classification)
 * Nhận diện 3 trạng thái: Long Đẹp (1), Short Đẹp (0), và Thị trường Rác/Nhiễu (2)
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
            console.log(`🧠 [AI ENGINE V8.2] Nạp thành công bộ não Đa Lớp cho ${symbol}! (Input: 20)`);
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

    async predict(historyCandles, symbol) {
        await this.init(symbol);

        // Mặc định trả về 20 số 0 nếu chưa đủ dữ liệu (Cần ít nhất 240 nến cho khung 4H)
        const defaultFeatures = new Array(20).fill(0);
        if (!this.sessions[symbol] || historyCandles.length < 240) {
            return { winProb: 0.5, features: defaultFeatures };
        }

        const current = historyCandles[historyCandles.length - 1];
        const len = historyCandles.length;

        // ========================================================
        // 1. VÙNG THỜI GIAN VÀNG (KILL ZONES UTC+7)
        // ========================================================
        const date = new Date(current.timestamp);
        const utc7Hours = (date.getUTCHours() + 7) % 24;
        const timeFloat = utc7Hours + (date.getUTCMinutes() / 60.0);

        const is_dead_zone = (timeFloat >= 7.0 && timeFloat < 13.0) ? 1 : 0;
        const is_london_open = (timeFloat >= 13.5 && timeFloat <= 16.5) ? 1 : 0;
        const is_ny_open = (timeFloat >= 19.0 && timeFloat <= 22.5) ? 1 : 0;

        // ========================================================
        // 2. MTFA: XU HƯỚNG 15M & 4H
        // ========================================================
        const open15m = historyCandles[Math.max(0, len - 15)].open;
        const trend_15m = current.close > open15m ? 1 : -1;

        const open4h = historyCandles[Math.max(0, len - 240)].open;
        const trend_4h = current.close > open4h ? 1 : -1;

        let sum_close_240 = 0;
        for (let i = Math.max(0, len - 240); i < len; i++) sum_close_240 += historyCandles[i].close;
        const ema20_4h_proxy = sum_close_240 / Math.min(240, len);
        const dist_to_4h_ema = (current.close - ema20_4h_proxy) / (ema20_4h_proxy + 1e-8);

        // ========================================================
        // 3. FVG 15M MITIGATION (LẤP ĐẦY KHOẢNG TRỐNG)
        // ========================================================
        let fvg_bull_mitigation = 0;
        let fvg_bear_mitigation = 0;

        if (len >= 45) {
            const candle1_high = Math.max(...historyCandles.slice(len - 45, len - 30).map(c => c.high));
            const candle1_low = Math.min(...historyCandles.slice(len - 45, len - 30).map(c => c.low));
            const candle3_high = Math.max(...historyCandles.slice(len - 15, len).map(c => c.high));
            const candle3_low = Math.min(...historyCandles.slice(len - 15, len).map(c => c.low));

            if (candle3_low > candle1_high) {
                const fvg_size = candle3_low - candle1_high;
                fvg_bull_mitigation = Math.max(0, Math.min(1, (candle3_low - current.low) / (fvg_size + 1e-8)));
            }
            if (candle3_high < candle1_low) {
                const fvg_size = candle1_low - candle3_high;
                fvg_bear_mitigation = Math.max(0, Math.min(1, (current.high - candle3_high) / (fvg_size + 1e-8)));
            }
        }

        // ========================================================
        // 4. DÒNG TIỀN & BẪY SỔ LỆNH (SPOOFING)
        // ========================================================
        const vpin = current.vpin || 0;
        const funding_rate = current.funding_rate || 0;
        const ob_imb_norm = current.ob_imb_norm || 0;
        const spoofing_index = Math.abs(ob_imb_norm) * (1 - Math.abs(vpin));

        const price_roc_5 = (current.close - historyCandles[Math.max(0, len - 5)].close) / (historyCandles[Math.max(0, len - 5)].close + 1e-8);
        let vpin_sum_5 = 0;
        for (let i = Math.max(0, len - 5); i < len; i++) vpin_sum_5 += (historyCandles[i].vpin || 0);

        let cum_delta_div = 0;
        if (price_roc_5 >= -0.001 && vpin_sum_5 < -0.5) cum_delta_div = -1;
        if (price_roc_5 <= 0.001 && vpin_sum_5 > 0.5) cum_delta_div = 1;

        // ========================================================
        // 5. CẤU TRÚC (SWING) & TÂM LÝ (EXHAUSTION, TRAP)
        // ========================================================
        const recent20 = historyCandles.slice(-21, -1);
        const swing_high = Math.max(...recent20.map(c => c.high));
        const swing_low = Math.min(...recent20.map(c => c.low));

        const dist_to_swing_high = (swing_high - current.close) / (current.close + 1e-8);
        const dist_to_swing_low = (current.close - swing_low) / (current.close + 1e-8);

        const bear_trap = (current.low < swing_low && current.close > swing_low) ? 1 : 0;
        const bull_trap = (current.high > swing_high && current.close < swing_high) ? 1 : 0;

        const body = Math.abs(current.close - current.open) + 1e-8;
        const prev = historyCandles[len - 2];
        const prev_body = Math.abs(prev.close - prev.open);
        const upper_wick = current.high - Math.max(current.open, current.close);
        const lower_wick = Math.min(current.open, current.close) - current.low;

        let vol_sum_20 = 0;
        for (let i = Math.max(0, len - 20); i < len; i++) vol_sum_20 += historyCandles[i].volume;
        const vol_avg_20 = vol_sum_20 / 20 + 1e-8;

        const bull_rejection = (lower_wick / body) * (current.volume / vol_avg_20);
        const bear_rejection = (upper_wick / body) * (current.volume / vol_avg_20);

        let exhaustion_ratio = 1.0;
        if (prev_body < 1e-6) {
            exhaustion_ratio = (current.volume > vol_avg_20 * 1.5) ? 999.0 : 1.0;
        } else {
            exhaustion_ratio = (body / prev_body) * (current.volume / vol_avg_20);
        }

        // ==========================================
        // 6. ĐÓNG GÓI TENSOR VÀ CHẠY AI MULTI-CLASS
        // ==========================================
        const features20 = Float32Array.from([
            vpin, funding_rate, ob_imb_norm,
            bull_rejection, bear_rejection, bear_trap, bull_trap,
            trend_15m, trend_4h, dist_to_4h_ema,
            is_dead_zone, is_london_open, is_ny_open,
            exhaustion_ratio, spoofing_index,
            fvg_bull_mitigation, fvg_bear_mitigation,
            dist_to_swing_high, dist_to_swing_low, cum_delta_div
        ]);

        const tensor = new ort.Tensor('float32', features20, [1, 20]);

        let winProb = 0.5; // Mặc định là 0.5 (Tức là AI_UNCERTAIN / Thị trường Nhiễu)
        
        try {
            const results = await this.sessions[symbol].run({ float_input: tensor });
            let probs;
            
            if (results.probabilities && results.probabilities.data) {
                probs = results.probabilities.data;
            } else if (results.output_probability && results.output_probability.data) {
                probs = results.output_probability.data;
            }

            if (probs && probs.length >= 3) {
                const prob_short = probs[0]; // Xác suất là Lệnh Short chuẩn
                const prob_long = probs[1];  // Xác suất là Lệnh Long chuẩn
                const prob_noise = probs[2]; // Xác suất là Thị trường Rác

                // Nếu xác suất Long áp đảo cả rác và short
                if (prob_long > 0.6 && prob_long > prob_noise) {
                    winProb = prob_long; // Truyền nguyên gốc (Vd: 0.75 -> Sẽ kích hoạt Long)
                } 
                // Nếu xác suất Short áp đảo
                else if (prob_short > 0.6 && prob_short > prob_noise) {
                    winProb = 1 - prob_short; // Đảo ngược để < 0.4 (Vd: 1 - 0.75 = 0.25 -> Sẽ kích hoạt Short)
                }
                // Còn lại: winProb giữ nguyên 0.5 (Báo động cho DeepThinker khóa cò)
            }
        } catch (err) {
            console.error(`❌ [AI ERROR] ${symbol}:`, err.message);
        }

        return { winProb: winProb, features: Array.from(features20) };
    }
}
module.exports = new AiEngine();