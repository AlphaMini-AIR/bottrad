/**
 * src/services/ai/AiEngine.js - Version 5.0 (7 Features & Dynamic Models)
 */
const ort = require('onnxruntime-node');
const path = require('path');
const fs = require('fs');

class AiEngine {
    constructor() {
        // Quản lý nhiều model cùng lúc cho từng coin
        this.sessions = {};
    }

    async init(symbol) {
        if (!this.sessions[symbol]) {
            // Đọc model động theo tên coin sinh ra từ Python
            const modelPath = path.join(__dirname, `../../../model_${symbol}.onnx`);
            if (fs.existsSync(modelPath)) {
                this.sessions[symbol] = await ort.InferenceSession.create(modelPath);
                console.log(`🧠 [AI ENGINE] Đã nạp thành công bộ não ONNX V5.0 cho ${symbol}!`);
            } else {
                console.warn(`⚠️ [AI ENGINE] Không tìm thấy model cho ${symbol} tại ${modelPath}`);
            }
        }
    }

    /**
     * LƯU Ý: Phải truyền thêm symbol vào từ main.js
     */
    async predict(historyCandles, symbol) {
        await this.init(symbol);

        if (!this.sessions[symbol]) return 0;

        if (historyCandles.length < 721) {
            console.warn(`⚠️ [AI] ${symbol} không đủ 721 nến lịch sử.`);
            return 0; 
        }

        const current = historyCandles[historyCandles.length - 1];

        // --- CÔNG THỨC TOÁN HỌC KHỚP VỚI PANDAS PYTHON (Chia N-1) ---
        const calcStd = (arr) => {
            if (arr.length <= 1) return 0;
            const mean = arr.reduce((a, b) => a + b) / arr.length;
            const variance = arr.map(x => Math.pow(x - mean, 2)).reduce((a, b) => a + b) / (arr.length - 1);
            return Math.sqrt(Math.max(0, variance));
        };

        // 1. Hurst Proxy (Variance Ratio 20m)
        let ret1m_arr = [];
        let ret20m_arr = [];
        for (let i = historyCandles.length - 20; i < historyCandles.length; i++) {
            let ret1 = (historyCandles[i].close - historyCandles[i - 1].close) / historyCandles[i - 1].close;
            let ret20 = (historyCandles[i].close - historyCandles[i - 20].close) / historyCandles[i - 20].close;
            ret1m_arr.push(ret1);
            ret20m_arr.push(ret20);
        }
        const std_ret1m = calcStd(ret1m_arr) || 1e-8;
        const std_ret20m = calcStd(ret20m_arr) || 0;
        const hurst_proxy = std_ret20m / (Math.sqrt(20) * std_ret1m + 1e-8);

        // 2. Distance to VWAP 4h (240m)
        let sum_pv = 0;
        let sum_vol = 0;
        for (let i = historyCandles.length - 240; i < historyCandles.length; i++) {
            const c = historyCandles[i];
            sum_pv += c.close * c.volume;
            sum_vol += c.volume;
        }
        const vwap_4h = sum_pv / (sum_vol + 1e-8);
        const dist_vwap = (current.close - vwap_4h) / vwap_4h;

        // 3. Wick_to_Body & Vol_Acceleration
        const body = Math.abs(current.close - current.open) + 1e-8;
        const wick = current.high - current.low;
        const wick_to_body = wick / body;

        let sum_vol_15 = 0;
        for (let i = historyCandles.length - 15; i < historyCandles.length; i++) {
            sum_vol_15 += historyCandles[i].volume;
        }
        const avg_vol_15 = sum_vol_15 / 15;
        const vol_acceleration = current.volume / (avg_vol_15 + 1e-8);

        // 4. Funding Delta 12h (720m)
        const old_funding = historyCandles[historyCandles.length - 721].funding_rate || 0;
        const current_funding = current.funding_rate || 0;
        const funding_delta_12h = current_funding - old_funding;

        // 5. ATR Normalized (14 chu kỳ)
        let sumTr = 0;
        for (let i = historyCandles.length - 14; i < historyCandles.length; i++) {
            let h = historyCandles[i].high;
            let l = historyCandles[i].low;
            let pc = historyCandles[i - 1].close;
            let tr = Math.max(h - l, Math.abs(h - pc), Math.abs(l - pc));
            sumTr += tr;
        }
        const atr_norm = (sumTr / 14) / current.close;

        // 6. RSI (14 chu kỳ)
        let gains = 0, losses = 0;
        for (let i = historyCandles.length - 14; i < historyCandles.length; i++) {
            let diff = historyCandles[i].close - historyCandles[i - 1].close;
            if (diff > 0) gains += diff;
            else losses -= diff;
        }
        const rs = (gains / 14) / ((losses / 14) + 1e-8);
        const rsi = 100 - (100 / (1 + rs));

        // 7. Đóng gói Tensor 7 chiều (Khớp hoàn toàn với danh sách features bên Python)
        const inputData = Float32Array.from([
            hurst_proxy, dist_vwap, wick_to_body, vol_acceleration, funding_delta_12h, atr_norm, rsi
        ]);

        // Cập nhật tensor nhận mảng 7 chiều
        const tensor = new ort.Tensor('float32', inputData, [1, 7]);

        try {
            const feeds = { float_input: tensor };
            const results = await this.sessions[symbol].run(feeds);
            
            let winProb = 0;
            if (results.probabilities && results.probabilities.data) {
                winProb = results.probabilities.data[1] !== undefined ? results.probabilities.data[1] : 0;
            } else if (results.output_probability && results.output_probability.data) {
                 winProb = results.output_probability.data[1]; 
            }
            return winProb;
        } catch (err) {
            console.error(`❌ [AI ENGINE] Lỗi khi chạy mô hình ${symbol}:`, err.message);
            return 0;
        }
    }
}

module.exports = new AiEngine();