/**
 * src/services/ai/AiEngine.js
 */
const ort = require('onnxruntime-node');
const path = require('path');

class AiEngine {
    constructor() {
        this.session = null;
    }

    async init() {
        if (!this.session) {
            const modelPath = path.join(__dirname, '../../../ai_quant_sniper.onnx');
            this.session = await ort.InferenceSession.create(modelPath);
            console.log('🧠 [AI ENGINE] Đã nạp thành công bộ não ONNX V4.1 vào RAM!');
        }
    }

    /**
     * historyCandles: Mảng chứa ít nhất 721 documents từ MarketData
     * Sắp xếp thời gian tăng dần (Nến mới nhất nằm ở cuối mảng)
     */
    async predict(historyCandles) {
        await this.init();

        if (historyCandles.length < 721) {
            console.warn('⚠️ [AI] Không đủ 721 nến lịch sử để tính Quant Features.');
            return 0; // Trả về 0% win rate để từ chối giao dịch
        }

        const current = historyCandles[historyCandles.length - 1];

        // --- HÀM HỖ TRỢ TÍNH ĐỘ LỆCH CHUẨN (STANDARD DEVIATION) ---
        const calcStd = (arr) => {
            const mean = arr.reduce((a, b) => a + b) / arr.length;
            const variance = arr.map(x => Math.pow(x - mean, 2)).reduce((a, b) => a + b) / arr.length;
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

        // 5. Đóng gói Tensor 5 chiều (Khớp 100% với file Python)
        // Thứ tự: hurst, vwap, wick, vol_acc, fund_delta
        const inputData = Float32Array.from([
            hurst_proxy, dist_vwap, wick_to_body, vol_acceleration, funding_delta_12h
        ]);

        const tensor = new ort.Tensor('float32', inputData, [1, 5]);

        // 6. Chạy Suy Luận ONNX
        try {
            const feeds = { float_input: tensor };
            const results = await this.session.run(feeds);

            // Xử lý dữ liệu trả về từ XGBoost ONNX (Lấy xác suất Class 1)
            // Cấu trúc trả về thường là mảng probabilities[0][1] hoặc flat array
            let winProb = 0;
            if (results.probabilities.data) {
                // Tùy phiên bản ONNX, xác suất nhãn 1 thường ở index 1
                winProb = results.probabilities.data[1] !== undefined ? results.probabilities.data[1] : 0;
            }
            return winProb;
        } catch (err) {
            console.error('❌ [AI ENGINE] Lỗi khi chạy mô hình:', err.message);
            return 0;
        }
    }
}

module.exports = new AiEngine();