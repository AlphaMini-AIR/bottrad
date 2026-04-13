/**
 * src/services/ai/ModelRunner.js
 */
class ModelRunner {
    // Giả lập HMM: Đọc xu hướng vĩ mô dựa trên nến 4h/1d (Sẽ nâng cấp ở Sprint 4)
    // Hiện tại: Trả về trạng thái dựa trên biến động giá gần nhất của nến trong DB
    async getMacroRegime(symbol) {
        // Sau này sẽ load HMM.onnx
        // Hiện tại: Giả lập ngẫu nhiên để Hưng test được logic Hit & Run
        const regimes = ['TREND_UP', 'TREND_DOWN', 'SIDEWAY'];
        return regimes[Math.floor(Math.random() * regimes.length)];
    }

    // Giả lập XGBoost: Dự đoán xác suất dựa trên OBI thực tế
    async predictProbability(symbol, currentObi) {
        // Sau này sẽ load XGBoost.onnx
        // Hiện tại: Xác suất tỉ lệ thuận với cường độ OBI + độ nhiễu
        const noise = (Math.random() - 0.5) * 0.1;
        let prob = Math.abs(currentObi) + noise;
        return Math.min(Math.max(prob, 0), 1);
    }
}

module.exports = new ModelRunner();