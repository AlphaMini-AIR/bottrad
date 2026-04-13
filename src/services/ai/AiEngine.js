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
            console.log('🧠 [AI ENGINE] Đã nạp thành công bộ não ONNX vào Node.js!');
        }
    }

    async predict(currentCandle, prev1m, prev15m, prev4h) {
        await this.init();

        // 1. TÍNH TOÁN CÁC BIẾN (FEATURES) KHỚP VỚI BẢN TRAIN PYTHON
        const returns_1m = (currentCandle.close - prev1m.close) / prev1m.close;
        const returns_15m = (currentCandle.close - prev15m.close) / prev15m.close;
        const returns_4h = (currentCandle.close - prev4h.close) / prev4h.close;

        // Thay volatility bằng rel_strength (Giá hiện tại / Giá trung bình 20 nến trước)
        // Vì chúng ta không có EMA20 thực thụ ở đây, mình sẽ lấy giá Close / Giá 15m trước để mô phỏng
        const rel_strength = currentCandle.close / prev15m.close;

        // 2. ĐƯA VÀO MẢNG (Phải đúng thứ tự như list features bên Python)
        // Thứ tự: returns_1m, returns_15m, returns_4h, rel_strength
        const inputData = Float32Array.from([
            returns_1m,
            returns_15m,
            returns_4h,
            rel_strength
        ]);

        const tensor = new ort.Tensor('float32', inputData, [1, 4]);

        // 3. CHẠY SUY LUẬN
        const feeds = { float_input: tensor };
        const results = await this.session.run(feeds);

        // 4. TRẢ VỀ XÁC SUẤT
        const probabilities = results.probabilities.data;
        return probabilities[1]; // Xác suất phe LONG
    }
}

module.exports = new AiEngine();