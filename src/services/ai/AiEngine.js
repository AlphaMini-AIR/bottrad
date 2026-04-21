/**
 * src/services/ai/AiEngine.js - V16.2 (Production Ready + Snapshot Guard)
 * Tối ưu hóa mỏ neo BTC, Xử lý linh hoạt Output ONNX & Vá lỗi Mù Dữ Liệu
 */
const ort = require('onnxruntime-node');
const path = require('path');
const fs = require('fs');
const InMemoryBuffer = require('../data/InMemoryBuffer'); // Dùng để gọi data BTC và Snapshot Micro

class AiEngine {
    constructor() {
        this.sessions = {};
        this.watchers = {};
        this.isReloading = {};
        // Cache danh sách các coin CHƯA CÓ model để không bị check ổ cứng liên tục (Gây lag VPS)
        this.missingModels = new Set();
    }

    async init(symbol) {
        // Nếu đã xác nhận là không có model từ trước, bỏ qua luôn để tiết kiệm tài nguyên
        if (this.sessions[symbol] || this.missingModels.has(symbol)) return;

        const modelPath = path.join(__dirname, `../../../model_v16_${symbol}.onnx`);
        const success = await this.loadModelSafe(symbol, modelPath);

        if (success) {
            this.setupAtomicWatcher(symbol, modelPath);
        } else {
            // Ghi nhớ coin này chưa có file .onnx (Đang ở Tầng 3 thử việc)
            this.missingModels.add(symbol);
        }
    }

    async loadModelSafe(symbol, modelPath) {
        if (this.isReloading[symbol]) return false;
        this.isReloading[symbol] = true;

        try {
            if (fs.existsSync(modelPath)) {
                // Dùng CPU backend để đảm bảo tương thích đa nền tảng VPS
                const session = await ort.InferenceSession.create(modelPath, { executionProviders: ['cpu'] });
                this.sessions[symbol] = session;
                console.log(`🤖 [AiEngine] Đã nạp Model V16 cho ${symbol}`);
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
                    console.log(`🔄 [AiEngine] Phát hiện Model ${symbol} mới. Đang nạp lại (Hot-Reload)...`);
                    await this.loadModelSafe(symbol, modelPath);
                }, 2000); // Trì hoãn 2s để file copy hoàn tất tránh lỗi gián đoạn
            }
        });
    }

    async predict(symbol, historyCandles) {
        // Cần ít nhất 2 nến để so sánh (current và prev)
        if (!this.sessions[symbol] || !historyCandles || historyCandles.length < 2) return null;

        try {
            const current = historyCandles[historyCandles.length - 1];
            const prev = historyCandles[historyCandles.length - 2];

            // =========================================================================
            // 🟢 [BẢN VÁ LỖI MÙ DỮ LIỆU]: Lấy Snapshot chốt sổ từ InMemoryBuffer
            // =========================================================================
            const closedData = InMemoryBuffer.getLatestClosedCandle(symbol);
            if (!closedData || !closedData.micro) {
                // Chưa có cây nến nào đóng hoàn toàn kể từ khi bật Bot
                return null;
            }

            const micro = closedData.micro;
            const macro = closedData.macro || { funding_rate: 0 };

            // 🟢 1. Feature Engineering cơ bản
            current.body_size = (Math.abs(current.close - current.open) / current.open) * 100;
            current.wick_size = (((current.high - current.low) - Math.abs(current.close - current.open)) / current.open) * 100;

            // 🟢 2. Tính toán BTC Relative Strength (Dùng cache chống Look-ahead)
            let btc_relative_strength = 0;
            const btcData = InMemoryBuffer.getHistoryObjects('BTCUSDT');

            if (btcData && btcData.length >= 2) {
                const btcCurrent = btcData[btcData.length - 1];
                const btcPrev = btcData[btcData.length - 2];

                const timeDiff = Math.abs(current.openTime - btcCurrent.openTime);
                // CHỐNG NHÌN TRỘM: Đảm bảo nến BTCUSDT kéo ra khớp hoàn toàn về Timestamp
                if (timeDiff < 65000) {
                    const altcoin_pct = ((current.close - prev.close) / prev.close) * 100;
                    const btc_pct = ((btcCurrent.close - btcPrev.close) / btcPrev.close) * 100;
                    btc_relative_strength = altcoin_pct - btc_pct;
                } else {
                    console.warn(`⚠️ [AiEngine] ${symbol} lệch nhịp BTC (${timeDiff}ms). Tạm bỏ qua RS.`);
                }
            }

            // 🟢 3. Xây dựng Mảng Đặc Trưng (Features) KHỚP 100% VỚI PYTHON
            const features13 = [
                current.body_size,
                current.wick_size,
                current.volume,
                macro.funding_rate,
                current.vpin || 0,            // Lấy từ current vì đã được tính ở luồng mảng Float32Array
                micro.ob_imb_top20 || 0,      // Lấy từ Snapshot
                micro.liq_long_vol || 0,      // 🚀 Lấy từ Snapshot: Lượng Long bị cháy
                micro.liq_short_vol || 0,     // 🚀 Lấy từ Snapshot: Lượng Short bị cháy
                micro.max_buy_trade || 0,     // 🚀 Lấy từ Snapshot: Lệnh Taker Buy lớn nhất
                micro.max_sell_trade || 0,    // 🚀 Lấy từ Snapshot: Lệnh Taker Sell lớn nhất
                btc_relative_strength,
                micro.spread_close || 0,      // Lấy từ Snapshot
                current.close
            ];

            // 🟢 4. Biên dịch sang Tensor và chạy suy luận (Inference)
            const tensor = new ort.Tensor('float32', features13, [1, 13]);
            let winProb = 0.5; // Mặc định là Nhãn 2 (Nhiễu)

            const results = await this.sessions[symbol].run({ float_input: tensor });

            // Tự động nhận diện output (Xử lý tùy biến của ONNX do Sklearn sinh ra)
            const outputName = this.sessions[symbol].outputNames[1] || 'probabilities';
            let probs = results[outputName]?.data || results.probabilities?.data || results.output_probability?.data;

            if (probs && probs.length >= 3) {
                const prob_short = probs[0]; // Xác suất giảm (Short)
                const prob_long = probs[1];  // Xác suất tăng (Long)
                const prob_noise = probs[2]; // Xác suất nhiễu

                // Truyền thẳng xác suất Long/Short nếu nó vượt trội hơn tỷ lệ Nhiễu
                if (prob_long > prob_noise && prob_long > prob_short) {
                    winProb = prob_long; // (VD: 0.65 -> Sẽ kích hoạt Long ở DeepThinker)
                } else if (prob_short > prob_noise && prob_short > prob_long) {
                    winProb = 1 - prob_short; // Đảo ngược để < 0.5 (VD: 1 - 0.65 = 0.35 -> Kích hoạt Short)
                }
            } else {
                console.warn(`⚠️ [AiEngine] ONNX trả về mảng xác suất bất thường cho ${symbol}`);
            }

            // 🟢 5. Gọi bộ não Quản Trị Rủi Ro (DeepThinker)
            const DeepThinker = require('./DeepThinker');
            const deepThinkerResult = DeepThinker.evaluate(symbol, current.close, winProb, micro);

            // BẮT BUỘC SỬA ĐOẠN NÀY ĐỂ MAIN.JS HỨNG ĐƯỢC ĐẦY ĐỦ VÀ ĐÚNG TÊN
            return {
                winProb: winProb,          // Đổi từ 'prob' thành 'winProb' để khớp với main.js
                thinker: deepThinkerResult,
                features: features13       // Bơm toàn bộ 13 features ra ngoài để lưu Trade Journal
            };

        } catch (err) {
            console.error(`❌ [AiEngine] Lỗi suy luận ${symbol}:`, err);
            return null;
        }
    }
}

module.exports = new AiEngine();