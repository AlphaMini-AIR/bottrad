// src/ml/marketClassifier.ts
import { calcADX } from '../engine/indicators';

/**
 * Cấu hình cho bộ phân loại thị trường
 * @param adxThreshold: Dưới mức này được coi là Sideway (Tiêu chuẩn là 25)
 * @param bbThreshold: Độ rộng dải Bollinger (Dùng cho logic Squeeze nếu cần)
 */
export interface ClassifierConfig {
    adxThreshold?: number;
    bbThreshold?: number;
}

/**
 * PHÂN LOẠI TRẠNG THÁI THỊ TRƯỜNG (Dựa trên sức mạnh xu hướng ADX)
 * -----------------------------------------------------------
 * ADX > 25: Thị trường có xu hướng mạnh (Ưu tiên đánh Breakout)
 * ADX < 25: Thị trường đi ngang (Ưu tiên đánh Pullback/RSI hoặc đứng ngoài)
 */
export function classifyMarket(
    highs: number[],
    lows: number[],
    closes: number[],
    config: ClassifierConfig = {}
): 'UPTREND' | 'DOWNTREND' | 'SIDEWAY' {

    // 1. Khởi tạo ngưỡng (Mặc định ADX 25 là ngưỡng phân định Trend)
    const adxThreshold = config.adxThreshold || 25;

    // 2. Tính toán ADX từ dữ liệu nến
    // Library technicalindicators trả về mảng object: { adx: number, pdi: number, mdi: number }
    const adxResults = calcADX(highs, lows, closes, 14);

    // Phòng hờ trường hợp dữ liệu nến quá ít không tính được ADX
    if (adxResults.length === 0) {
        return 'SIDEWAY';
    }

    const lastADXData = adxResults[adxResults.length - 1];
    const currentADX = lastADXData.adx;

    // 3. LOGIC QUYẾT ĐỊNH

    // Trường hợp A: Xu hướng quá yếu (ADX < Threshold)
    if (currentADX < adxThreshold) {
        return 'SIDEWAY';
    }

    // Trường hợp B: Xu hướng mạnh (ADX >= Threshold)
    // Dựa vào +DI và -DI để biết là xu hướng Tăng hay Giảm
    // pdi (Plus Directional Indicator) > mdi (Minus Directional Indicator) => UPTREND
    if (lastADXData.pdi > lastADXData.mdi) {
        return 'UPTREND';
    } else if (lastADXData.mdi > lastADXData.pdi) {
        return 'DOWNTREND';
    }

    return 'SIDEWAY';
}