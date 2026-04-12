// pricePredictor.ts - Dự báo giá đơn giản (Linear Regression placeholder)

/**
 * Dự báo giá tiếp theo bằng Linear Regression đơn giản
 * @param closes Mảng giá đóng cửa
 * @param lookback Số phiên lookback (mặc định 20)
 */
export function predictNextPrice(closes: number[], lookback = 20): number {
  if (closes.length < lookback) return closes[closes.length - 1];
  const x = Array.from({ length: lookback }, (_, i) => i);
  const y = closes.slice(-lookback);
  const n = lookback;
  const sumX = x.reduce((a, b) => a + b, 0);
  const sumY = y.reduce((a, b) => a + b, 0);
  const sumXY = x.reduce((a, b, i) => a + b * y[i], 0);
  const sumX2 = x.reduce((a, b) => a + b * b, 0);
  const slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
  const intercept = (sumY - slope * sumX) / n;
  return slope * n + intercept;
}
