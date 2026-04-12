/**
 * Tìm K-Value tối ưu cho chiến thuật Volatility Breakout
 * Dựa trên pyupbit logic nhưng cải tiến theo phong cách AI của Hưng
 */
export function getOptimalK(klines: any[]): number {
    let bestK = 0.5;
    let maxProfit = -Infinity;

    for (let k = 0.4; k <= 0.6; k += 0.01) {
        let totalPnl = 0;
        for (let i = 1; i < klines.length; i++) {
            const range = klines[i - 1].high - klines[i - 1].low;
            const targetPrice = klines[i].open + (range * k);
            if (klines[i].high > targetPrice) {
                // Giả lập lợi nhuận nếu breakout
                totalPnl += (klines[i].close - targetPrice) / targetPrice;
            }
        }
        if (totalPnl > maxProfit) {
            maxProfit = totalPnl;
            bestK = k;
        }
    }
    return bestK;
}

/**
 * Tối ưu Position Size dựa trên rủi ro (Risk-based Sizing)
 */
export function calculateDynamicSize(balance: number, atr: number, price: number): number {
    const riskAmount = balance * 0.01; // Chấp nhận mất 1% tổng vốn nếu dính SL
    const stopLossDistance = atr * 1.5;
    const size = riskAmount / (stopLossDistance / price);
    return Math.min(size, balance * 0.5); // Không bao giờ quá 50% vốn cho 1 lệnh
}