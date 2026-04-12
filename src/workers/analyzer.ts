import { parentPort } from 'worker_threads';

interface KlineMessage {
    symbol: string;
    open: number;
    high: number;
    low: number;
    close: number;
    volume: number;
    openTime: Date;
}

if (parentPort) {
    parentPort.on('message', (data: KlineMessage) => {
        try {
            // Phân tích đơn giản: nến tăng → BUY, nến giảm → SELL, sideways → HOLD
            const { symbol, open, close, volume } = data;
            let action: 'BUY' | 'SELL' | 'HOLD' = 'HOLD';

            const changePercent = ((close - open) / open) * 100;

            if (changePercent > 0.05 && volume > 0) {
                action = 'BUY';
            } else if (changePercent < -0.05 && volume > 0) {
                action = 'SELL';
            }

            console.log(`[Worker] ${symbol} change: ${changePercent.toFixed(3)}% → ${action}`);
            parentPort?.postMessage({ action, symbol });
        } catch (err: unknown) {
            const message = err instanceof Error ? err.message : String(err);
            console.error(`[Worker] Error: ${message}`);
            parentPort?.postMessage({ action: 'HOLD', symbol: data?.symbol || 'UNKNOWN' });
        }
    });
}