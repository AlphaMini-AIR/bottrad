// src/engine/fastScalper.ts
import { EventEmitter } from 'events';
import { openPosition } from './positionManager';
import { circuitBreaker, apiRateLimiter } from './riskManager';

// Khởi tạo Event Bus để giao tiếp nội bộ không độ trễ
export const scalperEvents = new EventEmitter();

// Cache xu hướng thị trường (Được cập nhật mỗi 5 phút từ index.ts)
const marketTrendCache = new Map<string, 'LONG' | 'SHORT' | 'HOLD'>();

export function updateTrendCache(symbol: string, trend: 'LONG' | 'SHORT' | 'HOLD') {
    marketTrendCache.set(symbol, trend);
}

// Hàm khởi động bộ Scalper
export function startFastScalper() {
    console.log('[FastScalper] ⚡ Hệ thống Scalp Mili-giây đã sẵn sàng.');

    // Lắng nghe sự kiện "WHALE_DETECTED" từ luồng WebSocket
    scalperEvents.on('WHALE_DETECTED', async (data: { symbol: string, side: 'BUY' | 'SELL', usdVolume: number, price: number }) => {
        const { symbol, side, usdVolume, price } = data;

        // 1. Kiểm tra xu hướng gốc (4H) từ Cache (Tốc độ đọc RAM: 0.001 mili-giây)
        const currentTrend = marketTrendCache.get(symbol);

        // Chỉ đu theo Cá mập nếu nó đi CÙNG HƯỚNG với xu hướng dài hạn
        const isAligned = (side === 'BUY' && currentTrend === 'LONG') || (side === 'SELL' && currentTrend === 'SHORT');

        if (!isAligned) {
            console.log(`[FastScalper] ⚠️ Bỏ qua Whale ${side} ${symbol} ($${usdVolume.toFixed(0)}) vì ngược xu hướng ${currentTrend}.`);
            return;
        }

        // 2. Kiểm tra An toàn (Circuit Breaker)
        if (!circuitBreaker.canTrade().allowed || apiRateLimiter.getUsagePercent() > 90) return;

        // 3. THỰC THI LỆNH NGAY LẬP TỨC
        console.log(`[FastScalper] 🚀 BẮN LỆNH SCALP ${side} ${symbol} theo Whale Volume $${usdVolume.toFixed(0)}`);

        const quantity = 50 / price; // Scalp nhanh $50

        // Tạo một Analysis giả lập để lọt qua hàm openPosition
        const mockAnalysis = {
            signal: side === 'BUY' ? 'LONG' as const : 'SHORT' as const,
            score: 99, // Scalp có score tuyệt đối
            duration: 'FAST_SCALP' as const,
            predictedPath: [price * 1.005, price * 1.01, price * 1.015], // Cài TP siêu ngắn
            reason: `FAST SCALP: Whale ${side} $${usdVolume.toFixed(0)}`
        };

        // Bắn lệnh vào Hệ thống Quản lý Vị thế
        await openPosition(symbol, mockAnalysis.signal, price, quantity, mockAnalysis);
    });
}