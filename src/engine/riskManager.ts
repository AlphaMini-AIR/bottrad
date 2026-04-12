// src/engine/riskManager.ts

// ─── QUẢN LÝ VỐN/RỦI RO ĐỘNG ───────────────────────────────────────────────

/**
 * Tính khối lượng lệnh theo % vốn và mức rủi ro tối đa cho mỗi lệnh
 * @param capital Tổng vốn hiện tại
 * @param riskPercent % rủi ro tối đa cho mỗi lệnh (ví dụ 1 = 1%)
 * @param stopLossPercent % stop-loss so với entry (ví dụ 2 = 2%)
 * @param maxCapitalUsagePercent % vốn tối đa được phép dùng cho 1 lệnh (Mặc định 100% - không margin chéo)
 */
export function calcPositionSize(
    capital: number,
    riskPercent: number,
    stopLossPercent: number,
    maxCapitalUsagePercent: number = 100
): number {
    if (stopLossPercent <= 0) return 0;

    // Số tiền tối đa có thể mất cho 1 lệnh
    const maxLoss = capital * (riskPercent / 100);

    // Khối lượng lệnh lý thuyết
    const theoreticalSize = maxLoss / (stopLossPercent / 100);

    // Giới hạn khối lượng không vượt quá % vốn cho phép (Chống cháy do SL quá hẹp sinh ra đòn bẩy lớn)
    const maxAllowedSize = capital * (maxCapitalUsagePercent / 100);

    return Math.min(theoreticalSize, maxAllowedSize);
}

/**
 * Kiểm tra drawdown hiện tại
 * @param peakCapital Vốn cao nhất
 * @param currentCapital Vốn hiện tại
 */
export function calcDrawdown(peakCapital: number, currentCapital: number): number {
    if (peakCapital <= 0) return 0;
    return ((peakCapital - currentCapital) / peakCapital) * 100;
}

// ─── RATE LIMITER (Token Bucket) ───────────────────────────────────────────────

export class RateLimiter {
    private tokens: number;
    private lastRefill: number;

    constructor(
        private maxTokens: number,
        private refillPerSecond: number,
    ) {
        this.tokens = maxTokens;
        this.lastRefill = Date.now();
    }

    private refill(): void {
        const now = Date.now();
        const elapsed = (now - this.lastRefill) / 1000;
        this.tokens = Math.min(this.maxTokens, this.tokens + elapsed * this.refillPerSecond);
        this.lastRefill = now;
    }

    consume(weight: number = 1): boolean {
        this.refill();
        if (this.tokens >= weight) {
            this.tokens -= weight;
            return true;
        }
        return false;
    }

    async waitAndConsume(weight: number = 1): Promise<void> {
        while (!this.consume(weight)) {
            await new Promise(r => setTimeout(r, 100));
        }
    }

    getUsagePercent(): number {
        this.refill();
        return ((this.maxTokens - this.tokens) / this.maxTokens) * 100;
    }

    getRemainingTokens(): number {
        this.refill();
        return Math.floor(this.tokens);
    }
}

// ─── CIRCUIT BREAKER ───────────────────────────────────────────────────────────

export interface CircuitBreakerStatus {
    isTripped: boolean;
    reason: string;
    consecutiveLosses: number;
    dailyLossPercent: number;
    dailyTrades: number;
    lastTripped: string | null;
}

export class CircuitBreaker {
    private consecutiveLosses = 0;
    private dailyLossUsdt = 0;
    private dailyTrades = 0;
    private tripped = false;
    private tripReason = '';
    private lastTripped: Date | null = null;

    // Dùng chuỗi YYYY-MM-DD (UTC) để đồng bộ với reset nến D1 của sàn
    private currentUtcDay: string = new Date().toISOString().split('T')[0];

    constructor(
        private totalCapital: number,
        private maxConsecutiveLosses: number = 5,
        private maxDailyLossPercent: number = 3,
        private maxDailyTrades: number = 100,
    ) { }

    // BỔ SUNG: Cho phép hệ thống cập nhật vốn thực tế sau mỗi lệnh
    updateCapital(newCapital: number): void {
        this.totalCapital = newCapital;
    }

    recordTrade(pnlUsdt: number): void {
        this.checkDayReset();
        this.dailyTrades++;
        if (pnlUsdt < 0) {
            this.consecutiveLosses++;
            this.dailyLossUsdt += Math.abs(pnlUsdt);
        } else {
            this.consecutiveLosses = 0; // Thắng 1 lệnh là reset chuỗi thua
        }
        this.evaluate();
    }

    canTrade(): { allowed: boolean; reason: string } {
        this.checkDayReset();
        if (this.tripped) {
            return { allowed: false, reason: this.tripReason };
        }
        return { allowed: true, reason: '' };
    }

    reset(): void {
        this.tripped = false;
        this.tripReason = '';
        console.log('[CircuitBreaker] Manually reset');
    }

    getStatus(): CircuitBreakerStatus {
        this.checkDayReset();
        return {
            isTripped: this.tripped,
            reason: this.tripReason,
            consecutiveLosses: this.consecutiveLosses,
            dailyLossPercent: this.totalCapital > 0 ? (this.dailyLossUsdt / this.totalCapital) * 100 : 0,
            dailyTrades: this.dailyTrades,
            lastTripped: this.lastTripped?.toISOString() ?? null,
        };
    }

    private evaluate(): void {
        if (this.consecutiveLosses >= this.maxConsecutiveLosses) {
            this.trip(`${this.consecutiveLosses} thua liên tiếp (max: ${this.maxConsecutiveLosses})`);
        }
        const dailyLossPercent = this.totalCapital > 0 ? (this.dailyLossUsdt / this.totalCapital) * 100 : 0;
        if (dailyLossPercent >= this.maxDailyLossPercent) {
            this.trip(`Lỗ trong ngày ${dailyLossPercent.toFixed(2)}% (max: ${this.maxDailyLossPercent}%)`);
        }
        if (this.dailyTrades >= this.maxDailyTrades) {
            this.trip(`Đạt ${this.dailyTrades} lệnh/ngày (max: ${this.maxDailyTrades})`);
        }
    }

    private trip(reason: string): void {
        this.tripped = true;
        this.tripReason = reason;
        this.lastTripped = new Date();
        console.warn(`[CircuitBreaker] 🛑 TRIPPED: ${reason}`);
    }

    private checkDayReset(): void {
        // Lấy ngày hiện tại theo chuẩn UTC
        const todayUtc = new Date().toISOString().split('T')[0];

        if (this.currentUtcDay !== todayUtc) {
            this.dailyLossUsdt = 0;
            this.dailyTrades = 0;
            this.consecutiveLosses = 0;
            this.currentUtcDay = todayUtc;

            if (this.tripped) {
                this.tripped = false;
                this.tripReason = '';
                console.log(`[CircuitBreaker] Daily auto-reset triggered for UTC day: ${todayUtc}`);
            }
        }
    }
}

// ─── CONCURRENCY LIMITER ───────────────────────────────────────────────────────

export async function runWithConcurrency<T>(
    tasks: Array<() => Promise<T>>,
    maxConcurrent: number,
): Promise<PromiseSettledResult<T>[]> {
    const results: PromiseSettledResult<T>[] = [];
    const executing: Set<Promise<void>> = new Set();

    for (const task of tasks) {
        const p = task()
            .then((value) => {
                results.push({ status: 'fulfilled', value });
            })
            .catch((reason) => {
                results.push({ status: 'rejected', reason });
            })
            .finally(() => {
                executing.delete(p);
            });

        executing.add(p);
        if (executing.size >= maxConcurrent) {
            await Promise.race(executing);
        }
    }

    await Promise.all(executing);
    return results;
}

// ─── SINGLETON INSTANCES ───────────────────────────────────────────────────────

// Binance REST API: 2400 weight/min → refill 40/sec
export const apiRateLimiter = new RateLimiter(2400, 40);

// Circuit breaker: Khởi tạo với vốn mặc định (Nên gọi updateCapital() lúc bot boot lên)
export const circuitBreaker = new CircuitBreaker(1000, 5, 3, 100);