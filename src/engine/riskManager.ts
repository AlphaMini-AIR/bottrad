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
  private lastReset: number = Date.now();

  constructor(
    private totalCapital: number,
    private maxConsecutiveLosses: number = 5,
    private maxDailyLossPercent: number = 3,
    private maxDailyTrades: number = 100,
  ) {}

  recordTrade(pnlUsdt: number): void {
    this.checkDayReset();
    this.dailyTrades++;
    if (pnlUsdt < 0) {
      this.consecutiveLosses++;
      this.dailyLossUsdt += Math.abs(pnlUsdt);
    } else {
      this.consecutiveLosses = 0;
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
    const now = Date.now();
    const msPerDay = 24 * 60 * 60 * 1000;
    if (now - this.lastReset >= msPerDay) {
      this.dailyLossUsdt = 0;
      this.dailyTrades = 0;
      this.consecutiveLosses = 0;
      this.lastReset = now;
      if (this.tripped) {
        this.tripped = false;
        this.tripReason = '';
        console.log('[CircuitBreaker] Daily auto-reset');
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

// Circuit breaker: $1000 paper capital, conservative limits
export const circuitBreaker = new CircuitBreaker(1000, 5, 3, 100);
