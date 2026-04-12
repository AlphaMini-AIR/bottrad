// src/engine/strategyEngine.ts - Multi-strategy, multi-timeframe engine
import { backtestStrategy, BacktestResult } from './backtest';

// Generic type cho params để linh hoạt hơn
export interface StrategyConfig<T = Record<string, unknown>> {
    name: string;
    fn: Function; // Có thể trỏ tới type cụ thể của function chiến lược nếu bạn đã định nghĩa
    params: T;
    timeframe: string;
}

export class StrategyEngine {
    strategies: StrategyConfig[] = [];

    addStrategy(config: StrategyConfig) {
        this.strategies.push(config);
    }

    async runAll(dataByTimeframe: Record<string, any[]>): Promise<Record<string, BacktestResult>> {
        const results: Record<string, BacktestResult> = {};

        for (const strat of this.strategies) {
            const data = dataByTimeframe[strat.timeframe];

            // Chặn và log cảnh báo nếu không có dữ liệu cho timeframe được yêu cầu
            if (!data || data.length === 0) {
                console.warn(`[StrategyEngine] Bỏ qua ${strat.name}: Không có dữ liệu cho timeframe ${strat.timeframe}`);
                continue;
            }

            try {
                results[strat.name] = await backtestStrategy(strat.fn, data, strat.params);
            } catch (error) {
                console.error(`[StrategyEngine] Lỗi khi chạy backtest cho ${strat.name}:`, error);
            }
        }

        return results;
    }
}