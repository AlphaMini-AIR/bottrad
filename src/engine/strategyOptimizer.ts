// strategyOptimizer.ts - Tối ưu tham số chiến lược (bestK/grid search đơn giản)
import { backtestStrategy } from './backtest';

export async function gridSearch(paramsList: any[], strategyFn: Function, data: any) {
  let bestResult = null;
  let bestParams = null;
  for (const params of paramsList) {
    const result = await backtestStrategy(strategyFn, data, params);
    if (!bestResult || result.pnl > bestResult.pnl) {
      bestResult = result;
      bestParams = params;
    }
  }
  return { bestParams, bestResult };
}
