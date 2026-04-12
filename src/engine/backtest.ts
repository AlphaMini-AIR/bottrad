// src/engine/backtest.ts - Backtest nâng cao cho chiến lược
import fs from 'fs';

export interface BacktestTrade {
    time: string;
    symbol: string;
    side: 'LONG' | 'SHORT';
    entry: number;
    exit: number;
    quantity: number;
    pnl: number;
}

export interface BacktestResult {
    trades: BacktestTrade[];
    pnl: number;
    win: number;
    loss: number;
    breakeven: number; // BỔ SUNG: Tính lệnh hòa vốn
    winrate: number;
    maxDrawdown: number;
    equityCurve: number[];
}

export async function backtestStrategy(strategyFn: Function, data: any[], params: any): Promise<BacktestResult> {
    let pnl = 0;
    let win = 0;
    let loss = 0;
    let breakeven = 0;

    let equity = 10000; // giả lập vốn ban đầu $10,000
    let peak = equity;
    let maxDrawdown = 0;

    const trades: BacktestTrade[] = [];
    const equityCurve: number[] = [equity];

    for (const point of data) {
        const trade = await strategyFn(point, params);

        if (trade && trade.pnl !== undefined) {
            pnl += trade.pnl;
            equity += trade.pnl;

            // SỬA LỖI LOGIC: Tách riêng Win, Loss và Hòa vốn (Breakeven)
            if (trade.pnl > 0) win++;
            else if (trade.pnl < 0) loss++;
            else breakeven++;

            trades.push({
                time: trade.time || '',
                symbol: trade.symbol || '',
                side: trade.side || 'LONG',
                entry: trade.entry || 0,
                exit: trade.exit || 0,
                quantity: trade.quantity || 0,
                pnl: trade.pnl,
            });

            // Tính toán Max Drawdown (Sụt giảm tài khoản lớn nhất)
            if (equity > peak) {
                peak = equity;
            }
            const dd = ((peak - equity) / peak) * 100;
            if (dd > maxDrawdown) {
                maxDrawdown = dd;
            }

            equityCurve.push(equity);
        }
    }

    // Winrate chỉ tính trên tổng số lệnh thực sự Thắng hoặc Thua (bỏ qua hòa vốn)
    const totalDecisiveTrades = win + loss;

    return {
        trades,
        pnl,
        win,
        loss,
        breakeven,
        winrate: totalDecisiveTrades > 0 ? win / totalDecisiveTrades : 0,
        maxDrawdown,
        equityCurve,
    };
}

export function exportBacktestToCSV(result: BacktestResult, filePath: string) {
    const header = 'time,symbol,side,entry,exit,quantity,pnl\n';
    const rows = result.trades.map(t => `${t.time},${t.symbol},${t.side},${t.entry},${t.exit},${t.quantity},${t.pnl}`);
    fs.writeFileSync(filePath, header + rows.join('\n'));
}

export function exportEquityCurveToCSV(result: BacktestResult, filePath: string) {
    const header = 'step,equity\n';
    const rows = result.equityCurve.map((eq, i) => `${i},${eq}`);
    fs.writeFileSync(filePath, header + rows.join('\n'));
}