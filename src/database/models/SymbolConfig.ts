import { Schema, model, type Document } from 'mongoose';

export interface ISymbolConfig extends Document {
    symbol: string;
    optimalRsi: {
        oversold: number;
        overbought: number;
    };
    dcaDropPercent: number;
    maxDca: number;
    takeProfitPercent: number;
    fastScalpTpPercent: number;
    backtestPnl: number;
    backtestDrawdown: number;
    backtestTrades: number;
    backtestWinRate: number;
    lastLearned: Date;
}

const SymbolConfigSchema = new Schema({
    // unique: true ở đây đã tự động tạo index rồi
    symbol: { type: String, required: true, unique: true },
    optimalRsi: {
        oversold: { type: Number, required: true, default: 30 },
        overbought: { type: Number, required: true, default: 70 },
    },
    dcaDropPercent: { type: Number, required: true, default: 2 },
    maxDca: { type: Number, required: true, default: 3 },
    takeProfitPercent: { type: Number, required: true, default: 3 },
    fastScalpTpPercent: { type: Number, required: true, default: 1 },
    backtestPnl: { type: Number, default: 0 },
    backtestDrawdown: { type: Number, default: 0 },
    backtestTrades: { type: Number, default: 0 },
    backtestWinRate: { type: Number, default: 0 },
    lastLearned: { type: Date, default: null },
});

// XÓA BỎ DÒNG NÀY ĐỂ TRÁNH TRÙNG LẶP INDEX
// SymbolConfigSchema.index({ symbol: 1 }, { unique: true });

export const SymbolConfig = model<ISymbolConfig>('SymbolConfig', SymbolConfigSchema);