// src/database/models/SymbolConfig.ts
import mongoose, { Schema, Document } from 'mongoose';

export interface ISymbolConfig extends Document {
  symbol: string;
  optimalRsi: {
    oversold: number;
    overbought: number;
  };
  optimalK: number; // 🟢 Bổ sung trường này vào Interface
  classifierConfig?: {
    bbThreshold: number;
    adxThreshold: number;
    minVolMultiplier: number;
  };
  backtestWinRate?: number;
  dcaDropPercent: number;
  maxDca: number;
  takeProfitPercent: number;
  fastScalpTpPercent: number;
  lastLearned: Date;
}

const SymbolConfigSchema: Schema = new Schema({
  symbol: { type: String, required: true, unique: true, index: true },
  optimalRsi: {
    oversold: { type: Number, default: 30 },
    overbought: { type: Number, default: 70 }
  },
  optimalK: { type: Number, default: 0.5 }, // 🟢 Bổ sung trường này vào Schema
  classifierConfig: {
    bbThreshold: { type: Number, default: 0.02 },
    adxThreshold: { type: Number, default: 25 },
    minVolMultiplier: { type: Number, default: 8 }
  },
  backtestWinRate: { type: Number },
  dcaDropPercent: { type: Number, default: 2.5 },
  maxDca: { type: Number, default: 2 },
  takeProfitPercent: { type: Number, default: 2.0 },
  fastScalpTpPercent: { type: Number, default: 1.0 },
  lastLearned: { type: Date, default: Date.now }
});

export const SymbolConfig = mongoose.model<ISymbolConfig>('SymbolConfig', SymbolConfigSchema);