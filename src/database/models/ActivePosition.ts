import { Schema, model, type Document } from 'mongoose';

// ─── INTERFACES ────────────────────────────────────────────────────────────────

export interface DcaEntry {
  price: number;
  quantity: number;
  timestamp: Date;
}

export interface PredictedCandle {
  price: number;
  timestamp: number;
}

export interface IActivePosition extends Document {
  symbol: string;
  side: 'LONG' | 'SHORT';
  entryPrice: number;
  avgEntryPrice: number;
  totalQuantity: number;
  dcaEntries: DcaEntry[];
  dcaCount: number;
  duration: 'SWING' | 'FAST_SCALP';
  score: number;
  predictedCandles: PredictedCandle[];
  reason: string;
  status: 'OPEN' | 'CLOSED';
  closedAt: Date | null;
  exitPrice: number;
  pnl: number;
  closeReason: string;
  createdAt: Date;
  updatedAt: Date;
}

// ─── SCHEMA ────────────────────────────────────────────────────────────────────

const DcaEntrySchema = new Schema({
  price:     { type: Number, required: true },
  quantity:  { type: Number, required: true },
  timestamp: { type: Date, default: Date.now },
}, { _id: false });

const PredictedCandleSchema = new Schema({
  price:     { type: Number, required: true },
  timestamp: { type: Number, required: true },
}, { _id: false });

const ActivePositionSchema = new Schema({
  symbol:           { type: String, required: true },
  side:             { type: String, enum: ['LONG', 'SHORT'], required: true },
  entryPrice:       { type: Number, required: true },
  avgEntryPrice:    { type: Number, required: true },
  totalQuantity:    { type: Number, required: true },
  dcaEntries:       { type: [DcaEntrySchema], default: [] },
  dcaCount:         { type: Number, default: 0 },
  duration:         { type: String, enum: ['SWING', 'FAST_SCALP'], default: 'SWING' },
  score:            { type: Number, default: 0 },
  predictedCandles: { type: [PredictedCandleSchema], default: [] },
  reason:           { type: String, required: true },
  status:           { type: String, enum: ['OPEN', 'CLOSED'], default: 'OPEN' },
  closedAt:         { type: Date, default: null },
  exitPrice:        { type: Number, default: 0 },
  pnl:              { type: Number, default: 0 },
  closeReason:      { type: String, default: '' },
}, {
  timestamps: true, // createdAt + updatedAt tự động
});

// Index: query vị thế đang mở theo symbol
ActivePositionSchema.index({ symbol: 1, status: 1 });
ActivePositionSchema.index({ status: 1, createdAt: -1 });

export const ActivePosition = model<IActivePosition>('ActivePosition', ActivePositionSchema);
