import { Schema, model } from 'mongoose';

const PredictedCandleSchema = new Schema({
  price:     { type: Number, required: true },
  timestamp: { type: Number, required: true },
}, { _id: false });

const PaperTradeSchema = new Schema({
  symbol:           { type: String, required: true },
  side:             { type: String, enum: ['BUY', 'SELL'], required: true },
  entryPrice:       { type: Number, required: true },
  avgEntryPrice:    { type: Number, required: true },
  exitPrice:        { type: Number, default: 0 },
  quantity:         { type: Number, required: true },
  pnl:              { type: Number, default: 0 },
  reason:           { type: String, required: true },
  duration:         { type: String, enum: ['SWING', 'FAST_SCALP'], default: 'SWING' },
  score:            { type: Number, default: 0 },
  dcaCount:         { type: Number, default: 0 },
  predictedCandles: { type: [PredictedCandleSchema], default: [] },
  closedAt:         { type: Date, default: null },
  createdAt:        { type: Date, default: Date.now },
});

PaperTradeSchema.index({ symbol: 1, createdAt: -1 });

export const PaperTrade = model('PaperTrade', PaperTradeSchema);
