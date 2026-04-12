import { Schema, model } from 'mongoose';

const KlineSchema = new Schema({
  symbol:   { type: String, required: true },
  openTime: { type: Date, required: true },
  open:     { type: Number, required: true },
  high:     { type: Number, required: true },
  low:      { type: Number, required: true },
  close:    { type: Number, required: true },
  volume:   { type: Number, required: true },
});

// TTL Index: tự động xóa document sau 90 ngày (7776000 giây) tính từ openTime
KlineSchema.index(
  { openTime: 1 },
  { expireAfterSeconds: 7776000 }
);

// Index phụ trợ cho query theo symbol + thời gian
KlineSchema.index({ symbol: 1, openTime: -1 });

export const Kline = model('Kline', KlineSchema);
