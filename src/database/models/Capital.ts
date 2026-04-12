import mongoose, { Schema, Document } from 'mongoose';

export interface ICapital extends Document {
  user: string; // hoặc "default" nếu chỉ có 1 bot
  amount: number;
  updatedAt: Date;
}

const CapitalSchema = new Schema<ICapital>({
  user: { type: String, required: true, default: 'default', unique: true },
  amount: { type: Number, required: true, default: 10000 }, // mặc định 10,000 USDT
  updatedAt: { type: Date, default: Date.now },
});

export const Capital = mongoose.models.Capital || mongoose.model<ICapital>('Capital', CapitalSchema);
