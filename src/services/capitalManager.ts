import mongoose from 'mongoose';
import { Capital } from '../database/models/Capital';

// Taker fee mặc định trên Binance Futures là 0.05%, Maker fee 0.02%
const TAKER_FEE = 0.0005;
const MAKER_FEE = 0.0002;

// Trừ phí entry khi mở lệnh
export async function updateCapitalOnOpen(quantity: number, entry: number, isMaker = false) {
  await mongoose.connect(process.env.MONGODB_URI!);
  const fee = entry * quantity * (isMaker ? MAKER_FEE : TAKER_FEE);
  const cap = await Capital.findOneAndUpdate(
    { user: 'default' },
    { $inc: { amount: -fee }, $set: { updatedAt: new Date() } },
    { upsert: true, new: true }
  );
  return { capital: cap.amount, fee };
}

// Trừ phí exit và cộng/trừ PnL khi đóng lệnh
export async function updateCapitalOnClose(pnl: number, quantity: number, exit: number, isMaker = false) {
  await mongoose.connect(process.env.MONGODB_URI!);
  const fee = exit * quantity * (isMaker ? MAKER_FEE : TAKER_FEE);
  const net = pnl - fee;
  const cap = await Capital.findOneAndUpdate(
    { user: 'default' },
    { $inc: { amount: net }, $set: { updatedAt: new Date() } },
    { upsert: true, new: true }
  );
  return { capital: cap.amount, fee, net };
}
