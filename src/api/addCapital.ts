import { NextApiRequest, NextApiResponse } from 'next';
import mongoose from 'mongoose';
import { Capital } from '../database/models/Capital';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
  const { amount } = req.body;
  if (typeof amount !== 'number' || isNaN(amount) || amount <= 0) {
    return res.status(400).json({ error: 'Invalid amount' });
  }
  await mongoose.connect(process.env.MONGODB_URI!);
  const cap = await Capital.findOneAndUpdate(
    { user: 'default' },
    { $inc: { amount }, $set: { updatedAt: new Date() } },
    { upsert: true, new: true }
  );
  res.json({ capital: cap.amount });
}
