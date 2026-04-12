import { NextApiRequest, NextApiResponse } from 'next';
import mongoose from 'mongoose';
import { Capital } from '../database/models/Capital';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  await mongoose.connect('mongodb+srv://assistantsupdev_db_user:rCp0BrUushhwIKR8@binance.bjmukc0.mongodb.net/?appName=Binance');
  const cap = await Capital.findOne({ user: 'default' });
  res.json({ capital: cap ? cap.amount : 0 });
}
