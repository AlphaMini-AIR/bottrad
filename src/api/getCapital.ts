import { NextApiRequest, NextApiResponse } from 'next';
import mongoose from 'mongoose';
import { Capital } from '../database/models/Capital';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  await mongoose.connect(process.env.MONGODB_URI!);
  const cap = await Capital.findOne({ user: 'default' });
  res.json({ capital: cap ? cap.amount : 0 });
}
