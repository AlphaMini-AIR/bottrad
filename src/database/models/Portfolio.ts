import mongoose, { Schema, Document } from 'mongoose';

export interface IPortfolioPosition {
  symbol: string;
  quantity: number;
  avgEntryPrice: number;
  invested: number;
  realizedPnl: number;
  unrealizedPnl: number;
  updatedAt: Date;
}

export interface IPortfolio extends Document {
  user: string;
  positions: IPortfolioPosition[];
  totalInvested: number;
  totalUnrealizedPnl: number;
  totalRealizedPnl: number;
  updatedAt: Date;
}

const PortfolioPositionSchema = new Schema<IPortfolioPosition>({
  symbol: { type: String, required: true },
  quantity: { type: Number, required: true },
  avgEntryPrice: { type: Number, required: true },
  invested: { type: Number, required: true },
  realizedPnl: { type: Number, required: true },
  unrealizedPnl: { type: Number, required: true },
  updatedAt: { type: Date, required: true },
});

const PortfolioSchema = new Schema<IPortfolio>({
  user: { type: String, required: true },
  positions: { type: [PortfolioPositionSchema], default: [] },
  totalInvested: { type: Number, required: true },
  totalUnrealizedPnl: { type: Number, required: true },
  totalRealizedPnl: { type: Number, required: true },
  updatedAt: { type: Date, required: true },
});

export const Portfolio = mongoose.models.Portfolio || mongoose.model<IPortfolio>('Portfolio', PortfolioSchema);
