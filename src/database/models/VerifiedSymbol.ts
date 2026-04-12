// src/database/models/VerifiedSymbol.ts
import mongoose, { Schema, Document } from 'mongoose';

export interface IVerifiedSymbol extends Document {
    symbol: string;
    recommendedStrategy: 'SWING' | 'SCALP' | 'BOTH' | 'NONE';
    winRate: number;
    lastTested: Date;
    status: 'ACTIVE' | 'PENDING' | 'REJECTED'; // Để Hưng có thể bật/tắt thủ công nếu muốn
}

const VerifiedSymbolSchema: Schema = new Schema({
    symbol: {
        type: String,
        required: true,
        unique: true,
        index: true
    },
    recommendedStrategy: {
        type: String,
        enum: ['SWING', 'SCALP', 'BOTH', 'NONE'],
        default: 'NONE'
    },
    winRate: {
        type: Number,
        required: true
    },
    lastTested: {
        type: Date,
        default: Date.now
    },
    status: {
        type: String,
        enum: ['ACTIVE', 'PENDING', 'REJECTED'],
        default: 'ACTIVE'
    }
});

export const VerifiedSymbol = mongoose.models.VerifiedSymbol || mongoose.model<IVerifiedSymbol>('VerifiedSymbol', VerifiedSymbolSchema);