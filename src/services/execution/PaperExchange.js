const IExchange = require('./IExchange');
const ScoutTrade = require('../../models/ScoutTrade'); 

class PaperExchange extends IExchange {
    constructor(initialCapital = 200) {
        super();
        this.walletBalance = initialCapital;
        this.activePositions = new Map();
        
        this.FIXED_MARGIN = 2.0; 
        this.FEE_MAKER = 0.0002; 
        this.FEE_TAKER = 0.0004; 
        this.MIN_NOTIONAL = 5.0; 
        
        // [FIX]: Đảm bảo biến MOCK_FUNDING_RATE được khởi tạo
        this.MOCK_FUNDING_RATE = 0.0001; 

        this.symbolConfigs = {
            'BTCUSDT': { maxLeverage: 125, maintenanceMarginRate: 0.004 },
            'ETHUSDT': { maxLeverage: 100, maintenanceMarginRate: 0.004 },
            'DEFAULT': { maxLeverage: 20, maintenanceMarginRate: 0.01 }
        };
    }

    getSymbolConfig(symbol) { return this.symbolConfigs[symbol] || this.symbolConfigs['DEFAULT']; }

    async openTrade(symbol, orderType, entryPrice, leverage, mockConfidence) {
        const config = this.getSymbolConfig(symbol);

        if (leverage > config.maxLeverage) return null; 
        const notionalValue = this.FIXED_MARGIN * leverage;
        if (notionalValue < this.MIN_NOTIONAL) return null; 
        if (this.walletBalance < this.FIXED_MARGIN) return null; 

        const positionSize = notionalValue / entryPrice;
        const feeRate = (orderType === 'LIMIT_FOK' || orderType === 'MARKET') ? this.FEE_TAKER : this.FEE_MAKER;
        const entryFee = notionalValue * feeRate;

        this.walletBalance -= entryFee;

        const maintMargin = notionalValue * config.maintenanceMarginRate;
        const liquidationPrice = entryPrice - (this.FIXED_MARGIN - maintMargin) / positionSize;

        this.activePositions.set(symbol, {
            entryPrice, size: positionSize, leverage, margin: this.FIXED_MARGIN, 
            orderType, entryFee,
            liquidationPrice: liquidationPrice > 0 ? liquidationPrice : 0,
            highestPrice: entryPrice, 
            lowestPrice: entryPrice,  
            openTime: Date.now()
        });

        console.log(`💰 [SÀN ẢO] Mở ${symbol} | Giá: ${entryPrice.toFixed(4)} | Đòn bẩy: ${leverage}x`);
        return true;
    }

    async closeTrade(symbol, closePrice, reason) {
        if (!this.activePositions.has(symbol)) return null;

        const position = this.activePositions.get(symbol);
        const exitNotional = position.size * closePrice;
        const exitFee = exitNotional * this.FEE_TAKER; 
        
        const grossPnL = (closePrice - position.entryPrice) * position.size;

        let fundingFeeDeducted = 0;
        const hoursHeld = (Date.now() - position.openTime) / (1000 * 60 * 60);
        if (hoursHeld >= 8) {
            fundingFeeDeducted = (exitNotional * this.MOCK_FUNDING_RATE) * Math.floor(hoursHeld / 8);
        }

        const netPnL = grossPnL - position.entryFee - exitFee - fundingFeeDeducted;
        this.walletBalance += (position.margin + netPnL);
        const roiPercent = (netPnL / position.margin) * 100;

        // [FIX]: Tính MAE & MFE chuẩn gốc (Bỏ nhân Đòn bẩy, phản ánh biến động giá thực tế)
        const mfePercent = ((position.highestPrice - position.entryPrice) / position.entryPrice) * 100;
        const maePercent = ((position.lowestPrice - position.entryPrice) / position.entryPrice) * 100;

        console.log(`💳 [SÀN ẢO] Đóng ${symbol} (${reason}) | Lãi: ${netPnL.toFixed(3)}$ | Ví: ${this.walletBalance.toFixed(2)}$`);

        // BẮN DỮ LIỆU XUỐNG MONGODB
        ScoutTrade.create({
            symbol: symbol,
            orderType: position.orderType,
            leverage: position.leverage,
            margin: position.margin,
            size: position.size,
            entryPrice: position.entryPrice,
            entryFee: position.entryFee,
            openTime: new Date(position.openTime),
            closePrice: closePrice,
            exitFee: exitFee,
            fundingFee: fundingFeeDeducted,
            reason: reason,
            pnl: netPnL,
            roi: roiPercent,
            mae: maePercent,
            mfe: mfePercent
        }).then(() => console.log(`💾 [MONGO] Đã lưu lịch sử lệnh ${symbol}`))
          .catch(err => console.error(`❌ [MONGO] Lỗi lưu lệnh:`, err.message));

        this.activePositions.delete(symbol);
        return { symbol, netPnL, roiPercent };
    }

    async updateTick(symbol, currentPrice) {
        if (!this.activePositions.has(symbol)) return;
        const position = this.activePositions.get(symbol);

        if (currentPrice > position.highestPrice) position.highestPrice = currentPrice;
        if (currentPrice < position.lowestPrice) position.lowestPrice = currentPrice;

        if (currentPrice <= position.liquidationPrice) {
            console.log(`💀 [LIQUIDATED] ${symbol} chạm giá thanh lý ${position.liquidationPrice.toFixed(4)}!`);
            await this.closeTrade(symbol, currentPrice, 'LIQUIDATED');
        }
    }

    getWalletBalance() { return this.walletBalance; }
}

module.exports = new PaperExchange(200);