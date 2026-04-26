const IExchange = require('./IExchange');
const ScoutTrade = require('../../models/ScoutTrade'); 
const Redis = require('ioredis');
const fs = require('fs');
const path = require('path');

// Lấy cấu hình Redis để bắn tín hiệu Dashboard
const configPath = path.join(__dirname, '../../../system_config.json');
let config;
try { config = JSON.parse(fs.readFileSync(configPath, 'utf8')); } 
catch (e) { config = { REDIS_URL: process.env.REDIS_URL || 'redis://localhost:6379' }; }

class PaperExchange extends IExchange {
    constructor(initialCapital = 200) {
        super();
        this.walletBalance = initialCapital;
        this.activePositions = new Map();
        
        // Khởi tạo Redis để báo cáo Real-time
        this.pubClient = new Redis(config.REDIS_URL);
        this.pubClient.on('error', () => {}); // Tắt log spam nếu rớt mạng
        
        this.FIXED_MARGIN = 2.0; 
        this.FEE_MAKER = 0.0002; 
        this.FEE_TAKER = 0.0004; 
        this.MIN_NOTIONAL = 5.0; 
        this.MOCK_FUNDING_RATE = 0.0001; 

        this.symbolConfigs = {
            'BTCUSDT': { maxLeverage: 125, maintenanceMarginRate: 0.004 },
            'ETHUSDT': { maxLeverage: 100, maintenanceMarginRate: 0.004 },
            'DEFAULT': { maxLeverage: 20, maintenanceMarginRate: 0.01 }
        };
    }

    getSymbolConfig(symbol) { return this.symbolConfigs[symbol] || this.symbolConfigs['DEFAULT']; }

    // [CẬP NHẬT 1]: Thêm tham số type và features để lưu ký ức cho AI
    async openTrade(symbol, orderType, entryPrice, leverage, mockConfidence, type = 'LONG', features = null) {
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
        const liquidationPrice = type === 'LONG' 
            ? entryPrice - (this.FIXED_MARGIN - maintMargin) / positionSize
            : entryPrice + (this.FIXED_MARGIN - maintMargin) / positionSize; // Chuẩn bị sẵn cho SHORT

        const newPosition = {
            entryPrice, size: positionSize, leverage, margin: this.FIXED_MARGIN, 
            orderType, entryFee, type, features, // Lưu lại ký ức
            liquidationPrice: liquidationPrice > 0 ? liquidationPrice : 0,
            highestPrice: entryPrice, lowestPrice: entryPrice,  
            openTime: Date.now()
        };

        this.activePositions.set(symbol, newPosition);
        console.log(`💰 [SÀN ẢO] Mở ${type} ${symbol} | Giá: ${entryPrice.toFixed(4)} | Đòn bẩy: ${leverage}x`);

        // [CẬP NHẬT 2]: Lưu trạng thái OPEN vào MongoDB ngay lập tức để Dashboard hiển thị
        try {
            const dbRecord = await ScoutTrade.create({
                symbol, type, orderType, status: 'OPEN',
                leverage, margin: this.FIXED_MARGIN, size: positionSize,
                entryPrice, entryFee, openTime: new Date(newPosition.openTime),
                features // Ký ức lúc bóp cò
            });
            newPosition.dbId = dbRecord._id; // Giữ lại ID để lúc đóng lệnh update cho nhanh
        } catch (err) {
            console.error(`❌ [MONGO] Lỗi lưu lệnh OPEN:`, err.message);
        }

        // Báo Dashboard tải lại bảng lệnh
        this.pubClient.publish('dashboard:trades', JSON.stringify({ action: 'UPDATE' }));
        return true;
    }

    // [CẬP NHẬT 3]: Nhận features dự phòng (nếu OrderManager muốn ghi đè)
    async closeTrade(symbol, closePrice, reason, overrideFeatures = null) {
        if (!this.activePositions.has(symbol)) return null;

        const position = this.activePositions.get(symbol);
        const exitNotional = position.size * closePrice;
        const exitFee = exitNotional * this.FEE_TAKER; 
        
        let grossPnL = 0;
        if (position.type === 'LONG') {
            grossPnL = (closePrice - position.entryPrice) * position.size;
        } else { // SHORT
            grossPnL = (position.entryPrice - closePrice) * position.size;
        }

        let fundingFeeDeducted = 0;
        const hoursHeld = (Date.now() - position.openTime) / (1000 * 60 * 60);
        if (hoursHeld >= 8) {
            fundingFeeDeducted = (exitNotional * this.MOCK_FUNDING_RATE) * Math.floor(hoursHeld / 8);
        }

        const netPnL = grossPnL - position.entryFee - exitFee - fundingFeeDeducted;
        this.walletBalance += (position.margin + netPnL);
        const roiPercent = (netPnL / position.margin) * 100;

        // [CẬP NHẬT 4]: Tính MAE & MFE chuẩn gốc cho cả LONG và SHORT
        let mfePercent = 0, maePercent = 0;
        if (position.type === 'LONG') {
            mfePercent = ((position.highestPrice - position.entryPrice) / position.entryPrice) * 100;
            maePercent = ((position.lowestPrice - position.entryPrice) / position.entryPrice) * 100;
        } else { // SHORT
            mfePercent = ((position.entryPrice - position.lowestPrice) / position.entryPrice) * 100;
            maePercent = ((position.entryPrice - position.highestPrice) / position.entryPrice) * 100;
        }

        const finalFeatures = overrideFeatures || position.features;

        console.log(`💳 [SÀN ẢO] Đóng ${symbol} (${reason}) | Lãi: ${netPnL.toFixed(3)}$ | Ví: ${this.walletBalance.toFixed(2)}$`);

        // UPDATE LỆNH TRONG MONGODB TỪ OPEN THÀNH CLOSED
        try {
            const updatePayload = {
                status: 'CLOSED', closePrice, exitFee, fundingFee: fundingFeeDeducted,
                closeTime: new Date(), reason, pnl: netPnL, roi: roiPercent, 
                mae: maePercent, mfe: mfePercent, features: finalFeatures
            };

            if (position.dbId) {
                await ScoutTrade.findByIdAndUpdate(position.dbId, updatePayload);
            } else {
                // Fallback nếu rớt mạng lúc mở lệnh không có ID
                await ScoutTrade.findOneAndUpdate(
                    { symbol, status: 'OPEN' }, 
                    updatePayload, 
                    { sort: { openTime: -1 } }
                );
            }
        } catch (err) {
            console.error(`❌ [MONGO] Lỗi update lệnh CLOSED:`, err.message);
        }

        this.activePositions.delete(symbol);
        
        // Báo Dashboard tải lại biểu đồ và danh sách lệnh
        this.pubClient.publish('dashboard:trades', JSON.stringify({ action: 'UPDATE' }));
        return { symbol, netPnL, roiPercent };
    }

    async updateTick(symbol, currentPrice) {
        if (!this.activePositions.has(symbol)) return;
        const position = this.activePositions.get(symbol);

        if (currentPrice > position.highestPrice) position.highestPrice = currentPrice;
        if (currentPrice < position.lowestPrice) position.lowestPrice = currentPrice;

        // Logic thanh lý hỗ trợ cả LONG và SHORT
        if (position.type === 'LONG' && currentPrice <= position.liquidationPrice) {
            console.log(`💀 [LIQUIDATED] ${symbol} chạm giá thanh lý ${position.liquidationPrice.toFixed(4)}!`);
            await this.closeTrade(symbol, currentPrice, 'LIQUIDATED');
        } else if (position.type === 'SHORT' && currentPrice >= position.liquidationPrice) {
            console.log(`💀 [LIQUIDATED] ${symbol} chạm giá thanh lý ${position.liquidationPrice.toFixed(4)}!`);
            await this.closeTrade(symbol, currentPrice, 'LIQUIDATED');
        }
    }

    getWalletBalance() { return this.walletBalance; }
    hasActivePosition(symbol) { return this.activePositions.has(symbol); }
}

module.exports = new PaperExchange(200);