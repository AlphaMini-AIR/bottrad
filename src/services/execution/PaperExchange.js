
/**
 * src/services/execution/PaperExchange.js - V17.0 (Multi-DB Sniper Execution + Trajectory Tracker)
 * Nhiệm vụ: Quản lý vốn, theo dõi lệnh, và chốt lời/cắt lỗ (Bao gồm Trailing Stop).
 * Cập nhật V17: Ghi lại Quỹ đạo giá (Price Trajectory) để phục vụ việc Gán nhãn lại (Dynamic Re-labeling) ở Python.
 */const { getDbConnection } = require('../../config/db');const PaperTradeSchema = require('../../models/PaperTrade');const PaperAccountSchema = require('../../models/PaperAccount');const fs = require('fs');const path = require('path');const TIER_LIST_PATH = path.join(__dirname, '../../../tier_list.json');class PaperExchange {
    constructor() {
        this.activeTrades = new Map();
        this.TRADE_AMOUNT = 2.0;
        this.FEE_RATE = 0.0004;
    }

    /**
     * Hàm Helper: Lấy Model Account (Vốn tổng luôn nằm ở TIER-1 để dễ quản lý)
     */
    getAccountModel() {
        const conn = getDbConnection('tier1');
        return conn.model('PaperAccount', PaperAccountSchema);
    }

    /**
     * Hàm Helper: Lấy Model Trade (Lưu lệnh vào đúng DB chứa Coin đó)
     */
    getTradeModel(symbol) {
        let storageNode = 'tier1';
        try {
            if (fs.existsSync(TIER_LIST_PATH)) {
                const tierData = JSON.parse(fs.readFileSync(TIER_LIST_PATH, 'utf-8'));
                storageNode = tierData.storage_map[symbol] || 'scout';
            }
        } catch (e) {
            console.error('⚠️ [PaperExchange] Lỗi đọc storage_map, mặc định dùng Tier1');
        }
        const conn = getDbConnection(storageNode);
        return conn.model('PaperTrade', PaperTradeSchema);
    }

    async initAccount() {
        try {
            const AccountModel = this.getAccountModel();
            let acc = await AccountModel.findOne({ accountId: 'MAIN_PAPER' });

            if (!acc) {
                acc = await AccountModel.create({
                    accountId: 'MAIN_PAPER',
                    balance: 1000.0,
                    initialBalance: 1000.0
                });
                console.log('💰 [PaperExchange] Đã khởi tạo quỹ 1000$ tại DB TIER-1.');
            } else {
                console.log(`💰 [PaperExchange] Vốn hiện tại: ${acc.balance.toFixed(2)}$`);
            }
            this.account = acc;
        } catch (err) {
            console.error('❌ [PaperExchange] Lỗi khởi tạo tài khoản:', err.message);
        }
    }

    async openTrade(symbol, side, limitPrice, slPrice, tpPrice, trailingParams, prob, reason, features) {
        if (this.activeTrades.has(symbol)) return;

        const tradeSize = this.TRADE_AMOUNT / limitPrice;

        // Xác định DB vật lý để sau này đóng lệnh biết đường tìm
        let storageNode = 'tier1';
        try {
            const tierData = JSON.parse(fs.readFileSync(TIER_LIST_PATH, 'utf-8'));
            storageNode = tierData.storage_map[symbol] || 'scout';
        } catch (e) { }

        const newTrade = {
            symbol,
            side,
            entryPrice: limitPrice,
            slPrice,
            tpPrice,
            trailingParams,
            size: tradeSize,
            margin: this.TRADE_AMOUNT,
            status: 'OPEN',
            isTrailingActive: false,
            
            extremePrice: limitPrice, // Biến cũ phục vụ logic Trailing Stop

            // 🟢 TÍCH HỢP MỚI: Khởi tạo giá trị theo dõi Quỹ đạo giá (Price Trajectory)
            highestSinceOpen: limitPrice, // Đỉnh quỹ đạo
            lowestSinceOpen: limitPrice,  // Đáy quỹ đạo
            
            prob,
            reason,
            features, // Lưu features 13 thông số vào RAM để lúc đóng lệnh đẩy xuống DB
            storageNode,
            openTime: Date.now()
        };

        this.activeTrades.set(symbol, newTrade);
        console.log(`🔫 [BẮN TỈA] MỞ LỆNH ${side} ${symbol} | Giá: ${limitPrice} | DB: ${storageNode.toUpperCase()}`);
    }

    async monitorTrades() {
        if (this.activeTrades.size === 0) return;

        for (const [symbol, trade] of this.activeTrades.entries()) {
            const currentPrice = global.liveMicroData?.[symbol]?.close || global.currentMarkPrice?.[symbol];
            if (!currentPrice) continue;

            let isClosed = false;
            let closePrice = 0;
            let closeReason = '';

            // 🟢 TÍCH HỢP MỚI: CẬP NHẬT QUỸ ĐẠO GIÁ (O(1) Complexity)
            // Ghi lại liên tục xem trong lúc lệnh mở, giá đã từng leo cao nhất/thấp nhất là bao nhiêu
            if (currentPrice > trade.highestSinceOpen) trade.highestSinceOpen = currentPrice;
            if (currentPrice < trade.lowestSinceOpen) trade.lowestSinceOpen = currentPrice;

            // 1. CẬP NHẬT ĐỈNH/ĐÁY CHO TRAILING STOP
            if (trade.side === 'LONG') {
                if (currentPrice > trade.extremePrice) trade.extremePrice = currentPrice;
            } else {
                if (currentPrice < trade.extremePrice && currentPrice > 0) trade.extremePrice = currentPrice;
            }

            // 2. KÍCH HOẠT TRAILING STOP
            if (!trade.isTrailingActive) {
                if (trade.side === 'LONG' && currentPrice >= trade.trailingParams.activationPrice) {
                    trade.isTrailingActive = true;
                    console.log(`🔥 [TRAILING] ${symbol} kích hoạt bám đuổi!`);
                } else if (trade.side === 'SHORT' && currentPrice <= trade.trailingParams.activationPrice) {
                    trade.isTrailingActive = true;
                    console.log(`🔥 [TRAILING] ${symbol} kích hoạt bám đuổi!`);
                }
            }

            // 3. LOGIC CHỐT LỜI / CẮT LỖ
            if (trade.side === 'LONG') {
                if (currentPrice <= trade.slPrice) {
                    isClosed = true; closePrice = currentPrice; closeReason = 'HARD_SL';
                } else if (currentPrice >= trade.tpPrice && !trade.isTrailingActive) {
                    isClosed = true; closePrice = currentPrice; closeReason = 'HARD_TP';
                } else if (trade.isTrailingActive) {
                    const dynamicSL = trade.extremePrice * (1 - trade.trailingParams.callbackRate / 100);
                    if (currentPrice <= dynamicSL) {
                        isClosed = true; closePrice = currentPrice; closeReason = 'TRAILING_STOP';
                    }
                }
            } else {
                if (currentPrice >= trade.slPrice) {
                    isClosed = true; closePrice = currentPrice; closeReason = 'HARD_SL';
                } else if (currentPrice <= trade.tpPrice && !trade.isTrailingActive) {
                    isClosed = true; closePrice = currentPrice; closeReason = 'HARD_TP';
                } else if (trade.isTrailingActive) {
                    const dynamicSL = trade.extremePrice * (1 + trade.trailingParams.callbackRate / 100);
                    if (currentPrice >= dynamicSL) {
                        isClosed = true; closePrice = currentPrice; closeReason = 'TRAILING_STOP';
                    }
                }
            }

            if (isClosed) {
                await this.closeTrade(symbol, trade, closePrice, closeReason);
            }
        }
    }

    async closeTrade(symbol, trade, closePrice, closeReason) {
        this.activeTrades.delete(symbol);

        let pnl = (trade.side === 'LONG')
            ? (closePrice - trade.entryPrice) * trade.size
            : (trade.entryPrice - closePrice) * trade.size;

        const fee = (trade.entryPrice * trade.size * this.FEE_RATE) + (closePrice * trade.size * this.FEE_RATE);
        const netPnl = pnl - fee;

        try {
            // Lưu vào đúng DB vật lý của Coin
            const TradeModel = this.getTradeModel(symbol);
            const record = new TradeModel({
                symbol: trade.symbol,
                side: trade.side,
                entryPrice: trade.entryPrice,
                closePrice: closePrice,
                margin: trade.margin,
                netPnl: netPnl,
                outcome: netPnl > 0 ? 'WIN' : 'LOSS',
                closeReason: closeReason,

                // 🟢 TÍCH HỢP MỚI: Đẩy thông tin Quỹ đạo giá xuống Database
                // Dữ liệu này là nguồn sống để Python ban đêm quyết định Gán nhãn lại (Dynamic Relabeling)
                highestDuringTrade: trade.highestSinceOpen,
                lowestDuringTrade: trade.lowestSinceOpen,

                slPrice: trade.slPrice,
                tpPrice: trade.tpPrice,
                winProb: trade.prob,
                aiFeatures: trade.features,

                openTime: trade.openTime,
                closeTime: Date.now()
            });
            await record.save();

            // Cập nhật ví tổng ở TIER-1
            const AccountModel = this.getAccountModel();
            await AccountModel.updateOne(
                { accountId: 'MAIN_PAPER' },
                { $inc: { balance: netPnl } }
            );

            console.log(`${netPnl > 0 ? '✅' : '❌'} [ĐÓNG] ${symbol} | PnL: ${netPnl.toFixed(4)}$ | DB: ${trade.storageNode.toUpperCase()}`);
        } catch (err) {
            console.error(`❌ [PaperExchange] Lỗi khi đóng lệnh ${symbol}:`, err.message);
        }
    }
}module.exports = new PaperExchange();
