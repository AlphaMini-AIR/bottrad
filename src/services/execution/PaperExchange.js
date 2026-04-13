/**
 * src/services/execution/PaperExchange.js
 */
const PaperAccount = require('../../models/PaperAccount');

class PaperExchange {
    TAKER_FEE_RATE = 0.0005;

    // Cập nhật hàm mở lệnh (thêm hardSl và takeProfit)
    async openPosition(symbol, side, price, size, mode, hardSl, takeProfit) {
        try {
            const account = await PaperAccount.findOne({ account_id: 'main_paper' });
            if (!account) return false;

            if (account.open_positions.some(p => p.symbol === symbol)) return false;

            const leverage = 10;
            const notionalValue = price * size;
            const marginRequired = notionalValue / leverage;
            const fee = notionalValue * this.TAKER_FEE_RATE;

            if (account.balance < marginRequired + fee) return false;

            account.balance -= fee;

            account.open_positions.push({
                symbol,
                side,
                entry_price: price,
                margin: marginRequired,
                leverage,
                size,
                mode,
                hard_sl: hardSl,       // LƯU SL VÀO SỔ
                take_profit: takeProfit // LƯU TP VÀO SỔ
            });

            await account.save();

            console.log(`\n💸 [MỞ LỆNH] ${side} ${symbol.toUpperCase()}`);
            console.log(`   - Chế độ: ${mode}`);
            console.log(`   - Giá vào: ${price.toFixed(2)}`);
            console.log(`   - Cắt lỗ (SL): ${hardSl.toFixed(2)} | Chốt lời (TP): ${takeProfit.toFixed(2)}`);
            console.log(`   - Số dư ví: ${account.balance.toFixed(2)} USDT\n`);
            return true;
        } catch (error) {
            console.error('❌ [PAPER] Lỗi mở lệnh:', error.message);
            return false;
        }
    }

    // HÀM MỚI: Xử lý đóng lệnh và tính toán Lợi nhuận (PnL)
    async closePosition(account, positionIndex, closePrice, reason) {
        const p = account.open_positions[positionIndex];
        const notional = closePrice * p.size;
        const closeFee = notional * this.TAKER_FEE_RATE;

        // Tính PnL
        let pnl = 0;
        if (p.side === 'LONG') pnl = (closePrice - p.entry_price) * p.size;
        if (p.side === 'SHORT') pnl = (p.entry_price - closePrice) * p.size;

        const netPnl = pnl - closeFee;
        account.balance += netPnl;

        // Xóa lệnh khỏi sổ
        const closedSymbol = p.symbol;
        account.open_positions.splice(positionIndex, 1);
        await account.save();

        console.log(`\n🔔 [ĐÓNG LỆNH] ${p.side} ${closedSymbol.toUpperCase()} | Lý do: ${reason}`);
        console.log(`   - Giá đóng: ${closePrice.toFixed(2)}`);
        console.log(`   - Lợi nhuận (PnL): ${pnl > 0 ? '+' : ''}${pnl.toFixed(4)} USDT`);
        console.log(`   - Phí đóng: -${closeFee.toFixed(4)} USDT`);
        console.log(`   - Ròng: ${netPnl > 0 ? '+' : ''}${netPnl.toFixed(4)} USDT`);
        console.log(`   - Số dư mới: ${account.balance.toFixed(2)} USDT\n`);
    }

    // HÀM MỚI: Radar theo dõi giá liên tục
    async monitorPrices(livePrices) {
        const account = await PaperAccount.findOne({ account_id: 'main_paper' });
        if (!account || account.open_positions.length === 0) return;

        // Lặp ngược mảng để an toàn khi xóa phần tử
        for (let i = account.open_positions.length - 1; i >= 0; i--) {
            const p = account.open_positions[i];
            const currentPrice = livePrices[p.symbol];
            if (!currentPrice) continue;

            // Kiểm tra SL và TP
            if (p.side === 'LONG') {
                if (currentPrice <= p.hard_sl) await this.closePosition(account, i, currentPrice, 'Chạm Cắt Lỗ (SL)');
                else if (currentPrice >= p.take_profit) await this.closePosition(account, i, currentPrice, 'Chạm Chốt Lời (TP)');
            } else if (p.side === 'SHORT') {
                if (currentPrice >= p.hard_sl) await this.closePosition(account, i, currentPrice, 'Chạm Cắt Lỗ (SL)');
                else if (currentPrice <= p.take_profit) await this.closePosition(account, i, currentPrice, 'Chạm Chốt Lời (TP)');
            }
        }
    }
    async processFunding(fundingRateMap, currentPrices) {
        const account = await PaperAccount.findOne({ account_id: 'main_paper' });
        if (!account || account.open_positions.length === 0) return;

        for (let i = 0; i < account.open_positions.length; i++) {
            const p = account.open_positions[i];
            const fRate = fundingRateMap[p.symbol] || 0;
            const positionValue = p.size * currentPrices[p.symbol];

            // Tính toán ai trả tiền cho ai
            let fundingFee = 0;
            if (p.side === 'LONG') fundingFee = positionValue * fRate;
            if (p.side === 'SHORT') fundingFee = positionValue * -fRate;

            account.balance -= fundingFee;
            console.log(`⏰ [FUNDING] ${p.symbol.toUpperCase()} | Thay đổi số dư: ${-fundingFee > 0 ? '+' : ''}${-fundingFee.toFixed(4)} USDT | Ví: ${account.balance.toFixed(2)} USDT`);
        }

        account.last_funding_charged = new Date();
        await account.save();
    }
}

module.exports = new PaperExchange();