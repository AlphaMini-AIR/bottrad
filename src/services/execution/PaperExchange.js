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

            // --- FIXED: Giả lập Slippage (Trượt giá 0.05%) cho lệnh Market ---
            const slippage = 0.0005;
            const executedPrice = side === 'LONG' ? price * (1 + slippage) : price * (1 - slippage);

            const leverage = 10;
            const notionalValue = executedPrice * size;
            const marginRequired = notionalValue / leverage;
            const fee = notionalValue * this.TAKER_FEE_RATE;

            if (account.balance < marginRequired + fee) return false;

            account.balance -= fee;

            account.open_positions.push({
                symbol,
                side,
                entry_price: executedPrice, // Ghi nhận giá đã trượt
                margin: marginRequired,
                leverage,
                size,
                mode,
                hard_sl: hardSl,
                take_profit: takeProfit
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

        for (let i = account.open_positions.length - 1; i >= 0; i--) {
            const p = account.open_positions[i];
            const currentPrice = livePrices[p.symbol];
            if (!currentPrice) continue;

            // --- THÊM LOGIC TRAILING STOP (DỜI SL VỀ HÒA VỐN) ---
            const entry = p.entry_price;
            const tp = p.take_profit;
            const sl = p.hard_sl;

            // Tính toán giá trị lợi nhuận kỳ vọng
            const expectedProfitDistance = Math.abs(tp - entry);
            const currentDistance = Math.abs(currentPrice - entry);

            if (p.side === 'LONG') {
                // Nếu giá đã đi được hơn 50% quãng đường tới TP, dời SL lên điểm Entry (Hòa vốn)
                if (currentPrice > entry && currentDistance >= expectedProfitDistance * 0.5) {
                    if (p.hard_sl < entry) {
                        p.hard_sl = entry;
                        console.log(`🛡️ [TRAILING STOP] ${p.symbol} - Dời SL lên điểm hòa vốn: ${entry}`);
                        await account.save();
                    }
                }

                // Xử lý chốt lệnh
                if (currentPrice <= p.hard_sl) await this.closePosition(account, i, currentPrice, 'Chạm Cắt Lỗ (SL)');
                else if (currentPrice >= p.take_profit) await this.closePosition(account, i, currentPrice, 'Chạm Chốt Lời (TP)');

            } else if (p.side === 'SHORT') {
                // Nếu giá đi đúng hướng (giảm) hơn 50%, dời SL xuống điểm Entry
                if (currentPrice < entry && currentDistance >= expectedProfitDistance * 0.5) {
                    if (p.hard_sl > entry) {
                        p.hard_sl = entry;
                        console.log(`🛡️ [TRAILING STOP] ${p.symbol} - Dời SL xuống điểm hòa vốn: ${entry}`);
                        await account.save();
                    }
                }

                // Xử lý chốt lệnh
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