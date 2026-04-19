/**
 * src/services/execution/PaperExchange.js - V16.1 (Sniper Execution)
 * Vòng lặp 500ms: Quản lý vị thế ảo, tính PnL và kích hoạt Trailing Stop.
 */
const PaperTrade = require('../../models/PaperTrade'); // Cấu trúc Mongoose của bạn
const PaperAccount = require('../../models/PaperAccount'); // Lưu số dư 1000$

class PaperExchange {
    constructor() {
        this.activeTrades = new Map(); // Lưu lệnh đang chạy trên RAM để truy xuất siêu tốc (O(1))
        this.TRADE_AMOUNT = 2.0; // Đánh đều tay 2$ mỗi lệnh theo yêu cầu của bạn
        this.FEE_RATE = 0.0004; // Phí taker Binance (0.04%)
    }

    async initAccount() {
        // Khởi tạo tài khoản ảo 1000$ nếu chưa có
        let acc = await PaperAccount.findOne({ accountId: 'MAIN_PAPER' });
        if (!acc) {
            acc = new PaperAccount({ accountId: 'MAIN_PAPER', balance: 1000.0, initialBalance: 1000.0 });
            await acc.save();
            console.log('💰 [PaperExchange] Đã cấp vốn 1000$ cho Bot đánh thử.');
        } else {
            console.log(`💰 [PaperExchange] Vốn hiện tại: ${acc.balance.toFixed(2)}$`);
        }
    }

    // Hàm này được gọi từ main.js mỗi khi DeepThinker duyệt lệnh
    async openTrade(symbol, side, limitPrice, slPrice, tpPrice, trailingParams, prob, reason) {
        // Chỉ cho phép 1 coin có 1 lệnh tại 1 thời điểm
        if (this.activeTrades.has(symbol)) return;

        const tradeSize = this.TRADE_AMOUNT / limitPrice; // Số lượng coin mua được

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
            extremePrice: limitPrice, // Giá cao nhất/thấp nhất từ lúc mở lệnh
            prob,
            reason,
            openTime: Date.now()
        };

        this.activeTrades.set(symbol, newTrade);
        console.log(`🔫 [BẮN TỈA] MỞ LỆNH ${side} ${symbol} | Giá: ${limitPrice} | SL: ${slPrice} | TP: ${tpPrice}`);
    }

    // VÒNG LẶP 500ms SẼ GỌI HÀM NÀY LIÊN TỤC
    async monitorTrades() {
        if (this.activeTrades.size === 0) return;

        for (const [symbol, trade] of this.activeTrades.entries()) {
            const currentPrice = global.liveMicroData?.[symbol]?.close || global.currentMarkPrice?.[symbol];
            if (!currentPrice) continue;

            let isClosed = false;
            let closePrice = 0;
            let closeReason = '';

            // 1. CẬP NHẬT ĐỈNH/ĐÁY CHO TRAILING STOP
            if (trade.side === 'LONG') {
                if (currentPrice > trade.extremePrice) trade.extremePrice = currentPrice;
            } else {
                if (currentPrice < trade.extremePrice && currentPrice > 0) trade.extremePrice = currentPrice;
            }

            // 2. KÍCH HOẠT TRAILING STOP NẾU VƯỢT VẠCH ĐÍCH 50%
            if (!trade.isTrailingActive) {
                if (trade.side === 'LONG' && currentPrice >= trade.trailingParams.activationPrice) {
                    trade.isTrailingActive = true;
                    console.log(`🔥 [TRAILING] ${symbol} đã kích hoạt bám đuổi chốt lời!`);
                } else if (trade.side === 'SHORT' && currentPrice <= trade.trailingParams.activationPrice) {
                    trade.isTrailingActive = true;
                    console.log(`🔥 [TRAILING] ${symbol} đã kích hoạt bám đuổi chốt lời!`);
                }
            }

            // 3. LOGIC CHỐT LỜI / CẮT LỖ SIÊU TỐC
            if (trade.side === 'LONG') {
                // Rớt xuống SL cứng -> Cắt lỗ
                if (currentPrice <= trade.slPrice) {
                    isClosed = true; closePrice = currentPrice; closeReason = 'HARD_SL';
                }
                // Bơm mạnh qua TP cứng -> Chốt lời ngay nếu chưa bật Trailing
                else if (currentPrice >= trade.tpPrice && !trade.isTrailingActive) {
                    isClosed = true; closePrice = currentPrice; closeReason = 'HARD_TP';
                }
                // Nếu đang Trailing: Tụi 0.5% từ Đỉnh -> Chốt Lời Động (Râu nến)
                else if (trade.isTrailingActive) {
                    const dynamicSL = trade.extremePrice * (1 - trade.trailingParams.callbackRate / 100);
                    if (currentPrice <= dynamicSL) {
                        isClosed = true; closePrice = currentPrice; closeReason = 'TRAILING_STOP';
                    }
                }
            } 
            else if (trade.side === 'SHORT') {
                // Bơm lên cắn SL cứng -> Cắt lỗ
                if (currentPrice >= trade.slPrice) {
                    isClosed = true; closePrice = currentPrice; closeReason = 'HARD_SL';
                }
                // Rớt qua TP cứng -> Chốt lời
                else if (currentPrice <= trade.tpPrice && !trade.isTrailingActive) {
                    isClosed = true; closePrice = currentPrice; closeReason = 'HARD_TP';
                }
                // Nếu đang Trailing: Giật lên 0.5% từ Đáy -> Chốt Lời Động
                else if (trade.isTrailingActive) {
                    const dynamicSL = trade.extremePrice * (1 + trade.trailingParams.callbackRate / 100);
                    if (currentPrice >= dynamicSL) {
                        isClosed = true; closePrice = currentPrice; closeReason = 'TRAILING_STOP';
                    }
                }
            }

            // 4. XỬ LÝ KHI ĐÓNG LỆNH
            if (isClosed) {
                await this.closeTrade(symbol, trade, closePrice, closeReason);
            }
        }
    }

    async closeTrade(symbol, trade, closePrice, closeReason) {
        // Xóa khỏi RAM để vòng lặp 500ms không quét nữa
        this.activeTrades.delete(symbol);

        // Tính lợi nhuận (PnL)
        let pnl = 0;
        if (trade.side === 'LONG') {
            pnl = (closePrice - trade.entryPrice) * trade.size;
        } else {
            pnl = (trade.entryPrice - closePrice) * trade.size;
        }

        // Trừ phí giao dịch 2 đầu (Mở và Đóng lệnh)
        const fee = (trade.entryPrice * trade.size * this.FEE_RATE) + (closePrice * trade.size * this.FEE_RATE);
        const netPnl = pnl - fee;
        const isWin = netPnl > 0;

        // Lưu vào MongoDB để làm UI Báo cáo
        const record = new PaperTrade({
            symbol: trade.symbol,
            side: trade.side,
            entryPrice: trade.entryPrice,
            closePrice: closePrice,
            margin: trade.margin,
            netPnl: netPnl,
            outcome: isWin ? 'WIN' : 'LOSS',
            closeReason: closeReason,
            openTime: trade.openTime,
            closeTime: Date.now()
        });
        await record.save();

        // Cập nhật Số dư tài khoản 1000$
        await PaperAccount.updateOne(
            { accountId: 'MAIN_PAPER' },
            { $inc: { balance: netPnl } }
        );

        const icon = isWin ? '✅' : '❌';
        console.log(`${icon} [ĐÓNG LỆNH] ${symbol} | Lý do: ${closeReason} | PnL: ${netPnl.toFixed(4)}$`);
    }
}

module.exports = new PaperExchange();