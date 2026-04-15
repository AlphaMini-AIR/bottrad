src/services/execution/PaperExchange.js

/**
 * src/services/execution/PaperExchange.js - Version 4.5 (Hoàn tất Vòng lặp Học tập)
 */
const PaperAccount = require('../../models/PaperAccount');
const AutoRetrainScheduler = require('../ops/AutoRetrainScheduler'); // [THÊM MỚI]: Nạp trạm thu thập kinh nghiệm

class PaperExchange {
    constructor() {
        this.accountId = 'main_paper';
    }

    /**
     * Hàm lấy dữ liệu tài khoản
     */
    async getAccount() {
        let acc = await PaperAccount.findOne({ account_id: this.accountId });
        if (!acc) {
            acc = await PaperAccount.create({
                account_id: this.accountId,
                balance: 1000, // Vốn giả lập ban đầu
                open_positions: [],
                history: []
            });
        }
        return acc;
    }

    /**
     * [MẮT XÍCH CUỐI CÙNG 1/2]: Nhận bối cảnh (entryFeatures) và lưu vào lệnh
     */
    async openPosition(symbol, side, currentPrice, size, mode, hardSl, takeProfit, entryFeatures = []) {
        const acc = await this.getAccount();
        
        // Trừ phí giả lập (0.05% Taker fee để test cho khắc nghiệt)
        const fee = (size * currentPrice) * 0.0005;
        acc.balance -= fee;

        const newPosition = {
            symbol,
            side,
            size,
            entryPrice: currentPrice,
            sl: hardSl,
            tp: takeProfit,
            mode,
            openedAt: new Date(),
            features: entryFeatures // 💾 LƯU TRỮ BỨC TRANH THỊ TRƯỜNG VÀO DATABASE
        };

        acc.open_positions.push(newPosition);
        await acc.save();
        
        console.log(`✅ [PAPER] Đã mở ${side} ${symbol} | Giá: ${currentPrice} | Phí: ${fee.toFixed(3)}$`);
    }

    /**
     * [MẮT XÍCH CUỐI CÙNG 2/2]: Radar dò giá và Chốt lệnh -> Xuất bài học
     */
    async monitorPrices(currentPrices) {
        const acc = await this.getAccount();
        if (!acc || acc.open_positions.length === 0) return;

        let isModified = false;

        // Quét từng lệnh đang mở
        for (let i = acc.open_positions.length - 1; i >= 0; i--) {
            const pos = acc.open_positions[i];
            const currentPrice = currentPrices[pos.symbol];
            
            if (!currentPrice) continue;

            let isClosed = false;
            let exitReason = '';
            
            // Kiểm tra SL / TP
            if (pos.side === 'LONG') {
                if (currentPrice >= pos.tp) { isClosed = true; exitReason = 'TAKE_PROFIT'; }
                else if (currentPrice <= pos.sl) { isClosed = true; exitReason = 'STOP_LOSS'; }
            } else if (pos.side === 'SHORT') {
                if (currentPrice <= pos.tp) { isClosed = true; exitReason = 'TAKE_PROFIT'; }
                else if (currentPrice >= pos.sl) { isClosed = true; exitReason = 'STOP_LOSS'; }
            }

            // Nếu chạm cản, tiến hành đóng lệnh
            if (isClosed) {
                // Trừ phí đóng lệnh (0.05%)
                const closeFee = (pos.size * currentPrice) * 0.0005;
                
                // Tính PnL (Lợi nhuận gộp)
                let grossPnl = 0;
                if (pos.side === 'LONG') grossPnl = (currentPrice - pos.entryPrice) * pos.size;
                if (pos.side === 'SHORT') grossPnl = (pos.entryPrice - currentPrice) * pos.size;
                
                // Lợi nhuận ròng
                const netPnl = grossPnl - closeFee;
                
                // Cập nhật số dư
                acc.balance += (pos.size * pos.entryPrice) + netPnl; 

                console.log(`\n🔔 [PAPER CHỐT LỆNH] ${pos.side} ${pos.symbol} chạm ${exitReason}!`);
                console.log(`💵 PnL: ${netPnl.toFixed(3)}$ | Số dư mới: ${acc.balance.toFixed(2)}$`);

                // =========================================================
                // 🚀 TRUYỀN DỮ LIỆU SANG TRẠM HỌC TẬP (AUTO RETRAIN)
                // =========================================================
                AutoRetrainScheduler.logCompletedTrade(
                    pos.symbol, 
                    pos.side, 
                    pos.entryPrice, 
                    currentPrice, 
                    netPnl, 
                    pos.features // Nộp lại mảng 7 biến số đã lưu lúc mở lệnh
                );

                // =========================================================
                // 🧠 BÁO CÁO CHO DEEP THINKER ĐỂ RÚT KINH NGHIỆM NGAY LẬP TỨC
                // =========================================================
                const DeepThinker = require('../ai/DeepThinker'); // Gọi trực tiếp để tránh circular require
                const isWin = netPnl > 0;
                const riskRewardActual = isWin ? (grossPnl / Math.abs(pos.entryPrice - pos.sl)) : 0; 
                DeepThinker.learnFromTrade(isWin, riskRewardActual);

                // Xóa lệnh khỏi mảng
                acc.open_positions.splice(i, 1);
                isModified = true;
            }
        }

        if (isModified) {
            await acc.save();
        }
    }
}

module.exports = new PaperExchange();

