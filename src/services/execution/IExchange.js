/**
 * Interface chuẩn hóa cho mọi sàn giao dịch (Paper hoặc Live)
 * Bất kỳ module sàn nào cũng phải implement các hàm này.
 */
class IExchange {
    constructor() {
        if (this.constructor === IExchange) {
            throw new Error("Không thể khởi tạo trực tiếp Interface IExchange");
        }
    }

    async openTrade(symbol, orderType, entryPrice, leverage, mockConfidence) { throw new Error("Chưa implement openTrade"); }
    async closeTrade(symbol, closePrice, reason) { throw new Error("Chưa implement closeTrade"); }
    async updateTick(symbol, currentPrice) { throw new Error("Chưa implement updateTick"); }
    getWalletBalance() { throw new Error("Chưa implement getWalletBalance"); }
}

module.exports = IExchange;