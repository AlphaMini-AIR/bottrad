/**
 * src/services/ops/Reconciler.js
 */
const PaperAccount = require('../../models/PaperAccount');

class Reconciler {
    constructor() {
        this.tradeMode = process.env.TRADE_MODE || 'PAPER';
    }

    async sync() {
        if (this.tradeMode === 'PAPER') {
            await this.syncPaper();
        } else {
            await this.syncLive();
        }
    }

    async syncPaper() {
        const account = await PaperAccount.findOne({ account_id: 'main_paper' });
        if (!account) return;

        // Đối soát cơ bản cho ví giả lập
        if (account.balance < 0) {
            console.log('🚨 [RECONCILER] Số dư âm! Reset về 0 để bảo toàn hệ thống.');
            account.balance = 0;
            await account.save();
        }
    }

    async syncLive() {
        // Phần này sẽ gọi API Binance để check vị thế thực tế ở Sprint sau
        // console.log('🔍 [RECONCILER] Đang đối soát với sàn Binance...');
    }
}

module.exports = new Reconciler();