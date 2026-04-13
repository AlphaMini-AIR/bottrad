/**
 * src/services/ops/CircuitBreaker.js
 */
const PaperAccount = require('../../models/PaperAccount');
const ActiveRoster = require('../../models/ActiveRoster');

class CircuitBreaker {
    // Ngưỡng dừng lỗ tối đa cho phép của toàn bộ tài khoản (%)
    MAX_ACCOUNT_DRAWDOWN = 0.05; // 5%
    async checkSafety() {
        const account = await PaperAccount.findOne({ account_id: 'main_paper' });
        if (!account) return false;

        // Nếu vốn tụt quá 5% (tức là còn dưới 95 USDT)
        if (account.balance < 100 * (1 - this.MAX_ACCOUNT_DRAWDOWN)) {
            console.log(`\n🚨 [CIRCUIT BREAKER] TỔNG VỐN SỤT GIẢM QUÁ 5% (${account.balance.toFixed(2)} USDT). DỪNG TOÀN BỘ HỆ THỐNG!`);
            return false;
        }

        return true;
    }

    // Kiểm tra xem một đồng coin cụ thể có đang bị "cấm thi đấu" không
    async isCoinSafe(symbol) {
        const roster = await ActiveRoster.findOne().sort({ created_at: -1 });
        if (!roster) return true;

        const coinStatus = roster.coins.find(c => c.symbol === symbol.toUpperCase());

        // Nếu coin này đã bị đánh dấu is_active = false, không cho bóp cò
        if (coinStatus && !coinStatus.is_active) {
            console.log(`\n🚫 [CIRCUIT BREAKER] ${symbol.toUpperCase()} đang bị đình chỉ do thua lỗ.`);
            return false;
        }
        return true;
    }

    // Hàm cập nhật khi một lệnh bị lỗ (gọi sau khi Đóng lệnh)
    async reportLoss(symbol) {
        // Logic: Nếu coin thua 3 lần liên tiếp, set is_active = false
        // (Tạm thời logic này Hưng có thể update trong DB hoặc chúng ta sẽ viết sâu hơn ở bản Dashboard)
    }
}

module.exports = new CircuitBreaker();