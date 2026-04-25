class RiskGuard {
    constructor(initialCapital = 200) {
        this.INITIAL_CAPITAL = initialCapital;
        
        // CÁC NGƯỠNG CHỊU ĐỰNG RỦI RO (RISK TOLERANCE)
        this.MAX_CONCURRENT_TRADES = 5;      // Tối đa 5 lệnh cùng lúc
        this.DRAWDOWN_LIMIT = 0.30;          // Sụt giảm 30% -> Chết
        this.MACRO_VOLATILITY_THRESHOLD = 10.0; // ATR BTC > 10% -> Bão Flash Crash

        // TRẠNG THÁI HỆ THỐNG
        this.KILL_SWITCH_TRIGGERED = false;
        this.CIRCUIT_BREAKER_LOCKED_UNTIL = 0;
    }

    // Kiểm tra xem hệ thống có đang bị khóa không?
    isHalted() {
        if (this.KILL_SWITCH_TRIGGERED) return true;
        if (Date.now() < this.CIRCUIT_BREAKER_LOCKED_UNTIL) return true;
        return false;
    }

    // ĐẠO LUẬT 1: Chặn nhồi lệnh (Tránh rủi ro tập trung)
    canOpenNewTrade(currentActiveCount, pendingCount) {
        if (this.isHalted()) return false;
        
        if ((currentActiveCount + pendingCount) >= this.MAX_CONCURRENT_TRADES) {
            // Trả về false ngầm để bot không spam log, chỉ âm thầm từ chối
            return false;
        }
        return true;
    }

    // ĐẠO LUẬT 2: Drawdown Killswitch (Ngắt cầu dao khi lỗ 30%)
    checkDrawdown(currentBalance) {
        if (this.KILL_SWITCH_TRIGGERED) return false;

        const drawdown = (this.INITIAL_CAPITAL - currentBalance) / this.INITIAL_CAPITAL;
        if (drawdown >= this.DRAWDOWN_LIMIT) {
            console.log(`\n🚨🚨🚨 [KILL SWITCH KÍCH HOẠT] 🚨🚨🚨`);
            console.log(`Tài khoản sụt giảm ${(drawdown * 100).toFixed(2)}%. Vốn còn ${currentBalance.toFixed(2)}$.`);
            this.KILL_SWITCH_TRIGGERED = true;
            this.sendTelegramAlert(`🚨 TÀI KHOẢN RỚT -30%. KÍCH HOẠT KILL SWITCH. VỐN CÒN: ${currentBalance.toFixed(2)}$`);
            return false; // Trả về false để OrderManager biết mà PANIC SELL
        }
        return true; // Vẫn trong ngưỡng an toàn
    }

    // ĐẠO LUẬT 3: Macro Circuit Breaker (Né bão thị trường)
    // Tham số btcAtrPercent là tỷ lệ % biến động của BTC (Ví dụ: 10.5)
    checkMacroCircuitBreaker(btcAtrPercent) {
        if (this.KILL_SWITCH_TRIGGERED) return false;

        if (btcAtrPercent > this.MACRO_VOLATILITY_THRESHOLD) {
            const lockDuration = 24 * 60 * 60 * 1000; // Khóa 24 Giờ
            this.CIRCUIT_BREAKER_LOCKED_UNTIL = Date.now() + lockDuration;
            
            console.log(`\n🌪️ [CIRCUIT BREAKER] Bão Flash Crash! BTC biến động ${btcAtrPercent.toFixed(2)}%. Khóa bot 24h.`);
            this.sendTelegramAlert(`🌪️ FLASH CRASH DETECTED. KHÓA TOÀN BỘ HỆ THỐNG GIAO DỊCH TRONG 24 GIỜ.`);
            return false;
        }
        return true;
    }

    // Cổng thông báo ngoại vi
    sendTelegramAlert(message) {
        // TODO: Tích hợp Axios gọi API Telegram thực tế tại đây
        console.log(`📲 [TELEGRAM ALERT] ${message}`);
    }
}

// Khởi tạo Singleton với số vốn 200 USDT đồng bộ với PaperExchange
module.exports = new RiskGuard(200);