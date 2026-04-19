/**
 * src/services/ai/DeepThinker.js - V16.1 (Context-Aware Mastermind)
 * Tối ưu hóa: Ngưỡng bóp cò động (nhìn dòng tiền), Cấp phát thông số Trailing Stop.
 */
const fs = require('fs');
const path = require('path');

class DeepThinker {
    constructor() {
        this.whitelist = {};
        this.loadWhitelist();
    }

    loadWhitelist() {
        try {
            const data = fs.readFileSync(path.join(__dirname, '../../../white_list.json'), 'utf8');
            this.whitelist = JSON.parse(data).active_coins;
            console.log('🧠 [DeepThinker] Đã nạp chiến thuật & Quản trị rủi ro từ white_list.json');
        } catch (error) {
            console.error('❌ [DeepThinker] Không đọc được white_list.json:', error.message);
        }
    }

    evaluate(symbol, winProb, historyCandles, features) {
        const config = this.whitelist[symbol];
        if (!config) return { approved: false, reason: 'N/A - Không có trong Whitelist' };

        // --- 1. GIẢI MÃ VI CẤU TRÚC (Từ AiEngine truyền sang) ---
        // Tham chiếu mảng features: [ob_imb(0), spread(1), bid_vol(2), ask_vol(3), max_buy(4), max_sell(5), liq_l(6), liq_s(7), funding(8), taker_buy(9), body(10), wick(11), btc_beta(12)]
        const taker_buy_ratio = features[9];
        const btc_relative_strength = features[12];
        const spread = features[1];

        // --- 2. NGƯỠNG BÓP CÒ ĐỘNG (DYNAMIC THRESHOLD) ---
        // Tiêu chuẩn gốc
        let longThreshold = 0.60;
        let shortThreshold = 0.40;

        // Nếu Altcoin đang khỏe hơn BTC (btc_beta > 0) và dòng tiền Mua mạnh -> Dễ dãi hơn với lệnh Long, khắt khe hơn với Short
        if (btc_relative_strength > 0.001 && taker_buy_ratio > 0.55) {
            longThreshold -= 0.03;  // Giảm còn 0.57 là bóp cò
            shortThreshold -= 0.05; // Giảm còn 0.35 mới dám đánh Short
        } 
        // Ngược lại, nếu yếu hơn BTC và xả mạnh -> Dễ dãi hơn với lệnh Short
        else if (btc_relative_strength < -0.001 && taker_buy_ratio < 0.45) {
            shortThreshold += 0.03; // Tăng lên 0.43 là bóp cò
            longThreshold += 0.05;  // Tăng lên 0.65 mới dám đánh Long
        }

        // Chốt Quyết định
        let side = null;
        if (winProb >= longThreshold) {
            side = 'LONG';
        } else if (winProb <= shortThreshold) {
            side = 'SHORT';
        } else {
            return { approved: false, reason: `REJECT: Không đủ tự tin (Prob: ${winProb.toFixed(3)} | L_Req: ${longThreshold.toFixed(2)}, S_Req: ${shortThreshold.toFixed(2)})` };
        }

        // --- 3. KIỂM TRA CHỐNG TRƯỢT GIÁ THẢM HỌA ---
        if (spread > 0.005) { // Spread > 0.5%
            return { approved: false, reason: `REJECT: Sổ lệnh quá mỏng, dễ bị trượt giá (Spread: ${spread.toFixed(4)})` };
        }

        // --- 4. TÍNH TOÁN ĐỘ BIẾN ĐỘNG ATR (KHỚP 100% TRIPLE-BARRIER PYTHON) ---
        if (historyCandles.length < 16) return { approved: false, reason: 'REJECT: Thiếu nến tính ATR' };
        const recentCandles = historyCandles.slice(-16, -1); 
        
        let atrSum = 0;
        recentCandles.forEach(c => atrSum += (c.high - c.low));
        const currentAtr = atrSum / 15;

        if (currentAtr === 0) return { approved: false, reason: 'REJECT: Không có thanh khoản (ATR=0)' };

        // --- 5. TỌA ĐỘ TÁC CHIẾN (SL, TP cứng) ---
        const currentPrice = historyCandles[historyCandles.length - 1].close;
        const slDistance = currentAtr * config.sl_mult;
        const tpDistance = currentAtr * config.tp_mult;

        let slPrice, tpPrice, limitPrice;
        if (side === 'LONG') {
            limitPrice = currentPrice;
            slPrice = limitPrice - slDistance;
            tpPrice = limitPrice + tpDistance;
        } else {
            limitPrice = currentPrice;
            slPrice = limitPrice + slDistance;
            tpPrice = limitPrice - tpDistance;
        }

        // --- 6. CẤP PHÁT TỌA ĐỘ CHO BẮN TỈA BÁM ĐUỔI (TRAILING STOP) ---
        // Tính năng mới: Khi giá đi được 50% quãng đường tới TP -> Bắt đầu Trailing Stop
        const trailingActivationDistance = tpDistance * 0.5; 
        const trailingActivationPrice = side === 'LONG' 
            ? limitPrice + trailingActivationDistance 
            : limitPrice - trailingActivationDistance;
        const trailingCallbackRate = 0.5; // (0.5%) Nếu giá giật ngược lại 0.5% từ đỉnh -> Cắt chốt lời

        // --- 7. BỘ LỌC TỬ THẦN ---
        const riskPct = (slDistance / currentPrice) * 100;
        if (riskPct > 5.0) {
            return { approved: false, reason: `REJECT: Bão lớn, SL quá xa (${riskPct.toFixed(2)}% > 5%)` };
        }

        // TRẢ VỀ TÍN HIỆU HOÀN HẢO CHO VÒNG LẶP 500MS
        return {
            approved: true,
            side: side,
            limitPrice: parseFloat(limitPrice.toFixed(4)),
            slPrice: parseFloat(slPrice.toFixed(4)),
            tpPrice: parseFloat(tpPrice.toFixed(4)),
            
            // Gửi cấu hình thông minh cho Lính bắn tỉa (Node.js 500ms)
            trailingParams: {
                activationPrice: parseFloat(trailingActivationPrice.toFixed(4)),
                callbackRate: trailingCallbackRate
            },
            
            timeLimit: config.time_limit,
            prob: parseFloat(winProb.toFixed(3)),
            reason: `Duyệt ${side} (WinProb: ${winProb.toFixed(2)}) | Lực BTC: ${btc_relative_strength.toFixed(4)}`
        };
    }
}

module.exports = new DeepThinker();