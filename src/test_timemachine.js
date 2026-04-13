/**
 * src/test_timemachine.js (Bản Test Cầu Dao Khẩn Cấp)
 */
const axios = require('axios');
const connectDB = require('./config/db');
const PaperAccount = require('./models/PaperAccount');
const OrderManager = require('./services/execution/OrderManager');

async function runTest() {
    await connectDB();

    // --- KỊCH BẢN 1: GIẢ LẬP THUA LỖ NẶNG ---
    console.log('📉 [TEST] Giả lập tài khoản bị lỗ nặng (còn 90 USDT)...');
    await PaperAccount.updateOne(
        { account_id: 'main_paper' },
        { balance: 90.00, open_positions: [] }, // Ép số dư xuống dưới ngưỡng 95%
        { upsert: true }
    );

    console.log('📥 [TEST] Đang tải nến để thử bóp cò...');
    const res = await axios.get('https://fapi.binance.com/fapi/v1/klines', {
        params: { symbol: 'BTCUSDT', interval: '1m', limit: 1 }
    });
    const entryPrice = parseFloat(res.data[0][1]);

    console.log(`🚀 [TEST] Thử gửi tín hiệu LONG tại giá: ${entryPrice}`);

    const decision = {
        action: 'EXECUTE',
        symbol: 'btcusdt',
        side: 'LONG',
        mode: 'STANDARD'
    };

    // Gửi sang OrderManager - Nơi có gắn Cầu dao CircuitBreaker
    await OrderManager.executeSignal(decision, entryPrice);

    // --- KỊCH BẢN 2: RESET VỀ 100 ĐỂ KIỂM TRA PHỤC HỒI ---
    console.log('\n🔄 [TEST] Thử reset ví về 100 USDT để xem Cầu dao có mở lại không...');
    await PaperAccount.updateOne({ account_id: 'main_paper' }, { balance: 100.00 });

    console.log('🚀 [TEST] Thử bóp cò lại lần nữa...');
    await OrderManager.executeSignal(decision, entryPrice);

    console.log('\n✅ Hoàn tất kiểm tra Cầu dao!');
    process.exit(0);
}

runTest();