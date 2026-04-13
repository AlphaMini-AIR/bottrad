const connectDB = require('./config/db');
const PaperAccount = require('./models/PaperAccount');

const initSystem = async () => {
    await connectDB();

    try {
        // Kiểm tra xem đã có tài khoản giả lập chưa
        let account = await PaperAccount.findOne({ account_id: 'main_paper' });

        if (!account) {
            account = new PaperAccount({ balance: 100.00 });
            await account.save();
            console.log('💰 [INIT] Đã tạo ví giả lập: 100.00 USDT');
        } else {
            console.log(`💰 [INIT] Ví giả lập hiện có: ${account.balance} USDT`);
        }

        console.log('🚀 [TASK 1.1] HOÀN THÀNH: Database và Paper Wallet đã sẵn sàng.');
        process.exit(0);
    } catch (err) {
        console.error('❌ [INIT] Lỗi khởi tạo:', err.message);
        process.exit(1);
    }
};

initSystem();