/**
 * src/workers/TopTraderWorker.js - Tiến trình ngầm cách ly I/O
 */
const axios = require('axios');

// Đảm bảo biến toàn cục (RAM) đã tồn tại
if (!global.topTraderRatios) global.topTraderRatios = {};

class TopTraderWorker {
    constructor() {
        this.symbols = [];
        this.intervalMs = 15 * 60 * 1000; // 15 phút cập nhật 1 lần
        this.timer = null;
    }

    // Nhận danh sách coin từ main.js
    init(symbols) {
        this.symbols = symbols;
    }

    // Hàm lấy dữ liệu cho 1 đồng coin
    async fetchSymbol(symbol) {
        try {
            // Lấy dữ liệu 5 phút gần nhất
            const url = `https://fapi.binance.com/fapi/v1/topLongShortAccountRatio?symbol=${symbol}&period=5m&limit=1`;
            const res = await axios.get(url, { timeout: 5000 }); // Ép Timeout 5s để không bị treo
            
            if (res.data && res.data.length > 0) {
                const data = res.data[0];
                global.topTraderRatios[symbol] = {
                    longRatio: parseFloat(data.longAccount),
                    shortRatio: parseFloat(data.shortAccount),
                    timestamp: Date.now(),
                    isStale: false // Dữ liệu tươi mới
                };
            }
        } catch (err) {
            // 🛡️ PHÒNG VỆ: Không throw error làm sập bot. Chỉ đánh dấu là Stale (Hết đát)
            if (global.topTraderRatios[symbol]) {
                global.topTraderRatios[symbol].isStale = true;
            }
            console.warn(`⚠️ [WORKER] Cảnh báo mạng khi lấy TopTrader ${symbol}. Đã bật chế độ Suy giảm (Time Decay).`);
        }
    }

    // Gọi song song cho tất cả các coin
    async fetchAll() {
        if (this.symbols.length === 0) return;
        // Dùng Promise.allSettled để dù 1 coin lỗi thì các coin khác vẫn lấy được
        await Promise.allSettled(this.symbols.map(sym => this.fetchSymbol(sym)));
    }

    start() {
        if (this.symbols.length === 0) return;
        console.log(`⚙️ [BACKGROUND WORKER] Khởi động tiến trình TopTraderRatio cho ${this.symbols.length} coins...`);
        
        this.fetchAll(); // Chạy ngay lần đầu tiên
        this.timer = setInterval(() => this.fetchAll(), this.intervalMs); // Đặt lịch chạy ngầm
    }
}

module.exports = new TopTraderWorker();

