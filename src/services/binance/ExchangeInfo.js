/**
 * src/services/binance/ExchangeInfo.js
 */
const axios = require('axios');

class ExchangeInfo {
    constructor() {
        // Lưu trữ cấu hình trong RAM
        this.precisions = {};
    }

    /**
     * Khởi tạo: Chạy 1 lần duy nhất khi boot Bot để kéo cấu hình toàn sàn
     */
    async init() {
        try {
            console.log('🔄 [EXCHANGE INFO] Đang tải cấu hình Precision từ Binance...');
            const response = await axios.get('https://fapi.binance.com/fapi/v1/exchangeInfo');

            response.data.symbols.forEach(symbolData => {
                // Lọc ra quy tắc làm tròn Giá (PRICE_FILTER)
                const priceFilter = symbolData.filters.find(f => f.filterType === 'PRICE_FILTER');
                // Lọc ra quy tắc làm tròn Số lượng (LOT_SIZE)
                const lotFilter = symbolData.filters.find(f => f.filterType === 'LOT_SIZE');

                this.precisions[symbolData.symbol] = {
                    tickSize: priceFilter ? parseFloat(priceFilter.tickSize) : 0,
                    stepSize: lotFilter ? parseFloat(lotFilter.stepSize) : 0
                };
            });

            console.log('✅ [EXCHANGE INFO] Tải thành công! Sẵn sàng ép kiểu số thập phân.');
        } catch (error) {
            console.error('❌ [EXCHANGE INFO] Lỗi kéo thông tin sàn:', error.message);
        }
    }

    /**
     * Lấy cấu hình của 1 đồng Coin
     */
    getPrecision(symbol) {
        const symbolUpper = symbol.toUpperCase();
        if (!this.precisions[symbolUpper]) {
            console.warn(`⚠️ [WARN] Không tìm thấy cấu hình cho ${symbolUpper}, trả về mặc định.`);
            return { tickSize: 0.01, stepSize: 0.001 }; // Trả về fallback an toàn
        }
        return this.precisions[symbolUpper];
    }
}

// Xuất ra 1 instance duy nhất (Singleton Pattern)
module.exports = new ExchangeInfo();