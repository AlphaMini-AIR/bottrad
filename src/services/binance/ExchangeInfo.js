const axios = require('axios');

class ExchangeInfo {
    constructor() {
        this.precisions = {};
        this.symbolConfigs = {}; // [MỚI] Lưu giới hạn Notional tối thiểu từ Binance
    }

    async init() {
        try {
            console.log('🔄 [EXCHANGE INFO] Đang tải cấu hình động từ Binance...');
            const response = await axios.get('https://fapi.binance.com/fapi/v1/exchangeInfo');

            response.data.symbols.forEach(symbolData => {
                // Lọc ra các quy tắc của sàn
                const priceFilter = symbolData.filters.find(f => f.filterType === 'PRICE_FILTER');
                const lotFilter = symbolData.filters.find(f => f.filterType === 'LOT_SIZE');
                const notionalFilter = symbolData.filters.find(f => f.filterType === 'MIN_NOTIONAL');

                this.precisions[symbolData.symbol] = {
                    tickSize: priceFilter ? parseFloat(priceFilter.tickSize) : 0,
                    stepSize: lotFilter ? parseFloat(lotFilter.stepSize) : 0
                };

                // [MỚI] Nạp quy tắc Min Notional thực tế của từng cặp coin
                this.symbolConfigs[symbolData.symbol] = {
                    minNotional: notionalFilter ? parseFloat(notionalFilter.notional) : 5.0,
                    maxLeverage: 125, // Mock an toàn, OrderManager sẽ ép cứng 20x
                    maintenanceMarginRate: 0.01
                };
            });

            console.log('✅ [EXCHANGE INFO] Tải thành công! Đã lấy Min Notional động cho tất cả cặp coin.');
        } catch (error) {
            console.error('❌ [EXCHANGE INFO] Lỗi kéo thông tin sàn:', error.message);
        }
    }

    getPrecision(symbol) {
        const symbolUpper = symbol.toUpperCase();
        if (!this.precisions[symbolUpper]) {
            return { tickSize: 0.01, stepSize: 0.001 };
        }
        return this.precisions[symbolUpper];
    }

    // [MỚI] Hàm xuất cấu hình giới hạn lệnh (Không còn gắn cứng)
    getSymbolConfig(symbol) {
        const symbolUpper = symbol.toUpperCase();
        if (!this.symbolConfigs[symbolUpper]) {
            return { minNotional: 5.0, maxLeverage: 20, maintenanceMarginRate: 0.01 }; // Fallback an toàn
        }
        return this.symbolConfigs[symbolUpper];
    }
}

module.exports = new ExchangeInfo();