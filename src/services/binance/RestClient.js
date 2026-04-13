/**
 * src/services/binance/RestClient.js
 */
const axios = require('axios');

class RestClient {
    async getFundingRates() {
        try {
            // Dùng Batch API: Không truyền symbol để lấy toàn bộ 300+ coin trong 1 request
            const response = await axios.get('https://fapi.binance.com/fapi/v1/premiumIndex');
            const fundingData = {};

            response.data.forEach(item => {
                fundingData[item.symbol.toLowerCase()] = parseFloat(item.lastFundingRate);
            });

            return fundingData;
        } catch (error) {
            console.error('❌ [REST] Lỗi lấy Funding Rate:', error.message);
            return {}; // Trả về object rỗng nếu lỗi để không sập luồng
        }
    }
}

module.exports = new RestClient();