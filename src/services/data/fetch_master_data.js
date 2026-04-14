/**
 * src/services/data/fetch_master_data.js
 */
const axios = require('axios');
const MarketData = require('../../models/MarketData');

const BASE_URL = 'https://fapi.binance.com';

class MasterDataFetcher {
    constructor() {
        // Bộ nhớ tạm (Cache) để thực hiện Forward-fill khi rớt mạng
        this.macroCache = {
            open_interest: null,
            long_short_ratio: null,
            funding_rate: null,
            next_funding_time: null
        };
    }

    /**
     * Đồng bộ thời gian: Chuyển timestamp nến 1m về timestamp nến 5m gần nhất đã ĐÓNG
     * Ví dụ: 10:04 (1m) -> Trừ đi phần dư -> 10:00 (5m)
     */
    getAligned5mTime(timestamp) {
        return timestamp - (timestamp % 300000);
    }

    /**
     * Kéo dữ liệu Vĩ mô (Macro) và xử lý Forward-fill
     */
    async fetchMacroData(symbol, timestamp1m) {
        try {
            const time5m = this.getAligned5mTime(timestamp1m);
            const symbolUpper = symbol.toUpperCase();

            // Gọi song song 3 API Vĩ mô để tối ưu tốc độ
            const [oiRes, lsRes, fundingRes] = await Promise.all([
                axios.get(`${BASE_URL}/futures/data/openInterestHist`, {
                    params: { symbol: symbolUpper, period: '5m', limit: 1, endTime: time5m }
                }),
                axios.get(`${BASE_URL}/futures/data/globalLongShortAccountRatio`, {
                    params: { symbol: symbolUpper, period: '5m', limit: 1, endTime: time5m }
                }),
                axios.get(`${BASE_URL}/fapi/v1/premiumIndex`, {
                    params: { symbol: symbolUpper }
                })
            ]);

            // Lấy data an toàn (Tránh crash nếu API trả về mảng rỗng)
            const oiValue = oiRes.data.length > 0 ? parseFloat(oiRes.data[0].sumOpenInterest) : this.macroCache.open_interest;
            const lsValue = lsRes.data.length > 0 ? parseFloat(lsRes.data[0].longShortRatio) : this.macroCache.long_short_ratio;
            const fundingValue = parseFloat(fundingRes.data.lastFundingRate);
            const nextFundingTime = parseInt(fundingRes.data.nextFundingTime);

            // Cập nhật Cache
            this.macroCache = {
                open_interest: oiValue,
                long_short_ratio: lsValue,
                funding_rate: fundingValue,
                next_funding_time: nextFundingTime
            };

            return { ...this.macroCache, isStaleData: false };

        } catch (error) {
            console.log(`\n⚠️ [WARN] API Vĩ mô rớt mạng hoặc rate limit! Kích hoạt Forward-fill cho ${symbol}.`);
            // Nếu lỗi, trả về data cũ từ Cache và bật cờ isStaleData
            return { ...this.macroCache, isStaleData: true };
        }
    }

    /**
     * Kéo nến 1m hiện tại và gộp với Vĩ mô -> Lưu vào MongoDB
     */
    async fetchAndSave(symbol) {
        try {
            const symbolUpper = symbol.toUpperCase();

            // 1. Kéo nến 1 phút hiện tại (limit = 1, cây nến đang chạy hoặc vừa đóng)
            const klineRes = await axios.get(`${BASE_URL}/fapi/v1/klines`, {
                params: { symbol: symbolUpper, interval: '1m', limit: 1 }
            });

            const kline = klineRes.data[0];
            const timestamp1m = kline[0]; // Open time

            // 2. Kéo dữ liệu Vĩ mô tương ứng
            const macroData = await this.fetchMacroData(symbolUpper, timestamp1m);

            // 3. Đóng gói thành chuẩn Schema MarketData
            const docData = {
                symbol: symbolUpper,
                timestamp: timestamp1m,
                // Vi mô
                open: parseFloat(kline[1]),
                high: parseFloat(kline[2]),
                low: parseFloat(kline[3]),
                close: parseFloat(kline[4]),
                volume: parseFloat(kline[5]),
                taker_buy_base: parseFloat(kline[9]),
                // Vĩ mô
                ...macroData
            };

            // 4. Lưu vào Database (Dùng updateOne với upsert: true để không lỗi unique index)
            await MarketData.updateOne(
                { symbol: symbolUpper, timestamp: timestamp1m },
                { $set: docData },
                { upsert: true }
            );

            console.log(`✅ [DATA PIPELINE] Đã lưu thành công 1 phút của ${symbolUpper} | Đứt mạng: ${macroData.isStaleData}`);
            return docData;

        } catch (error) {
            console.error(`❌ [DATA PIPELINE] Lỗi nghiêm trọng khi kéo dữ liệu ${symbol}:`, error.message);
        }
    }
}

module.exports = new MasterDataFetcher();