// src/services/exchangeService.ts - Tích hợp API đa sàn qua CCXT
import ccxt, { Exchange } from 'ccxt';

export class ExchangeService {
    // Thay 'any' bằng type 'Exchange' của ccxt để code chuẩn TypeScript hơn
    public exchange: Exchange;

    constructor(exchangeId: string, apiKey?: string, secret?: string) {
        const opts: Record<string, any> = {
            enableRateLimit: true,
            timeout: 30000 // BỔ SUNG: Timeout 30 giây để tránh treo bot khi sàn lag
        };

        if (apiKey && secret) {
            opts.apiKey = apiKey;
            opts.secret = secret;
        }

        // @ts-ignore
        this.exchange = new ccxt[exchangeId](opts);
    }

    async fetchMarkets() {
        try {
            return await this.exchange.loadMarkets();
        } catch (error) {
            console.error(`[Exchange] Lỗi fetchMarkets:`, error);
            throw error;
        }
    }

    async fetchBalance() {
        try {
            return await this.exchange.fetchBalance();
        } catch (error) {
            console.error(`[Exchange] Lỗi fetchBalance:`, error);
            throw error;
        }
    }

    async fetchOHLCV(symbol: string, timeframe = '1h', since?: number, limit = 100) {
        try {
            return await this.exchange.fetchOHLCV(symbol, timeframe, since, limit);
        } catch (error) {
            // ĐẶC BIỆT: Lấy nến rất hay bị lỗi mạng tạm thời.
            // Trả về mảng rỗng [] thay vì throw error để bot không bị crash chu kỳ phân tích.
            console.error(`[Exchange] Lỗi fetchOHLCV cho ${symbol}:`, error instanceof Error ? error.message : String(error));
            return [];
        }
    }

    async createOrder(symbol: string, type: string, side: string, amount: number, price?: number, params = {}) {
        try {
            // Giữ nguyên logic của bạn, bọc try...catch để ghi log nếu kẹt Rate Limit / Thiếu tiền
            return await this.exchange.createOrder(symbol, type, side, amount, price, params);
        } catch (error) {
            console.error(`[Exchange] Lỗi đặt lệnh ${type} ${side} ${symbol}:`, error instanceof Error ? error.message : String(error));
            throw error;
        }
    }

    async fetchOpenOrders(symbol?: string) {
        try {
            return await this.exchange.fetchOpenOrders(symbol);
        } catch (error) {
            console.error(`[Exchange] Lỗi fetchOpenOrders cho ${symbol || 'all'}:`, error);
            throw error;
        }
    }

    async cancelOrder(id: string, symbol: string) {
        try {
            return await this.exchange.cancelOrder(id, symbol);
        } catch (error) {
            console.error(`[Exchange] Lỗi hủy lệnh ${id} của ${symbol}:`, error);
            throw error;
        }
    }
}