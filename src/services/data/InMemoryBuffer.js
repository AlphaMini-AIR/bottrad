/**
 * src/services/data/InMemoryBuffer.js - Version 5.3 + Phase 2 (Stale Data Guard) + AI Snapshot
 * Bám sát tuyệt đối kiến trúc Flat Array & Zero GC Core.
 */
const fs = require('fs');
const path = require('path');

// Đường dẫn lưu file jsonl chờ đồng bộ lên MongoDB
const LIVE_DATA_DIR = path.join(__dirname, '../../../data/live_buffer');
const SYNC_FILE_PATH = path.join(LIVE_DATA_DIR, 'syncing.jsonl');

// Đảm bảo thư mục tồn tại
if (!fs.existsSync(LIVE_DATA_DIR)) {
    fs.mkdirSync(LIVE_DATA_DIR, { recursive: true });
}

// Định nghĩa 11 cột dữ liệu thô (Raw Data) theo đúng thứ tự:
const COLS = 11; 

class InMemoryBuffer {
    constructor(maxSize = 721) {
        this.maxSize = maxSize;
        this.buffers = {};
        this.lastCandleTime = {}; // Bộ nhớ đệm lưu Timestamp phục vụ bắt mạch nến rác
        
        // 🟢 [THÊM MỚI] Bộ đệm riêng lưu trữ Ảnh chụp Nến đóng cho AI dự đoán
        this.closedCandlesSnapshot = {}; 
    }

    initSymbol(symbol) {
        if (!this.buffers[symbol]) {
            this.buffers[symbol] = {
                // 🚀 Dùng một mảng phẳng duy nhất kích thước 721 * 11
                data: new Float32Array(this.maxSize * COLS), 
                pointer: 0,
                isFull: false
            };
        }
        // 🟢 [THÊM MỚI] Khởi tạo mảng snapshot cho AI
        if (!this.closedCandlesSnapshot[symbol]) {
            this.closedCandlesSnapshot[symbol] = [];
        }
    }

    // Nạp dữ liệu khi bot khởi động (GIỮ NGUYÊN)
    hydrate(symbol, historicalCandles) {
        this.initSymbol(symbol);
        historicalCandles.forEach(candle => {
            this.push(symbol, candle, 0, 1.0, 0, 0, candle.close || candle.c);
        });
        console.log(`⚡ [BUFFER] Đã nạp ${historicalCandles.length} nến của ${symbol} lên RAM (Flat Array 11 Cols).`);
    }

    // Thuật toán chẩn đoán Nến Rác (GIỮ NGUYÊN TỐI ĐA)
    evaluateStaleData(symbol, candleData) {
        let isStale = false;
        const currentOpenTime = parseInt(candleData.t || candleData.openTime);
        const volume = parseFloat(candleData.v || candleData.volume);
        
        const tradeCount = candleData.n !== undefined ? parseInt(candleData.n) : 100; 

        if (this.lastCandleTime[symbol] && currentOpenTime) {
            const timeDiff = currentOpenTime - this.lastCandleTime[symbol];
            if (timeDiff > 65000) isStale = true;
        }

        if (volume === 0 || tradeCount < 5) {
            isStale = true;
        }

        const open = parseFloat(candleData.o || candleData.open);
        const close = parseFloat(candleData.c || candleData.close);
        const high = parseFloat(candleData.h || candleData.high);
        const low = parseFloat(candleData.l || candleData.low);
        
        const bodySize = Math.abs(close - open);
        const totalWick = (high - low) - bodySize;
        
        if (bodySize > 0 && (totalWick / bodySize) > 40) {
            isStale = true;
        }

        if (currentOpenTime) this.lastCandleTime[symbol] = currentOpenTime;

        return isStale;
    }

    // Nhận 11 tham số từ StreamAggregator (GIỮ NGUYÊN)
    push(symbol, candle, vpin = 0, ob_imb = 1.0, liq_long = 0, liq_short = 0, mark_close = null) {
        this.initSymbol(symbol);
        const buf = this.buffers[symbol];
        const p = buf.pointer;
        const offset = p * COLS;

        buf.data[offset + 0] = parseFloat(candle.open || candle.o);
        buf.data[offset + 1] = parseFloat(candle.high || candle.h);
        buf.data[offset + 2] = parseFloat(candle.low || candle.l);
        buf.data[offset + 3] = parseFloat(candle.close || candle.c);
        buf.data[offset + 4] = parseFloat(candle.volume || candle.v);
        buf.data[offset + 5] = parseFloat(candle.funding_rate || 0);
        buf.data[offset + 6] = vpin;
        buf.data[offset + 7] = ob_imb;
        buf.data[offset + 8] = liq_long;
        buf.data[offset + 9] = liq_short;
        buf.data[offset + 10] = mark_close !== null ? parseFloat(mark_close) : parseFloat(candle.close || candle.c);

        buf.pointer++;
        if (buf.pointer >= this.maxSize) {
            buf.pointer = 0;
            buf.isFull = true;
        }

        if (candle.isFinal || candle.x) {
            const isStale = this.evaluateStaleData(symbol, candle);
            const record = {
                symbol: symbol,
                openTime: parseInt(candle.t),
                ohlcv: {
                    open: buf.data[offset + 0], high: buf.data[offset + 1],
                    low: buf.data[offset + 2], close: buf.data[offset + 3],
                    volume: buf.data[offset + 4], quoteVolume: parseFloat(candle.q || 0),
                    takerBuyQuote: parseFloat(candle.Q || 0), tradeCount: parseInt(candle.n || 0)
                },
                micro: {
                    ob_imb_top20: ob_imb, spread_close: 0, bid_vol_1pct: 0, ask_vol_1pct: 0,
                    max_buy_trade: 0, max_sell_trade: 0, liq_long_vol: liq_long, liq_short_vol: liq_short
                },
                macro: { funding_rate: buf.data[offset + 5] },
                isStaleData: isStale
            };

            fs.appendFile(SYNC_FILE_PATH, JSON.stringify(record) + '\n', (err) => {
                if (err) console.error(`❌ [InMemoryBuffer] Lỗi ghi file syncing:`, err.message);
            });
        }
    }

    // =========================================================================
    // 🟢 [CÁC HÀM BỔ SUNG MỚI ĐỂ VÁ LỖI "MÙ DỮ LIỆU" CHO AI]
    // =========================================================================

    /**
     * Hàm này sẽ được gọi từ StreamAggregator NGAY TRƯỚC KHI reset dữ liệu
     * Nó chụp ảnh toàn bộ Micro Data của cây nến vừa đóng băng để AI xài dần
     */
    pushFullClosedCandle(symbol, kline, microData) {
        this.initSymbol(symbol);
        
        // Deep copy để dữ liệu không bị biến thành 0 khi Global bị reset
        this.closedCandlesSnapshot[symbol].push({
            kline: JSON.parse(JSON.stringify(kline)),
            micro: JSON.parse(JSON.stringify(microData))
        });

        // Chỉ lưu 5 cây nến đóng gần nhất để không tốn RAM
        if (this.closedCandlesSnapshot[symbol].length > 5) {
            this.closedCandlesSnapshot[symbol].shift();
        }
    }

    /**
     * AiEngine.js sẽ gọi hàm này để lấy mảng dữ liệu Micro nguyên vẹn
     */
    getLatestClosedCandle(symbol) {
        const arr = this.closedCandlesSnapshot[symbol];
        return arr && arr.length > 0 ? arr[arr.length - 1] : null;
    }

    // =========================================================================
    // HÀM CŨ TƯƠNG THÍCH NGƯỢC (GIỮ NGUYÊN)
    // =========================================================================
    getHistoryObjects(symbol) {
        const buf = this.buffers[symbol];
        if (!buf || !buf.isFull) return null;

        const result = new Array(this.maxSize);
        let idx = buf.pointer;

        for (let i = 0; i < this.maxSize; i++) {
            const offset = idx * COLS;
            result[i] = {
                open: buf.data[offset + 0], high: buf.data[offset + 1],
                low: buf.data[offset + 2], close: buf.data[offset + 3],
                volume: buf.data[offset + 4], funding_rate: buf.data[offset + 5],
                vpin: buf.data[offset + 6], ob_imb: buf.data[offset + 7],
                liq_long: buf.data[offset + 8], liq_short: buf.data[offset + 9], mark_close: buf.data[offset + 10]
            };
            idx++;
            if (idx >= this.maxSize) idx = 0;
        }
        return result;
    }

    getFastSamples(symbol) {
        const buf = this.buffers[symbol];
        if (!buf || !buf.isFull) return null;

        const samples = [];
        let idx = buf.pointer;

        for (let i = 0; i < this.maxSize; i++) {
            const offset = idx * COLS;
            samples.push(buf.data.subarray(offset, offset + COLS));
            idx++;
            if (idx >= this.maxSize) idx = 0;
        }
        return samples;
    }

    isReady(symbol) {
        return this.buffers[symbol] && this.buffers[symbol].isFull;
    }
}

module.exports = new InMemoryBuffer();