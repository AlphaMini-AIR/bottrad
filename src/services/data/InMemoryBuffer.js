/**
 * src/services/data/InMemoryBuffer.js - Version 5.3 + Phase 2 (Stale Data Guard)
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
// 0:open, 1:high, 2:low, 3:close, 4:volume, 5:funding, 6:vpin, 7:ob_imb, 8:liq_long, 9:liq_short, 10:mark_close
const COLS = 11; 

class InMemoryBuffer {
    constructor(maxSize = 721) {
        this.maxSize = maxSize;
        this.buffers = {};
        this.lastCandleTime = {}; // Bộ nhớ đệm lưu Timestamp phục vụ bắt mạch nến rác
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
    }

    // Nạp dữ liệu khi bot khởi động
    hydrate(symbol, historicalCandles) {
        this.initSymbol(symbol);
        historicalCandles.forEach(candle => {
            this.push(symbol, candle, 0, 1.0, 0, 0, candle.close || candle.c);
        });
        console.log(`⚡ [BUFFER] Đã nạp ${historicalCandles.length} nến của ${symbol} lên RAM (Flat Array 11 Cols).`);
    }

    /**
     * Thuật toán chẩn đoán Nến Rác (Chạy ngầm không ảnh hưởng tới RAM Array)
     */
    evaluateStaleData(symbol, candleData) {
        let isStale = false;
        const currentOpenTime = parseInt(candleData.t || candleData.openTime);
        const volume = parseFloat(candleData.v || candleData.volume);
        
        // Số lệnh (Nếu nến API cũ không có thì gán 100 để né màng lọc)
        const tradeCount = candleData.n !== undefined ? parseInt(candleData.n) : 100; 

        // 1. BẮT MẠCH: Đứt gãy thời gian (Network Time Gap > 65 giây)
        if (this.lastCandleTime[symbol] && currentOpenTime) {
            const timeDiff = currentOpenTime - this.lastCandleTime[symbol];
            if (timeDiff > 65000) isStale = true;
        }

        // 2. BẮT MẠCH: Rỗng thanh khoản
        if (volume === 0 || tradeCount < 5) {
            isStale = true;
        }

        // 3. BẮT MẠCH: Quét râu dị thường (Dài gấp 40 lần thân nến)
        const open = parseFloat(candleData.o || candleData.open);
        const close = parseFloat(candleData.c || candleData.close);
        const high = parseFloat(candleData.h || candleData.high);
        const low = parseFloat(candleData.l || candleData.low);
        
        const bodySize = Math.abs(close - open);
        const totalWick = (high - low) - bodySize;
        
        if (bodySize > 0 && (totalWick / bodySize) > 40) {
            isStale = true;
        }

        // Cập nhật lại thời gian cho nhịp đo tiếp theo
        if (currentOpenTime) this.lastCandleTime[symbol] = currentOpenTime;

        return isStale;
    }

    // Nhận 11 tham số từ StreamAggregator
    push(symbol, candle, vpin = 0, ob_imb = 1.0, liq_long = 0, liq_short = 0, mark_close = null) {
        this.initSymbol(symbol);
        const buf = this.buffers[symbol];
        const p = buf.pointer;
        const offset = p * COLS;

        // Ghi đè dữ liệu vào đúng vị trí của Mảng vòng (CỐT LÕI V5.3 - Không đổi)
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

        // ==========================================
        // 🚀 PHASE 2 ADD-ON: Ghi nến mới vào syncing.jsonl
        // Chỉ ghi nến đang chạy Live (có thuộc tính candle.isFinal từ Websocket)
        // Bỏ qua nến lịch sử lúc hydrate để không bị lặp data.
        if (candle.isFinal || candle.x) {
            const isStale = this.evaluateStaleData(symbol, candle);
            
            const record = {
                symbol: symbol,
                openTime: parseInt(candle.t),
                ohlcv: {
                    open: buf.data[offset + 0],
                    high: buf.data[offset + 1],
                    low: buf.data[offset + 2],
                    close: buf.data[offset + 3],
                    volume: buf.data[offset + 4],
                    quoteVolume: parseFloat(candle.q || 0),
                    takerBuyQuote: parseFloat(candle.Q || 0),
                    tradeCount: parseInt(candle.n || 0)
                },
                micro: {
                    ob_imb_top20: ob_imb,
                    spread_close: 0, // Cần bổ sung nếu StreamAggregator có tính
                    bid_vol_1pct: 0,
                    ask_vol_1pct: 0,
                    max_buy_trade: 0,
                    max_sell_trade: 0,
                    liq_long_vol: liq_long,
                    liq_short_vol: liq_short
                },
                macro: {
                    funding_rate: buf.data[offset + 5]
                },
                isStaleData: isStale
            };

            // Ép Node.js đẩy thẳng chuỗi vào file rỗng (Asynchronous an toàn)
            fs.appendFile(SYNC_FILE_PATH, JSON.stringify(record) + '\n', (err) => {
                if (err) console.error(`❌ [InMemoryBuffer] Lỗi ghi file syncing:`, err.message);
            });
        }
        // ==========================================
    }

    // 🛡️ HÀM CŨ TƯƠNG THÍCH NGƯỢC (Backward Compatibility)
    getHistoryObjects(symbol) {
        const buf = this.buffers[symbol];
        if (!buf || !buf.isFull) return null;

        const result = new Array(this.maxSize);
        let idx = buf.pointer;

        for (let i = 0; i < this.maxSize; i++) {
            const offset = idx * COLS;
            result[i] = {
                open: buf.data[offset + 0],
                high: buf.data[offset + 1],
                low: buf.data[offset + 2],
                close: buf.data[offset + 3],
                volume: buf.data[offset + 4],
                funding_rate: buf.data[offset + 5],
                vpin: buf.data[offset + 6],
                ob_imb: buf.data[offset + 7],
                liq_long: buf.data[offset + 8],
                liq_short: buf.data[offset + 9],
                mark_close: buf.data[offset + 10]
            };
            idx++;
            if (idx >= this.maxSize) idx = 0;
        }
        return result;
    }

    // 🚀 HÀM MỚI CHO BƯỚC 4 (AiEngine V5.3) - KHÔNG ĐỔI
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