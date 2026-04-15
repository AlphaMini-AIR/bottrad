/**
 * src/services/data/InMemoryBuffer.js - Version 5.3 (Flat Array & Zero GC Core)
 */

// Định nghĩa 11 cột dữ liệu thô (Raw Data) theo đúng thứ tự:
// 0:open, 1:high, 2:low, 3:close, 4:volume, 5:funding, 6:vpin, 7:ob_imb, 8:liq_long, 9:liq_short, 10:mark_close
const COLS = 11; 

class InMemoryBuffer {
    constructor(maxSize = 721) {
        this.maxSize = maxSize;
        this.buffers = {};
    }

    initSymbol(symbol) {
        if (!this.buffers[symbol]) {
            this.buffers[symbol] = {
                // 🚀 BẢN VÁ: Dùng một mảng phẳng duy nhất kích thước 721 * 11
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
            // Dữ liệu cũ từ DB chưa có Orderbook, Thanh lý, Mark Price
            // -> Gán mặc định an toàn: ob_imb = 1.0 (cân bằng), liq = 0, mark_close = close
            this.push(symbol, candle, 0, 1.0, 0, 0, candle.close || candle.c);
        });
        console.log(`⚡ [BUFFER] Đã nạp ${historicalCandles.length} nến của ${symbol} lên RAM (Flat Array 11 Cols).`);
    }

    // Nhận 11 tham số từ StreamAggregator
    push(symbol, candle, vpin = 0, ob_imb = 1.0, liq_long = 0, liq_short = 0, mark_close = null) {
        this.initSymbol(symbol);
        const buf = this.buffers[symbol];
        const p = buf.pointer;
        const offset = p * COLS;

        // Ghi đè dữ liệu vào đúng vị trí của Mảng vòng
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
    }

    // 🛡️ HÀM CŨ TƯƠNG THÍCH NGƯỢC (Backward Compatibility):
    // Phục vụ cho main.js V5.2 chưa kịp cập nhật, tránh crash hệ thống.
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

    // 🚀 HÀM MỚI CHO BƯỚC 4 (AiEngine V5.3):
    // Trả về mảng View (.subarray) không tốn 1 byte RAM nào cho bộ Garbage Collector.
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


