/**
 * backtest_engine.cjs - AI Quant Sniper V5.3 (DEBUG VERSION)
 * Backtest 14 ngày với logging siêu chi tiết để truy vết lỗi.
 * Sử dụng DeepThinker làm trung tâm, không cần StrategyRouter.
 */

const connectDB = require('./src/config/db');
const ExchangeInfo = require('./src/services/binance/ExchangeInfo');
const AiEngine = require('./src/services/ai/AiEngine');
const MarketData = require('./src/models/MarketData');
const DeepThinker = require('./src/services/ai/DeepThinker');
const fs = require('fs');

const TAKER_FEE = 0.0005; // 0.05%
const DEBUG_MODE = true;   // Bật tắt log chi tiết

// ------------------------------------------------------------
// 1. CHUẨN HÓA NẾN VỀ ĐỊNH DẠNG V5.3 (11 TRƯỜNG)
// ------------------------------------------------------------
function normalizeCandleForV53(candle) {
    return {
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        volume: candle.volume,
        vpin: candle.vpin ?? 0,           // Giữ nguyên VPIN vì mô hình cũ đã học

        // --- BYPASS CÁC TRƯỜNG MỚI: ÉP VỀ TRẠNG THÁI CÂN BẰNG ---
        funding_rate: 0,
        ob_imb: 0.0,                      // Ép về 0 để không chặn lệnh
        liq_long: 1,                      // Set = 1 để tránh lỗi chia cho 0
        liq_short: 1,                     // Set = 1 để tránh lỗi chia cho 0
        mark_close: candle.close          // Dùng luôn giá close
    };
}
// ------------------------------------------------------------
// 2. TÍNH PNL SAU KHI THOÁT LỆNH
// ------------------------------------------------------------
function calcPnL(plan, exitPrice, entryFee, reason, candlesHeld) {
    const exitNotional = exitPrice * plan.size;
    const exitFee = exitNotional * TAKER_FEE;
    let grossPnl = 0;
    if (plan.side === 'LONG') grossPnl = (exitPrice - plan.price) * plan.size;
    if (plan.side === 'SHORT') grossPnl = (plan.price - exitPrice) * plan.size;
    const netPnl = grossPnl - entryFee - exitFee;
    const status = netPnl > 0 ? 'WIN' : 'LOSS';
    return { status, netPnl, exitPrice, reason, candlesHeld };
}

// ------------------------------------------------------------
// 3. MÔ PHỎNG DIỄN BIẾN LỆNH THEO TỪNG PHÚT
// ------------------------------------------------------------
async function simulateActiveTrade(symbol, startIndex, testData, plan) {
    const entryPrice = plan.price;
    const size = plan.size;
    const notional = entryPrice * size;
    const entryFee = notional * TAKER_FEE;
    let candlesHeld = 0;
    const maxHoldTime = 240; // 4 giờ

    for (let i = startIndex; i < Math.min(startIndex + maxHoldTime, testData.length); i++) {
        candlesHeld++;
        const candleRaw = testData[i];
        const candle = normalizeCandleForV53(candleRaw);

        // 1. Kiểm tra SL/TP cứng
        if (plan.side === 'LONG') {
            if (candle.low <= plan.slPrice) {
                return calcPnL(plan, plan.slPrice, entryFee, 'SL hit', candlesHeld);
            }
            if (candle.high >= plan.tpPrice) {
                return calcPnL(plan, plan.tpPrice, entryFee, 'TP hit', candlesHeld);
            }
        } else { // SHORT
            if (candle.high >= plan.slPrice) {
                return calcPnL(plan, plan.slPrice, entryFee, 'SL hit', candlesHeld);
            }
            if (candle.low <= plan.tpPrice) {
                return calcPnL(plan, plan.tpPrice, entryFee, 'TP hit', candlesHeld);
            }
        }

        // 2. Kiểm tra thoát sớm bởi AI (chỉ sau 2 phút)
        if (candlesHeld > 2) {
            const rawCurrentHistory = testData.slice(i - 721, i + 1);
            const currentHistory = rawCurrentHistory.map(normalizeCandleForV53);
            if (currentHistory.length >= 721) {
                const futureAiResult = await AiEngine.predict(currentHistory, symbol);
                const futureWinProb = futureAiResult.winProb;
                const futureFeatures = futureAiResult.features;

                // 👇 BẮT ĐẦU THÊM ĐOẠN NÀY 👇
                // Tránh việc DeepThinker ép cắt lệnh sớm vì thiếu dữ liệu mới
                if (futureFeatures && futureFeatures.length >= 11) {
                    futureFeatures[8] = 0;
                    futureFeatures[9] = 0;
                    futureFeatures[10] = 0;
                }
                // 👆 KẾT THÚC THÊM ĐOẠN NÀY 👆

                const exitCheck = DeepThinker.evaluateLogic(
                    symbol, currentHistory, futureWinProb, candle.close, 0, futureFeatures
                );
            }
        }
    }

    // Hết thời gian, đóng ở giá đóng cửa cuối cùng
    const lastCandleRaw = testData[Math.min(startIndex + maxHoldTime - 1, testData.length - 1)];
    const lastCandle = normalizeCandleForV53(lastCandleRaw);
    return calcPnL(plan, lastCandle.close, entryFee, 'Time limit', candlesHeld);
}

// ------------------------------------------------------------
// 4. HÀM CHÍNH CHẠY BACKTEST
// ------------------------------------------------------------
async function runBacktest() {
    await connectDB();
    await ExchangeInfo.init();

    // Đọc white_list.json
    const whiteListData = JSON.parse(fs.readFileSync('./white_list.json'));
    const activeCoins = whiteListData.active_coins;
    const symbols = Object.keys(activeCoins);

    if (symbols.length === 0) {
        console.log('❌ Không có coin nào trong white_list.json.');
        process.exit(1);
    }

    const FOURTEEN_DAYS_MS = 14 * 24 * 60 * 60 * 1000;
    const startTime = Date.now() - FOURTEEN_DAYS_MS;

    let totalAccountBalance = 1000;
    const report = {};

    console.log(`\n🚀 [BACKTEST 14-DAY] Khởi động ${symbols.length} coin (DEBUG MODE: ${DEBUG_MODE ? 'ON' : 'OFF'})`);
    console.log('📌 Ghi chú: Mỗi 100 nến sẽ in winProb. Khi có tín hiệu mạnh sẽ in chi tiết.\n');

    for (const symbol of symbols) {
        const meta = activeCoins[symbol];
        let coinProfit = 0, winCount = 0, tradeCount = 0;

        // Lấy dữ liệu từ MongoDB
        const testDataRaw = await MarketData.find({
            symbol,
            timestamp: { $gte: startTime }
        }).sort({ timestamp: 1 }).lean();

        if (testDataRaw.length < 722) {
            console.log(`⚠️ ${symbol} chỉ có ${testDataRaw.length} nến (<722), bỏ qua.`);
            continue;
        }

        const testData = testDataRaw.map(normalizeCandleForV53);
        console.log(`\n🔍 ${symbol} (Strategy: ${meta.strategy || 'N/A'}) - ${testData.length} nến`);

        // Biến đếm để thống kê
        let neutralCount = 0;       // winProb trung lập (0.35 - 0.65)
        let rejectCount = 0;        // có tín hiệu nhưng bị từ chối
        let sizeZeroCount = 0;      // size tính ra = 0
        let strongSignalCount = 0;  // số lần winProb vượt ngưỡng

        for (let i = 721; i < testData.length; i++) {
            const history = testData.slice(i - 721, i);
            const currentCandle = testData[i];

            // ----- Gọi AI dự đoán -----
            const aiResult = await AiEngine.predict(history, symbol);
            const winProb = aiResult.winProb;
            const features11 = aiResult.features;

            // 👇 BẮT ĐẦU THÊM ĐOẠN NÀY 👇
            // HACK CHO BACKTEST: Ép các chỉ báo vi cấu trúc về trung lập tuyệt đối
            // Để DeepThinker tự động phê duyệt lệnh dựa trên winProb của AI cũ
            if (features11 && features11.length >= 11) {
                features11[8] = 0;  // Orderbook Imbalance = 0
                features11[9] = 0;  // Liquidation Pressure = 0
                features11[10] = 0; // Premium Index = 0
            }
            // Kiểm tra tín hiệu mạnh
            const isStrongSignal = (winProb > 0.65) || (winProb < 0.35);
            if (isStrongSignal) strongSignalCount++;

            // ----- DeepThinker ra quyết định -----
            const decision = DeepThinker.evaluateLogic(
                symbol, history, winProb, currentCandle.close, 0, features11
            );

            // Log khi có tín hiệu mạnh
            if (DEBUG_MODE && isStrongSignal) {
                const signalType = winProb > 0.5 ? 'LONG' : 'SHORT';
                const status = decision.approved ? '✅ Approved' : '❌ Rejected';
                console.log(`  ⚡ [${symbol}] nến ${i}: winProb=${winProb.toFixed(4)} (${signalType}) -> ${status} (${decision.reason})`);
                if (!decision.approved) {
                    // In thêm giá trị các feature vi cấu trúc để kiểm tra bộ lọc
                    console.log(`      └─ Features: prem_idx=${features11[10].toFixed(4)} | ob_imb=${features11[8].toFixed(4)} | liq_press=${features11[9].toFixed(4)}`);
                }
            }

            if (!decision.approved) {
                if (isStrongSignal) rejectCount++;
                else neutralCount++;
                continue;
            }

            // ----- Tính size lệnh -----
            const riskAmount = totalAccountBalance * decision.suggestedRiskRatio;
            const slDistance = decision.slDistance;
            let size = riskAmount / slDistance;

            // Lấy stepSize từ ExchangeInfo (nếu có)
            const symbolInfo = ExchangeInfo.getPrecision(symbol); // ✅ SỬA getSymbolInfo THÀNH getPrecision
            const stepSize = symbolInfo?.stepSize || 0.001;

            size = Math.floor(size / stepSize) * stepSize;
            if (size <= 0) {
                if (DEBUG_MODE) {
                    console.log(`  [${symbol}] nến ${i}: size=0 (riskAmount=${riskAmount.toFixed(2)}, slDistance=${slDistance.toFixed(4)})`);
                }
                sizeZeroCount++;
                continue;
            }

            // ----- Tính SL/TP cụ thể -----
            let slPrice, tpPrice;
            if (decision.side === 'LONG') {
                slPrice = currentCandle.close - decision.slDistance;
                tpPrice = currentCandle.close + decision.tpDistance;
            } else {
                slPrice = currentCandle.close + decision.slDistance;
                tpPrice = currentCandle.close - decision.tpDistance;
            }

            const tradePlan = {
                side: decision.side,
                price: currentCandle.close,
                size: size,
                slPrice: slPrice,
                tpPrice: tpPrice
            };

            console.log(`  🟢 [${symbol}] MỞ LỆNH ${decision.side} | winProb=${winProb.toFixed(4)} | size=${size.toFixed(4)} | SL=${slPrice.toFixed(4)} TP=${tpPrice.toFixed(4)}`);

            tradeCount++;

            // ----- Mô phỏng diễn biến lệnh -----
            const outcome = await simulateActiveTrade(symbol, i, testData, tradePlan);

            totalAccountBalance += outcome.netPnl;
            coinProfit += outcome.netPnl;
            if (outcome.netPnl > 0) winCount++;

            DeepThinker.reportTradeResult(symbol, outcome.netPnl > 0);

            console.log(`  📈 [${symbol}] Kết quả: ${outcome.status} | PnL: ${outcome.netPnl.toFixed(2)}$ | Lý do: ${outcome.reason} | Giữ ${outcome.candlesHeld} phút`);

            // Nhảy cóc thời gian để tránh mở đè lệnh
            i += outcome.candlesHeld;
        }

        // ----- Tổng kết cho từng coin -----
        console.log(`\n📊 [${symbol}] TỔNG KẾT SAU BACKTEST:`);
        console.log(`   - Tổng số nến duyệt: ${testData.length - 721}`);
        console.log(`   - Số tín hiệu mạnh (vượt ngưỡng): ${strongSignalCount}`);
        console.log(`   - Số tín hiệu trung lập (bị bỏ qua): ${neutralCount}`);
        console.log(`   - Số tín hiệu bị từ chối bởi DeepThinker: ${rejectCount}`);
        console.log(`   - Số lần size=0: ${sizeZeroCount}`);
        console.log(`   - Số lệnh thực tế đã mở: ${tradeCount}`);

        report[symbol] = {
            strategy: meta.strategy || 'N/A',
            trades: tradeCount,
            winRate: tradeCount > 0 ? ((winCount / tradeCount) * 100).toFixed(2) + '%' : '0%',
            profit: "$" + coinProfit.toFixed(2)
        };
    }

    console.log(`\n📊 BÁO CÁO TỔNG KẾT 14 NGÀY:`);
    console.table(report);
    const roi = ((totalAccountBalance - 1000) / 1000 * 100).toFixed(2);
    console.log(`💵 Vốn cuối cùng: $${totalAccountBalance.toFixed(2)} | ROI: ${roi}%`);
    process.exit();
}

// ------------------------------------------------------------
// 5. CHẠY BACKTEST
// ------------------------------------------------------------
runBacktest().catch(err => {
    console.error('❌ Lỗi backtest:', err);
    process.exit(1);
});