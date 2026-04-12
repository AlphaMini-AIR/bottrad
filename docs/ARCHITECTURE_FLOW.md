# Tổng hợp luồng hoạt động toàn hệ thống bot

## 1. Sơ đồ pipeline tổng thể

1. **Dữ liệu thị trường** (ExchangeService, marketDataFetcher)
   - Lấy nến, giá, volume, whale, money flow từ đa sàn (CCXT)
2. **Phân tích tín hiệu** (trendAnalyzer, indicators, marketClassifier)
   - Phân tích 3 tầng (4H, 1H, 15M), cộng hưởng whale/money flow, phân loại thị trường
3. **Động cơ chiến lược** (strategyEngine)
   - Chạy nhiều chiến lược song song, tối ưu hóa, backtest
4. **Quản lý vị thế** (positionManager)
   - Mở, đóng, nhồi lệnh DCA, chốt lời/cắt lỗ, ghi lịch sử
5. **Quản lý vốn/rủi ro** (riskManager)
   - Tính khối lượng lệnh, kiểm soát drawdown, circuit breaker, rate limit
6. **Quản lý danh mục** (portfolioManager)
   - Theo dõi tổng tài sản, PnL, phân bổ vốn đa coin
7. **Lưu trữ & báo cáo** (MongoDB, CSV, dashboard)
   - Lưu lịch sử, xuất báo cáo, phục vụ dashboard/giám sát

## 2. Mô tả chi tiết từng bước

### 1. Dữ liệu thị trường
- Sử dụng ExchangeService (CCXT) lấy dữ liệu nến, giá, volume, whale, money flow từ nhiều sàn.
- Dữ liệu được chuẩn hóa, lưu vào DB hoặc truyền trực tiếp cho pipeline phân tích.

### 2. Phân tích tín hiệu
- trendAnalyzer: Phân tích 3 tầng (EMA50 4H, volume 1H, RSI 15M), cộng hưởng whale/money flow, phát hiện trap.
- indicators: Tính toán EMA, SMA, MACD, RSI, Bollinger Bands.
- marketClassifier: Phân loại thị trường (UPTREND, DOWNTREND, SIDEWAY) để chọn chiến lược phù hợp.

### 3. Động cơ chiến lược
- strategyEngine: Chạy nhiều chiến lược song song, mỗi chiến lược có thể tối ưu tham số riêng, hỗ trợ backtest đa timeframe.

### 4. Quản lý vị thế
- positionManager: Mở vị thế mới, nhồi lệnh DCA, chốt lời/cắt lỗ, đóng vị thế, ghi lịch sử vào DB.

### 5. Quản lý vốn/rủi ro
- riskManager: Tính khối lượng lệnh tối ưu, kiểm soát drawdown, circuit breaker ngắt giao dịch khi rủi ro vượt ngưỡng, rate limiter chống spam API.

### 6. Quản lý danh mục
- portfolioManager: Theo dõi tổng tài sản, PnL, phân bổ vốn đa coin, cập nhật trạng thái danh mục theo thời gian thực.

### 7. Lưu trữ & báo cáo
- Lưu toàn bộ lịch sử giao dịch, vị thế, danh mục vào MongoDB.
- Xuất báo cáo CSV, phục vụ dashboard frontend hoặc giám sát từ xa.

## 3. Luồng hoạt động mẫu (scalping)
1. Lấy nến BTCUSDT từ Binance qua ExchangeService
2. Phân tích tín hiệu với trendAnalyzer (EMA50 4H, volume 1H, RSI 15M)
3. Nếu có tín hiệu, strategyEngine chọn chiến lược phù hợp
4. positionManager mở vị thế, quản lý DCA/chốt lời/cắt lỗ
5. riskManager kiểm soát khối lượng, drawdown, circuit breaker
6. portfolioManager cập nhật danh mục, tính PnL
7. Lưu lịch sử, xuất báo cáo

## 4. Nhận xét tổng thể
- Hệ thống module hóa, dễ mở rộng, kiểm thử từng phần độc lập.
- Đáp ứng chuẩn quốc tế về quản lý vốn, rủi ro, đa chiến lược, đa sàn, portfolio.
- Có thể tích hợp AI/ML, dashboard, hoặc mở rộng sang spot/option dễ dàng.
