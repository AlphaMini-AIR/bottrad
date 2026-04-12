# Tổng quan hệ thống bottrad

## 1. Tổng quan
Hệ thống bottrad là nền tảng giao dịch tự động đa chiến lược, đa sàn, tích hợp AI, quản lý vốn, portfolio, backtest, visualization, notification. Dữ liệu lưu trên MongoDB, dùng Redis cho cache/realtime, tích hợp CCXT cho đa sàn.

## 2. Các module chính
- **engine/**: backtest, indicators, strategyEngine, riskManager, positionManager...
- **services/**: exchangeService (CCXT), capitalManager, marketDataFetcher, notificationService, portfolioManager...
- **ml/**: pricePredictor, marketClassifier, historicalLearner, feedbackLoop...
- **database/models/**: SymbolConfig, Portfolio, PaperTrade, Kline, Capital, ActivePosition...

## 3. Các hàm chính, tác dụng, input/output
- Chi tiết từng hàm, input/output, tác dụng (xem phần tổng hợp ở trên).

## 4. Luồng xử lý tổng thể
```mermaid
flowchart TD
    A[MarketDataFetcher: Lấy dữ liệu nến, whale, radar, money flow] --> B[Analyzer/ML: Phân tích, dự báo, học lịch sử]
    B --> C[StrategyEngine: Ra quyết định giao dịch]
    C --> D[RiskManager: Tính khối lượng, kiểm soát rủi ro]
    D --> E[PositionManager: Mở/đóng/DCA vị thế]
    E --> F[CapitalManager: Cập nhật vốn]
    E --> G[PortfolioManager: Cập nhật portfolio]
    E --> H[NotificationService: Gửi thông báo]
    C --> I[Backtest/StrategyOptimizer: Đánh giá, tối ưu chiến lược]
    C --> J[ExchangeService (CCXT): Đặt lệnh đa sàn]
    subgraph AI/ML
      B
    end
    subgraph Backtest/Visualization
      I
    end
```

## 5. Cấu trúc dữ liệu chính
- SymbolConfig, ActivePosition, Portfolio, PaperTrade, Capital, Kline (xem chi tiết ở trên).

## 6. Đánh giá, bảo trì, mở rộng
- Dễ mở rộng, đa sàn, AI/ML, backtest mạnh, quản lý vốn/portfolio/rủi ro độc lập, notification dễ tích hợp.

## 7. Kết luận
Hệ thống đã tách biệt rõ các module, data flow minh bạch, dễ bảo trì, mở rộng, tích hợp AI, đa chiến lược, đa sàn, quản lý vốn/rủi ro/portfolio, notification, backtest, visualization.
