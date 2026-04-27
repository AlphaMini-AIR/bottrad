module.exports = {
  apps: [
    // ==========================================
    // 0. HEALTH CHECK - chỉ chạy 1 lần trước khi start thật
    // ==========================================
    // Lưu ý:
    // PM2 không nên giữ health-check chạy liên tục.
    // Nên chạy thủ công bằng:
    // node src/services/ops/SystemHealthCheck.js

    // ==========================================
    // 1. LUỒNG ĐÔI MẮT - PYTHON DATA FEED V19
    // ==========================================
    {
      name: "data-feed",
      script: "src/services/feed/feed_handler.py",
      interpreter: "python3",
      autorestart: true,
      watch: false,
      max_memory_restart: "700M",
      restart_delay: 3000,
      env: {
        PYTHONIOENCODING: "utf-8",
        REDIS_URL: "redis://127.0.0.1:6379"
      }
    },

    // ==========================================
    // 2. LUỒNG RADAR TUẦN TRA - NODE.JS
    // ==========================================
    {
      name: "radar-scanner",
      script: "src/workers/RadarManager.js",
      autorestart: true,
      watch: false,
      max_memory_restart: "400M",
      restart_delay: 3000,
      env: {
        NODE_ENV: "production"
      }
    },

    // ==========================================
    // 3. LUỒNG GHI NHỚ FEATURE - PHỤC VỤ TRAIN ONNX MỖI ĐÊM
    // ==========================================
    {
      name: "feature-recorder",
      script: "src/services/learning/FeatureRecorder.js",
      autorestart: true,
      watch: false,
      max_memory_restart: "500M",
      restart_delay: 3000,
      env: {
        NODE_ENV: "production",
        FEATURE_ARCHIVE_PREFIX: "features:archive",
        FEATURE_ARCHIVE_SAMPLE_INTERVAL_MS: 1000,
        FEATURE_ARCHIVE_TTL_SEC: 259200,
        FEATURE_ARCHIVE_MAXLEN: 300000
      }
    },

    // ==========================================
    // 4. LUỒNG BỘ NÃO AI & BÓP CÒ
    // ==========================================
    {
      name: "order-manager",
      script: "src/services/execution/OrderManager.js",
      autorestart: true,
      watch: false,
      max_memory_restart: "1000M",
      restart_delay: 5000,
      env: {
        NODE_ENV: "production",

        // Giai đoạn an toàn nhất hiện tại
        EXCHANGE_MODE: "PAPER",
        LIVE_DRY_RUN: "true",

        // Model
        MODEL_VERSION: "Universal_Scout.onnx"
      }
    },

    // ==========================================
    // 5. LUỒNG GIAO DIỆN TRẠM KIỂM SOÁT
    // ==========================================
    {
      name: "watchtower-ui",
      script: "src/services/dashboard/DashboardServer.js",
      autorestart: true,
      watch: false,
      max_memory_restart: "400M",
      restart_delay: 3000,
      env: {
        NODE_ENV: "production",
        DASHBOARD_HOST: "0.0.0.0",
        DASHBOARD_PORT: 3010
      }
    },

    {
      name: "hourly-learning",
      script: "src/training/hourly_self_learning.py",
      interpreter: "/root/bottrad/.venv/bin/python",
      args: "--loop",
      autorestart: true,
      watch: false,
      max_memory_restart: "1200M",
      env: {
        PYTHONIOENCODING: "utf-8"
      }
    }
  ]
};