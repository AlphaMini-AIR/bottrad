module.exports = {
  apps: [
    // ==========================================
    // 1. LUỒNG ĐÔI MẮT (PYTHON DATA FEED)
    // ==========================================
    {
      name: "data-feed",
      script: "src/feed/feed_handler.py",
      interpreter: "python3", // Đảm bảo gọi đúng Python 3 trên VPS
      autorestart: true,      // Tự động khởi động lại nếu sập
      watch: false,           // Tắt watch trên VPS để tránh restart liên tục khi file thay đổi
      max_memory_restart: "500M", // Tự restart nếu ngốn quá 500MB RAM (chống rò rỉ bộ nhớ)
      env: {
        PYTHONIOENCODING: "utf-8" // Sửa triệt để lỗi crash do in Emoji (🔥, 🧠, 🚀)
      }
    },
    
    // ==========================================
    // 2. LUỒNG RADAR TUẦN TRA (NODE.JS)
    // ==========================================
    {
      name: "radar-scanner",
      script: "src/workers/RadarManager.js",
      autorestart: true,
      watch: false,
      max_memory_restart: "300M"
    },
    
    // ==========================================
    // 3. LUỒNG BỘ NÃO AI & BÓP CÒ (NODE.JS)
    // ==========================================
    {
      name: "order-manager",
      script: "src/services/execution/OrderManager_V17.js",
      autorestart: true,
      watch: false,
      max_memory_restart: "800M" // Bộ não ONNX ngốn RAM hơn một chút khi inference
    },
    
    // ==========================================
    // 4. LUỒNG GIAO DIỆN TRẠM KIỂM SOÁT (NODE.JS)
    // ==========================================
    {
      name: "watchtower-ui",
      script: "src/dashboard/server.js",
      autorestart: true,
      watch: false,
      max_memory_restart: "300M",
      env: {
        NODE_ENV: "production",
        PORT: 3000 // Chạy web ở port 3000
      }
    }
  ]
};