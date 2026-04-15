# Sử dụng Node.js 22 slim (Debian-based, glibc)
FROM node:22-slim

# Cài đặt các gói build (có thể không cần nhưng để an toàn)
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    make \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Đặt thư mục làm việc
WORKDIR /app

# Copy package.json và package-lock.json
COPY package*.json ./

# Cài đặt dependencies
RUN npm ci --only=production

# Copy toàn bộ mã nguồn
COPY . .

# Tạo thư mục logs và phân quyền (nếu cần)
RUN mkdir -p /app/logs && chmod +x main.js

# Thiết lập biến môi trường
ENV NODE_ENV=production
ENV TZ=UTC

# Lệnh chạy
CMD ["node", "main.js"]