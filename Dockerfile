# Sử dụng Node.js 22 (phiên bản bạn đang dùng)
FROM node:22-alpine

# Cài đặt các gói cần thiết cho việc build native modules (nếu có)
RUN apk add --no-cache python3 make g++

# Đặt thư mục làm việc
WORKDIR /app

# Copy file package.json và package-lock.json trước để tận dụng cache layer
COPY package*.json ./

# Cài đặt dependencies
RUN npm ci --only=production

# Copy toàn bộ mã nguồn vào container
COPY . .

# Tạo volume cho thư mục logs để lưu trữ bền vững
VOLUME ["/app/logs"]

# Đảm bảo quyền thực thi cho entrypoint (nếu cần)
RUN chmod +x main.js

# Thiết lập biến môi trường mặc định (có thể ghi đè khi chạy)
ENV NODE_ENV=production
ENV TZ=UTC

# Expose cổng nếu sau này bạn muốn thêm API UI (tạm thời không cần)
# EXPOSE 3000

# Lệnh chạy bot
CMD ["node", "main.js"]