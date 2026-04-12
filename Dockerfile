# Dockerfile for bottrad (backend)
FROM node:20-alpine
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build
RUN rm -rf node_modules && npm install --production
CMD ["npm", "run", "start"]
