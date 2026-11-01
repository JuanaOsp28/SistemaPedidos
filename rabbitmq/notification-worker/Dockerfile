FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY worker.js ./
# Sin EXPOSE: el worker no ofrece ningun endpoint, solo consume de RabbitMQ.
CMD ["node", "worker.js"]
