FROM node:20-alpine
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY index.js ./
# Sin EXPOSE: el consumidor no ofrece ningun endpoint, solo lee de Kafka.
CMD ["node", "index.js"]
