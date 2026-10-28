FROM node:20-alpine

ENV NODE_ENV=production
WORKDIR /app

COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund

COPY services ./services

WORKDIR /app/services
CMD ["node", "proyector/index.js"]
