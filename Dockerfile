 FROM node:20-bullseye

RUN apt-get update && apt-get install -y \
  tesseract-ocr \
  tesseract-ocr-ara \
  tesseract-ocr-eng \
  poppler-utils \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY . .

ENV NODE_ENV=production
ENV PORT=10000
EXPOSE 10000

CMD ["node", "server.js"]
