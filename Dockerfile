 FROM node:20-bullseye

# =========================
# System deps for OCR + PDF
# =========================
RUN apt-get update && apt-get install -y \
  poppler-utils \
  tesseract-ocr \
  tesseract-ocr-ara \
  tesseract-ocr-eng \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# =========================
# App
# =========================
WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY . .

ENV NODE_ENV=production
ENV PORT=10000

EXPOSE 10000

CMD ["node", "server.js"]
