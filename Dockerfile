FROM node:20-bullseye

# =========================
# System deps for OCR + PDF rendering
# =========================
 RUN apt-get update && apt-get install -y \
  build-essential \
  python3 \
  pkg-config \
  libcairo2-dev \
  libpango1.0-dev \
  libjpeg-dev \
  libgif-dev \
  librsvg2-dev \
  tesseract-ocr \
  tesseract-ocr-ara \
  tesseract-ocr-eng \
  poppler-utils \
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

