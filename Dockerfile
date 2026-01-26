FROM node:20-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
  tesseract-ocr \
  tesseract-ocr-ara \
  tesseract-ocr-eng \
  ca-certificates \
  && rm -rf /var/lib/apt/lists/*

COPY package*.json ./
RUN npm install

COPY . .

ENV PORT=10000
EXPOSE 10000

CMD ["npm", "start"]
