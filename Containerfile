FROM node:22-alpine

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY common ./common
COPY DN ./DN
COPY RP ./RP
COPY etc ./etc

RUN mkdir -p DBdata
