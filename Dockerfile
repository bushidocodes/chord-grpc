FROM node:13-alpine

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm ci --quiet

COPY . .

EXPOSE 8440
