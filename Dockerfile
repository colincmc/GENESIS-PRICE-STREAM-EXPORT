FROM node:20.20.0-slim
WORKDIR /app
COPY package.json tsconfig.json ./
RUN npm install && npm cache clean --force
COPY src ./src
RUN npx tsc
RUN npm prune --production
EXPOSE 8794
CMD ["node", "dist/index.js"]
