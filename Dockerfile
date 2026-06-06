FROM node:22-alpine AS builder
WORKDIR /app

COPY package.json ./
RUN npm install

COPY tsconfig.json ./
COPY src ./src

RUN npm run build

FROM node:22-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV PROMPTS_DIR=/prompts
ENV EXTRACTOR_PROMPT_FILE=/prompts/extractor.txt
ENV CURATOR_PROMPT_FILE=/prompts/curator.txt

COPY package.json ./
RUN npm install --omit=dev

COPY --from=builder /app/dist ./dist
COPY public-prompts /prompts
COPY public-prompts /app/prompts

EXPOSE 4827
CMD ["node", "--require", "./dist/preload.js", "dist/index.js"]
