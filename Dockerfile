FROM node:22-alpine AS builder
WORKDIR /app

COPY build-context/ ./
RUN npm ci --workspace @persistio/server --include-workspace-root=false

COPY tsconfig.json packages/server/tsconfig.json
COPY src packages/server/src

RUN npm run build --workspace @persistio/server

FROM node:22-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV PROMPTS_DIR=/prompts
ENV EXTRACTOR_PROMPT_FILE=/prompts/extractor.txt
ENV CURATOR_PROMPT_FILE=/prompts/curator.txt

COPY build-context/ ./
RUN npm ci --omit=dev --workspace @persistio/server --include-workspace-root=false

COPY --from=builder /app/packages/server/dist packages/server/dist
COPY prompts/extractor.txt prompts/curator.txt prompts/manifest.json /prompts/
COPY prompts/extractor.txt prompts/curator.txt prompts/manifest.json /app/prompts/

WORKDIR /app/packages/server
EXPOSE 4827
CMD ["node", "--require", "./dist/preload.js", "dist/index.js"]
