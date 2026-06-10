# --- Build stage ---
FROM node:24-alpine AS build

RUN npm install -g pnpm@11

WORKDIR /app

# Install dependencies first so they cache independently of source changes
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile

COPY . .
RUN pnpm build

# --- Runtime stage ---
FROM nginxinc/nginx-unprivileged:1.29-alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

# nginx-unprivileged runs as uid 101 and listens on 8080
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget -qO /dev/null http://127.0.0.1:8080/ || exit 1
