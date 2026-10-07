# syntax=docker/dockerfile:1

# ---- 前端构建 ----
FROM node:20-bookworm-slim AS webbuild
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY web/ ./
RUN npm run build

# ---- 运行时 ----
FROM python:3.11-slim AS runtime
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY server/requirements.txt ./server/requirements.txt
RUN pip install -r server/requirements.txt
COPY server/ ./server/
COPY --from=webbuild /web/dist ./web/dist
RUN mkdir -p /app/data
ENV WEB_DIST=/app/web/dist \
    DATABASE_PATH=/app/data/interlock.db \
    EVENT_LOG_MAX_PER_DRILL=10000
EXPOSE 8000
CMD ["uvicorn", "--factory", "server.app.main:build_app", "--host", "0.0.0.0", "--port", "8000"]
