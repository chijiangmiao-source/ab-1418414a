FROM python:3.11-slim

# verify 的页面构建检查需要 node --check；运行时仅用 Python 标准库。
RUN apt-get update \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

ENV HOST=0.0.0.0 \
    PORT=8080 \
    DB_PATH=/data/interlock.db

EXPOSE 8080

HEALTHCHECK --interval=10s --timeout=3s --retries=5 \
    CMD python -c "import http.client,sys; c=http.client.HTTPConnection('127.0.0.1',8080,timeout=2); c.request('GET','/api/drills'); sys.exit(0 if c.getresponse().status==200 else 1)"

CMD ["python", "-m", "server.main"]
