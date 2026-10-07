# syntax=docker/dockerfile:1
# 一次性验收服务：代码测试 + 页面构建检查 + API/HTTP 冒烟与场景验证。
# 运行结束后退出，以退出码报告结果（0 = 全部通过）。
FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# 页面构建检查需要 Node.js
ARG NODE_VERSION=20.18.0
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates xz-utils \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL "https://nodejs.org/dist/v${NODE_VERSION}/node-v${NODE_VERSION}-linux-x64.tar.xz" \
    | tar -xJ -C /usr/local --strip-components=1

WORKDIR /repo
COPY server/requirements.txt server/requirements.txt
COPY verify/requirements.txt verify/requirements.txt
RUN pip install -r server/requirements.txt -r verify/requirements.txt

COPY server/ ./server/
COPY web/ ./web/
COPY verify/ ./verify/
COPY pytest.ini ./pytest.ini

ENV APP_URL=http://app:8000
CMD ["python", "verify/run_verify.py"]
