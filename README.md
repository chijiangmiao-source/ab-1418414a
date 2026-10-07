# 束线联锁监看台

束线值班员联锁状态监看系统：先取得某一提交水位内完整一致的通道快照，再连续接收该水位之后的变化；连接建立期间的事件不漏报、不重复报警，每个事件恰好归入**快照**或**增量**之一。

## 功能与语义

- **演练管理**：创建演练、按演练提交带稳定事件标识（`event_id`）的通道状态变更。
- **监看页**：实时显示当前水位、各通道状态、按全局序号排列的最近事件、边界重发去重计数、可恢复起点；断线自动重连，游标过期时明确提示并自动重新获取快照。
- **同事务写入**：状态投影（`channel_state`）与严格递增的全局事件日志（`events.seq`，AUTOINCREMENT）在同一 SQLite 事务中提交。
- **订阅分界**：SSE 订阅建立时固定水位并发送其内快照，之后只推送更高序号的事件；`after=N` 恢复时只发送 `seq > N` 的事件，边界重发由服务端与页面双重去重。
- **游标失效**：日志按每演练保留窗口截断（`EVENT_LOG_MAX_PER_DRILL`）。游标超出可恢复范围（已截断或超过当前水位）时：
  - REST `GET .../events?after=N` 返回 **410**，`detail.code = resync_required`；
  - SSE 流发送单条 **`resync`** 帧后结束；
  - 页面收到后提示“游标已超出可恢复范围”并自动重新获取快照。
- **幂等提交**：相同 `event_id` 且内容相同的重传返回原序号（HTTP 200，`deduplicated: true`）；以不同通道/状态/演练复用该标识返回 **409**，且不改写投影。

## API 摘要

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/drills` | 创建演练 |
| GET | `/api/drills` | 演练列表 |
| GET | `/api/drills/{id}` | 演练详情（水位、可恢复起点、通道投影） |
| GET | `/api/drills/{id}/snapshot` | 水位内一致快照（水位 + 通道 + 最近事件） |
| POST | `/api/drills/{id}/events` | 提交状态变更（幂等）；201 新建 / 200 去重 / 409 冲突 |
| GET | `/api/drills/{id}/events?after=&limit=` | 增量事件；410 表示需重新获取快照 |
| GET | `/api/drills/{id}/stream?after=` | SSE：`snapshot`/`resume`/`event`/`resync` 帧 |
| GET | `/api/health` | 健康检查（含当前水位与保留窗口） |

## 运行

```bash
# 启动系统（页面与 API 同端口）
docker compose up --build app          # http://localhost:8080

# 运行一次性验收服务（执行后退出，以退出码报告结果）
docker compose up --build --exit-code-from verify verify
```

`verify` 服务依次执行：代码测试（pytest）→ 页面构建检查（npm build）→ API/HTTP 冒烟 → 并发写入与订阅交界 → 断线补齐与边界去重 → 过期游标重快照，全部通过时退出码为 0，否则为 1。

> Compose 中 `app` 的 `EVENT_LOG_MAX_PER_DRILL=120` 是演示用的小保留窗口，使“过期游标”场景可被真实触发；生产部署请调大。

## 本地开发

```bash
python3 -m venv .venv && .venv/bin/pip install -r server/requirements.txt -r verify/requirements.txt
.venv/bin/uvicorn --factory server.app.main:build_app --reload   # API: :8000

cd web && npm ci && npm run dev      # 页面开发服务器（代理 /api 到 :8000）
npm run build                        # 页面构建检查

.venv/bin/python -m pytest           # 代码测试
APP_URL=http://127.0.0.1:8000 .venv/bin/python verify/run_verify.py   # 完整验收
```

## 结构

```
server/app/db.py      SQLite 存储：同事务写入、严格递增序号、幂等、游标校验
server/app/main.py    FastAPI：REST + SSE 订阅（快照/增量分界）
server/tests/         pytest：API、幂等、订阅边界、断线恢复、过期游标
web/                  React + TS 监看页（Vite 构建）
verify/run_verify.py  一次性验收服务（退出码报告结果）
docker-compose.yml    app（系统）+ verify（验收）
```
