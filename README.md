# 束线联锁监看（Beamline Interlock Monitor）

值班员监看联锁状态的"快照 + 增量"订阅服务：建连先取得**某一提交水位内完整且
一致的通道快照**，随后连续接收该水位之后的变化。连接建立期间的事件不会漏报，
也不会重复报警——每个事件恰好归入快照或增量之一。

## 业务规则

- 可创建演练（drill），在演练下提交带**稳定事件标识** `event_id` 的通道状态变更。
- 事件日志仅追加，`seq` 为**全局严格递增**序号（水位）。
- 状态投影（各通道当前状态）与事件写入在**同一个 SQLite 事务**中提交
  （`BEGIN IMMEDIATE`：插事件 → 更新投影 → 登记幂等键，同事务提交）。
- 相同 `(drill, event_id)` + 相同通道/状态重传：返回**原序号**，不写新事件、
  不改投影；以不同通道或状态复用该标识：**409 拒绝**，投影不被改写。
- 订阅（SSE `/stream`）：
  - 无游标：建连时在订阅条件变量下**固定水位 W**，原子返回 W 内一致快照，
    此后仅推送 `seq > W`；
  - 带 `after=N`（断线恢复）：先校验游标在可恢复范围内，仅推送 `seq > N`，
    边界严格大于，重连边界事件天然不重复；
  - 日志截断或游标超前水位（超出可恢复范围）：`/resume` 与 `/stream` 均返回
    **409 `snapshot_required`**，页面显示醒目横幅，明确要求重新获取快照。

## 运行

运行时仅依赖 Python 3.11 标准库；页面为原生 HTML/JS，无构建步骤。

```bash
python3 -m server.main            # 默认 0.0.0.0:8080，DB /data/interlock.db
PORT=8080 DB_PATH=/tmp/x.db python3 -m server.main
# 打开 http://localhost:8080
```

Docker Compose：

```bash
docker compose up --build web     # 服务
docker compose run --rm verify    # 一次性验收（见下）
```

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/drills` | 创建演练 `{id?, name?}` |
| GET  | `/api/drills` | 演练列表 |
| GET  | `/api/drills/{id}` | 快照（固定水位、通道投影）+ 最近事件 |
| POST | `/api/drills/{id}/events` | 提交变更 `{event_id, channel, state}` |
| GET  | `/api/drills/{id}/resume?after=N` | 断线补齐校验/取增量（409=需重取快照） |
| GET  | `/api/drills/{id}/stream?after=N` | SSE；无游标先 `snapshot`，有游标先 `resumed` |
| POST | `/api/drills/{id}/trim` | 截断 `{before_seq}`（抬升可恢复下限） |

SSE 帧：`event: snapshot`（水位内快照）、`event: resumed`（恢复元数据）、
`event: event`（单条增量，按全局序号排列）、`: ping` 心跳。

## verify（一次性验收服务）

`python3 -m verify.run`（或 compose 的 `verify` 服务）启动真实 HTTP/SSE 服务后
执行并**以退出码报告结果**（0=全部通过）：

1. 构建检查：`compileall`、`node --check web/app.js`、静态页面可访问且含关键元素；
2. 代码测试：`unittest`（事务原子性、幂等标识、标识复用拒绝、截断边界、100 并发写）；
3. API/HTTP 冒烟：CRUD、幂等重传原序号、冲突 409、参数 400、未知演练 404；
4. 并发写入与订阅交界：建连固定水位，快照/增量恰好分区（含**建连期间并发写入**
   的接力场景，校验无漏报、无重复、快照投影等价于水位内事件回放）；
5. 断线补齐：以前次已应用序号恢复，边界严格大于，重复 resume 幂等；
6. 过期游标：截断后 REST 与 SSE 均 409 `snapshot_required`，重取快照后恢复增量；
7. Compose 中存在执行后退出的 `verify` 服务定义。

## 目录

```
server/    HTTP/SSE 服务（stdlib http.server + SQLite WAL）
web/       监看页（index.html / app.js / styles.css）
tests/     存储层单元测试
verify/    一次性验收服务
```
