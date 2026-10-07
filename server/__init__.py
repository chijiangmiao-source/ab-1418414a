"""束线联锁监看服务（beamline interlock monitor）。

- events  : 仅追加的事件日志，seq 为全局严格递增序号（水位）。
- channels: 状态投影，与事件写入在同一个 SQLite 事务中提交。
- 订阅    : 建连时固定水位并返回该水位内的一致快照，此后只推送更高序号事件。
"""
