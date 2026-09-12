# 冷链运输温度监控（演示版）

替代"司机电话 + 厂家后台"的人工盯车：建运输任务时定温度上下限，车载设备走
**MQTT** 上报温度和在线状态，超温 / 离线自动出告警，调度确认、升级、记录处理过程，
异常全程留痕可回放。

## 结构

```
server/
  db.py          # SQLite 表结构：任务 / 温度数据 / 告警 / 处理记录 / 设备状态
  core.py        # 核心业务：越限判定、告警状态机、去重、迟到数据合并、离线看门狗
  mqtt_ingest.py # MQTT 订阅端（telemetry / status 两个 topic）+ 离线看门狗线程
  broker.py      # 内嵌 MQTT broker（amqtt，纯 Python，无需 Docker/mosquitto）
  app.py         # Flask API + 单页看板（任务、告警操作、异常时间线）
simulator/
  device_sim.py  # 车载设备模拟器：断网本地缓存、恢复补传、历史消息重发
demo.py          # 一键演示：完整跑一趟"超温→处理→断网→补传→迟到数据"的运输
tests/
  test_core.py   # 去重、迟到不重开、新异常开新单、离线告警、状态机等 17 个用例
```

## 运行

```bash
pip install paho-mqtt amqtt        # 仅这两个三方依赖（另有 Flask）

python3 demo.py                    # 一键演示，约 12 秒看完整趟运输 + 异常时间线
python3 -m unittest discover -s tests   # 跑测试

# 单独起服务（带看板页面 http://127.0.0.1:5090）
python3 -m server.app --with-broker --http-port 5090 --mqtt-port 1883
# 再跑设备模拟器往 coldchain/{device_id}/telemetry 发数据即可
```

## MQTT 约定

| Topic | 载荷 | 说明 |
|---|---|---|
| `coldchain/{device_id}/telemetry` | `{msg_id, seq, temp, ts}` | QoS1；`ts` 是设备采样时刻 |
| `coldchain/{device_id}/status` | `{state: online\|offline}` | retained；设备掉线由 broker 发 LWT |

## 关键设计

**断网数据不丢**：样本带设备侧采样时刻 `ts` 和唯一 `msg_id`。设备断网时在本地缓存
（`DeviceSim.buffer`），恢复后原样补传；服务端按到达时间与采样时间的差值标记
`backfilled`，时间线里能看到哪些是补传的。

**重复消息不重复报警**：
- 温度数据按 `(device_id, msg_id)` 唯一约束去重，`INSERT OR IGNORE`，QoS1 重传、
  补报重发直接忽略，返回 `duplicated: true`；
- 同一次持续越限只开一张告警单，后续越限样本并入该单（延长异常窗口、刷新峰值），
  不刷屏。

**已处理的异常不被迟到数据改回**：告警状态机 `OPEN → ACKED → ESCALATED → RESOLVED`，
`RESOLVED` 是终态，任何操作（含数据写入）都不能改回。越限样本到达时：
1. 有未关闭的同类告警 → 并入该单；
2. 采样时刻落在某张**已关闭**告警的异常窗口内 → 是那次已处理异常的迟到数据，
   只修正统计并记一条 `LATE_DATA` 留痕，状态保持 RESOLVED；
3. 否则（包括落在两次已处理异常之间的空档）→ 是真正的新异常，开新单。

**离线判定双保险**：设备掉线时 broker 通过 LWT 发布 retained `offline`；服务端另有
看门狗线程，超过 `offline_grace_sec` 没收到任何消息也判离线（兜底 LWT 丢失、服务端
重启等场景）。设备恢复在线后离线告警自动关闭并留痕（`AUTO_RESOLVED`），温度告警则
必须人工处理关闭。

**完整时间线**：`GET /api/shipments/<id>/timeline` 把任务节点、越限样本（含补传标记）、
告警开单/确认/升级/处理记录/关闭按时间合并排序，看板页面同步展示。

## 主要接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/shipments` | 建运输任务 `{name, device_id, temp_min, temp_max, offline_grace_sec}`（同设备在途任务幂等） |
| POST | `/api/shipments/<id>/complete` | 完成任务（幂等） |
| GET  | `/api/shipments` · `/api/shipments/<id>/timeline` | 任务列表 · 异常时间线 |
| GET  | `/api/alerts?status=open&shipment_id=` | 告警列表 |
| POST | `/api/alerts/<id>/ack` · `/escalate` · `/resolve` | 确认 / 升级 / 关闭（非法迁移返回 409） |
| POST | `/api/alerts/<id>/notes` | 追加处理记录 |
| GET  | `/api/devices` | 设备在线状态 |

## 后续可扩展

- 阈值目前按任务配置，可加"任务进行中改阈值"（改动只影响之后到达的数据，历史判定不变）；
- 告警通知现在只在看板/接口，可接 webhook（钉钉/企业微信）在 OPENED/ESCALATED 时推送；
- 设备侧可加多探头（payload 加 `probe` 字段，按探头分别判定）；
- SQLite 换 PostgreSQL 即可上量，核心逻辑都集中在 `core.py`，不依赖 Flask。
