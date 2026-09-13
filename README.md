# 冷链运输温度监控（演示版）

替代"司机电话 + 厂家后台"的人工盯车：建运输任务时定温度上下限和负责人升级链，车载设备走
**MQTT** 上报温度和在线状态，超温 / 离线自动出告警并**按偏差与持续时长分级**，超过等待时间
**沿升级链自动升级给下一位负责人**（同级通知不重复），调度确认、升级、记录处理过程，
异常全程留痕可回放。

## 结构

```
server/
  db.py          # SQLite 表结构：任务 / 温度数据 / 告警（含级别与负责人）/ 处理记录 / 设备状态
  core.py        # 核心业务：越限判定、告警分级、状态机、超时自动升级、通知去重、迟到数据合并、
                 #   跨任务补传归属、离线看门狗
  mqtt_ingest.py # MQTT 订阅端（telemetry / status 两个 topic）+ 离线/升级看门狗线程
  broker.py      # 内嵌 MQTT broker（amqtt，纯 Python，无需 Docker/mosquitto）
  app.py         # Flask API + 单页看板（任务、告警操作、异常时间线、设备跨任务时间线）
simulator/
  device_sim.py  # 车载设备模拟器：断网本地缓存、恢复补传、历史消息重发
demo.py          # 一键演示：完整跑一趟"超温→分级→自动升级→处理→断网→补传→迟到数据"的运输，
                 #   外加断网横跨两趟任务的补传分窗与跨任务时间线
tests/
  test_core.py   # 去重、分级、自动升级、通知去重、迟到不重开不重升、离线恢复/继承、
                 # 跨任务补传归属、孤儿样本留存等 49 个用例
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

**断网数据不丢，补传各归各趟**：样本带设备侧采样时刻 `ts` 和唯一 `msg_id`。设备断网时在本地缓存
（`DeviceSim.buffer`），恢复后原样补传。服务端按 `ts` 落窗归属：采样时刻落在哪趟任务的
`[created_at, finished_at]` 窗口内就归哪趟——**哪怕那趟已经完成**；一段断网横跨两趟任务时，
缓存样本按各自窗口分别落到对应的任务上，不会全堆在当前在途的那趟。到达时刻与采样时刻差值
超过阈值标记 `backfilled`，时间线里能看到哪些是补传的（任务完成后补传的会特别标注）。

- 落到**已完成**任务的样本只能修正统计和时间线：越限样本落在某张告警的异常窗口内 → 并入该单
  （刷新峰值/窗口）并记 `LATE_DATA` 留痕，状态、级别、负责人、通知记录全部冻结；窗口外只留
  样本本身。绝不开新告警、绝不改级别、绝不换负责人。
- 哪个窗口都不在的样本（两趟之间的空档、任务开始前/结束后）按**孤儿样本**留存
  （`shipment_id=NULL`），`GET /api/telemetry/orphans` 可查，绝不开告警。
- 全程按 `(device_id, msg_id)` 唯一约束去重，重复补传仍然只算一次。
- `GET /api/devices/<id>/timeline` 给出设备视角的跨任务完整时间线：各趟任务节点、告警事件、
  越限样本、孤儿样本按时间合并，断网横跨几趟任务的补传全程可回放。

**重复消息不重复报警**：
- 温度数据按 `(device_id, msg_id)` 唯一约束去重，`INSERT OR IGNORE`，QoS1 重传、
  补报重发直接忽略，返回 `duplicated: true`；
- 同一次持续越限只开一张告警单，后续越限样本并入该单（延长异常窗口、刷新峰值），
  不刷屏。

**告警分级**：温度告警按`偏差`和`持续时长`定级（`core.SEVERITY_RULES`）：
偏差 ≥2℃ 或持续 ≥120s 升 **L2·严重**，偏差 ≥4℃ 或持续 ≥300s 升 **L3·紧急**，否则 **L1·提示**。
持续越限并入告警单时会重算级别，**只升不降**（`LEVEL_UP` 留痕）；离线告警无温度梯度，
固定定 L2。级别随每次越限样本刷新，严重的异常自然浮到上面，不会和普通提示挤在一起。

**超时自动升级**：任务可配 `escalation_chain`（负责人链）和 `escalate_after_sec`（等待时间）。
告警开单即指派链上第一位负责人；看门狗发现现任负责人超过等待时间还没处理完，自动把
`assignee` 推进给下一位并记 `ESCALATED`（actor=system）。确认/记录不暂停计时，只有关闭
（RESOLVED）或任务完成才停止；到链顶后不再升级。手动 `/escalate` 同样沿链推进。

**同级通知不重复**：通知按 `(级别, 负责人)` 去重——开单、级别上升、负责人更换时各发一次
（`NOTIFY` 事件，即 webhook 的挂接点），同一异常在同一级别对同一人绝不重发。

**已处理的异常不被迟到数据改回**：告警状态机 `OPEN → ACKED → ESCALATED → RESOLVED`，
`RESOLVED` 是终态，任何操作（含数据写入、自动升级）都不能改回。越限样本到达时：
1. 有未关闭的同类告警 → 并入该单；
2. 采样时刻落在某张**已关闭**告警的异常窗口内 → 是那次已处理异常的迟到数据，
   只修正统计并记一条 `LATE_DATA` 留痕，状态、级别、负责人、通知记录全部冻结；
3. 否则（包括落在两次已处理异常之间的空档）→ 是真正的新异常，开新单。

**离线判定双保险**：设备掉线时 broker 通过 LWT 发布 retained `offline`；服务端另有
看门狗线程，超过 `offline_grace_sec` 没收到任何消息也判离线（兜底 LWT 丢失、服务端
重启等场景）。设备恢复在线后离线告警自动关闭并留恢复记录（`AUTO_RESOLVED`），温度告警
必须人工处理关闭，不会被顺手关掉；重复上线 / retained 状态重投是幂等的，不会产生
重复恢复记录。

**新任务继承离线状态**：离线告警原本只靠"在线→离线"跳变触发；若设备在上一趟结束时
就已离线（或从未上线），新任务等不到跳变。看门狗为此补一条路径：在途任务的设备当前
离线且超过 `offline_grace_sec` 宽限（从任务创建时刻起算）→ 为该任务补开 OFFLINE 告警
（`watchdog-inherit`），同样定级、指派负责人、走升级链；该任务已有未关闭离线单则跳过，
重复检查不重复开单；宽限期内设备恢复则不开单。

**完整时间线**：`GET /api/shipments/<id>/timeline` 把任务节点、越限样本（含补传标记）、
告警开单/确认/升级/处理记录/关闭按时间合并排序，看板页面同步展示。

## 主要接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/shipments` | 建运输任务 `{name, device_id, temp_min, temp_max, offline_grace_sec, escalation_chain, escalate_after_sec}`（同设备在途任务幂等） |
| POST | `/api/shipments/<id>/complete` | 完成任务（幂等） |
| GET  | `/api/shipments` · `/api/shipments/<id>/timeline` | 任务列表 · 异常时间线 |
| GET  | `/api/devices/<id>/timeline` | 设备视角跨任务完整时间线（含孤儿样本） |
| GET  | `/api/telemetry/orphans?device_id=` | 无任务窗口的留存样本 |
| GET  | `/api/alerts?status=open&shipment_id=` | 告警列表（含 `severity` / `assignee`） |
| POST | `/api/alerts/<id>/ack` · `/escalate` · `/resolve` | 确认 / 升级（沿链推进负责人）/ 关闭（非法迁移返回 409） |
| POST | `/api/alerts/<id>/notes` | 追加处理记录 |
| GET  | `/api/devices` | 设备在线状态 |

## 后续可扩展

- 分级规则目前是 `core.SEVERITY_RULES` 全局常量，可下沉为按任务配置（改动只影响之后到达的数据）；
- 通知现在是 `NOTIFY` 事件（看板/时间线可查），接 webhook（钉钉/企业微信）即可真推送；
- 设备侧可加多探头（payload 加 `probe` 字段，按探头分别判定）；
- SQLite 换 PostgreSQL 即可上量，核心逻辑都集中在 `core.py`，不依赖 Flask。
