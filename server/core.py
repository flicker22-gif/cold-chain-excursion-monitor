"""核心业务逻辑：温度判定、告警分级与生命周期、超时自动升级、去重、迟到数据合并、
跨任务补传归属、离线看门狗。

所有函数接收显式的 `now`（epoch 秒），单测可以注入确定性时间。
每个函数自开自关事务（BEGIN IMMEDIATE），MQTT 线程 / HTTP 线程 / 看门狗线程并发安全。
"""
import json
import sqlite3

from . import db as dbmod

OPEN_STATUSES = ("OPEN", "ACKED", "ESCALATED")

# 告警状态机：合法迁移。RESOLVED 是终态——已处理的异常不能被任何数据改回。
# ack 允许从 ESCALATED 确认（升级不妨碍“我来处理”）；escalate 允许在 ESCALATED
# 上重复执行，以便沿升级链继续往上推。
_TRANSITIONS = {
    "ack":      {"from": {"OPEN", "ESCALATED"}, "to": "ACKED", "event": "ACKED"},
    "escalate": {"from": {"OPEN", "ACKED", "ESCALATED"}, "to": "ESCALATED", "event": "ESCALATED"},
    "resolve":  {"from": {"OPEN", "ACKED", "ESCALATED"}, "to": "RESOLVED", "event": "RESOLVED"},
}

# 到达时刻比采样时刻晚超过该值，视为断网期间的补传数据
BACKFILL_AGE_SEC = 1.0

# 温度告警分级：偏差（℃）或持续时长（秒）任一达到阈值即定该级；级别只升不降。
SEVERITY_LEVELS = ("L1", "L2", "L3")
SEVERITY_LABELS = {"L1": "提示", "L2": "严重", "L3": "紧急"}
SEVERITY_RULES = {
    "L2": {"deviation": 2.0, "duration": 120.0},
    "L3": {"deviation": 4.0, "duration": 300.0},
}
# 在途失联没有温度梯度可言，固定按“严重”定级
OFFLINE_SEVERITY = "L2"


class DomainError(Exception):
    """业务规则冲突（如非法状态迁移），API 层映射为 409。"""


# ---------------------------------------------------------------- 运输任务

def create_shipment(db_path, name, device_id, temp_min, temp_max, offline_grace_sec, now,
                    escalation_chain=None, escalate_after_sec=60.0):
    if temp_min >= temp_max:
        raise DomainError("temp_min 必须小于 temp_max")
    chain = [str(x).strip() for x in (escalation_chain or []) if str(x).strip()]
    if escalate_after_sec <= 0:
        raise DomainError("escalate_after_sec 必须大于 0")
    with dbmod.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        dup = conn.execute(
            "SELECT id FROM shipments WHERE device_id=? AND status='IN_TRANSIT'", (device_id,)
        ).fetchone()
        if dup:  # 同一台车同时只能有一个在途任务，重复创建返回已有任务（幂等）
            return get_shipment(db_path, dup["id"])
        cur = conn.execute(
            "INSERT INTO shipments(name, device_id, temp_min, temp_max, offline_grace_sec,"
            " escalation_chain, escalate_after_sec, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (name, device_id, temp_min, temp_max, offline_grace_sec,
             json.dumps(chain, ensure_ascii=False), escalate_after_sec, now),
        )
        conn.commit()
        return get_shipment(db_path, cur.lastrowid)


def complete_shipment(db_path, shipment_id, now):
    with dbmod.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT * FROM shipments WHERE id=?", (shipment_id,)).fetchone()
        if not row:
            raise DomainError("任务不存在")
        if row["status"] == "COMPLETED":
            return get_shipment(db_path, shipment_id)  # 幂等
        conn.execute(
            "UPDATE shipments SET status='COMPLETED', finished_at=? WHERE id=?", (now, shipment_id)
        )
        conn.commit()
        return get_shipment(db_path, shipment_id)


def get_shipment(db_path, shipment_id):
    with dbmod.connect(db_path) as conn:
        row = conn.execute("SELECT * FROM shipments WHERE id=?", (shipment_id,)).fetchone()
        return dict(row) if row else None


def list_shipments(db_path):
    with dbmod.connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM shipments ORDER BY id DESC").fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------- 温度数据接入

def _find_shipment_for_ts(conn, device_id, device_ts):
    """按设备采样时刻归属任务：窗口 [created_at, finished_at]（在途任务视为 +∞）。

    断网横跨两趟任务时，恢复后补传的缓存样本按各自窗口分别落窗——
    哪怕那趟已经完成，也归当时那趟运输，而不是当前在途的任务。
    """
    return conn.execute(
        "SELECT * FROM shipments WHERE device_id=? AND created_at<=?"
        " AND (finished_at IS NULL OR finished_at>=?) ORDER BY id DESC LIMIT 1",
        (device_id, device_ts, device_ts),
    ).fetchone()


def ingest_telemetry(db_path, device_id, msg_id, seq, temp, device_ts, now):
    """接入一条温度数据。

    归属规则（按设备采样时刻 device_ts 落窗）：
    - 采样时刻落在某趟任务的窗口内 → 归该趟，哪怕它已经完成；
    - 实时样本（到达≈采样）兜底归在途任务，容忍设备时钟轻微偏差；
    - 哪个窗口都不在（两趟之间的空档、任务开始前/结束后）→ 孤儿样本：
      去重后留存（shipment_id=NULL），可查，绝不开告警。

    处理规则：
    - 按 (device_id, msg_id) 唯一约束去重：重复消息/补报重发只算一次；
    - 在途任务：仅实时样本更新设备在线状态（补传的历史样本证明不了链路已恢复）；
      越限时：有未关闭告警则并入，落在已关闭告警窗口内则记 LATE_DATA 留痕，
      否则开新告警；
    - 已完成任务：只修正统计与时间线——越限样本落在某张告警窗口内则并入该单
      并记 LATE_DATA（状态/级别/负责人/通知记录全部冻结），窗口外只留样本本身，
      绝不开新单、绝不改级别、绝不换负责人。
    """
    with dbmod.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        backfilled = 1 if now - device_ts > BACKFILL_AGE_SEC else 0
        ship = _find_shipment_for_ts(conn, device_id, device_ts)
        if ship is None and not backfilled:
            ship = conn.execute(
                "SELECT * FROM shipments WHERE device_id=? AND status='IN_TRANSIT'"
                " ORDER BY id DESC LIMIT 1",
                (device_id,),
            ).fetchone()
        if ship is None:
            # 孤儿样本：采样时刻不在任何任务窗口内。按明确规则留存，随时可查。
            cur = conn.execute(
                "INSERT OR IGNORE INTO telemetry"
                " (device_id, shipment_id, msg_id, seq, temp, device_ts, arrived_at, backfilled)"
                " VALUES (?,NULL,?,?,?,?,?,?)",
                (device_id, msg_id, seq, temp, device_ts, now, backfilled),
            )
            conn.commit()
            return {"accepted": True, "duplicated": cur.rowcount == 0,
                    "orphan": True, "backfilled": bool(backfilled)}

        cur = conn.execute(
            "INSERT OR IGNORE INTO telemetry"
            " (device_id, shipment_id, msg_id, seq, temp, device_ts, arrived_at, backfilled)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (device_id, ship["id"], msg_id, seq, temp, device_ts, now, backfilled),
        )
        if not backfilled and ship["status"] == "IN_TRANSIT":
            # 只有实时样本能证明链路此刻是通的：置在线并顺带关闭挂着的离线告警。
            # 断网补传的是历史样本，不碰在线状态/心跳，避免“设备在线、离线单却开着”的状态翻转。
            _mark_online(conn, device_id, now, source="telemetry")
        if cur.rowcount == 0:
            conn.commit()
            return {"accepted": True, "duplicated": True}

        result = {"accepted": True, "duplicated": False, "backfilled": bool(backfilled),
                  "shipment_id": ship["id"]}
        if ship["status"] == "COMPLETED":
            # 已完成任务：补传样本只能修正统计和时间线
            result["shipment_completed"] = True
            if temp > ship["temp_max"] or temp < ship["temp_min"]:
                result["alert"] = _merge_late_into_completed(conn, ship, temp, device_ts, now)
            conn.commit()
            return result
        if temp > ship["temp_max"]:
            result["alert"] = _on_violation(conn, ship, "TEMP_HIGH", temp, device_ts, now)
        elif temp < ship["temp_min"]:
            result["alert"] = _on_violation(conn, ship, "TEMP_LOW", temp, device_ts, now)
        else:
            _maybe_recovered(conn, ship, temp, device_ts, now)
        conn.commit()
        return result


def _merge_late_into_completed(conn, ship, temp, ts, now):
    """已完成任务上的越限补传：落在某张告警窗口内 → 修正统计并记 LATE_DATA 留痕；
    窗口外 → 只留样本（时间线可见）。绝不开新单，状态/级别/负责人/通知记录全部冻结。"""
    alert_type = "TEMP_HIGH" if temp > ship["temp_max"] else "TEMP_LOW"
    a = conn.execute(
        "SELECT * FROM alerts WHERE shipment_id=? AND type=? AND first_ts<=? AND last_ts>=?"
        " ORDER BY id DESC LIMIT 1",
        (ship["id"], alert_type, ts, ts),
    ).fetchone()
    if not a:
        return None  # 样本已留存，时间线里仍能看到这条越限
    _merge_into_alert(conn, a, temp, ts)
    _add_event(
        conn, a["id"], "LATE_DATA", "system",
        f"任务完成后补传（temp={temp}℃, device_ts={ts:.3f}）：仅修正统计，"
        f"状态/级别/负责人冻结", now,
    )
    return {"alert_id": a["id"], "late": True, "shipment_completed": True}


def _on_violation(conn, ship, alert_type, temp, ts, now):
    open_alert = conn.execute(
        f"SELECT * FROM alerts WHERE shipment_id=? AND type=?"
        f" AND status IN ({','.join('?' * len(OPEN_STATUSES))}) ORDER BY id DESC LIMIT 1",
        (ship["id"], alert_type, *OPEN_STATUSES),
    ).fetchone()
    if open_alert:
        _merge_into_alert(conn, open_alert, temp, ts)
        _refresh_severity(conn, ship, open_alert["id"], now)
        return {"alert_id": open_alert["id"], "merged": True}

    # 迟到数据：采样时刻落在某张已处理告警的异常窗口内 → 属于那次已处理的异常，
    # 修正统计、留痕，但状态保持 RESOLVED，绝不开新单；级别/负责人/通知记录也
    # 一并冻结——已处理的异常不能被旧数据重新升级。
    resolved = conn.execute(
        "SELECT * FROM alerts WHERE shipment_id=? AND type=? AND status='RESOLVED'"
        " AND first_ts<=? AND last_ts>=? ORDER BY id DESC LIMIT 1",
        (ship["id"], alert_type, ts, ts),
    ).fetchone()
    if resolved:
        _merge_into_alert(conn, resolved, temp, ts)
        _add_event(
            conn, resolved["id"], "LATE_DATA", "system",
            f"迟到数据并入已处理告警（temp={temp}℃, device_ts={ts:.3f}），状态与级别不变", now,
        )
        return {"alert_id": resolved["id"], "late": True}

    severity = _classify_temp(alert_type, temp, 0.0, ship)
    chain = _chain_of(ship)
    assignee = chain[0] if chain else None
    cur = conn.execute(
        "INSERT INTO alerts(shipment_id, device_id, type, severity, assignee, assignee_idx,"
        " assignee_since, opened_at, first_ts, last_ts, peak_temp, detail)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (ship["id"], ship["device_id"], alert_type, severity, assignee, 0,
         now if assignee else None, now, ts, ts, temp,
         f"温度越{'上' if alert_type == 'TEMP_HIGH' else '下'}限"),
    )
    _add_event(conn, cur.lastrowid, "OPENED", "system",
               f"温度 {temp}℃ 越限（阈值 {ship['temp_min']}~{ship['temp_max']}℃），"
               f"定级 {severity}·{SEVERITY_LABELS[severity]}"
               + (f"，负责人 {assignee}" if assignee else ""), now)
    _maybe_notify(conn, cur.lastrowid, now)
    return {"alert_id": cur.lastrowid, "opened": True}


def _merge_into_alert(conn, alert, temp, ts):
    if alert["type"] == "TEMP_HIGH":
        peak = max(alert["peak_temp"], temp)
    elif alert["type"] == "TEMP_LOW":
        peak = min(alert["peak_temp"], temp)
    else:
        peak = alert["peak_temp"]
    conn.execute(
        "UPDATE alerts SET first_ts=MIN(first_ts, ?), last_ts=MAX(last_ts, ?), peak_temp=? WHERE id=?",
        (ts, ts, peak, alert["id"]),
    )


# ---------------------------------------------------------------- 分级与通知

def _chain_of(ship):
    return json.loads(ship["escalation_chain"] or "[]")


def _classify_temp(alert_type, peak_temp, duration_sec, ship):
    """按温度偏差与持续时长定级（L1/L2/L3）。"""
    if alert_type == "TEMP_HIGH":
        deviation = peak_temp - ship["temp_max"]
    else:
        deviation = ship["temp_min"] - peak_temp
    level = "L1"
    for lv in ("L2", "L3"):
        rule = SEVERITY_RULES[lv]
        if deviation >= rule["deviation"] or duration_sec >= rule["duration"]:
            level = lv
    return level


def _refresh_severity(conn, ship, alert_id, now):
    """未关闭告警并入新越限样本后重算级别：只升不降，升级留痕并通知现任负责人。"""
    a = conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
    if not a or a["status"] not in OPEN_STATUSES or a["type"] == "OFFLINE":
        return
    duration = a["last_ts"] - a["first_ts"]
    new = _classify_temp(a["type"], a["peak_temp"], duration, ship)
    if SEVERITY_LEVELS.index(new) <= SEVERITY_LEVELS.index(a["severity"]):
        return
    if a["type"] == "TEMP_HIGH":
        deviation = a["peak_temp"] - ship["temp_max"]
    else:
        deviation = ship["temp_min"] - a["peak_temp"]
    conn.execute("UPDATE alerts SET severity=? WHERE id=?", (new, alert_id))
    _add_event(conn, alert_id, "LEVEL_UP", "system",
               f"级别上升 {a['severity']} → {new}（偏差 {deviation:.1f}℃，持续 {duration:.0f}s）", now)
    _maybe_notify(conn, alert_id, now)


def _maybe_notify(conn, alert_id, now):
    """给现任负责人发通知。同一告警的 (级别, 负责人) 组合只通知一次——
    同一异常不重复发同级通知；级别上升或负责人更换时才再发。"""
    a = conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
    if not a or not a["assignee"]:
        return
    if (a["notified_severity"] == a["severity"]
            and a["notified_assignee_idx"] == a["assignee_idx"]):
        return
    label = f"{a['severity']}·{SEVERITY_LABELS[a['severity']]}"
    if a["type"] == "OFFLINE":
        text = f"通知 {a['assignee']}：设备离线（{label}）"
    else:
        text = (f"通知 {a['assignee']}：{a['type']} 告警 {label}"
                f"（峰值 {a['peak_temp']}℃，已持续 {a['last_ts'] - a['first_ts']:.0f}s）")
    _add_event(conn, alert_id, "NOTIFY", "system", text, now)
    conn.execute(
        "UPDATE alerts SET notified_severity=?, notified_assignee_idx=? WHERE id=?",
        (a["severity"], a["assignee_idx"], alert_id),
    )


def _maybe_recovered(conn, ship, temp, ts, now):
    """温度回到正常区间：给未关闭的温度告警补一条 RECOVERED 记录（不自动关单，仍需人工处理）。"""
    for alert_type in ("TEMP_HIGH", "TEMP_LOW"):
        a = conn.execute(
            f"SELECT * FROM alerts WHERE shipment_id=? AND type=?"
            f" AND status IN ({','.join('?' * len(OPEN_STATUSES))}) ORDER BY id DESC LIMIT 1",
            (ship["id"], alert_type, *OPEN_STATUSES),
        ).fetchone()
        if not a or ts <= a["last_ts"]:
            continue  # 迟到/乱序的正常样本不算恢复
        last = conn.execute(
            "SELECT action FROM alert_events WHERE alert_id=? ORDER BY id DESC LIMIT 1", (a["id"],)
        ).fetchone()
        if not last or last["action"] != "RECOVERED":
            _add_event(conn, a["id"], "RECOVERED", "system",
                       f"温度恢复至 {temp}℃（正常区间），等待人工确认处理", now)


# ---------------------------------------------------------------- 设备在线状态

def set_device_online(db_path, device_id, online, now, source="status"):
    """设备上线/离线。离线开 OFFLINE 告警；上线自动关闭该设备的离线告警（留痕）。"""
    with dbmod.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        prev = conn.execute("SELECT * FROM device_state WHERE device_id=?", (device_id,)).fetchone()
        if online:
            _mark_online(conn, device_id, now, source=source)
        else:
            conn.execute(
                "INSERT INTO device_state(device_id, online, last_seen) VALUES (?,0,?)"
                " ON CONFLICT(device_id) DO UPDATE SET online=0",
                (device_id, now),
            )
            if prev and prev["online"]:
                ship = conn.execute(
                    "SELECT * FROM shipments WHERE device_id=? AND status='IN_TRANSIT'"
                    " ORDER BY id DESC LIMIT 1",
                    (device_id,),
                ).fetchone()
                if ship:
                    _open_offline_alert(
                        conn, ship, now,
                        note=f"设备离线，超过 {ship['offline_grace_sec']}s 无数据（{source}）")
        conn.commit()


def _open_offline_alert(conn, ship, now, note):
    """开一张 OFFLINE 告警单：定级、指派升级链第一位负责人并通知。调用方需已持有事务。"""
    chain = _chain_of(ship)
    assignee = chain[0] if chain else None
    cur = conn.execute(
        "INSERT INTO alerts(shipment_id, device_id, type, severity, assignee,"
        " assignee_idx, assignee_since, opened_at, first_ts, last_ts, detail)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (ship["id"], ship["device_id"], "OFFLINE", OFFLINE_SEVERITY, assignee, 0,
         now if assignee else None, now, now, now, note),
    )
    _add_event(conn, cur.lastrowid, "OPENED", "system",
               note + f"，定级 {OFFLINE_SEVERITY}·{SEVERITY_LABELS[OFFLINE_SEVERITY]}"
               + (f"，负责人 {assignee}" if assignee else ""), now)
    _maybe_notify(conn, cur.lastrowid, now)
    return cur.lastrowid


def check_timeouts(db_path, now):
    """看门狗，覆盖两类离线判定：

    1. 在途任务的设备超过 offline_grace_sec 没有任何消息 → 判离线
       （MQTT 的 LWT 能覆盖大部分掉线；这里兜底 LWT 丢失、服务端重启等场景）；
    2. 新任务继承离线状态：任务创建时设备就已经离线（上一趟结束时掉线未恢复）
       或从未上线，等不到“在线→离线”跳变 → 宽限期后为该任务补开 OFFLINE 告警；
       该任务已有未关闭离线单则跳过，重复检查不重复开单。
    """
    with dbmod.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT ds.device_id, ds.last_seen, s.offline_grace_sec FROM device_state ds"
            " JOIN shipments s ON s.device_id = ds.device_id AND s.status='IN_TRANSIT'"
            " WHERE ds.online=1"
        ).fetchall()
        pending = conn.execute(
            "SELECT s.id, s.created_at, s.offline_grace_sec, ds.online FROM shipments s"
            " LEFT JOIN device_state ds ON ds.device_id = s.device_id"
            " WHERE s.status='IN_TRANSIT'"
        ).fetchall()
    for r in rows:
        if r["last_seen"] is not None and now - r["last_seen"] > r["offline_grace_sec"]:
            set_device_online(db_path, r["device_id"], False, now, source="watchdog")
    for r in pending:
        if r["online"]:
            continue  # 设备在线（含从未见过设备时的 NULL → 按离线处理）
        if now - r["created_at"] > r["offline_grace_sec"]:
            _inherit_offline_alert(db_path, r["id"], now)


def _inherit_offline_alert(db_path, shipment_id, now):
    """为在途任务补开 OFFLINE 告警（任务开始时设备已离线/从未上线，宽限期后仍未恢复）。

    幂等：该任务已有未关闭的离线单则直接返回；设备在宽限期内恢复在线也不开单。
    """
    with dbmod.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        ship = conn.execute(
            "SELECT * FROM shipments WHERE id=? AND status='IN_TRANSIT'", (shipment_id,)
        ).fetchone()
        if not ship:
            return
        ds = conn.execute(
            "SELECT * FROM device_state WHERE device_id=?", (ship["device_id"],)
        ).fetchone()
        if ds and ds["online"]:
            return  # 设备已恢复在线
        if now - ship["created_at"] <= ship["offline_grace_sec"]:
            return  # 仍在宽限期内
        dup = conn.execute(
            f"SELECT id FROM alerts WHERE shipment_id=? AND type='OFFLINE'"
            f" AND status IN ({','.join('?' * len(OPEN_STATUSES))})",
            (shipment_id, *OPEN_STATUSES),
        ).fetchone()
        if dup:
            return  # 重复检查不重复开单
        if ds is None:
            note = f"设备从未上线，超过宽限 {ship['offline_grace_sec']}s 未收到任何数据（watchdog-inherit）"
        else:
            note = f"任务开始时设备已离线，宽限 {ship['offline_grace_sec']}s 后仍未恢复（watchdog-inherit）"
        _open_offline_alert(conn, ship, now, note=note)
        conn.commit()


def check_escalations(db_path, now):
    """升级看门狗：未关闭告警的现任负责人超过 escalate_after_sec 没处理完 →
    自动升级给链上的下一位负责人并通知。

    只扫描 OPEN/ACKED/ESCALATED 且任务在途的告警：RESOLVED 是终态，已处理的
    异常绝不会被（含迟到数据在内的）任何机制重新升级；确认/记录不暂停计时，
    只有关闭或任务完成才停止。
    """
    with dbmod.connect(db_path) as conn:
        rows = conn.execute(
            f"SELECT a.id FROM alerts a"
            f" JOIN shipments s ON s.id=a.shipment_id AND s.status='IN_TRANSIT'"
            f" WHERE a.status IN ({','.join('?' * len(OPEN_STATUSES))})",
            OPEN_STATUSES,
        ).fetchall()
    for r in rows:
        _auto_escalate(db_path, r["id"], now)


def _auto_escalate(db_path, alert_id, now):
    with dbmod.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        a = conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
        if not a or a["status"] not in OPEN_STATUSES:
            return  # 扫描后被人工处理掉了
        ship = conn.execute(
            "SELECT * FROM shipments WHERE id=? AND status='IN_TRANSIT'", (a["shipment_id"],)
        ).fetchone()
        if not ship:
            return
        chain = _chain_of(ship)
        idx = a["assignee_idx"] + 1
        if not chain or idx >= len(chain):
            return  # 无升级链，或已在最高负责人处
        since = a["assignee_since"] if a["assignee_since"] is not None else a["opened_at"]
        if now - since < ship["escalate_after_sec"]:
            return  # 现任负责人还没超时
        conn.execute(
            "UPDATE alerts SET status='ESCALATED', assignee_idx=?, assignee=?, assignee_since=?"
            " WHERE id=?",
            (idx, chain[idx], now, alert_id),
        )
        _add_event(conn, alert_id, "ESCALATED", "system",
                   f"超过 {ship['escalate_after_sec']:.0f}s 未处理完，自动升级："
                   f"负责人 → {chain[idx]}", now)
        _maybe_notify(conn, alert_id, now)
        conn.commit()


def _mark_online(conn, device_id, now, source):
    """标记设备在线（实时数据或 online 状态消息调用）。

    在线状态发生 离线→在线 跳变时，自动关闭该设备所有未关闭的 OFFLINE 告警并留痕，
    保证设备状态与告警状态一致。调用方需已持有事务。
    """
    prev = conn.execute(
        "SELECT * FROM device_state WHERE device_id=?", (device_id,)
    ).fetchone()
    conn.execute(
        "INSERT INTO device_state(device_id, online, last_seen) VALUES (?,1,?)"
        " ON CONFLICT(device_id) DO UPDATE SET online=1, last_seen=excluded.last_seen",
        (device_id, now),
    )
    if not prev or not prev["online"]:
        rows = conn.execute(
            f"SELECT * FROM alerts WHERE device_id=? AND type='OFFLINE'"
            f" AND status IN ({','.join('?' * len(OPEN_STATUSES))})",
            (device_id, *OPEN_STATUSES),
        ).fetchall()
        for a in rows:
            conn.execute(
                "UPDATE alerts SET status='RESOLVED', resolved_at=? WHERE id=?", (now, a["id"])
            )
            _add_event(conn, a["id"], "AUTO_RESOLVED", "system",
                       f"设备恢复在线（{source}），离线告警自动关闭", now)


def list_devices(db_path):
    with dbmod.connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM device_state ORDER BY device_id").fetchall()
        return [dict(r) for r in rows]


# ---------------------------------------------------------------- 告警处理

def transition_alert(db_path, alert_id, op, actor, note, now):
    """确认 / 升级 / 关闭。非法迁移（含操作已关闭告警）抛 DomainError。

    手动升级会沿任务配置的升级链把负责人推进一位并通知新负责人；
    已在链顶则只记升级事件，负责人不变。
    """
    if op not in _TRANSITIONS:
        raise DomainError(f"未知操作: {op}")
    rule = _TRANSITIONS[op]
    with dbmod.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        a = conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
        if not a:
            raise DomainError("告警不存在")
        if a["status"] not in rule["from"]:
            raise DomainError(f"告警当前状态 {a['status']}，不能执行 {op}")
        resolved_at = now if rule["to"] == "RESOLVED" else None
        conn.execute(
            "UPDATE alerts SET status=?, resolved_at=COALESCE(?, resolved_at) WHERE id=?",
            (rule["to"], resolved_at, alert_id),
        )
        if op == "escalate":
            ship = conn.execute(
                "SELECT * FROM shipments WHERE id=?", (a["shipment_id"],)
            ).fetchone()
            chain = _chain_of(ship)
            idx = a["assignee_idx"] + 1
            if chain and idx < len(chain):
                conn.execute(
                    "UPDATE alerts SET assignee_idx=?, assignee=?, assignee_since=? WHERE id=?",
                    (idx, chain[idx], now, alert_id),
                )
                note = (note + "；" if note else "") + f"负责人 → {chain[idx]}"
        _add_event(conn, alert_id, rule["event"], actor, note, now)
        if op == "escalate":
            _maybe_notify(conn, alert_id, now)
        conn.commit()
        return get_alert(db_path, alert_id)


def add_note(db_path, alert_id, actor, note, now):
    """记录处理过程（未关闭的告警）。"""
    with dbmod.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        a = conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
        if not a:
            raise DomainError("告警不存在")
        if a["status"] == "RESOLVED":
            raise DomainError("告警已关闭，不能追加处理记录")
        _add_event(conn, alert_id, "NOTE", actor, note, now)
        conn.commit()
        return get_alert(db_path, alert_id)


def get_alert(db_path, alert_id):
    with dbmod.connect(db_path) as conn:
        a = conn.execute("SELECT * FROM alerts WHERE id=?", (alert_id,)).fetchone()
        if not a:
            return None
        events = conn.execute(
            "SELECT * FROM alert_events WHERE alert_id=? ORDER BY id", (alert_id,)
        ).fetchall()
        out = dict(a)
        out["events"] = [dict(e) for e in events]
        return out


def list_alerts(db_path, status=None, shipment_id=None):
    sql, args = "SELECT * FROM alerts WHERE 1=1", []
    if status == "open":
        sql += f" AND status IN ({','.join('?' * len(OPEN_STATUSES))})"
        args += list(OPEN_STATUSES)
    elif status:
        sql += " AND status=?"
        args.append(status)
    if shipment_id:
        sql += " AND shipment_id=?"
        args.append(shipment_id)
    sql += " ORDER BY id DESC"
    with dbmod.connect(db_path) as conn:
        return [dict(r) for r in conn.execute(sql, args).fetchall()]


def _add_event(conn, alert_id, action, actor, note, now):
    conn.execute(
        "INSERT INTO alert_events(alert_id, action, actor, note, created_at) VALUES (?,?,?,?,?)",
        (alert_id, action, actor, note, now),
    )


# ---------------------------------------------------------------- 时间线

def list_orphan_telemetry(db_path, device_id=None):
    """孤儿样本：到达时采样时刻不在任何任务窗口内（两趟之间的空档、任务开始前/
    结束后的补传）。按明确规则留存（shipment_id=NULL），随时可查。"""
    sql, args = "SELECT * FROM telemetry WHERE shipment_id IS NULL", []
    if device_id:
        sql += " AND device_id=?"
        args.append(device_id)
    sql += " ORDER BY device_ts"
    with dbmod.connect(db_path) as conn:
        return [dict(r) for r in conn.execute(sql, args).fetchall()]


def _shipment_timeline_items(conn, ship):
    """一趟任务的时间线条目：任务节点、告警事件、越限样本（含补传/完成后补传标记）。"""
    items = [{
        "ts": ship["created_at"], "kind": "shipment", "shipment_id": ship["id"],
        "text": f"任务创建：{ship['name']}（{ship['device_id']}，阈值 "
                f"{ship['temp_min']}~{ship['temp_max']}℃）",
    }]
    if ship["finished_at"]:
        items.append({"ts": ship["finished_at"], "kind": "shipment",
                      "shipment_id": ship["id"], "text": "任务完成"})

    alerts = conn.execute(
        "SELECT * FROM alerts WHERE shipment_id=? ORDER BY id", (ship["id"],)
    ).fetchall()
    for a in alerts:
        events = conn.execute(
            "SELECT * FROM alert_events WHERE alert_id=? ORDER BY id", (a["id"],)
        ).fetchall()
        for e in events:
            items.append({
                "ts": e["created_at"], "kind": "alert_event", "shipment_id": ship["id"],
                "alert_id": a["id"], "alert_type": a["type"], "action": e["action"],
                "actor": e["actor"], "text": e["note"],
            })

    violations = conn.execute(
        "SELECT * FROM telemetry WHERE shipment_id=? AND (temp<? OR temp>?) ORDER BY device_ts",
        (ship["id"], ship["temp_min"], ship["temp_max"]),
    ).fetchall()
    for v in violations:
        note = ""
        if v["backfilled"]:
            note = "（断网补传）"
            if ship["finished_at"] and v["arrived_at"] > ship["finished_at"]:
                note = "（任务完成后补传，仅修正统计）"
        items.append({
            "ts": v["device_ts"], "kind": "violation", "shipment_id": ship["id"],
            "text": f"越限样本 {v['temp']}℃" + note,
            "temp": v["temp"], "backfilled": bool(v["backfilled"]),
            "arrived_at": v["arrived_at"],
        })
    return items


def get_timeline(db_path, shipment_id):
    """一次运输的完整时间线：任务节点、越限样本、告警开单/处理/关闭，按时间排序。"""
    with dbmod.connect(db_path) as conn:
        ship = conn.execute("SELECT * FROM shipments WHERE id=?", (shipment_id,)).fetchone()
        if not ship:
            return None
        items = _shipment_timeline_items(conn, ship)
        items.sort(key=lambda x: (x["ts"], 0 if x["kind"] == "shipment" else 1))
        return {"shipment": dict(ship), "items": items}


def get_device_timeline(db_path, device_id):
    """设备视角的跨任务完整时间线：各趟任务节点、告警事件、越限样本（含补传标记），
    加上无任务窗口的孤儿样本，按时间合并排序——一段断网横跨几趟任务的补传全程可见。"""
    with dbmod.connect(db_path) as conn:
        ships = conn.execute(
            "SELECT * FROM shipments WHERE device_id=? ORDER BY created_at, id", (device_id,)
        ).fetchall()
        items = []
        for ship in ships:
            items.extend(_shipment_timeline_items(conn, ship))
        orphans = conn.execute(
            "SELECT * FROM telemetry WHERE device_id=? AND shipment_id IS NULL"
            " ORDER BY device_ts",
            (device_id,),
        ).fetchall()
        for o in orphans:
            items.append({
                "ts": o["device_ts"], "kind": "orphan", "shipment_id": None,
                "text": f"孤儿样本 {o['temp']}℃（采样时刻不在任何任务窗口内，已留存可查）",
                "temp": o["temp"], "backfilled": bool(o["backfilled"]),
                "arrived_at": o["arrived_at"],
            })
        items.sort(key=lambda x: (x["ts"], 0 if x["kind"] == "shipment" else 1))
        return {"device_id": device_id,
                "shipments": [s["id"] for s in ships], "items": items}
