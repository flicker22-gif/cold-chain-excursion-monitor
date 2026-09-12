"""核心业务逻辑：温度判定、告警生命周期、去重、迟到数据合并、离线看门狗。

所有函数接收显式的 `now`（epoch 秒），单测可以注入确定性时间。
每个函数自开自关事务（BEGIN IMMEDIATE），MQTT 线程 / HTTP 线程 / 看门狗线程并发安全。
"""
import sqlite3

from . import db as dbmod

OPEN_STATUSES = ("OPEN", "ACKED", "ESCALATED")

# 告警状态机：合法迁移。RESOLVED 是终态——已处理的异常不能被任何数据改回。
_TRANSITIONS = {
    "ack":      {"from": {"OPEN"}, "to": "ACKED", "event": "ACKED"},
    "escalate": {"from": {"OPEN", "ACKED"}, "to": "ESCALATED", "event": "ESCALATED"},
    "resolve":  {"from": {"OPEN", "ACKED", "ESCALATED"}, "to": "RESOLVED", "event": "RESOLVED"},
}

# 到达时刻比采样时刻晚超过该值，视为断网期间的补传数据
BACKFILL_AGE_SEC = 1.0


class DomainError(Exception):
    """业务规则冲突（如非法状态迁移），API 层映射为 409。"""


# ---------------------------------------------------------------- 运输任务

def create_shipment(db_path, name, device_id, temp_min, temp_max, offline_grace_sec, now):
    if temp_min >= temp_max:
        raise DomainError("temp_min 必须小于 temp_max")
    with dbmod.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        dup = conn.execute(
            "SELECT id FROM shipments WHERE device_id=? AND status='IN_TRANSIT'", (device_id,)
        ).fetchone()
        if dup:  # 同一台车同时只能有一个在途任务，重复创建返回已有任务（幂等）
            return get_shipment(db_path, dup["id"])
        cur = conn.execute(
            "INSERT INTO shipments(name, device_id, temp_min, temp_max, offline_grace_sec, created_at)"
            " VALUES (?,?,?,?,?,?)",
            (name, device_id, temp_min, temp_max, offline_grace_sec, now),
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

def ingest_telemetry(db_path, device_id, msg_id, seq, temp, device_ts, now):
    """接入一条温度数据。

    - 按 (device_id, msg_id) 唯一约束去重：重复消息/补报重发不会重复入库、重复报警；
    - 仅实时样本（到达时刻≈采样时刻）更新设备在线状态：置在线并自动关闭挂着的离线告警；
      断网补传的历史样本不碰在线状态/心跳——它证明不了链路此刻已恢复；
    - 越限时：有未关闭告警则并入（延长窗口、刷峰值），不新建；
      最近的同类告警已 RESOLVED 且样本落在其异常窗口内 → 记 LATE_DATA，绝不开新单；
      否则是真正的新一次异常，开新告警。
    """
    with dbmod.connect(db_path) as conn:
        conn.execute("BEGIN IMMEDIATE")
        ship = conn.execute(
            "SELECT * FROM shipments WHERE device_id=? AND status='IN_TRANSIT' ORDER BY id DESC LIMIT 1",
            (device_id,),
        ).fetchone()
        if not ship:
            return {"accepted": False, "reason": "no_active_shipment"}

        backfilled = 1 if now - device_ts > BACKFILL_AGE_SEC else 0
        cur = conn.execute(
            "INSERT OR IGNORE INTO telemetry"
            " (device_id, shipment_id, msg_id, seq, temp, device_ts, arrived_at, backfilled)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (device_id, ship["id"], msg_id, seq, temp, device_ts, now, backfilled),
        )
        if not backfilled:
            # 只有实时样本能证明链路此刻是通的：置在线并顺带关闭挂着的离线告警。
            # 断网补传的是历史样本，不碰在线状态/心跳，避免“设备在线、离线单却开着”的状态翻转。
            _mark_online(conn, device_id, now, source="telemetry")
        if cur.rowcount == 0:
            conn.commit()
            return {"accepted": True, "duplicated": True}

        result = {"accepted": True, "duplicated": False, "backfilled": bool(backfilled)}
        if temp > ship["temp_max"]:
            result["alert"] = _on_violation(conn, ship, "TEMP_HIGH", temp, device_ts, now)
        elif temp < ship["temp_min"]:
            result["alert"] = _on_violation(conn, ship, "TEMP_LOW", temp, device_ts, now)
        else:
            _maybe_recovered(conn, ship, temp, device_ts, now)
        conn.commit()
        return result


def _on_violation(conn, ship, alert_type, temp, ts, now):
    open_alert = conn.execute(
        f"SELECT * FROM alerts WHERE shipment_id=? AND type=?"
        f" AND status IN ({','.join('?' * len(OPEN_STATUSES))}) ORDER BY id DESC LIMIT 1",
        (ship["id"], alert_type, *OPEN_STATUSES),
    ).fetchone()
    if open_alert:
        _merge_into_alert(conn, open_alert, temp, ts)
        return {"alert_id": open_alert["id"], "merged": True}

    # 迟到数据：采样时刻落在某张已处理告警的异常窗口内 → 属于那次已处理的异常，
    # 修正统计、留痕，但状态保持 RESOLVED，绝不开新单。
    resolved = conn.execute(
        "SELECT * FROM alerts WHERE shipment_id=? AND type=? AND status='RESOLVED'"
        " AND first_ts<=? AND last_ts>=? ORDER BY id DESC LIMIT 1",
        (ship["id"], alert_type, ts, ts),
    ).fetchone()
    if resolved:
        _merge_into_alert(conn, resolved, temp, ts)
        _add_event(
            conn, resolved["id"], "LATE_DATA", "system",
            f"迟到数据并入已处理告警（temp={temp}℃, device_ts={ts:.3f}），状态不变", now,
        )
        return {"alert_id": resolved["id"], "late": True}

    cur = conn.execute(
        "INSERT INTO alerts(shipment_id, device_id, type, opened_at, first_ts, last_ts, peak_temp, detail)"
        " VALUES (?,?,?,?,?,?,?,?)",
        (ship["id"], ship["device_id"], alert_type, now, ts, ts, temp,
         f"温度越{'上' if alert_type == 'TEMP_HIGH' else '下'}限"),
    )
    _add_event(conn, cur.lastrowid, "OPENED", "system",
               f"温度 {temp}℃ 越限（阈值 {ship['temp_min']}~{ship['temp_max']}℃）", now)
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
                    cur = conn.execute(
                        "INSERT INTO alerts(shipment_id, device_id, type, opened_at, first_ts, last_ts, detail)"
                        " VALUES (?,?,?,?,?,?,?)",
                        (ship["id"], device_id, "OFFLINE", now, now, now,
                         f"设备离线（{source}）"),
                    )
                    _add_event(conn, cur.lastrowid, "OPENED", "system",
                               f"设备离线，超过 {ship['offline_grace_sec']}s 无数据（{source}）", now)
        conn.commit()


def check_timeouts(db_path, now):
    """看门狗：在途任务的设备超过 offline_grace_sec 没有任何消息 → 判离线。

    MQTT 的 LWT 能覆盖大部分掉线；看门狗兜底 LWT 丢失（如服务端重启期间）的情况。
    """
    with dbmod.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT ds.device_id, ds.last_seen, s.offline_grace_sec FROM device_state ds"
            " JOIN shipments s ON s.device_id = ds.device_id AND s.status='IN_TRANSIT'"
            " WHERE ds.online=1"
        ).fetchall()
    for r in rows:
        if r["last_seen"] is not None and now - r["last_seen"] > r["offline_grace_sec"]:
            set_device_online(db_path, r["device_id"], False, now, source="watchdog")


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
    """确认 / 升级 / 关闭。非法迁移（含操作已关闭告警）抛 DomainError。"""
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
        _add_event(conn, alert_id, rule["event"], actor, note, now)
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

def get_timeline(db_path, shipment_id):
    """一次运输的完整时间线：任务节点、越限样本、告警开单/处理/关闭，按时间排序。"""
    with dbmod.connect(db_path) as conn:
        ship = conn.execute("SELECT * FROM shipments WHERE id=?", (shipment_id,)).fetchone()
        if not ship:
            return None
        items = [{
            "ts": ship["created_at"], "kind": "shipment",
            "text": f"任务创建：{ship['name']}（{ship['device_id']}，阈值 "
                    f"{ship['temp_min']}~{ship['temp_max']}℃）",
        }]
        if ship["finished_at"]:
            items.append({"ts": ship["finished_at"], "kind": "shipment", "text": "任务完成"})

        alerts = conn.execute(
            "SELECT * FROM alerts WHERE shipment_id=? ORDER BY id", (shipment_id,)
        ).fetchall()
        for a in alerts:
            events = conn.execute(
                "SELECT * FROM alert_events WHERE alert_id=? ORDER BY id", (a["id"],)
            ).fetchall()
            for e in events:
                items.append({
                    "ts": e["created_at"], "kind": "alert_event",
                    "alert_id": a["id"], "alert_type": a["type"], "action": e["action"],
                    "actor": e["actor"], "text": e["note"],
                })

        violations = conn.execute(
            "SELECT * FROM telemetry WHERE shipment_id=? AND (temp<? OR temp>?) ORDER BY device_ts",
            (shipment_id, ship["temp_min"], ship["temp_max"]),
        ).fetchall()
        for v in violations:
            items.append({
                "ts": v["device_ts"], "kind": "violation",
                "text": f"越限样本 {v['temp']}℃"
                        + ("（断网补传）" if v["backfilled"] else ""),
                "temp": v["temp"], "backfilled": bool(v["backfilled"]),
            })

        items.sort(key=lambda x: (x["ts"], 0 if x["kind"] == "shipment" else 1))
        return {"shipment": dict(ship), "items": items}
