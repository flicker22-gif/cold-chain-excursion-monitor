"""一键演示：起 broker + 服务端 + 设备模拟器，跑一趟带完整异常的冷链运输。

场景：正常运输 → 超温告警（按偏差/持续分级 L1→L2，超时未处理沿负责人链自动升级，
同级通知不重复）→ 处理关闭 → 断网（离线告警、本地缓存）→ 恢复（补传不丢、离线单
自动关闭留恢复记录、超温单不被顺手关掉、重复上线幂等）→ 断网期间的严重超温开新单
→ 重复消息去重 → 迟到数据并入已关闭告警（不重开、不重新升级）→ 完成任务
→ 设备离线状态下创建下一趟：新任务继承离线状态，宽限期后告警，恢复后自动关闭
→ 断网横跨两趟任务：缓存样本按采样时刻分别落窗（已完成任务只修正统计、空档样本
留存为孤儿、在途任务正常开单）→ 设备视角的跨任务完整时间线。

  python3 demo.py
"""
import json
import os
import sys
import tempfile
import threading
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from server import app as server_app, broker, core, db as dbmod, mqtt_ingest
from simulator.device_sim import DeviceSim

HTTP_PORT, MQTT_PORT = 5091, 18884
BASE = f"http://127.0.0.1:{HTTP_PORT}"
DB = os.path.join(tempfile.gettempdir(), "coldchain_demo.db")

T0 = time.time()


def log(msg):
    print(f"[{time.time() - T0:6.1f}s] {msg}")


def api(path, method="GET", body=None):
    req = urllib.request.Request(
        BASE + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


def wait_for(desc, pred, timeout=8):
    deadline = time.time() + timeout
    while time.time() < deadline:
        got = pred()
        if got:
            return got
        time.sleep(0.2)
    raise TimeoutError(f"等待超时: {desc}")


def main():
    if os.path.exists(DB):
        os.remove(DB)
    dbmod.init_db(DB)
    server_app.DB_PATH = DB

    broker.start_broker(MQTT_PORT)
    mqtt_ingest.start_ingest(DB, port=MQTT_PORT)
    mqtt_ingest.start_watchdog(DB, interval=0.3)
    threading.Thread(
        target=lambda: server_app.app.run(port=HTTP_PORT, threaded=True, use_reloader=False),
        daemon=True).start()
    import logging
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    time.sleep(0.8)
    log(f"环境就绪：MQTT broker :{MQTT_PORT}，监控看板 {BASE}")

    # 1. 建运输任务：2~8℃，负责人升级链 3 级，2 秒不处理完就自动升级
    ship = api("/api/shipments", "POST", {
        "name": "疫苗运输·沪A12345", "device_id": "truck-01",
        "temp_min": 2.0, "temp_max": 8.0, "offline_grace_sec": 2.0,
        "escalation_chain": ["调度员-王芳", "值班经理-李强", "运营总监-赵敏"],
        "escalate_after_sec": 2.0})
    sid = ship["id"]
    log(f"创建运输任务 #{sid}：{ship['name']}，阈值 2~8℃，升级链 王芳→李强→赵敏（2s 逐级）")

    # 2. 设备上线，正常运输
    sim = DeviceSim("truck-01", port=MQTT_PORT, interval=0.5)
    sim.connect()
    sim.start()
    log("设备 truck-01 上线，开始上报温度（5℃ 正常）")
    time.sleep(2.5)

    # 3. 超温：偏差 1.6℃ → L1，通知链上第一位负责人
    sim.temp_fn = lambda t, s: 9.6
    log("冷机故障，温度升到 9.6℃ …")
    a1 = wait_for("超温告警", lambda: next(
        (a for a in api("/api/alerts?status=open") if a["type"] == "TEMP_HIGH"), None))
    log(f"⚠ 告警 #{a1['id']} TEMP_HIGH，级别 {a1['severity']}（偏差 1.6℃），"
        f"负责人 {a1['assignee']}，已通知")

    # 4. 偏差拉大 → 级别自动上升；迟迟不处理 → 沿升级链自动升级
    sim.temp_fn = lambda t, s: 10.5
    wait_for("级别升为 L2", lambda: api(f"/api/alerts/{a1['id']}")["severity"] == "L2")
    log("温度 10.5℃（偏差 ≥2℃）：级别 L1 → L2，新级别重新通知负责人")
    wait_for("自动升级给值班经理",
             lambda: api(f"/api/alerts/{a1['id']}")["assignee"] == "值班经理-李强")
    log("超过 2s 未处理完 → 自动升级：负责人 调度员-王芳 → 值班经理-李强")
    wait_for("自动升级给运营总监",
             lambda: api(f"/api/alerts/{a1['id']}")["assignee"] == "运营总监-赵敏")
    log("仍未处理完 → 再升级：负责人 → 运营总监-赵敏（链顶，不再升级）")

    # 5. 总监接手处理：确认 → 记录 → 温度回落 → 关闭
    api(f"/api/alerts/{a1['id']}/ack", "POST",
        {"actor": "运营总监-赵敏", "note": "已远程指导司机切换备用冷机"})
    api(f"/api/alerts/{a1['id']}/notes", "POST",
        {"actor": "司机-张师傅", "note": "备用冷机已启动，温度开始回落"})
    sim.temp_fn = lambda t, s: 6.0
    time.sleep(1.5)
    api(f"/api/alerts/{a1['id']}/resolve", "POST",
        {"actor": "运营总监-赵敏", "note": "温度恢复 6℃，本次异常处理完毕"})
    log("温度恢复，告警已关闭 ✅")

    # 6. 断网：设备本地缓存，服务端 LWT 判离线（离线告警同样分级、走升级链）
    sim.temp_fn = lambda t, s: 12.2
    sim.drop_network()
    log("📵 车辆进入隧道断网！设备本地缓存数据（期间温度 12.2℃）")
    a_off = wait_for("离线告警", lambda: next(
        (a for a in api("/api/alerts?status=open") if a["type"] == "OFFLINE"), None))
    log(f"⚠ 告警 #{a_off['id']} OFFLINE，级别 {a_off['severity']}，负责人 {a_off['assignee']}")
    time.sleep(2.5)

    # 7. 恢复：补传不丢，离线单自动关闭并留恢复记录，超温单不被顺手关掉
    sim.connect()
    log("📶 网络恢复，设备补传缓存数据 …")
    wait_for("离线告警自动关闭", lambda: api(f"/api/alerts/{a_off['id']}")["status"] == "RESOLVED")
    a_off_done = api(f"/api/alerts/{a_off['id']}")
    assert any(e["action"] == "AUTO_RESOLVED" for e in a_off_done["events"])
    log("离线告警已自动关闭（AUTO_RESOLVED 留痕），断网期间的超温单保持打开")

    # 重复上线（retained 状态重投）：幂等，不产生重复恢复记录
    n_events = len(a_off_done["events"])
    sim.publish_status("online")
    sim.publish_status("online")
    time.sleep(1.0)
    a_off_again = api(f"/api/alerts/{a_off['id']}")
    assert a_off_again["status"] == "RESOLVED" and len(a_off_again["events"]) == n_events
    log("重复上线 ×2：服务端幂等，离线单保持已关闭，无重复恢复记录 ✅")

    a2 = wait_for("断网期间超温开新单", lambda: next(
        (a for a in api(f"/api/alerts?shipment_id={sid}")
         if a["type"] == "TEMP_HIGH" and a["id"] != a1["id"]), None))
    log(f"⚠ 补传数据触发新告警 #{a2['id']} TEMP_HIGH，级别 {a2['severity']}（偏差 ≥4℃）")
    sim.temp_fn = lambda t, s: 5.0
    time.sleep(1.0)
    api(f"/api/alerts/{a2['id']}/resolve", "POST",
        {"actor": "调度员-王芳", "note": "补传数据确认，出隧道后温度已回落"})
    log("第二次超温告警已关闭 ✅")

    # 8. 重复消息去重：重发一条历史样本，不产生新告警
    before = len(api(f"/api/alerts?shipment_id={sid}"))
    sim.resend(sim.history[3])
    sim.resend(sim.history[3])
    time.sleep(1.0)
    after = len(api(f"/api/alerts?shipment_id={sid}"))
    assert before == after, "重复消息不应产生新告警"
    log(f"重发历史消息 ×2：服务端去重，告警数不变（{after} 条）✅")

    # 9. 迟到数据落在已关闭告警的窗口内：留痕但不重开、不重新升级
    a1_now = api(f"/api/alerts/{a1['id']}")
    late_ts = (a1_now["first_ts"] + a1_now["last_ts"]) / 2
    sim.send_raw(msg_id="truck-01-late-0001", temp=9.9, ts=late_ts)
    time.sleep(1.0)
    a1_after = api(f"/api/alerts/{a1['id']}")
    assert a1_after["status"] == "RESOLVED", "已关闭告警不能被迟到数据改回"
    assert a1_after["severity"] == a1_now["severity"], "迟到数据不能改变已处理告警的级别"
    assert a1_after["assignee"] == a1_now["assignee"], "迟到数据不能改变已处理告警的负责人"
    new_actions = [e["action"] for e in a1_after["events"][len(a1_now["events"]):]]
    assert new_actions == ["LATE_DATA"], "迟到数据只留痕，不重新通知/升级"
    log(f"迟到数据（ts 落在告警 #{a1['id']} 窗口内）：仅留痕 LATE_DATA，"
        f"级别/负责人/通知记录均不变 ✅")

    # 10. 完成任务，输出完整时间线
    sim.disconnect()
    api(f"/api/shipments/{sid}/complete", "POST")
    log("运输完成。完整异常时间线：\n")

    tl = api(f"/api/shipments/{sid}/timeline")
    base = tl["items"][0]["ts"]
    kind_icon = {"shipment": "🚚", "violation": "🌡", "alert_event": "📋"}
    for it in tl["items"]:
        head = ""
        if it["kind"] == "alert_event":
            head = f"[告警#{it['alert_id']} {it['alert_type']}·{it['action']}" + \
                   (f"·{it['actor']}" if it.get("actor") else "") + "] "
        print(f"  +{it['ts'] - base:6.1f}s {kind_icon.get(it['kind'], '·')} {head}{it['text']}")

    # 11. 上一趟结束后设备一直没联网：新任务继承离线状态，宽限期后告警
    sim.connect()       # 设备短暂上线（上一趟已完成，无在途任务）
    time.sleep(1.0)
    sim.drop_network()  # 又掉了：LWT 置离线，但此时没有在途任务 → 不开单
    time.sleep(1.0)
    ship2 = api("/api/shipments", "POST", {
        "name": "疫苗运输·沪A12345（返程）", "device_id": "truck-01",
        "temp_min": 2.0, "temp_max": 8.0, "offline_grace_sec": 2.0,
        "escalation_chain": ["调度员-王芳", "值班经理-李强", "运营总监-赵敏"],
        "escalate_after_sec": 2.0})
    log(f"设备仍离线，创建下一趟任务 #{ship2['id']} …")
    off3 = wait_for("新任务继承离线告警", lambda: next(
        (a for a in api(f"/api/alerts?shipment_id={ship2['id']}") if a["type"] == "OFFLINE"), None))
    time.sleep(1.5)  # 看门狗重复检查
    n_off = len([a for a in api(f"/api/alerts?shipment_id={ship2['id']}")
                 if a["type"] == "OFFLINE"])
    assert n_off == 1, "重复检查不能重复开单"
    log(f"⚠ 告警 #{off3['id']} OFFLINE：新任务继承离线状态，宽限期后开单"
        f"（负责人 {off3['assignee']}，重复检查仍只有 1 张）")
    sim.connect()
    wait_for("继承的离线告警自动关闭", lambda: api(f"/api/alerts/{off3['id']}")["status"] == "RESOLVED")
    log("设备恢复上线，继承的离线告警自动关闭（AUTO_RESOLVED 留痕）✅")
    api(f"/api/shipments/{ship2['id']}/complete", "POST")
    log(f"任务 #{ship2['id']} 完成")

    # 12. 断网横跨两趟任务：缓存样本按采样时刻分别落窗——已完成任务只修正统计，
    #     两趟之间空档的样本留存为孤儿，在途任务正常开单
    ship3 = api("/api/shipments", "POST", {
        "name": "疫苗运输·沪A12345（第三程）", "device_id": "truck-01",
        "temp_min": 2.0, "temp_max": 8.0, "offline_grace_sec": 2.0,
        "escalation_chain": ["调度员-王芳", "值班经理-李强", "运营总监-赵敏"],
        "escalate_after_sec": 2.0})
    sid3 = ship3["id"]
    log(f"创建任务 #{sid3}（第三程），设备在线正常运输")
    time.sleep(1.5)

    sim.temp_fn = lambda t, s: 11.5
    sim.drop_network()
    log("📵 车辆再次断网！温度 11.5℃，设备本地缓存 …")
    time.sleep(1.5)  # 这段缓存样本落在第三程窗口内

    api(f"/api/shipments/{sid3}/complete", "POST")
    log(f"任务 #{sid3} 到达任务点完成运输——车还在断网，缓存数据尚未传上来")
    time.sleep(1.0)  # 两趟之间的空档：这段缓存样本没有对应任务

    ship4 = api("/api/shipments", "POST", {
        "name": "疫苗运输·沪A12345（第四程）", "device_id": "truck-01",
        "temp_min": 2.0, "temp_max": 8.0, "offline_grace_sec": 2.0,
        "escalation_chain": ["调度员-王芳", "值班经理-李强", "运营总监-赵敏"],
        "escalate_after_sec": 2.0})
    sid4 = ship4["id"]
    log(f"创建任务 #{sid4}（第四程），设备仍在断网缓存")
    time.sleep(1.5)  # 这段缓存样本落在第四程窗口内

    sim.connect()
    log("📶 网络恢复，断网期间缓存的样本一次性补传上来（横跨三段时间）…")
    time.sleep(2.0)

    # 落在已完成第三程窗口内的样本：只进时间线/修正统计，绝不开新告警
    assert not [a for a in api(f"/api/alerts?shipment_id={sid3}") if a["type"] == "TEMP_HIGH"], \
        "已完成任务上的补传越限不能开新告警"
    tl3 = api(f"/api/shipments/{sid3}/timeline")
    assert any(i["kind"] == "violation" and i["backfilled"] for i in tl3["items"])
    log(f"补传样本落到已完成任务 #{sid3}：只进时间线，不开新告警、不动级别/负责人 ✅")

    # 空档样本：留存为孤儿，接口可查；重复补传仍只算一次
    orphans = api("/api/telemetry/orphans?device_id=truck-01")
    assert orphans, "两趟之间空档的样本应留存为孤儿"
    log(f"两趟之间空档的 {len(orphans)} 条样本没有对应任务：按孤儿留存，接口可查 ✅")
    o0 = orphans[0]
    sim.send_raw(msg_id=o0["msg_id"], temp=o0["temp"], ts=o0["device_ts"])
    time.sleep(1.0)
    assert len(api("/api/telemetry/orphans?device_id=truck-01")) == len(orphans)
    log("孤儿样本重复补传：服务端去重，仍只算一次 ✅")

    # 落在第四程窗口内的样本：任务在途，正常开单
    a4 = wait_for("第四程补传越限开单", lambda: next(
        (a for a in api(f"/api/alerts?shipment_id={sid4}") if a["type"] == "TEMP_HIGH"), None))
    log(f"⚠ 补传样本落到在途任务 #{sid4}：正常开告警 #{a4['id']} TEMP_HIGH（{a4['severity']}）")

    # 任务完成后又有迟到数据落入已关闭告警窗口：只修正统计，状态/级别/负责人冻结
    a1_now = api(f"/api/alerts/{a1['id']}")
    late_ts = (a1_now["first_ts"] + a1_now["last_ts"]) / 2
    sim.send_raw(msg_id="truck-01-late-0002", temp=10.8, ts=late_ts)
    time.sleep(1.0)
    a1_after = api(f"/api/alerts/{a1['id']}")
    assert a1_after["status"] == "RESOLVED", "已关闭告警不能被迟到数据改回"
    assert a1_after["severity"] == a1_now["severity"], "迟到数据不能改变已处理告警的级别"
    assert a1_after["assignee"] == a1_now["assignee"], "迟到数据不能改变已处理告警的负责人"
    assert a1_after["peak_temp"] == max(a1_now["peak_temp"], 10.8), "统计应被修正"
    log(f"任务 #{sid} 完成后又有迟到数据落入告警 #{a1['id']} 窗口：峰值修正为 "
        f"{a1_after['peak_temp']}℃，状态/级别/负责人冻结 ✅")

    sim.temp_fn = lambda t, s: 5.0
    time.sleep(1.0)
    api(f"/api/alerts/{a4['id']}/resolve", "POST",
        {"actor": "调度员-王芳", "note": "补传数据确认，现场已处理"})
    sim.disconnect()
    api(f"/api/shipments/{sid4}/complete", "POST")

    # 13. 设备视角的跨任务完整时间线：断网横跨几趟任务的补传全程
    log("运输全部完成。设备视角的跨任务完整时间线（含孤儿样本）：\n")
    dtl = api("/api/devices/truck-01/timeline")
    base = dtl["items"][0]["ts"]
    dev_icon = {"shipment": "🚚", "violation": "🌡", "alert_event": "📋", "orphan": "📦"}
    for it in dtl["items"]:
        head = ""
        if it["kind"] == "alert_event":
            head = f"[告警#{it['alert_id']} {it['alert_type']}·{it['action']}" + \
                   (f"·{it['actor']}" if it.get("actor") else "") + "] "
        tag = f"#{it['shipment_id']}" if it.get("shipment_id") else "无任务"
        print(f"  +{it['ts'] - base:6.1f}s [{tag:>5}] {dev_icon.get(it['kind'], '·')} "
              f"{head}{it['text']}")

    print(f"\n看板地址（单独起服务可看实时页面）：python3 -m server.app --with-broker")
    print("演示结束。")


if __name__ == "__main__":
    main()
