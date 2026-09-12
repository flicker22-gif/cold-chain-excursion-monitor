"""一键演示：起 broker + 服务端 + 设备模拟器，跑一趟带完整异常的冷链运输。

场景：正常运输 → 超温告警（确认→升级→记录→关闭）→ 断网（离线告警、本地缓存）
→ 恢复（补传不丢、自动关离线单、新一次超温开新单）→ 重复消息去重
→ 迟到数据并入已关闭告警（不重开）→ 完成任务，输出完整异常时间线。

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

    # 1. 建运输任务：2~8℃，离线宽限 2 秒
    ship = api("/api/shipments", "POST", {
        "name": "疫苗运输·沪A12345", "device_id": "truck-01",
        "temp_min": 2.0, "temp_max": 8.0, "offline_grace_sec": 2.0})
    sid = ship["id"]
    log(f"创建运输任务 #{sid}：{ship['name']}，阈值 2~8℃")

    # 2. 设备上线，正常运输
    sim = DeviceSim("truck-01", port=MQTT_PORT, interval=0.5)
    sim.connect()
    sim.start()
    log("设备 truck-01 上线，开始上报温度（5℃ 正常）")
    time.sleep(2.5)

    # 3. 超温
    sim.temp_fn = lambda t, s: 9.6
    log("冷机故障，温度升到 9.6℃ …")
    a1 = wait_for("超温告警", lambda: next(
        (a for a in api("/api/alerts?status=open") if a["type"] == "TEMP_HIGH"), None))
    log(f"⚠ 触发告警 #{a1['id']} TEMP_HIGH，峰值 {a1['peak_temp']}℃")

    # 4. 处理流程：确认 → 升级 → 记录 → 关闭
    api(f"/api/alerts/{a1['id']}/ack", "POST", {"actor": "调度员-王芳", "note": "已电话通知司机检查冷机"})
    log("告警已确认（调度员-王芳）")
    api(f"/api/alerts/{a1['id']}/escalate", "POST", {"actor": "值班经理-李强", "note": "10分钟未恢复，升级为重大异常"})
    log("告警已升级（值班经理-李强）")
    api(f"/api/alerts/{a1['id']}/notes", "POST", {"actor": "司机-张师傅", "note": "冷机压缩机重启，温度开始回落"})
    sim.temp_fn = lambda t, s: 6.0
    time.sleep(1.5)
    api(f"/api/alerts/{a1['id']}/resolve", "POST", {"actor": "调度员-王芳", "note": "温度恢复 6℃，本次异常处理完毕"})
    log("温度恢复，告警已关闭 ✅")

    # 5. 断网：设备本地缓存，服务端 LWT 判离线
    sim.temp_fn = lambda t, s: 10.4
    sim.drop_network()
    log("📵 车辆进入隧道断网！设备本地缓存数据（期间温度 10.4℃）")
    a_off = wait_for("离线告警", lambda: next(
        (a for a in api("/api/alerts?status=open") if a["type"] == "OFFLINE"), None))
    log(f"⚠ 触发告警 #{a_off['id']} OFFLINE（LWT）")
    time.sleep(2.5)

    # 6. 恢复：补传不丢，离线单自动关闭，断网期间的超温开新单
    sim.connect()
    log("📶 网络恢复，设备补传缓存数据 …")
    wait_for("离线告警自动关闭", lambda: api(f"/api/alerts/{a_off['id']}")["status"] == "RESOLVED")
    log("离线告警已自动关闭（设备恢复在线）")
    a2 = wait_for("断网期间超温开新单", lambda: next(
        (a for a in api(f"/api/alerts?shipment_id={sid}")
         if a["type"] == "TEMP_HIGH" and a["id"] != a1["id"]), None))
    log(f"⚠ 补传数据触发新告警 #{a2['id']} TEMP_HIGH（断网期间的异常不丢）")
    sim.temp_fn = lambda t, s: 5.0
    time.sleep(1.0)
    api(f"/api/alerts/{a2['id']}/resolve", "POST", {"actor": "调度员-王芳", "note": "补传数据确认，出隧道后温度已回落"})
    log("第二次超温告警已关闭 ✅")

    # 7. 重复消息去重：重发一条历史样本，不产生新告警
    before = len(api(f"/api/alerts?shipment_id={sid}"))
    sim.resend(sim.history[3])
    sim.resend(sim.history[3])
    time.sleep(1.0)
    after = len(api(f"/api/alerts?shipment_id={sid}"))
    assert before == after, "重复消息不应产生新告警"
    log(f"重发历史消息 ×2：服务端去重，告警数不变（{after} 条）✅")

    # 8. 迟到数据落在已关闭告警的窗口内：留痕但不重开
    a1_now = api(f"/api/alerts/{a1['id']}")
    late_ts = (a1_now["first_ts"] + a1_now["last_ts"]) / 2
    sim.send_raw(msg_id="truck-01-late-0001", temp=9.9, ts=late_ts)
    time.sleep(1.0)
    a1_after = api(f"/api/alerts/{a1['id']}")
    assert a1_after["status"] == "RESOLVED", "已关闭告警不能被迟到数据改回"
    assert any(e["action"] == "LATE_DATA" for e in a1_after["events"])
    log(f"迟到数据（ts 落在告警 #{a1['id']} 窗口内）：已留痕 LATE_DATA，状态仍为 RESOLVED ✅")

    # 9. 完成任务，输出完整时间线
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

    print(f"\n看板地址（单独起服务可看实时页面）：python3 -m server.app --with-broker")
    print("演示结束。")


if __name__ == "__main__":
    main()
