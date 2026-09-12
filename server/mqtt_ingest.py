"""MQTT 订阅端：把设备上报接进核心逻辑。

Topic 约定：
  coldchain/{device_id}/telemetry  {"msg_id","seq","temp","ts"}   QoS1
  coldchain/{device_id}/status     {"state":"online"|"offline"}  retained，离线由 LWT 兜底
"""
import json
import threading
import time

import paho.mqtt.client as mqtt

from . import core


def _device_id_from_topic(topic):
    parts = topic.split("/")
    return parts[1] if len(parts) == 3 and parts[0] == "coldchain" else None


def start_ingest(db_path, host="127.0.0.1", port=1883):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="coldchain-server-ingest")

    def on_connect(c, _u, _f, rc, _p=None):
        c.subscribe("coldchain/+/telemetry", qos=1)
        c.subscribe("coldchain/+/status", qos=1)

    def on_message(c, _u, msg):
        device_id = _device_id_from_topic(msg.topic)
        if not device_id:
            return
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            return
        now = time.time()
        if msg.topic.endswith("/telemetry"):
            core.ingest_telemetry(
                db_path, device_id,
                msg_id=str(payload.get("msg_id", "")),
                seq=payload.get("seq"),
                temp=float(payload["temp"]),
                device_ts=float(payload["ts"]),
                now=now,
            )
        elif msg.topic.endswith("/status"):
            state = payload.get("state")
            if state in ("online", "offline"):
                core.set_device_online(db_path, device_id, state == "online", now, source="mqtt-status")

    client.on_connect = on_connect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=5)
    client.connect(host, port, keepalive=10)
    client.loop_start()
    return client


def start_watchdog(db_path, interval=0.5):
    """周期检查设备心跳超时，兜底 LWT 之外的离线场景。"""
    def loop():
        while True:
            try:
                core.check_timeouts(db_path, time.time())
            except Exception:
                pass  # 看门狗不允许把服务搞挂
            time.sleep(interval)

    t = threading.Thread(target=loop, daemon=True, name="offline-watchdog")
    t.start()
    return t
