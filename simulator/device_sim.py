"""车载冷链设备模拟器：MQTT 上报温度与在线状态，断网本地缓存、恢复补传。

- 在线时按固定间隔上报 {"msg_id","seq","temp","ts"}（QoS1）；
- 断网（网络被切断）后样本缓存在本地 buffer，恢复后原样补传（原 msg_id / 原采样时刻）；
- 掉线靠 LWT 让 broker 代发 retained "offline"，重连后自己发 retained "online"；
- 支持手动重发历史消息（模拟 QoS1 重传/设备重发）验证服务端去重。
"""
import json
import threading
import time
import uuid

import paho.mqtt.client as mqtt


class DeviceSim:
    def __init__(self, device_id, host="127.0.0.1", port=1883, interval=0.5):
        self.device_id = device_id
        self.host, self.port = host, port
        self.interval = interval
        self.seq = 0
        self.online = False
        self.buffer = []          # 断网期间待补传的样本
        self.history = []         # 已发送的样本（用于模拟重发）
        self.temp_fn = lambda t, seq: 5.0
        self._client = None
        self._stop = threading.Event()
        self._thread = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------ 连接管理

    def _new_client(self):
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                        client_id=f"sim-{self.device_id}-{uuid.uuid4().hex[:6]}")
        c.will_set(self._topic("status"),
                   json.dumps({"state": "offline", "ts": time.time()}),
                   qos=1, retain=True)
        c.on_connect = self._on_connect
        return c

    def _topic(self, kind):
        return f"coldchain/{self.device_id}/{kind}"

    def _on_connect(self, c, _u, _f, rc, _p=None):
        with self._lock:
            self.online = True
        c.publish(self._topic("status"),
                  json.dumps({"state": "online", "ts": time.time()}), qos=1, retain=True)
        self._flush_buffer()

    def connect(self):
        self._client = self._new_client()
        self._client.connect(self.host, self.port, keepalive=5)
        self._client.loop_start()
        for _ in range(50):
            if self.online:
                break
            time.sleep(0.1)
        # 采样线程若已被 disconnect() 停掉则重新拉起：seq 单调递增，msg_id 不会重复
        if self._thread is not None and not self._thread.is_alive():
            self._stop.clear()
            self.start()

    def drop_network(self):
        """模拟断网：直接掐掉 socket（不发送 DISCONNECT），broker 走 LWT 判离线。"""
        with self._lock:
            self.online = False
        c, self._client = self._client, None
        if c:
            c.loop_stop()
            try:
                c._sock.close()
            except Exception:
                pass

    def disconnect(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)
        if self._client:
            self._client.disconnect()
            self._client.loop_stop()

    # ------------------------------------------------------------ 上报循环

    def start(self):
        self._thread = threading.Thread(target=self._loop, daemon=True,
                                        name=f"sim-{self.device_id}")
        self._thread.start()

    def _loop(self):
        while not self._stop.is_set():
            self.tick()
            time.sleep(self.interval)

    def tick(self, temp=None):
        """产生一个样本：在线直接发，离线进缓存。"""
        with self._lock:
            self.seq += 1
            sample = {
                "msg_id": f"{self.device_id}-{self.seq:06d}",
                "seq": self.seq,
                "temp": round(temp if temp is not None else self.temp_fn(time.time(), self.seq), 2),
                "ts": time.time(),
            }
            if self.online and self._client:
                self._publish(sample)
            else:
                self.buffer.append(sample)
        return sample

    def _publish(self, sample):
        self._client.publish(self._topic("telemetry"), json.dumps(sample), qos=1)
        self.history.append(sample)

    def _flush_buffer(self):
        """恢复在线后补传断网期间的缓存（保留原 msg_id 与采样时刻）。"""
        pending, self.buffer = self.buffer, []
        for sample in pending:
            self._publish(sample)

    # ------------------------------------------------------------ 测试钩子

    def resend(self, sample):
        """原样重发一条历史消息（模拟 QoS1 重传），服务端应按 msg_id 去重。"""
        if self._client and self.online:
            self._client.publish(self._topic("telemetry"), json.dumps(sample), qos=1)

    def publish_status(self, state):
        """补发一次在线状态（模拟 retained 重投/重复上线），服务端应幂等处理。"""
        if self._client and self.online:
            self._client.publish(self._topic("status"),
                                 json.dumps({"state": state, "ts": time.time()}),
                                 qos=1, retain=True)

    def send_raw(self, msg_id, temp, ts):
        """以指定 msg_id / 采样时刻补发一条（模拟迟到很久的补传数据）。"""
        if self._client and self.online:
            self._client.publish(self._topic("telemetry"), json.dumps(
                {"msg_id": msg_id, "seq": -1, "temp": temp, "ts": ts}), qos=1)
