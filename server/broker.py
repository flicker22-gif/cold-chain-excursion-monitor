"""内嵌 MQTT broker（amqtt，纯 Python），避免依赖外部 mosquitto / Docker。"""
import asyncio
import threading

from amqtt.broker import Broker

DEFAULT_CONFIG = {
    "listeners": {"default": {"type": "tcp", "bind": "127.0.0.1:{port}"}},
    "sys_interval": 0,
    "auth": {"allow-anonymous": True},
    "topic-check": {"enabled": False},
}


def start_broker(port=1883):
    """在后台线程里跑一个 MQTT broker，返回 (broker, loop)。"""
    config = {
        **DEFAULT_CONFIG,
        "listeners": {"default": {"type": "tcp", "bind": f"127.0.0.1:{port}"}},
    }
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    broker = Broker(config, loop=loop)

    ready = threading.Event()

    def run():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(broker.start())
        ready.set()
        loop.run_forever()

    t = threading.Thread(target=run, daemon=True, name="mqtt-broker")
    t.start()
    ready.wait(timeout=5)
    return broker, loop
