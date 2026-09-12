"""核心规则单测：去重、迟到数据不重开、新异常开新单、离线告警、状态机。"""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from server import core, db as dbmod


class CoreTestBase(unittest.TestCase):
    def setUp(self):
        fd, self.db = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        dbmod.init_db(self.db)
        self.ship = core.create_shipment(self.db, "测试任务", "dev-1", 2.0, 8.0, 5.0, now=1000.0)
        self.sid = self.ship["id"]

    def tearDown(self):
        os.unlink(self.db)

    def send(self, msg_id, temp, ts, now=None):
        return core.ingest_telemetry(self.db, "dev-1", msg_id, seq=None,
                                     temp=temp, device_ts=ts, now=now if now is not None else ts)


class TestDedup(CoreTestBase):
    def test_duplicate_message_not_recounted(self):
        r1 = self.send("m1", 9.5, 1001.0)
        r2 = self.send("m1", 9.5, 1001.0)
        r3 = self.send("m1", 9.5, 1001.0)
        self.assertFalse(r1["duplicated"])
        self.assertTrue(r2["duplicated"])
        self.assertTrue(r3["duplicated"])
        with dbmod.connect(self.db) as conn:
            n = conn.execute("SELECT COUNT(*) c FROM telemetry").fetchone()["c"]
        self.assertEqual(n, 1)

    def test_duplicate_message_no_extra_alert(self):
        self.send("m1", 9.5, 1001.0)
        self.send("m1", 9.5, 1001.0)
        self.send("m2", 9.6, 1002.0)
        self.send("m2", 9.6, 1002.0)
        alerts = core.list_alerts(self.db, shipment_id=self.sid)
        self.assertEqual(len(alerts), 1)  # 同一次异常只有一张单


class TestViolationMerging(CoreTestBase):
    def test_continuous_excursion_merges_into_one_alert(self):
        self.send("m1", 9.0, 1001.0)
        self.send("m2", 9.5, 1002.0)
        self.send("m3", 9.2, 1003.0)
        alerts = core.list_alerts(self.db, shipment_id=self.sid)
        self.assertEqual(len(alerts), 1)
        a = core.get_alert(self.db, alerts[0]["id"])
        self.assertEqual(a["first_ts"], 1001.0)
        self.assertEqual(a["last_ts"], 1003.0)
        self.assertEqual(a["peak_temp"], 9.5)

    def test_low_temp_alert(self):
        self.send("m1", 1.0, 1001.0)
        alerts = core.list_alerts(self.db, shipment_id=self.sid)
        self.assertEqual(alerts[0]["type"], "TEMP_LOW")


class TestLateData(CoreTestBase):
    def test_late_data_does_not_reopen_resolved_alert(self):
        self.send("m1", 9.5, 1001.0)
        self.send("m2", 9.6, 1002.0)
        aid = core.list_alerts(self.db)[0]["id"]
        core.transition_alert(self.db, aid, "resolve", "op", "已处理", now=1010.0)

        # 迟到数据落在已处理窗口内（ts <= last_ts）
        r = self.send("m3", 9.9, 1001.5, now=1020.0)
        self.assertTrue(r["alert"]["late"])
        a = core.get_alert(self.db, aid)
        self.assertEqual(a["status"], "RESOLVED")
        self.assertEqual(a["peak_temp"], 9.9)  # 统计被修正
        self.assertTrue(any(e["action"] == "LATE_DATA" for e in a["events"]))
        self.assertEqual(len(core.list_alerts(self.db)), 1)  # 没有开新单

    def test_new_excursion_after_resolve_opens_new_alert(self):
        self.send("m1", 9.5, 1001.0)
        aid = core.list_alerts(self.db)[0]["id"]
        core.transition_alert(self.db, aid, "resolve", "op", "已处理", now=1010.0)

        # 补传数据的采样时刻在已处理窗口之后 → 真正的新一次异常
        r = self.send("m2", 10.1, 1100.0, now=1200.0)
        self.assertTrue(r["alert"]["opened"])
        alerts = core.list_alerts(self.db, shipment_id=self.sid)
        self.assertEqual(len(alerts), 2)
        self.assertEqual(core.get_alert(self.db, aid)["status"], "RESOLVED")

    def test_late_data_merges_into_the_right_resolved_alert(self):
        # 两次已处理的异常：窗口 [1001,1002] 和 [1101,1102]
        self.send("m1", 9.5, 1001.0)
        self.send("m2", 9.6, 1002.0)
        a1 = core.list_alerts(self.db)[0]["id"]
        core.transition_alert(self.db, a1, "resolve", "op", "处理完", now=1010.0)
        self.send("m3", 10.0, 1101.0)
        self.send("m4", 10.1, 1102.0)
        a2 = [a for a in core.list_alerts(self.db) if a["id"] != a1][0]["id"]
        core.transition_alert(self.db, a2, "resolve", "op", "处理完", now=1110.0)

        # 迟到数据落在第一次异常的窗口内 → 并入第一次，不是最近一次
        r = self.send("m5", 9.9, 1001.5, now=1200.0)
        self.assertEqual(r["alert"]["alert_id"], a1)
        self.assertEqual(core.get_alert(self.db, a1)["status"], "RESOLVED")
        self.assertEqual(core.get_alert(self.db, a2)["status"], "RESOLVED")
        self.assertEqual(len(core.list_alerts(self.db)), 2)

        # 迟到数据落在两次异常之间的空档 → 是从未告警过的新异常，开新单
        r = self.send("m6", 9.8, 1050.0, now=1200.0)
        self.assertTrue(r["alert"]["opened"])
        self.assertEqual(len(core.list_alerts(self.db)), 3)

    def test_backfilled_flag(self):
        r = self.send("m1", 5.0, 1000.0, now=1005.0)
        self.assertTrue(r["backfilled"])


class TestOffline(CoreTestBase):
    def test_offline_alert_and_auto_resolve_on_reconnect(self):
        core.set_device_online(self.db, "dev-1", True, now=1000.0)
        core.set_device_online(self.db, "dev-1", False, now=1005.0)
        alerts = core.list_alerts(self.db, status="open")
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["type"], "OFFLINE")

        core.set_device_online(self.db, "dev-1", True, now=1010.0)
        a = core.get_alert(self.db, alerts[0]["id"])
        self.assertEqual(a["status"], "RESOLVED")
        self.assertTrue(any(e["action"] == "AUTO_RESOLVED" for e in a["events"]))

    def test_watchdog_timeout(self):
        core.set_device_online(self.db, "dev-1", True, now=1000.0)
        core.check_timeouts(self.db, now=1003.0)  # 未超 5s 宽限
        self.assertEqual(core.list_devices(self.db)[0]["online"], 1)
        core.check_timeouts(self.db, now=1006.0)  # 超过宽限
        self.assertEqual(core.list_devices(self.db)[0]["online"], 0)
        alerts = core.list_alerts(self.db, status="open")
        self.assertEqual(alerts[0]["type"], "OFFLINE")

    def test_telemetry_touches_last_seen(self):
        self.send("m1", 5.0, 1000.0, now=1000.0)
        core.check_timeouts(self.db, now=1004.0)
        self.assertEqual(core.list_devices(self.db)[0]["online"], 1)


class TestStateMachine(CoreTestBase):
    def test_full_workflow(self):
        self.send("m1", 9.5, 1001.0)
        aid = core.list_alerts(self.db)[0]["id"]
        a = core.transition_alert(self.db, aid, "ack", "op", "已知悉", now=1002.0)
        self.assertEqual(a["status"], "ACKED")
        a = core.transition_alert(self.db, aid, "escalate", "mgr", "升级", now=1003.0)
        self.assertEqual(a["status"], "ESCALATED")
        core.add_note(self.db, aid, "op", "现场处理中", now=1004.0)
        a = core.transition_alert(self.db, aid, "resolve", "op", "处理完毕", now=1005.0)
        self.assertEqual(a["status"], "RESOLVED")
        self.assertEqual([e["action"] for e in a["events"]],
                         ["OPENED", "ACKED", "ESCALATED", "NOTE", "RESOLVED"])

    def test_invalid_transitions_rejected(self):
        self.send("m1", 9.5, 1001.0)
        aid = core.list_alerts(self.db)[0]["id"]
        core.transition_alert(self.db, aid, "resolve", "op", "", now=1002.0)
        with self.assertRaises(core.DomainError):
            core.transition_alert(self.db, aid, "ack", "op", "", now=1003.0)
        with self.assertRaises(core.DomainError):
            core.transition_alert(self.db, aid, "escalate", "op", "", now=1003.0)
        with self.assertRaises(core.DomainError):
            core.add_note(self.db, aid, "op", "x", now=1003.0)

    def test_ack_twice_rejected(self):
        self.send("m1", 9.5, 1001.0)
        aid = core.list_alerts(self.db)[0]["id"]
        core.transition_alert(self.db, aid, "ack", "op", "", now=1002.0)
        with self.assertRaises(core.DomainError):
            core.transition_alert(self.db, aid, "ack", "op", "", now=1003.0)


class TestShipment(CoreTestBase):
    def test_one_active_shipment_per_device(self):
        again = core.create_shipment(self.db, "重复创建", "dev-1", 2.0, 8.0, 5.0, now=1001.0)
        self.assertEqual(again["id"], self.sid)

    def test_telemetry_rejected_without_active_shipment(self):
        core.complete_shipment(self.db, self.sid, now=2000.0)
        r = self.send("m1", 9.5, 2001.0)
        self.assertFalse(r["accepted"])

    def test_timeline_contains_key_events(self):
        self.send("m1", 9.5, 1001.0)
        aid = core.list_alerts(self.db)[0]["id"]
        core.transition_alert(self.db, aid, "resolve", "op", "done", now=1005.0)
        core.complete_shipment(self.db, self.sid, now=1010.0)
        tl = core.get_timeline(self.db, self.sid)
        kinds = [i["kind"] for i in tl["items"]]
        self.assertIn("shipment", kinds)
        self.assertIn("violation", kinds)
        self.assertIn("alert_event", kinds)


if __name__ == "__main__":
    unittest.main()
