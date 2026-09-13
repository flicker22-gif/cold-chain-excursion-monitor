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


class TestBackfillOfflineConsistency(CoreTestBase):
    """断网 → 缓存数据补传（先到正常、再到越限）→ 恢复上线：设备状态、告警、时间线一致。"""

    def go_offline(self):
        core.set_device_online(self.db, "dev-1", True, now=1000.0)
        core.set_device_online(self.db, "dev-1", False, now=1005.0)
        return next(a for a in core.list_alerts(self.db, status="open") if a["type"] == "OFFLINE")

    def test_late_normal_data_does_not_mark_device_online(self):
        # 第 1 步：断网，看门狗/LWT 判离线，开出 OFFLINE 告警
        off = self.go_offline()
        self.assertEqual(core.list_devices(self.db)[0]["online"], 0)

        # 第 2 步：先补传一条断网期间缓存的【正常】数据（ts 远早于到达时刻）
        r = self.send("buf-1", 5.0, 1006.0, now=1010.0)
        self.assertTrue(r["backfilled"])
        # 修复前的 bug：历史样本把设备翻成在线，离线单却还开着
        self.assertEqual(core.list_devices(self.db)[0]["online"], 0,
                         "补传的历史样本不能证明链路恢复，设备必须仍为离线")
        a = core.get_alert(self.db, off["id"])
        self.assertEqual(a["status"], "OPEN", "离线告警必须仍开着，状态不能不一致")
        self.assertFalse(any(e["action"] == "AUTO_RESOLVED" for e in a["events"]))
        # 最后心跳也不能被历史样本刷新，否则看门狗再也判不出离线
        self.assertLessEqual(core.list_devices(self.db)[0]["last_seen"], 1005.0)

    def test_backfilled_violation_still_opens_temp_alert_while_offline(self):
        # 补传期间超温行为保留：设备仍离线，但断网期间的越限不能丢、要开新单
        off = self.go_offline()
        self.send("buf-1", 5.0, 1006.0, now=1010.0)   # 先到的正常缓存数据
        r = self.send("buf-2", 9.8, 1007.0, now=1010.0)  # 随后到的越限缓存数据
        self.assertTrue(r["alert"]["opened"])

        open_alerts = core.list_alerts(self.db, status="open")
        types = {a["type"] for a in open_alerts}
        self.assertEqual(types, {"OFFLINE", "TEMP_HIGH"})  # 离线单仍开着，超温单也开了
        self.assertEqual(core.list_devices(self.db)[0]["online"], 0)

    def test_realtime_data_resolves_offline_alert_without_status_message(self):
        # 第 3 步（无显式 online 状态消息时）：一条实时数据本身即恢复证据
        off = self.go_offline()
        self.send("buf-1", 5.0, 1006.0, now=1010.0)
        core.ingest_telemetry(self.db, "dev-1", "live-1", None, 5.1,
                              device_ts=1011.0, now=1011.0)
        self.assertEqual(core.list_devices(self.db)[0]["online"], 1)
        a = core.get_alert(self.db, off["id"])
        self.assertEqual(a["status"], "RESOLVED")
        self.assertTrue(any(e["action"] == "AUTO_RESOLVED" for e in a["events"]))

    def test_online_status_after_backfill_closes_alert_and_timeline_is_coherent(self):
        # 第 3 步（正常路径）：设备重连发 online，离线单自动关闭
        off = self.go_offline()
        self.send("buf-1", 5.0, 1006.0, now=1010.0)
        self.send("buf-2", 9.8, 1007.0, now=1010.0)
        temp_alert = next(a for a in core.list_alerts(self.db, status="open")
                          if a["type"] == "TEMP_HIGH")
        core.set_device_online(self.db, "dev-1", True, now=1011.0)

        self.assertEqual(core.list_devices(self.db)[0]["online"], 1)
        self.assertEqual(core.get_alert(self.db, off["id"])["status"], "RESOLVED")
        # 恢复上线只关离线单，断网期间的超温单保持打开（人工处理，自动关单仅限 OFFLINE）
        self.assertEqual(core.get_alert(self.db, temp_alert["id"])["status"], "OPEN")

        # 时间线：越限样本按设备时刻落在 1007；离线单 OPENED/AUTO_RESOLVED
        # 按服务端时刻分别落在 1005/1011，顺序与真实因果一致，不出现“先恢复后离线”
        tl = core.get_timeline(self.db, self.sid)
        off_events = [(it["ts"], it["action"]) for it in tl["items"]
                      if it["kind"] == "alert_event" and it["alert_id"] == off["id"]]
        self.assertEqual([a for _, a in off_events], ["OPENED", "AUTO_RESOLVED"])
        self.assertTrue(all(ts <= 1005.0 for ts, a in off_events if a == "OPENED"))
        self.assertTrue(all(ts >= 1011.0 for ts, a in off_events if a == "AUTO_RESOLVED"))
        violations = [it for it in tl["items"] if it["kind"] == "violation"]
        self.assertEqual([(v["ts"], v["backfilled"]) for v in violations], [(1007.0, True)])

    def test_duplicate_backfill_does_not_touch_device_state(self):
        self.go_offline()
        self.send("buf-1", 5.0, 1006.0, now=1010.0)
        r = self.send("buf-1", 5.0, 1006.0, now=1012.0)  # 补报重发
        self.assertTrue(r["duplicated"])
        self.assertEqual(core.list_devices(self.db)[0]["online"], 0)
        self.assertLessEqual(core.list_devices(self.db)[0]["last_seen"], 1005.0)


class TestOfflineRecoveryCycle(CoreTestBase):
    """断网 → 恢复 → 重复上线：离线告警自动结束并留恢复记录，超温告警不被顺手关掉。"""

    def trip_offline(self, now):
        core.set_device_online(self.db, "dev-1", False, now=now)
        return next(a for a in core.list_alerts(self.db, status="open") if a["type"] == "OFFLINE")

    def test_disconnect_recover_duplicate_online(self):
        # 先制造一张未关闭的超温告警（设备随实时数据在线）
        self.send("t1", 9.5, 990.0, now=990.0)
        temp_alert = next(a for a in core.list_alerts(self.db, status="open")
                          if a["type"] == "TEMP_HIGH")

        # 断网：离线告警开单
        off = self.trip_offline(1000.0)
        self.assertEqual(off["status"], "OPEN")

        # 恢复上线：离线告警自动结束 + 恢复记录；超温告警保持打开
        core.set_device_online(self.db, "dev-1", True, now=1005.0, source="mqtt-status")
        off_after = core.get_alert(self.db, off["id"])
        self.assertEqual(off_after["status"], "RESOLVED")
        self.assertEqual(off_after["resolved_at"], 1005.0)
        auto = [e for e in off_after["events"] if e["action"] == "AUTO_RESOLVED"]
        self.assertEqual(len(auto), 1)
        self.assertIn("恢复在线", auto[0]["note"])  # 恢复记录留痕
        temp_after = core.get_alert(self.db, temp_alert["id"])
        self.assertEqual(temp_after["status"], "OPEN", "恢复在线不能顺手关掉超温告警")
        self.assertFalse(any(e["action"] in ("RESOLVED", "AUTO_RESOLVED")
                             for e in temp_after["events"]))

        # 重复上线（retained 重投/重复上报）：幂等，不产生重复恢复记录
        core.set_device_online(self.db, "dev-1", True, now=1006.0, source="mqtt-status")
        core.set_device_online(self.db, "dev-1", True, now=1007.0, source="mqtt-status")
        off_again = core.get_alert(self.db, off["id"])
        self.assertEqual(off_again["status"], "RESOLVED")
        self.assertEqual(len(off_again["events"]), len(off_after["events"]))
        self.assertEqual(len(core.list_alerts(self.db)), 2)  # 没有新开单

        # 接口视角：未关闭的只剩超温告警；设备在线
        self.assertEqual([a["type"] for a in core.list_alerts(self.db, status="open")],
                         ["TEMP_HIGH"])
        self.assertEqual(core.list_devices(self.db)[0]["online"], 1)

        # 时间线视角：离线单 OPENED → AUTO_RESOLVED；超温单只有 OPENED，无关闭类事件
        tl = core.get_timeline(self.db, self.sid)
        off_events = [it["action"] for it in tl["items"]
                      if it["kind"] == "alert_event" and it["alert_id"] == off["id"]]
        self.assertEqual(off_events, ["OPENED", "AUTO_RESOLVED"])
        temp_events = [it["action"] for it in tl["items"]
                       if it["kind"] == "alert_event" and it["alert_id"] == temp_alert["id"]]
        self.assertEqual(temp_events, ["OPENED"])

    def test_second_offline_cycle_opens_new_alert(self):
        core.set_device_online(self.db, "dev-1", True, now=995.0)
        off1 = self.trip_offline(1000.0)
        core.set_device_online(self.db, "dev-1", True, now=1005.0)
        off2 = self.trip_offline(1010.0)  # 第二次断网 → 新开一张离线单
        self.assertNotEqual(off1["id"], off2["id"])
        core.set_device_online(self.db, "dev-1", True, now=1015.0)
        # 两张离线单各自独立关闭、各自留恢复记录
        for off in (off1, off2):
            a = core.get_alert(self.db, off["id"])
            self.assertEqual(a["status"], "RESOLVED")
            self.assertEqual([e["action"] for e in a["events"]], ["OPENED", "AUTO_RESOLVED"])


class TestOfflineRecoveryApi(CoreTestBase):
    """接口视角：断网/恢复/重复上线的状态变化在 API 与时间线上可见。"""

    def setUp(self):
        super().setUp()
        from server import app as server_app
        self.app_mod = server_app
        self._old_db = server_app.DB_PATH
        server_app.DB_PATH = self.db
        self.client = server_app.app.test_client()

    def tearDown(self):
        self.app_mod.DB_PATH = self._old_db
        super().tearDown()

    def test_api_and_timeline_reflect_offline_recovery(self):
        self.send("t1", 9.5, 990.0, now=990.0)  # 超温告警开着
        core.set_device_online(self.db, "dev-1", False, now=1000.0)

        # 断网后：API 能看到离线 + 超温两张未关闭单，设备离线
        open_types = {a["type"] for a in self.client.get("/api/alerts?status=open").get_json()}
        self.assertEqual(open_types, {"TEMP_HIGH", "OFFLINE"})
        self.assertEqual(self.client.get("/api/devices").get_json()[0]["online"], 0)

        # 恢复 + 重复上线
        core.set_device_online(self.db, "dev-1", True, now=1005.0, source="mqtt-status")
        core.set_device_online(self.db, "dev-1", True, now=1006.0, source="mqtt-status")

        # API：未关闭的只剩超温单；设备在线；离线单已关闭且 resolved_at 是恢复时刻
        open_types = {a["type"] for a in self.client.get("/api/alerts?status=open").get_json()}
        self.assertEqual(open_types, {"TEMP_HIGH"})
        self.assertEqual(self.client.get("/api/devices").get_json()[0]["online"], 1)
        resolved = self.client.get("/api/alerts?status=RESOLVED").get_json()
        off = next(a for a in resolved if a["type"] == "OFFLINE")
        self.assertEqual(off["resolved_at"], 1005.0)

        # 时间线：离线单 OPENED → AUTO_RESOLVED；超温单无任何关闭类事件
        tl = self.client.get(f"/api/shipments/{self.sid}/timeline").get_json()
        actions = [(it["alert_type"], it["action"]) for it in tl["items"]
                   if it["kind"] == "alert_event"]
        self.assertIn(("OFFLINE", "OPENED"), actions)
        self.assertIn(("OFFLINE", "AUTO_RESOLVED"), actions)
        temp_actions = [a for t, a in actions if t == "TEMP_HIGH"]
        self.assertEqual(temp_actions, ["OPENED"])


class TestOfflineInheritance(CoreTestBase):
    """上一趟结束时设备已离线：新任务继承离线状态，宽限期后告警，重复检查不重复开单。"""

    def end_trip_offline(self):
        """上一趟运输途中掉线，任务结束时设备仍未恢复；返回新创建的下一趟任务。"""
        core.set_device_online(self.db, "dev-1", True, now=1001.0)
        core.set_device_online(self.db, "dev-1", False, now=1002.0)  # 老任务的 OFFLINE 单
        core.complete_shipment(self.db, self.sid, now=1003.0)
        # 设备一直离线……
        return core.create_shipment(self.db, "下一趟", "dev-1", 2.0, 8.0, 5.0, now=1010.0)

    def offline_alerts(self, shipment_id):
        return [a for a in core.list_alerts(self.db, shipment_id=shipment_id)
                if a["type"] == "OFFLINE"]

    def test_new_shipment_inherits_offline_after_grace(self):
        ship2 = self.end_trip_offline()

        core.check_timeouts(self.db, now=1014.0)  # 宽限期内：不开单
        self.assertEqual(self.offline_alerts(ship2["id"]), [])

        core.check_timeouts(self.db, now=1016.0)  # 宽限期后：继承离线状态开单
        offs = self.offline_alerts(ship2["id"])
        self.assertEqual(len(offs), 1)
        self.assertEqual(offs[0]["status"], "OPEN")

        core.check_timeouts(self.db, now=1017.0)  # 重复检查不重复开单
        core.check_timeouts(self.db, now=1018.0)
        self.assertEqual(len(self.offline_alerts(ship2["id"])), 1)

    def test_recovery_auto_resolves_inherited_alert(self):
        ship2 = self.end_trip_offline()
        core.check_timeouts(self.db, now=1016.0)
        off2 = self.offline_alerts(ship2["id"])[0]
        old_off = self.offline_alerts(self.sid)[0]  # 老任务那张还开着

        core.set_device_online(self.db, "dev-1", True, now=1020.0, source="mqtt-status")
        for off in (off2, old_off):  # 新老两张离线单都自动关闭并留恢复记录
            a = core.get_alert(self.db, off["id"])
            self.assertEqual(a["status"], "RESOLVED")
            self.assertTrue(any(e["action"] == "AUTO_RESOLVED" for e in a["events"]))

        # 新任务的时间线能看到完整状态变化：开单 → 自动恢复
        tl = core.get_timeline(self.db, ship2["id"])
        actions = [it["action"] for it in tl["items"] if it["kind"] == "alert_event"]
        self.assertEqual(actions, ["OPENED", "AUTO_RESOLVED"])

    def test_never_seen_device_alerts_after_grace(self):
        core.complete_shipment(self.db, self.sid, now=1000.0)
        ship2 = core.create_shipment(self.db, "新车首趟", "dev-2", 2.0, 8.0, 5.0, now=1000.0)
        core.check_timeouts(self.db, now=1004.0)  # 宽限期内
        self.assertEqual(self.offline_alerts(ship2["id"]), [])
        core.check_timeouts(self.db, now=1006.0)  # 设备从未上线，宽限期后告警
        offs = self.offline_alerts(ship2["id"])
        self.assertEqual(len(offs), 1)
        core.set_device_online(self.db, "dev-2", True, now=1008.0)  # 设备终于上线
        self.assertEqual(core.get_alert(self.db, offs[0]["id"])["status"], "RESOLVED")

    def test_recovery_within_grace_no_alert(self):
        ship2 = self.end_trip_offline()
        core.set_device_online(self.db, "dev-1", True, now=1013.0)  # 宽限期内恢复
        core.check_timeouts(self.db, now=1016.0)  # 距恢复仅 3s，未超宽限
        self.assertEqual(core.list_alerts(self.db, shipment_id=ship2["id"]), [])
        # 设备持续有数据，之后也不会被误判离线
        self.send("hb-1", 5.0, 1018.0, now=1018.0)
        core.check_timeouts(self.db, now=1020.0)
        self.assertEqual(core.list_alerts(self.db, shipment_id=ship2["id"]), [])

    def test_inherited_alert_has_assignee_and_escalates(self):
        core.complete_shipment(self.db, self.sid, now=1000.0)
        ship2 = core.create_shipment(self.db, "专车", "dev-2", 2.0, 8.0, 5.0, now=1000.0,
                                     escalation_chain=["调度-A", "主管-B"], escalate_after_sec=10.0)
        core.check_timeouts(self.db, now=1006.0)
        off = self.offline_alerts(ship2["id"])[0]
        self.assertEqual(off["assignee"], "调度-A")
        self.assertTrue(any(e["action"] == "NOTIFY"
                            for e in core.get_alert(self.db, off["id"])["events"]))
        core.check_escalations(self.db, now=1017.0)  # 继承的离线单同样走升级链
        self.assertEqual(core.get_alert(self.db, off["id"])["assignee"], "主管-B")


class ChainTestBase(CoreTestBase):
    """另建一个带负责人升级链的任务（dev-2）：链 调度-A → 主管-B → 总监-C，10s 未处理完升级。"""

    def setUp(self):
        super().setUp()
        self.ship2 = core.create_shipment(
            self.db, "冷链专车", "dev-2", 2.0, 8.0, 5.0, now=1000.0,
            escalation_chain=["调度-A", "主管-B", "总监-C"], escalate_after_sec=10.0)
        self.sid2 = self.ship2["id"]

    def send2(self, msg_id, temp, ts, now=None):
        return core.ingest_telemetry(self.db, "dev-2", msg_id, seq=None,
                                     temp=temp, device_ts=ts, now=now if now is not None else ts)

    def open_alert2(self, msg_id="m1", temp=9.0, ts=1001.0):
        self.send2(msg_id, temp, ts)
        return next(a for a in core.list_alerts(self.db, shipment_id=self.sid2))


class TestSeverity(CoreTestBase):
    def test_severity_upgrades_with_deviation(self):
        self.send("m1", 8.5, 1001.0)   # 偏差 0.5℃ → L1
        a = core.list_alerts(self.db)[0]
        self.assertEqual(a["severity"], "L1")
        self.send("m2", 10.5, 1002.0)  # 偏差 2.5℃ → L2
        self.send("m3", 12.5, 1003.0)  # 偏差 4.5℃ → L3
        a = core.get_alert(self.db, a["id"])
        self.assertEqual(a["severity"], "L3")
        ups = [e for e in a["events"] if e["action"] == "LEVEL_UP"]
        self.assertEqual(len(ups), 2)  # L1→L2→L3 各留痕一次

    def test_severity_upgrades_with_duration(self):
        self.send("m1", 8.5, 1001.0)
        a = core.list_alerts(self.db)[0]
        self.send("m2", 8.6, 1121.0)   # 持续 120s → L2
        self.assertEqual(core.get_alert(self.db, a["id"])["severity"], "L2")
        self.send("m3", 8.6, 1311.0)   # 持续 310s → L3
        self.assertEqual(core.get_alert(self.db, a["id"])["severity"], "L3")

    def test_severity_never_downgrades(self):
        self.send("m1", 10.5, 1001.0)  # 开单即 L2
        a = core.list_alerts(self.db)[0]
        self.assertEqual(a["severity"], "L2")
        self.send("m2", 8.5, 1002.0)   # 偏差回落但仍越限 → 并入，级别不降
        a = core.get_alert(self.db, a["id"])
        self.assertEqual(a["severity"], "L2")
        self.assertFalse(any(e["action"] == "LEVEL_DOWN" for e in a["events"]))

    def test_low_temp_deviation_grading(self):
        self.send("m1", -1.0, 1001.0)  # 低于下限 3℃ → L2
        a = core.list_alerts(self.db)[0]
        self.assertEqual((a["type"], a["severity"]), ("TEMP_LOW", "L2"))


class TestNotify(ChainTestBase):
    def test_notify_once_per_level_and_assignee(self):
        a = self.open_alert2()         # L1，通知 调度-A
        notifies = [e for e in core.get_alert(self.db, a["id"])["events"]
                    if e["action"] == "NOTIFY"]
        self.assertEqual(len(notifies), 1)
        self.assertIn("调度-A", notifies[0]["note"])

        self.send2("m2", 8.6, 1002.0)  # 同级继续越限 → 不重复通知
        self.send2("m3", 8.7, 1003.0)
        notifies = [e for e in core.get_alert(self.db, a["id"])["events"]
                    if e["action"] == "NOTIFY"]
        self.assertEqual(len(notifies), 1)

        self.send2("m4", 10.5, 1004.0)  # 级别上升 → 重新通知现任负责人
        notifies = [e for e in core.get_alert(self.db, a["id"])["events"]
                    if e["action"] == "NOTIFY"]
        self.assertEqual(len(notifies), 2)
        self.assertIn("L2", notifies[1]["note"])

    def test_no_chain_no_notify(self):
        self.send("x1", 9.5, 1001.0)   # dev-1 的任务没有升级链
        a = core.list_alerts(self.db, shipment_id=self.sid)[0]
        self.assertIsNone(a["assignee"])
        self.assertFalse(any(e["action"] == "NOTIFY"
                             for e in core.get_alert(self.db, a["id"])["events"]))


class TestEscalation(ChainTestBase):
    def test_auto_escalates_along_chain_then_stops_at_top(self):
        a = self.open_alert2()
        self.assertEqual(a["assignee"], "调度-A")

        core.check_escalations(self.db, now=1005.0)  # 未超 10s
        self.assertEqual(core.get_alert(self.db, a["id"])["assignee"], "调度-A")

        core.check_escalations(self.db, now=1012.0)  # 调度-A 超时 → 主管-B
        a = core.get_alert(self.db, a["id"])
        self.assertEqual((a["status"], a["assignee"]), ("ESCALATED", "主管-B"))

        core.check_escalations(self.db, now=1023.0)  # 主管-B 超时 → 总监-C
        a = core.get_alert(self.db, a["id"])
        self.assertEqual(a["assignee"], "总监-C")
        n_events = len(a["events"])

        core.check_escalations(self.db, now=1040.0)  # 已到链顶，不再升级、不再通知
        a = core.get_alert(self.db, a["id"])
        self.assertEqual(a["assignee"], "总监-C")
        self.assertEqual(len(a["events"]), n_events)

        escalations = [e for e in a["events"] if e["action"] == "ESCALATED"]
        self.assertTrue(all(e["actor"] == "system" for e in escalations))
        notifies = [e for e in a["events"] if e["action"] == "NOTIFY"]
        self.assertEqual(len(notifies), 3)  # 三位负责人各通知一次，同级不重复

    def test_resolved_alert_is_never_escalated(self):
        a = self.open_alert2()
        core.transition_alert(self.db, a["id"], "resolve", "主管-B", "处理完", now=1003.0)
        core.check_escalations(self.db, now=5000.0)
        a = core.get_alert(self.db, a["id"])
        self.assertEqual(a["status"], "RESOLVED")
        self.assertEqual(a["assignee"], "调度-A")
        self.assertFalse(any(e["action"] == "ESCALATED" for e in a["events"]))

    def test_late_data_does_not_rearm_resolved_alert(self):
        a = self.open_alert2()  # L1，负责人 调度-A
        self.send2("m2", 9.2, 1002.0)  # 异常窗口扩到 [1001, 1002]
        core.transition_alert(self.db, a["id"], "resolve", "主管-B", "处理完", now=1010.0)
        before = core.get_alert(self.db, a["id"])

        # 迟到数据落在已处理窗口内，且温度极端（若重算级别会到 L3）
        r = self.send2("late-1", 15.0, 1001.5, now=1020.0)
        self.assertTrue(r["alert"]["late"])
        core.check_escalations(self.db, now=5000.0)

        after = core.get_alert(self.db, a["id"])
        self.assertEqual(after["status"], "RESOLVED")
        self.assertEqual(after["peak_temp"], 15.0)            # 统计被修正
        self.assertEqual(after["severity"], before["severity"])  # 级别冻结在关闭时
        self.assertEqual(after["assignee"], before["assignee"])
        new_actions = [e["action"] for e in after["events"][len(before["events"]):]]
        self.assertEqual(new_actions, ["LATE_DATA"])          # 只留痕，不升级不通知

    def test_manual_escalate_advances_assignee(self):
        a = self.open_alert2()
        a = core.transition_alert(self.db, a["id"], "escalate", "调度-A", "处理不了", now=1002.0)
        self.assertEqual(a["assignee"], "主管-B")
        notifies = [e for e in a["events"] if e["action"] == "NOTIFY"]
        self.assertEqual(len(notifies), 2)  # 开单通知调度-A + 升级通知主管-B
        self.assertIn("主管-B", notifies[1]["note"])

        a = core.transition_alert(self.db, a["id"], "escalate", "主管-B", "", now=1003.0)
        self.assertEqual(a["assignee"], "总监-C")
        n_events = len(a["events"])
        a = core.transition_alert(self.db, a["id"], "escalate", "总监-C", "", now=1004.0)
        self.assertEqual(a["assignee"], "总监-C")  # 链顶：记事件但负责人不变、不重复通知
        self.assertEqual(len(a["events"]), n_events + 1)

    def test_ack_allowed_after_escalation(self):
        a = self.open_alert2()
        core.check_escalations(self.db, now=1012.0)  # 自动升级到 主管-B
        a = core.transition_alert(self.db, a["id"], "ack", "主管-B", "我来处理", now=1013.0)
        self.assertEqual(a["status"], "ACKED")

    def test_completed_shipment_stops_escalation(self):
        a = self.open_alert2()
        core.complete_shipment(self.db, self.sid2, now=1002.0)
        core.check_escalations(self.db, now=5000.0)
        a = core.get_alert(self.db, a["id"])
        self.assertEqual(a["assignee"], "调度-A")
        self.assertFalse(any(e["action"] == "ESCALATED" for e in a["events"]))

    def test_offline_alert_has_severity_and_escalates(self):
        core.set_device_online(self.db, "dev-2", True, now=1000.0)
        core.set_device_online(self.db, "dev-2", False, now=1005.0)
        a = next(x for x in core.list_alerts(self.db, status="open") if x["type"] == "OFFLINE")
        self.assertEqual((a["severity"], a["assignee"]), ("L2", "调度-A"))
        self.assertTrue(any(e["action"] == "NOTIFY"
                            for e in core.get_alert(self.db, a["id"])["events"]))

        core.check_escalations(self.db, now=1016.0)  # 离线 11s 没人处理 → 升级
        self.assertEqual(core.get_alert(self.db, a["id"])["assignee"], "主管-B")

        core.set_device_online(self.db, "dev-2", True, now=1020.0)  # 恢复在线自动关闭
        self.assertEqual(core.get_alert(self.db, a["id"])["status"], "RESOLVED")
        n_events = len(core.get_alert(self.db, a["id"])["events"])
        core.check_escalations(self.db, now=5000.0)  # 已关闭的离线单不再升级
        self.assertEqual(len(core.get_alert(self.db, a["id"])["events"]), n_events)


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
