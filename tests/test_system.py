"""系统级测试：时钟校正、补偿封存、纪元边界、损坏隔离与崩溃续跑。"""
from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
import urllib.request

from twin_replay.clock import correct_device_time, iso, to_epoch
from twin_replay.engine import FoldState, fold_events
from twin_replay.exceptions import Conflict, CorruptEvent, NotFound
from twin_replay.models import (
    SRC_COMPENSATED,
    SRC_CONFIRMED,
    SRC_REALTIME,
    canonical_digest,
)
from twin_replay.parsing import event_checksum, parse_batch, parse_event
from twin_replay.service import TwinService
from twin_replay.storage import Storage


def evt(event_id, device="WT-1", t="2026-10-07T10:00:00Z", metrics=None,
        unit="SN-A", model="v3", offset=0.0, kind="telemetry", batch="b1",
        comp=False, checksum=False):
    raw = {"event_id": event_id, "device_id": device, "kind": kind,
           "device_time": t, "clock_offset": offset,
           "metrics": metrics or {}, "unit_sn": unit, "model_version": model,
           "batch_id": batch}
    if comp:
        raw["is_compensation"] = True
    if checksum and kind == "telemetry":
        raw["checksum"] = event_checksum(raw)
    return raw


class ClockTests(unittest.TestCase):
    def test_positive_offset_means_fast_clock(self):
        # 设备时钟快 30 秒：显示 10:00:30 时基准时间是 10:00:00
        self.assertAlmostEqual(
            correct_device_time("2026-10-07T10:00:30Z", 30.0),
            to_epoch("2026-10-07T10:00:00Z"))

    def test_negative_offset_means_slow_clock(self):
        self.assertAlmostEqual(
            correct_device_time("2026-10-07T09:59:30Z", -30.0),
            to_epoch("2026-10-07T10:00:00Z"))

    def test_late_event_is_ordered_by_corrected_time(self):
        # 接收顺序颠倒，但校正时间决定真实先后
        late = parse_event(evt("a", t="2026-10-07T10:00:00Z"))
        early = parse_event(evt("b", t="2026-10-07T09:00:00Z"))
        self.assertLess(early.corrected_ts, late.corrected_ts)


class ParsingTests(unittest.TestCase):
    def test_partial_corruption_is_quarantined(self):
        payload = "\n".join([
            json.dumps(evt("g1"), ensure_ascii=False),
            "{坏 json",
            json.dumps({"event_id": "g2", "device_id": "WT-1"}, ensure_ascii=False),
            json.dumps(evt("g3"), ensure_ascii=False),
        ])
        result = parse_batch(payload, batch_id="bx")
        self.assertEqual([e.event_id for e in result.accepted], ["g1", "g3"])
        self.assertEqual([c["position"] for c in result.corrupted], [1, 2])

    def test_in_batch_and_persistent_duplicates(self):
        dup = evt("g1")
        result = parse_batch(
            json.dumps([evt("g1"), dup, evt("g2")]), batch_id="bx")
        self.assertEqual(len(result.accepted), 2)
        self.assertEqual(result.duplicates, ["g1"])

        svc = TwinService(Storage(":memory:"))
        svc.import_batch(json.dumps([evt("g1")]), batch_id="bx")
        report = svc.import_batch(json.dumps([evt("g1")]), batch_id="by")
        self.assertEqual(report["accepted_count"], 1)
        self.assertEqual(report["duplicates_count"], 1)
        self.assertEqual(len(svc.storage.events_for_device("WT-1")), 1)

    def test_checksum_mismatch_is_corruption(self):
        raw = evt("g1", checksum=True)
        raw["metrics"]["温度"] = 999.0  # 篡改但未重算校验和
        with self.assertRaises(CorruptEvent):
            parse_event(raw)

    def test_corrupt_line_does_not_poison_batch_report(self):
        svc = TwinService(Storage(":memory:"))
        report = svc.import_batch(
            json.dumps([evt("g1"), {"event_id": "x"}]), batch_id="bz")
        self.assertEqual(report["accepted_count"], 1)
        self.assertEqual(report["corrupted_count"], 1)
        batches = svc.storage.list_batches()
        self.assertEqual(batches[0]["corrupted_count"], 1)


class SealAndCompensationTests(unittest.TestCase):
    def setUp(self):
        self.svc = TwinService(Storage(":memory:"))
        self.svc.set_threshold("WT-1", "温度", min_value=0, max_value=80.0)

    def _ingest_base(self):
        rows = [
            evt("t1", t="2026-10-07T10:00:00Z", metrics={"温度": 70.0}),
            evt("t2", t="2026-10-07T10:01:00Z", metrics={"温度": 85.0}),
            evt("t3", t="2026-10-07T10:02:00Z", metrics={"温度": 70.0}),
        ]
        self.svc.import_batch(json.dumps(rows), batch_id="b1")

    def test_late_event_becomes_compensation_and_keeps_seal_intact(self):
        self._ingest_base()
        sealed = self.svc.confirm("WT-1", "2026-10-07T10:02:00Z")
        digest_before = sealed["digest"]

        # 迟到到封存区间内的事件：对 10:02:00 同一时刻读数的修正值
        self.svc.import_batch(
            json.dumps([evt("late", t="2026-10-07T10:02:00Z",
                            metrics={"温度": 71.5}, batch="b-late")]),
            batch_id="b-late")
        stored = [e for e in self.svc.storage.events_for_device("WT-1")
                  if e.event_id == "late"][0]
        self.assertTrue(stored.is_compensation)

        # 确认视图不变，验真通过
        verified = self.svc.verify_seal("WT-1")
        self.assertTrue(verified["intact"])
        confirmed = self.svc.view("WT-1", "2026-10-07T10:02:00Z",
                                  mode="confirmed")
        self.assertEqual(confirmed["fields"]["温度"]["value"], 70.0)
        self.assertEqual(confirmed["fields"]["温度"]["source"], SRC_CONFIRMED)
        self.assertEqual(
            self.svc.storage.get_sealed_snapshot(
                "WT-1", to_epoch("2026-10-07T10:02:00Z"))["digest"],
            digest_before)

        # 修订视图采用补偿值，且逐字段标注来源
        revised = self.svc.view("WT-1", "2026-10-07T10:02:00Z",
                                mode="revised")
        self.assertEqual(revised["fields"]["温度"]["value"], 71.5)
        self.assertEqual(revised["fields"]["温度"]["source"],
                         SRC_COMPENSATED)
        self.assertIn("温度", revised["compensated_metrics"])
        self.assertIn("late", revised["compensation_events"])

    def test_water_line_cannot_move_backward(self):
        self._ingest_base()
        self.svc.confirm("WT-1", "2026-10-07T10:02:00Z")
        with self.assertRaises(Conflict):
            self.svc.confirm("WT-1", "2026-10-07T10:01:00Z")

    def test_tampering_with_stored_body_breaks_verification(self):
        self._ingest_base()
        self.svc.confirm("WT-1", "2026-10-07T10:02:00Z")
        conn = self.svc.storage.conn
        conn.execute("UPDATE sealed_snapshots SET body_json=? WHERE device_id=?",
                     (json.dumps({"tampered": True}), "WT-1"))
        conn.commit()
        verified = self.svc.verify_seal("WT-1")
        self.assertFalse(verified["intact"])

    def test_anomaly_window_is_sealed_then_revised_by_compensation(self):
        self._ingest_base()
        sealed = self.svc.confirm("WT-1", "2026-10-07T10:01:30Z")
        # 封存时超温窗口仍开放
        self.assertEqual([w["metric"] for w in sealed["open_anomalies"]],
                         ["温度"])
        rows = self.svc.storage.sealed_windows(
            "WT-1", to_epoch("2026-10-07T10:01:30Z"))
        self.assertIsNone(rows[0]["end_ts"])

        # 补偿一条 09:59 就已恢复的数据 -> 修订视图认为窗口更早就关了
        self.svc.import_batch(
            json.dumps([evt("c", t="2026-10-07T09:59:00Z",
                            metrics={"温度": 90.0}, batch="b2")]),
            batch_id="b2")  # 更早的超温起点补偿
        revised = self.svc.view("WT-1", "2026-10-07T10:02:00Z")
        window = [w for w in revised["closed_anomalies"]
                  if w["metric"] == "温度"][0]
        self.assertTrue(window["compensated"])

    def test_on_time_event_after_seal_is_realtime_not_compensation(self):
        self._ingest_base()
        self.svc.confirm("WT-1", "2026-10-07T10:02:00Z")
        self.svc.import_batch(
            json.dumps([evt("new", t="2026-10-07T10:05:00Z",
                            metrics={"温度": 66.0}, batch="b3")]),
            batch_id="b3")
        view = self.svc.view("WT-1", "2026-10-07T10:05:00Z")
        self.assertEqual(view["fields"]["温度"]["source"], SRC_REALTIME)


class EpochTests(unittest.TestCase):
    def setUp(self):
        self.svc = TwinService(Storage(":memory:"))

    def test_device_replacement_clears_state(self):
        rows = [
            evt("t1", t="2026-10-07T10:00:00Z",
                metrics={"温度": 70.0, "负载": 0.7}),
            evt("r", t="2026-10-07T10:01:00Z", kind="device_replaced",
                metrics={"new_unit_sn": "SN-B"}, unit="SN-A"),
            evt("t2", t="2026-10-07T10:02:00Z",
                metrics={"温度": 50.0}, unit="SN-B"),
        ]
        self.svc.import_batch(json.dumps(rows), batch_id="b")
        cur = self.svc.current_state("WT-1")
        self.assertEqual(cur["epoch"]["unit_sn"], "SN-B")
        self.assertEqual(cur["fields"]["温度"]["value"], 50.0)
        # 负载属于旧设备，不应串到新设备视图里
        self.assertNotIn("负载", cur["fields"])

    def test_implicit_replacement_via_serial_number(self):
        rows = [
            evt("t1", t="2026-10-07T10:00:00Z",
                metrics={"温度": 70.0, "负载": 0.7}),
            evt("t2", t="2026-10-07T10:01:00Z",
                metrics={"温度": 50.0}, unit="SN-B"),
        ]
        self.svc.import_batch(json.dumps(rows), batch_id="b")
        cur = self.svc.current_state("WT-1")
        self.assertEqual(cur["epoch"]["unit_sn"], "SN-B")
        self.assertNotIn("负载", cur["fields"])

    def test_model_switch_keeps_measurements_but_resets_windows(self):
        self.svc.set_threshold("WT-1", "温度", min_value=0, max_value=80.0)
        rows = [
            evt("t1", t="2026-10-07T10:00:00Z", metrics={"温度": 90.0}),
            evt("m", t="2026-10-07T10:01:00Z", kind="model_switched",
                metrics={"new_model_version": "v4"}, model="v4", unit="SN-A"),
            evt("t2", t="2026-10-07T10:02:00Z", metrics={"温度": 91.0},
                model="v4"),
        ]
        self.svc.import_batch(json.dumps(rows), batch_id="b")
        cur = self.svc.current_state("WT-1")
        self.assertEqual(cur["epoch"]["model_version"], "v4")
        self.assertEqual(cur["fields"]["温度"]["value"], 91.0)
        # 旧模型下的超温窗口在切换点关闭，新模型下重新开窗
        closed_metrics = [(w["metric"], w["end_ts"]) for w in cur["closed_anomalies"]]
        self.assertTrue(any(m == "温度" for m, _ in closed_metrics))
        self.assertEqual(len(cur["open_anomalies"]), 1)

    def test_historical_view_inside_old_epoch(self):
        rows = [
            evt("t1", t="2026-10-07T10:00:00Z",
                metrics={"温度": 70.0, "负载": 0.7}),
            evt("r", t="2026-10-07T10:01:00Z", kind="device_replaced",
                metrics={"new_unit_sn": "SN-B"}, unit="SN-A"),
            evt("t2", t="2026-10-07T10:02:00Z",
                metrics={"温度": 50.0}, unit="SN-B"),
        ]
        self.svc.import_batch(json.dumps(rows), batch_id="b")
        view = self.svc.view("WT-1", "2026-10-07T10:00:30Z")
        self.assertEqual(view["epoch"]["unit_sn"], "SN-A")
        self.assertEqual(view["fields"]["负载"]["value"], 0.7)


class ReplayRecoveryTests(unittest.TestCase):
    def _populate(self, svc, n=25):
        rows = [evt(f"t{i:02d}", t=f"2026-10-07T10:{i:02d}:00Z",
                    metrics={"温度": 60.0 + i}, unit="SN-A")
                for i in range(n)]
        svc.import_batch(json.dumps(rows), batch_id="b")

    def test_checkpoint_resume_from_fresh_process(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "r.db")
            svc = TwinService(Storage(db))
            self._populate(svc)
            job = svc.create_replay("WT-1", "2026-10-07T10:24:00Z",
                                    job_id="j1")
            total = job["total_events"]
            svc.run_replay("j1", max_events=7)
            mid = svc.storage.get_job("j1")
            self.assertEqual(mid["status"], "running")
            self.assertEqual(mid["progress_pos"], 7)
            self.assertIsNotNone(mid["checkpoint_json"])
            svc.storage.close()

            # 全新进程：从未完成处继续
            svc2 = TwinService(Storage(db))
            recovered = svc2.resume_unfinished()
            self.assertEqual([j["job_id"] for j in recovered], ["j1"])
            final = svc2.get_replay("j1")
            self.assertEqual(final["status"], "done")
            self.assertEqual(final["progress_pos"], total)

            # 续跑结果与一次性完整回放一致
            direct = svc2.view("WT-1", "2026-10-07T10:24:00Z")
            self.assertEqual(final["result"]["fields"], direct["fields"])
            svc2.storage.close()

    def test_done_job_is_not_resumed(self):
        svc = TwinService(Storage(":memory:"))
        self._populate(svc, n=5)
        svc.create_replay("WT-1", "2026-10-07T10:04:00Z", job_id="j")
        svc.run_replay("j")
        self.assertEqual(svc.resume_unfinished(), [])

    def test_resume_pending_job_never_started(self):
        svc = TwinService(Storage(":memory:"))
        self._populate(svc, n=5)
        svc.create_replay("WT-1", "2026-10-07T10:04:00Z", job_id="j")
        done = svc.resume_unfinished()
        self.assertEqual(len(done), 1)
        self.assertEqual(done[0]["status"], "done")

    def test_frozen_event_set_ignores_late_arrivals(self):
        svc = TwinService(Storage(":memory:"))
        self._populate(svc, n=3)
        job = svc.create_replay("WT-1", "2026-10-07T10:02:00Z", job_id="j")
        self.assertEqual(job["total_events"], 3)
        # 任务冻结之后才到的新事件不改变该任务
        svc.import_batch(
            json.dumps([evt("later", t="2026-10-07T10:01:30Z",
                            metrics={"温度": 1.0}, comp=True, batch="b2")]),
            batch_id="b2")
        svc.run_replay("j")
        result = svc.get_replay("j")["result"]
        self.assertNotIn("later",
                         [c["event_id"] for c in result["fields"].values()])


class FoldDeterminismTests(unittest.TestCase):
    def test_checkpoint_roundtrip_matches_full_fold(self):
        svc = TwinService(Storage(":memory:"))
        svc.set_threshold("WT-1", "温度", min_value=0, max_value=80.0)
        rows = [evt(f"t{i}", t=f"2026-10-07T10:0{i}:00Z",
                    metrics={"温度": v}, unit="SN-A")
                for i, v in enumerate([70, 85, 90, 60])]
        svc.import_batch(json.dumps(rows), batch_id="b")
        events = svc.storage.events_for_device("WT-1")
        thresholds = svc.storage.get_thresholds("WT-1")

        full = fold_events(events, thresholds)
        st = FoldState()
        st = fold_events(events[:2], thresholds, state=st)
        ckpt = FoldState.from_checkpoint(st.to_checkpoint())
        st = fold_events(events[2:], thresholds, state=ckpt)
        self.assertEqual(st.snapshot("WT-1", 999).to_dict(),
                         full.snapshot("WT-1", 999).to_dict())


class ApiTests(unittest.TestCase):
    def test_http_roundtrip(self):
        from twin_replay.api import TwinServer
        svc = TwinService(Storage(":memory:"))
        server = TwinServer(svc, "127.0.0.1", 0)
        port = server.server_port
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            def post(path, body):
                req = urllib.request.Request(
                    f"http://127.0.0.1:{port}{path}",
                    data=json.dumps(body).encode(),
                    headers={"Content-Type": "application/json"},
                    method="POST")
                with urllib.request.urlopen(req) as resp:
                    return resp.status, json.loads(resp.read())

            def get(path):
                with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}") as r:
                    return json.loads(r.read())

            post("/devices/WT-1/thresholds",
                 {"metric": "温度", "min": 0, "max": 80})

            raw = json.dumps([evt("t1", metrics={"温度": 90.0})]).encode()
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/batches?batch_id=b1", data=raw,
                headers={"Content-Type": "application/json"}, method="POST")
            with urllib.request.urlopen(req) as resp:
                report = json.loads(resp.read())
            self.assertEqual(report["accepted_count"], 1)

            post("/devices/WT-1/confirm",
                 {"as_of": "2026-10-07T10:00:00Z"})
            state = get("/devices/WT-1/state")
            self.assertEqual(state["fields"]["温度"]["value"], 90.0)
            self.assertEqual(len(state["open_anomalies"]), 1)

            post("/replays", {"device_id": "WT-1",
                              "as_of": "2026-10-07T10:00:00Z"})
            jobs = get("/replays")["jobs"]
            self.assertEqual(len(jobs), 1)
            post(f"/replays/{jobs[0]['job_id']}/run", {})
            done = get(f"/replays/{jobs[0]['job_id']}")
            self.assertEqual(done["status"], "done")
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
