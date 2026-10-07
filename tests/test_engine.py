"""回放引擎端到端语义测试。"""
from __future__ import annotations

import json
import os
import tempfile
import unittest

from twin_replay import ReplayEngine, CorruptSnapshotError
from twin_replay.model import parse_event, parse_ts_ms, iso
from twin_replay.storage import JsonlLog

T = 1_700_000_000_000
GRID, GRACE = 10_000, 2_000
DEV = "WT-A01"


def ev(event_id, dt, **rest):
    return {"event_id": event_id, "device_id": DEV, "ts_ms": T + dt, **rest}


def temp(**fields):
    return fields


class EngineTestBase(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.dir = self._dir.name

    def tearDown(self):
        self._dir.cleanup()

    def engine(self):
        return ReplayEngine(self.dir, grid_ms=GRID, grace_ms=GRACE)


class TimeNormalizationTests(unittest.TestCase):
    def test_clock_skew_is_subtracted(self):
        row = {"event_id": "x", "device_id": "d", "ts_ms": 10_000,
               "clock_skew_ms": 300, "fields": {"a": 1}}
        self.assertEqual(parse_event(row, 9_999).t_ms, 9_700)
        row["clock_skew_ms"] = -500
        self.assertEqual(parse_event(row, 9_999).t_ms, 10_500)

    def test_iso_timestamp_supported(self):
        row = {"event_id": "x", "device_id": "d",
               "device_ts": "2026-10-07T10:00:00.300Z", "fields": {"a": 1}}
        self.assertEqual(parse_event(row, 0).t_ms,
                         parse_ts_ms("2026-10-07T10:00:00.300+00:00"))

    def test_seq_key_when_no_event_id(self):
        row = {"device_id": "d", "ts_ms": 1000, "seq": 7, "fields": {"a": 1}}
        event = parse_event(row, 0)
        self.assertEqual(event.key, "d:7")


class IngestTests(EngineTestBase):
    def test_batch_report_counts_and_deadletters(self):
        eng = self.engine()
        good = ev("e1", 0, fields={"温度": 70})
        report = eng.ingest([
            good,
            "坏行",                                   # 无法解析
            {"event_id": "eX", "device_id": DEV},     # 缺时间戳与字段
            ev("e1", 0, fields={"温度": 70}),         # 完全重复
            ev("e1", 0, fields={"温度": 99}),         # 同键冲突
        ], source="test", now=T + 1_000)
        self.assertEqual(report, {"received": 5, "accepted": 1, "duplicate": 1,
                                  "conflict": 1, "late_compensation": 0, "rejected": 2})
        dead = eng.deadletters()
        self.assertEqual(len(dead), 3)
        self.assertTrue(any("内容冲突" in d["reason"] for d in dead))

    def test_duplicate_after_restart_is_still_idempotent(self):
        eng = self.engine()
        eng.ingest([ev("e1", 0, fields={"温度": 70})], now=T + 1_000)
        eng2 = self.engine()
        report = eng2.ingest([ev("e1", 0, fields={"温度": 70})], now=T + 2_000)
        self.assertEqual(report["duplicate"], 1)
        self.assertEqual(report["accepted"], 0)


class SnapshotTests(EngineTestBase):
    def _advance_and_seal(self, eng):
        eng.ingest([
            ev("e1", 0, fields={"温度": 71.2, "负载": 80}),
            ev("e2", 5_000, fields={"温度": 72.0}),
        ], now=T + 6_000)
        eng.ingest([ev("e3", 30_000, fields={"温度": 90})], now=T + 32_000)

    def test_snapshots_sealed_on_grid(self):
        eng = self.engine()
        self._advance_and_seal(eng)
        boundaries = [s["window_end_ms"] for s in eng.list_snapshots(DEV)]
        self.assertEqual(boundaries, [T + 10_000, T + 20_000])

    def test_late_event_goes_to_compensation_and_keeps_snapshot(self):
        eng = self.engine()
        self._advance_and_seal(eng)
        snap_path = eng.storage.snapshot_path(DEV, T + 10_000)
        before = eng._read_snapshot(snap_path)
        with open(snap_path, "rb") as fh:
            raw_before = fh.read()

        report = eng.ingest([ev("e-late", 6_000, fields={"温度": 69.0})], now=T + 34_000)
        self.assertEqual(report["late_compensation"], 1)
        with open(snap_path, "rb") as fh:
            self.assertEqual(fh.read(), raw_before, "迟到数据不得改写已确认快照")
        self.assertEqual(before["state"]["fields"]["温度"], 72.0)

    def test_replay_marks_compensation_fields(self):
        eng = self.engine()
        self._advance_and_seal(eng)
        eng.ingest([ev("e-late", 6_000, fields={"温度": 69.0})], now=T + 34_000)

        # 早于补传事件：原始值
        early = eng.replay(DEV, T + 5_500)
        self.assertEqual(early["fields"]["温度"]["value"], 72.0)
        self.assertEqual(early["fields"]["温度"]["source"], "original")
        self.assertEqual(early["compensation"]["applied_events"], 0)

        # 晚于补传事件：补偿值并被显式标注
        view = eng.replay(DEV, T + 8_000)
        self.assertEqual(view["fields"]["温度"]["value"], 69.0)
        self.assertEqual(view["fields"]["温度"]["source"], "compensation")
        self.assertIn("温度", view["compensation"]["marked_fields"])
        # 未被补传触及的字段仍是原始来源
        self.assertEqual(view["fields"]["负载"]["source"], "original")

    def test_event_past_watermark_but_window_unsealed_is_original(self):
        # 稀疏到达：首个事件之后窗口从未封存；即便后来事件已越过水位线，
        # 落在尚未封存窗口内的迟到事件仍是原始数据
        eng = self.engine()
        eng.ingest([ev("e1", 0, fields={"温度": 70})], now=T + 1_000)
        report = eng.ingest([ev("e2", 5_000, fields={"温度": 71})], now=T + 30_000)
        self.assertEqual(report["late_compensation"], 0)
        self.assertEqual(report["accepted"], 1)
        view = eng.replay(DEV, T + 5_000)
        self.assertEqual(view["fields"]["温度"]["source"], "original")
        self.assertEqual(view["fields"]["温度"]["value"], 71)

    def test_batch_order_does_not_change_classification(self):
        # 同一批里无论先后，封存前各窗口都未关闭：两条都应是原始数据
        eng = self.engine()
        report = eng.ingest([
            ev("late", 1_000, fields={"温度": 69}),
            ev("adv", 30_000, fields={"温度": 90}),
        ], now=T + 31_000)
        self.assertEqual((report["accepted"], report["late_compensation"]), (2, 0))

    def test_event_after_window_sealed_is_compensation_even_within_grace(self):
        eng = self.engine()
        self._advance_and_seal(eng)
        sealed = eng.list_snapshots(DEV)[0]["window_end_ms"]
        # 事件时间比已封存边界只早 1ms，仍属于补偿
        report = eng.ingest([ev("x", (sealed - T) - 1, fields={"温度": 1})], now=T + 100_000)
        self.assertEqual(report["late_compensation"], 1)

    def test_compensation_does_not_corrupt_later_original_values(self):
        eng = self.engine()
        self._advance_and_seal(eng)
        # 补传修正 T+6s，但 T+30s 的原始读数 90 必须仍然是最终值
        eng.ingest([ev("e-late", 6_000, fields={"温度": 69.0})], now=T + 34_000)
        view = eng.replay(DEV, T + 40_000)
        self.assertEqual(view["fields"]["温度"]["value"], 90)
        self.assertEqual(view["fields"]["温度"]["source"], "original")

    def test_tampered_snapshot_is_detected_on_restart(self):
        eng = self.engine()
        self._advance_and_seal(eng)
        snap_path = eng.storage.snapshot_path(DEV, T + 10_000)
        with open(snap_path, encoding="utf-8") as fh:
            payload = json.load(fh)
        payload["state"]["fields"]["温度"] = 0.0
        with open(snap_path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
        with self.assertRaises(CorruptSnapshotError):
            self.engine()


class AlarmTests(EngineTestBase):
    def test_alarm_window_clear_and_replacement(self):
        eng = self.engine()
        eng.ingest([
            ev("a-on", 1_000, event_type="alarm", alarm={"code": "E103", "active": True}),
            ev("a-off", 9_000, event_type="alarm", alarm={"code": "E103", "active": False}),
            ev("b-on", 12_000, event_type="alarm", alarm={"code": "E201", "active": True}),
            ev("swap", 60_000, event_type="replacement",
               new_serial="SN-9002", new_model_version="v3"),
            ev("after", 65_000, model_version="v3", fields={"温度": 55}),
        ], now=T + 70_000)

        view = eng.replay(DEV, T + 50_000)
        window = next(w for w in view["alarm_windows"] if w["code"] == "E103")
        self.assertEqual(window["end_reason"], "cleared")
        window2 = next(w for w in view["alarm_windows"] if w["code"] == "E201")
        self.assertIsNone(window2["end_ms"], "未解除的报警应显示为持续中")

        after = eng.replay(DEV, T + 66_000)
        self.assertEqual(after["unit_serial"], "SN-9002")
        self.assertEqual(after["model_version"], "v3")
        self.assertEqual(set(after["fields"]), {"温度"})
        self.assertEqual(after["fields"]["温度"]["value"], 55)
        closed = [w for w in after["alarm_windows"] if w["code"] == "E201"]
        self.assertEqual(closed[-1]["end_reason"], "replacement")
        self.assertEqual([h["unit_serial"] for h in after["unit_history"]][-1], "SN-9002")


class ModelVersionTests(EngineTestBase):
    def test_field_carries_model_version_at_sample_time(self):
        eng = self.engine()
        eng.ingest([
            ev("v1", 0, model_version="v1", fields={"温度": 70}),
            ev("v2", 5_000, model_version="v2", fields={"温度": 71}),
        ], now=T + 6_000)
        view = eng.replay(DEV, T + 5_500)
        self.assertEqual(view["fields"]["温度"]["model_version"], "v2")
        early = eng.replay(DEV, T + 1_000)
        self.assertEqual(early["fields"]["温度"]["model_version"], "v1")


class RestartTests(EngineTestBase):
    def test_state_survives_restart(self):
        eng = self.engine()
        eng.ingest([
            ev("e1", 0, fields={"温度": 70}),
            ev("e2", 30_000, fields={"温度": 90, "负载": 85}),
        ], now=T + 32_000)
        view_before = eng.replay(DEV, T + 31_000)

        eng2 = self.engine()
        view_after = eng2.replay(DEV, T + 31_000)
        self.assertEqual(json.dumps(view_before, sort_keys=True, ensure_ascii=False),
                         json.dumps(view_after, sort_keys=True, ensure_ascii=False))

    def test_interrupted_replay_task_is_reclaimed(self):
        eng = self.engine()
        eng.ingest([ev("e1", 0, fields={"温度": 70})], now=T + 1_000)
        t1 = eng.create_replay(DEV, T + 1_000, label="完成的", now=T + 2_000)
        eng.run_task(t1["id"])
        t2 = eng.create_replay(DEV, T + 1_000, label="中断的", now=T + 3_000)
        # 模拟进程在 running 状态崩溃
        with JsonlLog(eng.storage.replay_path) as log:
            log.append(dict(eng.get_task(t2["id"]), status="running", attempts=1))

        eng2 = self.engine()
        tasks = {t["label"]: t for t in eng2.list_tasks()}
        self.assertEqual(tasks["完成的"]["status"], "completed")
        self.assertEqual(tasks["中断的"]["status"], "pending")
        self.assertIn("重新认领", tasks["中断的"]["error"])
        done = eng2.run_pending()
        self.assertEqual([t["label"] for t in done], ["中断的"])
        self.assertEqual(done[0]["status"], "completed")
        self.assertEqual(done[0]["result"]["fields"]["温度"]["value"], 70)


class ServiceSmokeTests(EngineTestBase):
    """通过真实 HTTP 端口跑一遍主要接口。"""

    def test_http_roundtrip(self):
        import threading
        import urllib.request
        from http.server import ThreadingHTTPServer
        from twin_replay.service import ApiHandler, AppState

        eng = self.engine()
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), ApiHandler)
        httpd.app = AppState(eng, self.dir)
        port = httpd.server_address[1]
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            def call(method, path, body=None):
                data = json.dumps(body).encode() if body is not None else None
                req = urllib.request.Request(
                    f"http://127.0.0.1:{port}{path}", data=data, method=method,
                    headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=5) as resp:
                    return resp.status, json.loads(resp.read())

            code, report = call("POST", "/events", {"events": [
                ev("e1", 0, fields={"温度": 70}),
                "坏行",
            ]})
            self.assertEqual(code, 200)
            self.assertEqual(report["accepted"], 1)
            self.assertEqual(report["rejected"], 1)

            _, devices = call("GET", "/devices")
            self.assertEqual(devices["devices"][0]["device_id"], DEV)

            _, status = call("GET", f"/devices/{DEV}/status")
            self.assertEqual(status["fields"]["温度"]["value"], 70)

            _, task = call("POST", "/replays",
                           {"device_id": DEV, "as_of_ms": T + 100, "label": "冒烟"})
            self.assertEqual(task["status"], "pending")
            _, run = call("POST", f"/replays/{task['id']}/run")
            self.assertEqual(run["status"], "completed")
        finally:
            httpd.shutdown()
            httpd.server_close()


if __name__ == "__main__":
    unittest.main()
