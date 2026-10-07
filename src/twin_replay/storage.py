"""SQLite 持久化层。

只负责存取与幂等去重，不含业务判定。关键表：

- ``events``：校正后的不可变事件日志（``event_id`` 唯一，重复上报被拒绝）；
- ``confirmations``：每设备的封存水位线；
- ``sealed_snapshots``：封存时刻的视图原文与指纹，永不再改写；
- ``anomaly_windows``：封存时固化的异常窗口（sealed=1）；
- ``batches``：每批次的接收/隔离统计与坏行明细；
- ``replay_jobs``：可断点续跑的重放任务，``progress_seq`` 记录处理进度；
- ``thresholds``：每设备每指标的正常区间。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any, Optional

from .models import Event

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  device_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  corrected_ts REAL NOT NULL,
  device_time TEXT NOT NULL,
  clock_offset REAL NOT NULL DEFAULT 0,
  fields_json TEXT NOT NULL,
  unit_sn TEXT NOT NULL DEFAULT '',
  model_version TEXT NOT NULL DEFAULT '',
  batch_id TEXT NOT NULL DEFAULT '',
  checksum TEXT,
  is_compensation INTEGER NOT NULL DEFAULT 0,
  received_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_events_dev_ts ON events(device_id, corrected_ts, seq);

CREATE TABLE IF NOT EXISTS confirmations (
  device_id TEXT PRIMARY KEY,
  up_to_ts REAL NOT NULL,
  confirmed_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS sealed_snapshots (
  device_id TEXT NOT NULL,
  as_of REAL NOT NULL,
  digest TEXT NOT NULL,
  body_json TEXT NOT NULL,
  sealed_at REAL NOT NULL,
  PRIMARY KEY (device_id, as_of)
);

CREATE TABLE IF NOT EXISTS anomaly_windows (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  device_id TEXT NOT NULL,
  metric TEXT NOT NULL,
  start_ts REAL NOT NULL,
  start_event_id TEXT NOT NULL,
  end_ts REAL,
  end_event_id TEXT,
  peak REAL,
  sealed INTEGER NOT NULL DEFAULT 0,
  compensated INTEGER NOT NULL DEFAULT 0,
  seal_as_of REAL NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS ix_anom_dev ON anomaly_windows(device_id, metric);

CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY,
  received_at REAL NOT NULL,
  accepted_count INTEGER NOT NULL,
  corrupted_count INTEGER NOT NULL,
  duplicates_count INTEGER NOT NULL DEFAULT 0,
  corrupted_json TEXT NOT NULL,
  duplicates_json TEXT NOT NULL DEFAULT '[]'
);

CREATE TABLE IF NOT EXISTS replay_jobs (
  job_id TEXT PRIMARY KEY,
  device_id TEXT NOT NULL,
  as_of_ts REAL,
  created_at REAL NOT NULL,
  updated_at REAL NOT NULL,
  status TEXT NOT NULL,
  progress_pos INTEGER NOT NULL DEFAULT 0,
  total_events INTEGER NOT NULL DEFAULT 0,
  event_seqs_json TEXT NOT NULL DEFAULT '[]',
  params_json TEXT NOT NULL DEFAULT '{}',
  checkpoint_json TEXT,
  result_json TEXT,
  error TEXT
);
CREATE INDEX IF NOT EXISTS ix_jobs_status ON replay_jobs(status);

CREATE TABLE IF NOT EXISTS thresholds (
  device_id TEXT NOT NULL,
  metric TEXT NOT NULL,
  min_value REAL,
  max_value REAL,
  PRIMARY KEY (device_id, metric)
);
"""


class Storage:
    def __init__(self, path: str = ":memory:"):
        self.path = path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        with self._lock:
            self.conn.commit()
            self.conn.close()

    # ---- 事件 ----------------------------------------------------------

    def insert_event(self, parsed: Any, *, force_compensation: bool = False,
                     received_at: Optional[float] = None) -> Optional[Event]:
        """插入一条已解析事件。event_id 重复时返回 None（幂等去重）。"""
        received_at = time.time() if received_at is None else received_at
        is_comp = 1 if (parsed.is_compensation or force_compensation) else 0
        row_data = (
            parsed.event_id, parsed.device_id, parsed.kind, parsed.corrected_ts,
            parsed.device_time, parsed.clock_offset,
            json.dumps(parsed.metrics, ensure_ascii=False, default=str),
            parsed.unit_sn, parsed.model_version, parsed.batch_id,
            parsed.checksum, is_comp, received_at,
        )
        with self._lock:
            try:
                cur = self.conn.execute(
                    """INSERT INTO events(event_id, device_id, kind, corrected_ts, device_time,
                       clock_offset, fields_json, unit_sn, model_version, batch_id, checksum,
                       is_compensation, received_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    row_data,
                )
            except sqlite3.IntegrityError:
                return None
            self.conn.commit()
            return self.get_event_by_seq(cur.lastrowid)

    def get_event_by_seq(self, seq: int) -> Optional[Event]:
        with self._lock:
            row = self.conn.execute("SELECT * FROM events WHERE seq=?", (seq,)).fetchone()
        return Event.from_row(dict(row)) if row else None

    def existing_event_ids(self, ids: list[str]) -> set[str]:
        if not ids:
            return set()
        placeholders = ",".join("?" * len(ids))
        with self._lock:
            rows = self.conn.execute(
                f"SELECT event_id FROM events WHERE event_id IN ({placeholders})", ids
            ).fetchall()
        return {r["event_id"] for r in rows}

    def events_for_device(self, device_id: str) -> list[Event]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM events WHERE device_id=? ORDER BY corrected_ts, seq",
                (device_id,),
            ).fetchall()
        return [Event.from_row(dict(r)) for r in rows]

    def list_devices(self) -> list[str]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT DISTINCT device_id FROM events ORDER BY device_id"
            ).fetchall()
        return [r["device_id"] for r in rows]

    def max_event_seq(self) -> int:
        with self._lock:
            row = self.conn.execute("SELECT COALESCE(MAX(seq),0) AS m FROM events").fetchone()
        return int(row["m"])

    # ---- 确认与封存 -----------------------------------------------------

    def get_confirmation(self, device_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM confirmations WHERE device_id=?", (device_id,)
            ).fetchone()
        return dict(row) if row else None

    def set_confirmation(self, device_id: str, up_to_ts: float,
                         confirmed_at: Optional[float] = None) -> None:
        confirmed_at = time.time() if confirmed_at is None else confirmed_at
        with self._lock:
            self.conn.execute(
                """INSERT INTO confirmations(device_id, up_to_ts, confirmed_at)
                   VALUES (?,?,?)
                   ON CONFLICT(device_id) DO UPDATE SET
                     up_to_ts=excluded.up_to_ts, confirmed_at=excluded.confirmed_at""",
                (device_id, up_to_ts, confirmed_at),
            )
            self.conn.commit()

    def save_sealed_snapshot(self, device_id: str, as_of: float, digest: str,
                             body: dict[str, Any], sealed_at: Optional[float] = None) -> None:
        sealed_at = time.time() if sealed_at is None else sealed_at
        with self._lock:
            self.conn.execute(
                """INSERT INTO sealed_snapshots(device_id, as_of, digest, body_json, sealed_at)
                   VALUES (?,?,?,?,?)
                   ON CONFLICT(device_id, as_of) DO NOTHING""",
                (device_id, as_of, digest,
                 json.dumps(body, ensure_ascii=False, default=str), sealed_at),
            )
            self.conn.commit()

    def get_sealed_snapshot(self, device_id: str, as_of: float) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM sealed_snapshots WHERE device_id=? AND ABS(as_of-?)<1e-6",
                (device_id, as_of),
            ).fetchone()
        if not row:
            return None
        data = dict(row)
        data["body"] = json.loads(data.pop("body_json"))
        return data

    def list_sealed_points(self, device_id: str) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT device_id, as_of, digest, sealed_at FROM sealed_snapshots"
                " WHERE device_id=? ORDER BY as_of",
                (device_id,),
            ).fetchall()
        return [dict(r) for r in rows]

    # ---- 异常窗口 -------------------------------------------------------

    def replace_sealed_windows(self, device_id: str, windows: list[Any],
                               seal_as_of: float) -> None:
        """封存时固化窗口：同一封存点的窗口行不可变，只补入新行。"""
        with self._lock:
            for w in windows:
                exists = self.conn.execute(
                    "SELECT 1 FROM anomaly_windows WHERE device_id=? AND metric=?"
                    " AND ABS(start_ts-?)<1e-6 AND ABS(seal_as_of-?)<1e-6 AND sealed=1",
                    (device_id, w.metric, w.start_ts, seal_as_of),
                ).fetchone()
                if exists:
                    continue
                # 开放窗口在封存点被“冻结”：不写 end_ts，保留其开放形态。
                is_open_frozen = w.end_ts is None or w.end_ts > seal_as_of + 1e-6
                end_ts = None if is_open_frozen else w.end_ts
                end_event_id = None if is_open_frozen else w.end_event_id
                self.conn.execute(
                    """INSERT INTO anomaly_windows(device_id, metric, start_ts, start_event_id,
                       end_ts, end_event_id, peak, sealed, compensated, seal_as_of)
                       VALUES (?,?,?,?,?,?,?,1,?,?)""",
                    (device_id, w.metric, w.start_ts, w.start_event_id, end_ts,
                     end_event_id, w.peak, 1 if w.compensated else 0, seal_as_of),
                )
            self.conn.commit()

    def sealed_windows(self, device_id: str, seal_as_of: Optional[float] = None) -> list[dict[str, Any]]:
        with self._lock:
            if seal_as_of is None:
                rows = self.conn.execute(
                    "SELECT * FROM anomaly_windows WHERE device_id=? AND sealed=1"
                    " ORDER BY seal_as_of, start_ts, metric",
                    (device_id,),
                ).fetchall()
            else:
                rows = self.conn.execute(
                    "SELECT * FROM anomaly_windows WHERE device_id=? AND sealed=1"
                    " AND ABS(seal_as_of-?)<1e-6 ORDER BY start_ts, metric",
                    (device_id, seal_as_of),
                ).fetchall()
        return [dict(r) for r in rows]

    # ---- 批次 -----------------------------------------------------------

    def save_batch(self, result_dict: dict[str, Any], received_at: Optional[float] = None) -> None:
        received_at = time.time() if received_at is None else received_at
        with self._lock:
            self.conn.execute(
                """INSERT INTO batches(batch_id, received_at, accepted_count, corrupted_count,
                   duplicates_count, corrupted_json, duplicates_json)
                   VALUES (?,?,?,?,?,?,?)
                   ON CONFLICT(batch_id) DO NOTHING""",
                (result_dict["batch_id"], received_at,
                 result_dict["accepted_count"], result_dict["corrupted_count"],
                 result_dict.get("duplicates_count", len(result_dict.get("duplicates", []))),
                 json.dumps(result_dict.get("corrupted", []), ensure_ascii=False, default=str),
                 json.dumps(result_dict.get("duplicates", []), ensure_ascii=False, default=str)),
            )
            self.conn.commit()

    def list_batches(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT batch_id, received_at, accepted_count, corrupted_count, duplicates_count"
                " FROM batches ORDER BY received_at"
            ).fetchall()
        return [dict(r) for r in rows]

    # ---- 重放任务 --------------------------------------------------------

    def create_job(self, job: dict[str, Any]) -> None:
        with self._lock:
            self.conn.execute(
                """INSERT INTO replay_jobs(job_id, device_id, as_of_ts, created_at, updated_at,
                   status, progress_pos, total_events, event_seqs_json, params_json)
                   VALUES (?,?,?,?,?,'pending',0,?,?,?)""",
                (job["job_id"], job["device_id"], job.get("as_of_ts"),
                 job["created_at"], job["created_at"], job.get("total_events", 0),
                 json.dumps(job.get("event_seqs", [])),
                 json.dumps(job.get("params", {}), ensure_ascii=False, default=str)),
            )
            self.conn.commit()

    def update_job(self, job_id: str, **fields: Any) -> None:
        if not fields:
            return
        fields["updated_at"] = time.time()
        cols = ", ".join(f"{k}=?" for k in fields)
        with self._lock:
            self.conn.execute(f"UPDATE replay_jobs SET {cols} WHERE job_id=?",
                              (*fields.values(), job_id))
            self.conn.commit()

    def get_job(self, job_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM replay_jobs WHERE job_id=?", (job_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_jobs(self, status: Optional[str] = None) -> list[dict[str, Any]]:
        with self._lock:
            if status:
                rows = self.conn.execute(
                    "SELECT * FROM replay_jobs WHERE status=? ORDER BY created_at", (status,)
                ).fetchall()
            else:
                rows = self.conn.execute(
                    "SELECT * FROM replay_jobs ORDER BY created_at").fetchall()
        return [dict(r) for r in rows]

    def unfinished_jobs(self) -> list[dict[str, Any]]:
        """pending 与崩溃时残留的 running 任务都可继续。"""
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM replay_jobs WHERE status IN ('pending','running')"
                " ORDER BY created_at"
            ).fetchall()
        return [dict(r) for r in rows]

    # ---- 阈值 ------------------------------------------------------------

    def set_threshold(self, device_id: str, metric: str,
                      min_value: Optional[float], max_value: Optional[float]) -> None:
        with self._lock:
            self.conn.execute(
                """INSERT INTO thresholds(device_id, metric, min_value, max_value)
                   VALUES (?,?,?,?)
                   ON CONFLICT(device_id, metric) DO UPDATE SET
                     min_value=excluded.min_value, max_value=excluded.max_value""",
                (device_id, metric, min_value, max_value),
            )
            self.conn.commit()

    def get_thresholds(self, device_id: str) -> dict[str, dict[str, Optional[float]]]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT metric, min_value, max_value FROM thresholds WHERE device_id=?",
                (device_id,),
            ).fetchall()
        return {r["metric"]: {"min": r["min_value"], "max": r["max_value"]} for r in rows}
