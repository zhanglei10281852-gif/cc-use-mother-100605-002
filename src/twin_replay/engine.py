"""设备状态回放引擎。

职责：
* 接收事件、幂等去重、按宽限水位线封存哈希链式快照；
* 迟到（越过水位线）数据进入独立补偿日志，不触碰已确认快照；
* 任意时间点重建设备视图：从最近检查点出发，把原始事件与补偿事件
  按事件时间合并折叠，并逐字段标注 original / compensation；
* 设备更换（新纪元）与模型版本切换的折叠；
* 持久化重放任务，崩溃后重新认领未完成任务。
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import time
import uuid
from typing import Any, Optional

from .model import COMPENSATION, ORIGINAL, Event, EventParseError, iso, parse_event
from .storage import JsonlLog, Storage

DEFAULT_GRID_MS = 60_000
DEFAULT_GRACE_MS = 5_000


class CorruptSnapshotError(RuntimeError):
    """快照内容被篡改或损坏。"""


def now_ms() -> int:
    return int(time.time() * 1000)


def _digest(payload: dict) -> str:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _empty_state() -> dict:
    return {
        "fields": {},
        "field_meta": {},
        "unit_serial": "",
        "unit_serial_meta": None,
        "model_version": "",
        "unit_history": [],
        "active_alarms": {},
        "alarm_windows": [],
        "last_event_ms": None,
    }


class Fold:
    """把事件按时间顺序折叠进设备状态，同时记录每个现值的来源标签。"""

    def __init__(self, state: dict):
        self.state = state
        self.provenance: dict[str, str] = {}
        # 检查点里的值都是已确认的原始数据
        for name in state["fields"]:
            self.provenance[f"field:{name}"] = ORIGINAL
        if state.get("unit_serial"):
            self.provenance["unit_serial"] = ORIGINAL
        for code in state["active_alarms"]:
            self.provenance[f"alarm:{code}"] = ORIGINAL

    def apply(self, rec: dict, tag: str) -> None:
        st = self.state
        kind = rec["kind"]
        t = rec["t_ms"]
        st["last_event_ms"] = t

        if kind == "telemetry":
            if rec.get("model_version"):
                st["model_version"] = rec["model_version"]
            for name, value in rec["fields"].items():
                st["fields"][name] = value
                st["field_meta"][name] = {
                    "model_version": rec.get("model_version") or st.get("model_version", ""),
                    "seq": rec.get("seq", 0),
                    "updated_ms": t,
                    "received_ms": rec.get("received_ms", t),
                }
                self.provenance[f"field:{name}"] = tag

        elif kind == "alarm":
            code = rec["alarm_code"]
            active = rec["alarm_active"]
            if active:
                if code not in st["active_alarms"]:
                    st["active_alarms"][code] = {"start_ms": t, "start_source": tag}
            elif code in st["active_alarms"]:
                entry = st["active_alarms"].pop(code)
                st["alarm_windows"].append({
                    "code": code,
                    "start_ms": entry["start_ms"],
                    "start_source": entry.get("start_source", ORIGINAL),
                    "end_ms": t,
                    "end_source": tag,
                    "end_reason": "cleared",
                })
            self.provenance[f"alarm:{code}"] = tag

        elif kind == "replacement":
            # 更换物理机：关闭全部在途报警，清空现场状态，开启新纪元
            for code, entry in list(st["active_alarms"].items()):
                st["alarm_windows"].append({
                    "code": code,
                    "start_ms": entry["start_ms"],
                    "start_source": entry.get("start_source", ORIGINAL),
                    "end_ms": t,
                    "end_source": tag,
                    "end_reason": "replacement",
                })
            st["active_alarms"].clear()
            st["fields"].clear()
            st["field_meta"].clear()
            serial = rec["new_serial"]
            model = rec.get("new_model_version") or rec.get("model_version") or ""
            st["unit_serial"] = serial
            st["model_version"] = model
            st["unit_serial_meta"] = {
                "since_ms": t, "source_event": rec["key"],
                "received_ms": rec.get("received_ms", t),
            }
            st["unit_history"].append({
                "unit_serial": serial, "model_version": model,
                "since_ms": t, "source": tag,
            })
            self.provenance["unit_serial"] = tag
            for name in list(self.provenance):
                if name.startswith("field:") or name.startswith("alarm:"):
                    self.provenance.pop(name)


class DeviceRuntime:
    def __init__(self, device_id: str, grid_ms: int, grace_ms: int):
        self.device_id = device_id
        self.grid_ms = grid_ms
        self.grace_ms = grace_ms
        self.state = _empty_state()
        self.buffer: dict[str, dict] = {}      # 已接受但尚未封存的事件
        self.watermark_ms: Optional[int] = None
        self.sealed_until_ms: Optional[int] = None
        self.max_event_ms: Optional[int] = None

    def observed(self, t_ms: int) -> None:
        self.max_event_ms = t_ms if self.max_event_ms is None else max(self.max_event_ms, t_ms)
        candidate = t_ms - self.grace_ms
        self.watermark_ms = candidate if self.watermark_ms is None else max(self.watermark_ms, candidate)


class ReplayEngine:
    def __init__(self, root: str, grid_ms: int = DEFAULT_GRID_MS, grace_ms: int = DEFAULT_GRACE_MS):
        self.storage = Storage(root)
        self.grid_ms = grid_ms
        self.grace_ms = grace_ms
        self.devices: dict[str, DeviceRuntime] = {}
        self.seen: dict[str, dict] = {}         # 幂等键 -> 已存记录
        self.originals: dict[str, list[dict]] = {}  # 设备 -> 全部已接受原始事件
        self.compensation: dict[str, list[dict]] = {}
        self.snapshot_index: dict[str, list[int]] = {}
        self._snapshot_cache: dict[str, dict] = {}
        self.replay_tasks: dict[str, dict] = {}
        self.recovery_warnings: list[str] = []
        self._ingest_seq = 0
        self._load()

    # ---------------------------------------------------------------- 启动恢复
    def _load(self) -> None:
        # 1) 快照清单 + 内容校验 + 链式校验
        for path in self.storage.list_snapshots():
            payload = self._read_snapshot(path)
            self.snapshot_index.setdefault(payload["device_id"], []).append(
                payload["window_end_ms"])
        for device_id, boundaries in self.snapshot_index.items():
            boundaries.sort()
            prev_digest = ""
            for b in boundaries:
                payload = self._read_snapshot(self.storage.snapshot_path(device_id, b))
                if payload["prev_digest"] != prev_digest:
                    raise CorruptSnapshotError(f"设备 {device_id} 快照 {b} 前驱哈希断链")
                prev_digest = payload["digest"]
            latest = boundaries[-1]
            payload = self._read_snapshot(self.storage.snapshot_path(device_id, latest))
            runtime = self._rt(device_id)
            runtime.state = copy.deepcopy(payload["state"])
            runtime.sealed_until_ms = latest

        # 2) 原始事件日志（全部保留；缓冲只装检查点之后的部分）
        for rec in JsonlLog.read(self.storage.events_path):
            self.seen[rec["key"]] = rec
            self._ingest_seq = max(self._ingest_seq, rec.get("ingest_seq", 0))
            rt = self._rt(rec["device_id"])
            rt.observed(rec["t_ms"])
            self.originals.setdefault(rec["device_id"], []).append(rec)
            if rt.sealed_until_ms is None or rec["t_ms"] >= rt.sealed_until_ms:
                rt.buffer[rec["key"]] = rec

        # 3) 补偿日志
        for rec in JsonlLog.read(self.storage.comp_path):
            self.seen[rec["key"]] = rec
            self.compensation.setdefault(rec["device_id"], []).append(rec)

        # 4) 重放任务（running 是崩溃残留，重新认领）
        for rec in JsonlLog.read(self.storage.replay_path):
            if rec.get("status") == "running":
                rec = dict(rec, status="pending", error="进程中断后重新认领")
            self.replay_tasks[rec["id"]] = rec

    def _read_snapshot(self, path: str) -> dict:
        if path not in self._snapshot_cache:
            payload = self.storage.read_json(path)
            body = {k: v for k, v in payload.items() if k != "digest"}
            if _digest(body) != payload.get("digest"):
                raise CorruptSnapshotError(f"快照校验失败（疑似被改写）: {path}")
            self._snapshot_cache[path] = payload
        return self._snapshot_cache[path]

    # ---------------------------------------------------------------- 写入路径
    def _rt(self, device_id: str) -> DeviceRuntime:
        if device_id not in self.devices:
            self.devices[device_id] = DeviceRuntime(device_id, self.grid_ms, self.grace_ms)
        return self.devices[device_id]

    def ingest(self, records: list, source: str = "http", now: Optional[int] = None) -> dict:
        """批量摄入。单条失败进入死信，不影响同批其它记录。

        迟到判定以批次开始前的封存边界为准：同一批中同时到达、目标窗口
        尚未封存的事件，无论在批次中的先后顺序，都按原始数据处理；
        封存统一在批次末尾进行。
        """
        received = now or now_ms()
        accepted = duplicate = conflict = late = rejected = 0
        # 批次前快照：device_id -> 已封存边界（缺失即尚未有任何快照）
        late_boundary = {d: rt.sealed_until_ms for d, rt in self.devices.items()}
        touched: set[str] = set()
        with JsonlLog(self.storage.events_path) as events_log, \
                JsonlLog(self.storage.comp_path) as comp_log, \
                JsonlLog(self.storage.deadletter_path) as dead_log:
            for row in records:
                try:
                    event = parse_event(row, received)
                except EventParseError as exc:
                    dead_log.append({"received_ms": received, "source": source,
                                     "raw": _safe(row), "reason": str(exc)})
                    rejected += 1
                    continue
                prior = self.seen.get(event.key)
                if prior is not None:
                    if prior.get("content_hash") == event.content_hash:
                        duplicate += 1
                        continue
                    dead_log.append({"received_ms": received, "source": source,
                                     "raw": event.as_storage(source, 0),
                                     "reason": f"幂等键 {event.key} 内容冲突，拒绝改写"})
                    conflict += 1
                    continue

                rt = self._rt(event.device_id)
                threshold = late_boundary.get(event.device_id)
                is_late = threshold is not None and event.t_ms < threshold
                self._ingest_seq += 1
                record = event.as_storage(source, self._ingest_seq)
                self.seen[event.key] = record

                if is_late:
                    comp_log.append(record)
                    self.compensation.setdefault(event.device_id, []).append(record)
                    late += 1
                    continue

                events_log.append(record)
                self.originals.setdefault(event.device_id, []).append(record)
                rt.buffer[event.key] = record
                rt.observed(event.t_ms)
                touched.add(event.device_id)
                accepted += 1

            # 全部记录落盘后，按设备统一封存到批后水位线
            for device_id in touched:
                self._seal_due(self.devices[device_id])
        return {"received": len(records), "accepted": accepted, "duplicate": duplicate,
                "conflict": conflict, "late_compensation": late, "rejected": rejected}

    def _seal_due(self, rt: DeviceRuntime) -> None:
        # 窗口约定：网格窗口 [B, B+grid) 在水位线到达 B+grid 时封存，
        # 快照边界 B 覆盖事件时间严格小于 B 的全部事件。
        if not rt.buffer or rt.watermark_ms is None:
            return
        # 防御：哪怕水位线异常超前，也不封存晚于最新缓冲事件窗口的空区间
        max_t = max(r["t_ms"] for r in rt.buffer.values())
        cap_boundary = ((max_t // self.grid_ms) + 1) * self.grid_ms
        if rt.sealed_until_ms is None:
            min_t = min(r["t_ms"] for r in rt.buffer.values())
            boundary = ((min_t // self.grid_ms) + 1) * self.grid_ms
        else:
            boundary = rt.sealed_until_ms + self.grid_ms
        while rt.watermark_ms >= boundary and boundary <= cap_boundary:
            self._seal(rt, boundary)
            boundary += self.grid_ms

    def _seal(self, rt: DeviceRuntime, boundary: int) -> None:
        prev_boundary = rt.sealed_until_ms
        due = sorted(
            (r for r in rt.buffer.values() if r["t_ms"] < boundary),
            key=lambda r: (r["t_ms"], r.get("ingest_seq", 0)),
        )

        if prev_boundary is not None:
            sealed_state = copy.deepcopy(
                self._read_snapshot(self.storage.snapshot_path(rt.device_id, prev_boundary))["state"])
            prior_windows = sealed_state["alarm_windows"]
            prev_digest = self._read_snapshot(
                self.storage.snapshot_path(rt.device_id, prev_boundary))["digest"]
        else:
            sealed_state = _empty_state()
            prior_windows = []
            prev_digest = ""

        folder = Fold(sealed_state)
        applied: list[str] = []
        for rec in due:
            folder.apply(rec, ORIGINAL)
            applied.append(rec["key"])
        interval_windows = sealed_state["alarm_windows"][len(prior_windows):]

        window_start = min((r["t_ms"] for r in due), default=boundary - self.grid_ms)
        payload = {
            "device_id": rt.device_id,
            "grid_ms": rt.grid_ms,
            "window_start_ms": window_start,
            "window_end_ms": boundary,
            "state": sealed_state,
            "alarm_windows": interval_windows,
            "events_applied": applied,
            "prev_digest": prev_digest,
            "sealed_ms": now_ms(),
        }
        payload["digest"] = _digest(payload)
        path = self.storage.snapshot_path(rt.device_id, boundary)
        self.storage.write_json_atomic(path, payload)
        self._snapshot_cache[path] = payload
        self.snapshot_index.setdefault(rt.device_id, []).append(boundary)
        self.snapshot_index[rt.device_id].sort()

        rt.state = sealed_state
        for key in applied:
            rt.buffer.pop(key, None)
        rt.sealed_until_ms = boundary

    # ---------------------------------------------------------------- 查询/回放
    def list_devices(self) -> list[dict]:
        ids = set(self.devices) | set(self.compensation) | set(self.snapshot_index)
        out = []
        for device_id in sorted(ids):
            rt = self.devices.get(device_id)
            out.append({
                "device_id": device_id,
                "watermark_ms": rt.watermark_ms if rt else None,
                "watermark": iso(rt.watermark_ms) if rt and rt.watermark_ms is not None else None,
                "sealed_until_ms": rt.sealed_until_ms if rt else None,
                "snapshots": len(self.snapshot_index.get(device_id, [])),
                "buffered": len(rt.buffer) if rt else 0,
                "compensation_events": len(self.compensation.get(device_id, [])),
            })
        return out

    def replay(self, device_id: str, as_of_ms: int) -> dict:
        """按标准时间 as_of_ms 重建设备视图。

        存在补偿事件时，选择早于最早补偿时间的最近快照作为检查点，
        之后把原始事件与补偿事件按事件时间合并折叠，保证时序正确；
        已确认快照本身在任何情况下都不会被改写。
        """
        comps = sorted(
            (r for r in self.compensation.get(device_id, []) if r["t_ms"] <= as_of_ms),
            key=lambda r: (r["t_ms"], r.get("ingest_seq", 0)),
        )
        boundaries = self.snapshot_index.get(device_id, [])
        if comps:
            first_comp_t = comps[0]["t_ms"]
            # 快照边界 B 覆盖 t < B；只有 B <= 首个补偿时间，该检查点才不含补偿窗口
            eligible = [b for b in boundaries if b <= first_comp_t and b <= as_of_ms]
        else:
            eligible = [b for b in boundaries if b <= as_of_ms]

        if eligible:
            base_boundary = eligible[-1]
            snap = self._read_snapshot(self.storage.snapshot_path(device_id, base_boundary))
            state = copy.deepcopy(snap["state"])
        else:
            base_boundary = None
            state = _empty_state()

        pending_originals = [
            r for r in self.originals.get(device_id, [])
            if r["t_ms"] <= as_of_ms and (base_boundary is None or r["t_ms"] >= base_boundary)
        ]
        # 检查点已特意回退到早于最早补偿时间的位置，原始与补偿事件在此一并合并折叠
        merged: list[tuple[dict, str]] = [(r, ORIGINAL) for r in pending_originals]
        merged.extend((r, COMPENSATION) for r in comps)
        merged.sort(key=lambda item: (item[0]["t_ms"], item[0].get("ingest_seq", 0),
                                      0 if item[1] == ORIGINAL else 1))
        folder = Fold(state)
        touched_comp: set[str] = set()
        for rec, tag in merged:
            folder.apply(rec, tag)
            if tag == COMPENSATION:
                if rec["kind"] == "telemetry":
                    touched_comp.update(rec["fields"])
                elif rec["kind"] == "alarm":
                    touched_comp.add(f"alarm:{rec['alarm_code']}")
                elif rec["kind"] == "replacement":
                    touched_comp.add("unit_serial")

        windows = copy.deepcopy(state["alarm_windows"])
        for code, entry in sorted(state["active_alarms"].items()):
            if entry["start_ms"] <= as_of_ms:
                windows.append({
                    "code": code,
                    "start_ms": entry["start_ms"],
                    "start_source": entry.get("start_source", ORIGINAL),
                    "end_ms": None,
                    "end_source": None,
                    "end_reason": None,
                })
        windows.sort(key=lambda w: (w["start_ms"], w["code"] or ""))

        fields_out = {}
        for name, value in sorted(state["fields"].items()):
            meta = state["field_meta"].get(name, {})
            fields_out[name] = {
                "value": value,
                "source": folder.provenance.get(f"field:{name}", ORIGINAL),
                "updated_ms": meta.get("updated_ms"),
                "updated_at": iso(meta["updated_ms"]) if meta.get("updated_ms") is not None else None,
                "model_version": meta.get("model_version", ""),
                "seq": meta.get("seq"),
                "received_ms": meta.get("received_ms"),
            }

        marked_fields = sorted(n for n, f in fields_out.items() if f["source"] == COMPENSATION)
        active_codes = [w["code"] for w in windows if w["end_ms"] is None]
        return {
            "device_id": device_id,
            "as_of_ms": as_of_ms,
            "as_of": iso(as_of_ms),
            "base_snapshot_ms": base_boundary,
            "unit_serial": state["unit_serial"] or None,
            "unit_serial_source": folder.provenance.get("unit_serial") if state["unit_serial"] else None,
            "model_version": state["model_version"] or None,
            "fields": fields_out,
            "active_alarms": [
                {"code": c, "source": folder.provenance.get(f"alarm:{c}", ORIGINAL)}
                for c in active_codes
            ],
            "alarm_windows": windows,
            "unit_history": state.get("unit_history", []),
            "compensation": {
                "applied_events": len(comps),
                "latest_event_ms": max((r["t_ms"] for r in comps), default=None),
                "marked_fields": marked_fields,
                "touched_fields": sorted(touched_comp),
            },
        }

    def status(self, device_id: str, as_of_ms: Optional[int] = None) -> dict:
        if as_of_ms is None:
            as_of_ms = now_ms()
            rt = self.devices.get(device_id)
            if rt and rt.max_event_ms is not None:
                as_of_ms = max(as_of_ms, rt.max_event_ms)
            for rec in self.compensation.get(device_id, []):
                as_of_ms = max(as_of_ms, rec["t_ms"])
        return self.replay(device_id, as_of_ms)

    def list_snapshots(self, device_id: str) -> list[dict]:
        return [
            {"window_end_ms": b,
             "window_end": iso(b),
             "path": os.path.relpath(self.storage.snapshot_path(device_id, b), self.storage.root)}
            for b in self.snapshot_index.get(device_id, [])
        ]

    def deadletters(self) -> list[dict]:
        return list(JsonlLog.read(self.storage.deadletter_path))

    # ---------------------------------------------------------------- 重放任务
    def create_replay(self, device_id: str, as_of_ms: int, label: str = "",
                      now: Optional[int] = None) -> dict:
        stamp = now or now_ms()
        task = {
            "id": uuid.uuid4().hex[:12],
            "device_id": device_id,
            "as_of_ms": as_of_ms,
            "label": label,
            "status": "pending",
            "attempts": 0,
            "result": None,
            "error": None,
            "created_ms": stamp,
            "updated_ms": stamp,
        }
        self._append_task(task)
        return task

    def _append_task(self, task: dict) -> None:
        with JsonlLog(self.storage.replay_path) as log:
            log.append(task)
        self.replay_tasks[task["id"]] = task

    def run_pending(self, limit: Optional[int] = None) -> list[dict]:
        pending = sorted(
            (t for t in self.replay_tasks.values() if t["status"] == "pending"),
            key=lambda t: t["created_ms"])
        return [self.run_task(t["id"]) for t in (pending[:limit] if limit is not None else pending)]

    def run_task(self, task_id: str) -> dict:
        task = self.replay_tasks[task_id]
        running = dict(task, status="running", attempts=task["attempts"] + 1,
                       error=None, updated_ms=now_ms())
        self._append_task(running)
        try:
            result = self.replay(running["device_id"], running["as_of_ms"])
            finished = dict(running, status="completed", result=result, updated_ms=now_ms())
        except Exception as exc:  # 任务失败保留错误，可再次运行
            finished = dict(running, status="failed",
                            error=f"{type(exc).__name__}: {exc}", updated_ms=now_ms())
        self._append_task(finished)
        return finished

    def list_tasks(self) -> list[dict]:
        return [self.replay_tasks[k]
                for k in sorted(self.replay_tasks, key=lambda i: self.replay_tasks[i]["created_ms"])]

    def get_task(self, task_id: str) -> dict:
        return self.replay_tasks[task_id]


def _safe(row: Any) -> Any:
    try:
        json.dumps(row, ensure_ascii=False)
        return row
    except (TypeError, ValueError):
        return repr(row)
