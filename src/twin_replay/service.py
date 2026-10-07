"""服务层：把持久化、折叠引擎和封存规则组装成对外能力。

核心规则汇总：

1. **重复上报**：``event_id`` 唯一约束去重。
2. **迟到事件**：设备已有封存水位线 ``up_to`` 时，校正时间不晚于水位线的新事件
   一律打上 ``is_compensation`` 标记进入补偿通道，不能进入确认视图。
3. **封存**：把水位线之前按时到达的事件折叠成快照，哈希入库；水位线只进不退。
4. **设备更换 / 模型切换**：折叠状态机里的硬/软纪元边界，状态不跨边界串值。
5. **回放**：任务冻结事件序列（seq 列表）与参数，带检查点，可跨进程继续。
"""
from __future__ import annotations

import json
import time
import uuid
from typing import Any, Optional

from .clock import Epoch, iso
from .engine import FoldState, fold_events
from .exceptions import Conflict, NotFound
from .models import (
    SRC_COMPENSATED,
    SRC_CONFIRMED,
    SRC_REALTIME,
    Event,
)
from .parsing import BatchResult, ParsedEvent, parse_batch
from .storage import Storage


class TwinService:
    def __init__(self, storage: Storage):
        self.storage = storage

    # ---- 摄入 -----------------------------------------------------------

    def ingest(self, parsed: ParsedEvent) -> Optional[Event]:
        """摄入一条已解析事件，返回落库事件；重复返回 None。"""
        confirmation = self.storage.get_confirmation(parsed.device_id)
        force_comp = False
        if confirmation is not None and parsed.corrected_ts <= confirmation["up_to_ts"]:
            # 迟到到已封存区间：只能作为补偿数据存在。
            force_comp = True
        return self.storage.insert_event(parsed, force_compensation=force_comp)

    def import_batch(self, payload: Any, batch_id: str = "") -> dict[str, Any]:
        """批量导入：坏行隔离、重复去重、好事件全部落库。"""
        result: BatchResult = parse_batch(payload, batch_id=batch_id)
        batch_id = result.batch_id or batch_id or f"auto-{uuid.uuid4().hex[:12]}"
        result.batch_id = batch_id
        for parsed in result.accepted:
            existing = self.storage.existing_event_ids([parsed.event_id])
            if existing:
                result.duplicates.append(parsed.event_id)
                continue
            self.ingest(parsed)
        report = result.to_dict()
        self.storage.save_batch(report)
        return report

    def list_devices(self) -> list[str]:
        return self.storage.list_devices()

    # ---- 阈值 -----------------------------------------------------------

    def set_threshold(self, device_id: str, metric: str,
                      min_value: Any = None, max_value: Any = None) -> None:
        self.storage.set_threshold(
            device_id, metric,
            None if min_value is None else float(min_value),
            None if max_value is None else float(max_value),
        )

    # ---- 事件选择 --------------------------------------------------------

    def _device_events(self, device_id: str) -> list[Event]:
        return self.storage.events_for_device(device_id)

    @staticmethod
    def _eligible(events: list[Event], as_of: float, include_compensation: bool) -> list[Event]:
        out = []
        for ev in events:
            if ev.corrected_ts > as_of:
                continue
            if not include_compensation and ev.is_compensation:
                continue
            out.append(ev)
        return out

    # ---- 封存确认 --------------------------------------------------------

    def confirm(self, device_id: str, as_of: Any) -> dict[str, Any]:
        """把某时间点之前的按时数据封存为不可变确认快照。"""
        from .clock import to_epoch
        as_of_ts = to_epoch(as_of)
        prior = self.storage.get_confirmation(device_id)
        if prior is not None:
            if as_of_ts < prior["up_to_ts"] - 1e-6:
                raise Conflict(
                    f"封存水位线只能前进：当前 {prior['up_to_ts']}，请求 {as_of_ts}")
            if abs(as_of_ts - prior["up_to_ts"]) <= 1e-6:
                sealed = self.storage.get_sealed_snapshot(device_id, as_of_ts)
                return sealed["body"]

        events = self._eligible(self._device_events(device_id), as_of_ts,
                                include_compensation=False)
        thresholds = self.storage.get_thresholds(device_id)
        state = fold_events(events, thresholds, as_of=as_of_ts)
        snap = state.snapshot(device_id, as_of_ts, confirmed_up_to=as_of_ts)
        # 确认视图中的字段来源统一标注为 confirmed。
        for cell in snap.fields.values():
            cell.source = SRC_CONFIRMED
        body = snap.to_dict()

        self.storage.save_sealed_snapshot(device_id, as_of_ts, snap.digest(), body)
        self.storage.replace_sealed_windows(device_id, state.closed_windows
                                            + list(state.open_windows.values()),
                                            seal_as_of=as_of_ts)
        self.storage.set_confirmation(device_id, as_of_ts)
        return body

    def verify_seal(self, device_id: str, as_of: Optional[Any] = None) -> dict[str, Any]:
        """用封存时的原文重算哈希，与库存指纹比对，返回验真结果。"""
        if as_of is None:
            points = self.storage.list_sealed_points(device_id)
            if not points:
                raise NotFound(f"设备 {device_id} 没有封存快照")
            point = points[-1]["as_of"]
        else:
            from .clock import to_epoch
            point = to_epoch(as_of)
        sealed = self.storage.get_sealed_snapshot(device_id, point)
        if sealed is None:
            raise NotFound(f"设备 {device_id} 在 {point} 没有封存快照")
        stored_body = sealed["body"]
        stored_digest = stored_body.pop("digest", None)
        from .models import canonical_digest
        recomputed = canonical_digest(stored_body)
        stored_body["digest"] = stored_digest
        return {
            "device_id": device_id,
            "as_of": stored_body.get("as_of"),
            "stored_digest": sealed["digest"],
            "recomputed_digest": recomputed,
            "body_embeds_digest": stored_digest,
            "intact": sealed["digest"] == recomputed and stored_digest == recomputed,
        }

    # ---- 视图重建 --------------------------------------------------------

    def view(self, device_id: str, as_of: Any, mode: str = "revised") -> dict[str, Any]:
        """按任意时间点重建设备视图。mode: revised | confirmed。"""
        from .clock import to_epoch
        as_of_ts = to_epoch(as_of)
        if mode not in ("revised", "confirmed"):
            raise ValueError("mode 只能是 revised 或 confirmed")
        all_events = self._device_events(device_id)
        if not all_events:
            raise NotFound(f"设备 {device_id} 没有任何事件")
        thresholds = self.storage.get_thresholds(device_id)
        confirmation = self.storage.get_confirmation(device_id)
        up_to = confirmation["up_to_ts"] if confirmation else None

        if mode == "confirmed":
            events = self._eligible(all_events, as_of_ts, include_compensation=False)
            state = fold_events(events, thresholds, as_of=as_of_ts)
            snap = state.snapshot(
                device_id, as_of_ts,
                confirmed_up_to=up_to if (up_to is not None and up_to <= as_of_ts) else None)
            # 确认视图排除补偿；逐格区分“已封存确认”与“水位线之后的按时数据”。
            for cell in snap.fields.values():
                cell.source = (SRC_CONFIRMED if up_to is not None and cell.ts <= up_to
                               else SRC_REALTIME)
            body = snap.to_dict()
            body["view_mode"] = "confirmed"
            body["seal"] = self._seal_reference(device_id, as_of_ts, up_to)
            return body

        # revised：补偿事件参与折叠，字段血缘逐格标注。
        events = self._eligible(all_events, as_of_ts, include_compensation=True)
        state = fold_events(events, thresholds, as_of=as_of_ts)
        snap = state.snapshot(
            device_id, as_of_ts,
            confirmed_up_to=up_to if (up_to is not None and up_to <= as_of_ts) else None)
        for metric, cell in snap.fields.items():
            if cell.source == SRC_COMPENSATED:
                continue
            if up_to is not None and cell.ts <= up_to:
                cell.source = SRC_CONFIRMED
            else:
                cell.source = SRC_REALTIME
        body = snap.to_dict()
        body["view_mode"] = "revised"
        body["seal"] = self._seal_reference(device_id, as_of_ts, up_to)
        body["compensation_events"] = sorted(
            {cell.event_id for cell in snap.fields.values()
             if cell.source == SRC_COMPENSATED})
        return body

    def _seal_reference(self, device_id: str, as_of_ts: float,
                        up_to: Optional[float]) -> Optional[dict[str, Any]]:
        if up_to is None:
            return None
        exact = self.storage.get_sealed_snapshot(device_id, up_to)
        return {
            "confirmed_up_to": iso(up_to),
            "query_within_sealed_range": as_of_ts <= up_to + 1e-6,
            "digest": exact["digest"] if exact else None,
        }

    def current_state(self, device_id: str) -> dict[str, Any]:
        """设备当前状态 = 以最新事件时间为 as_of 的修订视图。"""
        events = self._device_events(device_id)
        if not events:
            raise NotFound(f"设备 {device_id} 没有任何事件")
        return self.view(device_id, events[-1].corrected_ts, mode="revised")

    def sealed_windows(self, device_id: str) -> list[dict[str, Any]]:
        return self.storage.sealed_windows(device_id)

    # ---- 重放任务（可恢复）----------------------------------------------

    def create_replay(self, device_id: str, as_of: Any, mode: str = "revised",
                      job_id: Optional[str] = None) -> dict[str, Any]:
        from .clock import to_epoch
        as_of_ts = to_epoch(as_of)
        all_events = self._device_events(device_id)
        include_comp = mode == "revised"
        eligible = self._eligible(all_events, as_of_ts, include_compensation=include_comp)
        # 冻结事件集合与封存水位线：任务执行/续跑期间即使系统状态继续变化，
        # 该任务的回放结果也保持确定。
        confirmation = self.storage.get_confirmation(device_id)
        frozen_up_to = confirmation["up_to_ts"] if confirmation else None
        job = {
            "job_id": job_id or f"job-{uuid.uuid4().hex[:12]}",
            "device_id": device_id,
            "as_of_ts": as_of_ts,
            "total_events": len(eligible),
            "event_seqs": [ev.seq for ev in eligible],
            "params": {"mode": mode, "confirmed_up_to": frozen_up_to},
            "created_at": time.time(),
        }
        self.storage.create_job(job)
        return self.storage.get_job(job["job_id"])

    def run_replay(self, job_id: str, *, checkpoint_every: int = 100,
                   step_delay: float = 0.0,
                   max_events: Optional[int] = None) -> dict[str, Any]:
        """执行（或继续）一个重放任务，周期性落检查点。

        ``max_events`` 用于演示/测试：本次最多再处理多少条事件，到达后把
        检查点落盘并保持 running（模拟进程在处理中途被杀掉），下次继续。
        """
        job = self.storage.get_job(job_id)
        if job is None:
            raise NotFound(f"任务 {job_id} 不存在")
        if job["status"] == "done":
            return job

        self.storage.update_job(job_id, status="running")
        seqs: list[int] = json.loads(job["event_seqs_json"])
        params = json.loads(job["params_json"] or "{}")
        thresholds = self.storage.get_thresholds(job["device_id"])

        state = FoldState()
        pos = int(job["progress_pos"] or 0)
        if job["checkpoint_json"]:
            state = FoldState.from_checkpoint(json.loads(job["checkpoint_json"]))
        elif pos > 0:
            # 有进度但检查点缺失（异常遗留）：从头重放，保证正确性。
            pos = 0

        budget = None if max_events is None else max(0, max_events)
        index = pos
        while index < len(seqs):
            if budget is not None and index - pos >= budget:
                break
            ev = self.storage.get_event_by_seq(seqs[index])
            if ev is None:  # 事件日志只增不删，理论上不会发生
                self.storage.update_job(job_id, status="failed",
                                        error=f"事件 seq={seqs[index]} 缺失")
                raise NotFound(f"事件 seq={seqs[index]} 缺失")
            state.apply(ev, thresholds)
            index += 1
            if step_delay:
                time.sleep(step_delay)
            if index % max(1, checkpoint_every) == 0 or index == len(seqs):
                self.storage.update_job(
                    job_id, progress_pos=index,
                    checkpoint_json=json.dumps(state.to_checkpoint(), ensure_ascii=False,
                                               default=str))

        if index < len(seqs):
            # 未跑完：检查点必须与进度一起落盘，供重启后继续。
            self.storage.update_job(
                job_id, status="running", progress_pos=index,
                checkpoint_json=json.dumps(state.to_checkpoint(), ensure_ascii=False,
                                           default=str))
            return self.storage.get_job(job_id)

        frozen_up_to = params.get("confirmed_up_to")
        snap = state.snapshot(job["device_id"], job["as_of_ts"],
                              confirmed_up_to=frozen_up_to)
        if params.get("mode") == "confirmed":
            for cell in snap.fields.values():
                cell.source = (SRC_CONFIRMED if frozen_up_to is not None
                               and cell.ts <= frozen_up_to else SRC_REALTIME)
        else:
            for cell in snap.fields.values():
                if cell.source == SRC_COMPENSATED:
                    continue
                cell.source = SRC_CONFIRMED if (frozen_up_to is not None
                                                and cell.ts <= frozen_up_to) \
                    else SRC_REALTIME
        result = snap.to_dict()
        result["view_mode"] = params.get("mode", "revised")
        self.storage.update_job(job_id, status="done", progress_pos=len(seqs),
                                result_json=json.dumps(result, ensure_ascii=False,
                                                       default=str))
        return self.storage.get_job(job_id)

    def resume_unfinished(self, **kwargs) -> list[dict[str, Any]]:
        """进程启动后调用：继续所有 pending / 崩溃残留 running 的任务。"""
        done = []
        for job in self.storage.unfinished_jobs():
            done.append(self.run_replay(job["job_id"], **kwargs))
        return done

    def get_replay(self, job_id: str) -> dict[str, Any]:
        job = self.storage.get_job(job_id)
        if job is None:
            raise NotFound(f"任务 {job_id} 不存在")
        out = dict(job)
        if job.get("result_json"):
            out["result"] = json.loads(job["result_json"])
        return out

    def list_replays(self, status: Optional[str] = None) -> list[dict[str, Any]]:
        jobs = self.storage.list_jobs(status)
        for j in jobs:
            if j.get("result_json"):
                j["result"] = json.loads(j.pop("result_json"))
        return jobs
