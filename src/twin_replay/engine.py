"""核心引擎：事件折叠、异常检测、纪元边界、封存与视图重建。

折叠（fold）是一个增量状态机：按 ``(corrected_ts, seq)`` 依次喂入事件，
维护“当前字段值 + 异常窗口”。它的状态可序列化，因此重放任务能做检查点、
跨进程继续。

两种视图：

- **confirmed（确认视图）**：只含封存水位线之前、按时到达的事件。封存后迟到
  的数据绝不进入此视图，封存快照带哈希指纹，可随时验真。
- **revised（修订视图）**：在确认视图之上叠加补偿事件；每个字段格都带来源
  标记（``realtime`` / ``confirmed`` / ``compensated``），受补偿影响的指标和
  异常窗口单独列出，值班员一眼能看出历史被哪些迟到数据修订过。
"""
from __future__ import annotations

import json
from typing import Any, Optional

from .clock import Epoch
from .exceptions import Conflict, NotFound
from .models import (
    AnomalyWindow,
    DEVICE_REPLACED,
    FieldCell,
    MODEL_SWITCHED,
    Snapshot,
    SRC_COMPENSATED,
    SRC_CONFIRMED,
    SRC_REALTIME,
    Event,
)


def _is_abnormal(value: Any, rule: Optional[dict[str, Optional[float]]]) -> bool:
    """数值指标按配置区间判异；非数值指标与未配置阈值的指标不判异。"""
    if not isinstance(value, (int, float)) or isinstance(value, bool) or rule is None:
        return False
    lo, hi = rule.get("min"), rule.get("max")
    if lo is not None and value < lo:
        return True
    if hi is not None and value > hi:
        return True
    return False


class FoldState:
    """折叠状态机的可序列化状态。

    fields: metric -> 最新 FieldCell（含血缘）
    windows: 各指标当前开启 + 已关闭的窗口
    unit_sn / model_version: 当前纪元标识
    """

    def __init__(self) -> None:
        self.unit_sn: Optional[str] = None
        self.model_version: Optional[str] = None
        self.fields: dict[str, FieldCell] = {}
        self.open_windows: dict[str, AnomalyWindow] = {}
        self.closed_windows: list[AnomalyWindow] = []
        self.last_seq: int = 0
        self.compensated_metrics: set[str] = set()

    # ---- 检查点 --------------------------------------------------------

    def to_checkpoint(self) -> dict[str, Any]:
        return {
            "unit_sn": self.unit_sn,
            "model_version": self.model_version,
            "last_seq": self.last_seq,
            "fields": {m: c.__dict__ for m, c in self.fields.items()},
            "open_windows": {m: w.__dict__ for m, w in self.open_windows.items()},
            "closed_windows": [w.__dict__ for w in self.closed_windows],
            "compensated_metrics": sorted(self.compensated_metrics),
        }

    @classmethod
    def from_checkpoint(cls, data: dict[str, Any]) -> "FoldState":
        st = cls()
        st.unit_sn = data.get("unit_sn")
        st.model_version = data.get("model_version")
        st.last_seq = int(data.get("last_seq", 0))
        st.fields = {
            m: FieldCell(**c) for m, c in data.get("fields", {}).items()
        }
        st.open_windows = {
            m: AnomalyWindow(**w) for m, w in data.get("open_windows", {}).items()
        }
        st.closed_windows = [AnomalyWindow(**w) for w in data.get("closed_windows", [])]
        st.compensated_metrics = set(data.get("compensated_metrics", []))
        return st

    # ---- 折叠一步 ------------------------------------------------------

    def apply(self, ev: Event,
              thresholds: dict[str, dict[str, Optional[float]]]) -> None:
        """把一条事件折叠进状态（调用方保证事件按校正时间有序）。"""
        self.last_seq = ev.seq

        if ev.kind == DEVICE_REPLACED:
            # 设备更换 = 硬纪元边界：旧序列号的状态全部清空，开启中的异常关闭。
            self._close_all_open(ev)
            self.fields.clear()
            self.unit_sn = str(ev.fields.get("new_unit_sn") or ev.unit_sn or "")
            self.model_version = ev.model_version or self.model_version
            return

        if ev.kind == MODEL_SWITCHED:
            # 模型版本切换 = 软纪元边界：保留实测值，只换版本，并关闭旧窗口
            # （旧工艺参数下的异常不应记到新模型头上）。
            self._close_all_open(ev)
            self.model_version = str(ev.fields.get("new_model_version")
                                    or ev.model_version or "")
            if ev.unit_sn:
                self.unit_sn = ev.unit_sn
            return

        # 普通遥测：建立/更新纪元标识
        if ev.unit_sn:
            if self.unit_sn is not None and ev.unit_sn != self.unit_sn:
                # 隐式设备更换：序列号变了但没发控制事件，同样按硬边界处理。
                self._close_all_open(ev)
                self.fields.clear()
            self.unit_sn = ev.unit_sn
        if ev.model_version and ev.model_version != self.model_version:
            # 遥测自带的模型版本发生变化视为软切换：关闭旧模型下的窗口。
            if self.model_version is not None:
                self._close_all_open(ev)
            self.model_version = ev.model_version

        for metric, value in ev.fields.items():
            source = SRC_COMPENSATED if ev.is_compensation else SRC_REALTIME
            cell = FieldCell(
                value=value, ts=ev.corrected_ts, event_id=ev.event_id,
                unit_sn=ev.unit_sn, model_version=ev.model_version,
                batch_id=ev.batch_id, source=source,
            )
            self.fields[metric] = cell
            if ev.is_compensation:
                self.compensated_metrics.add(metric)
            self._update_anomaly(ev, metric, value, thresholds.get(metric),
                                 compensated=ev.is_compensation)

    def _update_anomaly(self, ev: Event, metric: str, value: Any,
                        rule: Optional[dict[str, Optional[float]]],
                        compensated: bool) -> None:
        window = self.open_windows.get(metric)
        bad = _is_abnormal(value, rule)
        if bad and window is None:
            self.open_windows[metric] = AnomalyWindow(
                device_id=ev.device_id, metric=metric, start_ts=ev.corrected_ts,
                start_event_id=ev.event_id, peak=value if isinstance(value, (int, float)) else None,
                compensated=compensated,
            )
        elif bad and window is not None:
            if isinstance(value, (int, float)):
                window.peak = value if window.peak is None else max(window.peak, value)
            if compensated:
                window.compensated = True
        elif not bad and window is not None:
            window.end_ts = ev.corrected_ts
            window.end_event_id = ev.event_id
            if compensated:
                window.compensated = True
            self.closed_windows.append(window)
            del self.open_windows[metric]

    def _close_all_open(self, ev: Event) -> None:
        for metric in list(self.open_windows):
            w = self.open_windows.pop(metric)
            w.end_ts = ev.corrected_ts
            w.end_event_id = ev.event_id
            self.closed_windows.append(w)

    # ---- 产出快照 ------------------------------------------------------

    def snapshot(self, device_id: str, as_of: Epoch,
                 confirmed_up_to: Optional[float] = None) -> Snapshot:
        snap = Snapshot(
            device_id=device_id, as_of=float(as_of),
            unit_sn=self.unit_sn, model_version=self.model_version,
            fields=dict(self.fields),
            open_anomalies=[
                # 排序后的副本，避免外部修改状态机内部
                AnomalyWindow(**{**w.__dict__})
                for w in sorted(self.open_windows.values(),
                                key=lambda x: (x.start_ts, x.metric))
            ],
            closed_anomalies=[
                AnomalyWindow(**{**w.__dict__})
                for w in self.closed_windows
            ],
            confirmed_up_to=confirmed_up_to,
            compensated_metrics=set(self.compensated_metrics),
        )
        return snap


def fold_events(events: list[Event],
                thresholds: dict[str, dict[str, Optional[float]]],
                *, as_of: Optional[float] = None,
                state: Optional[FoldState] = None,
                progress_cb=None) -> FoldState:
    """把有序事件折叠进状态机。``as_of`` 给出时只应用不晚于它的事件。"""
    st = state or FoldState()
    for ev in events:
        if as_of is not None and ev.corrected_ts > as_of:
            break
        st.apply(ev, thresholds)
        if progress_cb is not None:
            progress_cb(ev)
    return st
