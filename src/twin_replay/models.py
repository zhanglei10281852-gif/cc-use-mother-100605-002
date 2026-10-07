"""领域模型：遥测事件、异常窗口、设备视图快照。

所有时间在进入本层之前都已由 :mod:`twin_replay.clock` 校正为 UTC epoch 秒。
快照使用规范化 JSON + SHA-256 得到内容指纹；确认（封存）时指纹一并入库，
任何试图改变封存内容的补偿写入都会被拒绝。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Optional

from .clock import iso

# 事件种类：普通遥测 / 设备更换 / 模型版本切换
TELEMETRY = "telemetry"
DEVICE_REPLACED = "device_replaced"
MODEL_SWITCHED = "model_switched"
CONTROL_KINDS = (DEVICE_REPLACED, MODEL_SWITCHED)

# 字段数据来源
SRC_REALTIME = "realtime"        # 封存水位线之后到达的普通数据
SRC_CONFIRMED = "confirmed"      # 已封存水位线之前、经确认的数据
SRC_COMPENSATED = "compensated"  # 封存之后补到历史时刻的数据（补偿）


def canonical_digest(payload: Any) -> str:
    """对任意可 JSON 化结构计算稳定的 SHA-256 指纹（完整 64 位）。"""
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class Event:
    """一条已校正、已落库的事件（不可变值对象）。"""

    seq: int                      # 入库顺序号（接收次序）
    event_id: str                 # 幂等键，重复上报被去重
    device_id: str
    kind: str
    corrected_ts: float           # 校正后的基准时间
    device_time: str              # 设备时钟原始时间（保留备查）
    clock_offset: float
    fields: dict[str, Any]        # telemetry: {指标: 值}
    unit_sn: str                  # 上报时的设备实物序列号（更换即换纪元）
    model_version: str            # 上报时的工艺模型版本
    batch_id: str
    checksum: Optional[str]
    is_compensation: bool

    def to_row(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "event_id": self.event_id,
            "device_id": self.device_id,
            "kind": self.kind,
            "corrected_ts": self.corrected_ts,
            "device_time": self.device_time,
            "clock_offset": self.clock_offset,
            "fields": self.fields,
            "unit_sn": self.unit_sn,
            "model_version": self.model_version,
            "batch_id": self.batch_id,
            "checksum": self.checksum,
            "is_compensation": 1 if self.is_compensation else 0,
        }

    @classmethod
    def from_row(cls, row: dict[str, Any]) -> "Event":
        return cls(
            seq=row["seq"],
            event_id=row["event_id"],
            device_id=row["device_id"],
            kind=row["kind"],
            corrected_ts=float(row["corrected_ts"]),
            device_time=row["device_time"],
            clock_offset=float(row["clock_offset"] or 0.0),
            fields=json.loads(row["fields_json"]),
            unit_sn=row["unit_sn"],
            model_version=row["model_version"],
            batch_id=row["batch_id"] or "",
            checksum=row["checksum"],
            is_compensation=bool(row["is_compensation"]),
        )


@dataclass
class AnomalyWindow:
    """一段连续的异常区间。``end_ts`` 为 None 表示截至查询时刻仍未恢复。"""

    device_id: str
    metric: str
    start_ts: float
    start_event_id: str
    end_ts: Optional[float] = None
    end_event_id: Optional[str] = None
    peak: Optional[float] = None
    sealed: bool = False
    compensated: bool = False  # 区间是否由/被补偿数据塑造

    def to_dict(self) -> dict[str, Any]:
        return {
            "device_id": self.device_id,
            "metric": self.metric,
            "start_ts": iso(self.start_ts),
            "start_event_id": self.start_event_id,
            "end_ts": iso(self.end_ts) if self.end_ts is not None else None,
            "end_event_id": self.end_event_id,
            "peak": self.peak,
            "status": "open" if self.end_ts is None else "closed",
            "sealed": self.sealed,
            "compensated": self.compensated,
        }


@dataclass
class FieldCell:
    """设备视图中的单个指标值及其血缘信息。"""

    value: Any
    ts: float
    event_id: str
    unit_sn: str
    model_version: str
    batch_id: str
    source: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "value": self.value,
            "ts": iso(self.ts),
            "event_id": self.event_id,
            "unit_sn": self.unit_sn,
            "model_version": self.model_version,
            "batch_id": self.batch_id,
            "source": self.source,
        }


@dataclass
class Snapshot:
    """某设备在任意时间点的完整视图。"""

    device_id: str
    as_of: float
    unit_sn: Optional[str]
    model_version: Optional[str]
    fields: dict[str, FieldCell] = field(default_factory=dict)
    open_anomalies: list[AnomalyWindow] = field(default_factory=list)
    closed_anomalies: list[AnomalyWindow] = field(default_factory=list)
    confirmed_up_to: Optional[float] = None
    compensated_metrics: set[str] = field(default_factory=set)

    # ---- 序列化与指纹 -------------------------------------------------

    def digest(self) -> str:
        """内容指纹：to_dict() 输出去掉 digest 字段后的 SHA-256。

        验真时对库存原文做同样的“去掉 digest 再哈希”即可逐字节复现，
        任何字段被改动都会导致指纹不符。
        """
        body = self.to_dict()
        body.pop("digest", None)
        return canonical_digest(body)

    def to_dict(self, include_closed: bool = True) -> dict[str, Any]:
        body = {
            "device_id": self.device_id,
            "as_of": iso(self.as_of),
            "epoch": {"unit_sn": self.unit_sn, "model_version": self.model_version},
            "fields": {metric: cell.to_dict() for metric, cell in sorted(self.fields.items())},
            "open_anomalies": [w.to_dict() for w in self.open_anomalies],
            "closed_anomalies": [w.to_dict() for w in self.closed_anomalies] if include_closed else [],
            "confirmed_up_to": iso(self.confirmed_up_to) if self.confirmed_up_to is not None else None,
            "compensated_metrics": sorted(self.compensated_metrics),
        }
        body_without_digest = dict(body)
        body["digest"] = canonical_digest(body_without_digest)
        return body
