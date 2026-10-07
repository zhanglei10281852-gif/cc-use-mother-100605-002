"""遥测事件的解析与校验，以及批次部分损坏的隔离。

一条原始事件示例（JSON）::

    {
      "event_id": "evt-1",
      "device_id": "WT-01",
      "device_time": "2026-10-07T10:00:00Z",
      "clock_offset": 5.0,
      "metrics": {"温度": 72.1, "负载": 0.8},
      "unit_sn": "SN-A", "model_version": "m-v3",
      "batch_id": "b-001", "checksum": "..."
    }

控制事件用 ``kind`` 区分：``device_replaced`` / ``model_switched``。
批次支持逐行 JSON（JSON Lines）或一个 JSON 数组；损坏的行被记录并跳过，
不会影响同批中完好的行。
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Optional

from .clock import correct_device_time
from .exceptions import CorruptEvent, ValidationError

_VALID_KINDS = {"telemetry", "device_replaced", "model_switched"}


def event_checksum(raw: dict[str, Any]) -> str:
    """按约定字段计算校验和：sha256(event_id|device_time|规范化metrics)。"""
    body = json.dumps(
        {"e": raw.get("event_id"), "t": raw.get("device_time"),
         "m": raw.get("metrics", {})},
        sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str,
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


@dataclass
class ParsedEvent:
    """通过解析与校验、等待入库的事件。"""

    event_id: str
    device_id: str
    kind: str
    corrected_ts: float
    device_time: str
    clock_offset: float
    metrics: dict[str, Any]
    unit_sn: str
    model_version: str
    batch_id: str
    checksum: Optional[str]
    is_compensation: bool = False


@dataclass
class BatchResult:
    """批次导入结果：好事件 + 被隔离的坏行 + 去重统计。"""

    batch_id: str
    accepted: list[ParsedEvent] = field(default_factory=list)
    corrupted: list[dict[str, Any]] = field(default_factory=list)
    duplicates: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.corrupted

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "accepted_count": len(self.accepted),
            "corrupted_count": len(self.corrupted),
            "duplicates_count": len(self.duplicates),
            "corrupted": self.corrupted,
            "duplicates": self.duplicates,
        }


def _coerce_number(name: str, value: Any) -> float:
    if isinstance(value, bool):  # bool 是 int 的子类，显式拒绝
        raise ValidationError(f"{name} 必须是数字")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            pass
    raise ValidationError(f"{name} 必须是数字，得到 {value!r}")


def parse_event(raw: Any, *, default_batch: str = "", position: int = -1) -> ParsedEvent:
    """解析并校验单条原始事件；失败抛 :class:`CorruptEvent`。"""
    if not isinstance(raw, dict):
        raise CorruptEvent("事件必须是 JSON 对象", position=position)

    def need(key: str) -> Any:
        if key not in raw or raw[key] in (None, ""):
            raise CorruptEvent(f"缺少必填字段 {key}", position=position)
        return raw[key]

    try:
        event_id = str(need("event_id")).strip()
        device_id = str(need("device_id")).strip()
        kind = str(raw.get("kind", "telemetry")).strip()
        if kind not in _VALID_KINDS:
            raise CorruptEvent(f"未知事件种类 {kind!r}", position=position)

        device_time_raw = need("device_time")
        clock_offset = _coerce_number("clock_offset", raw.get("clock_offset", 0.0))
        corrected = correct_device_time(device_time_raw, clock_offset)

        metrics_in = raw.get("metrics", {})
        if not isinstance(metrics_in, dict):
            raise CorruptEvent("metrics 必须是对象", position=position)
        metrics: dict[str, Any] = {}
        for key, val in metrics_in.items():
            key = str(key)
            if not key.strip():
                raise CorruptEvent("指标名不能为空", position=position)
            # 数值指标转 float；数字字符串也转；其余（如工艺档位字符串）原样保留。
            if not isinstance(val, (int, float, str)):
                raise CorruptEvent(f"指标 {key!r} 的值类型不受支持: {type(val).__name__}",
                                   position=position)
            if isinstance(val, (int, float)):
                metrics[key] = float(val)
            elif isinstance(val, str) and _looks_numeric(val):
                metrics[key] = float(val.strip())
            else:
                metrics[key] = val

        unit_sn = str(raw.get("unit_sn", "") or "").strip()
        model_version = str(raw.get("model_version", "") or "").strip()
        if kind == "telemetry" and not unit_sn:
            raise CorruptEvent("telemetry 事件必须带 unit_sn", position=position)
        if kind == "telemetry" and not model_version:
            raise CorruptEvent("telemetry 事件必须带 model_version", position=position)

        batch_id = str(raw.get("batch_id", default_batch) or default_batch).strip()
        checksum = raw.get("checksum")
        checksum = str(checksum).strip() if checksum else None
        if checksum:
            expected = event_checksum(raw)
            if checksum != expected:
                raise CorruptEvent("校验和不匹配，事件可能在传输中损坏", position=position)

        is_comp = bool(raw.get("is_compensation", False))
        return ParsedEvent(
            event_id=event_id, device_id=device_id, kind=kind,
            corrected_ts=corrected, device_time=str(device_time_raw),
            clock_offset=clock_offset, metrics=metrics,
            unit_sn=unit_sn, model_version=model_version,
            batch_id=batch_id, checksum=checksum, is_compensation=is_comp,
        )
    except CorruptEvent:
        raise
    except (ValidationError, ValueError, TypeError) as exc:
        raise CorruptEvent(str(exc), position=position) from None


def _looks_numeric(text: str) -> bool:
    try:
        float(text.strip())
        return True
    except ValueError:
        return False


def parse_batch(payload: str | bytes | list[dict[str, Any]],
                batch_id: str = "") -> BatchResult:
    """解析整个批次。坏行进入 ``corrupted`` 隔离区，好行继续处理。"""
    result = BatchResult(batch_id=batch_id)
    raw_text: Optional[str] = None
    records: list[Any]

    if isinstance(payload, (list, tuple)):
        records = list(enumerate(payload))
    else:
        raw_text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
        text = raw_text.strip()
        if not text:
            raise CorruptEvent("批次为空")
        if text[0] in "[{":
            # 可能是 JSON 数组，也可能是单行对象。
            try:
                loaded = json.loads(text)
                records = list(enumerate(loaded if isinstance(loaded, list) else [loaded]))
            except json.JSONDecodeError as err:
                if text[0] == "[":
                    raise CorruptEvent(f"批次 JSON 无法解析: {err}") from None
                records = _jsonl_records(text, result)
        else:
            records = _jsonl_records(text, result)

    seen: set[str] = set()
    for index, raw in records:
        try:
            parsed = parse_event(raw, default_batch=batch_id, position=index)
        except CorruptEvent as exc:
            result.corrupted.append({
                "position": index,
                "reason": exc.reason,
                "raw": _safe_raw(raw),
            })
            continue
        if parsed.event_id in seen:
            result.duplicates.append(parsed.event_id)
            continue
        seen.add(parsed.event_id)
        if not parsed.batch_id:
            parsed.batch_id = batch_id
        result.accepted.append(parsed)
    return result


def _jsonl_records(text: str, result: BatchResult) -> list[tuple[int, Any]]:
    """返回 (原始行号, 记录)；损坏行直接记入批次隔离区。"""
    records: list[tuple[int, Any]] = []
    for index, line in enumerate(text.splitlines()):
        line = line.strip()
        if not line:
            continue
        try:
            records.append((index, json.loads(line)))
        except json.JSONDecodeError as exc:
            result.corrupted.append({
                "position": index,
                "reason": f"JSON 行无法解析: {exc.msg}",
                "raw": line[:500],
            })
    return records


def _safe_raw(raw: Any) -> str:
    try:
        return json.dumps(raw, ensure_ascii=False, default=str)[:500]
    except (TypeError, ValueError):
        return str(raw)[:500]
