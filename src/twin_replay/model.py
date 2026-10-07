"""遥测事件领域模型：解析、时钟归一化与幂等键。"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from typing import Any

# 字段数据来源
ORIGINAL = "original"            # 正常时间内到达，已纳入确认快照
COMPENSATION = "compensation"    # 越过水位线后到达的补偿数据

EVENT_KINDS = ("telemetry", "alarm", "replacement")


class EventParseError(ValueError):
    """单条事件记录无法解析。"""


def parse_ts_ms(value: Any) -> int:
    """把输入时间解析为纪元毫秒。

    接受：整数/浮点数（直接视为毫秒），或 ISO-8601 字符串（Z 结尾也可）。
    """
    if isinstance(value, bool):
        raise EventParseError("时间戳不能是布尔值")
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.strip():
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        # 纯数字字符串视为纪元毫秒（查询参数无法携带类型）
        if text.lstrip("-").isdigit():
            return int(text)
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            raise EventParseError(f"无法解析时间戳: {value!r}") from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    raise EventParseError(f"无法解析时间戳: {value!r}")


def iso(ms: int) -> str:
    """毫秒纪元转 UTC ISO 字符串。"""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _require_text(row: dict, name: str) -> str:
    value = row.get(name)
    if not isinstance(value, str) or not value.strip():
        raise EventParseError(f"字段 {name} 必须是非空字符串")
    return value.strip()


@dataclass(frozen=True)
class Event:
    """一条已归一化的遥测事件。"""
    key: str                 # 幂等键
    device_id: str
    kind: str                 # telemetry / alarm / replacement
    t_ms: int                 # 归一化后的标准事件时间
    received_ms: int          # 服务端接收时间
    seq: int
    unit_serial: str          # 事件发生时的物理机序列号（可空）
    model_version: str        # 事件采用的模型版本（可空）
    fields: dict[str, Any]    # telemetry：温度/负载/工艺参数
    alarm_code: str | None    # alarm：报警码
    alarm_active: bool | None  # alarm：True 开始 / False 结束
    new_serial: str | None    # replacement：新物理机序列号
    new_model_version: str | None
    content_hash: str

    def as_storage(self, source: str, ingest_seq: int) -> dict:
        return {
            "key": self.key,
            "device_id": self.device_id,
            "kind": self.kind,
            "t_ms": self.t_ms,
            "received_ms": self.received_ms,
            "seq": self.seq,
            "unit_serial": self.unit_serial,
            "model_version": self.model_version,
            "fields": self.fields,
            "alarm_code": self.alarm_code,
            "alarm_active": self.alarm_active,
            "new_serial": self.new_serial,
            "new_model_version": self.new_model_version,
            "content_hash": self.content_hash,
            "source": source,
            "ingest_seq": ingest_seq,
        }


def _content_hash(payload: dict) -> str:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def parse_event(row: Any, received_ms: int) -> Event:
    """把一条外部 JSON 记录解析为 Event，任何不合规之处抛 EventParseError。

    约定：clock_skew_ms = 设备时钟 − 标准时钟，因此
    标准时间 = 设备时间 − clock_skew_ms。
    """
    if not isinstance(row, dict):
        raise EventParseError("记录必须是 JSON 对象")
    device_id = _require_text(row, "device_id")
    kind = row.get("event_type", "telemetry")
    if kind not in EVENT_KINDS:
        raise EventParseError(f"未知 event_type: {kind!r}")

    if "ts_ms" in row:
        device_ms = parse_ts_ms(row["ts_ms"])
    elif "device_ts" in row:
        device_ms = parse_ts_ms(row["device_ts"])
    else:
        raise EventParseError("缺少时间戳（ts_ms 或 device_ts）")
    skew = row.get("clock_skew_ms", 0)
    if isinstance(skew, bool) or not isinstance(skew, (int, float)):
        raise EventParseError("clock_skew_ms 必须是数字")
    t_ms = device_ms - int(skew)

    event_id = row.get("event_id")
    seq_raw = row.get("seq")
    if event_id is not None:
        if not isinstance(event_id, str) or not event_id.strip():
            raise EventParseError("event_id 必须是非空字符串")
        key = event_id.strip()
        seq = int(seq_raw) if isinstance(seq_raw, int) and not isinstance(seq_raw, bool) else 0
    else:
        if not isinstance(seq_raw, int) or isinstance(seq_raw, bool):
            raise EventParseError("缺少 event_id 时必须提供整数 seq")
        seq = seq_raw
        key = f"{device_id}:{seq}"

    unit_serial = str(row.get("unit_serial", "") or "").strip()
    model_version = str(row.get("model_version", "") or "").strip()

    fields: dict[str, Any] = {}
    alarm_code: str | None = None
    alarm_active: bool | None = None
    new_serial: str | None = None
    new_model_version: str | None = None

    if kind == "telemetry":
        raw_fields = row.get("fields")
        if not isinstance(raw_fields, dict) or not raw_fields:
            raise EventParseError("telemetry 事件必须携带非空 fields 对象")
        for fname, fvalue in raw_fields.items():
            if not isinstance(fname, str) or not fname.strip():
                raise EventParseError("fields 的键必须是非空字符串")
            if isinstance(fvalue, bool) or not isinstance(fvalue, (int, float, str)):
                raise EventParseError(f"字段 {fname} 的值只支持数字或字符串")
            fields[fname.strip()] = fvalue
    elif kind == "alarm":
        node = row.get("alarm") if isinstance(row.get("alarm"), dict) else row
        code = node.get("code")
        active = node.get("active")
        if not isinstance(code, str) or not code.strip():
            raise EventParseError("alarm 事件必须提供 code")
        if not isinstance(active, bool):
            raise EventParseError("alarm 事件必须提供布尔 active")
        alarm_code, alarm_active = code.strip(), active
    else:  # replacement
        new_serial = row.get("new_serial")
        if not isinstance(new_serial, str) or not new_serial.strip():
            raise EventParseError("replacement 事件必须提供 new_serial")
        new_serial = new_serial.strip()
        nmv = row.get("new_model_version")
        if nmv is not None:
            if not isinstance(nmv, str) or not nmv.strip():
                raise EventParseError("new_model_version 必须是非空字符串")
            new_model_version = nmv.strip()

    content = {
        "device_id": device_id, "kind": kind, "t_ms": t_ms, "seq": seq,
        "unit_serial": unit_serial, "model_version": model_version,
        "fields": fields, "alarm_code": alarm_code, "alarm_active": alarm_active,
        "new_serial": new_serial, "new_model_version": new_model_version,
    }
    return Event(
        key=key, device_id=device_id, kind=kind, t_ms=t_ms,
        received_ms=received_ms, seq=seq, unit_serial=unit_serial,
        model_version=model_version, fields=fields,
        alarm_code=alarm_code, alarm_active=alarm_active,
        new_serial=new_serial, new_model_version=new_model_version,
        content_hash=_content_hash(content),
    )
