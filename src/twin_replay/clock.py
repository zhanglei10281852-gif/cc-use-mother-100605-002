"""时间与设备时钟偏差工具。

设备上报的 ``device_time`` 来自可能偏快/偏慢的设备时钟。约定
``clock_offset = 设备时钟 - 基准时钟``（秒，正值表示设备时钟偏快），因此::

    修正时间 = device_time - clock_offset

系统内部统一使用 UTC epoch 秒（float）做排序与比较，展示时再转回 ISO-8601。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Union

Epoch = float
TimeLike = Union[str, int, float, datetime]


def to_epoch(value: TimeLike) -> Epoch:
    """把 ISO-8601 字符串、epoch 数字或 datetime 归一化为 UTC epoch 秒。"""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        return float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("时间字符串为空")
        # 3.11 的 fromisoformat 可以识别 'Z'，旧写法也顺手兼容。
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"无法解析时间 {value!r}: {exc}") from None
    else:
        raise TypeError(f"不支持的时间类型: {type(value)!r}")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def correct_device_time(device_time: TimeLike, clock_offset: float = 0.0) -> Epoch:
    """按设备时钟偏差计算基准时间（epoch 秒）。"""
    return to_epoch(device_time) - float(clock_offset or 0.0)


def iso(epoch: Epoch) -> str:
    """epoch 秒转 ISO-8601 UTC 字符串。"""
    return datetime.fromtimestamp(float(epoch), tz=timezone.utc).isoformat()
