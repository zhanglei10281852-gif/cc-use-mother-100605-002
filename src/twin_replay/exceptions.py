"""twin_replay 的领域异常类型。"""


class TwinReplayError(Exception):
    """所有领域错误的基类。"""


class ValidationError(TwinReplayError):
    """遥测事件或请求参数不合法。"""


class CorruptEvent(TwinReplayError):
    """批次中单行数据损坏（JSON、字段或校验和错误）。"""

    def __init__(self, reason: str, raw: str = "", position: int = -1):
        super().__init__(reason)
        self.reason = reason
        self.raw = raw
        self.position = position


class NotFound(TwinReplayError):
    """设备、快照或任务不存在。"""


class Conflict(TwinReplayError):
    """与当前系统状态冲突，例如试图让确认快照回退。"""
