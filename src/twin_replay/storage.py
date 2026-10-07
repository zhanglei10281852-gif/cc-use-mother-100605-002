"""持久化层：只增日志、原子快照与崩溃恢复。

所有日志均为 JSON Lines，追加后 fsync；快照通过临时文件 + os.replace 原子落盘。
启动时重新扫描日志即可还原全部状态，不依赖额外的数据库。
"""
from __future__ import annotations

import json
import os
import tempfile
from typing import Any, Iterator

EVENTS_LOG = "events.jsonl"
COMP_LOG = "compensation.jsonl"
DEADLETTER_LOG = "deadletter.jsonl"
REPLAY_LOG = "replay_tasks.jsonl"
SNAPSHOT_DIR = "snapshots"


class JsonlLog:
    """单条 JSON 行的只增日志。"""

    def __init__(self, path: str):
        self.path = path
        self._fh: Any = None

    def open(self) -> "JsonlLog":
        self._fh = open(self.path, "a", encoding="utf-8")
        return self

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None

    def __enter__(self) -> "JsonlLog":
        if self._fh is None:
            self.open()
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def append(self, record: dict) -> None:
        assert self._fh is not None, "日志尚未打开"
        self._fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    @staticmethod
    def read(path: str) -> Iterator[dict]:
        if not os.path.exists(path):
            return
        with open(path, "r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, 1):
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)


class Storage:
    """封装数据目录下的全部文件。"""

    def __init__(self, root: str):
        self.root = os.path.abspath(root)
        os.makedirs(self.snapshot_dir, exist_ok=True)
        self.events_path = os.path.join(self.root, EVENTS_LOG)
        self.comp_path = os.path.join(self.root, COMP_LOG)
        self.deadletter_path = os.path.join(self.root, DEADLETTER_LOG)
        self.replay_path = os.path.join(self.root, REPLAY_LOG)

    @property
    def snapshot_dir(self) -> str:
        return os.path.join(self.root, SNAPSHOT_DIR)

    def snapshot_path(self, device_id: str, grid_ms: int) -> str:
        safe = device_id.replace(os.sep, "_")
        return os.path.join(self.snapshot_dir, f"{safe}__{grid_ms}.json")

    def list_snapshots(self) -> list[str]:
        if not os.path.isdir(self.snapshot_dir):
            return []
        return [
            os.path.join(self.snapshot_dir, name)
            for name in os.listdir(self.snapshot_dir)
            if name.endswith(".json")
        ]

    def write_json_atomic(self, path: str, payload: dict) -> None:
        directory = os.path.dirname(path) or "."
        fd, tmp = tempfile.mkstemp(prefix=".tmp-", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2))
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
            # 持久化目录项
            dir_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def read_json(self, path: str) -> dict:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
