"""基于标准库的 JSON HTTP API。

路由：

- ``GET  /devices``                                 设备列表
- ``POST /batches?batch_id=...``                    批量导入（JSON 数组或 JSONL）
- ``GET  /batches``                                 批次接收/隔离统计
- ``POST /devices/{id}/thresholds``                 设置阈值 {metric,min,max}
- ``POST /devices/{id}/confirm``                    封存确认 {as_of}
- ``GET  /devices/{id}/seal?as_of=...``             封存快照哈希验真
- ``GET  /devices/{id}/state``                      当前状态
- ``GET  /devices/{id}/view?as_of=...&mode=...``    任意时间点视图
- ``GET  /devices/{id}/windows?seal_as_of=...``     固化异常窗口
- ``POST /replays``                                 创建重放任务
- ``POST /replays/{job_id}/run``                    执行/继续
- ``POST /replays/resume``                          继续所有未完成任务
- ``GET  /replays[?status=...]`` / ``GET /replays/{id}``
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

from .exceptions import Conflict, NotFound, TwinReplayError
from .service import TwinService


def _json_bytes(obj, status: int = 200) -> tuple[int, bytes]:
    return status, json.dumps(obj, ensure_ascii=False, default=str).encode("utf-8")


class _Handler(BaseHTTPRequestHandler):
    server_version = "TwinReplay/0.1"

    # service 由 TwinServer 注入到类上
    service: TwinService = None  # type: ignore[assignment]

    def log_message(self, fmt, *args):  # 安静一点
        if getattr(self.server, "verbose", False):
            super().log_message(fmt, *args)

    # ---- 基础收发 -------------------------------------------------------

    def _send(self, status: int, body: bytes, content_type: str = "application/json"):
        self.send_response(status)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, status: int = 200):
        code, body = _json_bytes(obj, status)
        self._send(code, body)

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def _read_json(self) -> dict:
        raw = self._read_body()
        if not raw:
            return {}
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise _HttpError(400, f"请求体不是合法 JSON: {exc}") from None
        if not isinstance(data, dict):
            raise _HttpError(400, "请求体必须是 JSON 对象（批量数据请放在 events 字段）")
        return data

    def _handle_error(self, exc: Exception):
        if isinstance(exc, _HttpError):
            self._json({"error": exc.message}, exc.status)
        elif isinstance(exc, NotFound):
            self._json({"error": str(exc)}, 404)
        elif isinstance(exc, Conflict):
            self._json({"error": str(exc)}, 409)
        elif isinstance(exc, TwinReplayError):
            self._json({"error": str(exc)}, 422)
        elif isinstance(exc, (ValueError, TypeError)):
            self._json({"error": str(exc)}, 400)
        else:
            self._json({"error": f"内部错误: {exc}"}, 500)

    # ---- 路由 -----------------------------------------------------------

    def do_GET(self):
        try:
            self._route_get()
        except Exception as exc:  # noqa: BLE001 - 边界处统一转 JSON 错误
            self._handle_error(exc)

    def do_POST(self):
        try:
            self._route_post()
        except Exception as exc:  # noqa: BLE001
            self._handle_error(exc)

    def _route_get(self):
        parts = [p for p in urlsplit(self.path).path.split("/") if p]
        query = {k: v[-1] for k, v in parse_qs(urlsplit(self.path).query).items()}
        svc: TwinService = self.service

        if parts == ["devices"]:
            return self._json({"devices": svc.list_devices()})
        if parts == ["batches"]:
            return self._json({"batches": svc.storage.list_batches()})
        if len(parts) == 3 and parts[0] == "devices" and parts[2] == "seal":
            return self._json(svc.verify_seal(parts[1], query.get("as_of")))
        if len(parts) == 3 and parts[0] == "devices" and parts[2] == "windows":
            from .clock import to_epoch
            seal_at = to_epoch(query["seal_as_of"]) if query.get("seal_as_of") else None
            return self._json({"windows": svc.sealed_windows(parts[1], seal_at)})
        if len(parts) == 3 and parts[0] == "devices" and parts[2] == "state":
            return self._json(svc.current_state(parts[1]))
        if len(parts) == 3 and parts[0] == "devices" and parts[2] == "view":
            if "as_of" not in query:
                raise _HttpError(400, "view 必须提供 as_of 查询参数")
            return self._json(svc.view(parts[1], query["as_of"],
                                       query.get("mode", "revised")))
        if parts == ["replays"]:
            return self._json({"jobs": svc.list_replays(query.get("status"))})
        if len(parts) == 2 and parts[0] == "replays":
            return self._json(svc.get_replay(parts[1]))
        raise _HttpError(404, f"未知路径: /{'/'.join(parts)}")

    def _route_post(self):
        parts = [p for p in urlsplit(self.path).path.split("/") if p]
        query = {k: v[-1] for k, v in parse_qs(urlsplit(self.path).query).items()}
        svc: TwinService = self.service

        if parts == ["batches"]:
            raw = self._read_body()
            report = svc.import_batch(raw, batch_id=query.get("batch_id", ""))
            return self._json(report, 200 if report["corrupted_count"] == 0 else 207)
        if len(parts) == 3 and parts[0] == "devices" and parts[2] == "thresholds":
            data = self._read_json()
            if "metric" not in data:
                raise _HttpError(400, "缺少 metric")
            svc.set_threshold(parts[1], str(data["metric"]),
                              data.get("min"), data.get("max"))
            return self._json({"ok": True})
        if len(parts) == 3 and parts[0] == "devices" and parts[2] == "confirm":
            data = self._read_json()
            if "as_of" not in data:
                raise _HttpError(400, "缺少 as_of")
            return self._json(svc.confirm(parts[1], data["as_of"]))
        if parts == ["replays"]:
            data = self._read_json()
            for key in ("device_id", "as_of"):
                if key not in data:
                    raise _HttpError(400, f"缺少 {key}")
            job = svc.create_replay(str(data["device_id"]), data["as_of"],
                                    str(data.get("mode", "revised")),
                                    job_id=data.get("job_id"))
            return self._json(job, 201)
        if len(parts) == 3 and parts[0] == "replays" and parts[2] == "run":
            job = svc.run_replay(parts[1])
            return self._json(svc.get_replay(parts[1]) if job["status"] == "done" else job)
        if parts == ["replays", "resume"]:
            jobs = svc.resume_unfinished()
            return self._json({"resumed": [j["job_id"] for j in jobs], "count": len(jobs)})
        raise _HttpError(404, f"未知路径: /{'/'.join(parts)}")


class _HttpError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class TwinServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, service: TwinService, host: str = "127.0.0.1", port: int = 8080,
                 verbose: bool = False):
        self.verbose = verbose
        handler = type("_BoundHandler", (_Handler,), {"service": service})
        super().__init__((host, port), handler)
        self.service = service
