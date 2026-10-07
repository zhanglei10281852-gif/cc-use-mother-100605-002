"""HTTP 服务：批量导入、状态查询、快照/死信查看、重放任务管理。

仅依赖标准库。默认数据目录 ./.twin_data，可用 TWIN_DATA_DIR 环境变量覆盖。
"""
from __future__ import annotations

import json
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .engine import DEFAULT_GRACE_MS, DEFAULT_GRID_MS, CorruptSnapshotError, ReplayEngine
from .model import EventParseError, parse_ts_ms


def create_app(data_dir: str | None = None, grid_ms: int = DEFAULT_GRID_MS,
               grace_ms: int = DEFAULT_GRACE_MS) -> "AppState":
    root = data_dir or os.environ.get("TWIN_DATA_DIR", os.path.join(os.getcwd(), ".twin_data"))
    try:
        engine = ReplayEngine(root, grid_ms=grid_ms, grace_ms=grace_ms)
    except CorruptSnapshotError as exc:
        raise SystemExit(f"启动失败：{exc}") from exc
    return AppState(engine, root)


class AppState:
    def __init__(self, engine: ReplayEngine, root: str):
        self.engine = engine
        self.root = root
        self.lock = threading.Lock()


def _json_response(handler: BaseHTTPRequestHandler, code: int, payload) -> None:
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "TwinReplay/0.1"

    def log_message(self, fmt, *args):  # 精简日志
        print(f"[http] {self.address_string()} - {fmt % args}")

    @property
    def app(self) -> AppState:
        return self.server.app  # type: ignore[attr-defined]

    # ------------------------------------------------------------ GET
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        parts = [p for p in parsed.path.split("/") if p]
        query = parse_qs(parsed.query)
        app = self.app
        try:
            if parts == ["health"]:
                _json_response(self, 200, {"status": "ok", "data_dir": app.root})
            elif parts == ["devices"]:
                with app.lock:
                    _json_response(self, 200, {"devices": app.engine.list_devices()})
            elif len(parts) == 3 and parts[0] == "devices" and parts[2] == "status":
                as_of = query.get("as_of", [None])[0]
                as_of_ms = parse_ts_ms(as_of) if as_of else None
                with app.lock:
                    _json_response(self, 200, app.engine.status(parts[1], as_of_ms))
            elif len(parts) == 3 and parts[0] == "devices" and parts[2] == "snapshots":
                with app.lock:
                    _json_response(self, 200, {"snapshots": app.engine.list_snapshots(parts[1])})
            elif parts == ["deadletters"]:
                with app.lock:
                    _json_response(self, 200, {"deadletters": app.engine.deadletters()})
            elif parts == ["replays"]:
                with app.lock:
                    _json_response(self, 200, {"tasks": app.engine.list_tasks()})
            elif len(parts) == 2 and parts[0] == "replays":
                with app.lock:
                    _json_response(self, 200, app.engine.get_task(parts[1]))
            else:
                _json_response(self, 404, {"error": "未知路由", "path": parsed.path})
        except KeyError:
            _json_response(self, 404, {"error": "任务不存在"})
        except EventParseError as exc:
            _json_response(self, 400, {"error": f"时间参数无效: {exc}"})
        except Exception as exc:  # noqa: BLE001 - 服务不因单请求崩溃
            _json_response(self, 500, {"error": f"{type(exc).__name__}: {exc}"})

    # ------------------------------------------------------------ POST
    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        parts = [p for p in parsed.path.split("/") if p]
        try:
            raw = self._read_body()
        except ValueError as exc:
            _json_response(self, 400, {"error": str(exc)})
            return

        if parts in (["events"], ["import"]):
            try:
                records = self._parse_records(raw, parse_qs(parsed.query))
            except (ValueError, json.JSONDecodeError) as exc:
                _json_response(self, 400, {"error": f"请求体解析失败: {exc}"})
                return
            with self.app.lock:
                report = self.app.engine.ingest(records, source="http")
            _json_response(self, 200, report)
            return

        if parts == ["replays"]:
            try:
                body = json.loads(raw or "{}")
                device_id = body["device_id"]
                as_of_ms = parse_ts_ms(body.get("as_of_ms"))
            except (json.JSONDecodeError, KeyError, EventParseError) as exc:
                _json_response(self, 400, {"error": f"需要 device_id 与 as_of_ms: {exc}"})
                return
            with self.app.lock:
                task = self.app.engine.create_replay(
                    device_id, as_of_ms, str(body.get("label", "")))
            _json_response(self, 202, task)
            return

        if parts == ["replays", "run"]:
            with self.app.lock:
                done = self.app.engine.run_pending()
            _json_response(self, 200, {"executed": [self._brief(t) for t in done]})
            return

        if len(parts) == 3 and parts[0] == "replays" and parts[2] == "run":
            with self.app.lock:
                task = self.app.engine.run_task(parts[1])
            _json_response(self, 200, task)
            return

        _json_response(self, 404, {"error": "未知路由", "path": parsed.path})

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", 0) or 0)
        if length > 16 * 1024 * 1024:
            raise ValueError("请求体超过 16MiB 限制")
        return self.rfile.read(length) if length else b""

    @staticmethod
    def _parse_records(raw: bytes, query) -> list:
        fmt = query.get("format", ["json"])[0]
        text = raw.decode("utf-8").strip()
        if not text:
            return []
        if fmt == "jsonl":
            records = []
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    # 非 dict 记录会在批量摄入时进入死信，不影响同批其它行
                    records.append(line)
            return records
        payload = json.loads(text)
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict) and isinstance(payload.get("events"), list):
            return payload["events"]
        raise ValueError("请求体应为 JSON 数组或 {\"events\": [...]}")

    @staticmethod
    def _brief(task: dict) -> dict:
        return {"id": task["id"], "label": task.get("label", ""),
                "device_id": task["device_id"],
                "as_of_ms": task["as_of_ms"], "status": task["status"],
                "error": task.get("error")}


def serve(host: str = "127.0.0.1", port: int = 8080, data_dir: str | None = None,
          grid_ms: int = DEFAULT_GRID_MS, grace_ms: int = DEFAULT_GRACE_MS) -> None:
    app = create_app(data_dir, grid_ms=grid_ms, grace_ms=grace_ms)
    httpd = ThreadingHTTPServer((host, port), ApiHandler)
    httpd.app = app  # type: ignore[attr-defined]
    print(f"[http] 设备状态回放服务监听 http://{host}:{port}  数据目录 {app.root}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[http] 已停止")
    finally:
        httpd.server_close()
