"""离线命令行工具与端到端演示。

常用：

    python -m twin_replay.cli demo --db demo.db     # 完整故事线演示（含崩溃续跑）
    python -m twin_replay.cli serve --db demo.db --port 8080
    python -m twin_replay.cli import events.jsonl --batch b1 --db demo.db
    python -m twin_replay.cli view WT-01 --as-of 2026-10-07T10:05:00Z --db demo.db
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Optional

from .clock import iso, to_epoch
from .exceptions import TwinReplayError
from .parsing import event_checksum
from .service import TwinService
from .storage import Storage


def _open(db: str) -> tuple[Storage, TwinService]:
    storage = Storage(db)
    return storage, TwinService(storage)


def _print(obj: Any) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, default=str))


# --------------------------------------------------------------------------
# 演示数据：一条完整的风机故事线
# --------------------------------------------------------------------------

def _evt(event_id: str, device_id: str, device_time: str, metrics: dict[str, Any],
         unit_sn: str = "SN-A", model: str = "m-v3", clock_offset: float = 0.0,
         batch_id: str = "b-main", kind: str = "telemetry",
         is_compensation: bool = False, with_checksum: bool = True) -> dict[str, Any]:
    raw: dict[str, Any] = {
        "event_id": event_id, "device_id": device_id, "kind": kind,
        "device_time": device_time, "clock_offset": clock_offset,
        "metrics": metrics, "unit_sn": unit_sn, "model_version": model,
        "batch_id": batch_id,
    }
    if is_compensation:
        raw["is_compensation"] = True
    if with_checksum and kind == "telemetry":
        raw["checksum"] = event_checksum(raw)
    return raw


def demo_events() -> list[dict[str, Any]]:
    """温度超温报警的完整过程：升温 -> 超温 -> 维持 -> 回落，含时钟偏差。"""
    d = "WT-01"
    rows = [
        # 设备时钟快 30 秒：device_time 10:00:30 实际是 10:00:00
        _evt("e1", d, "2026-10-07T10:00:30Z", {"温度": 68.0, "负载": 0.70, "工艺档位": "稳燃"},
             clock_offset=30.0),
        _evt("e2", d, "2026-10-07T10:01:30Z", {"温度": 74.5, "负载": 0.78},
             clock_offset=30.0),
        _evt("e3", d, "2026-10-07T10:02:30Z", {"温度": 81.2, "负载": 0.83},
             clock_offset=30.0),   # 超过 80 度阈值，异常开始
        _evt("e4", d, "2026-10-07T10:03:30Z", {"温度": 83.6, "负载": 0.85},
             clock_offset=30.0),
        # 这条故意晚发（见 demo 流程），先不放入主批次
    ]
    late = _evt("e5-late", d, "2026-10-07T10:04:00Z", {"温度": 79.9, "负载": 0.80},
                clock_offset=30.0, batch_id="b-late")
    rows += [
        _evt("e6", d, "2026-10-07T10:05:30Z", {"温度": 76.0, "负载": 0.74},
             clock_offset=30.0),   # 回落，异常窗口关闭
        _evt("e7", d, "2026-10-07T10:06:30Z", {"温度": 70.0, "负载": 0.70},
             clock_offset=30.0),
        # 设备更换：齿轮箱换新，序列号变化
        _evt("e8", d, "2026-10-07T10:07:30Z", {"new_unit_sn": "SN-B"},
             unit_sn="SN-A", model="m-v3", clock_offset=30.0,
             kind="device_replaced", with_checksum=False),
        # 新设备第一批读数
        _evt("e9", d, "2026-10-07T10:08:30Z", {"温度": 65.0, "负载": 0.60},
             unit_sn="SN-B", clock_offset=30.0),
        # 工艺模型从 v3 切到 v4
        _evt("e10", d, "2026-10-07T10:09:30Z", {"new_model_version": "m-v4"},
             unit_sn="SN-B", model="m-v4", clock_offset=30.0,
             kind="model_switched", with_checksum=False),
        _evt("e11", d, "2026-10-07T10:10:30Z", {"温度": 66.2, "负载": 0.62, "工艺档位": "经济"},
             unit_sn="SN-B", model="m-v4", clock_offset=30.0),
    ]
    return rows, late


def corrupted_lines() -> str:
    """构造一个部分损坏的 JSONL 批次（3 好 2 坏 + 1 重复）。"""
    d = "WT-02"
    good = [
        _evt("w1", d, "2026-10-07T11:00:00Z", {"温度": 50.0}, unit_sn="SN-X",
             batch_id="b-corrupt"),
        _evt("w2", d, "2026-10-07T11:01:00Z", {"温度": 51.0}, unit_sn="SN-X",
             batch_id="b-corrupt"),
        _evt("w1", d, "2026-10-07T11:00:00Z", {"温度": 50.0}, unit_sn="SN-X",
             batch_id="b-corrupt"),  # 批内重复 event_id
    ]
    lines = [json.dumps(good[0], ensure_ascii=False),
             '{"event_id": "bad1", "device_id": "WT-02", 设备时间坏了}',  # JSON 损坏
             json.dumps(good[1], ensure_ascii=False),
             json.dumps({"event_id": "bad2", "device_id": "WT-02",
                         "device_time": "2026-10-07T11:02:00Z"}, ensure_ascii=False),  # 缺 unit_sn
             json.dumps(good[2], ensure_ascii=False)]
    return "\n".join(lines)


def run_demo(args: argparse.Namespace) -> None:
    if os.path.exists(args.db):
        os.remove(args.db)
    storage, svc = _open(args.db)

    print("=" * 70)
    print("场景：远程运维中心接管风机 WT-01，值班员需要复盘超温报警全过程")
    print("=" * 70)

    svc.set_threshold("WT-01", "温度", min_value=0, max_value=80.0)
    svc.set_threshold("WT-01", "负载", min_value=0.2, max_value=0.95)

    rows, late = demo_events()
    print("\n[1] 批量导入主遥测批次（每条事件带设备时钟偏差 +30s 与校验和）")
    report = svc.import_batch(json.dumps(rows, ensure_ascii=False), batch_id="b-main")
    _print(report)

    print("\n[2] 导入一个部分损坏的批次（坏行隔离，好行照常入库）")
    _print(svc.import_batch(corrupted_lines(), batch_id="b-corrupt"))

    print("\n[3] 值班员在 10:05:00Z 对历史做确认封存（不可变快照）")
    sealed = svc.confirm("WT-01", "2026-10-07T10:05:00Z")
    print(f"    封存快照指纹: {sealed['digest'][:20]}...")
    print(f"    封存时开启中的异常: {[w['metric'] for w in sealed['open_anomalies']]}")

    print("\n[4] 迟到事件 e5-late 到达（设备时钟 10:04:00Z，落在封存区间内）")
    late_report = svc.import_batch(json.dumps([late]), batch_id="b-late")
    _print(late_report)
    stored = [e for e in storage.events_for_device("WT-01") if e.event_id == "e5-late"][0]
    print(f"    -> 系统自动把它标为补偿数据: is_compensation={stored.is_compensation}")

    print("\n[5] 重复上报 e1 再次发送 -> 幂等去重")
    dup = svc.import_batch(json.dumps([rows[0]]), batch_id="b-dup")
    _print(dup)

    print("\n[6] 确认视图 @10:05（补偿数据不可见，快照与封存一致）")
    confirmed = svc.view("WT-01", "2026-10-07T10:05:00Z", mode="confirmed")
    print(f"    温度: {confirmed['fields']['温度']['value']} "
          f"来源={confirmed['fields']['温度']['source']}")
    revised = svc.view("WT-01", "2026-10-07T10:05:00Z", mode="revised")
    print(f"    修订视图温度: {revised['fields']['温度']['value']} "
          f"来源={revised['fields']['温度']['source']} "
          f"(受补偿指标: {revised['compensated_metrics']})")

    print("\n[7] 封存快照哈希验真（补偿写入没有改动任何已确认内容）")
    _print(svc.verify_seal("WT-01"))

    print("\n[8] 异常窗口复盘：封存固化窗口 + 补偿对窗口的修订")
    sealed_wins = storage.sealed_windows("WT-01", to_epoch("2026-10-07T10:05:00Z"))
    for w in sealed_wins:
        if w["end_ts"] is None:
            span = "封存时仍开放"
        else:
            span = f"{iso(w['start_ts'])} -> {iso(w['end_ts'])}"
        print(f"    [封存] {w['metric']}: {span} (峰值 {w['peak']})")
    cur = svc.current_state("WT-01")
    for w in cur["closed_anomalies"]:
        tag = " [含补偿]" if w["compensated"] else ""
        print(f"    [修订] {w['metric']}: {w['start_ts']} -> {w['end_ts']} "
              f"峰值 {w['peak']}{tag}")

    print("\n[9] 设备更换 + 模型切换后的视图（状态不跨纪元串值）")
    print(f"    当前实物序列号: {cur['epoch']['unit_sn']} "
          f"模型版本: {cur['epoch']['model_version']}")

    print("\n[10] 创建重放任务 @10:10:30（修订视图），模拟处理到一半进程崩溃")
    job = svc.create_replay("WT-01", "2026-10-07T10:10:30Z", mode="revised",
                            job_id="job-demo")
    print(f"    任务 {job['job_id']}，冻结事件数 {job['total_events']}")
    # 处理一半并在中途退出（检查点已落盘、状态停在 running），等价于进程被杀。
    half = max(1, job["total_events"] // 2)
    svc.run_replay("job-demo", max_events=half)
    crashed = storage.get_job("job-demo")
    print(f"    进程崩溃时：status={crashed['status']} progress={crashed['progress_pos']}"
          f"/{crashed['total_events']}（检查点已持久化）")

    print("\n[11] 进程重启 -> resume 从未完成处继续，无需重跑前半段")
    resumed = svc.resume_unfinished()
    final = svc.get_replay("job-demo")
    print(f"    续跑完成: status={final['status']} progress={final['progress_pos']}"
          f"/{final['total_events']}（续跑了 {len(resumed)} 个任务）")
    result = final["result"]
    print(f"    回放终态温度={result['fields']['温度']['value']} "
          f"序列号={result['epoch']['unit_sn']} 模型={result['epoch']['model_version']}")

    print("\n[12] 用一个全新的服务实例（等价于换进程）再恢复一次：空转")
    storage.close()
    storage2, svc2 = _open(args.db)
    again = svc2.resume_unfinished()
    print(f"    未完成任务数={len(again)}")
    storage2.close()
    storage = None
    print(f"\n演示完成，数据库保留在 {args.db}，可用 serve 子命令启动 HTTP 服务。")


# --------------------------------------------------------------------------
# 普通 CLI 子命令
# --------------------------------------------------------------------------

def cmd_serve(args: argparse.Namespace) -> None:
    from .api import TwinServer
    storage, svc = _open(args.db)
    # 重启即恢复未完成任务
    recovered = svc.resume_unfinished()
    if recovered:
        print(f"启动时续跑了 {len(recovered)} 个未完成重放任务", file=sys.stderr)
    server = TwinServer(svc, host=args.host, port=args.port, verbose=args.verbose)
    print(f"twin_replay 监听 http://{args.host}:{args.port}", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        storage.close()


def cmd_import(args: argparse.Namespace) -> None:
    storage, svc = _open(args.db)
    with open(args.file, "rb") as fh:
        payload = fh.read()
    _print(svc.import_batch(payload, batch_id=args.batch))
    storage.close()


def cmd_state(args: argparse.Namespace) -> None:
    storage, svc = _open(args.db)
    _print(svc.current_state(args.device))
    storage.close()


def cmd_view(args: argparse.Namespace) -> None:
    storage, svc = _open(args.db)
    _print(svc.view(args.device, args.as_of, mode=args.mode))
    storage.close()


def cmd_confirm(args: argparse.Namespace) -> None:
    storage, svc = _open(args.db)
    _print(svc.confirm(args.device, args.as_of))
    storage.close()


def cmd_verify(args: argparse.Namespace) -> None:
    storage, svc = _open(args.db)
    _print(svc.verify_seal(args.device, args.as_of))
    storage.close()


def cmd_threshold(args: argparse.Namespace) -> None:
    storage, svc = _open(args.db)
    svc.set_threshold(args.device, args.metric, args.min, args.max)
    print("ok")
    storage.close()


def cmd_replay(args: argparse.Namespace) -> None:
    storage, svc = _open(args.db)
    if args.replay_action == "create":
        _print(svc.create_replay(args.device, args.as_of, mode=args.mode,
                                 job_id=args.job_id))
    elif args.replay_action == "run":
        svc.run_replay(args.job_id)
        _print(svc.get_replay(args.job_id))
    elif args.replay_action == "resume":
        jobs = svc.resume_unfinished()
        _print({"resumed": [j["job_id"] for j in jobs]})
    elif args.replay_action == "get":
        _print(svc.get_replay(args.job_id))
    elif args.replay_action == "list":
        _print({"jobs": svc.list_replays(args.status)})
    storage.close()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="twin_replay", description="风机遥测封存与回放系统")
    p.add_argument("--db", default="twin.db", help="SQLite 数据库路径")
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("demo", help="端到端故事线演示（含崩溃续跑）")
    sp.set_defaults(func=run_demo)

    sp = sub.add_parser("serve", help="启动 HTTP API 服务")
    sp.add_argument("--host", default="127.0.0.1")
    sp.add_argument("--port", type=int, default=8080)
    sp.add_argument("--verbose", action="store_true")
    sp.set_defaults(func=cmd_serve)

    sp = sub.add_parser("import", help="批量导入 JSON/JSONL 文件")
    sp.add_argument("file")
    sp.add_argument("--batch", default="")
    sp.set_defaults(func=cmd_import)

    sp = sub.add_parser("state", help="查询设备当前状态")
    sp.add_argument("device")
    sp.set_defaults(func=cmd_state)

    sp = add_as_of(sub.add_parser("view", help="按任意时间点重建视图"))
    sp.add_argument("--mode", choices=["revised", "confirmed"], default="revised")
    sp.set_defaults(func=cmd_view)

    sp = add_as_of(sub.add_parser("confirm", help="封存确认快照"))
    sp.set_defaults(func=cmd_confirm)

    sp = add_as_of(sub.add_parser("verify", help="封存快照哈希验真"))
    sp.set_defaults(func=cmd_verify)

    sp = sub.add_parser("threshold", help="设置指标正常区间")
    sp.add_argument("device")
    sp.add_argument("metric")
    sp.add_argument("--min", type=float, default=None)
    sp.add_argument("--max", type=float, default=None)
    sp.set_defaults(func=cmd_threshold)

    sp = sub.add_parser("replay", help="可恢复的重放任务")
    sp.add_argument("replay_action", choices=["create", "run", "resume", "get", "list"])
    sp.add_argument("job_id_or_device", nargs="?")
    sp.add_argument("--device")
    sp.add_argument("--as-of", dest="as_of")
    sp.add_argument("--mode", choices=["revised", "confirmed"], default="revised")
    sp.add_argument("--job-id", dest="job_id")
    sp.add_argument("--status")
    sp.set_defaults(func=cmd_replay)
    return p


def add_as_of(sp: argparse.ArgumentParser) -> argparse.ArgumentParser:
    sp.add_argument("device")
    sp.add_argument("--as-of", dest="as_of", required=True)
    return sp


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    # replay 子命令的位置参数兼容
    if getattr(args, "command", "") == "replay":
        token = args.job_id_or_device
        if args.replay_action == "create":
            if not args.device:
                args.device = token
        else:
            if not args.job_id:
                args.job_id = token
    try:
        args.func(args)
    except TwinReplayError as exc:
        print(f"错误: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
