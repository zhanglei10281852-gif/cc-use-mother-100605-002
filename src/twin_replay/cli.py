"""离线命令行：批量导入、状态查询、快照/死信查看、重放任务与全流程演示。

用法：
    python -m twin_replay.cli demo [--data-dir DIR]
    python -m twin_replay.cli import FILE.json|FILE.jsonl
    python -m twin_replay.cli serve [--port 8080]
    python -m twin_replay.cli devices
    python -m twin_replay.cli status DEVICE [--at 2026-10-07T10:00:00Z]
    python -m twin_replay.cli snapshots DEVICE
    python -m twin_replay.cli deadletters
    python -m twin_replay.cli replays [list|create DEVICE --at TS|run [ID]]
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys

from .engine import DEFAULT_GRACE_MS, DEFAULT_GRID_MS, CorruptSnapshotError, ReplayEngine
from .model import EventParseError, iso, parse_ts_ms
from .service import serve


def _open_engine(data_dir: str, grid_ms: int = DEFAULT_GRID_MS, grace_ms: int = DEFAULT_GRACE_MS) -> ReplayEngine:
    try:
        return ReplayEngine(data_dir, grid_ms=grid_ms, grace_ms=grace_ms)
    except CorruptSnapshotError as exc:
        raise SystemExit(f"启动中止：{exc}") from exc


def _print(payload) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _load_file(path: str) -> list:
    with open(path, "r", encoding="utf-8") as fh:
        text = fh.read()
    if path.endswith(".jsonl"):
        rows = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                rows.append(line)  # 坏行进死信
        return rows
    payload = json.loads(text)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and isinstance(payload.get("events"), list):
        return payload["events"]
    raise SystemExit("JSON 文件应为数组或 {\"events\": [...]}")


# ------------------------------------------------------------------ 演示场景
def run_demo(args) -> None:
    data_dir = os.path.abspath(args.data_dir)
    if os.path.exists(data_dir) and os.listdir(data_dir) and not args.keep:
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    GRID, GRACE = 10_000, 2_000
    T = 1_700_000_000_000  # 固定基准时间，演示可重复

    def ev(event_id, dt, **rest):
        row = {"event_id": event_id, "device_id": "WT-A01", "ts_ms": T + dt, **rest}
        return row

    print("=" * 72)
    print("步骤 1  进程启动，批量导入正常遥测与报警（含时钟偏差修正）")
    print("=" * 72)
    engine = _open_engine(data_dir, GRID, GRACE)
    batch1 = [
        ev("e1", 0, clock_skew_ms=300, model_version="v1",
           fields={"温度": 71.2, "负载": 80.1, "转速": 1450}),
        ev("e2", 5_000, clock_skew_ms=300, model_version="v1",
           fields={"温度": 72.0}),
        ev("e3", 12_000, event_type="alarm", clock_skew_ms=300,
           alarm={"code": "E103", "active": True}),
        ev("e4", 15_000, clock_skew_ms=300, model_version="v1",
           fields={"温度": 88.5, "负载": 91.4}),
        # —— 以下四条是“问题数据” ——
        "这一行根本不是JSON",                                   # 坏批次行
        {"event_id": "eX", "device_id": "WT-A01"},             # 缺时间戳
        ev("e1", 0, clock_skew_ms=300, model_version="v1",
           fields={"温度": 71.2, "负载": 80.1, "转速": 1450}),  # 重复上报
        ev("e1", 0, clock_skew_ms=300, model_version="v1",
           fields={"温度": 99.9}),                             # 同号不同内容
    ]
    report1 = engine.ingest(batch1, source="batch-20261007", now=T + 20_000)
    _print(report1)

    print("\n" + "=" * 72)
    print("步骤 2  后续遥测推进水位线，触发确认快照封存")
    print("=" * 72)
    report2 = engine.ingest([
        ev("e5", 30_000, clock_skew_ms=300, model_version="v1",
           fields={"温度": 90.1, "负载": 85.0}),
    ], source="batch-20261007", now=T + 32_000)
    _print(report2)
    _print(engine.list_snapshots("WT-A01"))
    digest_before = engine._read_snapshot(
        engine.storage.snapshot_path("WT-A01", T + 10_000))["digest"]
    print(f"快照 T+10s 封存摘要: {digest_before[:24]}…")

    print("\n" + "=" * 72)
    print("步骤 3  迟到 24 秒的温度修正事件 → 进入补偿日志，不回写快照")
    print("=" * 72)
    report3 = engine.ingest([
        ev("e-late", 6_000, clock_skew_ms=300, model_version="v1",
           fields={"温度": 69.0}),
    ], source="batch-补传", now=T + 34_000)
    _print(report3)
    digest_after = engine._read_snapshot(
        engine.storage.snapshot_path("WT-A01", T + 10_000))["digest"]
    print(f"同一快照摘要: {digest_after[:24]}…")
    print("快照是否保持不变:", digest_before == digest_after)

    print("\n" + "=" * 72)
    print("步骤 4  报警解除、模型版本切换、设备更换（新纪元）")
    print("=" * 72)
    _print(engine.ingest([
        ev("e6", 40_000, event_type="alarm", clock_skew_ms=300,
           alarm={"code": "E103", "active": False}),
        ev("e6b", 45_000, clock_skew_ms=-500, model_version="v2",
           fields={"温度": 65.3, "桨距角": 4.2}),
        ev("e7", 60_000, event_type="replacement", clock_skew_ms=-500,
           new_serial="SN-9002", new_model_version="v3"),
        ev("e8", 65_000, clock_skew_ms=-500, model_version="v3",
           fields={"温度": 55.0, "负载": 30.0}),
    ], source="运维单据", now=T + 70_000))

    print("\n" + "=" * 72)
    print("步骤 5  值班员创建四个时间点的重放任务，只运行第一个（模拟中断）")
    print("=" * 72)
    for label, dt in [("补传时刻", 8_000), ("报警升温期", 16_000),
                      ("降温切换期", 46_000), ("换机之后", 66_000)]:
        task = engine.create_replay("WT-A01", T + dt, label=label, now=T + 70_000)
        print(f"已入队 {task['id']}  {label}  as_of={iso(T + dt)}")
    first = engine.run_pending(limit=1)[0]
    print(f"已完成: {first['label']} -> {first['status']}")

    print("\n" + "=" * 72)
    print("步骤 6  进程重启：丢弃内存中的引擎，用同一数据目录重新打开")
    print("=" * 72)
    del engine
    engine = _open_engine(data_dir, GRID, GRACE)
    tasks = engine.list_tasks()
    print(f"恢复任务 {len(tasks)} 个，状态:",
          {t["label"]: t["status"] for t in tasks})
    finished = engine.run_pending()
    print("重启后续跑完成:", [(t["label"], t["status"]) for t in finished])

    print("\n" + "=" * 72)
    print("步骤 7  查看“补传时刻”视图（注意温度字段的补偿标记）")
    print("=" * 72)
    hot = next(t for t in engine.list_tasks() if t["label"] == "补传时刻")
    result = hot["result"]
    print(f"设备: {result['device_id']}  视图时间: {result['as_of']}")
    print(f"物理机序列号: {result['unit_serial']}（来源 {result['unit_serial_source']}）"
          f"  模型版本: {result['model_version']}")
    print("字段视图:")
    for name, f in result["fields"].items():
        mark = "  <== 补偿数据" if f["source"] == "compensation" else ""
        print(f"  {name:<6} = {f['value']:<6} 来源={f['source']:<12}"
              f" 采样={iso(f['updated_ms'])} 模型={f['model_version']}{mark}")
    print("报警窗口:")
    for w in result["alarm_windows"]:
        end = iso(w["end_ms"]) if w["end_ms"] else "（仍在持续）"
        print(f"  {w['code']}  {iso(w['start_ms'])} ~ {end}"
              f"  开始来源={w['start_source']} 结束来源={w['end_source']}")
    print("补偿汇总:", json.dumps(result["compensation"], ensure_ascii=False))

    print("\n" + "=" * 72)
    print("步骤 8  换机之后视图与当前状态、死信队列")
    print("=" * 72)
    new_view = engine.status("WT-A01", T + 66_000)
    print("换机后序列号:", new_view["unit_serial"],
          "模型:", new_view["model_version"], "字段:", list(new_view["fields"]))
    print("序列号沿革:")
    for h in new_view["unit_history"]:
        print(f"  {h['unit_serial']} / {h['model_version']} 自 {iso(h['since_ms'])} 起（来源 {h['source']}）")
    dead = engine.deadletters()
    print(f"死信 {len(dead)} 条:")
    for d in dead:
        print("  -", d["reason"])

    print("\n演示完成。数据目录:", data_dir)
    print("可继续执行：")
    print(f"  PYTHONPATH=src python3 -m twin_replay.cli devices --data-dir {data_dir}")
    print(f"  PYTHONPATH=src python3 -m twin_replay.cli status WT-A01 --data-dir {data_dir}")


# ------------------------------------------------------------------ 参数解析
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="twin_replay", description="风机设备状态回放系统")
    p.add_argument("--data-dir", default=os.environ.get("TWIN_DATA_DIR", ".twin_data"))
    sub = p.add_subparsers(dest="command", required=True)

    sp = sub.add_parser("demo", help="运行离线全流程演示")
    sp.add_argument("--keep", action="store_true", help="保留数据目录中已有内容")
    sp.set_defaults(func=run_demo)

    sp = sub.add_parser("import", help="从 .json/.jsonl 批量导入")
    sp.add_argument("file")
    sp.set_defaults(func=cmd_import)

    sp = sub.add_parser("serve", help="启动 HTTP 服务")
    sp.add_argument("--host", default="127.0.0.1")
    sp.add_argument("--port", type=int, default=8080)
    sp.add_argument("--grid-ms", type=int, default=DEFAULT_GRID_MS)
    sp.add_argument("--grace-ms", type=int, default=DEFAULT_GRACE_MS)
    sp.set_defaults(func=cmd_serve)

    sp = sub.add_parser("devices")
    sp.set_defaults(func=lambda a: _print({"devices": _open_engine(a.data_dir).list_devices()}))

    sp = sub.add_parser("status")
    sp.add_argument("device")
    sp.add_argument("--at", default=None, help="ISO 时间或纪元毫秒，缺省为当前")
    sp.set_defaults(func=cmd_status)

    sp = sub.add_parser("snapshots")
    sp.add_argument("device")
    sp.set_defaults(func=lambda a: _print({"snapshots": _open_engine(a.data_dir).list_snapshots(a.device)}))

    sp = sub.add_parser("deadletters")
    sp.set_defaults(func=lambda a: _print({"deadletters": _open_engine(a.data_dir).deadletters()}))

    sp = sub.add_parser("replays")
    sp.add_argument("action", nargs="?", default="list", choices=["list", "create", "run"])
    sp.add_argument("device_or_id", nargs="?", default=None)
    sp.add_argument("--at", default=None)
    sp.add_argument("--label", default="")
    sp.set_defaults(func=cmd_replays)
    return p


def cmd_import(args) -> None:
    rows = _load_file(args.file)
    engine = _open_engine(args.data_dir)
    _print(engine.ingest(rows, source=os.path.basename(args.file)))


def cmd_serve(args) -> None:
    serve(args.host, args.port, args.data_dir, grid_ms=args.grid_ms, grace_ms=args.grace_ms)


def cmd_status(args) -> None:
    engine = _open_engine(args.data_dir)
    as_of_ms = parse_ts_ms(args.at) if args.at else None
    _print(engine.status(args.device, as_of_ms))


def cmd_replays(args) -> None:
    engine = _open_engine(args.data_dir)
    if args.action == "list":
        _print({"tasks": engine.list_tasks()})
    elif args.action == "create":
        if not args.device_or_id or args.at is None:
            raise SystemExit("用法: replays create DEVICE --at TS")
        task = engine.create_replay(args.device_or_id, parse_ts_ms(args.at), args.label)
        _print(task)
    else:
        if args.device_or_id:
            _print(engine.run_task(args.device_or_id))
        else:
            _print({"executed": engine.run_pending()})


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
