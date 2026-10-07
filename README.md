# 风机设备状态回放系统（twin_replay）

接收带时间戳与设备时钟偏差的风机遥测事件，维护设备当前状态、异常窗口与可重放的
历史快照，让值班员按**任意时间点**重建当时的设备视图。迟到的补偿数据、重复上报、
设备更换、模型版本切换与部分批次损坏都不会改写已经确认的快照；回放结果逐字段标注
数据来自原始上报还是补偿数据。

## 设计要点

| 需求 | 实现方式 |
| --- | --- |
| 设备时钟偏差 | `标准时间 = ts_ms − clock_skew_ms`（skew 即“设备时钟 − 标准时钟”） |
| 确认快照 | 固定网格（默认 60s）+ 宽限水位线（默认 5s）。窗口 `[B, B+grid)` 在水位线越过 `B+grid` 时封存；快照内容含 SHA-256 摘要，且指向前一快照摘要形成哈希链 |
| 迟到事件 | 到达时事件时间已在水位线之前 → 写入独立的 `compensation.jsonl`，**永不回写快照**；快照文件字节级保持不变 |
| 回放 | 选取早于最早补偿事件的最近快照为检查点，把原始事件与补偿事件**按事件时间合并折叠**，字段标注 `original` / `compensation` |
| 重复上报 | `event_id`（或 `device_id:seq`）幂等去重；同键内容冲突进死信，拒绝覆盖 |
| 部分批次损坏 | 逐行校验，坏行进 `deadletter.jsonl`，同批其它记录照常入库 |
| 设备更换 | `replacement` 事件开启新纪元：关闭全部在途报警（结束原因 `replacement`）、清空现场状态、登记新序列号与模型版本，历史窗口完整保留 |
| 模型版本 | 字段值携带采样时的 `model_version`，切换后旧时间点仍显示旧版本 |
| 重启续跑 | 事件/补偿/死信/任务全部为只增 JSONL（追加即 fsync），快照原子替换；启动时重建状态、校验哈希链，`running` 任务自动重新认领 |

数据目录结构：

```
<data_dir>/
├── events.jsonl          # 已接受的原始事件（只增）
├── compensation.jsonl    # 迟到补偿事件（只增，与原始日志物理隔离）
├── deadletter.jsonl      # 坏行 / 冲突记录及原因
├── replay_tasks.jsonl    # 重放任务状态流（pending→running→completed/failed）
└── snapshots/<设备>__<边界ms>.json   # 确认快照（原子写入，哈希链）
```

## 事件格式

```json
{
  "event_id": "evt-0001",
  "device_id": "WT-A01",
  "event_type": "telemetry",
  "ts_ms": 1700000000000,
  "clock_skew_ms": 300,
  "seq": 42,
  "unit_serial": "SN-9001",
  "model_version": "v2",
  "fields": {"温度": 88.5, "负载": 91.4, "转速": 1450}
}
```

* 时间也可用 `"device_ts": "2026-10-07T10:00:00.300Z"`（ISO-8601）。
* `event_type`：`telemetry`（默认）/ `alarm`（带 `{"code","active"}`）/
  `replacement`（带 `new_serial`、可选 `new_model_version`）。
* 无 `event_id` 时以整数 `seq` 构成幂等键 `device_id:seq`。

## 快速开始

```bash
# 离线全流程演示（封存、补传、换机、崩溃续跑）
PYTHONPATH=src python3 -m twin_replay.cli demo

# 测试 / 编译
PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 -m compileall -q src tests run_cli.py

# HTTP 服务
PYTHONPATH=src python3 -m twin_replay.cli --data-dir ./data serve --port 8080
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/events` 或 `/import` | 批量导入，体为 JSON 数组或 `{"events":[...]}`；加 `?format=jsonl` 表示 JSON Lines（坏行进死信） |
| GET | `/devices` | 设备清单、水位线、快照数、缓冲与补偿计数 |
| GET | `/devices/{id}/status?as_of=<ms或ISO>` | 任意时间点视图（缺省为最新） |
| GET | `/devices/{id}/snapshots` | 确认快照清单 |
| GET | `/deadletters` | 死信及拒收原因 |
| POST | `/replays` | 创建重放任务 `{"device_id","as_of_ms","label"}` |
| POST | `/replays/run` · `/replays/{id}/run` | 执行全部待办 / 单个任务 |
| GET | `/replays` · `/replays/{id}` | 任务列表 / 任务详情（含结果） |
| GET | `/health` | 健康检查 |

导入响应示例：

```json
{"received": 8, "accepted": 4, "duplicate": 1, "conflict": 1,
 "late_compensation": 1, "rejected": 2}
```

状态视图中每个字段形如：

```json
{"value": 69.0, "source": "compensation", "updated_ms": 1700000006000,
 "model_version": "v1", "seq": 0}
```

并附带 `alarm_windows`（开始/结束时间、来源与结束原因 `cleared`/`replacement`）、
`unit_history` 与 `compensation` 汇总（`marked_fields` 即来自补偿数据的字段）。

## 命令行

```bash
python -m twin_replay.cli import examples/telemetry_sample.jsonl
python -m twin_replay.cli devices
python -m twin_replay.cli status WT-A01 --at 2026-10-07T10:00:00Z
python -m twin_replay.cli snapshots WT-A01
python -m twin_replay.cli deadletters
python -m twin_replay.cli replays create WT-A01 --at 1700000008000 --label 事故复盘
python -m twin_replay.cli replays run
```

全局参数 `--data-dir`（或环境变量 `TWIN_DATA_DIR`，默认 `./.twin_data`）须放在子命令前。

## 崩溃与篡改行为

* 进程在任务 `running` 时被杀死 → 重启后任务回到 `pending` 并带说明，
  由 `run_pending` / `/replays/run` 续跑；已完成任务不重放。
* 快照文件被修改或删除导致哈希链断裂 → 启动直接报 `CorruptSnapshotError` 并中止，
  不会用损坏数据对外服务。
