# 设备状态回放（twin_replay）

面向远程运维中心的风机遥测服务端：接收带时间戳与**设备时钟偏差**的遥测事件，
维护设备当前状态、异常窗口和可重放的历史快照，值班员可按**任意时间点**重建
当时的设备视图。仅使用 Python 3.11 标准库（HTTP 服务 + SQLite），无第三方依赖。

## 它解决什么问题

报警发生后现场只能看到"最后一条状态"。本系统让一次超温报警可以被完整复盘：
温度、负载、工艺档位是怎样**一起变化**的；迟到的修正数据如何被隔离为补偿、
而不会篡改已经确认的结论。

## 核心概念

| 概念 | 说明 |
| --- | --- |
| 时钟校正 | `修正时间 = device_time − clock_offset`（秒，正偏差=设备时钟偏快）。事件按修正时间排序，迟到事件按其真实发生时刻归位。 |
| 封存确认（seal） | 对时间点 T 之前的**按时数据**折叠成快照，计算 SHA-256 指纹与异常窗口一并固化，水位线只进不退。 |
| 补偿（compensation） | 封存之后才到达、且修正时间落在封存区间内的事件自动进入补偿通道。它**永远不能改写确认快照**。 |
| 字段血缘 | 修订视图中每个字段格标注来源：`confirmed`（已封存）/ `realtime`（封存后按时到达）/ `compensated`（迟到补偿）。 |
| 纪元（epoch） | 设备更换（`unit_sn` 变化）为硬边界，清空旧状态；模型版本切换为软边界，保留实测值但关闭旧窗口。状态不跨纪元串值。 |
| 异常窗口 | 数值指标按配置的正常区间判异，连续异常形成带起止与峰值的窗口；封存时冻结，补偿数据产生的修订窗口单独标记。 |
| 可恢复重放 | 重放任务冻结事件序列（seq 列表）、参数与封存水位线，周期性落检查点；进程重启后从断点继续，结果与一次跑完完全一致。 |

### 两种视图

- `confirmed`：值班员当时看到什么。排除一切补偿数据；封存快照可哈希验真。
- `revised`（默认）：事后掌握全部数据时的修订视图。补偿值生效并逐字段标注，
  受影响的指标（`compensated_metrics`）与事件（`compensation_events`）单独列出。

## 快速开始

```bash
# 端到端故事线演示（升温→超温→迟到补偿→设备更换→模型切换→崩溃续跑）
PYTHONPATH=src python3 -m twin_replay.cli --db demo.db demo

# 启动 HTTP 服务（启动时自动续跑未完成的重放任务）
PYTHONPATH=src python3 -m twin_replay.cli --db demo.db serve --port 8080

# 离线命令
PYTHONPATH=src python3 -m twin_replay.cli --db demo.db state WT-01
PYTHONPATH=src python3 -m twin_replay.cli --db demo.db view WT-01 \
    --as-of 2026-10-07T10:05:00Z --mode revised
PYTHONPATH=src python3 -m twin_replay.cli --db demo.db confirm WT-01 \
    --as-of 2026-10-07T10:05:00Z
```

测试与编译检查：

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
python3 -m compileall -q src tests run_cli.py
```

## HTTP API

```
GET  /devices                                  设备列表
POST /batches?batch_id=b1                      批量导入（JSON 数组或 JSONL；部分损坏返回 207）
GET  /batches                                  批次接收/隔离统计
POST /devices/{id}/thresholds                  {"metric":"温度","min":0,"max":80}
POST /devices/{id}/confirm                     {"as_of":"2026-10-07T10:05:00Z"}
GET  /devices/{id}/seal                        封存快照哈希验真（?as_of= 可指定封存点）
GET  /devices/{id}/state                       当前状态
GET  /devices/{id}/view?as_of=...&mode=...     任意时间点重建（revised|confirmed）
GET  /devices/{id}/windows?seal_as_of=...      封存固化的异常窗口
POST /replays                                  创建重放任务（冻结事件集合）
POST /replays/{job_id}/run                     执行/断点继续
POST /replays/resume                           继续所有未完成任务（服务启动时自动调用）
GET  /replays[?status=...] / GET /replays/{id}
```

事件示例：

```json
{
  "event_id": "evt-1",
  "device_id": "WT-01",
  "device_time": "2026-10-07T10:00:30Z",
  "clock_offset": 30.0,
  "metrics": {"温度": 81.2, "负载": 0.83, "工艺档位": "稳燃"},
  "unit_sn": "SN-A",
  "model_version": "m-v3",
  "batch_id": "b-001",
  "checksum": "可选：sha256(event_id|device_time|规范化metrics)"
}
```

设备更换与模型切换使用 `"kind": "device_replaced"`（metrics 带 `new_unit_sn`）
与 `"kind": "model_switched"`（metrics 带 `new_model_version`）。遥测事件中
`unit_sn` 直接变化也会被识别为隐式设备更换。

## 关键不变量

1. **事件不可变、幂等**：`event_id` 唯一，重复上报（批内/跨批）只计数不入库。
2. **封存不可变**：`sealed_snapshots` 与封存窗口只插入不更新；篡改库中原文会使
   `/seal` 的 SHA-256 验真失败；水位线回退返回 409。
3. **迟到不串改历史**：补偿事件与确认事件物理同表、逻辑隔离（`is_compensation`）。
4. **部分损坏不拖垮批次**：坏行进 `corrupted` 隔离区（位置、原因、原文摘要），
   好行照常入库，批次统计持久化。
5. **纪元隔离**：换设备后旧指标消失；切模型后旧异常窗口关闭。
6. **重放确定性**：任务冻结 seq 列表、参数和当时的封存水位线；续跑结果与
   `view()` 直接重算逐字节一致（有测试覆盖）。

## 代码结构

```
src/twin_replay/
  clock.py       # 设备时钟偏差校正与时间归一化
  parsing.py     # 事件解析、校验和、批次部分损坏隔离
  models.py      # Event / FieldCell / AnomalyWindow / Snapshot 与指纹
  engine.py      # 折叠状态机：异常检测、纪元边界、检查点序列化
  storage.py     # SQLite：事件/封存/窗口/批次/任务/阈值
  service.py     # 摄入分类、封存验真、两种视图、可恢复重放
  api.py         # 标准库 ThreadingHTTPServer JSON API
  cli.py         # 离线命令行 + 端到端演示
```

## 存储

单文件 SQLite（默认 WAL）。事件按 `(device_id, corrected_ts, seq)` 索引；
同修正时间的事件以接收序号 `seq` 决胜，保证折叠顺序确定。重放任务的检查点
保存在 `replay_jobs.checkpoint_json`，崩溃残留的 `running` 与未开始的
`pending` 任务都会被 `resume` 拾取。
