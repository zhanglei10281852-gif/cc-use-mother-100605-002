from twin_replay.core import Telemetry, summarize


if __name__ == "__main__":
    record = Telemetry.create("demo", "v1", "draft", "operator", {"场景": "接收遥测事件并生成设备摘要"})
    print(summarize(record))

