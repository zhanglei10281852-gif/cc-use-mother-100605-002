# 设备状态回放

这是一个用于遥测事件登记与状态摘要的 Python 后端起始项目。当前版本提供不可变记录、稳定摘要和命令行冒烟入口，后续业务流程通过模块边界继续扩展。

测试命令：

    PYTHONPATH=src python3 -m unittest discover -s tests -v

编译检查：

    python3 -m compileall -q src tests run_cli.py

命令行示例：

    python3 run_cli.py

