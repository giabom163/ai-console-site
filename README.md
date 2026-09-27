# gaokzx 文库采集流水线

主流程：`pipeline.py`（八阶段主控）/ `gaokzx_download.py`（采集下载）/ `batchcore.py`（批次与状态表）。
说明文档：`工作流架构说明.md`。

## 备份命名约定

pipeline.db 备份统一格式：pipeline.db.bak_<用途>_<YYYYMMDD_HHMMSS>
示例：
  pipeline.db.bak_offsetfix_20260926_225524
  pipeline.db.bak_cleanfix_20260927_103015
用途标识：offsetfix / cleanfix / migration / packmerge / layout ...
