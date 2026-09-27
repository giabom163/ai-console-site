# gaokzx 文库采集流水线

主流程：`pipeline.py`（八阶段主控）/ `gaokzx_download.py`（采集下载）/ `batchcore.py`（批次与状态表）。
说明文档：`工作流架构说明.md`。

## 备份命名约定

pipeline.db 备份统一格式：pipeline.db.bak_<用途>_<YYYYMMDD_HHMMSS>
示例：
  pipeline.db.bak_offsetfix_20260926_225524
  pipeline.db.bak_cleanfix_20260927_103015
用途标识：offsetfix / cleanfix / migration / packmerge / layout ...

## 最简全流程测试

命令（--batch-id 换成本地任一存在的历史批次）：
  python pipeline.py --auto --start-stage pack --batch-id <bid> \
    --test-root D:\work\_test_min --chunk 0

前置：_test_min 内需有 pipeline.db 副本、_批次\（manifest）与打包源阶段目录的对应专题子目录
（resolve 按四阶段根找文件，缺一阶段则该文件不入包）。

用途：不联网、不跑 OCR、GPU 零占用，分钟级验证整专题打包链路（pack -> accept）。
核心语义：GAOKZX_PACK_MODE=topic（默认）下中段续跑单批就地打包也按 run 合并（一个
专题一个 <topic>.zip，run 内全部批次 step7 dst 指向同一 zip）。
回退：GAOKZX_PACK_MODE=batch 即旧行为（每批一 zip，含 _rN 分片，pack 前自动 QC）。

## 阶段 2 已完成（整专题合并单包）

commit `8f2ce58` + `b696242`。

- 新增 `GAOKZX_PACK_MODE` 开关（默认 `topic`；`=batch` 回退旧行为）
- `step_pack` 改为分派器：topic → `step_pack_topic`、batch/单文件 → `step_pack_legacy`
- `step_pack_topic`：枚举 run 内所有批次 → 一专题一 `<topic>.zip`
- `_batch_zips_exist` 支持 run 级回退
- `verify_run_completeness` topic 模式按 run 级去重计数
- `_LAST_PACK_ZIPS` 多专题全校验

验证：
- 离线单测 `test_topic_pack.py` 22/22
- 隔离端到端 `--chunk 0` topic/batch 双模式通过
- 联网端到端 `--chunk 24`（阶段 A）：待用户执行后回写结果

回退：`GAOKZX_PACK_MODE=batch` + `git revert 8f2ce58 b696242`

## 待办（下轮独立议题）

- 问题 2：打包目录化命名（zip 内按 区/学校 层级重组 + 存量迁移 step_migrate_layout）；
  当前 topic zip 内保持打包源现有层级（<专题>/<子目录>/<文件>，即扁平命名现状）。
