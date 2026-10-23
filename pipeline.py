#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
文库数据采集 · 统一主控入口（pipeline.py）—— 2026-10-21 拆分后只保留入口职责。

原为单脚本全流程（整合 grabnew / downtopic / checkDonwloadedAndRename / ArrangePdfNames /
batch_remove_watermark / BatchOcrPdfAddTextLayer / checkZeroSizeFile / check_pdf_integrity），
**绝不自动全量跑**。无参数运行 → 交互式向导；命令见 --help 示例段。

流水线（STEP_* 常量；菜单/向导/日志/pipeline.db 全部引用）：
  1 download  : 专题页 -> 详情页 -> 下载 PDF（清单/断点续传）；空结果 rc=2 -> 全流程立即中止
  2 watermark : 去水印 —— 图层水印无损删除（保留文字层）；像素水印局部块修补（保文字层，超阈值才整页盖图）
  3 rename    : 规范重命名（年份·区·学校·年级·月份·考试·学科·答案）；重名自动 _1/_2 尾缀，绝不覆盖
  4 ocr       : 给"去水印后"成品补透明文字层（PaddleOCR 页级 OCR：GPU 优先 / CPU 物理核预算 / 页级缓存）
  5 qc        : 质控 —— 零字节 / PDF 完整性 / 无文字层，出报告
  6 ads       : 隐形水印清理 —— 重扫描本批次 -> reports/bottom_ads_<batch_id>.json ->「已清广告」（报告缺失/过旧/batch_id 不符即中止）
  7 pack      : 打包 zip 到 打包文件/（源默认「已清广告」，打包前自动 QC）

模块拆分（2026-10-21，三批合并执行完毕）：实现全部迁出，本文件只保留解释器守卫、
CLI 参数注册（argparse）、--step 分发与 main 入口，顶部显式导回全部迁移符号
（含下划线名），调用方与 wm_worker.py 均零改动：
  pcore     公共核心（初始化/DB/日志/锁/交互/依赖自检/has_targets）
  prename   文件整理重命名        pwm       去水印
  pdownload 嗅探与下载调度        pocr      OCR 补层与质控
  ppack     打包与验收            padmin    数据维护与状态
  pflow     交互菜单与自动编排

批次机制（每次任务必建）：batch_id + manifest（文库资源/_批次/batch_<batch_id>.json）；步骤2~7 只处理本批
manifest 内文件（scan_pdfs 统一入口），禁全目录扫描；_测试/验收测试 目录的文件绝不当生产文件处理。

四阶段目录（唯一真相源：pcore.STAGES）—— 待处理 D:\work\文库资源（下载落地，可能有水印）；
  去水印后 D:\work\文库资源_去水印后（像素级成品，文字层可能已丢）；已处理 D:\work\文库资源_已处理
  （补完文字层/重命名后的最终件）；打包文件 D:\work\文库资源_打包文件（待上传压缩包）。

设计要点：SQLite 状态表（pipeline.db）记每文件各步骤状态 → 增量只处理新增/失败；下载幂等（清单续传）；
  OCR 用"是否已有文字层"兜底去重；进度条三级降级（rich→tqdm→纯文本）；Ctrl+C 中断：第一次只设标志
  （当前文件收尾、状态落库），第二次强制退出、下次续传；长任务（水印/OCR）跑独立子进程隔离 C 层崩溃。
"""
import argparse
import os
import subprocess
import sys
from pathlib import Path


# ==== 解释器守卫（最高优先级，必须在任何重依赖 import 之前）====
# PATH 上的 python 常是裸运行时（没装包）：自动改用正确 venv 重新 exec 本脚本（WM_NO_REEXEC=1 关闭）。
def maybe_reexec_with_venv():
    """若当前解释器不是项目 venv，则改用 venv 重新 exec。返回 True 表示已重启。"""
    if os.environ.get("WM_NO_REEXEC") == "1": return False
    if not PY.exists():
        return False                       # venv 不存在 -> 不折腾，让后面的自检去处理
    if _norm_py(sys.executable) == _norm_py(PY):
        return False                       # 已经在正确解释器里
    # 关键：错误解释器下连基本包都没有，必须先重启再谈别的
    print(f"[env] 当前解释器缺少项目依赖，自动切换到项目环境重启：")
    print(f"      当前  {sys.executable}")
    print(f"      应为  {PY}")
    print(f"      （提示：用 gaokzx\\run.cmd 启动可跳过本切换）")
    sys.stdout.flush()
    os.environ["WM_NO_REEXEC"] = "1"       # 防止无限重启
    try:
        # 用 os.execv 原地替换进程：Ctrl+C / 退出码 / 参数都自然传递
        os.execv(str(PY), [str(PY), os.path.abspath(__file__)] + sys.argv[1:])
    except OSError:
        # execv 失败（极少见）-> 退回子进程方式，透传退出码
        rc = subprocess.call([str(PY), os.path.abspath(__file__)] + sys.argv[1:])
        raise SystemExit(rc)
    return True

from pcore import (
    PY,
    ROOT,
    STAGE_ADS,
    STAGE_CLEANED,
    STAGE_PACKED,
    _norm_py,
    bc,
    db,
    ensure_dirs,
    ledgerio,
    run_step_safe,
    step_check_env,
)

maybe_reexec_with_venv()

# ==================================================================
# 模块拆分（2026-10-21，批次2-4 合并执行完毕）：各 stage 实现已迁出，
# 此处显式导回保持调用方（run.cmd / 看板 / 计划任务 / wm_worker.py）零改动。
# 依赖分层：batchcore -> pcore -> pdownload/pocr/pwm/prename/ppack/padmin -> pflow
# 可变全局（DB / ONLY_FILE_IDS / ONLY_SUBDIR / AUTO_MODE / _LAST_PACK_ZIP(S) /
# FORCE_MISSING）一律经所属模块属性访问，禁止 from-import 快照。
# ==================================================================
import pcore
import ppack
import pflow
import pflowauto
from prename import (
    rollback_renames,
    step_rename,
)
from pwm import (
    _wm_process_one,          # wm_worker.py:29 经本文件 re-export 调用，勿删
    step_watermark,
)
from pdownload import (
    _handle_rc2,
    _handle_rc3,
    step_ads,
    step_discover_download,
)
from pocr import (
    OCR_WORKERS_DEFAULT,
    OCR_THREADS_PER_WORKER,
    OCR_WORKERS_HARD_CAP,
    step_clean_ocr_dirty,
    step_ocr,
    step_qc,
)
from ppack import (
    _final_zip_check,
    _parse_file_ids,
    reconcile_round,
    step_accept,
    step_fix_dst,
    step_pack,
)
from padmin import (
    step_cleanup,
    step_export_excel,
    step_migrate_done,
    step_prune_db,
)
from pflowauto import (
    run_auto,
)
from pflow import (
    run_wizard,
)


def main():
    ap = argparse.ArgumentParser(
        description="文库数据采集统一主控（单脚本全流程；无参数运行进入交互向导）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
常用示例（不加参数 = 交互向导）：
  python pipeline.py --step check-env [--install]     # 环境自检 / 自动安装缺失依赖
  python pipeline.py --ensure-dirs                    # 只创建四阶段目录后退出
  python pipeline.py --step download --topic 1 --limit 10
  python pipeline.py --step download --url "https://www.gaokzx.com/gk/shitiku/159032.html" --limit 3
  python pipeline.py --step all --url "https://www.gaokzx.com/gk/shitiku/159032.html" --force
  python pipeline.py --step watermark --dry-run       # 去水印（正式：加 --wm-workers 6 --limit 20）
  python pipeline.py --step watermark,rename,ocr,qc,pack --limit 3 --force   # 端到端验证前 3 个
  python pipeline.py --step clean-ocr-dirty [--dry-run]   # 跑 OCR 全量前清脏数据（只删记录不删 PDF）
  python pipeline.py --step fix-dst [--force]             # 还债：原地链步骤悬空 dst 回填当前权威路径（默认预览）
  python pipeline.py --step ocr --ocr-root "D:\\work\\文库资源_去水印后" --limit 8 --ocr-workers 4 --ocr-threads 1
  python pipeline.py --step rename [--rename-apply]   # 重命名（默认 dry-run 只出建议清单）
  python pipeline.py --step qc / --step pack          # 质控 / 打包（源默认「已清广告」，打包前自动 QC）
  python pipeline.py --step ads --dry-run --limit 20  # 隐形水印清理（自动重扫描本批次生成新报告）
  python pipeline.py --step export-excel              # 导出「文件处理状态总表.xlsx」（--step all 跑完自动执行）
  python pipeline.py --auto --url "https://www.gaokzx.com/gk/shitiku/159032.html"
  python pipeline.py --auto --start-stage ads --file-id F0001 --batch-id 20260918_161530_159755 --force
  python pipeline.py --step accept --file-id F0001    # 验收归档（移入 文库资源_completed）
""")
    ap.add_argument("--step", default=None,
                    help="不传则进入交互向导。可选：all|check-env|discover|download|clean-ocr-dirty|"
                         "watermark|rename|rollback-rename|ocr|qc|ads|pack|accept|export-excel|"
                         "migrate-done|prune-db|fix-dst|cleanup（可逗号组合，如 watermark,rename,ocr,qc,ads,pack）")
    ap.add_argument("--topic", default="all", help="下载专题：1 | 2 | all")
    ap.add_argument("--url", default=None, help="直接用专题页 URL 下载（替代 --topic）；专题目录名自动取页面 <title>，" "可用 --name 覆盖")
    ap.add_argument("--name", default=None, help="配合 --url：显式指定专题目录名")
    ap.add_argument("--paper-type", default=None,
                    help="按试卷类型筛选专题页表格行（如 北京卷；可逗号分隔多值）。"
                         "不传=抓全部（旧行为不变）；透传给下载器 --paper-type")
    ap.add_argument("--year-type", default=None,
                    help="按年份筛选专题页表格行（如 2026；可逗号分隔多值；匹配 col 列）。"
                         "不传=不筛选；透传给下载器 --year-type")
    ap.add_argument("--limit", type=int, default=None, help="本步只处理前 N 个文件")
    ap.add_argument("--ocr-root", default=None, help=f"OCR 根目录（默认 {STAGE_CLEANED}，即给去水印成品补文字层）")
    ap.add_argument("--qc-root", default=None, help=f"质控根目录（默认 {STAGE_CLEANED}）")
    ap.add_argument("--rename-root", default=None, help=f"重命名根目录（默认 {STAGE_CLEANED}）")
    ap.add_argument("--pack-root", default=None,
                    help=f"打包源目录（默认 {STAGE_ADS}「已清广告」；只有明确跳过隐形水印清理才允许用"
                         f"「{STAGE_CLEANED.name}」）")
    ap.add_argument("--pack-out", default=None, help=f"打包输出目录（默认 {STAGE_PACKED}）")
    ap.add_argument("--ads-report", default=None,
                    help="隐形水印清理（手工模式）：显式指定扫描报告 JSON；不传则在步骤内强制"
                         "重新扫描生成 reports/bottom_ads_<batch_id>.json")
    ap.add_argument("--ads-out", default=None, help=f"隐形水印清理：输出根目录（默认 {STAGE_ADS}）")
    ap.add_argument("--skip-ads", action="store_true", dest="skip_ads",
                    help="打包：明确跳过隐形水印清理（步骤6），允许打包源回退「去水印后」"
                         "（有警告：zip 可能含广告页）")
    ap.add_argument("--workers", type=int, default=6, help="下载并发数")
    ap.add_argument("--delay", type=float, default=0.5, help="下载间隔秒数（礼貌爬取）")
    ap.add_argument("--wm-workers", type=int, default=6, help="去水印并发子进程数（默认 6；1=串行）")
    ap.add_argument("--ocr-workers", type=int, default=OCR_WORKERS_DEFAULT,
                    help=f"OCR 并发子进程数（默认 {OCR_WORKERS_DEFAULT} = 自动：按物理核/内存/设备算）。"
                         f"[!] 上限 {OCR_WORKERS_HARD_CAP}：再高 16GB 内存会 OOM")
    ap.add_argument("--ocr-threads", "--threads", type=int, default=OCR_THREADS_PER_WORKER,
                    help=f"每个 OCR 子进程内部线程数（默认 {OCR_THREADS_PER_WORKER}=1；建议 1~2，"
                         "写入 OMP/MKL/OPENBLAS_NUM_THREADS 硬限制，防超订；--threads 为别名）")
    ap.add_argument("--ocr-batch", "--batch-size", type=int, default=4, dest="ocr_batch",
                    help="每个 OCR 子进程每批处理文件数（默认 4，摊薄模型加载；--batch-size 为别名，保守可设 1）")
    ap.add_argument("--device", choices=["auto", "cpu", "gpu", "cuda"], default=None,
                    help="OCR 计算设备（cuda 归一为 gpu）；不传=自动（有 N 卡走 GPU，总显存<8GB 自动回退 CPU）")
    ap.add_argument("--fp16", action="store_true",
                    help="OCR 半精度推理（仅 GPU，省显存降 OOM）；当前 PaddleOCR 不支持该参数时自动忽略")
    ap.add_argument("--src", default=None, help="去水印源目录（默认 待处理；压测时可指定）")
    ap.add_argument("--hardest", type=int, default=None, help="只处理源目录里最大的 N 个文件（压测最难样本）")
    ap.add_argument("--rename-apply", action="store_true", help="重命名：真的执行改名（不传则只出建议清单 dry-run）")
    ap.add_argument("--fix-conflicts-by-body", action="store_true", dest="fix_conflicts_by_body",
                    help="配合 --step rename --rename-apply：科目冲突文件按正文标题强制改名，"
                         "冲突逐条留档 _报告\\rename_conflicts.xlsx（仅单步可用，all/auto 不生效）")
    ap.add_argument("--rename-ocr", action="store_true", help="重命名：识别不出时用 OCR 兜底（慢）")
    ap.add_argument("--move", action="store_true", help="migrate-done：真删除源文件（默认只复制）")
    ap.add_argument("--force", "--force-recheck", action="store_true", dest="force",
                    help="忽略状态表/断点续传，强制重跑命中范围（端到端验证用）。"
                         "[!] 配合 --step all 时会连旧库一起重跑，建议加 --force-scope new")
    ap.add_argument("--force-scope", choices=["all", "new"], default="all",
                    help="--force 的作用范围：all=整库重跑（默认）；new=只强跑增量（产物不存在/"
                         "无文字层）的文件，保护旧库成果。跑新专题强烈建议 new。")
    ap.add_argument("--force-missing", action="store_true", dest="force_missing",
                    help="只强制重跑“DB=done 但磁盘产物缺失”的文件：先对本批次做 DB↔磁盘核对回滚缺失项，"
                         "其余完好文件照常跳过（不同于 --force 的全量重跑）。")
    ap.add_argument("--only-subdir", default=None,
                    help="只处理路径中含该子串的文件（如新专题目录名）。配合 --url 时若不指定，自动取"
                         "该专题的目录名 —— 这样 --step all 就只会处理新下载的这批，绝不动旧库。"
                         "传 'ALL' 可显式关闭该过滤（处理整库）。")
    ap.add_argument("--install", action="store_true", help="check-env：自动 pip 安装缺失依赖（默认只报告）")
    ap.add_argument("--dry-run", action="store_true", help="只打印将要做什么，不改动")
    ap.add_argument("--check", action="store_true", help="等价于 --step check-env（只做环境自检）")
    ap.add_argument("--ensure-dirs", action="store_true", help="只创建四阶段目录后退出")
    # ---- 常态化旧文件清理（--step cleanup，P2）----
    ap.add_argument("--before", default=None, help="cleanup：删除此日期（YYYY-MM-DD）之前（mtime）的旧文件（必填）")
    ap.add_argument("--roots", default=None,
                    help="cleanup：逗号分隔的扫描根目录（默认四阶段+已清广告）。可用别名 pending/cleaned/done/ads/packed 或中文目录名")
    ap.add_argument("--include-backup", action="store_true", dest="include_backup",
                    help="cleanup：同时扫描并整删 D:\\work 下 mtime 老的 *_backup_* 目录")
    ap.add_argument("--include-db-stale", action="store_true", dest="include_db_stale",
                    help="cleanup：连 B 类（DB 引用过的非活跃输入，started/skip/早期步）也删（默认不删；删除会断 DB 追溯链、不可恢复）")
    ap.add_argument("--include-db-ok", action="store_true", dest="include_db_ok",
                    help="cleanup：连 C 类（DB 里 step∈{4,5,6,7} 且 status=ok 的活跃产物）也删（默认不删，只删 A 孤立文件）")
    ap.add_argument("--apply", action="store_true",
                    help="cleanup：真正执行删除（不传=dry-run 只预览分类）；默认只删 A 类；真删后自动跑 prune-db")
    ap.add_argument("--auto", action="store_true",
                    help="无交互全自动：从 --start-stage（默认 download 从头）按 download->watermark->"
                         "rename->ocr->qc->ads->pack 顺序无人值守跑到打包，成功后标记「待验收」。"
                         "下载起点需配 --url 或 --topic。")
    ap.add_argument("--start-stage", default=None, dest="start_stage",
                    help="--auto 起点阶段：download|watermark|rename|ocr|qc|ads|pack（也接受中文 "
                         "去水印/重命名/质控/清广告/打包；默认 download=从头）")
    ap.add_argument("--test-root", default=None, dest="test_root",
                    help="隔离模式：DB/_批次/logs 全重定向到该目录（等价 GAOKZX_TEST_ROOT）。"
                         "用于端到端验证，绝不污染生产库。例：--test-root D:\\work\\_test")
    ap.add_argument("--chunk", type=int, default=24,
                    help="--auto 分批流水线：每批最多 N 个目标（默认 24，建议 20~30）；下载完一批立即接力"
                         "跑 去水印->重命名->OCR->质控->清广告->打包（pack 按轮增量，zip 名带 _rN）。"
                         "0 = 关闭分批（旧行为：全部下载后统一跑后续阶段）")
    ap.add_argument("--file-id", default=None, dest="file_id", help="只处理指定 file_id（逗号分隔可多个）——看板单文件调度/重跑使用")
    ap.add_argument("--batch-id", default=None, dest="batch_id", help="显式指定批次号（默认用 _批次/current.json 的当前批次）")
    ap.add_argument("--force-full-run", action="store_true", dest="force_full_run", default=False,
                    help="[机制1] --auto 收尾前做完整性断言：游标未到 total / 有下载文件未达验收步 / zip 数!=验收数 "
                         "-> 判「专题未跑完」，拒绝自动验收、保留 pending_accept、退出码 1。默认关闭（不加=行为完全不变）。")
    ap.add_argument("--ask-on-empty", action="store_true", dest="ask_on_empty", default=False,
                    help="[机制2a] --auto 下载返回空但游标未达 total 时强制询问是否中止（abort=退出1/skip=继续）。默认关。")
    ap.add_argument("--ask-on-stage-failure", action="store_true", dest="ask_on_stage_failure", default=False,
                    help="[机制2b] --auto 某阶段失败时强制询问中止/跳过后续。默认关（关=原样中止）。")
    ap.add_argument("--ask-on-resume", action="store_true", dest="ask_on_resume", default=False,
                    help="[机制2c] --auto 检测到上次未完成（游标<total）时询问是否续跑。默认关。")
    ap.add_argument("--force-rerun-failed", action="store_true", dest="force_rerun_failed", default=False,
                    help="[阶段3] --auto 即使本批新下载 0，只要有失败文件也继续重跑后续阶段重处理。默认关（关=原样跳过）。")
    # ---- 裸 URL 友好化（修 run.cmd 传参闪退）：parse_known_args 预检 unknown 里的 http(s) token，
    #      有则中文提示 + return 1（不 argparse sys.exit(2) 静默）；无则回落原 parse_args()，旧行为完全不变。----
    _pre, _unknown = ap.parse_known_args()
    if any(str(t).lower().startswith(("http://", "https://")) for t in _unknown):
        print(">>> [入口] 检测到裸 URL 参数：" + " ".join(str(t) for t in _unknown))
        print('>>> [入口] 本脚本不把 URL 当位置参数。请改用：--url "<链接>"（可配 --auto 或 --step download）。')
        print(">>> [入口] 或直接不带参数运行，进入交互菜单后选 [1] 输入网址。")
        return 1
    args = ap.parse_args()
    # ---- --test-root 双重校验：batchcore 模块加载时已固化 TEST_ROOT/DEFAULT_DB，若预扫描未生效需告警。----
    if getattr(args, "test_root", None):
        if os.environ.get("GAOKZX_TEST_ROOT") != args.test_root:
            print(">>> [test-root] [!] 预扫描未生效（模块级已固化）。"
                  "请确保 --test-root 作为程序首个参数，或改用环境变量 GAOKZX_TEST_ROOT。")
    # ---- OCR 设备/精度开关接线（供子进程环境继承；budget / worker 均读环境变量）----
    _dev = getattr(args, "device", None)
    if _dev:
        os.environ["OCR_DEVICE"] = "gpu" if _dev == "cuda" else _dev
    if getattr(args, "fp16", False):
        os.environ["OCR_FP16"] = "1"
    # ---- 独立开关（不需要 DB）----
    if getattr(args, "check", False):
        step_check_env(install=getattr(args, "install", False), yes=True)
        return
    if args.ensure_dirs:
        ensure_dirs()
        return
    # ---- 无交互全自动（CLI --auto / 看板调度）----
    if getattr(args, "auto", False): sys.exit(run_auto(args))
    if not args.step:
        run_wizard()          # 无参数 -> 交互向导
        return
    steps = set(args.step.split(","))
    # 单文件调度（--file-id）：统一收窄所有 scan_pdfs 取值入口（仅本次命令生效）
    pcore.ONLY_FILE_IDS = _parse_file_ids(getattr(args, "file_id", None))
    # check-env 不需要 DB，先处理
    if "check-env" in steps:
        step_check_env(install=args.install, yes=True)
        return
    # ---- 子目录过滤（保护旧库）：--url 跑新专题时默认只处理该专题目录，避免 --force 重跑整库。
    if args.only_subdir == "ALL": pcore.ONLY_SUBDIR = None
    elif args.only_subdir: pcore.ONLY_SUBDIR = args.only_subdir
    elif args.url:
        try:
            sys.path.insert(0, str(ROOT))
            from gaokzx_download import topic_from_url as _tfu
            pcore.ONLY_SUBDIR = _tfu(args.url, args.name)["name"]
        except Exception: pcore.ONLY_SUBDIR = None
        if pcore.ONLY_SUBDIR:
            print(f">>> [scope] 检测到 --url，自动把处理范围限定在子目录：" f"{pcore.ONLY_SUBDIR}")
            print(f">>> [scope] 旧库其它文件不会被触碰。如需处理整库，加 --only-subdir ALL")
    # 压测/小批量（--src/--hardest）写独立 DB，防污染生产（必须 global，否则 db() 仍写生产库——历史 bug）。
    if bc is not None and getattr(bc, "TEST_ROOT", ""):
        pass  # GAOKZX_TEST_ROOT 隔离模式：DB 已在模块加载时指向隔离目录，不回写生产路径
    else:
        pcore.DB = ROOT / "pipeline_test.db" if args.src or args.hardest else ROOT / "pipeline.db"
    ledgerio.configure(str(pcore.DB))      # 台账跟随同一库：压测不污染生产台账
    con = db()
    # ---- --force-missing（单步 CLI）：仅重跑 DB=done 但磁盘缺失的文件（先核对回滚，不开 --force 全量）----
    # --auto 路径已在每轮 _run_stages_auto 前调 reconcile，无需此处；且 --force-missing 不会置 force（不盲重跑）。
    if getattr(args, "force_missing", False):
        pflowauto.FORCE_MISSING = True
        _fbid = (getattr(args, "batch_id", None) or "").strip() or (bc.current_batch_id() if bc else None)
        if _fbid:
            print(f">>> [force-missing] 批次 {_fbid}：DB↔磁盘核对，仅重跑产物缺失文件（其余跳过）")
            try: reconcile_round(_fbid, verbose=True)
            except Exception as _e: print(f">>> [force-missing] 核对异常（忽略）：{type(_e).__name__}: {_e}")
        else:
            print(">>> [force-missing] 未找到当前批次（--batch-id / current.json），无法定位缺失文件。")
    # 每个步骤都包在 run_step_safe 里：崩溃 -> 中文提示 + 重试/跳过/返回，绝不闪退
    if "all" in steps or "discover" in steps or "download" in steps:
        if args.dry_run:
            print(f"[dry-run] 将运行采集下载 "
                  f"{'url=' + args.url if args.url else 'topic=' + args.topic}"
                  f" limit={args.limit}")
        else:
            ensure_dirs(verbose=False)
            _dl = run_step_safe("抓取下载", step_discover_download,
                                args.topic, args.limit, args.workers, args.delay,
                                url=args.url, name=getattr(args, "name", None),
                                paper_type=getattr(args, "paper_type", None),
                                year_type=getattr(args, "year_type", None))
            # rc=2（0 目标/0 PDF）硬保护：立即中止，绝不继续去水印/OCR/打包空转（提示语已在 step 内打印）。
            _dl_rc = _dl[0] if isinstance(_dl, (tuple, list)) and _dl else _dl
            if _dl_rc == 2:
                _handle_rc2("下载为空（rc=2）：立即中止流水线")
                con.close()
                return
            if _dl_rc == 3:
                _handle_rc3("：立即中止流水线", recent_n=3)
                con.close()
                return
    if "all" in steps or "watermark" in steps:
        run_step_safe("去水印", step_watermark, con,
                      limit=args.limit, dry=args.dry_run, force=args.force,
                      workers=args.wm_workers, src=args.src, hardest=args.hardest,
                      force_scope=args.force_scope, batch_id=args.batch_id)
    # clean-ocr-dirty 刻意不挂 "all"：它会删状态表记录，必须用户显式点名，避免误删。
    if "clean-ocr-dirty" in steps or "clean" in steps:
        run_step_safe("清理脏数据", step_clean_ocr_dirty, con, dry=args.dry_run)
    if "rollback-rename" in steps:
        # 补上 rollback-rename 接线（历史提示"可回滚"但 CLI 未注册，rollback_renames() 无人调用）。
        # 刻意不挂 "all"：回滚重命名会撤销改名成果，必须用户显式点名，避免 --step all 自动触发。
        rb_log = str(Path(args.rename_root or STAGE_CLEANED) / "_报告" / "rename_map.json")
        if not Path(rb_log).exists(): print(f">>> [rollback-rename] 找不到改名映射 {rb_log}，无事可做")
        else: run_step_safe("回滚重命名", rollback_renames, rb_log, dry=args.dry_run)
    if "all" in steps or "rename" in steps:
        # --fix-conflicts-by-body 仅单步 rename 生效：绝不参与 all/auto（无人值守下不自动强改，防误伤）
        _fcb = bool(getattr(args, "fix_conflicts_by_body", False))
        if _fcb and "all" in steps:
            print(">>> [rename] [!] --fix-conflicts-by-body 仅在 --step rename 下生效（all/auto 不自动强改）")
            _fcb = False
        run_step_safe("智能重命名", step_rename, con,
                      root=args.rename_root, limit=args.limit,
                      dry=not args.rename_apply, use_ocr=args.rename_ocr,
                      log_path=str(Path(args.rename_root or STAGE_CLEANED) / "_报告" / "rename_map.json"),
                      ask=(not args.rename_apply and not _fcb), batch_id=args.batch_id,
                      fix_conflicts=_fcb)
    if "all" in steps or "ocr" in steps:
        run_step_safe("OCR 补文字层", step_ocr, con,
                      args.limit, args.dry_run, args.ocr_root,
                      workers=args.ocr_workers, batch=args.ocr_batch,
                      threads_per_worker=args.ocr_threads, force=args.force,
                      force_scope=args.force_scope, batch_id=args.batch_id)
    if "all" in steps or "qc" in steps:
        # QC 的 limit 语义：--limit 只扫「本轮机刚产出的那批」（全量质检不要传 --limit）。
        run_step_safe("质控 QC", step_qc, con, args.dry_run,
                      args.qc_root, limit=args.limit, batch_id=args.batch_id)
    # 第 6 步：隐形水印清理（排在打包前，保证 zip 不含广告页）；rc=3（报告缺失/过旧/不匹配）立即中止并跳过打包。
    _ads_rc = 0
    if "all" in steps or "ads" in steps:
        _ads_rc = run_step_safe("隐形水印清理", step_ads, dry=args.dry_run,
                                report=args.ads_report, out=args.ads_out,
                                limit=args.limit, batch_id=args.batch_id,
                                force=args.force)
        if _ads_rc == 3: print(">>> 清广告报告校验失败，立即中止：跳过后续打包。")
    if _ads_rc != 3 and ("all" in steps or "pack" in steps):
        run_step_safe("打包 zip", step_pack, con, args.pack_root, args.pack_out,
                      limit=args.limit, dry=args.dry_run,
                      skip_wm=args.skip_ads, batch_id=args.batch_id)
    # 全流程收尾：自动导出「文件处理状态总表」（--step all 时）；也可 --step export-excel / 菜单 [c]。
    if ("all" in steps and not args.dry_run) or "export-excel" in steps:
        run_step_safe("导出状态总表", step_export_excel, con)
    # 验收归档（看板 [验收通过] 调用）：把「待验收」文件移入「文库资源_completed」
    if "accept" in steps: run_step_safe("验收归档", step_accept, batch_id=args.batch_id, file_id=args.file_id)
    if "prune-db" in steps: run_step_safe("清理失效路径", step_prune_db, con, dry=args.dry_run or not args.force)
    if "cleanup" in steps:
        # 旧文件清理：默认 dry-run 分类预览；--apply 才真删（默认只删 A 类，B/C 需显式开关，防误删）。
        run_step_safe("旧文件清理", step_cleanup, con, before=args.before, roots=args.roots,
                      include_backup=args.include_backup, include_db_ok=args.include_db_ok,
                      include_db_stale=args.include_db_stale, apply=args.apply)
    if "fix-dst" in steps:
        # 一次性还债：原地链步骤（2~5）悬空 dst/src 回填为 manifest 当前权威路径（默认预览，--force 写库）
        run_step_safe("悬空dst迁移", step_fix_dst, batch_id=args.batch_id,
                      dry=args.dry_run or not args.force)
    if "migrate-done" in steps:
        run_step_safe("归入已处理", step_migrate_done, con, dry=args.dry_run or not args.force, move=args.move)
    con.close()
    # 全流程收尾：--step all 时检查最终 zip 是否存在（不存在 -> "全流程未真正完成"）
    if "all" in steps and not args.dry_run:
        _zip_ok = bool(_final_zip_check())
        # 批次收尾：running 落为 done/incomplete（仅本轮机确实打过包时收尾，避免误改旧批次）。
        # topic 多专题时 _LAST_PACK_ZIP 仅记首个，"是否打过包"判定须并读 _LAST_PACK_ZIPS。
        _bid = bc.current_batch_id() if bc is not None else None
        if _bid and (ppack._LAST_PACK_ZIP or ppack._LAST_PACK_ZIPS):
            _bst = "done" if _zip_ok else "incomplete"
            if bc.set_batch_status(_bid, _bst, note="" if _zip_ok else "未生成最终 zip"):
                print(f">>> [batch] 批次 {_bid} 状态 -> {_bst}")
    print(">>> 流水线本轮机结束")

if __name__ == "__main__": sys.exit(main())
