#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""
文库数据采集 · 统一主控脚本（单脚本全流程）。整合原先零散脚本（grabnew / downtopic /
checkDonwloadedAndRename / ArrangePdfNames / batch_remove_watermark / BatchOcrPdfAddTextLayer /
checkZeroSizeFile / check_pdf_integrity），按顺序执行整条流水线，**绝不自动全量跑**。无参数运行 →
交互式向导；命令见 --help 示例段。

流水线（STEP_* 常量；菜单/向导/日志/pipeline.db 全部引用）：
  1 download  : 专题页 -> 详情页 -> 下载 PDF（清单/断点续传）；空结果 rc=2 -> 全流程立即中止
  2 watermark : 去水印 —— 图层水印无损删除（保留文字层）；像素水印局部块修补（保文字层，超阈值才整页盖图）
  3 rename    : 规范重命名（年份·区·学校·年级·月份·考试·学科·答案）；重名自动 _1/_2 尾缀，绝不覆盖
  4 ocr       : 给"去水印后"成品补透明文字层（PaddleOCR 页级 OCR：GPU 优先 / CPU 物理核预算 / 页级缓存）
  5 qc        : 质控 —— 零字节 / PDF 完整性 / 无文字层，出报告
  6 ads       : 隐形水印清理 —— 重扫描本批次 -> reports/bottom_ads_<batch_id>.json ->「已清广告」（报告缺失/过旧/batch_id 不符即中止）
  7 pack      : 打包 zip 到 打包文件/（源默认「已清广告」，打包前自动 QC）

批次机制（每次任务必建）：batch_id + manifest（文库资源/_批次/batch_<batch_id>.json）；步骤2~7 只处理本批
manifest 内文件（scan_pdfs 统一入口），禁全目录扫描；_测试/验收测试 目录的文件绝不当生产文件处理。

四阶段目录（唯一真相源：本文件顶部 STAGES）—— 待处理 D:\work\文库资源（下载落地，可能有水印）；
  去水印后 D:\work\文库资源_去水印后（像素级成品，文字层可能已丢）；已处理 D:\work\文库资源_已处理
  （补完文字层/重命名后的最终件）；打包文件 D:\work\文库资源_打包文件（待上传压缩包）。

设计要点：SQLite 状态表（pipeline.db）记每文件各步骤状态 → 增量只处理新增/失败；下载幂等（清单续传）；
  OCR 用"是否已有文字层"兜底去重；进度条三级降级（rich→tqdm→纯文本）；Ctrl+C 中断：第一次只设标志
  （当前文件收尾、状态落库），第二次强制退出、下次续传；长任务（水印/OCR）跑独立子进程隔离 C 层崩溃。
"""
import os
import re
import sys
import time
import argparse
import glob
import json
import signal
import sqlite3
import subprocess
import shutil
import datetime
from pathlib import Path

class _NullIO:
    """statusio / ledgerio 不可用时的空实现垫片：任何调用静默变 no-op，
    绝不影响主管线（可用 WM_STATUS_DEBUG=1 打开错误打印）。首调额外警告一次。"""
    _warned = False
    def __getattr__(self, _name):
        def _noop(*a, **kw):
            if not _NullIO._warned:
                _NullIO._warned = True
                print(f"[_NullIO] [!] 状态看板未加载，看板数据（detail_*.json）将不会更新；"
                      f"首个被吞调用：{_name}()")
            if os.environ.get("WM_STATUS_DEBUG") == "1":
                print(f"[_NullIO] no-op 垫片吞掉调用：{_name}(*{a!r}, **{kw!r})")
        return _noop

# 状态看板写入（D:\work\_status\statusio.py）——只加调用、失败静默。
sys.path.insert(0, r"D:\work\_status")
try: import statusio
except Exception: statusio = _NullIO()

# 文件处理台账（_status\ledgerio.py）：每文件每步的输入/产物路径与状态追加写入 pipeline.db 的 file_events 表（永不覆盖），供状态导出与审计。
try: import ledgerio
except Exception: ledgerio = _NullIO()

# ---- --test-root 预扫描（必须先于 batchcore import）----
# CLI 直接传隔离目录：等价设 GAOKZX_TEST_ROOT，但不需用户记环境变量。
# 必须在 import batchcore 之前做，因 batchcore 在模块加载时就固定 TEST_ROOT/DEFAULT_DB。
_TEST_ROOT_ARG = None
for _i, _a in enumerate(sys.argv):
    if _a == "--test-root" and _i + 1 < len(sys.argv):
        _TEST_ROOT_ARG = sys.argv[_i + 1].strip(); break
    if _a.startswith("--test-root="):
        _TEST_ROOT_ARG = _a.split("=", 1)[1].strip(); break
if _TEST_ROOT_ARG:
    os.environ["GAOKZX_TEST_ROOT"] = _TEST_ROOT_ARG
    print(f"[env] --test-root 生效：隔离目录 {_TEST_ROOT_ARG}")

# 批次核心（gaokzx\batchcore.py）：STEP 常量 / batch manifest / scan_pdfs 统一取文件入口 / file_steps
# 落库；失败静默不影响主流程。bc 保留 None 语义（调用方多处 `bc is not None` 判断），不做垫片。
try: import batchcore as bc
except Exception: bc = None
if bc is not None:
    STEP_DOWNLOAD = bc.STEP_DOWNLOAD
    STEP_WATERMARK = bc.STEP_WATERMARK
    STEP_RENAME = bc.STEP_RENAME
    STEP_OCR = bc.STEP_OCR
    STEP_QC = bc.STEP_QC
    STEP_AD_CLEAN = bc.STEP_AD_CLEAN
    STEP_PACK = bc.STEP_PACK
    STEP_ACCEPT = bc.STEP_ACCEPT
    STEP_NAMES = bc.STEP_NAMES
else:
    STEP_DOWNLOAD, STEP_WATERMARK, STEP_RENAME = 1, 2, 3
    STEP_OCR, STEP_QC, STEP_AD_CLEAN, STEP_PACK = 4, 5, 6, 7
    STEP_ACCEPT = 8
    STEP_NAMES = {1: "download", 2: "wm", 3: "rename", 4: "ocr", 5: "qc", 6: "ad_clean", 7: "pack", 8: "accept"}

# Windows 控制台默认 GBK：emoji 等无法编码的字符会让 print 抛 UnicodeEncodeError 崩溃
# 整个流程，这里统一改为"不可编码字符替换"；重定向文件时强制 UTF-8（防日志乱码），控制台保持本地编码。
for _s in (sys.stdout, sys.stderr):
    _reconf = getattr(_s, "reconfigure", None)
    if _reconf is None: continue
    try:
        if not _s.isatty(): _reconf(encoding="utf-8", errors="replace")
        else: _reconf(errors="replace")
    except (ValueError, OSError): pass

# ---- WORK 基路径选择（E5 完整隔离）----
# 隔离模式（--test-root / GAOKZX_TEST_ROOT）下，STAGE_* 全部随 WORK 落到隔离目录，
# 与 batchcore.WORK / STAGE_ALIAS 指向同一隔离树；生产模式零变化（仍 D:\work）。
_ISOLATED = bool(bc is not None and getattr(bc, "TEST_ROOT", ""))
_WORK_BASE = Path(bc.TEST_ROOT) if _ISOLATED else Path(r"D:\work")
if _ISOLATED:
    print(f"[env] STAGE 常量已隔离到 {_WORK_BASE}（原 D:\\work）")

ROOT = Path(r"D:\work\gaokzx")
# 统一日志根：隔离模式（--test-root / GAOKZX_TEST_ROOT）下 logs 整体落入隔离目录（ suspect_parse/
# 空跑/crash/自检日志零写入生产）；ROOT 本身不改——它还承担 gaokzx_download.py 等脚本目录职责。
# 注：worker 子进程（wm_*/batcho）自身的日志仍跟各自脚本里的路径走，不属本次改动范围。
LOG_ROOT = Path(bc.TEST_ROOT) if (bc is not None and getattr(bc, "TEST_ROOT", "")) else ROOT

# 统一分隔常量：=×68 主分隔线 / !×68 报警边框（全文横幅打印只用这两个，杜绝散落字面量）。
# 注：check_env / show_status 等 =×66 短分隔线刻意不并入（语义是"状态块"不是"横幅"）。
_SEP = "=" * 68
_BANG = "!" * 68
_DASH = "-" * 68     # 阶段/详情块等纯视觉分隔（不参与任何逻辑）

# 与 batchcore.clean_text 保持同步（跨模块复制，避免循环 import）；改动必须两处同时进行。
# 文本清洗：去 ANSI 转义 / 控制字符（openpyxl 拒绝 \x00-\x08\x0b\x0c\x0e-\x1f\x7f）、归一换行、截断。
# 子进程原始 stdout 含这些脏字符，凡要写入 file_steps / file_events / 状态表单元格的文本都应先过这里。
_ANSI_RX = re.compile(r'\x1b\[[0-9;?]*[A-Za-z]')
_CTRL_RX = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')
def clean_text(s, limit=2000):
    """清洗任意文本：去 ANSI、去控制字符、归一换行、截断。非字符串原样返回。"""
    if not isinstance(s, str):
        return s
    s = _ANSI_RX.sub('', s)
    s = _CTRL_RX.sub('', s)
    s = s.replace('\r\n', '\n').replace('\r', '\n')
    s = re.sub(r'\n{3,}', '\n\n', s)
    return s[:limit]

# ------------------------- 四阶段目录（唯一真相源） -------------------------
# 单脚本化：常量统一在顶部定义，其它模块只从这里 import，杜绝"扫描源目录而非成品目录"类不一致。
WORK = _WORK_BASE                            # 隔离时=bc.TEST_ROOT，生产时=D:\work（E5 完整隔离）
STAGE_PENDING = WORK / "文库资源"            # 待处理   —— 下载落地（可能有水印）
STAGE_CLEANED = WORK / "文库资源_去水印后"   # 去水印后 —— 像素级成品
STAGE_DONE = WORK / "文库资源_已处理"        # 已处理   —— 补完文字层 / 重命名后的最终件
STAGE_PACKED = WORK / "文库资源_打包文件"    # 打包文件 —— 待上传的压缩包
# 第 6 步「隐形水印清理」输出目录：刻意不入 STAGES 字典——ads 专用成品目录，不参与四阶段流转。
STAGE_ADS = WORK / "文库资源_已清广告"
# 第 8 阶段「已归档」：看板[验收通过]后文件移入的终态目录（不再参与处理）。
STAGE_COMPLETED = WORK / "文库资源_completed"

STAGES = {"待处理": STAGE_PENDING, "去水印后": STAGE_CLEANED, "已处理": STAGE_DONE, "打包文件": STAGE_PACKED}

# 无交互（自动）模式：AUTO_MODE=True 时 _ask/_ask_int/_ask_apply 等直接返回预设默认值（无人值守；
# 菜单[1]/CLI --auto/看板调度设置）。ONLY_FILE_IDS 非空时 scan_pdfs 只保留这些 file_id（单文件调度）。
AUTO_MODE = False
ONLY_FILE_IDS = None

# 自动验收开关（默认开）：一键全自动 pack 成功、批次标 pending_accept 后，直接 batch_accept -> accepted
# 形成闭环。环境变量 GAOKZX_AUTO_ACCEPT=0/false/off 关闭（关闭后仍停在 pending_accept，可看板/[e] 手动批量验收）。
AUTO_ACCEPT = (os.environ.get("GAOKZX_AUTO_ACCEPT", "1").strip().lower()
               not in ("0", "false", "no", "off"))

# 各步骤默认入口/出口
OUT = STAGE_PENDING                  # 去水印源 / 下载落地
# DB 路径跟随 batchcore（设 GAOKZX_TEST_ROOT 时整体落入隔离目录，测试不碰生产库）
DB = Path(bc.DEFAULT_DB) if bc is not None else ROOT / "pipeline.db"
OCR_REPO = WORK / "xer-doc-paddle-ocr"
PY = Path(r"C:\Users\Lenovo\.workbuddy\binaries\python\envs\default\Scripts\python.exe")
REQ_FILE = ROOT / "requirements.txt"

# ==== 解释器守卫（最高优先级，必须在任何重依赖 import 之前）====
# PATH 上的 python 常是裸运行时（没装包）：自动改用正确 venv 重新 exec 本脚本（WM_NO_REEXEC=1 关闭）。
_PEP668_HINT = "（若提示 externally-managed，说明该解释器是裸运行时，请用 venv 里的 python）"

def _norm_py(p):
    try: return os.path.normcase(os.path.realpath(str(p)))
    except OSError: return os.path.normcase(str(p))

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

# 依赖清单与自动安装（--step check-env / 向导 [d]）——(import 名, pip 包名, 用途, 是否必需)
DEP_TABLE = [
    ("pymupdf",        "pymupdf",       "PDF 读写 / 去水印 / 质控",  True),
    ("cv2",            "opencv-python", "像素级去水印",              True),
    ("numpy",          "numpy",         "像素矩阵运算",              True),
    ("requests",       "requests",      "下载 / 网址嗅探",           True),
    ("openpyxl",       "openpyxl",      "下载清单 Excel（续传）",     True),
    ("paddleocr",      "paddleocr",     "OCR 补文字层（很重）",       False),
    ("paddle",         "paddlepaddle",  "OCR 运行时（CPU 版；GPU 版见 requirements-gpu.txt）", False),
    ("tqdm",           "tqdm",          "进度条（降级方案）",         False),
    ("rich",           "rich",          "进度条（首选）",            False),
    ("psutil",         "psutil",        "内存护栏 / 硬件探测",        False),
    ("win32com.client","pywin32",       "Windows COM（可选）",       False),
]

# 哪些算"跑核心流程必需"。paddle 系列只在 OCR 步骤才要，所以自检时不算致命。
_CORE_NEEDED = {"pymupdf", "cv2", "numpy", "requests"}

def _missing_deps():
    """返回 (缺失列表, 已装列表)。每项为 DEP_TABLE 里的元组。"""
    missing, present = [], []
    for entry in DEP_TABLE:
        mod, pkg, why, req = entry
        try:
            __import__(mod)
            present.append(entry)
        except ImportError: missing.append(entry)
        except Exception:
            # 装了但导入报别的错（如 DLL 缺失）—— 也算不可用
            missing.append(entry)
    return missing, present

def step_check_env(install=False, yes=False):
    """环境自检 +（可选）自动安装缺失依赖。对应 --step check-env / 向导 [d]。
    install=False 只报告；install=True 用 venv 解释器执行 pip install。"""
    print("=" * 66)
    print("环境自检" + ("（自动安装模式）" if install else ""))
    print("=" * 66)
    print(f"  解释器 : {sys.executable}")
    print(f"  venv   : {PY}{'' if PY.exists() else '  [!! 不存在]'}")
    in_venv = _norm_py(sys.executable) == _norm_py(PY)
    print(f"  状态   : {'[OK] 正在使用项目 venv' if in_venv else '[!] 不是项目 venv（见上方自动切换提示）'}")
    try:
        import psutil
        pc = psutil.cpu_count(logical=False) or "?"
        lc = psutil.cpu_count(logical=True) or "?"
        mem = psutil.virtual_memory().total / 1024**3
        print(f"  硬件   : {pc} 物理 / {lc} 逻辑核，内存 {mem:.1f} GB")
        if mem < 20:
            print(f"           [!] 内存 {mem:.1f}GB → OCR 建议 ≤ " f"{safe_workers_for_memory(99)[0]} worker")
    except ImportError: psutil = None
    try:
        import paddle
        _cuda = bool(paddle.is_compiled_with_cuda())
        _dcnt = int(paddle.device.cuda.device_count()) if _cuda else 0
        _nvsmi = _ocr_gpu_present()
        if _cuda and _dcnt:
            print(f"  Paddle : GPU 版（CUDA），可见 {_dcnt} 块显卡 —— OCR 将走 GPU")
        elif _nvsmi:
            print(f"  Paddle : CPU 版 —— [!!] 检测到 NVIDIA 显卡但 Paddle 非 CUDA 版，OCR 将回退 CPU（慢）")
            print(f"           修复一： {PY} -m pip uninstall -y paddlepaddle")
            print(f"           修复二： {PY} -m pip install -r {ROOT / 'requirements-gpu.txt'}")
        else:
            print(f"  Paddle : CPU 版（本机无 NVIDIA 显卡，正常；已启用线程钉死 + 物理核预算）")
    except ImportError:
        pass
    print("-" * 66)
    missing, present = _missing_deps()
    for mod, pkg, why, req in present: print(f"  [OK]  {pkg:16s} {why}")
    for mod, pkg, why, req in missing:
        tag = "必需" if (req and pkg in _CORE_NEEDED) else "建议"
        print(f"  [!!]  {pkg:16s} 缺失（{tag}）—— {why}")
    # 目录 / DB / OCR 仓库
    print("-" * 66)
    for name, p in STAGES.items():
        print(f"  {'[OK]' if p.is_dir() else '[..]'}  {name:8s} {p}" f"{'' if p.is_dir() else '  (缺失)'}")
    print(f"  {'[OK]' if DB.exists() else '[..]'}  pipeline.db  {DB}")
    print(f"  {'[OK]' if OCR_REPO.is_dir() else '[!!]'}  OCR 仓库     {OCR_REPO}")
    if not missing:
        print("-" * 66)
        print("  [OK] 依赖齐全，可以开跑。")
        print("=" * 66)
        return True
    todo = [pkg for mod, pkg, why, req in missing if pkg in _CORE_NEEDED]
    if install:
        pkgs = sorted({pkg for mod, pkg, why, req in missing})
        print("-" * 66)
        print(f"  将安装 {len(pkgs)} 个包：{', '.join(pkgs)}")
        if not yes and _ask("确认安装？[Y/n]: ", "y").lower() != "y": print("  已取消安装。")
        else:
            _pip_install(pkgs)
            missing2, _ = _missing_deps()
            if missing2: print(f"  [!] 仍有 {len(missing2)} 个包不可用：" f"{', '.join(p for _, p, _, _ in missing2)}")
            else: print("  [OK] 全部安装完成。")
    else:
        print("-" * 66)
        print(f"  核心依赖缺失：{', '.join(todo) if todo else '（无）'}")
        print(f"  一键修复： python pipeline.py --step check-env --install")
        print(f"  或手动   ： {PY} -m pip install -r {REQ_FILE}{_PEP668_HINT}")
    print("=" * 66)
    return not [1 for m, p, w, r in missing if p in _CORE_NEEDED]

def _pip_install(pkgs):
    """用项目 venv 的 pip 安装（不用当前解释器，避免装到裸运行时里）。"""
    py = str(PY) if PY.exists() else sys.executable
    cmd = [py, "-m", "pip", "install", *pkgs]
    print(f">>> {(' '.join(cmd))}")
    try:
        r = subprocess.run(cmd, check=False)
        if r.returncode != 0: print(f"  [!] pip 退出码 {r.returncode}，可尝试手动安装（见上方命令）")
        return r.returncode == 0
    except OSError as e:
        print(f"  [!] pip 调用失败：{e}")
        return False

def fatal_hint(_step_name, exc):
    """把异常翻译成"用户看得懂 + 能动手修"的中文提示。返回提示文本。"""
    s = f"{type(exc).__name__}: {exc}"
    low = s.lower()
    tip = []
    if "no module named" in low or "modulenotfounderror" in low:
        m = re.search(r"named '?([\w.]+)'?", s)
        pkg = m.group(1) if m else "?"
        tip.append(f"缺少 Python 包 `{pkg}`。")
        tip.append(f"修复： python pipeline.py --step check-env --install")
    elif "permissionerror" in low: tip.append("文件被占用或无写权限。请关闭正在打开这些 PDF 的程序（阅读器/编辑器）后重试。")
    elif "no such file" in low or "filenotfound" in low:
        tip.append("路径不存在。检查该步骤的根目录参数（--ocr-root / --qc-root / --pack-root）。")
    elif "disk" in low or "no space" in low: tip.append("磁盘空间不足。清理 待处理/去水印后 或删除 .bak 备份后重试。")
    elif "memoryerror" in low or "cuda out of memory" in low:
        tip.append(f"内存不足。降低 worker 数（--ocr-workers 2）或加 --limit 分批跑。")
    elif "timeout" in low: tip.append("任务超时。可用 --limit 减小批量，或检查网络/磁盘 IO。")
    if not tip: tip.append("未识别的错误，请把上方 traceback 发出来。")
    return "\n".join("      · " + t for t in tip)

def run_step_safe(step_label, fn, *args, **kwargs):
    """用 try/except 包裹每一步：崩溃不闪退，给中文提示 + 选「重试/跳过该步/返回主菜单」。
    返回步骤函数返回值（若无则 True），False=用户跳过。首参刻意叫 step_label 而非 name——
    步骤函数自己可能有 name kwarg，否则撞成 TypeError: got multiple values for argument 'name'。"""
    while True:
        try:
            res = fn(*args, **kwargs)
            return res if res is not None else True
        except KeyboardInterrupt:
            print(f"\n[!] 【{step_label}】被用户中断。")
            return False
        except _BackToMenu:
            # 内层步骤（如 run_step_safe 嵌套调用）要求回主菜单 → 原样向上传递
            raise
        except SystemExit: raise
        except Exception as e:
            print(f"\n[ERR] 【{step_label}】执行失败")
            print(f"      原因: {type(e).__name__}: {e}")
            print(f"      可能修复方向:")
            print(fatal_hint(step_label, e))
            import traceback
            print("      ---- 详细 traceback（供排查）----")
            tb = traceback.format_exc().strip().splitlines()
            for line in tb[-8:]: print("      " + line)
            if AUTO_MODE:
                # 无交互模式：绝不进入"重试/跳过"询问——直接上抛，由 run_auto 记录失败并中止后续（防被当"跳过"空跑）。
                raise
            c = _ask(f"\n  [{step_label}] 请选择：[r] 重试  [s] 跳过该步  [q] 返回主菜单  > ", "s").lower()
            if c.startswith("r"):
                print(f">>> 重试【{step_label}】...")
                continue
            if c.startswith("q"): raise _BackToMenu()
            print(f">>> 已跳过【{step_label}】，继续后续步骤。")
            return False

class _BackToMenu(Exception):
    """内部信号：把控制权交回主菜单（被 run_wizard 捕获）。"""

# ---- 立即执行解释器守卫：必须在任何重依赖加载之前 ----
maybe_reexec_with_venv()

GAOKZX_DL = ROOT / "gaokzx_download.py"
WM_WORKER = ROOT / "wm_worker.py"      # 单文件去水印子进程入口（隔离 C 层崩溃 + 支持并发）
OCR_WORKER = ROOT / "wm_ocr_batch_worker.py"  # OCR 批处理子进程入口（引擎初始化一次/批）

WM_DETECT_MIN = 0.001    # 水印检测阈值（灰/浅蓝双通道并集像素占比）；旧版 0.8% 仅灰通道，漏检大量浅蓝水印
WM_BLUE_CLOSE = 101      # 蓝通道闭运算核：桥接水印被逐页差异内容切断的长断段
WM_EXT_BREL = 25         # 蓝扩展区的蓝偏下限（B-R）：越大越只清"纯蓝"，保护插画/照片里的蓝灰色
WM_CORE_MIN = 185        # 蓝核心区白化的亮度下限：白底水印（实测 p5≥197）清掉，压在水印下的深色文字/图形
                         # （混合后偏暗的像素）原样保留，避免缺字/破图
WM_GRAY_MIN = 185        # 灰通道白化的灰度下限（同一原则：低于 185 的中间调像素都有内容嫌疑）
WM_EXT_MIN = 185         # 蓝扩展区白化的亮度下限（同 185 原则；实测 135-185 区间的白化才是"缺字/破图"主因：
                         # 破坏像素 67% 出在此区间，阈值提到 185 后破坏归零，清除率仅损失 2.9pp）

# 进度条 / 日志 / 安全中断（内联版，不依赖 wfui 外部模块）：优先 rich -> tqdm -> 纯文本百分比
try:
    from rich.progress import (Progress, SpinnerColumn, BarColumn, TextColumn,
                               TimeElapsedColumn, TimeRemainingColumn,
                               MofNCompleteColumn)
    _HAS_RICH = True
except ImportError:
    Progress = SpinnerColumn = BarColumn = TextColumn = None
    TimeElapsedColumn = TimeRemainingColumn = MofNCompleteColumn = None
    _HAS_RICH = False
try:
    from tqdm import tqdm as _tqdm
    _HAS_TQDM = True
except ImportError:
    _tqdm = None
    _HAS_TQDM = False

_INTERRUPTED = {"flag": False}

def log_ok(name, extra=""):
    print(f"[OK]  {name} -> 处理完成{(' ' + extra) if extra else ''}", flush=True)

def log_err(name, reason=""):
    print(f"[ERR] {name} -> {reason}", flush=True)

def log_info(msg):
    print(f"[..]  {msg}", flush=True)

def install_sigint(tag="任务"):
    """Ctrl+C 安全中断：第一次只设标志（当前文件收尾），第二次强制退出。
    返回一个无参函数，调用返回 True 表示已请求中断。"""
    # 每装一次新步骤的 SIGINT handler 都清零标志：避免上一步骤的中断状态污染本步骤
    # （同一次运行跨步骤复用 _INTERRUPTED，必须重置）。
    _INTERRUPTED["flag"] = False
    def _h(_sig, _frame):
        if _INTERRUPTED["flag"]:
            print(f"\n[!] 再次收到 Ctrl+C，强制退出（{tag}状态已尽量保存）", flush=True)
            raise SystemExit(130)
        _INTERRUPTED["flag"] = True
        print(f"\n[!] 收到 Ctrl+C：当前文件处理完后安全停止 {tag}。" f"已完成状态已落库，下次自动续传。（再按一次强制退出）", flush=True)
    try: signal.signal(signal.SIGINT, _h)
    except (ValueError, OSError): pass
    return lambda: _INTERRUPTED["flag"]

class _Bar:
    """统一进度条。用法：with bar(total, "去水印") as b: for f in files: b.update(1, label=basename, ...)。
    update(name=..., ok=True/False) 时额外打一行 [OK]/[ERR] 明细，便于日志归档。"""
    def __init__(self, total, desc="进度", enabled=True):
        self.total = max(0, int(total or 0))
        self.desc = desc
        self.enabled = enabled
        self.n = 0
        self._p = self._tq = None
        self._rich_task = None
        self._t0 = time.time()

    def __enter__(self):
        if not self.enabled or self.total <= 0: return self
        if _HAS_RICH:
            p = self._p = Progress(
                SpinnerColumn(), TextColumn("[bold blue]{task.description}"),
                BarColumn(), MofNCompleteColumn(),
                TextColumn("{task.fields[label]}"),
                TimeElapsedColumn(), TimeRemainingColumn(), transient=False)
            p.start()
            self._rich_task = p.add_task(self.desc, total=self.total, label="")
        elif _HAS_TQDM: self._tq = _tqdm(total=self.total, desc=self.desc, ncols=100, dynamic_ncols=True)
        else: print(f"  {self.desc}: 0/{self.total}", flush=True)
        return self

    def update(self, k=1, label="", ok=None, name=None):
        self.n += k
        if self.enabled and self.total > 0:
            disp = (label or "")[:38]
            if self._p is not None: self._p.update(self._rich_task, advance=k, label=disp)
            elif self._tq is not None:
                self._tq.update(k)
                if disp: self._tq.set_postfix_str(disp)
            else:
                pct = self.n * 100.0 / self.total
                sys.stdout.write(f"\r  {self.desc}: {self.n}/{self.total} " f"({pct:.0f}%) {disp}".ljust(96))
                sys.stdout.flush()
        if name:
            if ok is True: log_ok(name)
            elif ok is False: log_err(name, label or "")

    def __exit__(self, *exc):
        el = time.time() - self._t0
        if self._p is not None: self._p.stop()
        elif self._tq is not None: self._tq.close()
        elif self.enabled and self.total > 0: sys.stdout.write("\n")
        if self.enabled and self.total > 0:
            rate = self.n / el if el > 0 else 0
            log_info(f"{self.desc} 结束：{self.n}/{self.total}，耗时 {el:.1f}s" f"（{rate:.2f} 个/s）")
        return False

def _wf_bar(total, desc="进度", enabled=True):
    return _Bar(total, desc, enabled)

def _wf_install_sigint(tag="任务"):
    return install_sigint(tag)

# ------------------------- 状态表 -------------------------
def _migrate_files_pk(con):
    """files 表主键 path -> (path, step) 一次性迁移（幂等）。旧结构单列主键会让同一文件被不同
    步骤写入时互相覆盖；SQLite 不能直接改主键，走「建新表->拷数据->替换」；失败回收残留新表并
    继续用旧结构，绝不中断运行。"""
    try:
        cols = con.execute("PRAGMA table_info(files)").fetchall()
        pk_cols = [c[1] for c in cols if c[5]]      # c[5] = 主键序号（0=非主键）
        if len(pk_cols) == 2 and set(pk_cols) == {"path", "step"}:
            return                                  # 已是新结构
        con.execute(
            "CREATE TABLE files_new("
            "  path TEXT, step TEXT, status TEXT, ts TEXT,"
            "  PRIMARY KEY(path, step))")
        con.execute(
            "INSERT OR REPLACE INTO files_new(path, step, status, ts) "
            "SELECT path, step, status, ts FROM files")
        con.execute("DROP TABLE files")
        con.execute("ALTER TABLE files_new RENAME TO files")
        con.commit()
        print(">>> [db] files 表主键已迁移为 (path, step)（旧记录已保留）")
    except sqlite3.Error as e:
        try: con.execute("DROP TABLE IF EXISTS files_new")
        except sqlite3.Error: pass
        print(f">>> [db] files 表迁移失败（{e}），本次继续使用旧结构")

def db():
    con = sqlite3.connect(str(DB))
    con.execute(
        """CREATE TABLE IF NOT EXISTS files(
              path TEXT, step TEXT, status TEXT, ts TEXT,
              PRIMARY KEY(path, step))"""
    )
    _migrate_files_pk(con)
    # 批次状态表（batches / file_steps）：lazily 建表幂等；并让 batchcore 跟随当前库（压测/生产切换一致）。
    if bc is not None:
        try:
            bc.configure(str(DB))
            bc.ensure_tables()
        except Exception: pass
    # 首次建连一次性自证：statusio 重入失败降级为 _NullIO 时醒目提示（看板失效根因排查）。
    if not getattr(db, "_statusio_checked", False):
        db._statusio_checked = True
        _st_t = type(statusio).__name__
        if _st_t == "_NullIO":
            print(_SEP)
            print(">>> [statusio] [!] 看板写入被静默禁用（_NullIO 垫片）！")
            print(">>> [statusio] 原因：import statusio 失败（尝试路径 D:\\work\\_status\\statusio.py）。")
            print(">>> [statusio] 修复：确保该文件存在且可 import；或设 WM_STATUS_DEBUG=1 观察吞掉的调用。")
            print('>>> [statusio] 检查：python -c "import sys;sys.path.insert(0,r\'D:\\work\\_status\');import statusio;print(statusio.__file__)"')
            print(_SEP)
        else:
            print(f">>> [statusio] 看板写入已启用（{_st_t}）")
    return con

def mark(con, path, step, status):
    con.execute(
        "INSERT OR REPLACE INTO files(path,step,status,ts) VALUES(?,?,?,?)",
        (path, step, status, datetime.datetime.now().isoformat(timespec="seconds")),
    )
    con.commit()

# ------------------------- 阶段目录工具 -------------------------
def ensure_dirs(verbose=True):
    """创建四个阶段目录（幂等）。"""
    created = []
    for name, p in STAGES.items():
        if not p.is_dir():
            p.mkdir(parents=True, exist_ok=True)
            created.append((name, p))
            if verbose: print(f"  [新建] {name:8s} -> {p}")
    for p in STAGES.values():
        if p.is_dir(): (p / "_报告").mkdir(exist_ok=True)
    if verbose and not created: print("  四个阶段目录均已存在，无需创建")
    return created

# ------------------------- 安全护栏：内存 / worker -------------------------
OCR_WORKERS_DEFAULT = 0      # 0 = 自动预算（按物理核/内存/设备定；见 ocr_worker_budget）；旧默认 4×4 已废
OCR_THREADS_PER_WORKER = 1   # 每进程内部线程数（写 OMP/MKL/OPENBLAS_NUM_THREADS；CPU 模式 1~2）
# 调参原则（旧版教训）：worker 数按"物理核"定，不按逻辑线程；严禁 workers×threads 超物理核（本机
# 16C32T）。旧默认 4×4=16 看似对齐，实际 Paddle 内部另有预处理线程 -> 超订抖动（单文件 100~413s）。
# 新预算：CPU=min(物理核,8)×1 线程（内存护栏再收紧到 ≤6）；GPU=2×1（显卡串行，多进程只抢显存）。
OCR_WORKERS_HARD_CAP = 6     # 超过必然 OOM（6 × ~1.9GB ≈ 11.4GB + 系统占用）
OCR_MEM_PER_WORKER_GB = 1.9
OCR_MEM_BUDGET_GB = 12.0     # 内存峰值硬预算
# ---- 显存护栏（A1/B3：把"显存"维度纳入并发预算，防 8GB 卡直接 OOM 卡死）----
# 单 worker 显存预算：2026-09-23 真跑事故实证——8GB 卡 2×1 时单 worker 实测常驻
# 4230MB（双 Paddle 引擎 + 高清图 det 激活），旧估 2.5GB 严重乐观致 OOM+冻结，
# 上调为 3.5GB 实测量级；另加 2×1 总显存硬门槛（见 OCR_VRAM_DUAL_MIN_TOTAL_MB）。
OCR_VRAM_PER_WORKER_GB = 3.5
OCR_VRAM_SERVER_GB = 5.0      # server 升档档位预算（参考，用于注释/未来细分）
OCR_VRAM_RESERVE_MB = 1024    # 系统/驱动保留显存
OCR_VRAM_DUAL_MIN_TOTAL_MB = 12288   # 2×1 双引擎升档硬门槛：总显存不足 12GB 一律 1×1（8GB 卡事故教训）
# ---- 停滞检测（C1/C2：替换旧 7200s 硬超时，按"无进度+显存占满"判卡死）----
OCR_STALL_TIMEOUT = 600       # 无进度且显存占满持续秒数 -> 判卡死 kill
OCR_STALL_HOLD_TIMEOUT = 1200 # 无进度且 worker 显存驻留（冻结不释放，事故形态：52% 占用永不到 90%）持续秒数 -> 也判卡死
OCR_CHUNK_ABS_MAX = 7200      # 子进程绝对超时兜底（不依赖显存，防极端卡死）

_OCR_GPU_CACHE = None
def _ocr_gpu_present():
    """父进程级 GPU 探测（只查 nvidia-smi；paddle 是否 CUDA 版由 OCR worker 自己确认）。结果缓存。"""
    global _OCR_GPU_CACHE
    if _OCR_GPU_CACHE is None:
        ok = False
        _smi = shutil.which("nvidia-smi")
        if _smi:
            try:
                _r = subprocess.run([_smi, "--query-gpu=name", "--format=csv,noheader"],
                                    capture_output=True, text=True, timeout=8)
                ok = (_r.returncode == 0) and bool((_r.stdout or "").strip())
            except Exception:
                ok = False
        _OCR_GPU_CACHE = ok
    return _OCR_GPU_CACHE

_OCR_VRAM_CACHE = None
def _ocr_vram_info():
    """返回 (total_mb, free_mb)；无 GPU 或探测失败返回 (0, 0)。结果缓存（仅启动期查一次）。"""
    global _OCR_VRAM_CACHE
    if _OCR_VRAM_CACHE is None:
        total = free = 0
        if _ocr_gpu_present():
            _smi = shutil.which("nvidia-smi")
            if _smi:
                try:
                    _r = subprocess.run(
                        [_smi, "--query-gpu=memory.total,memory.free",
                         "--format=csv,noheader,nounits"],
                        capture_output=True, text=True, timeout=8)
                    if _r.returncode == 0:
                        _parts = (_r.stdout or "").strip().splitlines()[0].split(",")
                        total = int(float(_parts[0]))
                        free = int(float(_parts[1]))
                except Exception:
                    pass
        _OCR_VRAM_CACHE = (total, free)
    return _OCR_VRAM_CACHE

def _ocr_gpu_active():
    """是否真的要走 GPU 预算：有 N 卡 且 未强制 OCR_DEVICE=cpu。"""
    if (_env := os.environ.get("OCR_DEVICE", "")).lower() == "cpu":
        return False
    return bool(_ocr_gpu_present())

def ocr_worker_budget(want=0, threads=0):
    """OCR 并发预算（物理核 + 内存 + 显存 + 设备）。返回 (workers, threads_per_worker, 说明或 None)。
    want/threads=0 时自动：CPU -> min(物理核,8)×1；GPU -> 1×1（单进程保守跑，防 CUDA OOM）。
    护栏顺序：内存（12GB/1.9GB≤6）→ 显存（B3 按【剩余可用】预算，开着浏览器/游戏自动少开）→ 物理核。"""
    try:
        import psutil
        phys = psutil.cpu_count(logical=False)
    except Exception:
        phys = None
    if not phys:
        phys = max(1, (os.cpu_count() or 8) // 2)
    gpu = _ocr_gpu_active()   # 有 N 卡 且 未强制 OCR_DEVICE=cpu
    # B1: GPU 模式强制每进程 1 线程（GPU 推理不吃 CPU 线程，多余线程白抢 RAM/核）
    if gpu:
        if threads and threads != 1:
            notes0 = ["GPU 模式：忽略线程参数，强制每进程 1 线程（GPU 不吃 CPU 线程）"]
        else:
            notes0 = []
        threads = 1
    else:
        if not threads: threads = OCR_THREADS_PER_WORKER
        threads = max(1, min(2, int(threads)))
        notes0 = []
    if not want:
        if gpu:
            # GPU 默认不再永远锁死 1×1：按当前空闲显存自动升档，上限 2×1（保守，防多 worker OOM）；
            # 浏览器/游戏吃掉显存后空闲不足会自动回 1×1（下方显存护栏再次夹紧）。
            vt, vf = _ocr_vram_info()
            _pw = max(0.5, OCR_VRAM_PER_WORKER_GB)
            _vc = 1
            if vt > 0:
                _vc = max(1, min(int((vt - OCR_VRAM_RESERVE_MB) // (_pw * 1024)),
                                 int((vf - OCR_VRAM_RESERVE_MB) // (_pw * 1024))))
            # 2×1 硬门槛：总显存 < 12GB 一律 1×1。8GB 卡上双 Paddle 引擎常驻+激活
            # 实测必爆（2026-09-23 真跑事故：2×1 后 worker 冻结 300s+ 零进度）。
            if vt < OCR_VRAM_DUAL_MIN_TOTAL_MB and _vc > 1:
                _vc = 1
                notes0.append(f"显存护栏：总显存 {vt}MB < {OCR_VRAM_DUAL_MIN_TOTAL_MB}MB 硬门槛，强制 1×1（8GB 卡双引擎事故实证，防升档爆显存）")
            want = min(2, _vc)
            if want >= 2:
                # 阈值自证：把升档公式的全部输入量打出来，人工可复算验证 2×1 不是拍脑袋
                notes0.append(
                    f"GPU 显存充足：自动升 2×1 提升吞吐（自证：总 {vt}MB≥{OCR_VRAM_DUAL_MIN_TOTAL_MB}MB 硬门槛、空闲 {vf}MB、"
                    f"预留 {OCR_VRAM_RESERVE_MB}MB、单 worker 实证 {OCR_VRAM_PER_WORKER_GB}GB → "
                    f"vcap={_vc}=min(按总({int((vt - OCR_VRAM_RESERVE_MB) // (_pw * 1024))}), "
                    f"按空闲({int((vf - OCR_VRAM_RESERVE_MB) // (_pw * 1024))}))，取上限 2；不足会自动降回 1×1）")
        else:
            want = min(phys, 8)
    want = max(1, int(want))
    notes = list(notes0)
    cap = min(int(OCR_MEM_BUDGET_GB // OCR_MEM_PER_WORKER_GB) or 1, OCR_WORKERS_HARD_CAP)
    if want > cap:
        notes.append(f"内存护栏：{want} worker × {OCR_MEM_PER_WORKER_GB}GB 超出 {OCR_MEM_BUDGET_GB:.0f}GB 预算，已降为 {cap}")
        want = cap
    # A1/B3: 显存护栏——按【剩余可用显存】预算（最常见卡爆诱因：浏览器/游戏已占显存）
    if gpu:
        vtotal, vfree = _ocr_vram_info()
        if vtotal > 0:
            pw = max(0.5, OCR_VRAM_PER_WORKER_GB)
            vcap_total = int((vtotal - OCR_VRAM_RESERVE_MB) // (pw * 1024))
            vcap_free = int((vfree - OCR_VRAM_RESERVE_MB) // (pw * 1024))
            vcap = max(1, min(vcap_total, vcap_free))
            if want > vcap:
                notes.append(f"显存护栏：空闲 {vfree}MB / 共 {vtotal}MB，单 worker≈{pw:.1f}GB → 并发压到 {vcap}（原 {want}）")
                want = vcap
    if want * threads > phys:
        _w2 = max(1, phys // threads)
        notes.append(f"线程预算：{want}×{threads} 超过物理核 {phys}，已降为 {_w2}")
        want = min(want, _w2)
    return want, threads, ("；".join(notes) or None)

def safe_workers_for_memory(workers, mem_budget_gb=OCR_MEM_BUDGET_GB):
    """按内存预算收紧 worker 数（仅供 step_check_env 内存告警计算用）。返回 (workers, 说明或 None)。"""
    workers = max(1, int(workers or 1))
    cap = int(mem_budget_gb // OCR_MEM_PER_WORKER_GB) or 1
    cap = min(cap, OCR_WORKERS_HARD_CAP)
    if workers > cap:
        return cap, (f"内存护栏：{workers} worker × {OCR_MEM_PER_WORKER_GB}GB " f"超出 {mem_budget_gb:.0f}GB 预算，已降为 {cap}")
    return workers, None

# 网址预检嗅探（轻量级，不下文件、不发大请求）
_SNIFF_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
_PDF_RX = re.compile(r'https?://[^\s"\'<>\\]+?\.pdf(?:\?[^\s"\'<>\\]*)?', re.I)
_HREF_RX = re.compile(r'<a[^>]+href=["\']([^"\']+)["\']', re.I)

class SniffResult:
    def __init__(self, url=""):
        self.url = url
        self.ok = False
        self.kind = "unknown"        # gaokzx_topic | html | direct_pdf
        self.title = ""
        self.count = 0
        self.names = []
        self.links = []
        self.error = ""
        self.note = ""
        self.paper_cands = []     # 供交互自选的"试卷类型"候选（collect_paper_candidates 产出；默认空）

def _sniff_session():
    import requests
    try:                        # 该站证书链有问题，屏蔽 InsecureRequestWarning 刷屏
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    except Exception: pass
    s = requests.Session()
    s.headers.update({"User-Agent": _SNIFF_UA,
                      "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
                      "Accept-Language": "zh-CN,zh;q=0.9"})
    s.verify = False            # 该站证书链有问题
    return s

def _looks_like_pdf(u):
    from urllib.parse import urlparse
    return urlparse(u).path.lower().endswith(".pdf")

def sniff_url(url, timeout=20, max_preview=10, paper_type=None):
    """嗅探一个 URL。永不抛异常；失败信息放进 result.error。"""
    from urllib.parse import urljoin, unquote, urlparse
    res = SniffResult(url=url)
    url = (url or "").strip()
    if not url:
        res.error = "空网址"
        return res
    if not re.match(r"^https?://", url, re.I):
        url = "https://" + url
        res.url = url
    if _looks_like_pdf(url):        # 直接 PDF 直链
        res.ok, res.kind, res.count = True, "direct_pdf", 1
        name = unquote(os.path.basename(urlparse(url).path))
        res.names, res.links, res.title = [name], [url], name
        return res
    try:
        r = _sniff_session().get(url, timeout=timeout)
        r.raise_for_status()
        html = r.text
    except Exception as e:
        res.error = f"请求失败：{type(e).__name__}: {e}"
        return res
    host = urlparse(url).netloc.lower()
    if "gaokzx.com" in host:        # 已知站点：走精确解析
        try:
            from gaokzx_parse import (extract_files, parse_topic,
                                      record_matches, collect_paper_candidates)
            recs = parse_topic(html)
            res.paper_cands = collect_paper_candidates(recs)   # 候选列表（默认不据此过滤）
            _flt_suffix = ""
            if paper_type:
                from gaokzx_download import _build_paper_terms
                _pterms = _build_paper_terms(paper_type)
                if _pterms:
                    _b = len(recs)
                    recs = [r for r in recs if record_matches(r, _pterms)]
                    _flt_suffix = f"（试卷类型筛选：{_b} -> {len(recs)}）"
            res.title = _html_title(html)
            if recs:
                res.kind = "gaokzx_topic"
                res.count = len(recs)
                res.names = [(r.get("text") or r.get("url") or "")[:80] for r in recs[:max_preview]]
                res.note = ("专题页：以上是「目标详情页」数量与标题；每个详情页内才是实际 PDF。"
                            "直接下载时会逐页解析；若目标页里还有下一级汇总页（多层站点），"
                            "会自动逐层下钻，实际 PDF 数可能更多。")
                res.links = [r.get("url", "") for r in recs]
            else:
                # 无表格 → 可能本身就是「详情页」（页内直接挂 PDF 附件）
                files = extract_files(html, include_images=False)
                res.kind = "gaokzx_detail" if files else "gaokzx_topic"
                res.count = len(files)
                res.names = [(f.get("fileName") or f.get("fileUrl") or "")[:80] for f in files[:max_preview]]
                res.note = (f"详情页：本页直接含 {len(files)} 个 PDF 附件，选择抓取后将直接下载附件。"
                            if files else
                            "页面里既无表格记录也未发现 PDF 附件；"
                            "请确认网址是专题页或详情页。")
                res.links = [f.get("fileUrl", "") for f in files]
            if _flt_suffix:
                res.note = (res.note + _flt_suffix) if res.note else _flt_suffix.strip("（）")
            res.ok = True
            return res
        except Exception as e: res.error = f"站点解析失败（回退通用抓取）：{e}"
    # 通用 HTML：抽 pdf 直链
    links = []
    for m in _PDF_RX.finditer(html):
        u = m.group(0).rstrip(").,;")
        if u not in links: links.append(u)
    if not links:
        for h in _HREF_RX.findall(html):
            u = urljoin(url, h)
            if _looks_like_pdf(u) and u not in links: links.append(u)
    if not links:
        for m in re.finditer(r'"(https?:[^"]+?\.pdf[^"]*)"', html, re.I):
            u = m.group(1).encode().decode("unicode_escape", "ignore")
            if u not in links: links.append(u)
    res.links, res.count = links, len(links)
    res.names = [unquote(os.path.basename(urlparse(u).path)) or u for u in links[:max_preview]]
    res.title = _html_title(html)
    res.ok, res.kind = True, "html"
    if not links: res.note = ("页面可访问，但没发现 .pdf 直链。可能是分页/JS 渲染页面，" "或该页只有详情链接；可尝试换成具体的专题页网址。")
    return res

def _html_title(html):
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.S | re.I)
    return m.group(1).strip()[:120] if m else ""

def format_sniff(res, preview=10):
    out_lines = ["=" * 66, "预检嗅探结果", "=" * 66, f"  网址   : {res.url}"]
    if res.title: out_lines.append(f"  标题   : {res.title}")
    out_lines.append(f"  类型   : {res.kind}")
    if not res.ok:
        out_lines += [f"  [ERR]  {res.error}", "=" * 66]
        return "\n".join(out_lines)
    out_lines.append(f"  预估数量: {res.count}")
    if res.note: out_lines.append(f"  说明   : {res.note}")
    if res.names:
        out_lines.append(f"  前 {min(preview, len(res.names))} 个：")
        for i, n in enumerate(res.names[:preview], 1): out_lines.append(f"    {i:2d}. {n}")
    out_lines.append("=" * 66)
    return "\n".join(out_lines)

# 文件整理与重命名（防覆盖）：L1 文件名正则 -> L2 PDF 首页文字层 -> L3 OCR 兜底（可选）；默认
# dry-run，重名自动 _2/_3 尾缀，改名前写映射表可回滚。
_SUBJECTS = ["语文", "数学", "英语", "物理", "化学", "生物", "历史", "地理", "政治",
             "思想政治", "理科综合", "文科综合", "理综", "文综", "信息技术", "通用技术"]
_GRADES = ["高三", "高二", "高一", "初中", "小学"]
_EXAM_TYPES = ["月考", "期中", "期末", "一模", "二模", "三模", "四模", "开学考",
               "开学考试", "联考", "模拟", "调研", "统练", "周测", "阶段测试",
               "质量检测", "适应性测试", "学业水平", "竞赛",
               "考前热身", "热身卷", "热身", "保温卷", "保温", "考前练", "冲刺", "预测试"]
# 答案状态：只认“显式关键词”，删去光杆“答案”（“试题及答案”这类汇总标题不得把单份试卷默认补成有答案）。
_ANSWER_STATES = [("无答案", ["无答案", "不含答案", "学生版", "无解答", "空白卷"]),
                  ("有答案", ["有答案", "含答案", "附答案", "教师版", "解析版", "有解析", "答案解析", "答案版"])]

_DISTRICTS = ["海淀", "朝阳", "西城", "东城", "丰台", "石景山", "门头沟", "房山",
              "通州", "顺义", "昌平", "大兴", "怀柔", "平谷", "密云", "延庆",
              "燕山", "经开区"]

# 学校白名单（正文兜底时的唯一可信来源）：来自全库 763 个真实 PDF 文件名的频次统计
# （出现≥2 次的 99 个校名）+ 常见学校人工补充。规则：stem（下载命名，可信）里抽校名
# 走结构模式（宽松 + 噪声过滤）；meta（PDF 正文，不可信）兜底**只认白名单命中**——
# 正文里"十一届三中全会 / 技术的对立统一中 / 多所高校规范大学"这类短语绝不会通过
# 边界与虚词校验（历史坑：正文校名取最长，覆盖了文件名里的真校名）。
_SCHOOL_WHITELIST = (
    # —— 来自全库真实文件名的频次校名 ——
    "十二中", "十五中", "八十中", "十八中", "十九中", "五十五中", "十四中",
    "一零一中", "一六一中", "一六六中", "二中", "四中", "一中", "八中", "九中",
    "人大附中", "北大附中", "清华附中", "北师大实验中学", "北师大二附中",
    "北师大附中", "首师大附中", "理工大附中", "交大附中", "民大附中",
    "中关村中学", "八一学校", "景山学校", "汇文中学", "广渠门中学",
    "东直门中学", "牛栏山一中", "和平街一中", "延庆一中", "海淀进修学校",
    "北京中学",
    # —— 常见学校人工补充 ——
    "十一学校", "一七一中学", "五十七中", "五十六中", "育英学校", "育英中学",
    "海淀实验中学", "陈经纶中学", "日坛中学", "工大附中", "三里屯一中",
    "潞河中学", "运河中学", "大峪中学", "京源学校", "北航实验学校", "石油附中",
    "科大附中", "地大附中", "矿院附中", "玉渊潭中学", "海淀外国语",
    "建华实验学校", "清华实验学校", "北大附中实验学校", "海淀教师进修学校",
)
# 长词优先（先命中"北京十二中"，而不是"十二中"截断）
_SCHOOL_WL_SORTED = tuple(sorted(_SCHOOL_WHITELIST, key=len, reverse=True))
# 白名单命中时的边界校验：pre 为虚词或"届级高初"（"十一届三中"的"届"、"高二中"的"高"）、
# post 为"全会考期"（"三中全会"的"全"、"中考"的"考"）一律判假。宁可漏检不可错配。
_WL_PRE_BAD = set("统的会与及各共被在为是并了等有对从向把让使得过着们这那些多届级高初")
_WL_POST_BAD = set("全会考期")

def _wl_boundary_ok(text, i, w):
    """白名单命中词的边界是否干净（前后字符不构成正文短语）。"""
    pre = text[i - 1] if i > 0 else ""
    post = text[i + len(w)] if i + len(w) < len(text) else ""
    if pre and pre in _WL_PRE_BAD: return False
    if post and post in _WL_POST_BAD: return False
    return True

def _school_wl_hit(text):
    """在 text 里找白名单校名（最长命中，含回补"北京"前缀）；无命中返回 ""。
    只用于 meta 兜底与终检——stem 抽取不走这里（见 _extract_school）。"""
    if not text: return ""
    best = ""
    for w in _SCHOOL_WL_SORTED:
        start = 0
        while True:
            i = text.find(w, start)
            if i < 0: break
            if _wl_boundary_ok(text, i, w):
                cand = ("北京" + w) if text[max(0, i - 2):i] == "北京" else w
                if len(cand) > len(best): best = cand
                break                       # 该词命中一次即可，省时间
            start = i + 1
    return best

# 学校名后缀（用于"北京市XX中学/XX附中/第N中学"这类结构）
_SCHOOL_TAILS = (r"(?:中学|学校|附中|附小|小学|初中|高中|职业学院|大学|学院|"
                 r"回民中学|实验中学|育英中学|潞河中学|进修学校)")

# "数字+中"模式（二中/八十中/一零一中/第101中学…）：模式驱动非白名单，任何"（北京市/第）+数字+中"或"北京市XX+学校后缀"都能抽（旧白名单静默丢校名）。
_SCHOOL_NUM_RX = re.compile(
    r"(?:(?:北京市?|第)\s*)?"
    r"((?:[零一二三四五六七八九十百]{1,4}|\d{1,4})中(?:学|附中)?)"
)
_SCHOOL_TAIL_RX = re.compile(r"((?:北京市?)?[\u4e00-\u9fa5]{2,10}?" + _SCHOOL_TAILS + r")")

# stem 模式的噪声词：正文短语误入候选时直接丢弃（历史坑："技术的对立统一中"含"的"、
# "多所高校规范大学"含"规范/高校"、"十一届三中"后跟"全会"）。
_SCHOOL_BAD_SUB = ("的", "全会", "统一", "规范", "高校", "被", "为", "是",
                   "并", "了", "等", "各", "多", "共", "含", "及", "与", "有",
                   "在", "对", "从", "向", "把", "让", "使", "得", "着", "过",
                   "期末", "期中", "月考", "开学", "毕业", "考试", "答案", "试题",
                   "中考", "高考", "联考", "统考", "模拟")

def _extract_school(hay, mode="stem"):
    """抽取学校名。mode 决定信任级别：
      "stem"：下载文件名（站点给的标题，可信）——结构三模式 + 白名单合并、取最长，
              带噪声过滤；
      "meta"：PDF 正文（不可信）——**只认白名单命中**（含边界校验），绝不泛化。
    历史教训：旧版对 全文 取最长，"十一届三中全会 / 技术的对立统一中 / 多所高校规范大学"
    把文件名里真正的校名（十五中/广渠门中学/北京十二中）覆盖掉——正文永远不能压倒文件名。
    三层策略（stem 模式）：1) 中文前缀 + 数字/中文数字 + 中(学)?（牛栏山一中/铁二中/八十中）；
    2) （北京）? + XX + 学校类后缀（清华附中/潞河中学）；3) （北京）? + 数字 + 中(学)（二中/九中/101中学）。
    结果的"北京"前缀先剥掉。"""
    if mode == "meta":
        return _school_wl_hit(hay)
    def _strip_bj(s):
        if s.startswith("北京市"): s = s[3:]
        elif s.startswith("北京"): s = s[2:]
        # 区前缀（"海淀区人大附中"来自旧命名）也剥掉，区信息由 region 字段独立表达
        m = re.match(r"^([\u4e00-\u9fa5]{1,3})区", s)
        if m and m.group(1) in _DISTRICTS: s = s[len(m.group(1)) + 1:]
        return s
    cands = []
    # 1) 带中文前缀的数字型：牛栏山一中 / 十里堡二中 / 铁二中
    for m in re.finditer(
            r"((?:北京市?|第)?[\u4e00-\u9fa5]{0,6}?"
            r"(?:[零一二三四五六七八九十百]{1,4}|\d{1,4})中(?:学|附中)?)", hay):
        cands.append(m.group(1))
    # 2) 后缀型：清华附中 / 中关村中学 / 潞河中学
    for m in _SCHOOL_TAIL_RX.finditer(hay): cands.append(m.group(1))
    # 3) 纯数字型兜底：二中 / 九中
    for m in _SCHOOL_NUM_RX.finditer(hay): cands.append(m.group(1))
    if not cands: return ""
    # 去噪：含虚词/正文短语的直接丢弃（不做前缀裁剪，宁可不要）
    cleaned = [c for c in cands if c and not any(b in c for b in _SCHOOL_BAD_SUB)]
    cleaned = [_strip_bj(c) for c in cleaned]
    cleaned = [c for c in cleaned if c and len(c) >= 2 and c != "北京"]
    if not cleaned: return ""
    # 冲突时优先更长的（信息更全）；长度相同取先出现的
    return max(cleaned, key=lambda s: (len(s), -hay.find(s)))

# 科目匹配"位置优先"：不能按 _SUBJECTS 列表顺序决胜负（与文字位置无关），否则延庆地理卷因正文出现
# "历史"二字被判成历史、生物卷因"化学"被判成化学；改为取 hay 中起始位置最靠前的科目，同位置取更长者。
_SUBJECT_ALIASES = (
    ("思想政治", "思想政治"),
    ("理科综合", "理科综合"),
    ("文科综合", "文科综合"),
    ("信息技术", "信息技术"),
    ("通用技术", "通用技术"),
    ("理综", "理综"),
    ("文综", "文综"),
    ("语文", "语文"),
    ("数学", "数学"),
    ("英语", "英语"),
    ("物理", "物理"),
    ("化学", "化学"),
    ("生物", "生物"),
    ("历史", "历史"),
    ("地理", "地理"),
    ("政治", "政治"),
)

def _pick_subject(hay):
    """在 hay 中挑科目：按**出现位置**决胜负，位置相同取词更长者。返回 "" 表示没命中。"""
    best, best_pos, best_len = "", None, 0
    for key, norm in _SUBJECT_ALIASES:
        pos = hay.find(key)
        if pos < 0: continue
        if best_pos is None or pos < best_pos or (pos == best_pos and len(key) > best_len):
            best, best_pos, best_len = norm, pos, len(key)
    return best

# 年份合法区间（校验门第一道）：明显越界的一律不采用
_YEAR_MIN, _YEAR_MAX = 2010, 2035

def _norm_year(text):
    """在 text 里找合法年份（2010~2035）；找不到返回 ""。"""
    for m in re.finditer(r"(20\d{2})", text or ""):
        if _YEAR_MIN <= int(m.group(1)) <= _YEAR_MAX:
            return m.group(1)
    return ""

def _meta_title_text(meta):
    """正文**标题区**：剔除“第N页/共M页”水印行后取前 3 个有效行并折叠字间空格。
    科目必须只看标题，不能扫整页：标题科目字常带空格（“语    文”），而题干/解析里的
    科目标词（“描述…的物理量”“降低化学反应…”）扫整页会造成假冲突，阻断合法改名。"""
    if not meta:
        return ""
    keep = []
    for l in (x.strip() for x in meta.splitlines()):
        if not l:
            continue
        if re.match(r"^第\s*\d+\s*页\s*/\s*共\s*\d+\s*页$", l):
            continue                      # 页码水印行不算标题区
        keep.append(l)
        if len(keep) >= 3:
            break
    return re.sub(r"\s+", "", "\n".join(keep))


def parse_pdf_name(text, meta_text=""):
    """从文件名（stem）与首页文字（meta）抽取规范字段，返回 dict。
    **字段信任级别（用户规则：原名优先，正文绝不覆盖关键字段）**：
      - 所有字段一律 **stem 优先**；
      - meta 仅当 stem 完全缺失时兜底：school 必须白名单命中（_extract_school mode="meta"），
        region 必须白名单命中，year/grade/subject/answer 为强词典/正则约束；
      - month/exam **不从 meta 兜底**（正文里的"6月""期中"等短语不得改写原文件名语义）。
    历史教训：旧版把 stem+meta 拼成单一 hay 统一正则——正文"十一届三中全会/技术的对立统一中/
    多所高校规范大学"被当成学校名，覆盖了文件名里真正的校名（十五中/广渠门中学/北京十二中）；
    科目按列表序取"首个命中"（正文出现"历史"二字，地理卷就被判成历史卷）。"""
    stem = text or ""
    meta = meta_text or ""
    hay = f"{stem}\n{meta}" if meta else stem
    out = {}
    # ---- 年：文件名优先；正文兜底也必须过 2010~2035 校验 ----
    out["year"] = _norm_year(stem) or _norm_year(meta)
    out["grade"] = (next((g for g in _GRADES if g in stem), "") or
                    next((g for g in _GRADES if g in meta), ""))
    # ---- 区：只在文件名里找；找不到再退回正文但必须命中已知区名（文件名"XX区"非贪婪取 1-3 汉字）----
    m = re.search(r"([\u4e00-\u9fa5]{1,3})区", stem)
    if m and m.group(1) in _DISTRICTS: out["region"] = m.group(1) + "区"
    else:
        # 2) 文件名里的裸区名（"延庆"这种不带"区"字的写法）
        hit = next((d for d in _DISTRICTS if d in stem), "")
        if hit: out["region"] = hit + ("区" if hit != "燕山" else "")
        else:
            # 3) 正文兜底：**必须命中已知区名白名单**，绝不接受任意"XX区"
            m = re.search(r"([\u4e00-\u9fa5]{1,3})区", meta)
            if m and m.group(1) in _DISTRICTS: out["region"] = m.group(1) + "区"
            else:
                hit = next((d for d in _DISTRICTS if d in meta), "")
                out["region"] = hit + ("区" if hit and hit != "燕山" else "")
    # ---- 学校：文件名结构抽取优先；正文兜底**只认白名单**（信任级别差异见 _extract_school）----
    out["school"] = _extract_school(stem, mode="stem") or _extract_school(meta, mode="meta")
    if out["school"] and out["region"] and out["school"].replace("北京", "") == out["region"]:
        out["school"] = ""          # "北京海淀"被判成学校 -> 丢弃，交给 region 字段
    # ---- 月份 / 考试类型：**只认文件名**（正文里的"6月""期中"不得改写语义）----
    m = re.search(r"(\d{1,2})月", stem)
    out["month"] = (m.group(1) + "月") if m else ""
    out["exam"] = next((e for e in _EXAM_TYPES if e in stem), "")
    # ---- 科目（用户规则：正文首页标题为准，文件名前缀仅辅助；二者冲突 -> 记 conflict 拒绝改名）----
    # meta 只取标题区（前 3 有效行、折叠字间空格），题干/解析里的科目标词不得参与判冲突。
    sub_stem = _pick_subject(stem)
    sub_meta = _pick_subject(_meta_title_text(meta))
    if sub_stem and sub_meta and sub_stem != sub_meta:
        out["subject"] = sub_meta               # 正文优先取值
        out["subject_conflict"] = True          # 但冲突：_rename_guard 据此拒绝改名
        out["subject_in_name"] = sub_stem       # 留存文件名前缀科目（冲突审核报表用）
    else:
        out["subject"] = sub_meta or sub_stem    # 正文有则用正文，否则文件名
        out["subject_conflict"] = False
        out["subject_in_name"] = ""
    # ---- 答案（用户规则：只从“文件名显式关键词”抽取，绝不默认补“有答案”；无关键词留空）----
    ans = ""
    for label, keys in _ANSWER_STATES:
        if any(k in stem for k in keys):        # 仅 stem：正文/题干里的“答案”不作数
            ans = label
            break
    if any(k in stem for k in ("无答案", "不含答案", "无解答", "学生版", "空白卷")):
        ans = "无答案"                           # 显式负向优先，杜绝“无答案被补成有答案”
    out["answer"] = ans
    out["raw"] = text
    return out

def _pdf_first_page_text(path, pages=1):
    try:
        import pymupdf as fitz
        with fitz.open(path) as d: return "\n".join(d[i].get_text() for i in range(min(pages, d.page_count)))
    except Exception: return ""

def build_norm_name(f, fallback_stem=""):
    """按字段拼规范化文件名；字段太少就保留原名（不乱改）。"""
    parts = []
    if f.get("year"): parts.append(f["year"])
    if "北京" not in (f.get("school") or "") and not f.get("region"): parts.append("北京")
    for k in ("region", "school", "grade", "month", "exam", "subject", "answer"):
        if f.get(k): parts.append(f[k])
    name = re.sub(r'[\\/:*?"<>|\r\n\t]', "_", "".join(parts)).strip(" .")
    return name if len(name) >= 6 else fallback_stem

def _rename_guard(f, stem):
    """重命名输出前的硬校验（用户规则：区/学校/科目/年份必须命中词典，否则回退原文件名）。
    返回 (ok, reason)；ok=False 时调用方保留原文件名（changed=False，建议清单里可见原因）。"""
    y = str(f.get("year") or "")
    if y and not (_YEAR_MIN <= int(y) <= _YEAR_MAX):
        return False, f"年份越界({y})"
    rg = str(f.get("region") or "")
    if rg and rg.rstrip("区") not in _DISTRICTS:
        return False, f"区名未命中词典({rg})"
    sj = str(f.get("subject") or "")
    if sj and sj not in _SUBJECTS:
        return False, f"科目未命中词典({sj})"
    if f.get("subject_conflict"):
        # 文件名前缀科目与正文首页科目不一致：不自动改名，保留原名交人工（杜绝“物理被改成数学”）
        return False, "科目冲突(文件名前缀≠正文)，拒绝改名"
    sc = str(f.get("school") or "")
    if sc:
        # 学校：要么在 stem 里实打实出现（含去"北京"形式），要么白名单命中（meta 抽取已保证）
        _sc_ok = (sc in stem or sc.replace("北京", "") in stem or
                  _school_wl_hit(sc) == sc)
        if not _sc_ok:
            return False, f"学校未通过校验({sc})"
    if not (y or rg or sc):
        return False, "关键字段不足(年份/区/学校均缺)"
    return True, ""

def unique_path(dst: Path):
    """目标已存在则返回 _1/_2/_3... 尾缀路径。绝不返回已存在的路径（防覆盖）。"""
    if not dst.exists(): return dst
    stem, suf, parent = dst.stem, dst.suffix, dst.parent
    n = 1
    while True:
        cand = parent / f"{stem}_{n}{suf}"
        if not cand.exists(): return cand
        n += 1

def plan_renames(root, limit=None, use_text=True, use_ocr=False, recursive=True, files=None):
    """生成改名建议清单（不改任何文件）。[!] 必须走 iter_pdfs() 而非 root.glob()——只有 iter_pdfs
    内部才有 ONLY_SUBDIR（--only-subdir/--url 自动限定）过滤，直接扫盘曾导致整库（含旧库）被一起
    改名。recursive=False 退化为只扫根目录一层；files 非 None 时直接用（批次清单已限定）。"""
    root = Path(root)
    # iter_pdfs() 产出 str 路径（供 glob/subprocess 用），这里要 Path 才能取 .stem/.name/.with_name。
    if files is not None: files = [Path(x) for x in files]
    elif recursive: files = [Path(x) for x in iter_pdfs(str(root))]
    else: files = sorted(root.glob("*.pdf"))
    if limit: files = files[:limit]
    plan = []
    guard_skips = []
    for p in files:
        stem = p.stem
        meta = _pdf_first_page_text(p) if use_text else ""
        f = parse_pdf_name(stem, meta)
        # L3：前两层缺关键字段且用户显式开启时才跑 OCR（贵）
        if use_ocr and (not f["subject"] or not f["year"]):
            try:
                f2 = _ocr_one_text(p)
                if f2: f = parse_pdf_name(stem, meta + "\n" + f2)
            except Exception: pass
        # 校验门：区/学校/科目/年份必须命中词典，不通过 -> 保留原文件名（不改名）
        _ok, _why = _rename_guard(f, stem)
        new_stem = build_norm_name(f, fallback_stem=stem)
        # 信息保真：stem 里出现的考试限定词必须全部保留到新名，丢任一 -> 拒绝改名（杜绝“丢三模/考前热身/保温卷”）。
        if _ok and new_stem != stem:
            _lost = [e for e in _EXAM_TYPES if e in stem and e not in new_stem]
            if _lost:
                _ok = False
                _why = (_why + "；" if _why else "") + f"限定词丢失({','.join(_lost)})"
        if not _ok:
            new_stem = stem
            guard_skips.append((p.name, _why))
        plan.append({"src": str(p), "old": p.name, "new_stem": new_stem,
                     "dst": str(p.with_name(new_stem + p.suffix)),
                     "changed": new_stem != stem,
                     "guard": ("" if _ok else _why),
                     "fields": {k: v for k, v in f.items() if k != "raw"}})
    if guard_skips:
        print(f"    [校验] {len(guard_skips)} 个文件未通过字段校验，保留原文件名：")
        for _n, _w in guard_skips[:12]:
            print(f"      - {_n}  ({_w})")
        if len(guard_skips) > 12: print(f"      ... 其余 {len(guard_skips) - 12} 个略")
    return plan

def _ocr_one_text(path):
    """L3 OCR 兜底：调 OCR 仓库给单文件加层后读回文字。仅用户显式开启时使用。"""
    sys.path.insert(0, str(OCR_REPO))
    from BatchOcrPdfAddTextLayer import check_pdf_ocr_add_text
    check_pdf_ocr_add_text(path)
    return _pdf_first_page_text(path, pages=2)

def _sha256_file(path):
    """文件内容 sha256（十六进制）；读失败返回 None。进程内按路径缓存，去重比对不重复哈希。"""
    import hashlib
    p = str(path)
    c = getattr(_sha256_file, "_cache", None)
    if c is None:
        c = _sha256_file._cache = {}
    try:
        _st = os.stat(p)
        sig = (p, _st.st_mtime_ns, _st.st_size)
    except OSError:
        return None
    if sig in c: return c[sig]
    try:
        hs = hashlib.sha256()
        with open(p, "rb") as f:
            for chunk in iter(lambda: f.read(1 << 20), b""):
                hs.update(chunk)
        c[sig] = hs.hexdigest()
    except OSError:
        c[sig] = None
    return c[sig]


def _same_content(src, dst):
    """内容是否一致：先比大小（廉价），大小相同再比 sha256。两者都一致才判重复。"""
    try:
        if Path(src).stat().st_size != Path(dst).stat().st_size:
            return False
    except OSError:
        return False
    hs, hd = _sha256_file(src), _sha256_file(dst)
    return hs is not None and hs == hd


def _fmt_rename_fields(fields):
    """将抽取字段拼为人读串（供“原名 -> 新名 -> 抽取字段”三段式日志）。"""
    fields = fields or {}
    _keys = [("year", "年"), ("region", "区"), ("school", "校"), ("grade", "年级"),
             ("month", "月"), ("exam", "考试"), ("subject", "科目"), ("answer", "答案")]
    s = " ".join(f"{lab}={fields[k]}" for k, lab in _keys if fields.get(k))
    if fields.get("subject_conflict"):
        s += " [!科目冲突]"
    return s or "(无)"


# 改名冲突审核工作表（真事故文件出口：不只留日志，逐条可审可处理）；测试隔离模式写入隔离目录
RENAME_CONFLICT_XLSX = WORK / "文库资源" / "_报告" / "rename_conflicts.xlsx"


def _collect_rename_conflicts(plan):
    """从改名计划提取冲突/拒绝条目：科目冲突（文件名前缀≠正文标题）与目标撞名类拒绝。
    每行：原名 / 正文识别科目 / 文件名科目 / 冲突字段 / 建议新名(正文优先) / 状态。"""
    rows = []
    for item in plan:
        fd = item.get("fields") or {}
        g = item.get("guard") or ""
        is_conf = bool(fd.get("subject_conflict")) or ("冲突" in g)
        if not is_conf:
            continue
        stem = Path(item["old"]).stem
        f2 = {k: v for k, v in fd.items() if k not in ("raw",)}
        f2["subject_conflict"] = False
        try:
            sug = build_norm_name(f2, fallback_stem=stem)
        except Exception:
            sug = ""
        if sug == stem:
            sug = ""                      # 建议名与原名相同 = 无可建议
        if item.get("by_body_fix"):
            status = "已按正文强改（冲突留档）"
        elif fd.get("subject_conflict"):
            status = "拒绝改名：科目冲突，待人工复核"
        else:
            status = f"保留原名：{g}"
        rows.append({"old": item["old"], "src": item["src"],
                     "body": fd.get("subject") or "(未识别)",
                     "name_sub": fd.get("subject_in_name") or _pick_subject(stem) or "(无)",
                     "conflict": g or "科目冲突(文件名前缀≠正文)",
                     "suggest": (sug + ".pdf") if sug else "(无建议)",
                     "status": status})
    return rows


def write_rename_conflicts_xlsx(plan, out_path=None):
    """生成/刷新 _报告\\rename_conflicts.xlsx（每次改名步骤全量覆写，含本轮处理结果）。
    无冲突也写表（单行“本轮无冲突”），避免旧报表误导人工审核。失败只告警不阻断主流程。"""
    rows = _collect_rename_conflicts(plan)
    p = Path(out_path) if out_path else RENAME_CONFLICT_XLSX
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill
        p.parent.mkdir(parents=True, exist_ok=True)
        wb = Workbook()
        ws = wb.active
        ws.title = "改名冲突审核"
        ws.append(["原名", "正文识别科目", "文件名科目", "冲突字段/原因", "建议新名(按正文)", "状态", "当前路径"])
        for c in ws[1]:
            c.font = Font(bold=True)
            c.fill = PatternFill("solid", fgColor="DDEBF7")
        for r in rows:
            ws.append([r["old"], r["body"], r["name_sub"], r["conflict"],
                       r["suggest"], r["status"], r["src"]])
        if not rows:
            ws.append(["(本轮无冲突)", "", "", "", "", "无待人工复核条目", ""])
        for col, w in (("A", 46), ("B", 12), ("C", 12), ("D", 34), ("E", 46), ("F", 26), ("G", 60)):
            ws.column_dimensions[col].width = w
        wb.save(str(p))
        if rows:
            print(f">>> [rename] 冲突审核表已生成：{p}（{len(rows)} 条待人工复核，"
                  "可用 --step rename --rename-apply --fix-conflicts-by-body 按正文强制改名）")
        return len(rows)
    except Exception as e:
        print(f">>> [rename] [!] 冲突审核表写入失败（不阻断）：{type(e).__name__}: {e}")
        return 0


def show_rename_conflicts():
    """菜单[v]：展示改名冲突审核表内容（无文件/无冲突给中文提示）。"""
    p = RENAME_CONFLICT_XLSX
    print("\n" + _SEP)
    print(f"改名冲突审核表：{p}")
    print(_SEP)
    if not p.exists():
        print("  尚未生成过冲突表（跑过 --step rename 后若有冲突会自动生成）。")
        return
    try:
        from openpyxl import load_workbook
        wb = load_workbook(str(p), read_only=True)
        ws = wb.active
        n = 0
        for row in ws.iter_rows(values_only=True):
            vals = ["" if v is None else str(v) for v in row]
            if all(not v for v in vals):
                continue
            n += 1
            if n == 1:
                print("  " + " | ".join(vals))
                print("  " + "-" * 64)
                continue
            print("  " + " | ".join(v[:36] for v in vals))
        wb.close()
        if n <= 1:
            print("  （无冲突记录）")
        else:
            print(f"\n  共 {n - 1} 条。处理入口：python pipeline.py --step rename --rename-apply "
                  "--fix-conflicts-by-body（人工确认后按正文强制改名并留档）")
    except Exception as e:
        print(f"  [!] 读取失败：{type(e).__name__}: {e}")


def _promote_conflicts_by_body(plan):
    """--fix-conflicts-by-body：把科目冲突条目转为「正文优先」改名建议（用户已确认模式）。
    重过 guard + 限定词保护，不通过的不提升。返回提升条数。"""
    n = 0
    for item in plan:
        fd = item.get("fields") or {}
        if not fd.get("subject_conflict"):
            continue
        stem = Path(item["src"]).stem
        f2 = {k: v for k, v in fd.items() if k != "raw"}
        f2["subject_conflict"] = False
        ok2, why2 = _rename_guard(f2, stem)
        new_stem = build_norm_name(f2, fallback_stem=stem)
        if ok2:
            _lost = [e for e in _EXAM_TYPES if e in stem and e not in new_stem]
            if _lost:
                ok2 = False
        if not ok2 or new_stem == stem:
            continue
        item["new_stem"] = new_stem
        item["dst"] = str(Path(item["src"]).with_name(new_stem + Path(item["src"]).suffix))
        item["changed"] = True
        item["guard"] = ""
        item["by_body_fix"] = True
        n += 1
    return n


def apply_renames(plan, dry=True, log_path=None, bid=None, fid_of=None):
    """执行改名（内容感知）：理想名未占用则直接改名；理想名已占用时按内容哈希判定——
    内容相同 -> 判重复，源文件移入 _duplicates；内容不同 -> 判冲突，保留原文件名不改名。
    绝不自动把已有 _1 升级成 _2。返回 (改动数, 映射列表)，映射写 JSON 便于回滚。
    bid/fid_of 提供时同步写 pipeline.db 的 file_steps（rename started/ok/fail/skip）并推进 manifest
    路径；dry 模式不写库。"""
    fid_of = fid_of or {}
    mapping, n = [], 0
    for item in plan:
        if not item["changed"]: continue
        src = Path(item["src"])
        if not src.exists():
            log_err(item["old"], "源文件不存在，跳过")
            _wskip(bid, fid_of.get(item["src"]), STEP_RENAME, item["src"], dry, "源文件不存在")
            statusio.update_detail("rename", item["src"], item["old"], "FAIL", note="源文件不存在")
            continue
        ideal = Path(item["dst"])
        # 冲突预检：理想名已被占用时按内容哈希判定重复/冲突；绝不把已有 _1 自动升级成 _2。
        if ideal.exists():
            if _same_content(src, ideal):
                # 内容相同 -> 判重复：源文件移入 _duplicates（不生成 _N，不占正规名）
                dupdir = ideal.parent / "_duplicates"
                move_to = unique_path(dupdir / ideal.name)
                if dry:
                    print(f"  {item['old']}\n      [=重复] 与已存在的 {ideal.name} 内容一致 -> 移入 _duplicates")
                    statusio.update_detail("rename", item["src"], item["old"], "SKIP", note="重复（同哈希）-> _duplicates")
                    continue
                try: dupdir.mkdir(parents=True, exist_ok=True)
                except OSError: pass
                _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "started", src=item["src"])
                try:
                    os.replace(str(src), str(move_to))
                    log_ok(f"{item['old']} -> [重复] _duplicates/{move_to.name}")
                    ledgerio.log("rename", "SKIP", src_path=str(src), dst_path=str(move_to),
                                 note="内容重复 -> _duplicates")
                    _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "ok", src=item["src"],
                           dst=str(move_to), error="dup->_duplicates")
                    # 回填上游各步登记的旧路径，防 dst 悬空（reconcile 误回滚根因）
                    if bc is not None and bid and fid_of.get(item["src"]):
                        try: bc.db_repath(bid, fid_of[item["src"]], item["src"], str(move_to))
                        except Exception: pass
                    statusio.update_detail("rename", item["src"], item["old"], "DONE", note="重复同哈希 -> _duplicates")
                except OSError as e:
                    log_err(item["old"], f"去重移动失败: {e}")
                    _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "fail", src=item["src"], error=f"去重移动失败: {e}")
                    statusio.update_detail("rename", item["src"], item["old"], "FAIL", note=f"去重移动失败: {e}")
                continue
            # 内容不同 -> 冲突：保留原文件名、不改名、不生成 _N，交人工复核
            if dry:
                print(f"  {item['old']}\n      [!冲突] 目标 {ideal.name} 已存在且内容不同 -> 保留原文件名")
                statusio.update_detail("rename", item["src"], item["old"], "SKIP", note=f"冲突:{ideal.name}")
                continue
            _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "started", src=item["src"])
            _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "skip", src=item["src"],
                   dst=str(ideal), error="目标已存在且内容不同（保留原名）")
            statusio.update_detail("rename", item["src"], item["old"], "SKIP",
                                   note=f"冲突（{ideal.name} 内容不同）保留原名")
            print(f"    [!冲突] {item['old']} 目标 {ideal.name} 已存在且内容不同 -> 保留原名，未改名")
            continue
        # 理想名未被占用：正常改名（无撞名就不需要 _N 尾缀）
        dst = ideal
        if dry:
            print(f"  {item['old']}\n      -> {dst.name}\n      抽取字段: {_fmt_rename_fields(item.get('fields'))}")
            statusio.update_detail("rename", item["src"], item["old"], "SKIP", note=f"dry-run 建议 -> {dst.name}")
            n += 1
            continue
        _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "started", src=item["src"])
        try:
            os.replace(str(src), str(dst))
            mapping.append({"from": str(src), "to": str(dst)})
            log_ok(f"{item['old']} -> {dst.name}")
            print(f"      抽取字段: {_fmt_rename_fields(item.get('fields'))}")
            try: _sz = dst.stat().st_size
            except OSError: _sz = None
            ledgerio.log("rename", "DONE", src_path=str(src), dst_path=str(dst),
                         size_bytes=_sz, note=f"{src.name} -> {dst.name}")
            _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "ok", src=item["src"], dst=str(dst))
            if bc is not None and bid and fid_of.get(item["src"]):
                try: bc.update_file_path(bid, fid_of[item["src"]], str(dst))
                except Exception: pass
                # 回填上游各步（watermark/ocr/qc）登记的旧路径为新名：从根上杜绝 dst 悬空
                try: bc.db_repath(bid, fid_of[item["src"]], item["src"], str(dst))
                except Exception: pass
            statusio.update_detail("rename", item["src"], item["old"], "DONE",
                                   src_bytes=_sz, dst_bytes=_sz,
                                   note=f"-> {dst.name}")
            n += 1
        except OSError as e:
            log_err(item["old"], f"改名失败: {e}")
            ledgerio.log("rename", "FAIL", src_path=str(item["src"]), note=f"改名失败: {e}")
            _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "fail", src=item["src"], error=f"改名失败: {e}")
            statusio.update_detail("rename", item["src"], item["old"], "FAIL", note=f"改名失败: {e}")
    if not dry and mapping and log_path:
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "w", encoding="utf-8") as fh: json.dump(mapping, fh, ensure_ascii=False, indent=1)
        print(f">>> 改名映射已写入 {log_path}（回滚：--step rollback-rename）")
    return n, mapping

def rollback_renames(log_path, dry=False):
    """按改名映射回滚（把 to 改回 from）。安全要求（都是踩过的坑）：支持 dry=True 逐条预演不落盘；
    目标名已存在时**跳过并告警**（绝不覆盖已有文件）；源名不存在时**跳过并告警**（不在预期状态，
    需人工核对）；返回 (成功数, 跳过数, 失败数)，全部打印，不静默。"""
    with open(log_path, encoding="utf-8") as fh: mapping = json.load(fh)
    ok = skip = fail = 0
    for m in reversed(mapping):
        cur, tgt = Path(m["to"]), Path(m["from"])
        if not cur.exists():
            print(f"  [跳过] 当前文件不存在: {cur.name}")
            skip += 1
            continue
        if tgt.exists():
            print(f"  [跳过] 回滚目标已存在，拒覆盖: {tgt.name}")
            skip += 1
            continue
        if dry:
            print(f"  {cur.name}\n      -> 回滚为 {tgt.name}")
            ok += 1
            continue
        try:
            tgt.parent.mkdir(parents=True, exist_ok=True)
            os.replace(str(cur), str(tgt))
            print(f"  [OK] {cur.name} -> {tgt.name}")
            ok += 1
        except OSError as e:
            print(f"  [失败] {cur.name}: {e}")
            fail += 1
    tag = "(dry-run)" if dry else ""
    print(f">>> {tag} 回滚：成功 {ok} / 跳过 {skip} / 失败 {fail}")
    return ok, skip, fail

def _ask_rename_choice():
    """重命名三选：1=生成建议清单 2=应用重命名 3=跳过（默认 1，零风险）。
    AUTO_MODE：自动选 2（应用重命名），重名仍自动加 _1/_2 尾缀，绝不覆盖。"""
    if AUTO_MODE:
        print("  [自动模式] 重命名操作 -> [2] 应用重命名（自动接受建议）")
        return "apply"
    print("\n请选择重命名操作：")
    print("  [1] 生成建议清单（只预览，不改名）")
    print("  [2] 应用重命名（真改名；重名自动加 _1/_2 尾缀，绝不覆盖）")
    print("  [3] 跳过重命名")
    try: c = input("请输入 [1/2/3]: ").strip() or "1"
    except (EOFError, KeyboardInterrupt):
        print()
        c = "1"
    return {"1": "list", "2": "apply", "3": "skip"}.get(c, "list")

def step_rename(_con=None, root=None, limit=None, dry=True, use_ocr=False,
                use_text=True, log_path=None, batch_id=None, ask=False,
                fix_conflicts=False):
    """重命名步骤。批次模式：文件范围来自当前批次 manifest（scan_pdfs("cleaned")），禁止全目录
    扫描；显式传 root 时为手工模式。ask=True 时询问三选：建议清单/应用/跳过；跳过也写
    pipeline.db: rename skipped，并提示打包文件名可能未优化。
    fix_conflicts=True（--fix-conflicts-by-body）：科目冲突条目转为「正文优先」改名建议，
    随本轮一并应用（CLI 显式传参即人工确认；仅单步 rename 可用，绝不参与 auto/all）；
    无论哪种模式，冲突审核表 _报告\\rename_conflicts.xlsx 均刷新留档。"""
    bid = _resolve_bid(batch_id)
    fid_of, files = {}, None
    if root:
        base = Path(root)
        if bid:
            print(">>> [rename] 手工模式（显式 root）：不按批次 manifest 过滤")
            bid = None
    else:
        base = STAGE_CLEANED
        if bc is not None and bid:
            pairs = _scan("cleaned", bid)
            files = [p for _f, p in pairs]
            fid_of = {p: _f for _f, p in pairs}
            if not files:
                print(f">>> [rename] 本批次在「去水印后」目录没有 PDF（批次 {bid}）。")
                print("        若刚去过水印，请确认产物已生成；否则先跑 步骤2 去水印。")
                return 0
        else:
            print(">>> [rename] 没有当前批次（_批次/current.json 不存在）。")
            print("        请先执行 步骤1 下载 + 步骤2 去水印，或显式传 --rename-root <目录>。")
            return 0
    print(f">>> [rename] 根目录 {base}（{'dry-run 预览' if dry else '正式执行'}）")
    if not base.is_dir():
        log_err(str(base), "目录不存在")
        return 0
    plan = plan_renames(base, limit=limit, use_text=use_text, use_ocr=use_ocr, files=files)
    if fix_conflicts and not dry:
        _np = _promote_conflicts_by_body(plan)
        print(f">>> [rename] --fix-conflicts-by-body：{_np} 条科目冲突转为正文优先改名建议（随本轮应用并留档）")
    elif fix_conflicts and dry:
        print(">>> [rename] [!] --fix-conflicts-by-body 需配合 --rename-apply 才生效（当前 dry-run 仅预览不改名）")
    # 冲突审核表：无论有无改名建议都刷新（含 by_body_fix 状态），真事故文件逐条可审可处理
    write_rename_conflicts_xlsx(plan)
    changed = [x for x in plan if x["changed"]]
    print(f"    共扫描 {len(plan)} 个，建议改名 {len(changed)} 个")
    statusio.begin_step("rename", total=len(changed), out_dir=str(base))
    if not changed:
        print("    无需改名。")
        for item in plan: _wskip(bid, fid_of.get(item["src"]), STEP_RENAME, item["src"], dry, "无需改名")
        statusio.end_step("rename", note="无需改名")
        return 0
    for item in changed[:50]:
        got = " ".join(v for v in item["fields"].values() if v)
        print(f"  · {item['old']}")
        print(f"      -> {item['new_stem']}.pdf")
        print(f"         识别字段: {got}")
    if len(changed) > 50: print(f"  ... 其余 {len(changed) - 50} 个略")
    # ---- 决策：三选（ask=True 时必须明确询问；否则按 dry 参数） ----
    mode = "apply" if not dry else "list"
    if ask: mode = _ask_rename_choice()
    if mode == "skip":
        print(">>> [rename] 已跳过重命名（pipeline.db: rename skipped）。")
        print("    提示：打包文件名可能未优化（仍使用原始文件名）。")
        for item in plan: _wskip(bid, fid_of.get(item["src"]), STEP_RENAME, item["src"], False, "用户选择跳过重命名")
        statusio.end_step("rename", status="SKIP", note="用户选择跳过（skipped）")
        return 0
    if mode == "list":
        print(">>> [rename] 仅生成建议清单，未执行改名（pipeline.db 不写 ok）。")
        # 看板：dry-run 也逐条记 SKIP 明细（不落盘任何文件）
        for _x in changed:
            statusio.update_detail("rename", _x["src"], _x["old"], "SKIP", note=f"建议清单 -> {_x['new_stem']}.pdf")
        statusio.end_step("rename", status="SKIP", note="建议清单（未改名）")
        print("    提示：加 --rename-apply 才会真正改名；或在向导里选 [2] 应用重命名。")
        return len(changed)
    # ---- mode == "apply"：正式改名 ----
    n, _ = apply_renames(plan, dry=False, log_path=log_path, bid=bid, fid_of=fid_of)
    for item in plan:   # 无需改动的文件落 skip 终态（状态表完整）
        if not item["changed"]: _wskip(bid, fid_of.get(item["src"]), STEP_RENAME, item["src"], False, "无需改名")
    print(f">>> [rename] 完成 {n} 个（重名自动加 _1/_2 尾缀，绝不覆盖）")
    statusio.end_step("rename", note=f"完成 {n} 个")
    return n

# 打包（zip）—— 本轮机最近一次正式打包生成的 zip（供全流程收尾「最终 zip 是否存在」检查）
_LAST_PACK_ZIP = None
# 整专题打包（topic 模式）一轮可产出多个 zip：伴生列表记全部，_final_zip_check 逐个核存在性；
# _LAST_PACK_ZIP 保留作单 zip 兼容语义（legacy 路径只产一个，只写它；topic 路径两者都写）。
_LAST_PACK_ZIPS = []


def _pack_mode():
    """打包模型开关（读环境变量，运行时可切换，无需重载模块）：
    GAOKZX_PACK_MODE = "topic"（默认）：整 run 合并，一个专题一个 <topic>.zip；
                     "batch"：旧行为，每批一个 zip（含 _rN 分片），完全等同 step_pack_legacy。"""
    return (os.environ.get("GAOKZX_PACK_MODE") or "topic").strip().lower()


def pack_folder(folder, dst_zip, arcbase=None, compresslevel=6):
    """把目录 folder 递归压缩成单个 dst_zip（os.walk 全量，仅收真实存在的文件、跳过 dst 自身），
    arcname 相对 arcbase（默认=folder，zip 根即 folder 内容）。返回 (写入文件数, dst_zip)。
    注：run 级合并打包（step_pack_topic）刻意不用本函数做全目录扫描，而按 manifest file_id 枚举
    + resolve_file_path 权威定位（承修 1 反截断，禁全目录扫描）；本函数保留作通用/手工整目录压包之用。"""
    import zipfile
    folder = Path(folder)
    base = Path(arcbase) if arcbase else folder
    dst_abs = os.path.abspath(str(dst_zip))
    n = 0
    with zipfile.ZipFile(dst_zip, "w", zipfile.ZIP_DEFLATED, compresslevel=compresslevel) as zf:
        for _root, _dirs, _files in os.walk(folder):
            for fn in _files:
                fp = os.path.join(_root, fn)
                if os.path.abspath(fp) == dst_abs:
                    continue
                zf.write(fp, os.path.relpath(fp, str(base)))
                n += 1
    return n, str(dst_zip)


def step_pack_legacy(con=None, root=None, out=None, limit=None, dry=False, _comment="",
              batch_id=None, ask_apply=True, skip_wm=False,
              tag="", only_unpacked=False):
    """第 7 步：打包 zip（STEP_PACK=7）。源默认「已清广告」（STAGE_ADS）；仅 skip_wm=True（步骤 6 未跑/被放弃）
    时才回退「去水印后」（STAGE_CLEANED）且必须警告。只打包当前 manifest 内文件（禁全目录扫描）；打包前自动 QC；
    dry-run 预览后询问"是否正式执行？[y/N]"（默认 N）；正式执行成功才写 ok 并记 _LAST_PACK_ZIP。返回 zip 路径或 None。
    tag：zip 文件名后缀（分批流水线 _rN 区分轮次）；only_unpacked=True：跳过已打包（ok/skip）的文件
    （每轮只打包本轮新文件），全部已打包时返回 "ALREADY"。"""
    import zipfile
    global _LAST_PACK_ZIP
    bid = _resolve_bid(batch_id)
    if bc is None or not bid:
        print(">>> [pack] 没有当前批次（_批次/current.json 不存在）。")
        print("        打包必须在批次内执行：请先跑 步骤1 抓取下载。")
        return None
    # ---- 打包源决策：默认「已清广告」；明确跳过隐形水印清理才允许「去水印后」 ----
    if root:
        src_root = Path(root)
        if bc.norm_key(src_root) == bc.norm_key(STAGE_CLEANED) and not skip_wm:
            print(">>> [pack] [!] 警告：打包源是「去水印后」而非默认的「已清广告」——")
            print("             这意味着隐形水印（底边广告）清理被跳过，zip 可能含广告页！")
    elif skip_wm:
        src_root = STAGE_CLEANED
        print(">>> [pack] [!] 警告：已明确跳过隐形水印清理（步骤6），打包源回退为「去水印后」")
        print("             zip 中可能保留底边广告页，建议先跑 步骤6 后再打包。")
    else: src_root = STAGE_ADS
    if not src_root.is_dir():
        print(f">>> [pack] 打包源目录不存在：{src_root}")
        if src_root == STAGE_ADS:
            print("        提示：请先跑 步骤6 隐形水印清理（会生成「已清广告」成品）；")
            print("        或在向导中明确跳过（有警告）/ CLI 加 --skip-ads 后打包「去水印后」。")
        return None
    # ---- 批次文件范围（manifest file_id 全量枚举 + 权威路径解析；不依赖 manifest.path 当前指向的物理阶段）----
    # 断点续跑（--auto --start-stage ocr 等）会用 _revive_manifest_paths 把 manifest.path 回退到更靠前阶段，
    # 若只按 manifest.path 前缀过滤会漏件（pack 只抓到被改动的少数文件 -> 截断包）。改为：优先用落在
    # 打包源目录下的 manifest.path（精确、避免同名歧义），否则用 resolve_file_path 按文件名找回本阶段实际成品。
    mf = bc.load_manifest(bid) or {}
    _recs = [f for f in mf.get("files", []) if f.get("file_id")]
    if ONLY_FILE_IDS:
        _want = {str(x).strip().upper() for x in ONLY_FILE_IDS}
        _recs = [f for f in _recs if str(f.get("file_id")).strip().upper() in _want]
    _src_key = bc.norm_key(src_root)
    pairs, _seen = [], set()
    for _rec in _recs:
        _fid = _rec.get("file_id")
        _pm = str(_rec.get("path") or "")
        _chosen = None
        if _pm and os.path.isfile(_pm) and bc.norm_key(_pm).startswith(_src_key + os.sep):
            _chosen = os.path.abspath(_pm)          # manifest.path 权威且在本阶段 -> 精确采用
        else:
            _pr = bc.resolve_file_path(bid, _fid)   # 找回被回退/挪动到本阶段的实际成品
            if _pr and bc.norm_key(_pr).startswith(_src_key + os.sep):
                _chosen = _pr
        if not _chosen or _chosen in _seen or bc.is_test_path(_chosen):
            continue
        _seen.add(_chosen)
        pairs.append((_fid, _chosen))
    fid_of = {p: _f for _f, p in pairs}
    files = [p for _f, p in pairs]
    if only_unpacked and files:
        # 分批流水线（only_unpacked）：跳过 file_steps 中已打包（ok/skip）的文件，每轮只打包本轮新增。
        # run_id+batch_id+sha256 限定：batch_id 本轮唯一（天然隔离历史轮次）；再比内容 sha，
        # 避免新批次文件撞历史名（内容不同）被误判“已打包”。
        _packed = set()
        if bc is not None:
            for _f in files:
                try:
                    _st = bc.db_step_get(bid, fid_of.get(_f), STEP_PACK) or {}
                    if str(_st.get("status") or "") not in ("ok", "skip"):
                        continue
                    _psha = _st.get("src_sha")
                    if _psha and _sha256_file(_f) != _psha:
                        continue          # 同名但内容已变 -> 不算已打包，需重新打包
                    _packed.add(_f)
                except Exception: pass
        if _packed:
            print(f">>> [pack] 分轮去重：{len(_packed)} 个文件此前已打包过，本轮跳过")
        files = [_f for _f in files if _f not in _packed]
        if not files:
            print(f">>> [pack] 本批次文件均已打包过（分轮去重），无需重复打包。")
            return "ALREADY"
    if limit: files = files[:limit]
    if not files:
        print(f">>> [pack] 本批次在「{src_root.name}」没有可打包的 PDF（batch {bid}）。")
        if src_root == STAGE_ADS: print("        提示：若尚未跑 步骤6 隐形水印清理，请先跑它；" "或在向导中明确跳过（有警告）。")
        return None
    dst_dir = Path(out) if out else STAGE_PACKED
    dst_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d")
    # 单文件调度（看板 --file-id）出"单文件小包"（按文件名，绝不覆盖整批包）；批次/全流程出整批包（{源目录名}_{日期}.zip）。
    if ONLY_FILE_IDS and len(files) == 1:
        _stem = Path(files[0]).stem[:60]
        zip_path = dst_dir / f"{_stem}_{stamp}.zip"
        print(f">>> [pack] 单文件打包模式（file_id={fid_of.get(files[0])}）")
    else: zip_path = dst_dir / f"{src_root.name}_{stamp}_{bid[:16]}{tag}.zip"   # bid[:16] 防同日多批互相覆盖
    print(f">>> [pack] 源 {src_root}（本批次 {len(files)} 个 PDF）-> {zip_path}")
    statusio.begin_step("pack", total=len(files), out_dir=str(dst_dir), note=f"zip: {zip_path.name}")
    while True:
        if dry:
            # ---- 预览：不生成 zip，也不写 file_steps（正式执行成功才写 ok） ----
            print(">>> [pack] (dry-run) 预览：未生成压缩包")
            if not ask_apply or not _ask_apply("打包 zip"):
                print(">>> [pack] 仅预览，未正式执行（pipeline.db 不写 ok）。")
                statusio.end_step("pack", status="SKIP", note="dry-run 预览")
                return str(zip_path)
            dry = False
            continue
        # ---- 正式执行：打包前自动 QC 一次（同样只查本批次 manifest 范围） ----
        _qc_stage = "ads" if src_root == STAGE_ADS else None
        print(f">>> [pack] 打包前自动 QC（批次 {bid}）...")
        _con, _tmp = con, False
        try:
            if _con is None:
                _con = db()
                _tmp = True
            step_qc(_con, False, None, batch_id=bid, stage=_qc_stage)
        except Exception as e: print(f">>> [pack] 打包前 QC 出错（{type(e).__name__}: {e}），" f"继续打包，请人工留意。")
        finally:
            if _tmp and _con is not None:
                try: _con.close()
                except Exception: pass
        total_bytes, n_fail = 0, 0
        with _wf_bar(len(files), "打包", enabled=True) as b:
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                for f in files:
                    fid = fid_of.get(f)
                    _bstep(bid, fid, STEP_PACK, "started", src=f)
                    arc = os.path.relpath(f, str(src_root))
                    try:
                        zf.write(f, arc)
                        total_bytes += os.path.getsize(f)
                        b.update(1, label=os.path.basename(f)[:38])
                        try: _sz = os.path.getsize(f)
                        except OSError: _sz = None
                        _bstep(bid, fid, STEP_PACK, "ok", src=f, dst=str(zip_path), src_sha=_sha256_file(f))
                        # 打包成功 = 该文件全流程完成 -> 标记「待验收」（看板可点[验收通过]/[打回重跑]）
                        if bc is not None:
                            _bstep(bid, fid, STEP_ACCEPT, bc.ST_PENDING_ACCEPT, src=f, dst=str(zip_path))
                        ledgerio.log("pack", "DONE", src_path=f,
                                     dst_path=str(zip_path), size_bytes=_sz,
                                     note=f"包内: {arc}")
                        statusio.update_detail("pack", f, os.path.basename(f), "DONE", src_bytes=_sz, note=arc)
                    except OSError as e:
                        n_fail += 1
                        b.update(1, label=os.path.basename(f)[:38], ok=False, name=os.path.basename(f))
                        log_err(os.path.basename(f), str(e))
                        _bstep(bid, fid, STEP_PACK, "fail", src=f, dst=str(zip_path), error=str(e)[:160])
                        ledgerio.log("pack", "FAIL", src_path=f, dst_path=str(zip_path), note=str(e)[:160])
                        statusio.update_detail("pack", f, os.path.basename(f), "FAIL", note=str(e)[:160])
        if not zip_path.exists():
            print(f">>> [pack] 打包失败：未生成 {zip_path}")
            statusio.end_step("pack", status="FAIL", note="zip 未生成")
            return None
        sz = os.path.getsize(zip_path) / 1024**3
        print(f">>> [pack] 完成：{zip_path}（{len(files)} 个，源 {total_bytes/1024**3:.2f}GB，" f"压缩后 {sz:.2f}GB）")
        if n_fail: print(f">>> [pack] [!] {n_fail} 个文件入包失败（详上），zip 不完整，请处理后重跑。")
        statusio.end_step("pack", note=f"zip: {zip_path.name}（{sz:.2f}GB）")
        _LAST_PACK_ZIP = str(zip_path)
        return str(zip_path)


# ---------------------- 整专题合并打包（问题 3，GAOKZX_PACK_MODE=topic） ----------------------
def step_pack(con=None, root=None, out=None, limit=None, dry=False, _comment="",
              batch_id=None, ask_apply=True, skip_wm=False,
              tag="", only_unpacked=False, run_id=None):
    """第 7 步打包（分派器）。GAOKZX_PACK_MODE=topic（默认）：整 run 合并，一专题一 zip（step_pack_topic）；
    =batch：旧行为（每批一 zip，含 _rN 分片），完全走 step_pack_legacy。
    单文件调度（ONLY_FILE_IDS）恒走 legacy 小包，不受开关影响。tag/only_unpacked 仅 legacy 有意义。"""
    if _pack_mode() != "topic" or ONLY_FILE_IDS:
        return step_pack_legacy(con, root, out, limit, dry, _comment, batch_id,
                                ask_apply, skip_wm, tag, only_unpacked)
    return step_pack_topic(con, root, out, limit, dry, _comment, batch_id,
                           ask_apply, skip_wm, run_id=run_id)


def step_pack_topic(con=None, root=None, out=None, limit=None, dry=False, _comment="",
                    batch_id=None, ask_apply=True, skip_wm=False, run_id=None):
    """整 run 合并打包：把同一次运行(run_id)下所有批次、经 resolve_file_path 权威定位（禁全目录
    扫描，承修 1 反截断）落在打包源目录下的成品，按专题目录分组，一专题产出一个 <topic>.zip
    （arcname 相对打包源根，保留 <专题>/…/文件 结构）。每个文件的 step=7 ok 按其原始
    (batch_id, file_id) 写入，dst 指向所属专题 zip——故 _batch_zips_exist 逐批仍可见、自动验收照常。
    返回：产出的 zip 路径列表（无产出则 None）。
    注：不重复触发 QC——调用方（_run_auto_impl）在流水线内 qc/ads 阶段刚跑过，打包前再跑属同轮重复；
    菜单单独 batch 打包路径由 step_pack_legacy 承担（其 pack 前 QC 语义保留在 legacy，不动）。"""
    import zipfile
    global _LAST_PACK_ZIP, _LAST_PACK_ZIPS
    bid0 = _resolve_bid(batch_id)
    if bc is None or (not bid0 and not run_id):
        print(">>> [pack] 没有当前批次（_批次/current.json 不存在），无法打包。")
        return None
    # ---- 打包源决策（与 legacy 同语义）：默认「已清广告」；skip_wm 才回退「去水印后」 ----
    if root:
        src_root = Path(root)
        if bc.norm_key(src_root) == bc.norm_key(STAGE_CLEANED) and not skip_wm:
            print(">>> [pack] [!] 警告：打包源是「去水印后」而非默认的「已清广告」——zip 可能含广告页！")
    elif skip_wm:
        src_root = STAGE_CLEANED
        print(">>> [pack] [!] 已明确跳过隐形水印清理（步骤6），打包源回退为「去水印后」，zip 中可能保留底边广告页。")
    else:
        src_root = STAGE_ADS
    if not src_root.is_dir():
        print(f">>> [pack] 打包源目录不存在：{src_root}")
        return None
    # ---- run 归属：显式 run_id 优先，否则由批次回推；仍无则退化为单批 ----
    run_id = run_id or (bc.db_run_of_batch(bid0) if bid0 else None)
    if run_id:
        bids = [b for b in (bc.db_batches_of_run(run_id) or []) if b]
    else:
        bids = [bid0] if bid0 else []
    if not bids:
        print(">>> [pack] 未找到可打包的批次。")
        return None
    # ---- 逐批枚举 manifest file_id + resolve 权威路径（禁全目录扫描，承修 1） ----
    _src_key = bc.norm_key(src_root)
    entries, _seen = [], set()          # (bid, fid, path)
    for b in bids:
        mf = bc.load_manifest(b) or {}
        for _rec in mf.get("files", []):
            _fid = _rec.get("file_id")
            if not _fid:
                continue
            _pm = str(_rec.get("path") or "")
            _chosen = None
            if _pm and os.path.isfile(_pm) and bc.norm_key(_pm).startswith(_src_key + os.sep):
                _chosen = os.path.abspath(_pm)          # manifest.path 权威且在本阶段 -> 精确采用
            else:
                _pr = bc.resolve_file_path(b, _fid)     # 找回被回退/挪动的实际成品
                if _pr and bc.norm_key(_pr).startswith(_src_key + os.sep):
                    _chosen = _pr
            if not _chosen or _chosen in _seen or bc.is_test_path(_chosen):
                continue
            _seen.add(_chosen)
            entries.append((b, _fid, _chosen))
    if limit:
        entries = entries[:limit]
    if not entries:
        print(f">>> [pack] 本次运行在「{src_root.name}」没有可打包的成品（run={run_id or bid0}）。")
        return None
    # ---- 按专题分组：arcname 相对 src_root，顶层目录名即专题；仅一层（无子目录）归入 "_散件" ----
    groups = {}                          # topic -> [(bid, fid, path, arc)]
    for b, fid, p in entries:
        arc = os.path.relpath(p, str(src_root))
        _parts = Path(arc).parts
        topic = _parts[0] if len(_parts) > 1 else "_散件"
        groups.setdefault(topic, []).append((b, fid, p, arc))
    dst_dir = Path(out) if out else STAGE_PACKED
    dst_dir.mkdir(parents=True, exist_ok=True)
    # ---- dry-run 预览：不生成 zip，不写 file_steps ----
    if dry:
        print(">>> [pack] (dry-run) 整专题打包预览：")
        for _t, _it in groups.items():
            print(f"        {_t}.zip  <- {len(_it)} 个 PDF")
        if not ask_apply or not _ask_apply("整专题打包 zip"):
            print(">>> [pack] 仅预览，未正式执行（pipeline.db 不写 ok）。")
            return None
        dry = False
    statusio.begin_step("pack", total=len(entries), out_dir=str(dst_dir),
                        note=f"{len(groups)} 个专题 zip")
    made, total_bytes, n_fail = [], 0, 0
    for topic, items in groups.items():
        zip_path = dst_dir / f"{topic}.zip"
        print(f">>> [pack] 整专题打包：{topic}（{len(items)} 个 PDF）-> {zip_path}")
        with _wf_bar(len(items), "打包", enabled=True) as b:
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                for bid, fid, p, arc in items:
                    _bstep(bid, fid, STEP_PACK, "started", src=p)
                    try:
                        zf.write(p, arc)
                        total_bytes += os.path.getsize(p)
                        b.update(1, label=os.path.basename(p)[:38])
                        try: _sz = os.path.getsize(p)
                        except OSError: _sz = None
                        _bstep(bid, fid, STEP_PACK, "ok", src=p, dst=str(zip_path), src_sha=_sha256_file(p))
                        _bstep(bid, fid, STEP_ACCEPT, bc.ST_PENDING_ACCEPT, src=p, dst=str(zip_path))
                        ledgerio.log("pack", "DONE", src_path=p, dst_path=str(zip_path),
                                     size_bytes=_sz, note=f"包内: {arc}")
                        statusio.update_detail("pack", p, os.path.basename(p), "DONE", src_bytes=_sz, note=arc)
                    except OSError as e:
                        n_fail += 1
                        b.update(1, label=os.path.basename(p)[:38], ok=False, name=os.path.basename(p))
                        log_err(os.path.basename(p), str(e))
                        _bstep(bid, fid, STEP_PACK, "fail", src=p, dst=str(zip_path), error=str(e)[:160])
                        ledgerio.log("pack", "FAIL", src_path=p, dst_path=str(zip_path), note=str(e)[:160])
                        statusio.update_detail("pack", p, os.path.basename(p), "FAIL", note=str(e)[:160])
        if zip_path.exists():
            made.append(str(zip_path))
    if not made:
        print(">>> [pack] 打包失败：未生成任何 zip")
        statusio.end_step("pack", status="FAIL", note="zip 未生成")
        return None
    statusio.end_step("pack", note=f"{len(made)} 个专题 zip")
    _LAST_PACK_ZIPS = list(made)          # 多专题全量记录（收尾逐个校验）
    _LAST_PACK_ZIP = made[0]              # 单 zip 兼容语义：非分批收尾仍只看这一个
    _gb = sum(os.path.getsize(z) for z in made) / 1024**3
    print(f">>> [pack] 整专题打包完成：{len(made)} 个 zip（{len(entries)} 个文件，源 {total_bytes/1024**3:.2f}GB，压缩后 {_gb:.2f}GB）")
    if n_fail:
        print(f">>> [pack] [!] {n_fail} 个文件入包失败（详上），zip 可能不完整。")
    return made


# ---------------------- 验收归档（看板 [验收通过]） ----------------------
def _stage_root_of(path):
    """判断路径属于哪个阶段根，返回 (tag, root)；不属于返回 (None, None)。"""
    try: ap = os.path.normcase(os.path.abspath(str(path)))
    except (OSError, ValueError): return None, None
    for tag, root in (("pending", STAGE_PENDING), ("cleaned", STAGE_CLEANED),
                      ("done", STAGE_DONE), ("ads", STAGE_ADS),
                      ("packed", STAGE_PACKED), ("completed", STAGE_COMPLETED)):
        rn = os.path.normcase(os.path.abspath(str(root)))
        if ap.startswith(rn + os.sep): return tag, root
    return None, None

def _parse_file_ids(v):
    """把 --file-id 参数（逗号分隔字符串/可迭代）规范成大写 set；空返回 None。"""
    if not v: return None
    xs = [str(x) for x in v] if isinstance(v, (set, list, tuple)) else str(v).split(",")
    out = {x.strip().upper() for x in xs if str(x).strip()}
    return out or None

def step_accept(batch_id=None, file_id=None):
    """验收通过（看板 [验收通过]）：把文件归档到「文库资源_completed」。file_id 指定只归档这些文件
    （逗号分隔），不传则归档本批次所有 pending_accept 的文件。动作：移动文件（保留相对阶段根目录结构）->
    更新 manifest 路径 -> file_steps step=8 写 accepted；目标已存在时拒绝覆盖（跳过）。返回 0=至少成功归档 1 个；1=无事可做/全部失败。"""
    if bc is None:
        print(">>> [accept] batchcore 不可用。")
        return 1
    bid = _resolve_bid(batch_id)
    if not bid:
        print(">>> [accept] 没有当前批次。")
        return 1
    mf = bc.load_manifest(bid) or {}
    fids = _parse_file_ids(file_id)
    targets = []
    for f in mf.get("files", []):
        fid = str(f.get("file_id") or "")
        if not fid: continue
        if fids is not None:
            if fid.upper() not in fids: continue
        else:
            # 批量模式：只归档「待验收」的文件（已验收/打回中的不动）
            st8 = bc.db_step_get(bid, fid, STEP_ACCEPT)
            if not st8 or st8.get("status") != "pending_accept": continue
        targets.append(f)
    if not targets:
        if fids: print(f">>> [accept] 批次 {bid} 中未找到指定文件" f"（{', '.join(sorted(fids))}）。")
        else: print(f">>> [accept] 批次 {bid} 没有「待验收」的文件可归档。")
        return 1
    STAGE_COMPLETED.mkdir(parents=True, exist_ok=True)
    n = 0
    for f in targets:
        fid = str(f.get("file_id") or "")
        p = str(f.get("path") or "")
        if not p or not os.path.isfile(p):
            print(f"  [跳过] {fid} 文件不存在: {p}")
            bc.db_step(bid, fid, STEP_ACCEPT, bc.ST_EXEC_FAIL, src=p, error=f"验收时文件不存在: {p}"[:200])
            continue
        tag, root = _stage_root_of(p)
        if tag == "completed":
            bc.db_step(bid, fid, STEP_ACCEPT, bc.ST_ACCEPTED, src=p, dst=p)
            n += 1
            continue
        if tag is None:
            print(f"  [跳过] {fid} 不在已知阶段目录: {p}")
            bc.db_step(bid, fid, STEP_ACCEPT, bc.ST_EXEC_FAIL, src=p, error=f"文件不在阶段目录内: {p}"[:200])
            continue
        rel = os.path.relpath(p, str(root))
        dst = STAGE_COMPLETED / rel
        if (os.path.normcase(os.path.abspath(str(dst))) != os.path.normcase(os.path.abspath(p)) and dst.exists()):
            print(f"  [跳过] {fid} 归档目标已存在，拒绝覆盖: {dst}")
            bc.db_step(bid, fid, STEP_ACCEPT, bc.ST_EXEC_FAIL, src=p, error=f"归档目标已存在: {dst.name}"[:200])
            continue
        try:
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(p, str(dst))
        except OSError as e:
            print(f"  [失败] {fid}: {e}")
            bc.db_step(bid, fid, STEP_ACCEPT, bc.ST_EXEC_FAIL, src=p, error=str(e)[:200])
            continue
        bc.update_file_path(bid, fid, str(dst))
        bc.db_step(bid, fid, STEP_ACCEPT, bc.ST_ACCEPTED, src=p, dst=str(dst))
        ledgerio.log("accept", "DONE", src_path=p, dst_path=str(dst), note="验收通过 -> 归档")
        print(f"  [OK] {fid} {os.path.basename(p)} -> {dst}")
        n += 1
    # 本批文件全部 accepted 时，批次状态推进为 accepted
    try:
        total = len(mf.get("files", []))
        acc_n = len({r["file_id"] for r in bc.db_files_of(bid, step=STEP_ACCEPT, status=bc.ST_ACCEPTED)})
        if total and acc_n >= total:
            bc.set_batch_status(bid, "accepted", note="全部文件验收归档")
            print(f">>> [accept] 批次 {bid} 全部 {total} 个文件已归档，批次状态 -> accepted")
    except Exception: pass
    print(f">>> [accept] 归档完成 {n}/{len(targets)} 个 -> {STAGE_COMPLETED}")
    return 0 if n else 1

def _final_zip_check():
    """全流程收尾：确认最终 zip 真实存在。成功 -> 返回非空 zip 路径字符串（truthy）；
    失败 -> 提示"全流程未真正完成"并返回 None（绝不返回空串，调用方 bool()/not 判定安全）。
    topic 整专题打包一轮可产出多个 zip（_LAST_PACK_ZIPS），逐个校验存在性，缺一即判失败。"""
    if _LAST_PACK_ZIPS:
        _missing = [z for z in _LAST_PACK_ZIPS if not Path(z).exists()]
        if _missing:
            print(f">>> [check] 全流程未真正完成：本轮机 {len(_LAST_PACK_ZIPS)} 个专题 zip 中 "
                  f"{len(_missing)} 个缺失：")
            for _m in _missing: print(f"      - {_m}")
            return None
        try: _szs = sum(Path(z).stat().st_size for z in _LAST_PACK_ZIPS) / 1024**3
        except OSError: _szs = 0
        print(f">>> [check] 本轮机 {len(_LAST_PACK_ZIPS)} 个专题 zip 均已确认存在（合计 {_szs:.2f}GB）")
        return _LAST_PACK_ZIPS[0]
    p = _LAST_PACK_ZIP
    if p and Path(p).exists():
        try: sz = Path(p).stat().st_size / 1024**3
        except OSError: sz = 0
        print(f">>> [check] 最终 zip 已确认存在：{p}（{sz:.2f}GB）")
        return p
    print(">>> [check] 全流程未真正完成：本轮机没有生成最终 zip。")
    print("            原因可能是：打包未执行 / 未正式执行（仅预览）/ 打包失败。")
    print("            请检查 步骤7 打包 zip 后重跑。")
    return None

# 调度层：真正干活的是 scan_bottom_ads.py（重新扫描生成新报告）+ clean_bottom_ads.py（只接受该
# 新报告，做 batch_id 硬校验）；两脚本自带看板写入，这里只负责按批次重扫 -> 消费新报告 -> 起子进程。
_ADS_SCRIPT = ROOT / "clean_bottom_ads.py"
_SCAN_SCRIPT = ROOT / "scan_bottom_ads.py"

def step_ads(dry=True, report=None, out=None, limit=None, batch_id=None, ask_apply=True,
             force=False):
    """第 6 步：隐形水印清理（重扫描 -> 消费本批新报告 -> 输出到「已清广告」）。生产模式（默认）：清理前必须在
    本批次上重新扫描生成 reports/bottom_ads_<batch_id>.json；clean_bottom_ads.py 只接受该新报告——缺失/过旧/
    batch_id 不匹配立即中止（rc=3）；绝不再读固定的旧报告「底边广告检查报告.json」。dry-run 预览后询问"是否正式执行？[y/N]"（默认 N）。返回 rc：0=正常；3=报告校验失败。"""
    if not _ADS_SCRIPT.exists():
        print(f">>> [ads] 找不到 {_ADS_SCRIPT}（本步骤不可用）")
        return 1
    bid = _resolve_bid(batch_id)
    if bc is None or not bid:
        print(">>> [ads] 没有当前批次（_批次/current.json 不存在）。")
        print("        隐形水印清理必须在批次内执行：请先跑 步骤1 抓取下载。")
        return 1
    # 空批次早退：本批次在清理源（cleaned/done）无 PDF 时不存在"报告缺失"问题，直接说明并跳过。
    if not (_scan("cleaned", bid) or _scan("done", bid)):
        print(f">>> [ads] 本批次在「去水印后 / 已处理」目录没有 PDF（batch {bid}）。")
        print("        清广告无需执行（请先跑 步骤2 去水印 或 步骤1 下载）。")
        return 0
    if report:
        # 手工模式（显式 --ads-report）：跳过重新扫描，但仍做 batch_id 校验
        rep = Path(report)
        print(f">>> [ads] [!] 手工模式（显式报告）：跳过重新扫描：{rep}")
    else:
        # 生产模式：强制重新扫描（只扫本批次 manifest 内的文件，禁止全目录扫描）
        print(f">>> [ads] 重新扫描本批次底边广告 -> {bc.batch_report_path(bid)}")
        _scan_cmd = [str(PY), str(_SCAN_SCRIPT), "--batch-id", bid]
        if ONLY_FILE_IDS:
            # 单文件调度（看板 --file-id）：只扫描指定文件，报告只含目标文件
            _fids_s = ",".join(sorted(ONLY_FILE_IDS))
            _scan_cmd += ["--file-id", _fids_s]
            print(f">>> [ads] 单文件模式：只扫描 {_fids_s}")
        r0 = subprocess.run(_scan_cmd)
        if r0.returncode != 0:
            print(f">>> [ads] 扫描失败（码 {r0.returncode}），中止清广告。")
            return 1
        rep = bc.batch_report_path(bid)
    if not rep.exists():
        print(f">>> [ads] 扫描报告不存在：{rep}（立即中止）")
        print(f"        请先运行： python scan_bottom_ads.py --batch-id {bid}")
        return 3
    def _run(mode):
        cmd = [str(PY), str(_ADS_SCRIPT), "--report", str(rep),
               "--batch-id", bid, "--out-root", str(out or STAGE_ADS)]
        if limit: cmd += ["--limit", str(limit)]
        cmd += ["--db-path", str(DB)]      # 台账跟随当前库（压测库/生产库）
        if force: cmd += ["--force"]      # 显式 --force：清端跳过三条件断点过滤，全量重跑
        if mode == "apply": cmd += ["--apply", "--fail_exit"]
        else: cmd += ["--dry-run"]
        print(">>> [ads] 运行:", " ".join(cmd))
        return subprocess.run(cmd).returncode
    # ---- dry-run 预览（clean 端 batch_id/过旧硬校验，失败即 rc=3）；仅 dry=True 预览+询问，dry=False 直接执行 ----
    if dry:
        rc = _run("dry")
        if rc == 3:
            print(">>> [ads] 报告校验失败（缺失/过旧/batch_id 不匹配），立即中止。")
            return 3
        if rc != 0:
            print(f">>> [ads] 预览失败（码 {rc}），中止。")
            return 1
        if not ask_apply or not _ask_apply("隐形水印清理"):
            print(">>> [ads] 仅预览，未正式执行（pipeline.db 不写 ok）。")
            return 0
    # ---- 正式执行（只有成功才由 clean 端写 file_steps 的 ok） ----
    print(">>> [ads] 确认正式执行 —— 写入「已清广告」目录 + 写 pipeline.db")
    rc = _run("apply")
    if rc == 3:
        print(">>> [ads] 报告校验失败，立即中止。")
        return 3
    if rc == 4:
        print(">>> [ads] 正式执行完成但有失败文件（详上）；需人工检查后重跑。")
        return 4
    if rc != 0:
        print(f">>> [ads] 正式执行失败（码 {rc}）。")
        return 1
    return 0

# ------------------------- 步骤 1：采集 + 下载 -------------------------
def _new_batch_for_url(url=None, name=None, topic="all", run_id=None):
    """为本次下载创建批次（manifest + current 指针 + batches 表）。失败返回 None。
    run_id：所属“一键全自动”运行分组键（分批流水线每轮一个新 batch_id，同属一个 run_id）。"""
    if bc is None: return None
    key = ""
    if url:
        m = re.search(r"/(\d{4,})\.html", url) or re.search(r"(\d{4,})", url)
        key = (m.group(1) if m else "url")
    else: key = f"topic{topic}"
    mf = bc.new_batch(url=url or "", name=name or "", key=key, run_id=run_id)
    bid = (mf or {}).get("batch_id")
    if bid:
        print(f">>> [batch] 本次任务批次 batch_id = {bid}" + (f"（run_id={run_id}）" if run_id else ""))
        print(f">>> [batch] manifest -> {bc.manifest_path(bid)}")
    return bid

def _is_benign_noise(line):
    """识别外部子进程输出的良性噪声行（Windows `where` 查找未命中时的“信息: 用提供的模式无法找到文件。”/
    “INFO: Could not find files ...”）：通配符未匹配属正常，不是错误，过滤掉避免被误读为 FAIL。"""
    if not line: return False
    s = line.strip()
    return ("用提供的模式无法找到" in s) or ("Could not find files for the given pattern" in s)

def step_discover_download(topic="all", limit=None, workers=6, delay=0.5,
                           url=None, name=None, dry=False, batch_id=None, offset=None,
                           expected_fp=None, paper_type=None):
    """采集 + 下载（步骤1）。每次任务先建批次（manifest），并把 --batch-id 传给下载器；下载器
    结束时输出 RESULT{target_pages, pdf_count, downloaded} 并以退出码 2 表示空结果。返回
    (rc, result)：rc=2 表示 0 目标/0 PDF —— 调用方必须立即中止全流程。
    offset：跳过前 N 个目标详情页（分批流水线用；透传下载器 --offset）。
    expected_fp：上次落库的目标页 URL 集合 sha1[:16]，与本次实际集合不一致时下载器会自动重置 offset=0
    并在 RESULT 里上报 offset_reset_reason='fingerprint_changed'（避免专题结构变化时旧 offset 越过新 total）。"""
    bid = batch_id
    if dry: print(">>> [discover/download] (dry-run) 只嗅探清单，不建批次、不下载")
    elif not bid: bid = _new_batch_for_url(url=url, name=name, topic=topic)
    cmd: list[str] = [str(PY), str(GAOKZX_DL)]
    if url:
        cmd += ["--url", str(url)]
        if name: cmd += ["--name", str(name)]
    else: cmd += ["--topic", topic]
    cmd += ["--workers", str(workers), "--delay", str(delay)]
    if limit: cmd += ["--limit", str(limit)]
    if offset: cmd += ["--offset", str(offset)]
    if expected_fp: cmd += ["--expected-fp", str(expected_fp)]
    if paper_type: cmd += ["--paper-type", str(paper_type)]
    if dry: cmd += ["--dry-run"]
    if bid: cmd += ["--batch-id", bid]
    cmd += ["--db-path", str(DB)]      # 台账跟随当前库（压测库/生产库）
    # E5 隔离：STAGE_PENDING 已随 WORK 落隔离树，直接传给下载器作落盘/清单根（非隔离=生产原行为）。
    _dl_out = STAGE_PENDING
    cmd += ["--out-root", str(_dl_out), "--list-dir", str(_dl_out / "清单")]
    print(">>> [discover/download] 运行:", " ".join(cmd))
    # 逐行转发下载器输出并抓取末尾 RESULT 行（下载器 print 均 flush）；子进程强制 UTF-8 输出。
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    result = None
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, encoding="utf-8", errors="replace", bufsize=1, env=env)
    try:
        for line in (proc.stdout or []):
            line = line.rstrip("\r\n")
            if line.startswith("RESULT"):
                try: result = json.loads(line[len("RESULT"):])
                except (ValueError, TypeError): result = None
                continue
            if line and not _is_benign_noise(line): print(line)
    finally: rc = proc.wait()
    if result:
        print(f">>> [download] 汇总：目标 {result.get('total_targets', result.get('target_pages'))} / "
              f"已存在 {result.get('skipped')} / 新下载 {result.get('downloaded')} / 失败 {result.get('failed')}")
    # 空结果硬保护：区分 (A) 真·空（目标页=0，正常）与 (B) 疑似解析失效（目标页>0 但 PDF=0，必须报警）。
    if rc == 2 or (result and result.get("empty") and not result.get("dry_run")):
        _tp = int((result or {}).get("target_pages") or 0)
        _pc = int((result or {}).get("pdf_count") or 0)
        # 情况 A：真·空（目标详情页本身就没解析出来）——正常信号，沿用 rc=2。
        if _tp == 0:
            print(">>> 未发现可下载PDF（目标详情页=0），已停止")
            return 2, result
        # 情况 B：疑似解析失效（有目标页但一页 PDF 都没有）——绝不当正常结束，返回 rc=3 硬中止。
        print(_SEP)
        print(">>> [SUSPICIOUS] 检测到疑似解析失效：")
        print(f">>>   目标详情页 = {_tp} 个，但 PDF 附件 = 0 个")
        print(">>>   可能原因：")
        print(">>>     1) 详情页结构已变化（gaokzx_parse.extract_files 需要更新）")
        print(">>>     2) 详情页本身是「下一级汇总页」（站点做了多层结构）")
        print(">>>     3) 页面改为 JS 渲染（静态 HTML 抓不到附件）")
        print(">>>   请人工检查前几个详情页的 HTML 结构后重试。")
        print(_SEP)
        # 落盘可疑证据，便于事后排查（url/topic 回填自函数入参）。
        try:
            _rep_dir = LOG_ROOT / "logs"
            _rep_dir.mkdir(parents=True, exist_ok=True)
            _stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            _rep = _rep_dir / f"suspect_parse_{_stamp}.json"
            with open(str(_rep), "w", encoding="utf-8") as _fh:
                json.dump({
                    "time": datetime.datetime.now().isoformat(),
                    "url": url or f"topic::{topic}",
                    "topic": topic,
                    "batch_id": bid,
                    "target_pages": _tp,
                    "pdf_count": _pc,
                    "downloaded": (result or {}).get("downloaded"),
                    "failed": (result or {}).get("failed"),
                    "result": result,
                }, _fh, ensure_ascii=False, indent=1)
            print(f">>> [SUSPICIOUS] 证据已落盘：{_rep}")
        except Exception as _e:
            print(f">>> [SUSPICIOUS] [!] 证据落盘失败（不阻断）：{type(_e).__name__}: {_e}")
        # rc=3：疑似解析失效 —— 写批次状态（不改游标/不标 accepted/不动 file_steps），让看板/下次运行可见。
        # 注：bc.set_batch_status 会同步更新 manifest.status 与 batches 表，但均不涉及游标/accepted/file_steps。
        if bc is not None and bid:
            try:
                bc.set_batch_status(
                    bid, "suspect_parse_failed",
                    note=f"疑似解析失效：目标详情页 {_tp} 个但 PDF 附件 0 个；"
                         f"详情页结构可能变化（详见 logs/suspect_parse_*.json）")
                print(f">>> [SUSPICIOUS] 批次 {bid} 状态 -> suspect_parse_failed（看板可见）")
            except Exception as _e:
                print(f">>> [SUSPICIOUS] [!] 批次状态写入失败（不阻断）：{type(_e).__name__}: {_e}")
        return 3, result   # rc=3 = 疑似解析失效（严格区分于 rc=2 真·空）
    if rc != 0:
        log_err("抓取下载", f"下载器异常退出（码 {rc}）")
        return (rc or 1), result
    print(">>> [discover/download] 完成")
    return 0, result


def _report_empty_run(url, topic, off=None, dres=None, batch_id=None,
                      reason=None, suggested_action=None):
    """空结束（真·空 rc=2）时落一份可读报告 logs/empty_run_*.json 并打印重跑选项；
    纯诊断辅助，任何异常都不得影响主流程（吞异常、仅告警）。
    reason：no_targets / offset_out_of_range / offset_at_end / config_error（供事后定位）。
    suggested_action：'rerun' / 'none' / 'investigate'。offset_at_end 时不再打印“建议重跑”提示。"""
    try:
        _rep_dir = LOG_ROOT / "logs"
        _rep_dir.mkdir(parents=True, exist_ok=True)
        _stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        _rep = _rep_dir / f"empty_run_{_stamp}.json"
        _tgt = (dres or {}).get("total_targets") if isinstance(dres, dict) else None
        _off_before = (dres or {}).get("offset_before") if isinstance(dres, dict) else None
        _reset = (dres or {}).get("offset_reset_reason") if isinstance(dres, dict) else None
        if reason is None:
            try:
                _off_i = int(off or 0); _tt_i = int(_tgt) if _tgt else 0
            except (TypeError, ValueError):
                _off_i, _tt_i = 0, 0
            if _reset:
                reason = _reset
            elif _off_i > 0 and _tt_i > 0 and _off_i >= _tt_i:
                reason = "offset_at_end"
            elif _off_i > 0 and _tt_i == 0:
                reason = "offset_out_of_range"
            else:
                reason = "no_targets"
        if suggested_action is None:
            suggested_action = "none" if reason == "offset_at_end" else (
                "auto-reset-in-progress" if reason in ("offset_out_of_range", "fingerprint_changed") else "rerun")
        with open(str(_rep), "w", encoding="utf-8") as _fh:
            json.dump({
                "time": datetime.datetime.now().isoformat(),
                "url": url or f"topic::{topic}",
                "topic": topic,
                "batch_id": batch_id,
                "cursor_offset": off,
                "offset_before": _off_before,
                "total_targets": _tgt,
                "reason": reason,
                "suggested_action": suggested_action,
                "result": dres,
            }, _fh, ensure_ascii=False, indent=1)
        print(f">>> [空跑] 已生成本轮空结束报告：{_rep}（reason={reason}）")
        if suggested_action == "none":
            print(">>> [空跑] 游标已自然抵达末尾 (offset == total_targets)：本次属正常结束，无需重跑。")
        else:
            print(">>> [空跑] 若确认该专题确有资源却 0 下载，可选重跑（游标已到末尾则无需）：")
            if url:
                print(f'>>>         python pipeline.py --auto --url "{url}"')
            else:
                print(f'>>>         python pipeline.py --auto --topic "{topic}"')
            print(">>>         python pipeline.py --step check-env   # 先核对环境/依赖")
    except Exception as _e:
        print(f">>> [空跑] [!] 报告生成失败（不阻断）：{type(_e).__name__}: {_e}")


def _load_recent_suspect_reports(max_n=5, within_hours=None):
    """读取 logs/suspect_parse_*.json 最近的 N 份；within_hours 非空时只保留该小时内的。
    返回 [{path, mtime, url, topic, target_pages, pdf_count, batch_id}, ...]，异常返回 []。"""
    out = []
    try:
        d = LOG_ROOT / "logs"
        if not d.is_dir(): return out
        files = sorted(d.glob("suspect_parse_*.json"),
                       key=lambda p: p.stat().st_mtime, reverse=True)
        import time as _t
        _now = _t.time()
        for p in files:
            if len(out) >= max_n: break
            try:
                _mt = p.stat().st_mtime
                if within_hours is not None and (_now - _mt) > within_hours * 3600:
                    continue
                with open(p, encoding="utf-8") as fh: _d = json.load(fh)
                out.append({
                    "path": p, "mtime": _mt,
                    "url": _d.get("url", ""),
                    "topic": _d.get("topic", ""),
                    "target_pages": _d.get("target_pages", 0),
                    "pdf_count": _d.get("pdf_count", 0),
                    "batch_id": _d.get("batch_id", ""),
                })
            except Exception:
                continue
    except Exception:
        return out
    return out


def _print_suspect_report(recent_n=3, full=False):
    """强制文字报告：打印最近 N 份 suspect_parse 报告的结构化摘要。
    full=True 时额外打印每份报告的可复制重跑命令 + 排查建议。
    任何异常都吞掉（诊断函数绝不影响主流程）。"""
    try:
        reps = _load_recent_suspect_reports(max_n=recent_n)
        if not reps:
            print("  （无解析失效报告）")
            return
        print(_SEP)
        print(f"解析失效报告（最近 {len(reps)} 份，详细见 logs/suspect_parse_*.json）：")
        print(_SEP)
        for r in reps:
            _t = datetime.datetime.fromtimestamp(r["mtime"]).strftime("%m-%d %H:%M:%S")
            print(f"  [{_t}] 目标页={r['target_pages']:>4}  PDF={r['pdf_count']:>4}  "
                  f"url={r['url'] or '-'}  batch={r['batch_id'] or '-'}")
        if full and reps:
            _r0 = reps[0]
            print(_DASH)
            print("  最近一次失败详情：")
            print(f"    URL       : {_r0['url'] or '-'}")
            print(f"    批次      : {_r0['batch_id'] or '-'}")
            print(f"    目标详情页: {_r0['target_pages']} 个（已抓取）")
            print(f"    PDF 附件  : {_r0['pdf_count']} 个（预期 >0）")
            print("  可能原因：")
            print("    1) 详情页结构已变化（gaokzx_parse.extract_files 需要更新）")
            print("    2) 详情页本身是「下一级汇总页」（站点多层结构）")
            print("    3) 页面改为 JS 渲染（静态 HTML 抓不到附件）")
            print("  排查建议：")
            print("    · 手工打开前 3 个详情页，确认页面里是否有 <a href=...pdf> 或附件下载按钮")
            print("    · 若结构已变，更新 gaokzx_parse.py 后重跑")
            print("    · 若是多层汇总页，需改下载器支持二级下钻")
            print("  重跑命令（游标未推进，可重试）：")
            if _r0['url']:
                print(f'    python pipeline.py --auto --url "{_r0["url"]}"')
            elif _r0['topic']:
                print(f'    python pipeline.py --auto --topic {_r0["topic"]}')
        print(_SEP)
    except Exception as _e:
        print(f"  [!] 解析失效报告打印失败（不阻断）：{type(_e).__name__}: {_e}")


def _print_rc3_banner(ctx=""):
    """rc=3（疑似解析失效）!×68 报警边框：硬中止提示 + 可选上下文。"""
    print("\n" + _BANG)
    print(f">>> [!] 检测到「疑似解析失效」（rc=3）{ctx}")
    print(">>>     批次已标记 suspect_parse_failed；未推进游标、未标 accepted。")
    print(_BANG)


def _handle_rc2(ctx_msg=""):
    """rc=2（真·空）：统一提示，返回 2。"""
    if ctx_msg: print(f"\n>>> {ctx_msg}")
    print(">>> 下载为空（rc=2）：不进入后续阶段。")
    return 2


def _handle_rc3(ctx_msg="", recent_n=3):
    """rc=3（疑似解析失效）：!×68 边框 + 完整报告，返回 3。"""
    _print_rc3_banner(ctx_msg)
    _print_suspect_report(recent_n=recent_n, full=True)
    return 3


def _after_download(rc, ctx=""):
    """下载阶段 rc 统一收尾提示（_download_topic_menu 两路径共用）：0=结束提示；
    2/3 转 _handle_rc2/_handle_rc3（recent_n=1 与原菜单行为一致）。"""
    if rc == 0:
        print("\n>>> 下载阶段结束。产物在:", STAGE_PENDING)
        return 0
    if rc == 2: return _handle_rc2(ctx)
    if rc == 3: return _handle_rc3(ctx, recent_n=1)
    return 0

# 全局子目录过滤：--only-subdir 设定后 iter_pdfs 只返回路径含该子串的文件（只处理新专题目录，不动旧库）。
ONLY_SUBDIR = None

def iter_pdfs(root=None):
    base = Path(root) if root else OUT
    for f in sorted(glob.glob(str(base / "**" / "*.pdf"), recursive=True)):
        if ONLY_SUBDIR and ONLY_SUBDIR not in f: continue
        # 测试/内部目录（_测试 / 验收测试）里的 PDF 绝不当生产文件处理。
        if bc is not None and bc.is_test_path(f): continue
        yield f

# 看板【重试】按钮把待重跑文件写进 _status\retry_queue.json；下面两个 helper 让三个步骤把被点名
# 的文件强制并入。纯增量、失败静默，队列文件缺失/损坏/任何异常都不影响主流程。
_RETRY_FILE = WORK / "_status" / "retry_queue.json"

def _retry_files_for(step, roots):
    """返回重试队列中该步 pending 的文件：[(绝对路径, rel), ...]。roots = 候选根目录（按优先级，
    本步入口在前）；用 rel 依次探测，找不到的跳过。"""
    try:
        with open(str(_RETRY_FILE), encoding="utf-8") as fh: data = json.load(fh)
    except Exception: return []
    out, seen = [], set()
    for it in (data.get("items") or []):
        try:
            if it.get("step") != step or it.get("status") != "pending": continue
            rel = str(it.get("rel") or "").strip()
            if not rel or ".." in rel or rel.startswith(("\\", "/")): continue
            for root in roots:
                p = Path(root) / rel
                if p.is_file():
                    s = str(p)
                    if s not in seen:
                        seen.add(s)
                        out.append((s, rel))
                    break
        except Exception: continue
    return out

def _retry_mark_done(step, rels):
    """把已处理完的重试项标记 done（读-改-写，tmp + os.replace 原子写回）。失败静默。"""
    rels = {str(r).casefold() for r in (rels or []) if r}
    if not rels: return
    try:
        with open(str(_RETRY_FILE), encoding="utf-8") as fh: data = json.load(fh)
        items = data.get("items") or []
        now = datetime.datetime.now().astimezone().isoformat(timespec="seconds")
        changed = False
        for it in items:
            if (it.get("step") == step and it.get("status") == "pending"
                    and str(it.get("rel") or "").casefold() in rels):
                it["status"] = "done"
                it["handled_at"] = now
                changed = True
        if changed:
            data["updated_at"] = now
            tmp = str(_RETRY_FILE) + ".tmp"
            with open(tmp, "w", encoding="utf-8") as fh: json.dump(data, fh, ensure_ascii=False, indent=1)
            os.replace(tmp, str(_RETRY_FILE))
    except Exception: pass

def has_text_layer(pdf):
    try:
        import pymupdf as fitz
        with fitz.open(pdf) as d: return bool(d[0].get_text().strip())
    except (RuntimeError, ValueError, OSError):
        # fitz 打开失败（损坏/不支持/路径异常）一律视为无文字层
        return False

def _ocr_page_estimate(pdf):
    """只读统计 PDF 的 (总页数, 缺文字层页数, 缺层页号列表)：仅 get_text，不渲染。失败返 (0,0,[])。"""
    try:
        import pymupdf as fitz
        with fitz.open(pdf) as d:
            total = d.page_count
            idxs = []
            for i in range(total):
                try:
                    if not (d[i].get_text() or "").strip():
                        idxs.append(i)
                except (RuntimeError, ValueError):
                    idxs.append(i)
            return total, len(idxs), idxs
    except (RuntimeError, ValueError, OSError):
        return 0, 0, []

def _ocr_cache_hit_estimate(page_map, dpi=200):
    """按页级缓存库（OCR_REPO/.ocr_page_cache.db）估算缺层页中已缓存的页数（近似值：引擎档位标签
    不同源时会低估；只读打开，任何异常按 0 计，绝不影响主流程）。"""
    db = OCR_REPO / ".ocr_page_cache.db"
    if not db.is_file():
        return 0
    conn = None
    try:
        import hashlib
        conn = sqlite3.connect(f"file:{db.as_posix()}?mode=ro", uri=True, timeout=5)
    except Exception:
        return 0
    env_det = (os.environ.get("OCR_DET_MODEL") or "").strip()
    env_rec = (os.environ.get("OCR_REC_MODEL") or "").strip()
    tier = "env_override" if (env_det or env_rec) else "v4_mobile"
    try:
        dpi = max(72, min(600, int(float(os.environ.get("OCR_DPI") or dpi))))
    except (ValueError, TypeError):
        dpi = 200
    hits = 0
    try:
        for f, (_tot, _need, idxs) in page_map.items():
            if not idxs:
                continue
            # 与 worker 的 _cache_key 保持同源：用文件内容 sha256 作为文件身份（重命名/移动不影响）。
            try:
                _hs = hashlib.sha256()
                with open(f, "rb") as _ff:
                    for _chunk in iter(lambda: _ff.read(1 << 20), b""):
                        _hs.update(_chunk)
                chash = _hs.hexdigest()
            except OSError:
                continue
            for i in idxs:
                raw = "|".join((chash, str(i), str(dpi), str(tier),
                                env_det, env_rec))
                key = hashlib.sha1(raw.encode("utf-8", "replace")).hexdigest()
                try:
                    if conn.execute("SELECT 1 FROM page_ocr WHERE key=?", (key,)).fetchone():
                        hits += 1
                except sqlite3.Error:
                    return hits
    finally:
        if conn is not None:
            try: conn.close()
            except Exception: pass
    return hits

# ------------------------- 步骤 2：OCR 加文字层 -------------------------
# 性能常量见文件顶部"安全护栏：内存 / worker"段（OCR_WORKERS_DEFAULT 等）。

def _clean_err(e):
    """异常对象 → 可入库短文本：str(e) 去 ANSI/控制字符、截断 300。
    子进程相关异常消息可能内嵌原始输出（含脏字符），run_chunk 各失败分支统一走这里。"""
    return clean_text(str(e), 300)

def step_ocr(con, limit=None, dry=False, root=None, workers=OCR_WORKERS_DEFAULT,
             batch=4, threads_per_worker=OCR_THREADS_PER_WORKER, force=False,
             force_scope="all", batch_id=None):
    """对"无文字层"的 PDF 调用 PaddleOCR 加透明文字层。批次模式：文件来自当前批次 manifest（scan_pdfs("cleaned")）；
    仅显式传 --ocr-root 时退化为手工目录模式（root 默认 STAGE_CLEANED，栅格重建已丢文字层必须补回）。DB 状态统一
    父进程写，子进程只回传 JSON。三处关键优化（8–10h -> 1–2h）：1) 批内摊薄模型加载（引擎只初始化一次）；
    2) 硬性线程预算防超订（实测 3 worker 无限制=594.9s 零加速，父进程用环境变量钉死每进程线程数）；
    3) 只开 det+rec，关方向分类/UVDoc 去扭曲/行方向分类。force_scope："all"=重跑全部命中文件；"new"=只强跑「无文字层」的。"""
    import concurrent.futures as cf
    import shutil
    import tempfile
    # ---- 取文件范围：批次 manifest 优先（禁止全目录扫描） ----
    bid = _resolve_bid(batch_id)
    fid_of = {}
    if root:
        # 手工模式（显式 --ocr-root）：不按批次过滤
        src_root = Path(root)
        if bid:
            print(">>> [ocr] 手工模式（显式 root）：不按批次 manifest 过滤")
            bid = None
        files = list(iter_pdfs(str(src_root)))
    else:
        src_root = STAGE_CLEANED
        if bc is not None and bid:
            pairs = _scan("cleaned", bid)
            files = [pp for _f, pp in pairs]
            fid_of = {pp: _f for _f, pp in pairs}
            print(f">>> [ocr] 批次 {bid}：去水印后目录命中 {len(files)} 个 PDF（manifest 限定）")
            if not files:
                print(">>> [ocr] 本批次在「去水印后」目录没有 PDF（可能尚未去水印）。")
                return
        else:
            print(">>> [ocr] 没有当前批次（_批次/current.json 不存在）。")
            print("        请先执行 步骤1 下载 + 步骤2 去水印，或显式传 --ocr-root <目录>。")
            return
    batch = max(1, int(batch or 1))
    # 低显存保守回退：有 N 卡但总显存 < 8GB 时强制走 CPU（单进程多线程），避免 GPU OCR 频繁 CUDA OOM。
    if _ocr_gpu_present() and os.environ.get("OCR_DEVICE", "").strip().lower() != "cpu":
        _vt = _ocr_vram_info()[0]
        if 0 < _vt < 8000:
            os.environ["OCR_DEVICE"] = "cpu"
            print(f"    [!] 显存 {_vt}MB < 8GB：OCR 保守回退 CPU（单进程多线程），避免 CUDA OOM")
    # 并发预算：物理核 + 内存 + 设备（GPU 默认 1×1 保守，CPU 默认 min(物理核,8)×1，内存护栏再收紧）
    workers, per_worker, _bnote = ocr_worker_budget(workers or 0, threads_per_worker or 0)
    if _bnote: print(f"    [!] {_bnote}")
    # 先筛出真正要 OCR 的（已有文字层的直接标记，不进子进程）；force=True 时忽略状态表与文字层判定。
    todo, skip = [], 0
    skipped_set = set()   # 看板重试并入时用于修正 skip 计数
    for f in files:
        if force_scope == "new" and has_text_layer(f):
            # 增量模式：已有文字层的一律跳过（保护旧成果），即使开了 --force
            skip += 1
            skipped_set.add(f)
            _wskip(bid, fid_of.get(f), STEP_OCR, f, dry, "已有文字层（增量跳过）")
            continue
        if not force:
            # 已完成且当前确有文字层（三条件：DB ok + 产物存在 + sha 一致）-> 跳过
            if _skip_ok(bid, fid_of.get(f), STEP_OCR, f, extra_ok=has_text_layer(f)):
                skip += 1
                skipped_set.add(f)
                _wskip(bid, fid_of.get(f), STEP_OCR, f, dry, "断点续传跳过（三条件）")
                continue
            # 已有文字层（不管状态）-> 标记完成跳过
            if has_text_layer(f):
                mark(con, f, "ocr", "ok")
                skip += 1
                skipped_set.add(f)
                _wskip(bid, fid_of.get(f), STEP_OCR, f, dry, "已有文字层")
                continue
        todo.append(f)
        if limit is not None and len(todo) >= limit: break
    # 看板重试队列：被点名的文件强制并入（绕过断点续传/已有文字层跳过，不受 --limit 影响）
    retry_items, skip = _merge_retry(
        "ocr", (src_root, STAGE_PENDING, STAGE_DONE, STAGE_ADS), todo, skipped_set, skip)
    print(f">>> [ocr] 【文件级】待处理 {len(todo)}、已跳过 {skip}（均为文件整体，非页级）；"
          f"并发 {workers}，批大小 {batch}，每进程线程 {per_worker}（预算 {workers}×{per_worker} 线程）；"
          f"设备 {'GPU' if _ocr_gpu_active() else 'CPU'}；根目录: {src_root}")
    if not todo:
        # 空跑防呆：区分"都跳过了"和"目录本来就是空的/已全部完成"
        if skip:
            print(f">>> [ocr] [!] 全部 {skip} 个文件都因断点续传/已有文字层被跳过（真跑 0 个）。")
            print(f"        如需强制重跑验证：加 --force")
            print(f"        例： python pipeline.py --step ocr --limit 3 --force")
        else:
            print(f">>> [ocr] 根目录 {src_root} 下没有 PDF 文件（真跑 0 个）。")
            print(f"        请确认目录是否正确（OCR 应指向「去水印后」）。")
        return
    # ---- 页级预估（ETA 基准）：只读 get_text 统计缺层页；页级缓存命中页零推理 ----
    page_map = {}
    tot_pages = tot_need = 0
    _t_est = time.time()
    for f in todo:
        _tp, _nd, _idxs = _ocr_page_estimate(f)
        page_map[f] = (_tp, _nd, _idxs)
        tot_pages += _tp
        tot_need += _nd
    hit_est = _ocr_cache_hit_estimate(page_map) if tot_need else 0
    eff_pages = max(1, tot_need - hit_est)
    print(f">>> [ocr] 页级预估：共 {tot_pages} 页，缺文字层 {tot_need} 页，"
          f"页级缓存预估命中 {hit_est} 页 -> 本次需推理约 {eff_pages} 页"
          f"（统计耗时 {time.time() - _t_est:.1f}s）")
    if dry:
        print(f">>> [ocr] (dry-run) 预告 {len(todo)} 个文件 / 缺层页 {tot_need}（缓存预估命中 {hit_est}）")
        statusio.begin_step("ocr", total=len(todo), skipped=skip, out_dir=str(src_root))
        statusio.end_step("ocr", status="SKIP", note="dry-run 预览")
        return
    statusio.begin_step("ocr", total=len(todo), skipped=skip, out_dir=str(src_root),
                        note=f"{workers} worker × {per_worker} 线程，批 {batch}，缺层页 {tot_need}")
    # 切批：轮转分配（todo[i::workers]）让各 worker 负载均衡
    batches = [todo[i::workers] for i in range(workers)]
    batches = [b for b in batches if b]
    # 再按 batch 上限切细：避免一个子进程批过大、进度完全不可见
    chunks = []
    for b in batches:
        for i in range(0, len(b), batch): chunks.append(b[i:i + batch])
    # 子进程环境：硬性线程预算 + 临时目录钉到 ASCII 路径（Paddle/PaddleX 在中文用户目录下会踩
    # 路径编码问题，统一用 D 盘 ASCII 目录）。
    tmp_home = ROOT / ".ocr_tmp"
    tmp_home.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    ocr_rec_batch = 16     # A3: 推理批大小（CUDA OOM 时父进程动态下调 16→8 后重派）
    # 线程数直接写死进环境变量：worker 进程 import paddle 前会读取，这是唯一能真正阻止 Paddle 偷吃全部 CPU 的位置。
    _t = str(per_worker)
    env["OCR_THREADS"] = _t
    env["OMP_NUM_THREADS"] = _t
    env["MKL_NUM_THREADS"] = _t
    env["OPENBLAS_NUM_THREADS"] = _t
    env["NUMEXPR_NUM_THREADS"] = _t
    env["VECLIB_MAXIMUM_THREADS"] = _t
    # 临时/缓存目录也钉到 ASCII 路径，规避中文用户名下的 PaddleX 编码坑
    env["TEMP"] = env["TMP"] = str(tmp_home)
    env["TMPDIR"] = str(tmp_home)
    # 关掉 paddle 的日志刷屏（只影响 stderr 体积，不影响结果）
    env.setdefault("GLOG_minloglevel", "2")
    # PY 与仓库路径含中文与否都要被上游工具正确解码
    env["PYTHONIOENCODING"] = "utf-8"
    # A3: 推理批大小（CUDA OOM 时父进程下调此值后重派；默认 16，降档 8）
    env["OCR_REC_BATCH"] = str(ocr_rec_batch)
    # A3: 显存碎片/预留策略——降低 CUDA OOM 诱因；setdefault 时明确打印当前值/是否被用户覆盖（不静默）。
    def _env_setdefault_report(_k, _d):
        _cur = env.get(_k)
        if _cur is None or str(_cur).strip() == "":
            env[_k] = _d
            print(f"    [env] {_k} = {_d}（默认注入，原未设置）")
        else:
            print(f"    [env] {_k} 当前值 = {_cur}（检测到用户已设置，保留不覆盖）")
    if _ocr_gpu_active():
        # PyTorch 变量对 Paddle 无副作用，一并设上防混用；expandable_segments 降碎片
        _env_setdefault_report("PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True")
        # Paddle 原生：不预占显存比例（0=按需增长），减少一次性显存预留导致的 OOM
        _env_setdefault_report("FLAGS_fraction_of_gpu_memory_to_use", "0")
    # 设备/精度透传给 worker（worker import paddle 前读取；--device cuda -> gpu 归一）
    _dev = os.environ.get("OCR_DEVICE", "").strip().lower()
    if _dev in ("cuda", "gpu"): env["OCR_DEVICE"] = "gpu"
    elif _dev == "cpu": env["OCR_DEVICE"] = "cpu"
    if os.environ.get("OCR_FP16", "").strip().lower() in ("1", "true", "yes"):
        env["OCR_FP16"] = "1"
        # 主管日志先明确 fp16 意向的去向；能否真正注入由 worker 按已装 PaddleOCR 构造签名判定，
        # 不支持时 worker 会打印“fp16 未生效+原因”警告，绝不静默忽略。
        if env.get("OCR_DEVICE") == "gpu":
            print("    [fp16] 用户请求 --fp16：已透传 worker；若安装版 PaddleOCR 不支持该参数，"
                  "worker 日志会明确打印“fp16 未生效+原因”（不会静默忽略）")
        else:
            print("    [fp16] 用户请求 --fp16 但当前设备非 GPU：半精度不会生效（worker 将打印警告）")
    def _keep_fail_output(src, idx_):
        """子进程异常时保留完整输出到 .ocr_tmp（含 traceback）供诊断；返回路径或 None。"""
        try:
            if os.path.isfile(src):
                dst = tmp_home / f"ocr_fail_{time.strftime('%m%d_%H%M%S')}_{idx_}.log"
                shutil.copyfile(src, dst)
                return str(dst)
        except OSError: pass
        return None
    def _read_prog(path, state):
        """增量读 .prog（jsonl，每文件一行）：实时写看板明细 + 控制台；
        state=[已读偏移, 残留半行, 最近 monitor 快照]；monitor 仅供停滞检测（零额外开销）。"""
        try:
            if os.path.isfile(path):
                with open(path, "rb") as pf:
                    pf.seek(state[0])
                    state[1] += pf.read()
                    state[0] = pf.tell()
        except OSError: return
        lines = state[1].split(b"\n")
        state[1] = lines.pop()          # 末尾可能是不完整行，留到下轮
        for ln in lines:
            ln = ln.strip()
            if not ln: continue
            try: rec = json.loads(ln.decode("utf-8", "replace"))
            except ValueError: continue
            _f = str(rec.get("file") or "")
            _ok = rec.get("status") == "ok"
            if _f:
                statusio.update_detail("ocr", _f, os.path.basename(_f),
                                       "DONE" if _ok else "FAIL",
                                       elapsed_sec=rec.get("sec"),
                                       text_layer=("有" if _ok else "—"),
                                       note=("批内完成（实时）" if _ok else
                                             str(rec.get("err"))[:160]))
            # 记录最近一次 monitor（含 gpu_mem_used/total），供 run_chunk 停滞检测
            if isinstance(rec.get("monitor"), dict):
                state[2] = rec["monitor"]
            print(f"    [·] {'OK  ' if _ok else 'FAIL'} "
                  f"{os.path.basename(_f) or '?'} {rec.get('sec')}s", flush=True)
    def run_chunk(idx_files):
        idx, files = idx_files
        _t_batch = time.time()      # 看板：批耗时（单文件耗时按批均摊，近似值）
        if bc is not None and bid and not dry:   # 批次：批内文件先标 started
            for _f in files: _bstep(bid, fid_of.get(_f), STEP_OCR, "started", src=_f)
        # 看板：批启动时先把整批标 RUNNING（供"处理中/卡死"展示）
        for _f in files: statusio.update_detail("ocr", _f, os.path.basename(_f), "RUNNING", note="批内排队/处理中")
        fd, lp = tempfile.mkstemp(suffix=".json", prefix="ocr_batch_")
        lp2 = lp + ".out"
        prog = lp + ".prog"     # worker 逐文件实时进度（jsonl）
        out = ""
        kind = None             # None / "STALL" / "TIMEOUT"
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh: json.dump(files, fh, ensure_ascii=False)
            # 子进程 stdout/stderr 直写文件：避免管道打满隐性死锁（capture_output 小管道会卡）；
            # Popen+轮询（而非 run 干等）：worker 每完成一个文件追加一行 .prog，1.5s 一扫 -> 看板实时。
            with open(lp2, "wb") as fo:
                proc = subprocess.Popen([str(PY), str(OCR_WORKER), lp], stdout=fo, stderr=subprocess.STDOUT, env=env)
                _pstate = [0, b"", None]   # [已读偏移, 半行残留, 最近 monitor 快照]
                _t_ck = time.time()
                _prev_off = 0
                _last_prog = time.time()    # 最近一次有进度行的时间
                _last_beat = time.time()    # 心跳节流（每 30s 最多一次）
                while proc.poll() is None:
                    _read_prog(prog, _pstate)
                    if _pstate[0] > _prev_off:      # 有新进度行（文件完成）
                        _prev_off = _pstate[0]
                        _last_prog = time.time()
                    # C1: 停滞检测——无进度（秒）且显存占满 -> 判卡死 kill（替代旧 7200s 硬超时）
                    _mon = _pstate[2] or {}
                    _gt = _mon.get("gpu_mem_total_mb") or 0
                    _gu = _mon.get("gpu_mem_used_mb") or 0
                    # 占满阈值对齐 worker 侧显存看门狗（90%）；旧 98% 过严致“快满不干”长时间不判死
                    _gpu_full = _gt > 0 and (_gu / _gt) >= 0.90
                    _idle = time.time() - _last_prog
                    # 事故形态补判：worker 冻结但显存驻留不释放（如 52% 恒占、永不到 90%），
                    # 长静默 + 占用≥2GB（远大于空闲基线）也判卡死，不再干等到 7200s 绝对超时
                    _gpu_hold = _gu >= 2048 and _idle >= OCR_STALL_HOLD_TIMEOUT
                    if (_idle > OCR_STALL_TIMEOUT and _gpu_full) or _gpu_hold:
                        proc.kill(); proc.wait()
                        kind = "STALL"
                        raise TimeoutError(f"STALL: 无进度 {int(_idle)}s（显存 {_gu}/{_gt}MB，"
                                           f"{'占满' if _gpu_full else '驻留'}），判卡死已 kill")
                    if time.time() - _t_ck > OCR_CHUNK_ABS_MAX:   # 绝对超时兜底（不依赖显存）
                        proc.kill(); proc.wait()
                        raise TimeoutError(f"子进程超过 {OCR_CHUNK_ABS_MAX}s 未结束")
                    # 心跳：长时间无新完成文件时定期报一句（区分“在跑但单页慢” vs “真卡死”）
                    if time.time() - _last_beat >= 30 and time.time() - _last_prog >= 30:
                        _last_beat = time.time()
                        print(f"    [心跳] 批{idx} 已 {int(time.time()-_t_ck)}s，"
                              f"{int(time.time()-_last_prog)}s 无新文件完成（显存 {_gu or '-'}/{_gt or '-'}MB）", flush=True)
                    time.sleep(1.5)
                _read_prog(prog, _pstate)   # 收尾把剩余行读净
            with open(lp2, "rb") as fi: out = fi.read().decode("utf-8", "replace")
            i = out.find("RESULT")
            if i < 0:
                _keep = _keep_fail_output(lp2, idx)
                return files, None, f"no RESULT: {clean_text(out[-400:], 400)}" + (f"（完整输出: {_keep}）" if _keep else ""), kind
            res = json.loads(out[i + 6:])
            if isinstance(res, list):   # 看板：批耗时回填进每条记录（供明细均摊）
                _dt = round(time.time() - _t_batch, 2)
                for _r in res:
                    if isinstance(_r, dict): _r.setdefault("batch_sec", _dt)
            return files, res, None, kind
        except TimeoutError as e:
            _ce = _clean_err(e)                # 异常文本可能携带子进程输出（含 ANSI/控制字符），入库前先清洗
            if kind == "STALL":
                return files, None, f"STALL: {_ce}", "STALL"
            _keep = _keep_fail_output(lp2, idx)
            return files, None, f"timeout: {_ce}" + (f"（完整输出: {_keep}）" if _keep else ""), "TIMEOUT"
        except json.JSONDecodeError as e:
            _keep = _keep_fail_output(lp2, idx)
            return files, None, f"bad json: {_clean_err(e)}" + (f"（完整输出: {_keep}）" if _keep else ""), None
        except Exception as e:
            _keep = _keep_fail_output(lp2, idx)
            return files, None, f"launch/timeout: {_clean_err(e)}" + (f"（完整输出: {_keep}）" if _keep else ""), None
        finally:
            for p in (lp, lp2, prog):
                try: os.unlink(p)
                except OSError: pass
    t0 = time.time()
    tally = {"ok": 0, "fail": 0}
    done = 0
    pages_done = 0        # 已完成文件的"缺层页"页数（页级 ETA 口径）
    infer_pages = 0       # 实际推理页数
    cache_hit_pages = 0   # 页级缓存命中页数
    pg_secs = []          # 每文件页均耗时（求页均）
    _pg_hist = []         # 滑动窗口样本 [(时间, 累计缺层页), ...]：近 N 个文件的页速，ETA 更平滑
    _retry_processed = set()    # 看板重试：实际派发过的文件（中断未派发的不算）
    stop = _wf_install_sigint("OCR")
    # 逐批落库（不攒到最后）：中途被打断也能保留已完成状态，重跑自动跳过
    bar = _wf_bar(len(todo), "OCR", enabled=True)
    redo = []               # 需降并发重试的文件（显存保护 / CUDA OOM / 卡死）
    current_chunks = chunks
    round_no = 0
    prev_workers = workers
    prev_batch = ocr_rec_batch
    MAX_ROUNDS = 2          # 自适应降级最多 2 轮，之后兜底标 fail 防无限循环
    with bar as _b:
        while current_chunks:
            round_no += 1
            if round_no > 1:
                print(f"    [自适应降级] 第 {round_no} 轮：并发 {prev_workers}→{workers}，"
                      f"REC_BATCH {prev_batch}→{ocr_rec_batch}，重派 {len(current_chunks)} 批 / {len(redo)} 文件")
            with cf.ThreadPoolExecutor(max_workers=workers) as ex:
                for files, res, err, kind in ex.map(run_chunk, list(enumerate(current_chunks))):
                    _retry_processed.update(files)
                    # ---- 卡死 / 超时批量失败：整批进 redo（不落库 fail，待降级重派）----
                    if kind == "STALL" or (err and err.startswith("STALL:")):
                        redo.extend(files)
                        print(f"    [!] 批卡死（{len(files)} 个）：{err[:160]} —— 降并发后重试")
                        continue
                    if err:
                        _en = f"批失败: {err[:160]}"
                        for f in files:
                            if has_text_layer(f):
                                # 子进程崩溃前该文件实际已完成（文字层已写回）：按成功落库，避免把已有成果误标
                                # fail（否则看板一片红，重跑时还要完整重处理）。
                                mark(con, f, "ocr", "ok")
                                tally["ok"] = tally.get("ok", 0) + 1
                                _bstep(bid, fid_of.get(f), STEP_OCR, "ok", src=f, dst=f, src_sha=_sha256_file(f), error="")
                                ledgerio.log("ocr", "DONE", src_path=f,
                                             dst_path=f, text_layer="有",
                                             note="批失败但文字层已写入（崩溃前完成）")
                                statusio.update_detail(
                                    "ocr", f, os.path.basename(f), "DONE",
                                    text_layer="有",
                                    note="批失败但文字层已写入（崩溃前完成）")
                                continue
                            mark(con, f, "ocr", "fail")
                            tally["fail"] += 1
                            _bstep(bid, fid_of.get(f), STEP_OCR, "fail", src=f, error=_en)
                            ledgerio.log("ocr", "FAIL", src_path=f, note=_en)
                            statusio.update_detail("ocr", f, os.path.basename(f), "FAIL", note=_en)
                        print(f"    [!] 批失败（{len(files)} 个）：{err[:200]}")
                        done += len(files)
                        pages_done += sum(page_map.get(f, (0, 0, []))[1] for f in files)
                        _b.update(len(files))
                        continue
                    # ---- 成功：逐 rec 落库；触发显存保护 / OOM 的进 redo ----
                    done_files = 0
                    for rec in res:
                        if rec.get("vram_oom") or rec.get("vram_guard"):
                            # 门槛：只有"真跑过推理"（有 ocr_pages / page_sec_avg）才认 OOM 降并发；
                            # 注意：不能拿 batch_sec 判——run_chunk 对它批内所有 rec 都回填了值（含 0s 崩溃的），
                            # batch_sec 恒有值 → 会误判为"推理过"。
                            _did_infer = bool(rec.get("ocr_pages") or rec.get("page_sec_avg"))
                            if _did_infer:
                                redo.append(rec["file"])
                                print(f"    [~] {os.path.basename(rec['file'])} 触发显存保护/OOM，降并发重试")
                                continue
                            # 未推理就失败：当普通 fail 处理（落库 fail 不再重派）
                            _errmsg = str(rec.get("err") or "未推理即失败（疑似渲染异常，非 OOM）")
                            mark(con, rec["file"], "ocr", "fail")
                            tally["fail"] = tally.get("fail", 0) + 1
                            _bstep(bid, fid_of.get(rec["file"]), STEP_OCR, "fail",
                                   src=rec["file"], error=_errmsg[:200])
                            ledgerio.log("ocr", "FAIL", src_path=rec["file"], note=_errmsg[:160])
                            statusio.update_detail("ocr", rec["file"],
                                                   os.path.basename(rec["file"]), "FAIL",
                                                   note=f"渲染失败（非 OOM）: {_errmsg[:120]}")
                            _b.update(1, label=os.path.basename(rec["file"])[:38], ok=False,
                                      name=os.path.basename(rec["file"]))
                            continue
                        st = rec.get("status", "fail")
                        mark(con, rec["file"], "ocr", st)
                        tally[st] = tally.get(st, 0) + 1
                        pages_done += page_map.get(rec.get("file"), (0, 0, []))[1]
                        if rec.get("page_sec_avg"): pg_secs.append(rec["page_sec_avg"])
                        cache_hit_pages += rec.get("cached_pages") or 0
                        infer_pages += rec.get("ocr_pages") or 0
                        _bstep(bid, fid_of.get(rec["file"]), STEP_OCR,
                               "ok" if st == "ok" else "fail",
                               src=rec["file"],
                               dst=(rec["file"] if st == "ok" else ""),
                               src_sha=(_sha256_file(rec["file"]) if st == "ok" else None),
                               error=(str(rec.get("err"))[:200] if st == "fail" else ""))
                        if st == "fail":
                            print(f"    [!] {os.path.basename(rec['file'])}: " f"{str(rec.get('err'))[:160]}")
                        _bsec = rec.get("batch_sec")
                        _bn = max(1, len(files))
                        ledgerio.log(
                            "ocr", "DONE" if st == "ok" else "FAIL",
                            src_path=rec["file"],
                            dst_path=(rec["file"] if st == "ok" else None),
                            elapsed_sec=(round(_bsec / _bn, 2) if _bsec else None),
                            text_layer=("有" if st == "ok" else None),
                            note=(str(rec.get("err"))[:160] if st == "fail"
                                  else "原地补文字层"))
                        statusio.update_detail(
                            "ocr", rec["file"], os.path.basename(rec["file"]),
                            "DONE" if st == "ok" else "FAIL",
                            elapsed_sec=(round(_bsec / _bn, 2) if _bsec else None),
                            text_layer=("有" if st == "ok" else "—"),
                            note=(str(rec.get("err"))[:160] if st == "fail" else
                                  (f"批均摊（批 {_bsec}s ÷ {_bn}）" if _bsec else "")))
                        done_files += 1
                    done += done_files
                    _b.update(done_files, label=os.path.basename(files[-1])[:38])
                    el = time.time() - t0
                    _pg_hist.append((time.time(), pages_done))
                    if len(_pg_hist) > 12: _pg_hist.pop(0)
                    # 滑动平均页速：用窗口首尾的“页数差/时间差”（而非累计均值），前期快、后期降速时 ETA 跟随得更快
                    _rate = 0.0
                    if len(_pg_hist) >= 2:
                        _dtw = _pg_hist[-1][0] - _pg_hist[0][0]
                        _dpp = _pg_hist[-1][1] - _pg_hist[0][1]
                        if _dtw > 0 and _dpp > 0: _rate = _dpp / _dtw
                    if _rate <= 0: _rate = pages_done / el if el > 0 else 0
                    rate = _rate
                    eta = max(0, eff_pages - pages_done) / rate if rate > 0 else 0
                    avg_pg = (sum(pg_secs) / len(pg_secs)) if pg_secs else 0
                    _den = infer_pages + cache_hit_pages
                    chr_ = (100.0 * cache_hit_pages / _den) if _den else 0.0
                    print(f"    [文件 {done}/{len(todo)} | 缺层页 {pages_done}/{eff_pages}] 耗时 {el:.0f}s，"
                          f"页速 {rate:.2f} 页/s，页均 {avg_pg:.2f}s，缓存命中 {chr_:.1f}%，"
                          f"预计剩余 {eta/60:.1f} 分钟")
                    if stop():
                        print("    [!] 收到中断信号：已完成批次已落库，停止派发新批次")
                        break
            if stop():
                break
            con.commit()
            # ---- 准备下一轮（自适应降级：B2 降并发 + A3 降 REC_BATCH）----
            if not redo:
                break
            if round_no >= MAX_ROUNDS:
                # 兜底：已达最大降级轮数仍失败，标 fail 防无限循环
                print(f"    [!] 已达 {MAX_ROUNDS} 轮降级仍失败，{len(redo)} 个文件标记 fail 兜底")
                for f in redo:
                    mark(con, f, "ocr", "fail")
                    tally["fail"] += 1
                    _bstep(bid, fid_of.get(f), STEP_OCR, "fail", src=f, error="显存降级重试仍失败")
                    ledgerio.log("ocr", "FAIL", src_path=f, note="显存降级重试仍失败")
                    statusio.update_detail("ocr", f, os.path.basename(f), "FAIL", note="显存降级重试仍失败")
                done += len(redo)
                _b.update(len(redo))
                redo = []
                break
            prev_workers = workers
            prev_batch = ocr_rec_batch
            if _ocr_gpu_active() and workers <= 1:
                # GPU 且已单进程仍显存不足：本轮转 CPU（保守兵底，防 CUDA OOM 无限重派）
                os.environ["OCR_DEVICE"] = "cpu"
                env["OCR_DEVICE"] = "cpu"
                workers, per_worker, _nb = ocr_worker_budget(0, 0)
                for _k in ("OCR_THREADS", "OMP_NUM_THREADS", "MKL_NUM_THREADS",
                           "OPENBLAS_NUM_THREADS", "NUMEXPR_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
                    env[_k] = str(per_worker)
                print(f"    [自适应降级] 显存仍不足，重派轮转 CPU（单进程 {per_worker} 线程）")
            else:
                workers = max(1, workers - 1)               # B2: 降并发
            ocr_rec_batch = max(1, ocr_rec_batch // 2)      # A3: REC_BATCH 16→8→4→…→1
            env["OCR_REC_BATCH"] = str(ocr_rec_batch)   # 同步给子进程
            # 重新分块（轮转均摊）+ 清空 redo 待重派
            current_chunks = [redo[i::workers] for i in range(workers)]
            current_chunks = [c for c in current_chunks if c]
            redo = []
            if not current_chunks:
                break
    _den = infer_pages + cache_hit_pages
    _chr = (100.0 * cache_hit_pages / _den) if _den else 0.0
    _avg = (sum(pg_secs) / len(pg_secs)) if pg_secs else 0.0
    print(f">>> [ocr] 完成 {tally.get('ok', 0)}，失败 {tally.get('fail', 0)}，" f"已跳过 {skip}，耗时 {time.time() - t0:.1f}s")
    print(f">>> [ocr] 页级统计：缺层页 {pages_done}/{eff_pages}，推理页 {infer_pages}，"
          f"缓存命中 {cache_hit_pages} 页（{_chr:.1f}%），页均 {_avg:.2f}s")
    if retry_items:   # 看板重试：已处理到的项标记 done（中断未派发的保持 pending）
        _retry_mark_done("ocr", [r for p, r in retry_items if p in _retry_processed])
    statusio.end_step("ocr", note=f"完成 {tally.get('ok', 0)} / " f"失败 {tally.get('fail', 0)} / 跳过 {skip}"
                                  f" / 缺层页 {tot_need}（缓存 {hit_est}）")

# ---------------------- 步骤 2.5：清理 OCR 脏数据 / 临时文件 ----------------------
def step_clean_ocr_dirty(con, dry=False):
    """跑全量 OCR 之前的卫生工作（--dry-run 只报告）：1) 删 files 表 step in ('ocr','ocr_dirty')
    且 status in ('fail','error') 的记录（删记录 ≠ 删 PDF；清掉才能让增量重跑重新纳入——历史
    bug 曾把整批误写 fail）；2) 删 ROOT/去水印目录下 tempfile 批文件、子进程输出、.bak 等垃圾。"""
    import glob as _glob
    import shutil as _shutil
    # --- 1) 状态表脏记录 ---
    rows = con.execute(
        "SELECT path, step, status FROM files "
        "WHERE step IN ('ocr','ocr_dirty') AND status IN ('fail','error')"
    ).fetchall()
    print(f">>> [clean-ocr-dirty] 待清脏记录 {len(rows)} 条（step=ocr 且 status=fail/error）")
    for p, s, st in rows[:20]: print(f"      - [{st}] {os.path.basename(p)}")
    if len(rows) > 20: print(f"      ... 其余 {len(rows) - 20} 条略")
    # --- 2) 临时垃圾目录 ---
    patterns = [
        str(ROOT / "ocr_batch_*"),          # 批文件（tempfile.mkstemp，可能落 ROOT）
        str(ROOT / "ocr_batch_*.out"),      # 子进程输出
        str(ROOT / ".ocr_tmp" / "*"),       # 子进程 TEMP 目录内容
        str(ROOT / "tmp*"),
        str(OUT.parent / (OUT.name + "_去水印后") / "*.bak"),  # 去水印备份残留
    ]
    junk = []
    for pat in patterns: junk.extend(_glob.glob(pat))
    # 去重 + 过滤掉不存在的
    junk = sorted({j for j in junk if os.path.exists(j)})
    print(f">>> [clean-ocr-dirty] 待清临时垃圾 {len(junk)} 项")
    for j in junk[:20]: print(f"      - {j}")
    if len(junk) > 20: print(f"      ... 其余 {len(junk) - 20} 项略")
    if dry:
        print(">>> [clean-ocr-dirty] (dry-run) 以上均未改动")
        return
    # 执行：先删 DB 记录，再删磁盘垃圾
    if rows:
        con.execute("DELETE FROM files " "WHERE step IN ('ocr','ocr_dirty') AND status IN ('fail','error')")
        con.commit()
    n_junk = 0
    for j in junk:
        try:
            if os.path.isdir(j): _shutil.rmtree(j, ignore_errors=True)
            else: os.unlink(j)
            n_junk += 1
        except OSError as e: print(f"      [!] 删除失败 {j}: {e}")
    print(f">>> [clean-ocr-dirty] 已清 {len(rows)} 条脏记录、{n_junk} 项临时垃圾")

# ------------------------- 步骤 3：质控 -------------------------
def step_qc(con, dry=False, root=None, limit=None, batch_id=None, stage=None):
    """质控（步骤5）：零字节 / PDF 完整性 / 文字层是否缺失。批次模式：文件范围来自当前批次 manifest
    （scan_pdfs("cleaned") 优先，空则回退 "done"）；显式传 root 时为手工模式。仅在问题文件上写 qc 状态，
    正常文件标记 ok（便于增量）；limit 非空时只检查前 N 个文件（配合 --limit 做小范围端到端验证）。"""
    import pymupdf as fitz
    bid = _resolve_bid(batch_id)
    fid_of = {}
    if root:
        base = Path(root)
        if bid:
            print(">>> [qc] 手工模式（显式 root）：不按批次 manifest 过滤")
            bid = None
        files = list(iter_pdfs(str(base)))
    else:
        _stages = (stage,) if stage else ("cleaned", "done")
        if bc is not None and bid:
            pairs = []
            for _sg in _stages:
                pairs = _scan(_sg, bid)
                if pairs: break
            files = [p for _f, p in pairs]
            fid_of = {p: _f for _f, p in pairs}
            base = STAGE_ADS if stage == "ads" else STAGE_CLEANED
            if not files:
                print(f">>> [qc] 本批次在「{'/'.join(_stages)}」目录没有 PDF（批次 {bid}）。")
                return
        else:
            print(">>> [qc] 没有当前批次（_批次/current.json 不存在）。")
            print("        请先跑完整流程，或显式传 --qc-root <目录>。")
            return
    zero, broken, no_text, total = [], [], [], 0
    if limit:
        files = files[:limit]
        print(f">>> [qc] 已按 --limit 限制为前 {len(files)} 个文件")
    # 看板重试队列：被点名的文件强制并入检查列表（不受 --limit 影响）
    retry_items, _ = _merge_retry("qc", (base, STAGE_PENDING, STAGE_DONE, STAGE_ADS), files)
    statusio.begin_step("qc", total=len(files), out_dir=str(base))
    bar = _wf_bar(len(files), "质控", enabled=not dry)
    with bar as _b:
        for f in files:
            total += 1
            if bc is not None and bid and fid_of.get(f) and not dry:
                _bstep(bid, fid_of[f], STEP_QC, "started", src=f)
            # 看板：当前文件标 RUNNING（qc 秒级，随即转终态）
            statusio.update_detail("qc", f, os.path.basename(f), "RUNNING", note="检查中")
            _qc_pages = None
            if os.path.getsize(f) == 0:
                zero.append(f)
                mark(con, f, "qc", "zero")
                _bstep(bid, fid_of.get(f), STEP_QC, "fail", src=f, error="零字节")
                ledgerio.log("qc", "FAIL", src_path=f, dst_path=f, size_bytes=0, note="零字节")
                statusio.update_detail("qc", f, os.path.basename(f), "FAIL", note="零字节")
                _b.update(1, label=os.path.basename(f)[:38], ok=False, name=os.path.basename(f))
                continue
            try:
                with fitz.open(f) as d:
                    _ = d.page_count
                    _qc_pages = int(d.page_count)   # 看板：页数（属性读取，零额外 IO）
            except (RuntimeError, ValueError, OSError):
                broken.append(f)
                mark(con, f, "qc", "broken")
                _bstep(bid, fid_of.get(f), STEP_QC, "fail", src=f, error="PDF 损坏/无法打开")
                ledgerio.log("qc", "FAIL", src_path=f, dst_path=f, note="PDF 损坏/无法打开")
                statusio.update_detail("qc", f, os.path.basename(f), "FAIL", note="PDF 损坏/无法打开")
                _b.update(1, label=os.path.basename(f)[:38], ok=False, name=os.path.basename(f))
                continue
            # 文档 7.2 要求"验证文字层是否正确添加"——统计可打开但无文字层的文件（多为栅格化产物，
            # 需另跑 OCR；仅信息统计，不影响 ok 标记）。
            _qc_has_txt = has_text_layer(f)   # 看板：结果同时用于明细，避免重复判定
            if not _qc_has_txt: no_text.append(f)
            mark(con, f, "qc", "ok")
            _bstep(bid, fid_of.get(f), STEP_QC, "ok", src=f, dst=f, error=("" if _qc_has_txt else "无文字层（需补 OCR）"))
            ledgerio.log("qc", "DONE" if _qc_has_txt else "WARN",
                         src_path=f, dst_path=f, pages=_qc_pages,
                         text_layer=("有" if _qc_has_txt else "无"),
                         note=("" if _qc_has_txt else "无文字层（需补 OCR）"))
            statusio.update_detail("qc", f, os.path.basename(f),
                                   "WARN" if not _qc_has_txt else "DONE",
                                   src_pages=_qc_pages, dst_pages=_qc_pages,
                                   text_layer=("有" if _qc_has_txt else "无"),
                                   note=("无文字层（需补 OCR）" if not _qc_has_txt else ""))
            _b.update(1, label=os.path.basename(f)[:38])
    # 报告落到被检目录的 _报告/ 下（跟着被检对象走，避免多目录混用一份报告）
    rep_dir = base / "_报告"
    try: rep_dir.mkdir(parents=True, exist_ok=True)
    except OSError: rep_dir = ROOT
    rep = rep_dir / "qc_report.txt"
    with open(rep, "w", encoding="utf-8") as fh:
        fh.write(f"批次 {bid or '(手工模式)'}\n")
        fh.write(f"根目录 {base}\n")
        fh.write(f"总数 {total}\n零字节 {len(zero)}\n损坏 {len(broken)}\n" f"无文字层 {len(no_text)}\n\n")
        for title, items in (("零字节", zero), ("损坏", broken), ("无文字层（需补 OCR）", no_text)):
            fh.write(f"== {title} ==\n")
            for x in items: fh.write(x + "\n")
            fh.write("\n")
    print(f">>> [qc] 根目录 {base}")
    print(f">>> [qc] 总数 {total} | 零字节 {len(zero)} | 损坏 {len(broken)} | " f"无文字层 {len(no_text)}")
    print(f">>> [qc] 报告: {rep}")
    if retry_items:   # 看板重试：qc 无提前中断，全部标记 done
        _retry_mark_done("qc", [r for _p, r in retry_items])
    statusio.end_step("qc", note=f"零字节 {len(zero)} / 损坏 {len(broken)} / " f"无文字层 {len(no_text)}")
    if zero or broken:
        print(">>> [qc] 需重下的文件（零字节/损坏）:")
        for x in (zero + broken)[:30]: print(f"      - {os.path.basename(x)}")

# 方法：跨页重复检测 + 像素级白化（栅格重建）+ 后续 OCR 回补文字层。两代水印两通道：1) 灰色：
#   跨页一致 + 中间调灰 + 中央区（排页眉/页脚防误伤）；2) 浅蓝：跨页一致 + 偏色（B>G>R）全页生效
#   （旧版只测灰度曾漏检）。干净文件直接跳过不动原文件。
def _render_rgb(pdf, scale=1.2, max_pages=10):
    """渲染前 N 页为 RGB float32 栈（尺寸归一化到首页）。"""
    import pymupdf as fitz
    import numpy as np
    import cv2
    doc = fitz.open(pdf)
    out = []
    ref = None
    for i in range(min(len(doc), max_pages)):
        pix = doc[i].get_pixmap(matrix=fitz.Matrix(scale, scale))
        arr = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)[..., :3]
        a = arr.astype(np.float32)
        if ref is None: ref = a.shape[:2]
        elif a.shape[:2] != ref:       # 不同页尺寸不一致 -> 归一化到首页尺寸，保证可跨页堆叠
            a = cv2.resize(a, (ref[1], ref[0]))
        out.append(a)
    doc.close()
    return np.stack(out) if out else None

def _wm_masks(stack):
    """由多页 RGB 栈计算低分辨率水印遮罩。返回 (gray_mask, blue_mask)。"""
    import numpy as np
    if stack is None: raise ValueError("_wm_masks: 无有效页栈")
    std = stack.std(axis=0).mean(axis=2)
    mean = stack.mean(axis=0)
    mean_gray = mean.mean(axis=2)
    rr, gg, bb = mean[..., 0], mean[..., 1], mean[..., 2]   # 均值图取色：中位数会丢掉"部分页偏淡"的水印行
    h, w = std.shape
    consistency = std < 16                      # 跨页几乎不变（核心约束）
    mid = (mean_gray > 110) & (mean_gray < 235)  # 中间调灰（排除纯黑字/纯白底）
    central = np.zeros_like(consistency)
    central[int(h * 0.06):int(h * 0.94), int(w * 0.06):int(w * 0.94)] = True   # 灰通道排除页眉页脚
    gray_mask = consistency & mid & central
    # 浅蓝水印：B 明显高于 R/G（半透明蓝叠白底）、R 不太低（排除深蓝 logo/正文）；判据比灰通道宽但"跨页一致"仍核心。
    blue_mask = consistency & (bb - rr > 8) & (bb > 110) & (rr > 70) & (bb - gg > 2)
    return gray_mask, blue_mask

def detect_watermark(pdf):
    """返回 (是否疑似水印, 水印像素占比)；干净文件返回 False。
    灰度与浅蓝双通道取并集（浅蓝水印常极淡，仅灰度会漏检），再做 3x3 开运算滤掉
    孤立的细碎一致性像素（文字抗锯齿噪声/修复残留），只保留成片水印区域。"""
    import cv2
    import numpy as np
    stack = _render_rgb(pdf, scale=1.2, max_pages=10)
    if stack is None or len(stack) < 2: return False, 0.0
    gray_mask, blue_mask = _wm_masks(stack)
    m = (gray_mask | blue_mask).astype(np.uint8)
    m = cv2.morphologyEx(m, cv2.MORPH_OPEN, cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3)))
    frac = float(m.mean())
    return frac > WM_DETECT_MIN, frac

def _strip_watermark_artifacts(raw):
    """删除内容流中所有 /Artifact ... /Subtype /Watermark ... BDC ... EMC 块。
    平衡扫描，只删标记了 /Watermark 的工件，避免误伤其他内容。返回 (new_str, removed_count)。"""
    out, i, n, removed = [], 0, len(raw), 0
    while i < n:
        bdc = raw.find("BDC", i)
        if bdc == -1:
            out.append(raw[i:]); break
        dict_end = raw.rfind(">>", i, bdc)
        if dict_end == -1:
            out.append(raw[i:]); break
        dict_start = raw.rfind("<<", i, dict_end)
        if dict_start == -1:
            out.append(raw[i:]); break
        if "/Watermark" in raw[dict_start:bdc + 3]:
            depth, p = 1, bdc + 3
            while p < n and depth > 0:
                nb, ne = raw.find("BDC", p), raw.find("EMC", p)
                if ne == -1:
                    p = n; break
                if nb != -1 and nb < ne:
                    depth += 1; p = nb + 3
                else:
                    depth -= 1; p = ne + 3
            out.append(raw[i:dict_start])   # 从 << 到 EMC 整段删除
            removed += 1
            i = p
        else:
            out.append(raw[i:bdc + 3]); i = bdc + 3
    return "".join(out), removed

def has_layer_watermark(pdf):
    """快速判据：任一页内容流出现 /Subtype /Watermark 标记（独立水印图层）。"""
    import pymupdf as fitz
    try: d = fitz.open(pdf)
    except Exception: return False
    try:
        for i in range(len(d)):
            raw = d[i].read_contents().decode("latin-1", "replace")
            if "/Watermark" in raw and "BDC" in raw: return True
        return False
    finally: d.close()

def remove_watermark_layer(pdf, out_pdf):
    """无损删除图层水印：仅剔出页面内容流里的 Watermark 工件块，不栅格化、不丢文字层；
    save(garbage=4) 回收被孤立的水印 XObject。返回真正移除的块数
    （0 表示结构异常，调用方应退回像素级栅格方案）。原文件不动。"""
    import pymupdf as fitz
    d = fitz.open(pdf)
    total = 0
    for i in range(len(d)):
        for xref in d[i].get_contents():
            raw_b = d.xref_stream(xref) or b""
            raw = raw_b.decode("latin-1", "replace")
            if "/Watermark" not in raw or "BDC" not in raw: continue
            new, removed = _strip_watermark_artifacts(raw)
            if removed:
                total += removed
                d.update_stream(xref, new.encode("latin-1"))
    if total == 0:
        d.close()
        return 0
    # ---- 清理悬空 ExtGState 引用（防下游 MuPDF error: cannot find ExtGState resource 'GSx'）----
    # 原理：删 /Watermark 工件块时，块内若定义过 ExtGState（通常 /GS1.../GSn），
    # 页面其他内容仍可能 ` /GSx gs ` 引用已失效资源。这里扫描每页内容流，
    # 收集所有 /GSxxx 名字；与页面 Resources.ExtGState 做差集；对差集里的名字，
    # 直接从内容流删掉 ` /GSxxx gs ` 指令（等价于 no-op，不影响渲染）。
    try:
        import re as _re
        _GS_REF = _re.compile(rb"/(GS\d+)\s+gs\b")
        for _pi in range(len(d)):
            _page = d[_pi]
            try:
                _raw = _page.read_contents()
            except Exception:
                continue
            if not _raw or b"/GS" not in _raw:
                continue
            _refs = set(_GS_REF.findall(_raw))
            if not _refs:
                continue
            # 页面 Resources.ExtGState 现状（缺失即视为悬空）
            _have = set()
            try:
                _res = _page.get_resources() or {}
                _ext = _res.get("ExtGState") or {}
                _have = {str(k).lstrip("/").encode("latin-1", "replace") for k in _ext}
            except Exception:
                _have = set()
            _missing = _refs - _have
            if not _missing:
                continue
            _new = _raw
            for _m in _missing:
                # 删 ` /GSx gs`（保留其他空格/换行，删前后紧邻的单个空格避免残留双空格）
                _new = _re.sub(rb"\s*/" + _m + rb"\s+gs\b", b"", _new)
            if _new != _raw:
                try:
                    _page.set_contents(_new)
                except Exception:
                    # set_contents 不可用：退回 update_stream 逐个 contents xref
                    try:
                        for _xr in _page.get_contents():
                            _s = d.xref_stream(_xr) or b""
                            if not _s:
                                continue
                            _s2 = _s
                            for _m in _missing:
                                _s2 = _re.sub(rb"\s*/" + _m + rb"\s+gs\b", b"", _s2)
                            if _s2 != _s:
                                d.update_stream(_xr, _s2)
                    except Exception:
                        pass
    except Exception:
        pass   # 清理失败绝不阻断（宁可留悬空引用，也不能让整步骤崩）
    d.save(out_pdf, deflate=True, garbage=4)
    d.close()
    return total

# 局部块贴图参数：差分掩码按 128px 网格聚合 + 膨胀合并相邻水印块；单页补丁数超过 60 或
# 补丁总面积超过页面 45% 时才退回整页栅格（此时也仅"盖图"，原页内容流与文字层保留）。
WM_PATCH_GRID = 128
WM_PATCH_PAD = 10
WM_PATCH_MAX_N = 60
WM_PATCH_MAX_FRAC = 0.45

def _diff_boxes(diff, grid=WM_PATCH_GRID, pad=WM_PATCH_PAD):
    """把差分掩码（bool 二维）聚合成贴块矩形列表 [(x0,y0,x1,y1), ...]（像素，已加 pad 外扩）。
    算法：网格化 -> 含差分的格标记 -> 膨胀 1 格合并相邻块 -> 连通域取包围盒。
    统一网格法（而非逐连通域）：限制贴块数量、天然合并散字水印，成本可控。"""
    import cv2
    import numpy as np
    h, w = diff.shape
    if not diff.any(): return []
    rows = (h + grid - 1) // grid
    cols = (w + grid - 1) // grid
    cell = cv2.resize(diff.astype(np.uint8), (cols, rows),
                      interpolation=cv2.INTER_AREA) > 0
    cell_u8 = cv2.dilate(cell.astype(np.uint8), np.ones((3, 3), np.uint8), iterations=1)
    n, lab = cv2.connectedComponents(cell_u8, connectivity=8)
    boxes = []
    for k in range(1, n):
        ys, xs = np.where(lab == k)
        x0, x1 = int(xs.min()), int(xs.max())
        y0, y1 = int(ys.min()), int(ys.max())
        boxes.append((max(0, x0 * grid - pad), max(0, y0 * grid - pad),
                      min(w, (x1 + 1) * grid + pad), min(h, (y1 + 1) * grid + pad)))
    return boxes

def remove_watermark_raster(pdf, out_pdf, scale=2.0):
    """像素级去水印（局部块修补版）：灰度+浅蓝双通道遮罩标记水印像素，**只在掩码命中区域**
    做白化重绘小块、贴回原页——原页面内容流与文字层全部保留（不再整页栅格重建、不再丢文字层）。
    仅当单页补丁过多/占比过大时才退回整页栅格；即便整页栅格也只是在原页上盖一层图，
    文字层仍在（OCR 阶段按页判断文字层，自然只补真正缺层的页）。
    返回 dict：{mode: patch|full|copy, raster_pages: [整页栅格页号...], n_patch: 总块数, note}。
    原文件不动，成果写到 out_pdf；同路径调用（in-place）也安全（临时文件 + 原子替换）。"""
    import pymupdf as fitz
    import cv2
    import numpy as np
    import tempfile
    rep = {"mode": "patch", "raster_pages": [], "n_patch": 0, "note": ""}
    stack = _render_rgb(pdf, scale=1.2, max_pages=10)
    if stack is None or len(stack) < 2:      # 空/单页 PDF 无跨页信息可判 -> 原件拷贝
        rep["mode"] = "copy"
        if os.path.abspath(pdf) != os.path.abspath(out_pdf):
            shutil.copy2(pdf, out_pdf)
        return rep
    gray_mask, blue_mask = _wm_masks(stack)
    # 跨页一致性只命中"白底上的水印芯"（压在内容上的随底色变化，不具一致性）。先轻度膨胀盖抗锯齿
    # 边缘，再闭运算桥接断段（蓝通道更长）。白化两档：核心区宽松、扩展区收紧，保护深色像素（防"静止"等被抹）。
    kern = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
    kern_c13 = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (13, 13))
    kern_cb = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (WM_BLUE_CLOSE, WM_BLUE_CLOSE))
    gray_mask = cv2.dilate(gray_mask.astype(np.uint8), kern, iterations=1)
    gray_mask = cv2.morphologyEx(gray_mask, cv2.MORPH_CLOSE, kern_c13)
    gray_mask = cv2.dilate(gray_mask, kern, iterations=1)
    blue_core = cv2.dilate(blue_mask.astype(np.uint8), kern, iterations=1)
    blue_big = cv2.morphologyEx(blue_core, cv2.MORPH_CLOSE, kern_cb)
    blue_big = cv2.dilate(blue_big, kern, iterations=1)
    doc = fitz.open(pdf)
    out = fitz.open()
    out.insert_pdf(doc)                      # 关键：原页原样保留（内容流 + 文字层 + 注释）
    for i in range(len(doc)):
        pix = doc[i].get_pixmap(matrix=fitz.Matrix(scale, scale))
        arr = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)[..., :3].copy()
        h, w = arr.shape[:2]
        mg = cv2.resize(gray_mask, (w, h), interpolation=cv2.INTER_NEAREST) > 0
        core = cv2.resize(blue_core, (w, h), interpolation=cv2.INTER_NEAREST) > 0
        big = cv2.resize(blue_big, (w, h), interpolation=cv2.INTER_NEAREST) > 0
        ext = big & ~core
        vals = arr.mean(axis=2)
        ai = arr.astype(np.int16)
        rr, gg, bb = ai[..., 0], ai[..., 1], ai[..., 2]
        # 全分辨率下的浅蓝判据（比检测稍宽，覆盖笔画边缘过渡）
        bluish = (bb - rr > 12) & (bb > 110) & (rr > 70) & (bb - gg > 2)
        bluish_ext = (bb - rr > WM_EXT_BREL) & (bb > 110) & (rr > 70) & (bb - gg > 2)
        # 1) 灰通道区：中间调灰（>WM_GRAY_MIN）置白；很亮（>235）须偏蓝（B-R>10）才白化（护浅色）；
        # 2) 蓝核心区：仅"偏蓝够亮（>WM_CORE_MIN）"像素（深色混合保留）；3) 蓝扩展区：仅"偏蓝够亮（>WM_EXT_MIN）"像素（白底清图文留）。
        mg_hit = mg & (vals > WM_GRAY_MIN) & ((bb - rr > 10) | (vals <= 235))
        wm_px = (mg_hit | (core & bluish & (vals > WM_CORE_MIN)) | (ext & bluish_ext & (vals > WM_EXT_MIN)))
        if not wm_px.any():
            continue                          # 本页无水印像素 -> 页面不动（零成本）
        inp = arr.copy()
        inp[wm_px] = 255
        diff = (inp != arr).any(axis=2)
        boxes = _diff_boxes(diff)
        pg = out[i]
        page_w, page_h = pg.rect.width, pg.rect.height
        # 换算比例：像素 -> 页面 pt（用实际 pix 尺寸，避免 round 误差）
        sx, sy = page_w / float(w), page_h / float(h)
        frac = (sum((b[2] - b[0]) * (b[3] - b[1]) for b in boxes)
                / float(w * h)) if boxes else 0.0
        if len(boxes) > WM_PATCH_MAX_N or frac > WM_PATCH_MAX_FRAC:
            # 补丁过多/占比过大：整页栅格（仍是"原页 + 盖图"，文字层保留）
            rgb = inp[..., ::-1]
            _ret, buf = cv2.imencode(".png", rgb, [cv2.IMWRITE_PNG_COMPRESSION, 2])
            pg.insert_image(pg.rect, stream=buf.tobytes())
            rep["raster_pages"].append(i)
            continue
        for (x0, y0, x1, y1) in boxes:
            patch = inp[y0:y1, x0:x1, ::-1]   # BGR
            _ret, buf = cv2.imencode(".png", patch, [cv2.IMWRITE_PNG_COMPRESSION, 2])
            pg.insert_image(fitz.Rect(x0 * sx, y0 * sy, x1 * sx, y1 * sy),
                            stream=buf.tobytes())
            rep["n_patch"] += 1
    if rep["raster_pages"]:
        rep["mode"] = "full"
        rep["note"] = f"{len(rep['raster_pages'])} 页整页栅格（原文字层仍在）"
    # 统一临时文件 + 原子替换：out_pdf 可能与 pdf 同路径（in-place），防 Windows 文件锁
    _fd, _tmp = tempfile.mkstemp(suffix=".pdf", dir=str(os.path.dirname(out_pdf) or "."))
    os.close(_fd)
    try:
        out.save(_tmp, deflate=True, garbage=4)
    finally:
        out.close()
        doc.close()
    os.replace(_tmp, out_pdf)
    return rep

# ------------------------- 步骤 4（前置）：末尾广告页 / 页脚推广行（无损） -------------------------
# 两者都"低风险、不碰像素"：* 末尾广告页：源文件末 1~N 页为站点推广页（"北京高考在线平台简介…"/"专注北京
#   高中升学…gaokzx…"），在【源】上按文字签名定页数，对【输出 PDF】无损删末 N 页（输出与源页数一致）。
# * 页脚推广行：少数矢量文件底部"关注北京高考在线官方微信：京考一点通…"矢量文字用 redact 标注 + apply_redactions()
#   无损覆盖（栅格化输出无文字层 -> no-op）。
# 识别必须在【源】上做（栅格化后文字层判定失效）；签名只用推广页专属词，'北京高考在线'/'京考一点通' 绝不可纳入
#   （否则尾页向前扫描误删正文页，已实测误删 8~15 页）。
AD_SIG = ("北京高考在线平台简介", "专注北京高中升学", "gaokzx")
FOOTER_PROMO_KEYWORDS = ("关注北京高考在线", "北京高考在线官方微信")

def _safe_save_replace(d, out_pdf):
    """PyMuPDF 同路径保存有潜在问题，统一先写临时文件再原子替换。
    注意：必须在关闭文档(释放对 out_pdf 的句柄)后再 replace，否则 Windows 下因文件被占用而 PermissionError。"""
    import tempfile
    import os as _os
    fd, tmp = tempfile.mkstemp(suffix=".pdf", dir=str(_os.path.dirname(out_pdf) or "."))
    _os.close(fd)
    try: d.save(tmp, deflate=True, garbage=4)
    finally: d.close()
    _os.replace(tmp, out_pdf)

def count_trailing_ad_pages(src):
    """在【源文件】上探测末尾广告页数量（尾页向前，遇首个非广告页即止）。
    广告页特征：页内文字含推广签名（站点名/域名/公众号）且文字量在正常推广区间。
    返回广告页数（0 表示无）。栅格化后的输出整页变成一张大图，会破坏文字层判定，故必须在源上探测。"""
    import pymupdf as fitz
    try: d = fitz.open(src)
    except Exception: return 0
    try:
        n = len(d)
        cnt = 0
        for i in range(n - 1, -1, -1):
            try: text = d[i].get_text()
            except Exception: break
            t = text.strip()
            # 仅用下界过滤空白/极短页（上界已移除——AD_SIG 才是真判据，且避免超长推广页被上界误判漏删）。
            if len(t) < 30: break
            if not any(sig in t for sig in AD_SIG): break
            cnt += 1
        return cnt
    finally: d.close()

def delete_trailing_ad_pages(out_pdf, n_ad):
    """从已生成的【输出 PDF】末尾删除 n_ad 页（广告页）。无损（仅删页，不碰其余页像素）。
    输出与源页数一致，故删源末 n_ad 页 == 删输出末 n_ad 页。返回实际删除数（0 表示无操作）。"""
    if n_ad <= 0: return 0
    import pymupdf as fitz
    import os as _os
    if not _os.path.exists(out_pdf): return 0
    d = fitz.open(out_pdf)
    try:
        if n_ad >= len(d): return 0
        for _ in range(n_ad): d.delete_page(len(d) - 1)
        _safe_save_replace(d, out_pdf)
        d = None
        return n_ad
    finally:
        if d is not None: d.close()

def remove_footer_promo(pdf, out_pdf):
    """无损删除矢量页脚推广行：对页脚区域(y>0.86*页高)命中推广关键词的文字 span
    加 redact 标注并 apply_redactions()（白底覆盖）。只动矢量文字、不碰栅格/烧入内容；
    栅格化输出无文字层 => 自动 no-op。返回是否改动。"""
    import pymupdf as fitz
    d = fitz.open(pdf)
    changed = False
    try:
        for pg in d:
            try: blocks = pg.get_text("dict")["blocks"]
            except Exception: continue
            page_h = pg.rect.height
            for b in blocks:
                if b.get("type") != 0:   # 仅处理文字块
                    continue
                for line in b.get("lines", []):
                    for span in line.get("spans", []):
                        y0 = span["bbox"][1]
                        txt = span["text"]
                        if y0 > page_h * 0.86 and any(k in txt for k in FOOTER_PROMO_KEYWORDS):
                            pg.add_redact_annot(fitz.Rect(*span["bbox"]), fill=(1, 1, 1))
                            changed = True
        if changed:
            for pg in d: pg.apply_redactions()
            _safe_save_replace(d, out_pdf)
            d = None
    finally:
        if d is not None: d.close()
    return changed

def _wm_process_one(pdf, out_root, src_root=None, dry=False):
    """单文件完整去水印：探测（图层/像素/广告页）+ 路线分流 + 落盘。**不写 DB**。专供 wm_worker.py
    在独立子进程里调用——子进程本身即崩溃隔离边界，故直接调用各探测函数（不再嵌套 _probe_source
    的二级子进程）。返回 dict：file/name/status(ok|none|fail|dry)/route/frac/n_ad/removed/footer/ad_del/err。"""
    src_root = Path(src_root) if src_root else OUT
    rel = os.path.relpath(pdf, str(src_root))
    outp = os.path.join(str(out_root), rel)
    name = os.path.basename(pdf)[:38]
    has_layer = has_layer_watermark(pdf)
    ok, frac = detect_watermark(pdf)
    n_ad = count_trailing_ad_pages(pdf)
    base = {"file": pdf, "name": name, "frac": round(float(frac), 6), "n_ad": int(n_ad)}
    if dry:
        route = "layer" if has_layer else ("raster" if ok else "none")
        return {**base, "dry": True, "route": route, "status": "dry"}
    os.makedirs(os.path.dirname(outp), exist_ok=True)
    # 路线 1：图层水印 -> 无损删（不栅格化、保留文字层）
    if has_layer:
        try: removed = remove_watermark_layer(pdf, outp)
        except Exception: removed = 0
        if removed > 0:
            footer = bool(remove_footer_promo(outp, outp))
            # 图层删后复检：仍检出像素水印（烧入式）则再用局部块修补白化（保留原文字层）。
            try: ok2, _ = detect_watermark(outp)
            except Exception: ok2 = False
            _rep2 = {}
            if ok2:
                try: _rep2 = remove_watermark_raster(outp, outp)
                except Exception: _rep2 = {}
            ad = delete_trailing_ad_pages(outp, n_ad)
            return {**base, "route": "layer", "status": "ok",
                    "removed": removed, "raster_followup": bool(ok2),
                    "footer": footer, "ad_del": ad,
                    "n_patch": _rep2.get("n_patch"),
                    "raster_pages": _rep2.get("raster_pages") or []}
        # 移除为 0 -> 结构异常，落到下方像素级兜底
    # 路线 2：无水印（也不含可删图层水印）
    if not has_layer and not ok:
        # 即便确属无水印，也要把源文件拷贝进产物目录，保证产物是完整 581 镜像
        shutil.copy2(pdf, outp)
        ad = delete_trailing_ad_pages(outp, n_ad)
        if n_ad > 0: return {**base, "route": "copy_ad", "status": "ok", "ad_del": ad}
        return {**base, "route": "none", "status": "ok", "ad_del": ad}
    # 路线 3：像素水印（烧入扫描图）-> 局部块修补白化（保留原页内容流与文字层；仅补丁
    # 过多/占比过大时才整页盖图，且页号写入 raster_pages 供日志追溯）。
    _rep3 = remove_watermark_raster(pdf, outp)
    ad = delete_trailing_ad_pages(outp, n_ad)
    return {**base, "route": "raster", "status": "ok", "ad_del": ad,
            "wm_mode": _rep3.get("mode"), "n_patch": _rep3.get("n_patch"),
            "raster_pages": _rep3.get("raster_pages") or []}

def _wm_consume(con, f, res, err, dry):
    """处理子进程返回结果：打印日志 +（非 dry）写 DB 状态。返回 ok/none/fail/dry。"""
    name = os.path.basename(f)[:38]
    if res is None:
        if not dry: mark(con, f, "wm", "fail")
        print(f"[WM] [!] 失败 {name} | {err}")
        return "fail"
    route, st = res.get("route"), res.get("status")
    pct = float(res.get("frac") or 0.0) * 100
    if dry:
        if route == "layer": r = "图层水印 -> 无损删除"
        elif route == "raster": r = f"像素水印(占比 {pct:.2f}%) -> 局部块修补"
        else: r = f"无水印(占比 {pct:.2f}%) 跳过"
        ad_note = f" + 删末尾广告 {res['n_ad']} 页" if res.get("n_ad") else ""
        print(f"[WM] {r}{ad_note} {name}")
        return "dry"
    if st == "ok":
        if route == "layer":
            print(f"[WM] 图层水印 -> 无损删除(移除 {res.get('removed')} 块) {name}")
            if res.get("n_patch"): print(f"[WM]   + 烧入残余局部块修补 {res['n_patch']} 块 {name}")
            if res.get("footer"): print(f"[WM]   + 页脚推广行 矢量删除 {name}")
            if res.get("ad_del"): print(f"[WM]   + 删末尾广告页 {res['ad_del']} 页 {name}")
        elif route == "raster":
            # 局部块修补为主（保留文字层）；整页盖图仅少数页且页号可追溯
            _np = res.get("n_patch")
            _rp = res.get("raster_pages") or []
            _how = (f"局部块修补 {_np} 块" if _np else "白化")
            if _rp: _how += f"，{len(_rp)} 页整页盖图(页号 {','.join(str(x + 1) for x in _rp[:8])}{'...' if len(_rp) > 8 else ''})"
            print(f"[WM] 检测到像素水印(占比 {pct:.2f}%) -> {_how} {name}")
            if res.get("ad_del"): print(f"[WM]   + 删末尾广告页 {res['ad_del']} 页 {name}")
        else:  # copy_ad
            print(f"[WM] 无水印但有广告页 {res.get('n_ad')} 页 -> 拷贝+无损删页 {name}")
        mark(con, f, "wm", "ok")
        return "ok"
    if st == "none":
        print(f"[WM] 无水印(占比 {pct:.2f}%) 跳过 {name}")
        mark(con, f, "wm", "none")
        return "none"
    mark(con, f, "wm", "fail")
    print(f"[WM] [!] 失败({route}) {name} {str(res.get('err', ''))[:200]}")
    return "fail"

# ------------------------- 单实例锁（防并发写同一 DB/输出目录） -------------------------
def _pid_alive(pid):
    """判断 PID 是否仍在运行（Windows 用 OpenProcess 探测，其它平台用 os.kill 0）。仅用于**辅助**
    判定锁归属，探测失败一律返回 None（未知），由调用方按"保守当存活"处理。"""
    try: pid = int(pid)
    except (TypeError, ValueError): return None
    if pid <= 0: return None
    try:
        if os.name == "nt":
            import ctypes
            _k32 = ctypes.windll.kernel32            # noinspection PyUnresolvedReferences
            h = _k32.OpenProcess(0x1000, False, pid)   # noinspection PyUnresolvedReferences
            if not h: return False
            _k32.CloseHandle(h)                      # noinspection PyUnresolvedReferences
            return True
        os.kill(pid, 0)
        return True
    except Exception: return None

def _acquire_lock():
    import time as _t
    lock = ROOT / "pipeline.wm.lock"
    if lock.exists():
        try: age = _t.time() - lock.stat().st_mtime
        except OSError: age = 0
        # 先看锁里 PID 是否还活着：明确死亡 -> 无条件视为残留锁直接接管（不必等 6h），
        # 修的是"崩过一次就把后续运行挡 6 小时"的老毛病。
        try: owner = lock.read_text(encoding="utf-8", errors="replace").strip()
        except OSError: owner = ""
        alive = _pid_alive(owner) if owner else None
        if alive is False:
            try: lock.unlink()
            except OSError: pass
        # 锁未过期（<6h）且无法证明进程已死 -> 视为真正在跑，放弃，避免并发写 DB/产物
        elif age < 6 * 3600: return False
        else:
            # 过期锁：多半是上次崩溃残留，覆盖之
            try: lock.unlink()
            except OSError: pass
    try:
        lock.write_text(str(os.getpid()))
        return True
    except OSError: return False

def _release_lock():
    lock = ROOT / "pipeline.wm.lock"
    try: lock.unlink()
    except OSError: pass

def _scan(stage, bid):
    """scan_pdfs 统一入口（批次 + --file-id 收窄）。全流程唯一的"取文件"路径：内部调
    batchcore.scan_pdfs；ONLY_FILE_IDS 非空时只保留其中的 file_id（看板单文件调度）。"""
    pairs = bc.scan_pdfs(stage, bid) if bc is not None else []
    if ONLY_FILE_IDS:
        want = {str(x).strip().upper() for x in ONLY_FILE_IDS}
        pairs = [pp for pp in pairs if str(pp[0] or "").strip().upper() in want]
    return pairs

def _bstep(bid, fid, step, status, src="", dst="", error="", src_sha=None):
    """file_steps 统一写库：started/ok/fail/skip（失败静默，不影响主流程）。src_sha 非空时记录输入内容哈希。"""
    try:
        if bc is None or not bid or not fid: return
        bc.db_step(bid, fid, step, status, src=clean_text(src, 500) or "", dst=clean_text(dst, 500) or "", error=clean_text(error, 500) or "", src_sha=src_sha)
    except Exception: pass

def _wskip(bid, fid, step, path, dry, why):
    """跳过状态写库：dry-run 不写；已 ok 的保留 ok（不倒退成 skip）。"""
    try:
        if dry or bc is None or not bid or not fid: return
        if bc.is_step_ok(bid, fid, step): return
        bc.db_step(bid, fid, step, bc.ST_SKIP, src=path, error=why)
    except Exception: pass

def _resolve_bid(batch_id):
    """批次号解析：显式参数优先，其次 current.json 当前批次。"""
    bid = (batch_id or "").strip() if isinstance(batch_id, str) else batch_id
    if not bid and bc is not None: bid = bc.current_batch_id()
    return bid


# --------------------- 空批次前置校验 / zip 确认 / 崩溃落盘 ---------------------
def has_targets(batch_id):
    """阶段入口前置校验：本批次 manifest 是否真的有可处理文件。
    manifest 不存在 / files=0 -> False（调用方应直接跳过该批，绝不带空批次进 wm/rename/ocr/qc/ads/pack/accept）。"""
    if bc is None or not batch_id:
        return False
    try:
        mf = bc.load_manifest(batch_id)
    except Exception:
        return False
    return bool(mf and mf.get("files"))


def _batch_zips_exist(batch_id):
    """返回本批次 step=7(打包) 状态 ok 且物理文件真实存在的 zip 路径列表（自动验收前确认 zip 已生成）。
    批次级无记录时回退查同 run_id 下任一兄弟批次的 step=7 ok zip（批次级存在性降级为 run 级）：
    配合问题 3 整专题合并包——即使中间批次自身未写 step=7，只要同一 run 已整专题打包，各批仍判“有 zip”。
    查不到 / 无记录 / 文件已不在 -> 空列表（调用方据此跳过 accept，绝不对“没有 zip 的批次”验收）。"""
    if bc is None or not batch_id:
        return []

    def _q(bids):
        out = []
        bids = [b for b in bids if b]
        if not bids:
            return out
        try:
            _zc = sqlite3.connect(str(DB))
            try:
                _ph = ",".join("?" * len(bids))
                rows = _zc.execute(
                    f"SELECT DISTINCT dst FROM file_steps WHERE batch_id IN ({_ph}) AND step=? "
                    "AND status='ok' AND dst LIKE '%.zip'", (*bids, STEP_PACK)).fetchall()
            finally:
                _zc.close()
            for _r in rows:
                _z = _r[0] if _r else None
                if _z and os.path.isfile(str(_z)) and _z not in out:
                    out.append(str(_z))
        except sqlite3.Error:
            pass
        return out

    zips = _q([batch_id])
    if zips:
        return zips
    # run 级回退
    try:
        _run_id = bc.db_run_of_batch(batch_id)
    except Exception:
        _run_id = None
    if _run_id:
        try:
            _sib = [b for b in (bc.db_batches_of_run(_run_id) or []) if b and b != batch_id]
        except Exception:
            _sib = []
        zips = _q(_sib)
    return zips


LOG_DIR = LOG_ROOT / "logs"
_CRASH_HOOK_ON = False     # install_auto_crash_hook 幂等标志（只装一次顶层异常钩子）


def _write_auto_crash_log(exc_obj=None, tag="auto"):
    """把当前异常（或传入异常对象）的完整 traceback 落盘到 D:\\work\\gaokzx\\logs\\auto_crash_<时间>.log。
    返回日志路径；写盘失败返回 None（绝不因“写日志”再抛异常影响主流程）。闪退专项排查要求：每次崩溃都留全量 traceback。"""
    import traceback as _tb
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        p = LOG_DIR / f"auto_crash_{stamp}.log"
        if exc_obj is not None:
            tb_text = "".join(_tb.format_exception(type(exc_obj), exc_obj, exc_obj.__traceback__))
        else:
            tb_text = _tb.format_exc()
        head = [
            f"[auto-crash] tag={tag} time={datetime.datetime.now().isoformat()}",
            f"[auto-crash] argv={' '.join(sys.argv[:6])}",
            f"[auto-crash] cwd={os.getcwd()}  python={sys.version.split()[0]}",
            _DASH,
        ]
        with open(str(p), "w", encoding="utf-8") as fh:
            fh.write("\n".join(head) + "\n" + tb_text + "\n")
        print(f">>> [auto] [!] 崩溃完整 traceback 已写入日志：{p}")
        return str(p)
    except Exception:
        return None


def install_auto_crash_hook():
    """安装全局未捕获异常钩子：任何逃逸到解释器顶层的崩溃都先落盘完整 traceback 再走默认处理。
    仅在全自动路径（--auto / 菜单[1]）调用，避免污染交互式正常使用。"""
    global _CRASH_HOOK_ON
    if _CRASH_HOOK_ON:
        return                     # 幂等：只装一次，多次 run_auto 不叠加钩子
    _CRASH_HOOK_ON = True
    _prev = sys.excepthook

    def _hook(etype, value, tb):
        try:
            _write_auto_crash_log(exc_obj=value, tag="uncaught")
        except Exception:
            pass
        try:
            _prev(etype, value, tb)
        except Exception:
            pass

    sys.excepthook = _hook

# --------------------- DB↔磁盘 一致性（跳过三条件 + 双向核对） ---------------------
FORCE_MISSING = False   # --force-missing：只强制重跑“DB done 但磁盘缺失”的文件


def _file_present(p):
    """产物真实存在且非零字节。"""
    try:
        return bool(p) and os.path.isfile(str(p)) and os.path.getsize(str(p)) > 0
    except OSError:
        return False


def _skip_ok(bid, fid, step, product_path, src_for_sha=None, extra_ok=None, curpath=None):
    """跳过三条件（缺一不可）：该步 DB.status==ok 且 产物真实存在(>0字节) 且 sha256(输入)==DB.src_sha。
    src_for_sha：用于算内容哈希的文件；省略时用 product_path（原地改写步骤：产物即输入）。
    extra_ok：步骤专属语义判定（如 ocr 传 has_text_layer 结果）。老数据无 src_sha 时补记并本轮放行（不误伤历史）。
    curpath：manifest 当前权威路径字典；原地链步 2~5 登记产物被下游 rename 改名而失效时，
    回落当前路径判断（与 reconcile 语义一致，避免单独重跑步骤2 把已改名健康文件全量重处理）。
    返回 True 才可安全跳过；否则重处理。"""
    if bc is None or not bid or not fid:
        return False
    if not bc.is_step_ok(bid, fid, step):
        return False
    if not _file_present(product_path):
        # 原地链回落：登记路径被下游改名，但当前权威路径存在 -> 用当前路径继续三条件判定
        cp = (curpath or {}).get(fid)
        if not (cp and _file_present(cp)
                and bc.STEP_WATERMARK <= int(step or 0) <= bc.STEP_QC):
            return False
        product_path = cp
    if extra_ok is not None and not extra_ok:
        return False
    stored = bc.get_step_sha(bid, fid, step)
    _tgt = src_for_sha or product_path
    cur = _sha256_file(_tgt) if _tgt else None
    if stored is None:
        if cur:
            try: bc.set_step_sha(bid, fid, step, cur)
            except Exception: pass
        return True                       # 老数据补记后本轮放行
    if cur is None:
        return False                      # 输入读不到 -> 不跳过，重处理
    return cur == stored                  # 内容被替换（sha 不等）-> 不跳过，重处理


def _manifest_curpath(bid):
    """从 manifest 取每个文件的【当前权威路径】file_id -> path。
    watermark/rename/ocr/qc 都在「去水印后」同一份工作文件上原地推进（rename 只改名并
    update_file_path），所以 step2 登记的 dst 是改名前旧名——缺失判定必须回落到当前名。"""
    out = {}
    try:
        mf = bc.load_manifest(bid) or {}
    except Exception:
        return out
    for f in mf.get("files", []):
        _fid, _p = f.get("file_id"), f.get("path")
        if _fid and _p:
            out[_fid] = _p
    return out


def _step_product_missing(bid, fid, step, rec, curpath):
    """某步登记产物是否真缺失：登记的 dst/src 不存在时，若该步属于「去水印后原地链」
    （watermark/rename/ocr/qc，步号 2~5）且文件当前权威路径仍在磁盘，则视为存在（仅被下游改名），
    不算 MISSING——杜绝把 331 个已改名的健康文件误回滚重跑。"""
    prod = rec.get("dst") or rec.get("src")
    if _file_present(prod):
        return False
    if step is not None and bc.STEP_WATERMARK <= int(step) <= bc.STEP_QC:
        cp = curpath.get(fid)
        if cp and _file_present(cp):
            return False          # 原地链文件只是改了名，仍在磁盘
    return True


def _pdf_sanity_check(p):
    """非零字节 + %PDF 头 + %%EOF 尾完整性校验：复用下载器同源实现（gaokzx_download._pdf_sanity）；
    导入异常时退化为仅非零字节判定（_file_present）。"""
    try:
        from gaokzx_download import _pdf_sanity
        return bool(_pdf_sanity(str(p)))
    except Exception:
        return _file_present(p)


def reconcile_round(bid, verbose=True, force_missing=False):
    """每轮开始对当前批次 manifest 做双向核对，修正 DB 与磁盘不一致（修“已跳过但产物 MISSING”）：
      (A) 某步 DB=ok 但其产物真缺失（登记路径与当前权威路径均不存在）-> 回滚该步及下游为 started，重处理；
          注：原地链步 2~5 因下游 rename 改名而登记路径失效的，不算缺失（回落当前路径判断）。
      (B) manifest 登记的当前路径磁盘存在且通过非零/非损坏校验，但 download 步无 ok 记录 -> 补记 download ok（含 sha）。
    force_missing=True（--force-missing）时额外回滚“产物存在但内容哈希与登记值不符”的 ok 行（输入被替换）。
    返回 (回滚文件数, 补记文件数)。"""
    if bc is None or not bid:
        return (0, 0)
    curpath = _manifest_curpath(bid)
    reset_files = set()
    sha_resets = 0
    try:
        rows = bc.db_files_of(bid)
    except Exception:
        rows = []
    step_of = {}
    for r in rows:
        step_of.setdefault(r.get("file_id"), []).append(r)
    for fid, frs in step_of.items():
        for r in frs:
            if (r.get("status") or "").lower() != "ok":
                continue
            _st = r.get("step")
            if _st == bc.STEP_ACCEPT:
                continue
            _missing = _step_product_missing(bid, fid, _st, r, curpath)
            _why = "产物缺失回滚"
            if not _missing and force_missing:
                # --force-missing：产物在但内容哈希与登记值不符（输入被替换）-> 同样回滚重跑
                _stored = r.get("src_sha") or bc.get_step_sha(bid, fid, _st)
                if _stored:
                    _tgt = (r.get("dst") or r.get("src"))
                    if not _file_present(_tgt):
                        _tgt = curpath.get(fid)
                    _cur = _sha256_file(_tgt) if _tgt else None
                    if _cur is not None and _cur != _stored:
                        _missing, _why = True, "内容哈希不符回滚(force-missing)"
                        sha_resets += 1
            if not _missing:
                continue
            try:
                # 回滚本步 + 其下游步骤（下标更大的处理步骤）为 started，令其重处理
                bc.db_step(bid, fid, _st, bc.ST_STARTED, error="reconcile: " + _why)
                for _ds in range(int(_st) + 1, bc.STEP_PACK + 1):
                    _dr = bc.db_step_get(bid, fid, _ds)
                    if _dr and (_dr.get("status") or "").lower() == "ok":
                        bc.db_step(bid, fid, _ds, bc.ST_STARTED, error="reconcile: 上游产物缺失回滚")
                reset_files.add(fid)
            except Exception:
                pass
    backfilled = 0
    bad_orphan = 0
    try:
        mf = bc.load_manifest(bid) or {}
        for f in mf.get("files", []):
            _fid, _p = f.get("file_id"), f.get("path")
            if not _fid or not _p:
                continue
            dl = bc.db_step_get(bid, _fid, bc.STEP_DOWNLOAD)
            present = _file_present(_p)
            if present and (not dl or (dl.get("status") or "").lower() != "ok"):
                if not _pdf_sanity_check(_p):
                    bad_orphan += 1          # 零字节/损坏：不补记，留给下载重拉
                    continue
                try:
                    bc.db_step(bid, _fid, bc.STEP_DOWNLOAD, "ok", src=_p, dst=_p,
                               src_sha=_sha256_file(_p))
                    backfilled += 1
                except Exception:
                    pass
    except Exception:
        pass
    if verbose and backfilled:
        print(f">>> [reconcile] 补记 {backfilled} 条，状态=ok(download)，"
              "原因=磁盘存在但 DB 无 download 记录（已过非零/非损坏校验）"
              + (f"；另有 {bad_orphan} 条磁盘文件未过完整性校验（零字节/损坏），不补记" if bad_orphan else ""))
    if verbose and (reset_files or sha_resets):
        print(f">>> [reconcile] DB↔磁盘核对：回滚 {len(reset_files)} 个文件至 pending 重处理"
              + (f"（其中哈希不符 {sha_resets} 步，force-missing）" if sha_resets else ""))
    return (len(reset_files), backfilled)


def step_fix_dst(batch_id=None, dry=True):
    """一次性迁移（还 watermark dst 悬空历史债）：把原地链步骤（2~5）在 file_steps 登记的
    已悬空 dst/src 更新为 manifest 当前权威路径（仅迁“登记路径不存在且当前路径存在”的行）。
    新改名的回填已由 apply_renames -> bc.db_repath 在源头保证，本函数只消化存量旧数据。
    默认 dry-run 预览，--force 才真正写库。返回迁移行数。"""
    if bc is None:
        print(">>> [fix-dst] batchcore 不可用，无法迁移。")
        return 0
    bids = []
    if batch_id:
        bids = [batch_id]
    else:
        try:
            _c = bc._conn()
            try:
                bids = [r[0] for r in _c.execute(
                    "SELECT DISTINCT batch_id FROM file_steps ORDER BY batch_id")]
            finally:
                _c.close()
        except Exception as e:
            print(f">>> [fix-dst] 枚举批次失败：{type(e).__name__}: {e}")
            return 0
    total = 0
    for bid in bids:
        curpath = _manifest_curpath(bid)
        if not curpath:
            continue
        try:
            rows = bc.db_files_of(bid)
        except Exception:
            rows = []
        upd = shown = 0
        for r in rows:
            _st = r.get("step")
            if _st is None or not (bc.STEP_WATERMARK <= int(_st) <= bc.STEP_QC):
                continue
            fid = r.get("file_id")
            cp = curpath.get(fid)
            if not cp or not _file_present(cp):
                continue
            for _col in ("dst", "src"):
                old = r.get(_col)
                if not old or _file_present(old):
                    continue
                if os.path.normcase(str(old)) == os.path.normcase(str(cp)):
                    continue
                if shown < 20:
                    print(f"    [fix-dst] 批{bid[:22]} fid={fid} 步{STEP_NAMES.get(int(_st))} {_col}: "
                          f"{os.path.basename(str(old))}  ->  {os.path.basename(str(cp))}")
                    shown += 1
                elif shown == 20:
                    print("      ... 本批其余略（--force 执行时全量迁移）")
                    shown += 1
                if not dry:
                    upd += bc.db_repath(bid, fid, str(old), str(cp))
                else:
                    upd += 1
        total += upd
        if upd:
            print(f">>> [fix-dst] 批次 {bid}：{'待迁移' if dry else '已迁移'} {upd} 处悬空登记")
    print(f">>> [fix-dst] 完成：{'预览（未改动，加 --force 正式写库）' if dry else '正式迁移'} 共 {total} 处")
    return total

def _merge_retry(step, dirs, target, skipped_set=None, skip=0):
    """看板重试队列并入：被点名的文件强制加入 target（绕过断点跳过，不受 --limit 影响）。
    返回 (retry_items, skip)；skipped_set 非空时同步修正 skip 计数。"""
    retry_items = []
    for _rfp, _rfr in _retry_files_for(step, dirs):
        if _rfp not in target:
            target.append(_rfp)
            if skipped_set is not None and _rfp in skipped_set: skip -= 1
        retry_items.append((_rfp, _rfr))
    if retry_items: print(f">>> [{step}] 看板重试并入 {len(retry_items)} 个文件")
    return retry_items, skip

def _ask_apply(step_cn):
    """dry-run 预览后的正式执行确认（默认 N；EOF/中断一律视为否）。
    AUTO_MODE：直接返回 True（自动确认正式执行，跳过人工确认）。"""
    if AUTO_MODE:
        print(f"  [自动模式] 预览确认【{step_cn}】-> y（直接正式执行）")
        return True
    try: ans = input(f"\n以上为预览结果。是否正式执行【{step_cn}】？[y/N]: ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        return False
    return ans in ("y", "yes")

def step_watermark(con, out_root=None, limit=None, dry=False, force=False,
                   workers=6, src=None, hardest=None, force_scope="all",
                   batch_id=None, ask_apply=True):
    """去水印主步骤。每个文件在独立子进程里完整处理（probe+route+落盘），workers>1 并发跑：隔离 C 层崩溃又吃满
    多核；DB 状态统一在父进程写（SQLite 单写者）。src/hardest 用于小批量压测（指定源目录 / 只取最大的 N 个）。
    批次模式：文件范围来自当前批次 manifest（scan_pdfs("pending", batch_id)）；只有显式传 --src 时退化为手工目录
    模式。dry-run 预览后询问"是否正式执行？[y/N]"（默认 N），确认后才真跑；只有真跑成功才写 ok（预览不写）。
    force_scope："all"=--force 作用所有文件（整库重跑，慎用）；"new"=只强跑「产物尚不存在」的增量文件（已处理照常跳过）。"""
    import concurrent.futures as cf
    import time
    import json
    if not _acquire_lock():
        print(">>> [wm] 已有其它 step_watermark 实例在运行（lock 未过期），本实例退出以避免并发写冲突")
        return
    try:
        # ---- 取文件范围：批次 manifest 优先（禁止全目录扫描） ----
        bid = _resolve_bid(batch_id)
        fid_of = {}
        if src:
            # 手工/压测模式：显式源目录（--src），不受批次限制；不写批次状态
            print(">>> [wm] [!] 手工模式（--src 显式指定）：不按批次 manifest 过滤，仅用于压测")
            bid = None
            src_root = Path(src)
            files = list(iter_pdfs(str(src_root)))
        else:
            src_root = STAGE_PENDING
            if bc is not None and bid:
                pairs = _scan("pending", bid)
                files = [pp for _f, pp in pairs]
                fid_of = {pp: _f for _f, pp in pairs}
                print(f">>> [wm] 批次 {bid}：待处理目录命中 {len(files)} 个 PDF（manifest 限定）")
                if not files:
                    print(">>> [wm] 本批次在「待处理」目录没有可处理的 PDF" "（可能已全部流转到下一阶段，或尚未下载）。")
                    print(f"        当前批次: {bid}")
                    return
            else:
                print(">>> [wm] 没有当前批次（_批次/current.json 不存在）。")
                print("        请先执行 步骤1 抓取下载（会自动创建批次），或显式传 --src <目录>。")
                return
        out_root = Path(out_root) if out_root else (src_root.parent / (src_root.name + "_去水印后"))
        os.makedirs(str(out_root), exist_ok=True)
        if hardest: files = sorted(files, key=lambda p: os.path.getsize(p), reverse=True)[: int(hardest)]
        # 原地链当前权威路径：下游 rename 改过名的文件，产物判定回落 manifest 当前名（与 reconcile 一致）
        _curpath = _manifest_curpath(bid) if (bc is not None and bid) else {}
        _outp_norm = os.path.normcase(str(out_root)) + os.sep
        todo, skip = [], 0
        skipped_set = set()   # 看板重试并入时用于修正 skip 计数
        for f in files:
            try:
                rel = os.path.relpath(f, str(src_root))
                prod = out_root / rel
                _prod = str(prod) if (prod.exists() and prod.stat().st_size > 0) else None
            except OSError:
                _prod = None
            if _prod is None:
                # 登记旧名不存在：若该文件当前权威路径已在去水印目录内（被 rename 改过名），视为产物存在
                _cp = _curpath.get(fid_of.get(f))
                if _cp and os.path.normcase(_cp).startswith(_outp_norm) and _file_present(_cp):
                    _prod = _cp
            if force and force_scope == "new":
                # 增量模式：产物已存在且非空 => 视为已处理，跳过（不重跑旧成果）
                if _prod:
                    skip += 1
                    skipped_set.add(f)
                    _wskip(bid, fid_of.get(f), STEP_WATERMARK, f, dry, "产物已存在（增量跳过）")
                    continue
            elif _skip_ok(bid, fid_of.get(f), STEP_WATERMARK, _prod or f,
                          extra_ok=bool(_prod), curpath=_curpath) and not force:
                # 三条件（DB ok + 产物存在 + sha 一致）才跳过；FORCE_MISSING 时缺失产物不跳过
                skip += 1
                skipped_set.add(f)
                _wskip(bid, fid_of.get(f), STEP_WATERMARK, f, dry, "断点续传跳过（三条件）")
                continue
            todo.append(f)
            if limit is not None and len(todo) >= limit: break
        # 看板重试队列：被点名的文件强制并入（绕过断点续传/产物已存在跳过，不受 --limit 影响）
        retry_items, skip = _merge_retry(
            "wm", (src_root, STAGE_CLEANED, STAGE_DONE, STAGE_ADS), todo, skipped_set, skip)
        workers = max(1, int(workers or 1))
        print(f">>> [wm] 待处理 {len(todo)}，已跳过 {skip}，并发 {workers}；" f"源: {src_root}；输出: {out_root}")
        if not todo:
            # 空跑防呆：不要只说"完成 0"，要明确告诉用户这是"被跳过"而不是"没问题"
            if skip:
                print(f">>> [wm] [!] 全部 {skip} 个文件都因断点续传被跳过（真跑 0 个）。")
                print(f"        如需强制重跑验证：加 --force")
                print(f"        例： python pipeline.py --step watermark --limit 3 --force")
            else:
                print(f">>> [wm] 源目录 {src_root} 下没有 PDF 文件（真跑 0 个）。")
                print(f"        请确认目录是否正确，或先跑 --step download 下载。")
            return
        statusio.begin_step("wm", total=len(todo), skipped=skip, out_dir=str(out_root), note=f"并发 {workers}")
        def _mark_running(f):
            """看板 + 批次状态：dispatch 时先标 RUNNING/started（失败静默）。"""
            if bc is not None and bid and fid_of.get(f) and not dry:
                bc.db_step(bid, fid_of[f], STEP_WATERMARK, bc.ST_STARTED, src=f)
            try: statusio.update_detail("wm", f, os.path.basename(f), "RUNNING", note="处理中")
            except Exception: pass
        def run_one(f):
            _t_one = time.time()        # 看板：单文件耗时
            cmd = [str(PY), str(WM_WORKER), f, str(out_root), str(src_root), "1" if dry else "0"]
            try: r = subprocess.run(cmd, capture_output=True, timeout=1800)
            except Exception as e: return f, None, f"launch/timeout: {e}"
            if r.returncode != 0:
                tail = r.stderr.decode("utf-8", "replace")[-300:]
                return f, None, f"exit {r.returncode}: {tail}"
            out = r.stdout.decode("utf-8", "replace")
            i = out.find("RESULT")
            if i < 0: return f, None, "no RESULT"
            try:
                res = json.loads(out[i + 6:])
                if isinstance(res, dict):   # 看板：回填单文件耗时（不改子进程协议）
                    res.setdefault("elapsed_sec", round(time.time() - _t_one, 2))
                return f, res, None
            except Exception as e: return f, None, f"bad json: {e}"
        def _wm_stat(fpath, r, res, err):
            """看板 + 台账：单文件结果写进 detail_wm.json 与 file_events。台账里 dst_path = 产物
            绝对路径（去水印后的存放位置）；dry-run(SKIP) 不算实际事件，不落台账。"""
            res = res or {}
            _st = {"ok": "DONE", "none": "DONE", "fail": "FAIL", "dry": "SKIP"}.get(r, "WARN")
            _route = res.get("route", "")
            _note = ""
            if r in ("ok", "dry"):
                if _route == "layer": _note = f"图层水印→无损删除({res.get('removed', '?')}块)"
                elif _route == "raster":
                    _np = res.get("n_patch"); _rp = res.get("raster_pages") or []
                    _note = (f"像素水印→局部块修补({_np}块)" if _np else "像素水印→白化")
                    if _rp: _note += f"；整页盖图{len(_rp)}页(文字层保留)"
                elif _route == "copy_ad": _note = "无水印+删广告页"
                else: _note = "无水印"
                if res.get("ad_del"): _note += f"；删广告页{res['ad_del']}页"
            elif r == "fail": _note = str(res.get("err") or err or "")[:160]
            _outp = None
            _dst_sz = _src_sz = None
            try:
                _rel = os.path.relpath(fpath, str(src_root))
                _outp = os.path.join(str(out_root), _rel)
                _dst_sz = os.path.getsize(_outp) if os.path.exists(_outp) else None
                _src_sz = os.path.getsize(fpath) if os.path.exists(fpath) else None
            except OSError: pass
            if _st != "SKIP":
                try:
                    ledgerio.log("wm", _st, src_path=fpath,
                                 dst_path=(_outp if _outp and os.path.exists(_outp)
                                           else None),
                                 size_bytes=_src_sz,
                                 elapsed_sec=res.get("elapsed_sec"),
                                 note=_note)
                except Exception: pass
            # 批次状态（file_steps）：只有正式执行的真实结果才写（ok/fail）；成功后同步推进 manifest 路径。
            if (bc is not None and bid and fid_of.get(fpath) and not dry):
                _ok = r in ("ok", "none")
                _dstp = _outp if (_outp and os.path.exists(_outp)) else None
                _bstep(bid, fid_of[fpath], STEP_WATERMARK,
                       bc.ST_OK if _ok else bc.ST_FAIL,
                       src=fpath, dst=_dstp,
                       src_sha=(_sha256_file(_dstp) if (_ok and _dstp) else None),
                       error=("" if _ok else _note))
                if _dstp:
                    try: bc.update_file_path(bid, fid_of[fpath], _dstp)
                    except Exception: pass
            try:
                statusio.update_detail("wm", fpath, os.path.basename(fpath), _st,
                                       elapsed_sec=res.get("elapsed_sec"),
                                       src_bytes=_src_sz, dst_bytes=_dst_sz,
                                       note=_note)
            except Exception: pass
        while True:
            t0 = time.time()
            tally = {"ok": 0, "none": 0, "fail": 0}
            _retry_processed = set()    # 看板重试：实际派发过的文件（中断未派发的不算）
            if dry: print(f">>> [wm] (dry-run) 预览中（{len(todo)} 个）：只探测不写产物、不落库")
            # 进度条 + Ctrl+C 安全中断（第一次按下让当前文件收尾后停，状态已逐条落库）
            stop = _wf_install_sigint("去水印")
            bar = _wf_bar(len(todo), "去水印", enabled=not dry)
            with bar as _b:
                if workers == 1:
                    for f in todo:
                        if stop():
                            print("    [!] 收到中断信号，停止派发新文件")
                            break
                        _mark_running(f)
                        rf, res, err = run_one(f)
                        _retry_processed.add(rf)
                        r = _wm_consume(con, rf, res, err, dry)
                        _wm_stat(rf, r, res, err)
                        if not dry: tally[r] = tally.get(r, 0) + 1
                        _b.update(1, label=os.path.basename(f),
                                  ok=(r == "ok"), name=(None if dry else os.path.basename(f)))
                else:
                    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
                        futs = {}
                        for f in todo:
                            _mark_running(f)
                            futs[ex.submit(run_one, f)] = f
                        for fut in cf.as_completed(futs):
                            f = futs[fut]
                            rf, res, err = fut.result()
                            _retry_processed.add(f)
                            r = _wm_consume(con, rf, res, err, dry)
                            _wm_stat(rf, r, res, err)
                            if not dry: tally[r] = tally.get(r, 0) + 1
                            _b.update(1, label=os.path.basename(f),
                                      ok=(r == "ok"), name=(None if dry else os.path.basename(f)))
                            if stop():
                                # 不再等待剩余任务，但已派发的让它自然结束
                                pass
            # 收尾自检：每个源文件在产物目录都应存在对应文件（none 类也会拷贝，见 _wm_process_one 路线 2）。
            missing_out = [
                f for f in files
                if not os.path.exists(os.path.join(str(out_root), os.path.relpath(f, str(src_root))))
            ]
            if dry:
                print(f">>> [wm] (dry-run) 预告 {len(todo)} 个，已跳过 {skip}；输出目录: {out_root}")
                # 用户要求：dry-run 预览后必须询问是否正式执行（默认 N）；确认才真跑，只有真跑成功才写 ok。
                if ask_apply and _ask_apply("去水印"):
                    print(">>> [wm] 确认正式执行 —— 开始真跑（写产物 + 写 pipeline.db）")
                    dry = False
                    continue
                print(">>> [wm] 仅预览，未正式执行（pipeline.db 不写 ok）。")
                break
            print(f">>> [wm] 完成 {tally['ok']}，无水印 {tally['none']}，失败 {tally['fail']}，"
                  f"已跳过 {skip}，耗时 {time.time() - t0:.1f}s；输出目录: {out_root}")
            if missing_out:
                print(f">>> [wm] 自检告警：{len(missing_out)} 个源文件在产物目录缺失：")
                for f in missing_out[:30]: print("   MISSING", f)
            else: print(f">>> [wm] 自检通过：{len(files)} 个源文件均有对应产物")
            break
        if retry_items and not dry:   # 看板重试：已处理到的项标记 done（dry-run 不动队列）
            _retry_mark_done("wm", [r for p, r in retry_items if p in _retry_processed])
        # 看板：正常收尾（dry-run 记 SKIP）
        statusio.end_step("wm", status=("SKIP" if dry else None),
                          note=("dry-run 预览" if dry else
                                f"完成 {tally['ok']} / 无水印 {tally['none']} / "
                                f"失败 {tally['fail']}"))
    except BaseException:   # 看板：异常中断也把状态落成 FAIL，避免看板永远 RUNNING
        statusio.end_step("wm", status="FAIL", note="步骤异常中断（详见控制台）")
        raise
    finally: _release_lock()

# 阶段目录迁移与清理
def step_prune_db(con, dry=True):
    """删除 pipeline.db 中源文件已不存在的记录（避免影响增量判断）。"""
    rows = con.execute("SELECT path, step FROM files").fetchall()
    missing = [(p, s) for p, s in rows if not os.path.exists(p)]
    print(f">>> [prune-db] 失效路径记录 {len(missing)} 条")
    for p, s in missing[:20]: print(f"      [{s}] {p}")
    if dry:
        print(">>> [prune-db] (dry-run) 未删除")
        return 0
    for p, s in missing: con.execute("DELETE FROM files WHERE path=? AND step=?", (p, s))
    con.commit()
    print(f">>> [prune-db] 已删除 {len(missing)} 条")
    return len(missing)

def step_cleanup(con, before, roots=None, include_backup=False,
                 include_db_ok=False, include_db_stale=False, apply=False, report_path=None):
    """常态化旧文件清理（P2）：按 mtime 阈值删四阶段目录里的旧 PDF/zip + 孤儿备份目录。
    默认 dry-run 只分类预览；--apply 才真删。默认作用域最保守——只删 A 类孤立文件。

    分类依据 pipeline.db 引用（file_steps.src/dst ∩ files.path，normcase 比对）：
      A 孤立旧文件（无任何 DB 引用）              -> 默认删（最安全，无追溯链可断）
      B DB 引用但非活跃产物（started/skip/早期步）  -> 默认保护；仅 --include-db-stale 才删（删会断 DB 追溯链、不可恢复）
      C DB 引用且 step∈{4,5,6,7} 且 status='ok'   -> 默认保护；仅 --include-db-ok 才删（活跃产物）
    安全：只删 .pdf/.zip；路径必须在 WORK(D:\\work) 下、非符号链接、size>0；绝不碰 .db/.json/.xlsx/.html/.py。
    返回 (deleted_count, failed_list, stats_dict)。"""
    import stat as _stat
    import shutil as _shutil
    from datetime import datetime as _dtm

    def _now():
        return _dtm.now().strftime("%Y-%m-%d %H:%M:%S")

    # ---- 阈值解析（--before 必填，YYYY-MM-DD）----
    try:
        cutoff = _dtm.strptime(str(before).strip(), "%Y-%m-%d").timestamp()
    except (ValueError, TypeError):
        print(f">>> [cleanup] [!] --before 需为 YYYY-MM-DD，收到：{before!r} —— 中止")
        return 0, [], {}

    # ---- 根目录集合（别名：英文 key / 中文目录名 / 绝对路径）----
    _alias = {
        "pending": STAGE_PENDING, "cleaned": STAGE_CLEANED, "done": STAGE_DONE,
        "ads": STAGE_ADS, "packed": STAGE_PACKED,
        "文库资源": STAGE_PENDING, "去水印后": STAGE_CLEANED, "已处理": STAGE_DONE,
        "已清广告": STAGE_ADS, "打包文件": STAGE_PACKED,
    }
    if roots:
        resolved_roots = []
        for tok in str(roots).split(","):
            tok = tok.strip()
            if not tok:
                continue
            if tok in _alias:
                resolved_roots.append(_alias[tok])
            else:
                resolved_roots.append(Path(tok))
        roots_list = resolved_roots
    else:
        roots_list = [STAGE_PENDING, STAGE_CLEANED, STAGE_DONE, STAGE_ADS, STAGE_PACKED]

    # ---- 构建 DB 引用集合：protected(C) / referenced(A/B 区分) ----
    protected, referenced = set(), set()
    try:
        for _step, _status, _src, _dst in con.execute(
                "SELECT step,status,src,dst FROM file_steps"):
            for _pth in (_src, _dst):
                if _pth:
                    referenced.add(os.path.normcase(str(_pth)))
            try:
                _si = int(_step)
            except (TypeError, ValueError):
                _si = -1
            if str(_status or "").lower() == "ok" and _si in (4, 5, 6, 7) and _dst:
                protected.add(os.path.normcase(str(_dst)))
    except Exception as e:
        print(f">>> [cleanup] [!] 读 file_steps 失败（分类退化为全 A）：{e}")
    try:
        for _row in con.execute("SELECT path FROM files"):
            if _row and _row[0]:
                referenced.add(os.path.normcase(str(_row[0])))
    except Exception:
        pass

    # ---- 扫描候选（显式 os.walk 枚举，不用 shell 通配符删除）----
    _EXT = (".pdf", ".zip")
    _work_norm = os.path.normcase(str(WORK)) + os.sep
    candidates = []   # (path, size, mtime, cat)
    per_dir = {}      # root_name -> counters
    for root in roots_list:
        rkey = str(root)
        cnt = per_dir.setdefault(rkey, {"A": 0, "B": 0, "C": 0, "bytes": 0})
        if not root.is_dir():
            continue
        for dirpath, dirnames, filenames in os.walk(str(root)):
            for fn in filenames:
                if os.path.splitext(fn)[1].lower() not in _EXT:
                    continue
                fp = os.path.join(dirpath, fn)
                try:
                    st = os.lstat(fp)
                except OSError:
                    continue
                if _stat.S_ISLNK(st.st_mode) or st.st_size <= 0:
                    continue
                if st.st_mtime >= cutoff:
                    continue
                cn = os.path.normcase(fp)
                cat = "C" if cn in protected else ("B" if cn in referenced else "A")
                candidates.append((fp, st.st_size, st.st_mtime, cat))
                cnt[cat] += 1
                cnt["bytes"] += st.st_size

    # ---- 孤儿备份目录（整删）----
    backup_dirs = []   # (path, file_count, bytes)
    if include_backup:
        try:
            for p in WORK.iterdir():
                try:
                    if p.is_dir() and "_backup_" in p.name and os.lstat(str(p)).st_mtime < cutoff:
                        _fc = _bs = 0
                        for dp, _dn, fns in os.walk(str(p)):
                            for f in fns:
                                try:
                                    _bs += os.path.getsize(os.path.join(dp, f)); _fc += 1
                                except OSError:
                                    pass
                        backup_dirs.append((p, _fc, _bs))
                except OSError:
                    continue
        except OSError:
            pass

    def _deletable(cat):
        # 默认只删 A（孤立）；B 需 --include-db-stale；C 需 --include-db-ok。
        if cat == "A":
            return True
        if cat == "B":
            return include_db_stale
        return include_db_ok

    to_del = [c for c in candidates if _deletable(c[3])]
    kept = [c for c in candidates if not _deletable(c[3])]

    # ---- 汇总文本 ----
    lines = []
    lines.append(f"# 清理{'执行' if apply else '预览(dry-run)'} @ {_now()}")
    lines.append(f"# 阈值 mtime < {before} 00:00  |  include_db_stale={include_db_stale}  include_db_ok={include_db_ok}  include_backup={include_backup}")
    _nA = sum(1 for c in candidates if c[3] == "A")
    _nB = sum(1 for c in candidates if c[3] == "B")
    _nC = sum(1 for c in candidates if c[3] == "C")
    lines.append(f"# 候选 {len(candidates)} 个；将删 {len(to_del)}；保留 {len(kept)}")
    lines.append(f"  [将删] A 类孤立文件        : {_nA} 个")
    _b_note = "（已启用 --include-db-stale，将随本次删除）" if include_db_stale else "（如需删请加 --include-db-stale）"
    lines.append(f"  [{'将删' if include_db_stale else '保留'}] B 类 DB 引用(非活跃) : {_nB} 个{_b_note}")
    _c_note = "（已启用 --include-db-ok，将随本次删除）" if include_db_ok else "（活跃产物，默认保护）"
    lines.append(f"  [{'将删' if include_db_ok else '保留'}] C 类 DB 活跃产物     : {_nC} 个{_c_note}")
    if _nB and not include_db_stale:
        lines.append("  [!] B 类是 DB 引用过的历史输入，删除将断链且不可恢复（除非有外部备份）；默认不删。")
    lines.append(f"# 备份目录候选：{len(backup_dirs)} 个")
    lines.append("")
    lines.append("## 按目录分解")
    for rkey, cc in per_dir.items():
        lines.append(f"  {rkey}: A={cc['A']} B={cc['B']} C={cc['C']} 旧字节={cc['bytes']/1048576:.1f}MB")
    lines.append("")
    lines.append(f"## 备份目录（整删）")
    for p, fc, bs in backup_dirs:
        lines.append(f"  {p}  文件数={fc} 字节={bs/1048576:.1f}MB")
    if not backup_dirs:
        lines.append("  （无 / 未启用 --include-backup）")
    lines.append("")
    _cat_name = {"A": "[A]孤立", "B": "[B]非活跃引用", "C": "[C]活跃ok"}
    lines.append("## 示例（前 30 个候选）")
    for fp, sz, mt, cat in candidates[:30]:
        lines.append(f"  {_cat_name[cat]} {'删' if _deletable(cat) else '留'} {_dtm.fromtimestamp(mt):%Y-%m-%d %H:%M} {sz/1024:.0f}KB  {fp}")

    preview_text = "\n".join(lines)
    print(preview_text)

    # ---- dry-run：写预览、不删 ----
    rp = Path(report_path) if report_path else (WORK / f"_cleanup_{'report' if apply else 'preview'}_{_dtm.now():%Y%m%d_%H%M%S}.txt")
    if not apply:
        try:
            with open(str(rp), "w", encoding="utf-8") as fh:
                fh.write(preview_text + "\n")
            print(f">>> [cleanup] dry-run 预览已写：{rp}")
            print(">>> [cleanup] 未删除任何文件。确认无误后加 --apply 执行真删（默认只删 A 孤立；B 需 --include-db-stale，C 需 --include-db-ok）。")
        except OSError as e:
            print(f">>> [cleanup] [!] 预览写入失败：{e}")
        return 0, [], {"A": _nA, "B": _nB, "C": _nC, "preview": str(rp)}

    # ---- 真删（已过 dry-run 人工确认，用户显式 --apply）----
    print(">>> [cleanup] [!] 进入真删模式……")
    _before_bytes = sum(c[1] for c in to_del)
    deleted, failed = 0, []
    for fp, sz, mt, cat in to_del:
        # 删除前三重安全校验
        cn = os.path.normcase(fp)
        if not (cn == os.path.normcase(str(WORK)) or cn.startswith(_work_norm)):
            failed.append((fp, "路径不在 D:\\work 下，拒删")); continue
        if os.path.islink(fp):
            failed.append((fp, "符号链接，拒删")); continue
        if os.path.splitext(fp)[1].lower() not in _EXT:
            failed.append((fp, "非 pdf/zip，拒删")); continue
        try:
            if os.path.getsize(fp) <= 0:
                os.remove(fp); continue
        except OSError:
            pass
        try:
            os.remove(fp)
            deleted += 1
        except OSError as e:
            failed.append((fp, f"{type(e).__name__}: {e}"))
    # 备份目录整删
    rmtree_done, rmtree_failed = 0, []
    for p, fc, bs in backup_dirs:
        if os.path.islink(str(p)):
            rmtree_failed.append((str(p), "符号链接，拒删")); continue
        try:
            _shutil.rmtree(str(p))
            rmtree_done += 1
        except OSError as e:
            rmtree_failed.append((str(p), f"{type(e).__name__}: {e}"))

    # 删后立刻 prune-db，清理已失效路径记录
    _pruned = step_prune_db(con, dry=False)

    rep = [preview_text, "",
           f"## 执行结果 @ {_now()}",
           f"删除前候选待删字节：{_before_bytes/1048576:.1f}MB",
           f"文件删除成功：{deleted}  失败：{len(failed)}",
           f"备份目录删除成功：{rmtree_done}  失败：{len(rmtree_failed)}",
           f"prune-db 清理失效记录：{_pruned} 条",
           "保留清单（C 类 + 未达阈值）：见上方分类计数；本次未删文件数=" + str(len(kept)),
           (f"B 类 {_nB} 个未删（默认保护；如需删请显式加 --include-db-stale）" if (_nB and not include_db_stale) else ""),
           ""]
    if failed:
        rep.append("### 删除失败清单")
        for fp, why in failed:
            rep.append(f"  {fp}  <- {why}")
    if rmtree_failed:
        rep.append("### 备份目录删除失败")
        for fp, why in rmtree_failed:
            rep.append(f"  {fp}  <- {why}")
    rep_text = "\n".join(rep)
    try:
        with open(str(rp), "w", encoding="utf-8") as fh:
            fh.write(rep_text + "\n")
    except OSError as e:
        print(f">>> [cleanup] [!] 报告写入失败：{e}")
    print(rep_text)
    print(f">>> [cleanup] 审计报告已写：{rp}")
    return deleted, failed + rmtree_failed, {"A": _nA, "B": _nB, "C": _nC, "deleted": deleted,
                                             "freed_bytes": _before_bytes, "pruned": _pruned, "report": str(rp)}


def step_migrate_done(_con=None, dry=True, move=False, only_with_text=True):
    """把"去水印后"里已补文字层的成品归入"已处理"。
    默认复制（可重来），--move 才真删源；dry-run 只预览。"""
    import pymupdf as fitz
    src_root, dst_root = STAGE_CLEANED, STAGE_DONE
    if not src_root.is_dir():
        log_err(str(src_root), "不存在")
        return 0
    dst_root.mkdir(parents=True, exist_ok=True)
    files = sorted(src_root.glob("**/*.pdf"))
    n = 0
    with _wf_bar(len(files), "归入已处理", enabled=not dry) as b:
        for p in files:
            skip = False
            if only_with_text:
                try:
                    with fitz.open(p) as d: skip = not d[0].get_text().strip()
                except Exception: skip = True
            rel = p.relative_to(src_root)
            dst = dst_root / rel
            if skip or dst.exists():
                b.update(1, label=str(rel)[:38])
                continue
            if dry:
                print(f"  [将{'移动' if move else '复制'}] {rel}")
                n += 1
                b.update(1, label=str(rel)[:38])
                continue
            try:
                dst.parent.mkdir(parents=True, exist_ok=True)
                (shutil.move if move else shutil.copy2)(str(p), str(dst))
                log_ok(str(rel))
                ledgerio.log("migrate", "DONE", src_path=str(p), dst_path=str(dst),
                             note=("移动" if move else "复制") + f" -> {dst_root.name}")
                n += 1
            except OSError as e:
                log_err(str(rel), str(e))
                ledgerio.log("migrate", "FAIL", src_path=str(p), note=str(e)[:160])
            b.update(1, label=str(rel)[:38])
    print(f">>> [migrate-done] 共 {n} 个文件{'（预览）' if dry else '已归位'} -> {dst_root}")
    return n

# 文件处理状态总表（Excel 导出）
def step_export_excel(_con=None):
    r"""导出「文件处理状态总表.xlsx」（台账 file_events + 看板 + 实体文件三方聚合）。内容：总览（逐文件全步骤状态 +
    存放位置）/ 步骤汇总 / 问题与待办 / 产物与目录。实现见 D:\work\_status\exporter.py；失败给中文提示，绝不中断
    流水线。con 参数仅为 run_step_safe 兼容。"""
    try:
        sys.path.insert(0, r"D:\work\_status")
        import exporter
    except Exception as e:
        print(f">>> [export] 导出模块不可用（{type(e).__name__}: {e}），跳过")
        return None
    stages = [("已处理", str(STAGE_DONE)), ("已清广告", str(STAGE_ADS)),
              ("去水印后", str(STAGE_CLEANED)), ("待处理", str(STAGE_PENDING)),
              ("打包文件", str(STAGE_PACKED))]
    out = WORK / "文件处理状态总表.xlsx"
    try:
        res = exporter.export(
            db_path=str(DB), status_json=str(WORK / "_status" / "status.json"),
            retry_json=str(WORK / "_status" / "retry_queue.json"),
            stages=stages, out_xlsx=str(out))
    except PermissionError:
        print(">>> [export] 导出失败：目标 Excel 被占用（请先关闭已打开的总表再重试）")
        return None
    except Exception as e:
        print(f">>> [export] 导出失败：{type(e).__name__}: {e}")
        return None
    print(f">>> [export] 状态总表已生成：{out}")
    print(f">>> [export] 台账事件 {res['events']} 条；逻辑文件 {res['files_logical']} 个" f"（现存 {res['files_alive']}）")
    return str(out)

def show_status():
    print("=" * 66)
    print("当前状态")
    print("=" * 66)
    # ---- 当前批次（manifest + scan_pdfs 统一入口，只计本批次） ----
    bid = bc.current_batch_id() if bc is not None else None
    # 看板候选①（仅显示回退，不写 DB/不改指针/不调 set_current）：当前批次是空批次
    # （manifest 无文件，如零新增轮次）时，回退展示最近一个非空批次，避免"本批次全 0"误导。
    _orig_bid = bid
    _empty_note = None
    if bid and bc is not None and not has_targets(bid):
        _rb = _pick_resumable_batch()
        if _rb and _rb != bid:
            bid = _rb
            _empty_note = f"（当前批次 {_orig_bid} 为空，显示回退至最近非空批次 {_rb}）"
        else:
            _empty_note = f"（当前批次 {_orig_bid} 为空，当前无任何非空批次可回退）"
    print(f"  当前批次 : {bid or '(无 —— 还没跑过下载；先执行 步骤1 抓取下载)'}")
    if _empty_note:
        print(f"             {_empty_note}")
    # 当前批次若为 suspect_parse_failed，醒目提示（用户一眼能看到，不用翻日志）
    if bid and bc is not None:
        try:
            _mf0 = bc.load_manifest(bid) or {}
            _bst0 = str(_mf0.get("status") or "")
            if _bst0 == "suspect_parse_failed":
                print("  " + _BANG)
                print(f"  [!] 本批次状态：{_bst0}")
                print(f"      → 疑似解析失效：目标详情页 > 0 但 PDF = 0")
                print(f"      → 详见 logs/suspect_parse_*.json（末尾有可复制的重跑命令）")
                print("  " + _BANG)
        except Exception:
            pass
    if bid and bc is not None:
        mf = bc.load_manifest(bid) or {}
        print(f"  批次文件 : {len(mf.get('files', []))} 个（manifest）")
        for name, alias in (("待处理", "pending"), ("去水印后", "cleaned"),
                            ("已处理", "done"), ("已清广告", "ads")):
            n = len(bc.scan_pdfs(alias, bid))
            print(f"    {name:8s}本批次 {n:5d} 个 PDF")
        # 打包产物是 zip（非 PDF），不能按 scan_pdfs 计（永远 0）-> 统计本批 pack 步登记的 zip 数
        _zips = set()
        try:
            _zc = sqlite3.connect(str(DB))
            try:
                for (_z,) in _zc.execute(
                        "SELECT DISTINCT dst FROM file_steps WHERE batch_id=? AND step=7 "
                        "AND status='ok' AND dst LIKE '%.zip'", (bid,)):
                    if _z:
                        _zips.add(os.path.normcase(str(_z)))
            finally:
                _zc.close()
        except sqlite3.Error:
            pass
        print(f"    打包 zip  本批次 {len(_zips):5d} 个 zip")
        counts = bc.db_step_counts(bid)
        print("-" * 66)
        if counts:
            print("  pipeline.db · file_steps（本批次，每步状态计数）：")
            for step in sorted(counts):
                nm = f"{STEP_NAMES.get(step, step)}"
                st = "  ".join(f"{k}={v}" for k, v in sorted(counts[step].items()))
                print(f"    [{step}] {nm:10s} {st}")
        else: print("  pipeline.db · file_steps：本批次暂无状态记录")
    print("-" * 66)
    # ---- 物理目录计数（含非本批次旧库文件，仅供粗看） ----
    for name, p in STAGES.items():
        if name == "打包文件":     # 打包产物是 zip，不能按 PDF 计
            n = len(list(p.glob("**/*.zip"))) if p.is_dir() else 0
            print(f"  [物理] {'打包 zip':8s} {n:5d} 个 zip  {p}")
            continue
        n = len(list(p.glob("**/*.pdf"))) if p.is_dir() else 0
        print(f"  [物理] {name:8s} {n:5d} 个 PDF   {p}")
    if DB.exists():
        con = sqlite3.connect(str(DB))
        try:
            print("  pipeline.db · files（历史累计）：")
            for step, st, cnt in con.execute(
                    "SELECT step,status,COUNT(*) FROM files GROUP BY step,status ORDER BY step"):
                print(f"    {step:12s} {st:8s} {cnt}")
        except sqlite3.Error: pass
        con.close()
    else: print("  pipeline.db 尚未创建")
    # 最近 24h 的解析失效报告（只读扫 logs/，不改任何状态）
    try:
        _susp = _load_recent_suspect_reports(max_n=3, within_hours=24)
        if _susp:
            print(_SEP)
            print("  [!] 最近 24h 有解析失效报告（需人工排查）：")
            for _r in _susp:
                _t = datetime.datetime.fromtimestamp(_r["mtime"]).strftime("%m-%d %H:%M")
                print(f"    - [{_t}] 目标={_r['target_pages']} PDF={_r['pdf_count']}  "
                      f"{_r['url'] or _r['topic']}")
            print(f"    详见：{LOG_ROOT / 'logs' / 'suspect_parse_*.json'}")
            print(_SEP)
    except Exception:
        pass
    print("=" * 66)

# 交互式向导（内联）
def _hr(ch="=", n=66):
    print(ch * n)

def _ask(prompt, default=None, auto=None):
    """智能输入：AUTO_MODE 时直接返回默认值（auto 优先，其次 default），打印“[自动模式]”
    日志并跳过人工确认；否则正常 input()。auto 参数为自动模式专属默认值（可与交互默认
    不同，如“先干跑预览？[Y/n]”交互默认 y、自动模式必须 n 直接真跑）。"""
    if AUTO_MODE:
        ans = auto if auto is not None else default
        ans = "" if ans is None else str(ans)
        print(f"  [自动模式] {prompt.strip()} -> {ans if ans else '(回车默认)'}")
        return ans
    try: s = input(prompt).strip()
    except EOFError:
        print()
        print("[!] 标准输入已关闭（EOF）。本次使用默认值。若在交互场景，请检查终端是否正常。")
        return default if default is not None else ""
    except KeyboardInterrupt:
        print()
        return default if default is not None else ""
    return s or (default if default is not None else "")


def _selfcheck(step, msg=""):
    """[自检] 只读打印（绝不改状态/不写库）：step 为阶段标识，msg 为附加自洽信息。"""
    print(f">>> [自检] step={step}" + (f" :: {msg}" if msg else ""))


def ask_continue_or_abort(reason, options=None):
    """强制询问继续/中止（绕过 AUTO_MODE 直接读 stdin）。
    options=允许的选项（默认 abort/skip）；rerun/full 本回合统一按 abort 处理（阶段3 扩展）。
    返回 'abort'|'skip'|'rerun'|'full'。EOF/不可用 -> 打印“无法询问，保守中止”并返回 'abort'。
    绝不静默跳过、绝不误标 accepted（调用方据返回值决定 return 1 / 继续）。"""
    opts = options or ["abort", "skip"]
    _disp = {"abort": "中止(默认)", "skip": "跳过/继续", "rerun": "重跑本批", "full": "忽略并跑完"}
    print(_SEP)
    print(f">>> [询问] {reason}")
    print("      选项：" + " / ".join(f"{o}={_disp.get(o, o)}" for o in opts))
    try:
        ans = input("      请输入（" + "/".join(opts) + "，回车=abort）：").strip().lower()
    except (EOFError, KeyboardInterrupt):
        print()
        print(">>> [询问] 无法询问（stdin 不可用/中断），保守中止。")
        return "abort"
    if ans == "":
        ans = "abort"
    if ans not in opts:
        print(f">>> [询问] 未识别输入 '{ans}'，按默认 abort 处理。")
        return "abort"
    if ans in ("rerun", "full"):
        print(f">>> [询问] 选项 {ans} 本回合未实现，按 abort 处理。")
        return "abort"
    return ans

def _ask_int(prompt, default=None, lo=0, hi=None, auto=None):
    """整数输入；AUTO_MODE 时直接返回 auto（无则 default，仍无则 lo），自动夹到 [lo, hi]。
    注意：auto 优先于 default，且二者可能不同（如交互默认 [Y/n]=y 的预览询问，自动模式需显式传 auto=n
    以免无人值守下沿用交互默认值）——调用方切勿依赖 default 作为自动模式取值。"""
    if AUTO_MODE:
        try: v = int(auto if auto is not None else (default if default is not None else lo))
        except (TypeError, ValueError): v = lo
        v = max(lo, v)
        if hi is not None: v = min(v, hi)
        print(f"  [自动模式] {prompt.strip()} -> {v}")
        return v
    while True:
        s = _ask(prompt, None if default is None else str(default))
        if s == "" and default is not None: return default
        try: v = int(s)
        except ValueError:
            print("  请输入数字。")
            continue
        if v < lo or (hi is not None and v > hi):
            print(f"  请输入 {lo}~{hi if hi is not None else '∞'} 之间的数字。")
            continue
        return v

# ---- 向导流程 ----
def _flow_ctx(no_batch_msg, stage, empty_msg):
    """向导公共中段：当前批次 -> 阶段扫描 -> 空防呆；返回 (bid, pairs)。
    无批次/无文件时打印对应提示并返回 (None, None)。"""
    bid = bc.current_batch_id() if bc is not None else None
    if not bid:
        print(no_batch_msg)
        return None, None
    pairs = _scan(stage, bid) if bc is not None else []
    print(f"  批次 : {bid}（{len(pairs)} 个 PDF，manifest 限定）")
    if not pairs:
        print(empty_msg.format(bid=bid))
        return None, None
    return bid, pairs

def _ask_paper_type(cands, ask=None):
    """交互式选择试卷类型（菜单 [d] 与 auto 暂停共用）。

    cands: collect_paper_candidates 产出的候选原文列表；空列表 -> 不询问直接 (None,'skip')。
    ask:   取词函数 prompt->str|None。默认用 _ask（菜单）；auto 传入直读 stdin 的闭包，
           在 EOF/中断时返回 None 以触发 'abort'（建批前保守中止）。
    返回 (paper_type, action)：
      'skip'  回车/0/3次非法 -> 不筛选，paper_type=None；
      'set'   选中候选或自由关键词 -> paper_type 为逗号拼接字符串；
      'abort' stdin 不可用（仅 auto）-> paper_type=None，调用方 return 1。
    """
    if not cands:
        return None, "skip"
    ask = ask or (lambda p: _ask(p))
    for _ in range(3):
        _hr("-")
        print("按试卷类型筛选（可选，直接回车=不筛选抓全部）：")
        for i, c in enumerate(cands[:30], 1):
            print(f"  {i:2d}. {c}")
        if len(cands) > 30:
            print(f"  （仅显示前 30 / 共 {len(cands)} 项，可直接输入关键词）")
        raw = ask("输入序号（可逗号多选）或关键词: ")
        if raw is None:
            return None, "abort"
        raw = raw.strip()
        if raw in ("", "0", "q"):
            return None, "skip"
        toks = [t for t in re.split(r"[,，、\s]+", raw) if t]
        if toks and all(t.isdigit() for t in toks):
            picked, bad = [], False
            for t in toks:
                idx = int(t)
                if 1 <= idx <= len(cands):
                    picked.append(cands[idx - 1])
                else:
                    bad = True
            if bad:
                print("  [!] 存在越界编号，请重输。")
                continue
            out = []
            for p in picked:
                if p not in out:
                    out.append(p)
            return ",".join(out), "set"
        return raw, "set"
    print("  未选择有效条件，按不筛选继续。")
    return None, "skip"


def flow_download():
    """输入网址 -> 嗅探 -> 交互决策 -> 受控执行。绝不自动全量。"""
    url = ""
    while True:
        _hr()
        print("步骤 1 · 抓取下载")
        _hr()
        if not url:
            url = _ask("请输入需要采集的网址 (输入 q 退出): ")
            if not url or url.lower() == "q": return
        print(f"\n>>> 正在轻量嗅探: {url}\n")
        res = sniff_url(url)
        print(format_sniff(res, preview=10))
        if not res.ok:
            c = _ask("[1] 更换网址  [2] 用已知专题(1/2)下载  [0] 返回: ", "0")
            if c == "1":
                url = ""
                continue
            if c == "2": return _download_topic_menu()
            return
        c = _decide_menu(res)
        if c == "3":
            url = ""
            continue
        if c == "4":
            print("已退出。")
            raise SystemExit(0)
        if c == "1": return _download_topic_menu(None, url=url, paper_cands=res.paper_cands)
        if c == "2":
            n = _ask_int("请输入要抓取的数量（如 10）: ", lo=1)
            return _download_topic_menu(n, url=url, paper_cands=res.paper_cands)

def _decide_menu(res):
    _hr("-")
    print(f"该页面预估可识别文件数：{res.count}")
    print("请选择操作：")
    print("  [1] 全部抓取")
    print("  [2] 抓取指定数量（仅测试用，如只抓 10 个）")
    print("  [3] 更换网址")
    print("  [4] 退出程序")
    _hr("-")
    return _ask("请输入选项 [1/2/3/4]: ", "4", auto="1")

def _download_topic_menu(limit=None, url=None, paper_type=None, paper_cands=None):
    """专题选择菜单。传了 url 就走自定义专题页（--url），否则走预设 topic 1/2/all。"""
    if url:
        print(f"\n将使用当前网址作为自定义专题：\n  {url}")
        if paper_cands:
            _pt, _act = _ask_paper_type(paper_cands, ask=_ask)
            if _act == "set":
                paper_type = _pt
        if _ask("确认开始？[y/N]: ", "n", auto="y").lower() != "y":
            print("已取消。")
            return
        ensure_dirs(verbose=False)
        print(f"\n>>> 将以 --url 下载" f"{' limit=' + str(limit) if limit else '（全量）'} ...")
        rc, _res = step_discover_download(limit=limit, url=url, paper_type=paper_type)
        return _after_download(rc, "——见上方 [SUSPICIOUS] 提示")
    print("\n选择专题：")
    print("  [1] 专题1 2025-2026北京各中学高三每月月考试题及答案")
    print("  [2] 专题2 2025-2026北京各区高三上学期期末试题及答案")
    print("  [a] 全部专题")
    t = _ask("请输入 [1/2/a]: ", "1")
    topic = "all" if t.lower() == "a" else t
    if topic not in ("1", "2", "all"): topic = "1"
    print(f"\n>>> 将以 topic={topic}" f"{' limit=' + str(limit) if limit else '（全量）'} 下载")
    if _ask("确认开始？[y/N]: ", "n", auto="y").lower() != "y":
        print("已取消。")
        return
    ensure_dirs(verbose=False)
    rc, _res = step_discover_download(topic=topic, limit=limit)
    return _after_download(rc, "——见上方 [SUSPICIOUS] 提示")

def flow_watermark():
    _hr()
    print("步骤 2 · 去水印（待处理 -> 去水印后）")
    _hr()
    ensure_dirs(verbose=False)
    bid, pairs = _flow_ctx("  没有当前批次：请先跑 步骤1 抓取下载（会自动创建批次）。", "pending", "  本批次在「待处理」目录没有 PDF（可能已全部流转）。")
    if not bid: return
    print(f"  输入 : {STAGE_PENDING}")
    print(f"  输出 : {STAGE_CLEANED}")
    print(f"  说明 : 图层水印走无损删除；像素水印走栅格重建+文字保护")
    limit = _ask_int("只处理前 N 个（回车=全部）: ", default=0, lo=0, auto=0) or None
    w = _ask_int("worker 数（回车=6）: ", default=6, lo=1, hi=16, auto=6)
    # 自动模式：跳过干跑预览直接真跑（默认 N）；不强制重跑（利用断点续传）
    dry = _ask("先干跑预览？[Y/n]: ", "y", auto="n").lower() == "y"
    # 空跑防呆：断点续传会把已完成的全部跳过，用户容易误以为"没事"。
    force = _ask("强制重跑（忽略断点续传，用于端到端验证）？[y/N]: ",
                 "n", auto="n").lower() == "y"
    if force: print("    [!] 已开启 --force：将忽略状态表，对所有命中文件真跑一遍。")
    con = db()
    step_watermark(con, limit=limit, dry=dry, workers=w, force=force, batch_id=bid)
    con.close()

def flow_ocr():
    _hr()
    print("步骤 4 · OCR 补文字层（去水印后）")
    _hr()
    ensure_dirs(verbose=False)
    bid, pairs = _flow_ctx("  没有当前批次：请先跑 步骤1 下载 + 步骤2 去水印。", "cleaned", "  本批次在「去水印后」目录没有 PDF（批次 {bid}）。")
    if not bid: return
    files = [p for _f, p in pairs]
    todo = sum(1 for f in files if not has_text_layer(f))
    has_text = len(files) - todo
    print(f"  根目录 : {STAGE_CLEANED}")
    print(f"  已有文字层: {has_text} 个（自动跳过）")
    print(f"  待 OCR    : {todo} 个")
    w_def, t_def, note = ocr_worker_budget()
    if note: print(f"  [!] {note}")
    print(f"  并发策略 : {w_def} worker × {t_def} 线程（自动按物理核/内存预算；OMP/MKL/OPENBLAS 硬限制，防超订）")
    print(f"  硬件设备 : {'GPU 优先（device=gpu，worker 内自动确认 CUDA 版）' if _ocr_gpu_active() else 'CPU（paddle 需为 GPU 版才会走 GPU，见 requirements.txt）'}")
    print(f"  内存护栏 : 峰值目标 < {OCR_MEM_BUDGET_GB:.0f}GB，" f"worker 上限 {OCR_WORKERS_HARD_CAP}")
    limit = _ask_int("只处理前 N 个（回车=全部；建议先跑 8 个做基准）: ", default=0, lo=0, auto=0) or None
    w = _ask_int(f"worker 数（回车={w_def}，上限 {OCR_WORKERS_HARD_CAP}）: ",
                 default=w_def, lo=1, hi=OCR_WORKERS_HARD_CAP, auto=w_def)
    t = _ask_int(f"每 worker 线程数（回车={t_def}，建议 1~2）: ",
                 default=t_def, lo=1, hi=32, auto=t_def)
    # 空跑防呆：已有文字层的文件会被跳过，导致"成功 0"。
    force = _ask("强制重跑（忽略已有文字层，用于端到端验证）？[y/N]: ",
                 "n", auto="n").lower() == "y"
    if force: print("    [!] 已开启 --force：将忽略状态表与文字层判定，对所有命中文件真跑。")
    # C3: 显存不足/不稳定时一键回退 CPU（环境变量已支持，这里接进菜单）
    if _ocr_gpu_active():
        if _ask("显存不足/不稳定？强制改用 CPU 跑（慢但稳，不抢显存）？[y/N]: ",
                "n", auto="n").lower() == "y":
            os.environ["OCR_DEVICE"] = "cpu"
            print("    [!] 已强制 OCR_DEVICE=cpu：本次用 CPU 推理（不抢显存，但更慢）")
    print("\n提示：Ctrl+C 可安全中断，已完成的会记入 pipeline.db，下次自动续传。")
    if _ask("确认开始？[y/N]: ", "n", auto="y").lower() != "y":
        print("已取消。")
        return
    con = db()
    step_ocr(con, limit, False, None, workers=w, threads_per_worker=t, force=force, batch_id=bid)
    con.close()

def flow_rename():
    _hr()
    print("步骤 3 · 文件整理与重命名（防覆盖）")
    _hr()
    ensure_dirs(verbose=False)
    bid, pairs = _flow_ctx("  没有当前批次：请先跑 步骤1 下载 + 步骤2 去水印。", "cleaned", "  本批次在「去水印后」目录没有 PDF（批次 {bid}）。")
    if not bid: return
    files = [p for _f, p in pairs]
    fid_of = {p: _f for _f, p in pairs}
    limit = _ask_int("只扫描前 N 个（回车=全部）: ", default=0, lo=0, auto=0) or None
    use_ocr = _ask("识别不出时用 OCR 兜底？（慢，默认否）[y/N]: ", "n", auto="n").lower() == "y"
    plan = plan_renames(STAGE_CLEANED, limit=limit, use_ocr=use_ocr, files=files)
    changed = [x for x in plan if x["changed"]]
    print(f"\n  共扫描 {len(plan)} 个，建议改名 {len(changed)} 个\n")
    for item in changed[:40]:
        got = " ".join(v for v in item["fields"].values() if v)
        print(f"  {item['old']}\n    -> {item['new_stem']}.pdf\n       识别: {got}")
    if len(changed) > 40: print(f"  ... 其余 {len(changed) - 40} 个略")
    if not changed:
        print("  无需改名。")
        for item in plan: _wskip(bid, fid_of.get(item["src"]), STEP_RENAME, item["src"], False, "无需改名")
        return
    # ---- 明确三选：生成建议清单 / 应用重命名 / 跳过 ----
    choice = _ask_rename_choice()
    if choice == "skip":
        for item in plan: _wskip(bid, fid_of.get(item["src"]), STEP_RENAME, item["src"], False, "用户选择跳过重命名")
        print(">>> [rename] 已跳过重命名（pipeline.db: rename skipped）。")
        print("    提示：打包文件名可能未优化（仍使用原始文件名）。")
        return "skip"
    if choice == "list":
        print(">>> [rename] 仅生成建议清单，未执行改名（pipeline.db 不写 ok）。")
        for _x in changed:
            statusio.update_detail("rename", _x["src"], _x["old"], "SKIP", note=f"建议清单 -> {_x['new_stem']}.pdf")
        print("    如需应用：重跑本步骤选 [2]，或 python pipeline.py --step rename --rename-apply")
        return "list"
    log = str(STAGE_CLEANED / "_报告" / "rename_map.json")
    n, _ = apply_renames(plan, dry=False, log_path=log, bid=bid, fid_of=fid_of)
    for item in plan:
        if not item["changed"]: _wskip(bid, fid_of.get(item["src"]), STEP_RENAME, item["src"], False, "无需改名")
    print(f"\n>>> 完成 {n} 个。回滚映射：{log}")
    return "apply"

def flow_qc():
    _hr()
    print("步骤 5 · 质量控制（零字节 / 损坏 / 无文字层）")
    _hr()
    bid = bc.current_batch_id() if bc is not None else None
    if not bid:
        print("  没有当前批次：请先跑 步骤1~4。")
        return
    pairs = _scan("cleaned", bid) or _scan("done", bid)
    if not pairs:
        print(f"  本批次在「去水印后 / 已处理」目录没有 PDF（批次 {bid}）。")
        return
    print(f"  批次 : {bid}（{len(pairs)} 个 PDF，manifest 限定）")
    con = db()
    step_qc(con, False, None, batch_id=bid)
    con.close()

def flow_pack():
    _hr()
    print("步骤 7 · 打包 zip")
    _hr()
    bid = bc.current_batch_id() if bc is not None else None
    if not bid:
        print("  没有当前批次：请先跑 步骤1 抓取下载（会自动创建批次）。")
        return
    # 打包源默认「已清广告」；只有明确跳过隐形水印清理才允许「去水印后」（有警告）
    ads_pairs = _scan("ads", bid)
    skip_wm = False
    if ads_pairs: print(f"  打包源 : 「{STAGE_ADS.name}」（本批次 {len(ads_pairs)} 个，默认）")
    else:
        print(f"  「{STAGE_ADS.name}」没有本批次 PDF（尚未跑 步骤6 隐形水印清理）。")
        if _ask("  是否明确跳过隐形水印清理，直接打包「去水印后」？（警告：zip 可能含广告页）[y/N]: ", "n", auto="n").lower() != "y":
            print("  已取消。建议先跑 步骤6 隐形水印清理。")
            return
        skip_wm = True
    out = _ask(f"输出目录（回车={STAGE_PACKED}）: ", str(STAGE_PACKED))
    # 自动模式：跳过预览直接打包（默认 N）
    dry = _ask("先预览？[Y/n]: ", "y", auto="n").lower() == "y"
    con = db()
    step_pack(con, None, out, dry=dry, batch_id=bid, skip_wm=skip_wm)
    con.close()

def flow_ads():
    _hr()
    print("步骤 6 · 隐形水印清理（底边广告：删页 / 遮盖 / 去隐形水印字）")
    _hr()
    bid = bc.current_batch_id() if bc is not None else None
    if not bid:
        print("  没有当前批次：请先跑 步骤1 抓取下载（会自动创建批次）。")
        return
    print(f"  批次 : {bid}")
    print("  说明 : 重新扫描本批次（禁止全目录扫描）->")
    print(f"         reports/bottom_ads_{bid}.json -> 输出到「{STAGE_ADS.name}」，绝不改源文件")
    out = _ask(f"输出根目录（回车={STAGE_ADS}）: ", str(STAGE_ADS))
    # 自动模式：跳过干跑预览直接真跑（默认 N）
    dry = _ask("先干跑预览？[Y/n]: ", "y", auto="n").lower() == "y"
    rc = step_ads(dry=dry, out=out, batch_id=bid)
    if rc == 3: print(">>> [ads] 报告校验失败，立即中止：后续打包将被跳过。")
    return rc

def flow_clean():
    _hr()
    print("清理脏数据 + 临时垃圾（防炸池）")
    _hr()
    print("将：清 pipeline.db 中 step=ocr 且 status=fail/error 的记录（只删记录不删 PDF）")
    print("    清 ocr_batch_* / .ocr_tmp/* / *.bak 等临时垃圾")
    con = db()
    step_clean_ocr_dirty(con, dry=True)
    if _ask("\n以上为预览。确认执行清理？[y/N]: ", "n").lower() != "y":
        print("已取消。")
        con.close()
        return
    step_clean_ocr_dirty(con, dry=False)
    con.close()

def flow_dirs():
    while True:
        _hr()
        print("目录与迁移（四阶段结构）")
        _hr()
        for name, p in STAGES.items():
            if p.is_dir():
                n = len(list(p.glob("**/*.pdf")))
                tot = sum(f.stat().st_size for f in p.glob("**/*") if f.is_file())
                print(f"  {name:8s} [存在] {p}  PDF {n} 个，{tot/1024**3:.2f} GB")
            else: print(f"  {name:8s} [缺失] {p}")
        print("\n  [1] 创建缺失的四个阶段目录")
        print("  [2] 清理 pipeline.db 中失效路径记录")
        print("  [3] 把已 OCR 的成品归入「已处理」（复制，不删源）")
        print("  [0] 返回")
        c = _ask("请选择: ", "0")
        if c == "1": ensure_dirs()
        elif c == "2":
            con = db()
            step_prune_db(con, dry=True)
            if _ask("确认删除这些失效记录？[y/N]: ", "n").lower() == "y": step_prune_db(con, dry=False)
            con.close()
        elif c == "3":
            step_migrate_done(dry=True)
            if _ask("以上为预览。确认复制到「已处理」？[y/N]: ", "n").lower() == "y": step_migrate_done(dry=False, move=False)
        elif c == "0": return

def flow_all():
    """全流程向导：按序跑，每步都需用户确认；每步都用 run_step_safe 兜底，不闪退。"""
    _hr()
    print("全流程向导（顺序执行，每步都要你确认）")
    _hr()
    print("顺序: 网址嗅探 -> 下载 -> 去水印 -> 重命名 -> OCR -> 质控 -> 隐形水印清理 -> 打包")
    _hr()
    _dl = run_step_safe("抓取下载", flow_download)
    # rc=2（0 目标页/0 待下载 PDF）/ rc=3（疑似解析失效）立即中止，绝不继续空转。_dl 可能是 (rc,result) 或 rc 本身。
    _dl_rc = _dl[0] if isinstance(_dl, (tuple, list)) and _dl else _dl
    if _dl_rc == 2:
        show_status()
        return _handle_rc2("下载为空（rc=2）：立即中止全流程")
    if _dl_rc == 3:
        show_status()
        return _handle_rc3("：立即中止全流程", recent_n=3)
    steps = [
        ("步骤2 去水印", flow_watermark),
        ("步骤3 智能重命名", flow_rename),
        ("步骤4 OCR 补文字层", flow_ocr),
        ("步骤5 质控 QC", flow_qc),
        ("步骤6 隐形水印清理", flow_ads),
        ("步骤7 打包 zip", flow_pack),
    ]
    for name, fn in steps:
        if _ask(f"\n继续【{name}】？[y/N]: ", "n").lower() == "y":
            rc = run_step_safe(name, fn)
            # 清广告报告校验失败（rc=3）-> 立即中止并跳过打包（避免打出含广告页的 zip）
            if fn is flow_ads and rc == 3:
                print("\n>>> 清广告报告校验失败，立即中止：跳过打包。")
                break
    # 全流程收尾：zip 不存在 -> 提示"未真正完成"；批次 running 落 done/incomplete（仅确实打过包时收尾，防误改）。
    _zip_ok = bool(_final_zip_check())
    _bid = bc.current_batch_id() if bc is not None else None
    # topic 多专题时 _LAST_PACK_ZIP 仅记首个，"是否打过包"判定须并读 _LAST_PACK_ZIPS（兼容旧路径）
    if _bid and (_LAST_PACK_ZIP or _LAST_PACK_ZIPS):
        _bst = "done" if _zip_ok else "incomplete"
        if bc.set_batch_status(_bid, _bst, note="" if _zip_ok else "未生成最终 zip"):
            print(f">>> [batch] 批次 {_bid} 状态 -> {_bst}")
    print("\n>>> 全流程向导结束。")
    show_status()

def flow_auto():
    """菜单[1] 一键全自动：输入网址（回车=探测批次断点续跑）-> 无人值守跑完整条流水线
    直达打包 + 自动验收（默认开）。零交互：下载/去水印/重命名/OCR/质控/清广告/打包全走 run_auto
    的自动默认值；打包成功后批次直接 pending_accept -> accepted（operator=auto）闭环。"""
    _hr()
    print("一键全自动（无人值守）")
    _hr()
    print("流程: 下载 -> 去水印 -> 重命名 -> OCR -> 质控 -> 隐形水印清理 -> 打包")
    print("全程零询问（自动取推荐值），直达打包完成（zip 产出）；")
    if AUTO_ACCEPT:
        print("打包成功后自动验收（pending_accept -> accepted），形成闭环；"
              "如需人工验收可设环境变量 GAOKZX_AUTO_ACCEPT=0 关闭。")
    else:
        print("已完成自动验收关闭（GAOKZX_AUTO_ACCEPT=0）：打包后批次停在「待验收」，请到文件看板或用菜单 [e] 验收。")
    _hr()
    url = _ask("请输入需要采集的网址（回车=跳过下载，自动探测批次断点续跑直达打包）: ").strip()
    if url.lower() in ("q", "quit"):
        print("已取消。")
        return
    if url:
        print(f"\n>>> 本次将全自动采集：{url}")
        print("    （下载完成后立刻继续后续全部步骤，中间无需任何确认）")
        start_stage = "download"
        _resume_bid = None
    else:
        _resume_bid = _pick_resumable_batch()
        if not _resume_bid:
            print("  当前没有批次可续跑；请重新运行并输入网址。")
            return
        _cur = bc.current_batch_id() if bc is not None else None
        if _cur and _resume_bid != _cur: print(f"  提示：当前指针批次 {_cur} 无文件，" f"已回溯到最近可用批次 {_resume_bid}")
        _det, _dnote = _detect_resume_stage(_resume_bid)
        start_stage = _det or "watermark"
        _st_cn = {"pack": "打包 zip", "qc": "质控 QC", "ocr": "OCR 补层", "rename": "智能重命名", "watermark": "去水印"}
        print(f"\n>>> 跳过下载：批次 {_resume_bid} 断点续跑，" f"起点「{_st_cn.get(start_stage, start_stage)}」")
        if _dnote: print(f"    探测结果：{_dnote}")
        print("    后续阶段自动接力，直到打包完成（zip 产出）。")
    print("    [!] 全自动是长时间重任务；Ctrl+C 可安全中断（状态已落库，" "下次回车续跑自动从断点直达打包）。")
    if _ask("确认开始？[y/N]: ", "n").lower() != "y":
        print("已取消。")
        return
    ns = argparse.Namespace(
        url=(url or None), topic="all", name=None, limit=None,
        workers=6, delay=0.5, wm_workers=6,
        ocr_workers=OCR_WORKERS_DEFAULT, ocr_threads=OCR_THREADS_PER_WORKER,
        start_stage=start_stage, file_id=None, batch_id=_resume_bid,
        force=False, dry_run=False, chunk=24, force_full_run=False,
        ask_on_empty=False, min_pdf_ratio=None, ask_on_stage_failure=False,
        ask_on_resume=False, force_rerun_failed=False)
    rc = run_auto(ns)
    if rc == 0:
        # 修「假成功」：rc==0 同时涵盖"真有产出"与"零新增空结束"，须用本次是否真产出 zip 区分
        # （两个产出标志已在 run_auto 开头重置，故此值严格反映本次运行）。
        _nz = len(_LAST_PACK_ZIPS) or (1 if _LAST_PACK_ZIP else 0)
        if _nz:
            _suffix = f"（共 {_nz} 个专题 zip）" if _nz > 1 else ""
            if AUTO_ACCEPT:
                print(f"\n>>> 一键全自动已完成 [OK] 打包 zip 已产出{_suffix}（见「文库资源_打包文件」）且已自动验收（accepted）。")
            else:
                print(f"\n>>> 一键全自动已完成 [OK] 打包 zip 已产出{_suffix}（见「文库资源_打包文件」），"
                      "已标为待验收；请去「文件看板」或用菜单 [e] 批量验收。")
        else:
            print("\n>>> 本次无新增 PDF，未产生新 zip，未执行自动验收；批次保持原状态。")
    elif rc == 2:
        _handle_rc2("前置条件不满足，流程未开跑（见上方提示）")
    elif rc == 3:
        _handle_rc3("：目标详情页 > 0 但 PDF = 0，本次未下载任何文件", recent_n=3)
    else:
        print(f"\n>>> [!] 一键全自动未成功（rc={rc}）——详见上方日志。")
    show_status()

def flow_batch_accept():
    """菜单 [e] 批量验收：列出所有 pending_accept 批次 -> 选 all / 多批次 batch_id / run:前缀
    -> 二次确认 -> batch_accept(operator=manual)。只把 step=8 的 pending_accept 改为 accepted，
    不碰 fail/rejected/已 accepted/其它 step；不删 zip、不回滚。"""
    _hr()
    print("批量验收（pending_accept -> accepted）")
    _hr()
    if bc is None:
        print(">>> [accept] batchcore 不可用。"); return
    pend = bc.db_pending_batches()
    if not pend:
        print(">>> [accept] 当前没有「待验收」（pending_accept）的批次。"); return
    print(f"共 {len(pend)} 个批次有待验收文件：")
    print(f"  {'batch_id':38s} {'run_id':30s} {'待验收':>5s}  zip样例")
    for p in pend:
        zn = os.path.basename(str(p.get("zip") or "")) or "-"
        print(f"  {str(p['batch_id'])[:38]:38s} {str(p.get('run_id') or '-')[:30]:30s}"
              f" {int(p.get('pending') or 0):5d}  {zn}")
    sel = _ask("\n输入验收范围： all=全部 / 逗号分隔多个 batch_id / run:<run_id前缀或完整> > ", "").strip()
    if not sel:
        print(">>> 已取消。"); return
    pend_ids = {p["batch_id"] for p in pend}
    batch_ids = []
    low = sel.lower()
    if low == "all":
        batch_ids = [p["batch_id"] for p in pend]
    elif low.startswith("run:") or low.startswith("run="):
        key = sel[4:].strip()
        try:
            _c = sqlite3.connect(str(DB))
            try:
                rows = _c.execute("SELECT batch_id FROM batches WHERE run_id LIKE ?",
                                  (key + "%",)).fetchall()
            finally:
                _c.close()
        except sqlite3.Error as e:
            print(f">>> [accept] [!] 查 run_id 失败：{e}"); return
        batch_ids = [r[0] for r in rows if r[0] in pend_ids]
        if not batch_ids:
            print(f">>> [accept] run_id '{key}' 下没有待验收批次。"); return
    else:
        want = [x.strip() for x in sel.replace("，", ",").split(",") if x.strip()]
        for w in want:
            if w in pend_ids:
                batch_ids.append(w)
            else:
                pref = [p["batch_id"] for p in pend if str(p["batch_id"]).startswith(w)]
                if pref:
                    batch_ids += pref
                else:
                    print(f">>> [accept] [!] 未找到待验收批次：{w}")
        batch_ids = sorted(set(batch_ids))
        if not batch_ids:
            print(">>> [accept] 没有匹配到待验收批次。"); return
    cnt = sum(int(p.get("pending") or 0) for p in pend if p["batch_id"] in set(batch_ids))
    if _ask(f"\n确认验收 {len(batch_ids)} 个批次、约 {cnt} 个文件（pending_accept -> accepted）？[y/N]: ",
            "n").lower() != "y":
        print(">>> 已取消。"); return
    res = bc.batch_accept(batch_ids=batch_ids, operator="manual")
    if res.get("ok"):
        print(f">>> [accept] 批量验收完成：已验收 {res.get('accepted', 0)} 个，"
              f"跳过 {res.get('skipped', 0)} 个，失败 {len(res.get('errors') or [])}。")
    else:
        print(f">>> [accept] [!] 批量验收失败：{'; '.join(res.get('errors') or [])}")
    for _b in batch_ids:
        try:
            ledgerio.log("accept", "DONE", note=f"[menu-e] 批量验收 operator=manual batch={_b}")
        except Exception:
            pass
    show_status()

# 无交互全自动后端（CLI --auto / 看板调度 / 菜单[1] 公共入口）
STAGE_SEQ = ("download", "watermark", "rename", "ocr", "qc", "ads", "pack")
_STAGE_ALIAS = {
    "download": "download", "discover": "download", "1": "download",
    "下载": "download", "抓取": "download", "采集": "download",
    "watermark": "watermark", "wm": "watermark", "2": "watermark",
    "去水印": "watermark", "水印": "watermark",
    "rename": "rename", "3": "rename", "重命名": "rename",
    "ocr": "ocr", "4": "ocr", "补层": "ocr", "补文字层": "ocr",
    "qc": "qc", "5": "qc", "质控": "qc",
    "ads": "ads", "ad_clean": "ads", "adclean": "ads", "6": "ads",
    "清广告": "ads", "隐形水印": "ads",
    "pack": "pack", "7": "pack", "打包": "pack",
}
# 起点阶段 -> (找回目录（按优先级）, 视为"已在输入位置"的 tag 集合)；打回重跑时按此把 manifest 路径回退到起点输入。
_REVIVE_PLAN = {
    "watermark": (("pending",), ("pending",)),
    "rename": (("cleaned",), ("cleaned",)),
    "ocr": (("cleaned",), ("cleaned",)),
    "qc": (("cleaned", "done"), ("cleaned", "done")),
    "ads": (("cleaned", "done"), ("cleaned", "done")),
    "pack": (("ads",), ("ads",)),
}

def normalize_stage(s):
    """把 --start-stage 的值规范成 STAGE_SEQ 的英文阶段名；无法识别返回 None。"""
    key = str(s or "").strip().lower()
    return _STAGE_ALIAS.get(key) if key else None

def _detect_resume_stage(bid):
    """菜单[1] 回车续跑的断点探测：按 manifest 读 file_steps，从"最晚的未完成环节"续跑（全部已到
    「已清广告」-> 直接 pack；全部补完文字层 -> 从 qc；其余 -> 最落后步骤）。返回 (stage, note)；
    探测失败返回 (None, "")。"""
    try:
        if bc is None or not bid: return None, ""
        mf = bc.load_manifest(bid) or {}
        fids = [str(f.get("file_id") or "") for f in mf.get("files", [])]
        fids = [x for x in fids if x]
        if not fids: return None, ""
        con = db()
        try:
            rows = con.execute(
                "SELECT file_id, step, status FROM file_steps WHERE batch_id=?",
                (str(bid),)).fetchall()
        finally: con.close()
        st = {}
        for fid, step, status in rows: st.setdefault(str(fid), {})[int(step)] = str(status or "")
        _ok = ("ok", "skip")
        def _all_done(step):
            return all(st.get(f, {}).get(step) in _ok for f in fids)
        n = len(fids)
        if _all_done(STEP_PACK): return "pack", f"{n} 个文件均已打包过，将重新执行打包 zip"
        if _all_done(STEP_AD_CLEAN): return "pack", f"{n} 个文件已全部到「已清广告」，直接执行打包 zip"
        if _all_done(STEP_OCR): return "qc", f"{n} 个文件文字层已补齐，从「质控 QC」续跑直达打包"
        if _all_done(STEP_WATERMARK): return "ocr", f"从「OCR 补层」续跑直达打包（{n} 个文件）"
        return "watermark", f"从「去水印」续跑直达打包（{n} 个文件）"
    except Exception: return None, ""

def _pick_resumable_batch():
    """菜单[1] 回车续跑的批次选择：优先 current；current 指向空批次（如刚探测过 0 PDF 的网址）
    时按 manifest 时间倒序回溯到最近真正有文件的批次，避免空跑。失败返回 None（调用方回退提示）。"""
    if bc is None: return None
    try:
        cur = bc.current_batch_id()
        if cur and (bc.load_manifest(cur) or {}).get("files"): return cur
        for p in sorted(bc.BATCH_DIR.glob("batch_*.json"), reverse=True):
            b = p.stem[len("batch_"):]
            if b == cur: continue
            if (bc.load_manifest(b) or {}).get("files"): return b
        return cur
    except Exception: return None

def _find_in_stage(root, name):
    """在阶段根目录树里按文件名（不区分大小写）找回文件；找不到返回 None。"""
    if not root or not root.is_dir() or not name: return None
    want = str(name).lower()
    for dirpath, _dirs, names in os.walk(str(root)):
        for n in names:
            if n.lower() == want: return os.path.join(dirpath, n)
    return None

def _revive_manifest_paths(bid, fids, stage):
    """打回重跑 / 中间续跑：把 manifest 路径回退到起点阶段的输入目录。背景：文件流走后 manifest.path 指向产物
    （去水印后/已清广告/…），从更早的 --start-stage 重跑时 scan_pdfs 在源目录找不到文件而空跑；本函数按「起点
    阶段需要的输入目录」找回文件并 update_file_path。返回 (回退成功数, [无法找回的 file_id])。"""
    if bc is None: return 0, []
    plan = _REVIVE_PLAN.get(stage)
    if not plan: return 0, []
    want_dirs, skip_tags = plan
    roots = {"pending": STAGE_PENDING, "cleaned": STAGE_CLEANED, "done": STAGE_DONE, "ads": STAGE_ADS}
    mf = bc.load_manifest(bid) or {}
    revived, missing = 0, []
    for f in mf.get("files", []):
        fid = str(f.get("file_id") or "")
        if not fid: continue
        if fids is not None and fid.upper() not in fids: continue
        p = str(f.get("path") or "")
        tag, _root = _stage_root_of(p) if p else (None, None)
        if tag in skip_tags and p and os.path.isfile(p):
            continue          # 已在输入目录（或文件仍在原处）—— 无需回退
        name = os.path.basename(p) or str(f.get("name") or "")
        src_p = None
        for w in want_dirs:
            src_p = _find_in_stage(roots.get(w), name)
            if src_p: break
        if src_p:
            if bc.update_file_path(bid, fid, src_p):
                print(f"  [revive] {fid} 路径回退 -> {src_p}")
                revived += 1
        else: missing.append(fid)
    return revived, missing

def _run_auto_stage(con, stage, bid, force, wm_workers=6, ocr_workers=None, ocr_threads=None,
                    tag="", only_unpacked=False):
    """run_auto 的单阶段分发。返回 None=通过（含"无事可做"）；非 None=失败码。
    tag/only_unpacked：仅透传给 step_pack（分批流水线：zip 名带轮次后缀、每轮只打包新文件）。"""
    if stage == "watermark":
        step_watermark(con, limit=None, dry=False, workers=wm_workers, force=force, batch_id=bid)
    elif stage == "rename":
        step_rename(con, limit=None, dry=False, use_ocr=False,
                    log_path=str(STAGE_CLEANED / "_报告" / "rename_map.json"),
                    batch_id=bid)
    elif stage == "ocr":
        _w, _t, _note = ocr_worker_budget(ocr_workers or 0, ocr_threads or 0)
        if _note: print(f"  [!] {_note}")
        step_ocr(con, None, False, None, workers=_w, batch=4,
                 threads_per_worker=_t, force=force, batch_id=bid)
    elif stage == "qc": step_qc(con, False, None, batch_id=bid)
    elif stage == "ads":
        rc = step_ads(dry=False, out=None, batch_id=bid, force=force)
        if rc == 3:
            print(">>> [auto] 清广告报告校验失败（缺失/过旧/批次不匹配），中止。")
            return 3
        if rc not in (0, None): return rc or 1
    elif stage == "pack":
        # 分批流水线 + topic：中间轮不单独出包（避一专题碎成 N 个 zip）；打包延后到 run 末统一进行。
        # only_unpacked=True 仅分批轮次会传（单轮/中段续跑为 False，照常就地打包）。
        if _pack_mode() == "topic" and only_unpacked:
            print(">>> [pack] 分批流水线(topic)：本批不单独出包，延后到本次运行结束按专题统一打包。")
            return None
        zip_p = step_pack(con, None, None, dry=False, batch_id=bid,
                          tag=tag, only_unpacked=only_unpacked)
        if zip_p == "ALREADY":
            print(">>> [pack] 本批次文件均已打包过（分轮去重），跳过本轮打包。")
            return None
        if not zip_p: return 1
    return None

def verify_run_completeness(run_id, url_ident):
    """机制1（--force-full-run 才调用）："跑不完不许结束"的完整性断言。纯只读校验，绝不改库/改状态。
    核验三件事，任一不满足即 ok=False（调用方据此拒绝自动验收、保留 pending_accept、退出码 1）：
      1) run 下每个批次：本轮"下载成功(step1=ok)"的文件都到达验收步(step8 ∈ {pending_accept, accepted, skip})；
      2) zip 内 PDF 数 == 待/已验收数（batch 模式逐批；topic 模式整 run 合并为少数专题 zip，改按 run 去重 zip 内 PDF 总数 == 各批终态之和）；
      3) 该下载 URL 的续跑游标 discover_cursor == total_target（专题确已遍历完）。
    返回 (ok, reasons)。无台账后端(bc=None)时返回 (True, []) —— 无从校验则保持旧行为、绝不误伤。
    reasons 汇总全部可判定信号（如 cursor=24 < total=46），供人工介入定位。"""
    reasons = []
    if bc is None:
        return True, []
    bids = []
    if run_id:
        try:
            bids = list(bc.db_batches_of_run(run_id) or [])
        except Exception:
            bids = []
    _topic = _pack_mode() == "topic"
    import zipfile as _zip
    _sum_term = 0
    for _b in bids:
        try:
            cnt = bc.db_step_counts(_b) or {}
        except Exception:
            cnt = {}
        _dl = int((cnt.get(STEP_DOWNLOAD) or {}).get(bc.ST_OK, 0))
        s8 = cnt.get(STEP_ACCEPT) or {}
        _pa = int(s8.get(bc.ST_PENDING_ACCEPT, 0))
        _ac = int(s8.get(bc.ST_ACCEPTED, 0))
        _sk = int(s8.get(bc.ST_SKIP, 0))
        _term = _pa + _ac
        _reached = _term + _sk
        if _dl and _reached < _dl:
            reasons.append(f"batch {_b}: 下载成功 {_dl} 个，仅 {_reached} 个到达验收步(step8)——有文件卡在打包/验收前")
        _sum_term += _term
        if not _topic:
            # batch 模式：逐批 zip 内 PDF 数 == 该批待/已验收数
            zips = _batch_zips_exist(_b)
            if zips and _term:
                _zn, _zok = 0, True
                for _z in zips:
                    try:
                        with _zip.ZipFile(_z) as _zf:
                            _zn += sum(1 for n in _zf.namelist() if n.lower().endswith('.pdf'))
                    except Exception:
                        _zok = False
                        reasons.append(f"batch {_b}: 无法读取 zip 校验文件数（{_z}）")
                        break
                if _zok and _zn != _term:
                    reasons.append(f"batch {_b}: zip 内 PDF {_zn} 个 != 待/已验收 {_term} 个")
    if _topic and bids and _sum_term:
        # topic 模式：整 run 合并成少数专题 zip，逐批计数无意义 -> run 级：去重 zip 内 PDF 总数 == 各批待/已验收之和
        _rz = []
        for _b in bids:
            for _z in _batch_zips_exist(_b):
                if _z not in _rz:
                    _rz.append(_z)
        if _rz:
            _zn, _zok = 0, True
            for _z in _rz:
                try:
                    with _zip.ZipFile(_z) as _zf:
                        _zn += sum(1 for n in _zf.namelist() if n.lower().endswith('.pdf'))
                except Exception:
                    _zok = False
                    reasons.append(f"run {run_id}: 无法读取 zip 校验文件数（{_z}）")
                    break
            if _zok and _zn != _sum_term:
                reasons.append(f"run {run_id}: 专题包内 PDF 共 {_zn} 个 != 各批待/已验收之和 {_sum_term}")
    if url_ident:
        try:
            _rid, _cur, _tot, _fp = bc.db_url_cursor(url_ident)
        except Exception:
            _rid, _cur, _tot = (None, 0, None)
        if _tot is not None:
            try:
                _cur_i, _tot_i = int(_cur or 0), int(_tot)
                if _cur_i < _tot_i:
                    reasons.append(f"cursor={_cur_i} < total={_tot_i}（URL 未跑到末尾：后面可能还有目标未采集）")
                elif _cur_i > _tot_i:
                    reasons.append(f"cursor={_cur_i} > total={_tot_i}（游标超过总量，异常）")
            except (TypeError, ValueError):
                reasons.append(f"cursor/total 非法：cursor={_cur} total={_tot}")
    return (len(reasons) == 0), reasons


def run_auto(args):
    """公共后端薄封装：调用 _run_auto_impl 后统一打印 [自检] EXIT=<code>（不改内部逻辑/返回码语义）。
    并作为 KeyboardInterrupt 兵底：中断时保留 pending_accept、不误标 accepted、退出码 1。"""
    try:
        _rc = _run_auto_impl(args)
    except KeyboardInterrupt:
        print("\n[!] [auto] 已中断（保留 pending_accept，未误标 accepted；状态已落库可续跑）。")
        _selfcheck("exit", "EXIT=1 (KeyboardInterrupt)")
        return 1
    _selfcheck("exit", f"EXIT={_rc}")
    return _rc


def _run_auto_impl(args):
    """无交互全自动（CLI --auto / 看板调度 / 菜单[1] 公共后端）。从 --start-stage（默认 download）开始，按
    download -> watermark -> rename -> ocr -> qc -> ads -> pack 顺序无人值守跑完；打包成功后批次状态 ->
    pending_accept（待验收）。--file-id 只处理指定文件（看板调度）；--batch-id 显式批次。退出码：0=打包成功；1=失败；2=参数不满足。
    --chunk>0（默认 24）且从 download 起跑、非单文件调度时进入分批流水线：每批最多 N 个目标，下载完一批立即
    接力跑阶段 2~7（pack 按轮增量打包、zip 名带 _rN），下载器空结果即结束；--chunk 0 = 旧行为（全量下载后统一跑）。
    下载游标按"URL"隔离断点续跑（换 URL 不继承旧 URL 的 offset）。退出码：0=打包成功或本批无新 PDF（正常空转）；
    1=阶段失败；2=参数不满足。未捕获崩溃会写完整 traceback 到 logs/auto_crash_*.log。"""
    global AUTO_MODE, ONLY_FILE_IDS, FORCE_MISSING, _LAST_PACK_ZIP, _LAST_PACK_ZIPS
    _LAST_PACK_ZIP = None          # 修「假成功」前置：重置本次产出标志，杜绝跨运行残留导致误判"已产出/已验收"
    _LAST_PACK_ZIPS = []           # 同上：topic 多 zip 伴生列表一并重置
    _run_bids = []                 # 本次运行创建的各轮 batch_id（收尾统一标待验收）；函数开头初始化，保证各 start 路径均有定义
    install_auto_crash_hook()      # 任何逃逸到顶层的崩溃都先落盘 traceback（闪退专项：绝不"无日志闪退"）
    raw_stage = getattr(args, "start_stage", None)
    if getattr(args, "force_missing", False):
        # --auto --force-missing：每轮 reconcile 额外回滚“产物缺失/哈希不符”的 ok 行，
        # 回滚后 is_step_ok=False，后续阶段必然重处理（不会被 _skip_ok 二次拦截）。
        FORCE_MISSING = True
        print(">>> [auto] --force-missing 已启用：每轮核对后强制重跑产物缺失/内容变更文件")
    if raw_stage and normalize_stage(raw_stage) is None:
        print(f">>> [auto] 无法识别的 --start-stage：{raw_stage}")
        print(f"            可用：{', '.join(STAGE_SEQ)}（或中文别名：去水印/重命名/OCR/…）")
        return 2
    start = normalize_stage(raw_stage) or "download"
    fids = _parse_file_ids(getattr(args, "file_id", None))
    force = bool(getattr(args, "force", False))
    dry = bool(getattr(args, "dry_run", False))
    try:
        chunk = int(getattr(args, "chunk", 24))
    except (TypeError, ValueError):
        chunk = 24
    chunk = max(0, chunk)        # 0 = 关闭分批流水线（旧行为：全量下载后统一跑后续阶段）
    AUTO_MODE = True
    ONLY_FILE_IDS = fids
    _staged_multi = False        # 分批流水线标志：True 时阶段 2~7 已在下载循环内逐批跑完
    _run_id = None               # 分批块内赋值；收尾兜底（机制1 verify 用；非分批路径保持 None 由 db_run_of_batch 回推）
    _url_ident = None            # 同上：本次下载 URL 的游标标识（机制1 verify 用）
    _new_total = 0               # 本次运行累计"新下载"文件数（收尾判"本批无新 PDF"正常结束）；默认 0 兼容所有路径
    print(_SEP)
    print("一键全自动（无人值守）" + ("  [dry-run]" if dry else ""))
    print(_SEP)
    print(f"  起点阶段 : {start}（{STAGE_SEQ.index(start) + 1}/{len(STAGE_SEQ)}）")
    print(f"  文件范围 : {', '.join(sorted(fids)) if fids else '整批'}")
    print(f"  重跑模式 : {'强制重跑 --force' if force else '断点续传（已完成自动跳过）'}")
    if start == "download" and chunk > 0 and not fids and not dry:
        print(f"  分批流水线 : 每批最多 {chunk} 个（下载一批 -> 立即续跑后续阶段 -> 下一批）")
    print(_SEP)
    try:
        _dbw = (os.access(str(DB), os.W_OK) if DB.exists() else 'new')
    except Exception:
        _dbw = '?'
    try:
        _ow = os.access(str(STAGE_CLEANED), os.W_OK)
    except Exception:
        _ow = '?'
    _selfcheck("entry", f"start={start} chunk={chunk} dry={dry} force={force} "
                        f"url={getattr(args, 'url', None) or '-'} db_writable={_dbw} out_writable={_ow}")
    try:
        if bc is None:
            print(">>> [auto] batchcore 不可用（无法批次化与落库），中止。")
            return 2
        ledgerio.configure(str(DB))
        # ---- 批次解析 ----
        bid = (getattr(args, "batch_id", None) or "").strip() or None
        if bid and bc.load_manifest(bid) is None:
            print(f">>> [auto] 批次 {bid} 不存在（manifest 缺失），中止。")
            return 2
        if not bid: bid = bc.current_batch_id()
        if start != "download" and not bid:
            print(">>> [auto] 没有当前批次（也未见 --batch-id），中止。")
            print("        提示：从中间阶段续跑需要先有一次下载（步骤1）。")
            return 2
        # ---- [机制2c 默认关] 检测到上次未完成（同 URL 游标<total 或批次非终态）时询问是否续跑；abort->退出1，skip->继续原流程。----
        if getattr(args, "ask_on_resume", False):
            _ar_url = getattr(args, "url", None) or f"topic::{getattr(args, 'topic', None) or 'all'}"
            try:
                _arun, _aoff, _atot, _afp = bc.db_url_cursor(_ar_url)
            except Exception:
                _arun, _aoff, _atot = (None, 0, None)
            _aoff = _aoff or 0
            _incomplete = (_atot is not None and _aoff < int(_atot))
            if not _incomplete and bid:
                try:
                    _bst0 = (bc.load_manifest(bid) or {}).get("status")
                except Exception:
                    _bst0 = None
                _incomplete = bool(_bst0) and _bst0 not in ("accepted", "done")
            if _incomplete:
                _ch = ask_continue_or_abort(f"检测到上次未完成（游标 {_aoff}/{_atot or '?'} 或批次 {bid} 非终态）：是否续跑？", ["abort", "skip"])
                if _ch == "abort":
                    print(">>> [auto] 用户选择中止续跑（保留 pending_accept）。")
                    return 1
                print(">>> [auto] 用户选择续跑（沿用断点）。")
        # ---- file_id 校验（目标必须在本批 manifest 内）----
        if fids and start != "download":
            mf = bc.load_manifest(bid) or {}
            have = {str(f.get("file_id") or "").upper() for f in mf.get("files", [])}
            miss = sorted(fids - have)
            if miss:
                print(f">>> [auto] 批次 {bid} 中找不到：{', '.join(miss)}，中止。")
                return 2
        # ---- 阶段 2~7 执行闭包：分批模式每轮下载后立即调用；_rnd=0 表示单轮全量（旧行为） ----
        idx0 = STAGE_SEQ.index(start)
        wm_w = getattr(args, "wm_workers", None) or 6
        ocr_w = getattr(args, "ocr_workers", None)
        ocr_t = getattr(args, "ocr_threads", None)
        def _run_stages_auto(_con, _bid, _rnd=0):
            """顺序执行 STAGE_SEQ[idx0:]（跳过 download）。_rnd>0（分批第 N 轮）：pack 只打包
            未打包过的文件（only_unpacked），zip 名带 _rN 后缀。返回 None=通过；1=某阶段失败。"""
            _tag = f"_r{_rnd}" if _rnd else ""
            # 阶段入口前置校验（修问题 3）：空批次（manifest 不存在 / files=0）绝不带进 wm/rename/ocr/qc/ads/pack，
            # 直接优雅跳过（返回 None=通过），防"空 manifest 进后续阶段"导致的 None/崩溃。
            if not has_targets(_bid):
                print(f">>> [skip] 空批次（无 manifest 或 files=0），跳过全部后续阶段：{_bid}")
                return None
            # 每轮开始：对当前批次 manifest 做 DB↔磁盘双向核对（done但产物缺→回滚重处理；有产物无DB→补记）。
            # 修“已跳过 N 但产物目录 MISSING”：把一致性纠正放在所有阶段之前，后续跳过判定基于纠正后的状态。
            if not dry:
                try:
                    reconcile_round(_bid, verbose=True, force_missing=FORCE_MISSING)
                except Exception as _re:
                    print(f">>> [reconcile] 核对异常（忽略，不阻断）：{type(_re).__name__}: {_re}")
            for stage in STAGE_SEQ[idx0:]:
                if stage == "download": continue
                if dry:
                    print(f"[dry-run] 将执行阶段：{stage}")
                    continue
                print("\n" + _DASH)
                print(f"[auto] 阶段：{stage}" + (f"（分批第 {_rnd} 轮）" if _rnd else ""))
                print(_DASH)
                rc = _run_auto_stage(_con, stage, _bid, force, wm_workers=wm_w,
                                     ocr_workers=ocr_w, ocr_threads=ocr_t,
                                     tag=_tag, only_unpacked=bool(_rnd))
                if rc is not None:
                    # [机制2b 默认关] 阶段失败时询问：skip->跳过该失败阶段继续后续，abort/其它->中止返回1。
                    if getattr(args, "ask_on_stage_failure", False):
                        _ch = ask_continue_or_abort(f"阶段 {stage} 失败（rc={rc}）：中止还是跳过该阶段继续后续？", ["abort", "skip"])
                        if _ch == "skip":
                            print(f">>> [auto] 用户选择跳过失败阶段 {stage}，继续后续阶段。")
                            continue
                        print(f">>> [auto] 用户选择中止（阶段 {stage} 失败，保留 pending_accept）。")
                        return 1
                    print(f">>> [auto] 阶段 {stage} 未通过（rc={rc}），中止后续阶段。")
                    return 1
            return None
        def _dl_action(rc_dl, off, dres, bid, staged, url, topic):
            """下载返回码分流（分批/单轮共用）。返回 ("continue"|"break"|"return", code_or_None)：
            3=疑似解析失效硬中止；2=真·空（落盘空跑报告后：分批 break / 单轮 return 0）；
            False=用户跳过；其它非 0=下载未成功。"""
            if rc_dl == 3: return ("return", 3)
            if rc_dl == 2:
                _report_empty_run(url, topic, off, dres, bid)
                return ("break", None) if staged else ("return", 0)
            # rc_dl is False 特指 run_step_safe 的"用户跳过/中断"返回值（输入 s 或 KeyboardInterrupt），
            # 非下载器 rc=0；且 False==0 会被 not in (0,...) 漏判，故必须先于该判断拦截。
            if rc_dl is False:
                print(">>> [auto] 下载被用户跳过，中止。")
                return ("return", 1)
            if rc_dl not in (0, True, None):
                print(f">>> [auto] 下载未成功（rc={rc_dl}），中止。")
                return ("return", 1)
            return ("continue", None)
        # ---- 阶段1：下载（需要 --url / --topic）----
        if start == "download":
            url = getattr(args, "url", None)
            topic = (getattr(args, "topic", None) or "all")
            if not url and topic not in ("1", "2", "all"):
                print(">>> [auto] 下载起点需要 --url 或 --topic（1|2|all），中止。")
                return 2
            if dry: print(f"[dry-run] 将下载：{'url=' + url if url else 'topic=' + topic}")
            else:
                ensure_dirs(verbose=False)
                _pt = getattr(args, "paper_type", None)
                if url and not _pt:
                    # 未显式 --paper-type：进入下载前暂停一次（仅问一次，结果贯穿所有轮）。
                    # 候选取自本页表格「试卷类型」列（collect_paper_candidates）；非交互 EOF/中断 -> 保守中止 return 1
                    # （建批次之前，不动游标/manifest/file_steps）。回车/选"不筛选" -> 与原全量路径一致。
                    try:
                        _cands = getattr(sniff_url(url), "paper_cands", []) or []
                    except Exception:
                        _cands = []
                    if _cands:
                        def _ask_auto(_p):
                            try:
                                return input(_p)
                            except (EOFError, KeyboardInterrupt):
                                return None
                        _pt, _act = _ask_paper_type(_cands, ask=_ask_auto)
                        if _act == "abort":
                            print(">>> [auto] 已取消（建批次前中止，未动游标/批次）。")
                            return 1
                _dl_delay = getattr(args, "delay", 0.5)
                if _dl_delay is None: _dl_delay = 0.5
                _dl_workers = getattr(args, "workers", None) or 6
                _cap = getattr(args, "limit", None)
                if chunk > 0 and not fids:
                    # ---- 分批流水线：每批最多 chunk 个目标，每轮独立 batch_id + 独立 manifest（不跨轮累积），
                    #      全部挂在一个 run_id 下；下载完一批立即接力跑阶段 2~7。----
                    _staged_multi = True
                    # 断点续跑游标：按"下载 URL"隔离（修：旧逻辑按 run_id 维护，换 URL 仍继承旧 offset
                    #   -> 新专题页前 N 个目标被误跳过，甚至 offset>该页 total 直接 0 PDF 空跑）。
                    # 同一 URL 续跑 -> 沿用该 URL 自己的 offset 与 run 分组；换 URL -> 天然 offset=0、新 run。
                    _url_ident = (url or f"topic::{topic}")
                    _prev_run, off, _ptotal, _pfp = (bc.db_url_cursor(_url_ident)
                                                     if bc is not None else (None, 0, None, None))
                    off = off or 0
                    _run_id = _prev_run or (bc.new_batch_id("run") if bc is not None else None)
                    # 明确提示"换 URL 不继承旧游标"：旧 current 指针批次若属另一 URL，本 URL 从 0 起。
                    if off == 0 and bid:
                        try:
                            _old_url = (bc.load_manifest(bid) or {}).get("url")
                        except Exception:
                            _old_url = None
                        if _old_url and _old_url != _url_ident:
                            print(f">>> [auto] 检测到 URL 变化，重置 offset=0（旧 url={_old_url}, 新 url={_url_ident}）")
                    _new_total = 0          # 本次运行累计"新下载"文件数（判定"本批无新 PDF"正常收尾 vs 有产出）
                    print(f">>> [auto] 分批流水线启动：每批最多 {chunk} 个目标；总量"
                          f"{'不限（下载器空结果即结束）' if not _cap else '上限 ' + str(_cap)}；run_id={_run_id}")
                    if off:
                        print(f">>> [auto] 续跑同一 URL：run_id={_run_id}，从该 URL 游标 offset={off} 继续"
                              f"（本 URL 已累计推进 {off} 个目标页）。")
                    r_idx = 0
                    while r_idx < 200:
                        if _cap is not None and off >= _cap:
                            print(f">>> [auto] 已达总量上限 {_cap}，分批下载结束。")
                            break
                        per = chunk if _cap is None else min(chunk, _cap - off)
                        r_idx += 1
                        # 每轮独立 batch + 独立 manifest（仅含本轮目标，绝不跨轮累积），归入同一 run_id。
                        _bid = _new_batch_for_url(url=url, name=getattr(args, "name", None),
                                                  topic=topic, run_id=_run_id)
                        if not _bid:
                            print(">>> [auto] 无法创建本轮批次（manifest），中止。")
                            return 2
                        bid = _bid
                        _run_bids.append(_bid)
                        print("\n" + _SEP)
                        print(f"[auto] 分批第 {r_idx} 轮（批次 {_bid}）：目标第 {off + 1}~{off + per} 个")
                        print(_SEP)
                        _dl = run_step_safe(
                            f"抓取下载(第{r_idx}批)", step_discover_download, topic, per,
                            _dl_workers, _dl_delay, url=url,
                            name=getattr(args, "name", None), batch_id=_bid, offset=off,
                            expected_fp=_pfp, paper_type=_pt)
                        rc_dl = _dl[0] if isinstance(_dl, (tuple, list)) and _dl else _dl
                        _dres = _dl[1] if isinstance(_dl, (tuple, list)) and len(_dl) > 1 else None
                        # rc=3（疑似解析失效）：硬中止，绝不落入 rc==2「正常空结束」分支。
                        if rc_dl == 3:
                            _print_rc3_banner("（有目标页但 0 PDF）：中止本次运行；未走「正常空结束」；"
                                              "未推进游标、未标 accepted（详见 logs/suspect_parse_*.json）")
                            return 3
                        # ---- 下载器内部触发的 offset 重置（历史偏移越界 / 目标集合指纹变化）：
                        # 同步本地 off，按 reason 判定是否需换 run_id（指纹变化 -> 新 run，避免跨专题集混同一 run）。
                        if isinstance(_dres, dict) and _dres.get("offset_reset_reason"):
                            _rsn = _dres.get("offset_reset_reason")
                            _ob = _dres.get("offset_before") if _dres.get("offset_before") is not None else off
                            _tt_now = _dres.get("total_targets")
                            off = int(_dres.get("offset") or 0)
                            print(f">>> [auto] 检测到游标越界/变化，已自动重置："
                                  f"reason={_rsn} offset_before={_ob} total_targets={_tt_now} offset_after={off}")
                            if _rsn == "fingerprint_changed" and bc is not None:
                                try:
                                    _run_id = bc.new_batch_id("run")
                                    # 将本轮刚建的新批次重新归入新 run（下一轮亦沿用）。
                                    bc.db_batch(_bid, run_id=_run_id)
                                    print(f">>> [auto] 目标集合指纹变化 -> 启用新 run_id={_run_id}，批次 {_bid} 已归入新 run")
                                except Exception as _e:
                                    print(f">>> [auto] [!] 新 run_id 切换异常（不阻断）：{type(_e).__name__}: {_e}")
                            # 本轮后以新 fp 为参考（避免下一轮重复触发同一重置）。
                            _pfp = _dres.get("target_fp") or _pfp
                        if rc_dl == 2:
                            # 下载器空结果（0 目标页 / 0 待下载 PDF）：这是"本批无新 PDF"的正常信号，不是失败。
                            # 停止分批下载循环，交由收尾统一按"正常结束（无需打包）"处理（退出码 0）；
                            # 绝不回 2、绝不再进后续阶段、绝不写"全流程未真正完成"。
                            # 区分三种情形：
                            #   a) offset_at_end：off > 0 且 off 已达本次 total_targets（自然抵尾）——不催重跑。
                            #   b) no_targets：本 URL 真的 0 目标（首轮且游标 0）。
                            #   c) 其余（中途 rc=2）：目标已全部覆盖，同样自然结束。
                            _tt_now2 = (_dres or {}).get("total_targets") if isinstance(_dres, dict) else None
                            try:
                                _tt_i2 = int(_tt_now2) if _tt_now2 is not None else None
                            except (TypeError, ValueError):
                                _tt_i2 = None
                            if off and _tt_i2 and off >= _tt_i2:
                                _empty_reason = "offset_at_end"
                                print(f">>> [auto] 第 {r_idx} 批：游标 offset={off} 已抵达末尾"
                                      f"（total_targets={_tt_i2}），分批下载正常结束。")
                            elif r_idx == 1 and off == 0:
                                _empty_reason = "no_targets"
                                print(">>> [auto] 第 1 批即为空（该 URL 未发现可下载 PDF）。")
                            else:
                                _empty_reason = "offset_at_end" if (off and (not _tt_i2 or off >= (_tt_i2 or 0))) else "no_targets"
                                print(f">>> [auto] 第 {r_idx} 批无新目标（游标 offset={off} 已到末尾 / 目标已全部覆盖），分批下载结束。")
                            # [机制2a 默认关] 下载返回空但游标未达 total（疑似未跑完）时强制询问；abort->保留 pending_accept 退出1，skip->维持原 break。
                            if getattr(args, "ask_on_empty", False):
                                _tt = (_dres or {}).get("total_targets") if isinstance(_dres, dict) else None
                                if _tt is None and bc is not None:
                                    try: _tt = bc.db_url_cursor(_url_ident)[2]
                                    except Exception: _tt = None
                                if _tt is not None and off < int(_tt):
                                    _ch = ask_continue_or_abort(f"下载返回空但游标 {off}/{_tt} 未达 total，疑似专题未跑完：中止还是按空结束继续？", ["abort", "skip"])
                                    if _ch == "abort":
                                        print(">>> [auto] 用户选择中止，保留 pending_accept（未误标 accepted）。")
                                        return 1
                                    print(">>> [auto] 用户选择按空结果继续（走原分批结束路径）。")
                            # 真·空收尾：落盘空跑报告 + break（交给收尾按"正常结束"处理）。
                            _report_empty_run(url, topic, off, _dres, _bid,
                                              reason=_empty_reason,
                                              suggested_action=("none" if _empty_reason == "offset_at_end" else "rerun"))
                            # 同步一次游标的 total_target/target_fp（自然抵尾/目标变化后下次读到新鲜值）。
                            if bc is not None:
                                try:
                                    bc.db_url_set_cursor(_url_ident, off, run_id=_run_id,
                                                         total_target=_tt_i2,
                                                         target_fp=(_dres or {}).get("target_fp") if isinstance(_dres, dict) else None)
                                except Exception:
                                    pass
                            break
                        # rc 分流（False=用户跳过 / 非 0=未成功 / 0=继续）；rc2/3 已在上方处理。
                        _act, _code = _dl_action(rc_dl, off, _dres, _bid, True, url, topic)
                        if _act == "return": return _code
                        _total = (_dres or {}).get("total_targets") if isinstance(_dres, dict) else None
                        off += per
                        # 游标夹紧：目标总数已知时累计游标不得越过 total（防 168>165 过冲；未知则维持原推进量，行为不变）。
                        if _total is not None:
                            try: off = min(off, int(_total))
                            except (TypeError, ValueError): pass
                        # 推进并持久化游标（按 URL，含目标总数 + 目标集合 fp）：本批后续阶段接力前即落库，中途崩溃也可续跑。
                        if bc is not None:
                            try: bc.db_url_set_cursor(_url_ident, off, run_id=_run_id, total_target=_total,
                                                      target_fp=(_dres or {}).get("target_fp") if isinstance(_dres, dict) else None)
                            except Exception: pass
                        _newn = (_dres or {}).get("downloaded") if isinstance(_dres, dict) else 0
                        try: _new_total += int(_newn or 0)
                        except (TypeError, ValueError): pass
                        _round_files = (_dres or {}).get("pdf_count") if isinstance(_dres, dict) else None
                        # 三数字：本轮新增（新下载）/ 历史累积（游标覆盖目标数）/ 实际待处理（本轮 manifest 带入后续阶段的文件数）。
                        print(f">>> [auto] 第 {r_idx} 轮统计：本轮新增 {_newn} / 历史累积 {off} / "
                              f"实际待处理 {_round_files if _round_files is not None else '本批'} 个文件（批次 {_bid}）")
                        _selfcheck(f"post-download r{r_idx}",
                                   f"target_pages={(_dres or {}).get('target_pages') if isinstance(_dres, dict) else '-'} "
                                   f"pdf_count={_round_files} downloaded={_newn} "
                                   f"failed={(_dres or {}).get('failed') if isinstance(_dres, dict) else '-'} total_targets={_total}")
                        # 空跑保护：本轮新下载为 0 => 本批无新文件，跳过后续 wm/rename/ocr/qc/ads/pack（不再空扫），仅推进游标探测下一批。
                        # [阶段3 默认关] --force-rerun-failed：新下载 0 但本批有失败文件 -> 不跳过，继续重跑后续阶段。
                        _fail = (_dres or {}).get("failed") if isinstance(_dres, dict) else 0
                        _rerun_fail = bool(getattr(args, "force_rerun_failed", False)) and _fail
                        if _newn == 0 and not _rerun_fail:
                            print(f">>> [auto] 第 {r_idx} 轮新下载 0（已存在为主，本批无新文件），跳过本轮后续阶段"
                                  + (f"（失败 {_fail}）" if _fail else "") + "，继续探测下一批。")
                        else:
                            if _newn == 0 and _rerun_fail:
                                print(f">>> [auto] 第 {r_idx} 轮新下载 0，但 --force-rerun-failed 启用且本批失败 {_fail} -> 继续重跑后续阶段。")
                            _con = db()
                            try: _rc = _run_stages_auto(_con, _bid, r_idx)
                            finally: _con.close()
                            if _rc is not None:
                                print(f">>> [auto] 分批第 {r_idx} 轮后续阶段未通过，中止。")
                                return 1
                            _selfcheck("post-pack", f"batch={_bid} zip_exists={_batch_zips_exist(_bid)}")
                        # 结束条件：已知目标总数且游标已覆盖全部（offset>=total）-> 正常收尾，不再空转下一批。
                        if _total and off >= int(_total):
                            print(f">>> [auto] 游标 {off} 已覆盖全部 {_total} 个目标，分批下载结束。")
                            break
                    else:
                        print(">>> [auto] [!] 分批轮数达 200 上限，停止（防死循环）。")
                else:
                    # 旧行为（单轮全量）：一次性下载后统一跑阶段 2~7
                    _dl = run_step_safe(
                        "抓取下载", step_discover_download, topic,
                        getattr(args, "limit", None),
                        _dl_workers,
                        _dl_delay, url=url, name=getattr(args, "name", None),
                        paper_type=_pt)
                    rc_dl = _dl[0] if isinstance(_dl, (tuple, list)) and _dl else _dl
                    # rc=3（疑似解析失效）：硬中止，绝不落入 rc==2「正常空结束」分支。
                    if rc_dl == 3:
                        _print_rc3_banner("（有目标页但 0 PDF）：中止本次运行；未走「正常空结束」；"
                                          "未推进游标、未标 accepted（详见 logs/suspect_parse_*.json）")
                        return 3
                    # rc 分流与分批路径共用（False=用户跳过 / 非 0=未成功 / 2=落盘空跑报告后 return 0，
                    # 空结果为正常信号：不进后续阶段、不报"未完成"，退出码 0）。
                    _act, _code = _dl_action(rc_dl, None, None, bid, False, url, topic)
                    if _act == "return": return _code
                    bid = bc.current_batch_id() or bid
                    if not bid:
                        print(">>> [auto] 下载后未找到批次，中止。")
                        return 2
                    print(f">>> [auto] 下载完成，批次 {bid}")
        # ---- 路径回退（打回重跑：manifest 路径退回起点输入目录）----
        if start != "download" and not dry:
            _rev, _miss = _revive_manifest_paths(bid, fids, start)
            if _rev: print(f">>> [auto] 已回退 {_rev} 个文件的路径到起点阶段输入目录")
            if _miss:
                print(f">>> [auto] [!] {len(_miss)} 个文件在输入目录找不到可处理的文件：")
                for _m in _miss[:8]: print(f"        - {_m}")
                print("        中止（文件可能已被移动/删除；可改用更晚的 --start-stage）。")
                return 2
        # ---- 打回重跑标记（--force 且点名了文件）----
        if force and fids and start != "download":
            for _fid in sorted(fids):
                bc.db_step(bid, _fid, STEP_ACCEPT, bc.ST_REJECTED, error=f"打回重跑（起点 {start}）")
            print(f">>> [auto] 已复位 {len(fids)} 个文件的验收状态 -> rejected")
        # ---- 阶段 2~7：分批模式已在下载循环内逐批跑完；否则此处统一跑一轮（含 dry-run 预告） ----
        if not _staged_multi:
            con = db()
            try: _rc = _run_stages_auto(con, bid, 0)
            finally: con.close()
            if _rc is not None:
                return 1
        # ---- 分批 + topic：各轮 wm..ads 已在下载循环内逐批跑完、打包已延后；此处按 run 统一整专题打包一次 ----
        #      （dry 仅预览；_staged_multi=False 时已在上面就地打包，不重复）。----
        if _staged_multi and _pack_mode() == "topic":
            _con = db()
            try:
                _rp = step_pack(_con, None, None, dry=dry, run_id=_run_id)
            finally:
                try: _con.close()
                except Exception: pass
            if not dry and _new_total > 0 and not _rp:
                print(">>> [auto] [!] 整专题打包未产出 zip。")
                return 1
        if dry:
            print("\n>>> [auto] (dry-run) 以上为将执行的阶段，未实际改动。")
            return 0
        # ---- 有效批次集合（分批=本次各轮 / 单轮=[bid]）----
        if _staged_multi and _run_bids:
            _all_bids = list(_run_bids)
        else:
            _all_bids = [bid] if bid else []
        _selfcheck("pre-accept", f"bids={len(_all_bids)} force_full_run={getattr(args, 'force_full_run', False)} (verify 默认关->不调)")
        # ---- 机制1（--force-full-run，默认关）：跑不完不许结束。核验 游标到 total + 每批文件达验收步 + zip 数==验收数；
        #      任一不满足 -> 不标 accepted（保留 pending_accept 供人工核查）+ 打印全部 reasons + 退出码 1。
        #      前移至"零新增 return 0"之前，使 --force-full-run 也能拦"零新增但游标未达 total"的未跑完场景；
        #      默认关时本块恒不执行，零新增 return 0 与所有既有路径行为完全不变。----
        if getattr(args, "force_full_run", False):
            _vrun = _run_id if (_staged_multi and _run_id) else (bc.db_run_of_batch(bid) if (bc and bid) else None)
            _vurl = _url_ident if _staged_multi else ((bc.load_manifest(bid) or {}).get("url") if (bc and bid) else None)
            _vc_ok, _vc_reasons = verify_run_completeness(_vrun, _vurl)
            if not _vc_ok:
                print(_SEP)
                print(">>> [完整性] --force-full-run 校验未通过：判「专题未跑完」，拒绝自动验收（保留 pending_accept）。")
                for _rs in _vc_reasons:
                    print(f"    - {_rs}")
                print(">>> [完整性] 请人工确认后处理：去掉 --force-full-run 以现有产出结束，或续跑补齐剩余目标后重跑。")
                print(_SEP)
                return 1
        # ---- 空结果正常结束：本次既无"新下载"、本轮也未产出任何 zip -> 视为"本批无新 PDF"，退出码 0。
        #      绝不进后续打包/验收、绝不写"全流程未真正完成"（修问题 2；旧 pending_accept 不回滚也不新置）。----
        if _new_total == 0 and not _LAST_PACK_ZIP and not _LAST_PACK_ZIPS:
            print(">>> [auto] 本批无新 PDF，正常结束（无需打包）。")
            print(">>> [auto] 本次零新增，未产出本批次 zip，不触发自动验收（批次保持原状态）。")
            print(_SEP)
            return 0
        # ---- 收尾：最终 zip 确认 + 有效批次（manifest 确有文件者）标「待验收」；空批次只记录不推进（修问题 3/5）----
        if not _final_zip_check(): return 1
        _valid_bids = [b for b in _all_bids if b and has_targets(b)]
        for _b in _all_bids:
            if _b and not has_targets(_b):
                print(f">>> [skip] 空批次（manifest 无文件），不标待验收/不验收：{_b}")
        for _b in _valid_bids:
            try: bc.set_batch_status(_b, "pending_accept", note="全自动流程完成，等待人工验收")
            except Exception: pass
        # ---- 自动验收（AUTO_ACCEPT，默认开）：pack 成功且确认 zip 真实存在才 accept；无 zip 只告警 ----
        _auto_ok = 0
        if AUTO_ACCEPT:
            for _b in _valid_bids:
                if not _batch_zips_exist(_b):
                    # 没有 zip 就不验收（防对未打包/空批次误置 accepted）；保留 pending_accept 供人工核查
                    print(f">>> [auto] [!] 批次 {_b} 未见已生成的 zip，跳过自动验收（保留 pending_accept）")
                    continue
                try:
                    _ar = bc.batch_accept(batch_ids=[_b], operator="auto")
                except Exception as _ae:
                    _ar = {"ok": False, "accepted": 0,
                           "errors": [f"{type(_ae).__name__}: {_ae}"]}
                if _ar.get("ok"):
                    _auto_ok += int(_ar.get("accepted") or 0)
                    print(f">>> [auto] 自动验收完成：{_ar.get('accepted', 0)} 个文件 -> accepted，批次 {_b}"
                          + (f"（跳过 {_ar.get('skipped')}）" if _ar.get("skipped") else ""))
                else:
                    # 自动验收失败：绝不删 zip、绝不回滚打包；保留 pending_accept 供手动批量验收
                    print(f">>> [auto] [!] 批次 {_b} 自动验收失败（保留 pending_accept，可在看板或菜单 [e] 手动批量验收）："
                          f"{'; '.join(_ar.get('errors') or [])}")
        print(_SEP)
        if AUTO_ACCEPT and _auto_ok:
            print(f">>> 全流程跑完并已自动验收：{_auto_ok} 个文件 -> accepted（zip 见「文库资源_打包文件」）")
        elif AUTO_ACCEPT:
            print(">>> 全流程跑完（本轮无待验收文件可自动验收；如需人工归档请到「文件看板」）")
        else:
            print(">>> 全流程跑完，请去「文件看板」进行人工验收")
        if _staged_multi:
            print(f">>> 本次运行 run_id={_run_id} 共 {len(_valid_bids)} 个有效轮次批次"
                  + (f"已自动验收（accepted）" if AUTO_ACCEPT and _auto_ok else "已更新为「待验收」（pending_accept）"))
        else:
            print(f">>> 批次 {bid} " + (f"已自动验收（accepted）" if AUTO_ACCEPT and _auto_ok else "状态「待验收」（pending_accept）"))
        print(_SEP)
        return 0
    except KeyboardInterrupt:
        print("\n[!] [auto] 已中断（状态已落库，可再次运行续传）。")
        return 1
    except SystemExit as e:
        print(f"\n[!] [auto] 步骤触发退出（code={getattr(e, 'code', '?')}）。")
        return 1
    except Exception as e:
        print(f"\n[ERR] [auto] 执行失败：{type(e).__name__}: {e}")
        print(fatal_hint("自动流程", e))
        import traceback
        # 全量 traceback 落盘（闪退专项排查：每次崩溃都要留完整堆栈供事后定位，不凭猜）
        _logp = _write_auto_crash_log(exc_obj=e, tag="run_auto")
        if not _logp:
            # 落盘失败兜底：至少控制台输出完整 traceback（不只最后 8 行）
            for line in traceback.format_exc().strip().splitlines(): print("      " + line)
        else:
            for line in traceback.format_exc().strip().splitlines()[-8:]: print("      " + line)
        return 1
    finally: AUTO_MODE = False

MENU = """
──────────────────────────────────────────────────────────────
  文库数据采集 · 统一主控（单脚本全流程）
──────────────────────────────────────────────────────────────
  [1] 一键全自动       （输入网址 -> 无人值守跑完整条流水线 -> 直达打包 + 自动验收，完成后批次状态 accepted（GAOKZX_AUTO_ACCEPT=0 可关））
  [2] 全流程向导       （同上顺序，但每步都要你确认；适合盯质量）
  [3] 步骤1 抓取下载   （输入网址 -> 嗅探 -> 选择数量）
  [4] 步骤2 去水印     （待处理 -> 去水印后）
  [5] 步骤3 智能重命名 （默认只出建议清单，重名自动加 _1/_2 尾缀）
  [6] 步骤4 OCR 补层   （默认只补「去水印后」，4 worker × 6 线程）
  [7] 步骤5 质控 QC    （零字节 / 损坏 / 无文字层）
  [8] 步骤6 隐形水印清理（重扫描新报告 -> 已清广告，打包前执行）
  [9] 步骤7 打包 zip   （已清广告 -> 打包文件）
  [a] 清理脏数据       （清 fail/error 记录 + 临时垃圾）
  [b] 目录与迁移       （四阶段结构 / 建目录 / 归位）
  [c] 导出状态总表     （文件处理状态总表.xlsx：逐文件全步骤状态+存放位置）
  [e] 批量验收         （pending_accept -> accepted：支持 all / 多批次 / run_id 一键验收）
  [v] 查看改名冲突     （rename_conflicts.xlsx：科目冲突/拒绝改名逐条审核 + 处理入口提示）
  [s] 查看状态
  [d] 环境自检/安装依赖（缺包会在这里一键装好）
  [0] 退出
──────────────────────────────────────────────────────────────
"""

def run_wizard():
    """交互式主菜单。每个步骤都包在 run_step_safe 里：
    崩溃时给中文提示 + 让用户选「重试 / 跳过 / 返回主菜单」，绝不闪退。"""
    install_sigint("菜单")
    # 进菜单前先快速自检一次：缺核心包就直接提示并给一键修复入口
    missing, _ = _missing_deps()
    core_missing = [p for m, p, w, r in missing if p in _CORE_NEEDED]
    if core_missing:
        print("\n" + "!" * 66)
        print(f"[!] 检测到核心依赖缺失：{', '.join(core_missing)}")
        print(f"[!] 这会导致后续步骤崩溃。建议先执行菜单 [d] 自动安装。")
        print("!" * 66 + "\n")
    # 菜单项 -> (中文名, 执行函数)；[e] 为旧键位兼容（等同 [c] 导出）
    actions = {
        "1": ("一键全自动", flow_auto),
        "2": ("全流程向导", flow_all),
        "3": ("步骤1 抓取下载", flow_download),
        "4": ("步骤2 去水印", flow_watermark),
        "5": ("步骤3 智能重命名", flow_rename),
        "6": ("步骤4 OCR 补层", flow_ocr),
        "7": ("步骤5 质控 QC", flow_qc),
        "8": ("步骤6 隐形水印清理", flow_ads),
        "9": ("步骤7 打包 zip", flow_pack),
        "a": ("清理脏数据", flow_clean),
        "b": ("目录与迁移", flow_dirs),
        "c": ("导出状态总表", step_export_excel),
        "e": ("批量验收", flow_batch_accept),
    }
    _eof_n = 0        # 连续 EOF 计数（stdin 关闭时防死循环；读到输入即清零）
    while True:
        print(MENU)
        # 修「空回车静默退出」：不再走 _ask(默认 "0")——直读 input：空回车/Ctrl+C 重显示菜单，
        # 仅显式输入 0 才退出；连续 2 次 EOF（stdin 不可用，如 <nul 重定向）才提示并退出。
        try:
            c = input("请选择操作: ").strip().lower()
            _eof_n = 0
        except KeyboardInterrupt:
            print()
            continue                       # Ctrl+C 不再退出，重显示菜单
        except EOFError:
            _eof_n += 1
            print()
            if _eof_n >= 2:
                print(">>> [!] 标准输入不可用（连续 EOF）。请用 run.cmd 启动，或确保在交互式终端运行。")
                return
            print("[!] 未读到输入（EOF），请重新输入（再遇 EOF 将退出）。")
            continue
        if not c:
            continue                        # 空回车 -> 重显示菜单，不退出
        try:
            if c in actions:
                act_name, fn = actions[c]
                run_step_safe(act_name, fn)      # 崩溃 -> 中文提示 + 重试/跳过/返回
            elif c == "s": show_status()
            elif c == "v": show_rename_conflicts()
            elif c == "d":
                run_step_safe("环境自检", lambda: step_check_env(install=False))
                if _ask("  是否立即自动安装缺失依赖？[y/N]: ", "n").lower() == "y":
                    run_step_safe("依赖安装", lambda: step_check_env(install=True, yes=True))
            elif c == "0":
                print("再见。")
                return
            else: print("无效选项，请重新输入。")
        except _BackToMenu: print("\n[..] 已返回主菜单。")
        except SystemExit: raise
        except KeyboardInterrupt: print("\n[!] 已中断，返回主菜单。")
        except Exception as e:
            # 兜底：连 run_step_safe 都没兜住的意外，也绝不闪退
            print(f"\n[ERR] 操作出错: {type(e).__name__}: {e}")
            print(fatal_hint("菜单", e))

# ------------------------- 入口 -------------------------
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
    ap.add_argument("--min-pdf-ratio", type=float, default=None, dest="min_pdf_ratio",
                    help="[机制2a] 配合 --ask-on-empty：记录本轮 pdf/目标低比例阈值（0~1）以触发询问。默认 None=不启用。")
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
    global ONLY_FILE_IDS
    ONLY_FILE_IDS = _parse_file_ids(getattr(args, "file_id", None))
    # check-env 不需要 DB，先处理
    if "check-env" in steps:
        step_check_env(install=args.install, yes=True)
        return
    # ---- 子目录过滤（保护旧库）：--url 跑新专题时默认只处理该专题目录，避免 --force 重跑整库。
    global ONLY_SUBDIR
    if args.only_subdir == "ALL": ONLY_SUBDIR = None
    elif args.only_subdir: ONLY_SUBDIR = args.only_subdir
    elif args.url:
        try:
            sys.path.insert(0, str(ROOT))
            from gaokzx_download import topic_from_url as _tfu
            ONLY_SUBDIR = _tfu(args.url, args.name)["name"]
        except Exception: ONLY_SUBDIR = None
        if ONLY_SUBDIR:
            print(f">>> [scope] 检测到 --url，自动把处理范围限定在子目录：" f"{ONLY_SUBDIR}")
            print(f">>> [scope] 旧库其它文件不会被触碰。如需处理整库，加 --only-subdir ALL")
    # 压测/小批量（--src/--hardest）写独立 DB，防污染生产（必须 global，否则 db() 仍写生产库——历史 bug）。
    global DB
    if bc is not None and getattr(bc, "TEST_ROOT", ""):
        pass  # GAOKZX_TEST_ROOT 隔离模式：DB 已在模块加载时指向隔离目录，不回写生产路径
    else:
        DB = ROOT / "pipeline_test.db" if args.src or args.hardest else ROOT / "pipeline.db"
    ledgerio.configure(str(DB))      # 台账跟随同一库：压测不污染生产台账
    con = db()
    # ---- --force-missing（单步 CLI）：仅重跑 DB=done 但磁盘缺失的文件（先核对回滚，不开 --force 全量）----
    # --auto 路径已在每轮 _run_stages_auto 前调 reconcile，无需此处；且 --force-missing 不会置 force（不盲重跑）。
    global FORCE_MISSING
    if getattr(args, "force_missing", False):
        FORCE_MISSING = True
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
                                paper_type=getattr(args, "paper_type", None))
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
        if _bid and (_LAST_PACK_ZIP or _LAST_PACK_ZIPS):
            _bst = "done" if _zip_ok else "incomplete"
            if bc.set_batch_status(_bid, _bst, note="" if _zip_ok else "未生成最终 zip"):
                print(f">>> [batch] 批次 {_bid} 状态 -> {_bst}")
    print(">>> 流水线本轮机结束")

if __name__ == "__main__": sys.exit(main())
