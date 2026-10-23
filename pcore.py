#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""gaokzx 流水线 · 公共核心（pcore）—— 2026-10-21 自 pipeline.py 拆出。

职责：初始化段（UTF-8/statusio/ledgerio/test-root 预扫描/batchcore import/STAGE 常量）、
日志与进度条（log_*/_Bar）、DB 写入标记（mark）、文件枚举与重试队列（iter_pdfs/_retry_*）、
单实例锁（_acquire_lock 族）、步骤跳过判定（_skip_ok/_file_present/_manifest_curpath）、
通用交互确认（_ask_apply/_merge_retry）。
被 pipeline.py / prename.py / pwm.py 共享；不反向依赖任何新模块。
"""
from pathlib import Path
import datetime
import glob
import json
import os
import re
import signal
import sqlite3
import subprocess
import sys
import time

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
def mark(con, path, step, status):
    con.execute(
        "INSERT OR REPLACE INTO files(path,step,status,ts) VALUES(?,?,?,?)",
        (path, step, status, datetime.datetime.now().isoformat(timespec="seconds")),
    )
    con.commit()
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

# ==================================================================
# 以下区块自 pipeline.py 下沉（2026-10-21 批次2-4 合并执行）
# ==================================================================

# ------------------------- 解释器守卫辅助 -------------------------
_PEP668_HINT = "（若提示 externally-managed，说明该解释器是裸运行时，请用 venv 里的 python）"

def _norm_py(p):
    try: return os.path.normcase(os.path.realpath(str(p)))
    except OSError: return os.path.normcase(str(p))

# ------------------------- 状态表 / 阶段目录 -------------------------
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

# ------------------------- 依赖自检 / 自动安装（step_check_env 族） -------------------------
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
    # 内存/GPU 探测函数归 pocr（OCR 护栏归一处）；延迟 import 避免 pcore->pocr 模块级循环。
    try:
        from pocr import safe_workers_for_memory as _swfm, _ocr_gpu_present as _ogp
    except Exception:
        _swfm = _ogp = None
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
        if mem < 20 and _swfm:
            print(f"           [!] 内存 {mem:.1f}GB → OCR 建议 ≤ " f"{_swfm(99)[0]} worker")
    except ImportError: psutil = None
    try:
        import paddle
        _cuda = bool(paddle.is_compiled_with_cuda())
        _dcnt = int(paddle.device.cuda.device_count()) if _cuda else 0
        _nvsmi = _ogp() if _ogp else False
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

# ------------------------- 崩溃/异常中文提示 -------------------------
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

# ------------------------- 通用交互（_ask 族 / 横线） -------------------------
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

# ------------------------- 空批次前置校验 -------------------------
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

# ------------------------- 步骤安全包装 / 主菜单回退信号 -------------------------
# （2026-10-22 自 pflow.py 下沉：run_step_safe 被 pflow/pflowauto/pipeline 三方共用，
#   归入 pcore 消除 pflow<->pflowauto 循环依赖）
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
            if pcore.AUTO_MODE:
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
