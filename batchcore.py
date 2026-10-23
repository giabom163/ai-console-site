# -*- coding: utf-8 -*-
r"""批次核心模块（D:\work\gaokzx\batchcore.py）
=================================================
全流程「批次化」的唯一真相源，被 pipeline.py / gaokzx_download.py /
scan_bottom_ads.py / clean_bottom_ads.py 共同引用。纯标准库实现。

三件事：
  1) 统一步骤编号（STEP_DOWNLOAD=1 ... STEP_PACK=7）—— 菜单 / 向导 / 日志 /
     pipeline.db 全部引用这里的常量，不再各写各的字符串。
  2) 批次（batch_id）与 manifest：
        D:\work\文库资源\_批次\batch_<batch_id>.json
     每次任务（一轮下载）创建一个批次；去水印 / 重命名 / OCR / QC / 清广告 /
     打包只读取本批次 manifest 里的文件清单，禁止全目录扫描。
     current.json 记录"当前批次"，供单步菜单模式接力使用。
  3) pipeline.db 状态落库（追加式 file_events 之外的**状态表**）：
        batches     —— 批次账头（batch_id / created_at / url / status）
        file_steps  —— 每文件每步状态：batch_id / file_id / step / status /
                       src / dst / error / updated_at（每个状态变化写一次）

状态枚举（与用户口径一致）：
    started —— 步骤开始
    ok      —— 正式执行成功（dry-run 预览不写 ok）
    fail    —— 失败（error 列记录原因）
    skip    —— 跳过（含断点续传跳过、重命名"无需改名"等）

设计约束：
  * 纯标准库，任何 3.8+ 解释器可 import；
  * 对外函数永不抛异常——失败静默（WM_STATUS_DEBUG=1 时打印），
    绝不影响主流程；返回值形式表达"没做成"（None / [] / False）；
  * 线程安全：内部 RLock + busy_timeout；
  * manifest 写盘用 tmp + os.replace 原子替换。
"""
import json
import os
import re
import sqlite3
import threading
from datetime import datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# 统一步骤编号（菜单 / 向导 / 日志 / pipeline.db 全部引用）
# ---------------------------------------------------------------------------
STEP_DOWNLOAD = 1
STEP_WATERMARK = 2
STEP_RENAME = 3
STEP_OCR = 4
STEP_QC = 5
STEP_AD_CLEAN = 6
STEP_PACK = 7
# 第 8 个"步骤"不是流水线处理步骤，而是验收状态位（文件级）：
# 打包成功 -> pending_accept（待验收）；看板[验收通过] -> accepted（已归档）；
# [打回重跑] -> rejected；看板调度任务非 0 退出 -> exec_fail（执行失败）。
STEP_ACCEPT = 8

STEP_NAMES = {
    STEP_DOWNLOAD: "download",
    STEP_WATERMARK: "wm",
    STEP_RENAME: "rename",
    STEP_OCR: "ocr",
    STEP_QC: "qc",
    STEP_AD_CLEAN: "ad_clean",
    STEP_PACK: "pack",
    STEP_ACCEPT: "accept",
}
# 状态枚举（1~7 步）
ST_STARTED = "started"
ST_OK = "ok"
ST_FAIL = "fail"
ST_SKIP = "skip"
# 验收状态枚举（step=STEP_ACCEPT 专用）
ST_PENDING_ACCEPT = "pending_accept"   # 待验收（打包成功后写入）
ST_ACCEPTED = "accepted"               # 已验收（文件已归档到「文库资源_completed」）
ST_REJECTED = "rejected"               # 已打回重跑
ST_EXEC_FAIL = "exec_fail"             # 看板调度的自动任务失败

# ---------------------------------------------------------------------------
# 目录与常量
# ---------------------------------------------------------------------------
WORK = Path(r"D:\work")
BATCH_DIR = WORK / "文库资源" / "_批次"
REPORTS_DIR = Path(r"D:\work\gaokzx\reports")
DEFAULT_DB = r"D:\work\gaokzx\pipeline.db"

# 生产路径固定快照（测试隔离断言用，不随 TEST_ROOT 变）
PROD_BATCH_DIR = str(BATCH_DIR)
PROD_DB = DEFAULT_DB
# 测试隔离（修「测试污染生产 current.json」根因）：设 GAOKZX_TEST_ROOT 后，
# _批次/manifest/pipeline.db 全部落到该独立目录，与生产物理隔离。
TEST_ROOT = (os.environ.get("GAOKZX_TEST_ROOT") or "").strip()
if TEST_ROOT:
    _TW = Path(TEST_ROOT)
    _TW.mkdir(parents=True, exist_ok=True)
    WORK = _TW
    BATCH_DIR = _TW / "_批次"
    BATCH_DIR.mkdir(parents=True, exist_ok=True)
    DEFAULT_DB = str(_TW / "pipeline.db")


# 阶段目录（与 pipeline.py STAGES 保持一致；scan_pdfs 的 stage 参数
# 既接受目录路径（str/Path），也接受这里的别名）
# completed = 验收归档目录（看板[验收通过]后文件移入，终态，不再参与处理）。
STAGE_COMPLETED = WORK / "文库资源_completed"
STAGE_ALIAS = {
    "pending": WORK / "文库资源",
    "cleaned": WORK / "文库资源_去水印后",
    "done": WORK / "文库资源_已处理",
    "ads": WORK / "文库资源_已清广告",
    "packed": WORK / "文库资源_打包文件",
    "completed": STAGE_COMPLETED,
}

# 生产流程必须跳过的路径片段（测试文件 / 内部目录）
_TEST_SEGMENTS = ("_测试", "验收测试")

_lock = threading.RLock()
_db_path = DEFAULT_DB
_DEBUG = os.environ.get("WM_STATUS_DEBUG") == "1"


def _dbg(what, e):
    if _DEBUG:
        try:
            print(f"[batchcore] {what}: {type(e).__name__}: {e}", flush=True)
        except Exception:
            pass


def configure(db_path):
    """切换状态库（进程内全局）。"""
    global _db_path
    with _lock:
        _db_path = os.path.abspath(str(db_path or DEFAULT_DB))


def _now_iso():
    return datetime.now().astimezone().isoformat(timespec="seconds")


def _np(p):
    """规范化路径字符串（大小写归一 + 反斜杠），用于比较。"""
    return os.path.normcase(os.path.normpath(os.path.abspath(str(p))))


def norm_key(p):
    """路径规范化键（与内部比较逻辑一致），供外部做 路径 -> 记录 映射。"""
    return _np(p)


def is_test_path(path):
    """测试/内部路径判定：任一路径段命中 _TEST_SEGMENTS 即视为测试文件。"""
    try:
        parts = Path(str(path)).parts
    except Exception:
        return False
    for seg in parts:
        for t in _TEST_SEGMENTS:
            if t in seg:
                return True
    return False


# ---------------------------------------------------------------------------
# 批次与 manifest
# ---------------------------------------------------------------------------
_BATCH_ID_SEQ = 0


def new_batch_id(key="manual"):
    """生成 batch_id：时间戳(秒+毫秒) + 进程内自增序号 + 来源尾号。
    如 20260918_161530_159755。[重] 分批流水线一轮建一个 batch，同一秒内也会多次调用，
    必须用毫秒+进程内递增序号保证唯一，否则同秒两轮会撞 batch_id、互相覆盖 manifest/DB。"""
    global _BATCH_ID_SEQ
    _BATCH_ID_SEQ += 1
    now = datetime.now()
    base = now.strftime("%Y%m%d_%H%M%S")
    tail = f"{now.microsecond // 1000:03d}{_BATCH_ID_SEQ % 1000:03d}"
    key = str(key or "manual").strip() or "manual"
    return f"{base}_{tail}_{key}"


def manifest_path(batch_id):
    return BATCH_DIR / f"batch_{batch_id}.json"


def _write_json_atomic(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(path) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=1)
    os.replace(tmp, str(path))


def new_batch(url="", name="", key="", run_id=None):
    """创建批次：写 manifest 骨架 + batches 表 + current 指针。失败返回 None。
    run_id：所属“一键全自动”运行分组键（与 batch_id 拆分）；单步/独立下载无 run
    上下文时，自动生成 single_<batch_id> 自成单批 run，保证导出/台账 run_id 列不断档。"""
    try:
        bid = new_batch_id(key)
        if not run_id:
            run_id = "single_" + bid
        mf = {
            "batch_id": bid,
            "run_id": run_id or "",
            "created_at": _now_iso(),
            "url": url or "",
            "name": name or "",
            "status": "running",
            "download": {},
            "files": [],
            "updated_at": _now_iso(),
        }
        with _lock:
            _write_json_atomic(manifest_path(bid), mf)
            _write_json_atomic(BATCH_DIR / "current.json", {"batch_id": bid, "run_id": run_id or ""})
        db_batch(bid, url=url, name=name, status="running", run_id=run_id)
        return mf
    except Exception as e:
        _dbg("new_batch", e)
        return None


def load_manifest(batch_id):
    """读 manifest。失败返回 None。"""
    try:
        with open(str(manifest_path(batch_id)), "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as e:
        _dbg("load_manifest", e)
        return None


def save_manifest(mf):
    """原子回写 manifest。"""
    try:
        mf["updated_at"] = _now_iso()
        with _lock:
            _write_json_atomic(manifest_path(mf["batch_id"]), mf)
        return True
    except Exception as e:
        _dbg("save_manifest", e)
        return False


def current_batch_id():
    """读 current.json 指针。没有返回 None。"""
    try:
        with open(str(BATCH_DIR / "current.json"), "r", encoding="utf-8") as fh:
            return json.load(fh).get("batch_id") or None
    except Exception:
        return None


def set_current(batch_id):
    try:
        with _lock:
            _write_json_atomic(BATCH_DIR / "current.json", {"batch_id": batch_id})
        return True
    except Exception as e:
        _dbg("set_current", e)
        return False


def register_files(batch_id, entries):
    """把文件登记进 manifest（下载完成后调用）。

    entries: [{"path": 绝对路径, "name": 展示名(可选), "status": 下载状态(可选)}]
    新文件分配 file_id（F0001 递增；已存在的按规范路径匹配复用）。
    返回本批 files 列表（含 file_id），失败返回 []。
    """
    mf = load_manifest(batch_id)
    if not mf:
        return []
    try:
        by_path = {_np(f["path"]): f for f in mf.get("files", []) if f.get("path")}
        max_n = 0
        for f in mf.get("files", []):
            fid = str(f.get("file_id") or "")
            if fid.startswith("F") and fid[1:].isdigit():
                max_n = max(max_n, int(fid[1:]))
        for e in entries:
            p = e.get("path")
            if not p:
                continue
            k = _np(p)
            if k in by_path:
                # 已登记：更新路径/名称/下载状态
                rec = by_path[k]
                rec["path"] = os.path.abspath(str(p))
                if e.get("name"):
                    rec["name"] = e["name"]
                if e.get("status"):
                    rec["download_status"] = e["status"]
                continue
            max_n += 1
            rec = {
                "file_id": f"F{max_n:04d}",
                "name": e.get("name") or os.path.basename(str(p)),
                "path": os.path.abspath(str(p)),
                "download_status": e.get("status") or "",
            }
            mf.setdefault("files", []).append(rec)
            by_path[k] = rec
        save_manifest(mf)
        return mf.get("files", [])
    except Exception as e:
        _dbg("register_files", e)
        return []


def set_download_summary(batch_id, summary):
    """把下载统计写入 manifest.download。"""
    try:
        mf = load_manifest(batch_id)
        if not mf:
            return False
        mf["download"] = dict(summary or {})
        return save_manifest(mf)
    except Exception as e:
        _dbg("set_download_summary", e)
        return False


def set_batch_status(batch_id, status, note=""):
    """更新 manifest + batches 表状态。"""
    try:
        mf = load_manifest(batch_id)
        if mf:
            mf["status"] = status
            save_manifest(mf)
        db_batch(batch_id, status=status, note=note)
        return True
    except Exception as e:
        _dbg("set_batch_status", e)
        return False


def update_file_path(batch_id, file_id, new_path):
    """文件在流程中换了位置（去水印/清广告/重命名后）时更新 manifest。"""
    try:
        mf = load_manifest(batch_id)
        if not mf:
            return False
        for f in mf.get("files", []):
            if f.get("file_id") == file_id:
                f["path"] = os.path.abspath(str(new_path))
                save_manifest(mf)
                return True
        return False
    except Exception as e:
        _dbg("update_file_path", e)
        return False


def find_file_id(batch_id, path):
    """按当前路径反查 file_id（找不到返回 None）。"""
    try:
        mf = load_manifest(batch_id)
        if not mf:
            return None
        k = _np(path)
        for f in mf.get("files", []):
            if f.get("path") and _np(f["path"]) == k:
                return f.get("file_id")
        return None
    except Exception as e:
        _dbg("find_file_id", e)
        return None


# 阶段名 -> 成品路径 索引缓存（进程内一次构建，供 resolve_file_path 快速按名查实际成品）。
_STAGE_NAME_IDX = {}
# resolve 时按此阶段优先级查找「真实存在的成品」：越靠流程末端越权威。
_RESOLVE_STAGE_PRIORITY = ("ads", "done", "cleaned", "pending")


def _stage_name_index(root):
    """递归扫某阶段目录下所有 .pdf，建 {小写文件名: [绝对路径...]} 索引（进程内缓存）。"""
    k = _np(root)
    idx = _STAGE_NAME_IDX.get(k)
    if idx is not None:
        return idx
    idx = {}
    try:
        for dirpath, _dn, fns in os.walk(str(root)):
            for nm in fns:
                if nm.lower().endswith(".pdf"):
                    idx.setdefault(nm.lower(), []).append(os.path.join(dirpath, nm))
    except Exception as e:
        _dbg("_stage_name_index", e)
    _STAGE_NAME_IDX[k] = idx
    return idx


def resolve_file_path(bid, fid):
    """返回该 file_id 在阶段目录里「真实存在」的成品绝对路径（优先级 ads>done>cleaned>pending）。

    不信任 manifest.path 当前指向的阶段：断点续跑（--auto --start-stage ocr 等）会用
    _revive_manifest_paths 把 path 回退到更靠前的阶段，导致下游按 manifest.path 物理过滤时漏件
    （如 pack 只抓到被改动的少数文件 -> 截断包）。这里改为按文件名在各权威阶段目录里找实际成品。
    找不到任何阶段副本时，兜底返回 manifest.path（若存在）；仍无则 None。
    """
    try:
        mf = load_manifest(bid) or {}
        rec = next((f for f in mf.get("files", [])
                    if str(f.get("file_id")) == str(fid)), None)
        if not rec:
            return None
        path = str(rec.get("path") or "")
        name = str(rec.get("name") or "") or os.path.basename(path)
        if name:
            low = name.lower()
            for key in _RESOLVE_STAGE_PRIORITY:
                root = STAGE_ALIAS.get(key)
                if not root:
                    continue
                try:
                    if not Path(root).is_dir():
                        continue
                except Exception:
                    continue
                for cand in _stage_name_index(root).get(low, []):
                    if os.path.isfile(cand):
                        return os.path.abspath(cand)
        # 兜底：manifest.path 本身存在
        if path and os.path.isfile(path):
            return os.path.abspath(path)
        return None
    except Exception as e:
        _dbg("resolve_file_path", e)
        return None


def scan_pdfs(stage, batch_id, file_ids=None):
    """本批次在指定阶段（stage）里实际存在的 PDF 清单。

    全流程唯一的"取文件"入口：状态查看 / 去水印 / OCR / QC / 重命名 /
    清广告 / 打包都必须经由它，禁止全目录扫描。

    file_ids：可选，仅保留这些 file_id（如看板单文件调度传 {"F0001"}；
              None/空 = 不限制，返回全批）。

    返回 [(file_id, 绝对路径字符串), ...]；文件不在该 stage 下、
    物理不存在、或属于测试路径（_测试）的一律不返回。
    """
    out = []
    try:
        if batch_id is None:
            return []
        fids = None
        if file_ids:
            fids = {str(x).strip().upper() for x in file_ids if str(x).strip()}
            if not fids:
                fids = None
        stage_dir = STAGE_ALIAS.get(str(stage).lower(), None)
        stage_s = _np(stage_dir if stage_dir else stage)
        mf = load_manifest(batch_id)
        if not mf:
            return []
        for f in mf.get("files", []):
            p = f.get("path")
            if not p:
                continue
            if fids is not None and str(f.get("file_id") or "").strip().upper() not in fids:
                continue
            ps = _np(p)
            if not ps.startswith(stage_s + os.sep) and ps != stage_s:
                continue
            if is_test_path(ps):
                continue
            if not os.path.isfile(ps):
                continue
            out.append((f.get("file_id"), os.path.abspath(str(p))))
    except Exception as e:
        _dbg("scan_pdfs", e)
    return out


# ---------------------------------------------------------------------------
# pipeline.db：batches / file_steps 状态表
# ---------------------------------------------------------------------------
def _conn():
    """懒建连接 + 建表（幂等）。失败向上抛，由调用方静默。"""
    con = sqlite3.connect(_db_path, timeout=10)
    con.execute("PRAGMA busy_timeout=5000")
    con.execute(
        """CREATE TABLE IF NOT EXISTS batches(
              batch_id TEXT PRIMARY KEY,
              created_at TEXT,
              url TEXT,
              name TEXT,
              status TEXT,
              note TEXT,
              updated_at TEXT)"""
    )
    con.execute(
        """CREATE TABLE IF NOT EXISTS file_steps(
              batch_id TEXT NOT NULL,
              file_id TEXT NOT NULL,
              step INTEGER NOT NULL,
              status TEXT NOT NULL,
              src TEXT,
              dst TEXT,
              error TEXT,
              updated_at TEXT NOT NULL,
              PRIMARY KEY(batch_id, file_id, step))"""
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_fs_batch ON file_steps(batch_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_fs_step ON file_steps(step, status)")
    # 分批流水线下载游标：记录每批已推进到的 discover 偏移量 + 目标总数，供断点续跑从游标继续
    # （修历史坑：每次重跑都从 offset=0 重新解析专题页 -> 永远卡在前 N 个下不到第 N+1 个）。
    con.execute(
        """CREATE TABLE IF NOT EXISTS batch_cursor(
              batch_id TEXT PRIMARY KEY,
              discover_cursor INTEGER NOT NULL DEFAULT 0,
              total_target INTEGER,
              updated_at TEXT NOT NULL)"""
    )
    _ensure_cols(con)
    return con


def _ensure_cols(con):
    """幂等列迁移：为已存在的老表补齐新增列（CREATE TABLE IF NOT EXISTS 不会给存量表加列）。
    - file_steps.src_sha：该步输入文件的内容 sha256，供“跳过三条件”校验产物未被替换。
    - batches.run_id / batch_cursor.run_id：一次“一键全自动”运行的分组键（与 batch_id 拆分）。
    - file_steps.accepted_at/accepted_by：批量验收（pending_accept -> accepted）写入的验收时间/操作者
      （auto 自动 / web 看板 / manual 主控）；batches.accepted_at/accepted_by 为批次级验收快照。"""
    #   - batch_cursor.target_fp：本 URL 目标页 URL 集合的 sha1 前 16 位指纹；下次读游标时若与本次
    #     实际解析到的集合 fp 不一致 -> 视为专题结构变化（增/删目标页），强制 offset=0 重跑并切换 run_id，
    #     修历史坑：专题页目标集合变化后旧 offset 已越过新 total（曾见 offset=168 > 新 total=165 -> 空跑 rc=2）。
    for table, col, decl in (
        ("file_steps", "src_sha", "TEXT"),
        ("batches", "run_id", "TEXT"),
        ("batch_cursor", "run_id", "TEXT"),
        ("batch_cursor", "target_fp", "TEXT"),
        ("file_steps", "accepted_at", "TEXT"),
        ("file_steps", "accepted_by", "TEXT"),
        ("batches", "accepted_at", "TEXT"),
        ("batches", "accepted_by", "TEXT"),
    ):
        try:
            cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
            if cols and col not in cols:
                con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
        except sqlite3.Error:
            pass


def ensure_tables():
    """确保 batches / file_steps 表存在（幂等建表）。失败静默返回 False。"""
    try:
        with _lock:
            con = _conn()
            try:
                return True
            finally:
                con.close()
    except Exception as e:
        _dbg("ensure_tables", e)
        return False


def db_batch_set_cursor(batch_id, discover_cursor, total_target=None, run_id=None, target_fp=None):
    """记录/更新某批次的下载发现游标（分批流水线断点续跑用）。失败静默返回 False。
    target_fp：本 URL 目标页集合的 sha1[:16] 指纹（可选，仅 URL 键游标使用；批级游标传 None 保留原值）。"""
    try:
        with _lock:
            con = _conn()
            try:
                con.execute(
                    "INSERT INTO batch_cursor(batch_id, discover_cursor, total_target, updated_at, run_id, target_fp)"
                    " VALUES(?,?,?,?,?,?) ON CONFLICT(batch_id) DO UPDATE SET"
                    " discover_cursor=excluded.discover_cursor,"
                    " total_target=COALESCE(excluded.total_target, batch_cursor.total_target),"
                    " run_id=COALESCE(excluded.run_id, batch_cursor.run_id),"
                    " target_fp=COALESCE(excluded.target_fp, batch_cursor.target_fp),"
                    " updated_at=excluded.updated_at",
                    (batch_id, int(discover_cursor or 0),
                     (int(total_target) if total_target is not None else None), _now_iso(),
                     run_id or None, target_fp or None))
                con.commit()
            finally:
                con.close()
        return True
    except Exception as e:
        _dbg("db_batch_set_cursor", e)
        return False


# ---- URL 规范化：哈希前先归一，令“同一页面的不同写法”命中同一游标 ----
# 历史坑（本轮 item 2）：游标键直接 sha1(原始 url)，仅差一个 utm_source / 尾部斜杠 / host 大小写，
# 就会被当成“另一条 URL”而 offset 归零，导致同一专题反复从头解析、断点续跑形同虚设。
# 归一规则（务必与 run_auto 的“换 URL 判定”一致）：
#   1) 去掉 fragment（#...）；2) host 统一小写；3) 去掉 path 尾部斜杠；
#   4) 去掉常见跟踪参数 utm_* / from / spm / share_* / _t / timestamp；5) 保留的 query 按键排序去顺序差异。
_URL_TRACK_EXACT = ("from", "spm", "_t", "timestamp")


def _is_tracking_param(k):
    """判断 query 参数名是否为可丢弃的跟踪参数（utm_*/share_*/from/spm/_t/timestamp，大小写不敏感）。"""
    lk = (k or "").strip().lower()
    if lk in _URL_TRACK_EXACT:
        return True
    if lk.startswith("utm_") or lk == "utm":
        return True
    if lk.startswith("share_"):
        return True
    return False


def normalize_url(url):
    """把下载 URL 规范化为稳定字符串（供哈希成游标键）。解析失败时回退 strip() 原串——
    规范化绝不抛异常影响主流程（宁可退化为“按原串区分”，也不崩溃）。"""
    s = str(url or "").strip()
    if not s:
        return s
    try:
        from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
        parts = urlsplit(s)
        host = (parts.hostname or "").lower()
        netloc = parts.netloc
        if host:
            userinfo = parts.netloc.rsplit("@", 1)[0] + "@" if "@" in parts.netloc else ""
            try:
                port = f":{parts.port}" if parts.port else ""
            except ValueError:
                port = ""
            netloc = f"{userinfo}{host}{port}"
        path = parts.path.rstrip("/")            # 去尾部斜杠（根路径归一为空串）
        qpairs = [(k, v) for (k, v) in parse_qsl(parts.query, keep_blank_values=True)
                  if not _is_tracking_param(k)]
        qpairs.sort(key=lambda kv: (kv[0].lower(), kv[1]))   # 排序消除参数顺序差异
        query = urlencode(qpairs)
        return urlunsplit((parts.scheme.lower(), netloc, path, query, ""))  # fragment 恒空 -> 丢弃 #...
    except Exception:
        return s


# ---- URL 维度断点续跑游标（修历史坑：run 游标仅按 run_id 维护，换 URL 仍继承旧 offset）----
# 一次“一键全自动”下多轮批次共享同一 run_id，但 run_id 无法区分“这条 URL 跑到第几个目标”。
# 现象：给旧 run 的 current 批次换一个新专题 URL，db_run_of_batch 反查到旧 run_id -> 继承旧 offset，
# 新 URL 前 N 个目标被整体跳过（曾见 offset=24 > 该页 total=18，直接 0 PDF 空跑）。游标键必须按 URL 隔离。
# 复用 batch_cursor 表，键 = "URL::"+sha1(normalize_url(url))[:16]；run_id 列记该 URL 归属的运行分组键（续跑沿用同一 run）。
def _url_cursor_key(url):
    import hashlib
    ident = normalize_url(url)      # 先归一再哈希：utm/尾斜杠/大小写/参数顺序差异都命中同一游标
    return "URL::" + hashlib.sha1(ident.encode("utf-8")).hexdigest()[:16]


def db_url_cursor(url):
    """读某下载 URL 的续跑游标 -> (run_id, discover_cursor, total_target, target_fp)；
    无记录/失败返回 (None, 0, None, None)。target_fp 是本 URL 上次落库时目标页 URL 集合的 sha1[:16]，
    供调用方与新解析到的集合 fp 比对：不一致 -> 视为专题结构变化，强制 offset 重置 + 换 run_id。"""
    if not url:
        return (None, 0, None, None)
    try:
        with _lock:
            con = _conn()
            try:
                row = con.execute(
                    "SELECT run_id, discover_cursor, total_target, target_fp FROM batch_cursor WHERE batch_id=?",
                    (_url_cursor_key(url),)).fetchone()
            finally:
                con.close()
        if not row:
            return (None, 0, None, None)
        return (row[0] or None, int(row[1] or 0), row[2], (row[3] or None) if len(row) > 3 else None)
    except Exception as e:
        _dbg("db_url_cursor", e)
        return (None, 0, None, None)


def db_url_set_cursor(url, discover_cursor, run_id=None, total_target=None, target_fp=None):
    """记录/更新某下载 URL 的续跑游标（含归属 run_id 与目标集合 fp）。失败静默返回 False。
    target_fp 传 None 时保留旧值（COALESCE），传具体值则覆盖。"""
    if not url:
        return False
    return db_batch_set_cursor(_url_cursor_key(url), discover_cursor,
                               total_target=total_target, run_id=run_id, target_fp=target_fp)


def db_batch(batch_id, url=None, name=None, status=None, note=None, run_id=None):
    """UPSERT 批次账头。失败静默返回 False。"""
    try:
        with _lock:
            con = _conn()
            try:
                row = con.execute("SELECT batch_id FROM batches WHERE batch_id=?",
                                  (batch_id,)).fetchone()
                now = _now_iso()
                if row is None:
                    con.execute(
                        "INSERT INTO batches(batch_id,created_at,url,name,status,"
                        "note,updated_at,run_id) VALUES(?,?,?,?,?,?,?,?)",
                        (batch_id, now, url or "", name or "",
                         status or "running", note or "", now, run_id or None))
                else:
                    sets, vals = ["updated_at=?"], [now]
                    if status is not None:
                        sets.append("status=?"); vals.append(status)
                    if note is not None:
                        sets.append("note=?"); vals.append(note)
                    if url:
                        sets.append("url=?"); vals.append(url)
                    if name:
                        sets.append("name=?"); vals.append(name)
                    if run_id:
                        sets.append("run_id=?"); vals.append(run_id)
                    vals.append(batch_id)
                    con.execute(f"UPDATE batches SET {','.join(sets)} WHERE batch_id=?", vals)
                con.commit()
                return True
            finally:
                con.close()
    except Exception as e:
        _dbg("db_batch", e)
        return False


# 与 pipeline.clean_text 保持同步（跨模块复制，避免循环 import）；改动必须两处同时进行。
# 函数体逐字符一致（含 _ANSI_RX/_CTRL_RX 定义与清洗顺序）。仅注释/docstring 可有差异。
# 文本清洗（与 pipeline.clean_text 同源副本，不跨模块 import 避免循环依赖）：去 ANSI/控制字符、
# 归一换行、截断。db_step 写库前对 src/dst/error 统一收口，作为所有直调点的最后防线。
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


def db_step(batch_id, file_id, step, status, src=None, dst=None, error="", src_sha=None):
    """UPSERT 一个文件的某步骤状态（每次状态变化写一次，updated_at 刷新）。
    src_sha 非空时记录该步输入内容哈希；省略时保留已存值（COALESCE）。
    src/dst/error 入库前统一 clean_text 清洗（500 字）：收口所有绕过 _bstep 的直调点。"""
    try:
        step = int(step)
        status = str(status).strip().lower()
        src = clean_text(src, 500) or None
        dst = clean_text(dst, 500) or None
        error = clean_text(error, 500) or ""
        with _lock:
            con = _conn()
            try:
                con.execute(
                    "INSERT INTO file_steps(batch_id,file_id,step,status,src,dst,"
                    "error,updated_at,src_sha) VALUES(?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(batch_id,file_id,step) DO UPDATE SET "
                    "status=excluded.status, "
                    "src=COALESCE(excluded.src, file_steps.src), "
                    "dst=COALESCE(excluded.dst, file_steps.dst), "
                    "src_sha=COALESCE(excluded.src_sha, file_steps.src_sha), "
                    "error=excluded.error, updated_at=excluded.updated_at",
                    (batch_id, file_id, step, status,
                     src, dst, error, _now_iso(),
                     src_sha or None))
                con.commit()
                return True
            finally:
                con.close()
    except Exception as e:
        _dbg("db_step", e)
        return False


def db_repath(batch_id, file_id, old_path, new_path):
    """原地改名后把各步骤登记的旧路径回填为新路径（dst/src 两列精确匹配旧值才更新）。
    杜绝下游 rename 改名后 step2(watermark) 的 dst 悬空——reconcile/查询不再依赖回落兜底。
    返回更新行数；失败静默返回 0。"""
    if not batch_id or not file_id or not old_path or not new_path or old_path == new_path:
        return 0
    try:
        with _lock:
            con = _conn()
            try:
                n = 0
                for col in ("dst", "src"):
                    cur = con.execute(
                        f"UPDATE file_steps SET {col}=?, updated_at=? "
                        "WHERE batch_id=? AND file_id=? AND " + col + "=?",
                        (new_path, _now_iso(), str(batch_id), str(file_id), old_path))
                    n += max(0, cur.rowcount)
                con.commit()
                return n
            finally:
                con.close()
    except Exception as e:
        _dbg("db_repath", e)
        return 0


def db_step_get(batch_id, file_id, step):
    """读一个 (文件,步骤) 状态行 -> dict 或 None。"""
    try:
        with _lock:
            con = _conn()
            try:
                con.row_factory = sqlite3.Row
                r = con.execute(
                    "SELECT * FROM file_steps WHERE batch_id=? AND file_id=? AND step=?",
                    (batch_id, file_id, int(step))).fetchone()
                return dict(r) if r else None
            finally:
                con.close()
    except Exception as e:
        _dbg("db_step_get", e)
        return None


def is_step_ok(batch_id, file_id, step):
    """该文件该步是否已 ok（断点续传判断用）。"""
    r = db_step_get(batch_id, file_id, step)
    return bool(r and r.get("status") == ST_OK)


def get_step_sha(batch_id, file_id, step):
    """读该文件该步记录的输入内容 sha256（无则 None）。"""
    r = db_step_get(batch_id, file_id, step)
    return (r or {}).get("src_sha")


def set_step_sha(batch_id, file_id, step, src_sha):
    """单独补写某步的输入内容 sha256（不改动状态）。失败静默。"""
    try:
        with _lock:
            con = _conn()
            try:
                con.execute(
                    "UPDATE file_steps SET src_sha=? WHERE batch_id=? AND file_id=? AND step=?",
                    (src_sha, batch_id, file_id, int(step)))
                con.commit()
                return True
            finally:
                con.close()
    except Exception as e:
        _dbg("set_step_sha", e)
        return False


def db_run_of_batch(batch_id):
    """查某批次所属 run_id（无则 None）。"""
    try:
        with _lock:
            con = _conn()
            try:
                r = con.execute("SELECT run_id FROM batches WHERE batch_id=?",
                                (batch_id,)).fetchone()
            finally:
                con.close()
        return (r[0] if r and r[0] else None)
    except Exception as e:
        _dbg("db_run_of_batch", e)
        return None


def db_batches_of_run(run_id):
    """列出某次运行下的全部 batch_id（按创建时间升序）。"""
    try:
        with _lock:
            con = _conn()
            try:
                rows = con.execute(
                    "SELECT batch_id FROM batches WHERE run_id=? ORDER BY created_at ASC",
                    (run_id,)).fetchall()
            finally:
                con.close()
        return [r[0] for r in rows]
    except Exception as e:
        _dbg("db_batches_of_run", e)
        return []


def db_step_counts(batch_id):
    """本批次 {step: {status: n}} 统计。"""
    try:
        with _lock:
            con = _conn()
            try:
                out = {}
                for step, st, n in con.execute(
                        "SELECT step,status,COUNT(*) FROM file_steps "
                        "WHERE batch_id=? GROUP BY step,status", (batch_id,)):
                    out.setdefault(step, {})[st] = n
                return out
            finally:
                con.close()
    except Exception as e:
        _dbg("db_step_counts", e)
        return {}


def db_files_of(batch_id, step=None, status=None):
    """查本批次 file_steps 行（可按 step/status 过滤）。"""
    try:
        with _lock:
            con = _conn()
            try:
                con.row_factory = sqlite3.Row
                sql = "SELECT * FROM file_steps WHERE batch_id=?"
                vals = [batch_id]
                if step is not None:
                    sql += " AND step=?"; vals.append(int(step))
                if status is not None:
                    sql += " AND status=?"; vals.append(str(status).lower())
                return [dict(r) for r in con.execute(sql, vals).fetchall()]
            finally:
                con.close()
    except Exception as e:
        _dbg("db_files_of", e)
        return []


def batch_report_path(batch_id):
    r"""清广告扫描报告的标准路径：D:\work\gaokzx\reports\bottom_ads_<batch_id>.json"""
    return REPORTS_DIR / f"bottom_ads_{batch_id}.json"


# ---------------------------------------------------------------------------
# 批量验收族（2026-10-22 拆至 bcaccept.py；此处 re-export 保持 `import batchcore as bc` 兼容。
# 注意：bcaccept 函数内 import batchcore，模块级加载顺序无环——新增引用方一律 import batchcore）
# ---------------------------------------------------------------------------
from bcaccept import (
    _ACCEPT_CHUNK,
    _chunks,
    batch_accept,
    db_pending_batches,
)
