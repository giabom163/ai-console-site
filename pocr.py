#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""gaokzx 流水线 · OCR 补文字层与质控（pocr）—— 2026-10-21 自 pipeline.py 拆出。

职责：OCR 内存/显存护栏（ocr_worker_budget 族）、文字层检测与缓存估算、
step_ocr（474 行事故驱动代码）/ step_clean_ocr_dirty / step_qc。
依赖：pcore；BatchOcrPdfAddTextLayer 在函数内延迟 import。
被 pipeline.py / pflow / ppack / pcore.step_check_env（延迟 import 护栏函数）共享。
"""
from pathlib import Path
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import tempfile
import time
import pcore
from pcore import (
    OCR_REPO,
    OUT,
    PY,
    ROOT,
    STAGE_ADS,
    STAGE_CLEANED,
    STAGE_DONE,
    STAGE_PENDING,
    STEP_OCR,
    STEP_QC,
    _bstep,
    _merge_retry,
    _resolve_bid,
    _retry_mark_done,
    _scan,
    _sha256_file,
    _skip_ok,
    _wf_bar,
    _wf_install_sigint,
    _wskip,
    bc,
    clean_text,
    iter_pdfs,
    ledgerio,
    mark,
    statusio,
)


OCR_WORKER = ROOT / "wm_ocr_batch_worker.py"  # OCR 批处理子进程入口（引擎初始化一次/批，服务 step_ocr）


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
