#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""gaokzx 流水线 · 打包与验收（ppack）—— 2026-10-21 自 pipeline.py 拆出。

职责：打包双模式（step_pack_legacy / step_pack_topic + step_pack 分派）、验收归档
（step_accept）、zip 校验（_final_zip_check/_batch_zips_exist）、崩溃落盘、
DB↔磁盘核对（reconcile_round）、悬空 dst 修复（step_fix_dst）。持有 _LAST_PACK_ZIP(S)。
依赖：pcore、pocr（step_qc）。被 pipeline.py / pflow / test_topic_pack.py 共享。
"""
from pathlib import Path
import datetime
import os
import shutil
import sqlite3
import sys
import zipfile
import pcore
from pcore import (
    LOG_ROOT,
    STAGE_ADS,
    STAGE_CLEANED,
    STAGE_COMPLETED,
    STAGE_DONE,
    STAGE_PACKED,
    STAGE_PENDING,
    STEP_ACCEPT,
    STEP_NAMES,
    STEP_PACK,
    _DASH,
    _ask_apply,
    _bstep,
    _file_present,
    _manifest_curpath,
    _resolve_bid,
    _sha256_file,
    _wf_bar,
    bc,
    db,
    ledgerio,
    log_err,
    statusio,
)
from pocr import (
    step_qc,
)


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


def step_pack_legacy(con=None, root=None, out=None, limit=None, dry=False, _comment="",
              batch_id=None, ask_apply=True, skip_wm=False,
              tag="", only_unpacked=False):
    """第 7 步：打包 zip（STEP_PACK=7）。源默认「已清广告」（STAGE_ADS）；仅 skip_wm=True（步骤 6 未跑/被放弃）
    时才回退「去水印后」（STAGE_CLEANED）且必须警告。只打包当前 manifest 内文件（禁全目录扫描）；打包前自动 QC；
    dry-run 预览后询问"是否正式执行？[y/N]"（默认 N）；正式执行成功才写 ok 并记 _LAST_PACK_ZIP。返回 zip 路径或 None。
    tag：zip 文件名后缀（分批流水线 _rN 区分轮次）；only_unpacked=True：跳过已打包（ok/skip）的文件
    （每轮只打包本轮新文件），全部已打包时返回 "ALREADY"。"""
    import zipfile
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
    if pcore.ONLY_FILE_IDS:
        _want = {str(x).strip().upper() for x in pcore.ONLY_FILE_IDS}
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
    if pcore.ONLY_FILE_IDS and len(files) == 1:
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
    单文件调度（pcore.ONLY_FILE_IDS）恒走 legacy 小包，不受开关影响。tag/only_unpacked 仅 legacy 有意义。"""
    if _pack_mode() != "topic" or pcore.ONLY_FILE_IDS:
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
            _zc = sqlite3.connect(str(pcore.DB))
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
    """每轮开始对当前批次 manifest 做双向核对，修正 pcore.DB 与磁盘不一致（修“已跳过但产物 MISSING”）：
      (A) 某步 pcore.DB=ok 但其产物真缺失（登记路径与当前权威路径均不存在）-> 回滚该步及下游为 started，重处理；
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
              "原因=磁盘存在但 pcore.DB 无 download 记录（已过非零/非损坏校验）"
              + (f"；另有 {bad_orphan} 条磁盘文件未过完整性校验（零字节/损坏），不补记" if bad_orphan else ""))
    if verbose and (reset_files or sha_resets):
        print(f">>> [reconcile] pcore.DB↔磁盘核对：回滚 {len(reset_files)} 个文件至 pending 重处理"
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
