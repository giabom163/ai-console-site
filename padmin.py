#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""gaokzx 流水线 · 数据维护与状态（padmin）—— 2026-10-21 自 pipeline.py 拆出。

职责：DB 失效路径清理（step_prune_db）、旧文件清理（step_cleanup）、
归入已处理（step_migrate_done）、状态总表导出（step_export_excel）、状态查看（show_status）。
依赖：pcore、exporter（延迟 import）、pdownload（_load_recent_suspect_reports）、
pflow（_pick_resumable_batch，延迟 import 防分层成环）。被 pipeline.py / pflow 共享。
"""
from pathlib import Path
import datetime
import os
import shutil
import sqlite3
import sys
import pcore
from pcore import (
    LOG_ROOT,
    STAGES,
    STAGE_ADS,
    STAGE_CLEANED,
    STAGE_DONE,
    STAGE_PACKED,
    STAGE_PENDING,
    STEP_NAMES,
    WORK,
    _BANG,
    _SEP,
    _wf_bar,
    bc,
    has_targets,
    ledgerio,
    log_err,
    log_ok,
)
from pdownload import (
    _load_recent_suspect_reports,
)


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
      A 孤立旧文件（无任何 pcore.DB 引用）              -> 默认删（最安全，无追溯链可断）
      B pcore.DB 引用但非活跃产物（started/skip/早期步）  -> 默认保护；仅 --include-db-stale 才删（删会断 pcore.DB 追溯链、不可恢复）
      C pcore.DB 引用且 step∈{4,5,6,7} 且 status='ok'   -> 默认保护；仅 --include-db-ok 才删（活跃产物）
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

    # ---- 构建 pcore.DB 引用集合：protected(C) / referenced(A/B 区分) ----
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
    lines.append(f"  [{'将删' if include_db_stale else '保留'}] B 类 pcore.DB 引用(非活跃) : {_nB} 个{_b_note}")
    _c_note = "（已启用 --include-db-ok，将随本次删除）" if include_db_ok else "（活跃产物，默认保护）"
    lines.append(f"  [{'将删' if include_db_ok else '保留'}] C 类 pcore.DB 活跃产物     : {_nC} 个{_c_note}")
    if _nB and not include_db_stale:
        lines.append("  [!] B 类是 pcore.DB 引用过的历史输入，删除将断链且不可恢复（除非有外部备份）；默认不删。")
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
            db_path=str(pcore.DB), status_json=str(WORK / "_status" / "status.json"),
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
    # 看板候选①（仅显示回退，不写 pcore.DB/不改指针/不调 set_current）：当前批次是空批次
    # （manifest 无文件，如零新增轮次）时，回退展示最近一个非空批次，避免"本批次全 0"误导。
    _orig_bid = bid
    _empty_note = None
    if bid and bc is not None and not has_targets(bid):
        from pflowauto import _pick_resumable_batch
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
            _zc = sqlite3.connect(str(pcore.DB))
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
    if pcore.DB.exists():
        con = sqlite3.connect(str(pcore.DB))
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
