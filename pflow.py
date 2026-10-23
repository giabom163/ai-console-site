#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""gaokzx 流水线 · 交互菜单与自动编排（pflow）—— 2026-10-21 自 pipeline.py 拆出。

职责：交互主菜单（MENU/run_wizard/flow_* 族）、步骤安全包装（run_step_safe）。
auto 编排/断点续跑已迁至 pflowauto.py（from pflowauto import 回保持调用方零改动）。
依赖：pcore/prename/pwm/pdownload/pocr/ppack/padmin/pflowauto。被 pipeline.py 共享。
"""
import argparse
import os
import re
import sqlite3
import traceback
import pcore
import ppack
from pcore import (
    AUTO_ACCEPT,
    STAGES,
    STAGE_ADS,
    STAGE_CLEANED,
    STAGE_DONE,
    STAGE_PACKED,
    STAGE_PENDING,
    STEP_ACCEPT,
    STEP_AD_CLEAN,
    STEP_DOWNLOAD,
    STEP_OCR,
    STEP_PACK,
    STEP_RENAME,
    STEP_WATERMARK,
    _BackToMenu,
    _CORE_NEEDED,
    _DASH,
    _SEP,
    _ask,
    _ask_int,
    _hr,
    _missing_deps,
    _scan,
    _selfcheck,
    _wskip,
    ask_continue_or_abort,
    bc,
    db,
    ensure_dirs,
    fatal_hint,
    has_targets,
    install_sigint,
    ledgerio,
    run_step_safe,
    statusio,
    step_check_env,
)
from prename import (
    _ask_rename_choice,
    apply_renames,
    plan_renames,
    show_rename_conflicts,
    step_rename,
)
from pwm import (
    step_watermark,
)
from pdownload import (
    _after_download,
    _handle_rc2,
    _handle_rc3,
    _new_batch_for_url,
    _print_rc3_banner,
    _report_empty_run,
    format_sniff,
    sniff_url,
    step_ads,
    step_discover_download,
)
from pocr import (
    OCR_MEM_BUDGET_GB,
    OCR_THREADS_PER_WORKER,
    OCR_WORKERS_DEFAULT,
    OCR_WORKERS_HARD_CAP,
    _ocr_gpu_active,
    has_text_layer,
    ocr_worker_budget,
    step_clean_ocr_dirty,
    step_ocr,
    step_qc,
)
from ppack import (
    _batch_zips_exist,
    _final_zip_check,
    _pack_mode,
    _parse_file_ids,
    _stage_root_of,
    _write_auto_crash_log,
    install_auto_crash_hook,
    reconcile_round,
    step_pack,
)
from pflowauto import (
    _ask_paper_type,
    _detect_resume_stage,
    _flow_ctx,
    _pick_resumable_batch,
    run_auto,
)
from padmin import (
    show_status,
    step_export_excel,
    step_migrate_done,
    step_prune_db,
)


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
        if c == "1": return _download_topic_menu(None, url=url, paper_cands=res.paper_cands, year_cands=res.year_cands)
        if c == "2":
            n = _ask_int("请输入要抓取的数量（如 10）: ", lo=1)
            return _download_topic_menu(n, url=url, paper_cands=res.paper_cands, year_cands=res.year_cands)

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

def _download_topic_menu(limit=None, url=None, paper_type=None, paper_cands=None,
                         year_type=None, year_cands=None):
    """专题选择菜单。传了 url 就走自定义专题页（--url），否则走预设 topic 1/2/all。"""
    if url:
        print(f"\n将使用当前网址作为自定义专题：\n  {url}")
        if paper_cands:
            _pt, _act = _ask_paper_type(paper_cands, ask=_ask)
            if _act == "set":
                paper_type = _pt
        if year_cands:
            _yt, _yact = _ask_paper_type(year_cands, ask=_ask, label="年份")
            if _yact == "set":
                year_type = _yt
        if _ask("确认开始？[y/N]: ", "n", auto="y").lower() != "y":
            print("已取消。")
            return
        ensure_dirs(verbose=False)
        print(f"\n>>> 将以 --url 下载" f"{' limit=' + str(limit) if limit else '（全量）'} ...")
        rc, _res = step_discover_download(limit=limit, url=url, paper_type=paper_type, year_type=year_type)
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
    # topic 多专题时 ppack._LAST_PACK_ZIP 仅记首个，"是否打过包"判定须并读 ppack._LAST_PACK_ZIPS（兼容旧路径）
    if _bid and (ppack._LAST_PACK_ZIP or ppack._LAST_PACK_ZIPS):
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
        year_type=None,
        force=False, dry_run=False, chunk=24, force_full_run=False,
        ask_on_empty=False, ask_on_stage_failure=False,
        ask_on_resume=False, force_rerun_failed=False)
    rc = run_auto(ns)
    if rc == 0:
        # 修「假成功」：rc==0 同时涵盖"真有产出"与"零新增空结束"，须用本次是否真产出 zip 区分
        # （两个产出标志已在 run_auto 开头重置，故此值严格反映本次运行）。
        _nz = len(ppack._LAST_PACK_ZIPS) or (1 if ppack._LAST_PACK_ZIP else 0)
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
            _c = sqlite3.connect(str(pcore.DB))
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
