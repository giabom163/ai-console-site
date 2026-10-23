#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""gaokzx 流水线 · 网址嗅探与下载调度（pdownload）—— 2026-10-21 自 pipeline.py 拆出。

职责：专题页 URL 嗅探（sniff_url 族）、专题发现与批量下载（step_discover_download 族，
含 rc2/rc3 下载中止处理）、隐形水印清理调度（step_ads）。
依赖：pcore（公共核心）、batchcore；gaokzx_download 在函数内延迟 import。
被 pipeline.py / pflow / padmin 共享；不反向依赖其他新模块。
"""
from pathlib import Path
import datetime
import json
import os
import re
import subprocess
import pcore
from pcore import (
    LOG_ROOT,
    PY,
    ROOT,
    STAGE_ADS,
    STAGE_PENDING,
    _BANG,
    _DASH,
    _SEP,
    _ask_apply,
    _resolve_bid,
    _scan,
    bc,
    log_err,
)


GAOKZX_DL = ROOT / "gaokzx_download.py"


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
        self.year_cands = []      # 供交互自选的"年份"候选（collect_year_candidates 产出；默认空）

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

def sniff_url(url, timeout=20, max_preview=10, paper_type=None, year_type=None):
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
                                      record_matches, record_matches_year,
                                      collect_paper_candidates, collect_year_candidates)
            recs = parse_topic(html)
            res.paper_cands = collect_paper_candidates(recs)   # 候选列表（默认不据此过滤）
            res.year_cands = collect_year_candidates(recs)     # 年份候选（取自 col 列，默认不据此过滤）
            _flt_suffix = ""
            if paper_type:
                from gaokzx_download import _build_paper_terms
                _pterms = _build_paper_terms(paper_type)
                if _pterms:
                    _b = len(recs)
                    recs = [r for r in recs if record_matches(r, _pterms)]
                    _flt_suffix = f"（试卷类型筛选：{_b} -> {len(recs)}）"
            if year_type:
                from gaokzx_download import _build_year_terms
                _yterms = _build_year_terms(year_type)
                if _yterms:
                    _b2 = len(recs)
                    recs = [r for r in recs if record_matches_year(r, _yterms)]
                    _flt_suffix += f"（年份筛选：{_b2} -> {len(recs)}）"
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
        if pcore.ONLY_FILE_IDS:
            # 单文件调度（看板 --file-id）：只扫描指定文件，报告只含目标文件
            _fids_s = ",".join(sorted(pcore.ONLY_FILE_IDS))
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
        cmd += ["--db-path", str(pcore.DB)]      # 台账跟随当前库（压测库/生产库）
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
                           expected_fp=None, paper_type=None, year_type=None):
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
    if year_type: cmd += ["--year-type", str(year_type)]
    if dry: cmd += ["--dry-run"]
    if bid: cmd += ["--batch-id", bid]
    cmd += ["--db-path", str(pcore.DB)]      # 台账跟随当前库（压测库/生产库）
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
        # 分批流水线后续轮次（offset > 0）：本批目标页都没 PDF 附件是正常现象
        # （"作文题目"/"范文"等非附件页），按真·空返回 rc=2，让分批循环 break，
        # 不再误判 rc=3 中止整个 run、也不再丢掉已跑完的前几轮成果。
        if offset and int(offset) > 0:
            print(f">>> 本批 {_tp} 个目标页均无 PDF 附件（offset={offset}，非首轮），按正常空结束处理。")
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
