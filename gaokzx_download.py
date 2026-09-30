# -*- coding: utf-8 -*-
"""
gaokzx.com 专题资源采集器
=========================

用法
----
    # 1) 只看清单，不下载（干跑）
    python gaokzx_download.py --topic all --dry-run

    # 2) 先跑 3 条，验证链路
    python gaokzx_download.py --topic 1 --limit 3

    # 3) 全量下载
    python gaokzx_download.py --topic all

    # 4) 断点调试（在关键位置自动停下，进 pdb）
    python gaokzx_download.py --topic 1 --limit 1 --debug

    # 5) 也可以直接用 pdb 跑
    python -m pdb gaokzx_download.py --topic 1 --limit 1

    # 6) 直接指定专题页 URL（无需登记到 TOPICS）
    python gaokzx_download.py --url "https://www.gaokzx.com/gk/shitiku/159032.html" --dry-run
    python gaokzx_download.py --url "https://www.gaokzx.com/gk/shitiku/159032.html" --limit 3
    # 专题目录名默认取页面 <title>；如需固定可加 --name
    python gaokzx_download.py --url "<URL>" --name "专题3_xxx"

流程
----
    专题页 ──解析表格──► 目标详情页列表
          └─► 逐页抓取 ──解析 __NUXT_DATA__──► PDF 直链
                       └─► 下载到 本地目录
                       └─► 汇总成 清单.xlsx（含下载状态，便于断点续传）

设计要点（针对"站点规则老变"）
-----------------------------
* 解析全部走「表头感知 + 左邻上下文」，不写死列号/列名；
* 附件全部走 devalue 载荷，不依赖 DOM class 名（改版换 class 也不影响）；
* 断点续传：以 manifest 为准，已下载且文件存在则跳过；
* 全程 debug 日志（--verbose 打印每次请求/解析的中间变量）。
"""
import argparse
import json
import os
import re
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import unquote, urlparse

import openpyxl
import requests
import urllib3
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from gaokzx_parse import (abs_url, detail_title, extract_files, paper_type_key,
                          parse_topic, record_matches)

# 批次核心（D:\work\gaokzx\batchcore.py）——同目录，直接 import；
# 负责批次 manifest 与 file_steps 状态表（下载步骤的 started/ok/skip/fail 落库）。
try:
    import batchcore as bc
except Exception:
    bc = None

# 文件处理台账（D:\work\_status\ledgerio.py）——把每个 PDF 的落地路径与
# 下载状态追加写入 pipeline.db 的 file_events 表；纯标准库、失败静默，
# 绝不影响下载。默认生产库，pipeline.py 会传 --db-path 跟随当前库。
sys.path.insert(0, r"D:\work\_status")
try:
    import ledgerio
except Exception:
    ledgerio = None

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Windows 控制台默认 GBK 编码：遇到不可编码字符时替换输出而非崩溃
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(errors="replace")
    except (AttributeError, ValueError, OSError):
        pass

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
TOPICS = {
    "1": {
        "name": "专题1_2025-2026北京各中学高三每月月考试题及答案",
        "url": "https://www.gaokzx.com/gk/shitiku/145733.html",
    },
    "2": {
        "name": "专题2_2025-2026北京各区高三上学期期末试题及答案",
        "url": "https://www.gaokzx.com/gk/shitiku/151404.html",
    },
}

OUT_ROOT = r"D:\work\文库资源"
LIST_DIR = None   # 清单输出目录；None=OUT_ROOT/清单。--test-root 隔离时由 pipeline 传 --out-root/--list-dir 覆盖
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
BAD_FS = re.compile(r'[\\/:*?"<>|\r\n\t]')

# 站点标题后缀（"xxx_北京高考在线" / "-高考在线" 等），目录名/章节名里要去掉
SITE_SUFFIX_RX = re.compile(r"[\-_|—]?\s*(北京高考在线|高考在线|北京高考网)\s*$")

# 中间汇总页下钻深度上限：总汇总页 -> 年级汇总页 -> 试卷详情页。
# 站点既有两层形态（专题页 -> 详情页），也有三层形态（如 147414 总汇总），
# 解析阶段自动逐层下钻；上限只为防站点异常回环导致的无限展开。
MAX_DRILL_DEPTH = 4

LOG = lambda *a, **k: print(*a, **k, flush=True)


def clean_site_title(t):
    """清洗站点标题：去掉 "_北京高考在线" 之类后缀。"""
    return SITE_SUFFIX_RX.sub("", (t or "").strip()).strip()


def topic_from_url(url, name=None):
    """把任意专题页 URL 变成一个可直接使用的 topic 配置。

    - 传了 name 就用 name；
    - 否则抓页面 <title> 清洗成合法目录名（去掉站点后缀，压掉非法字符）。
    返回 dict(name, url, key)，key 用 URL 尾号，便于清单溯源。
    """
    if not name:
        try:
            s = make_session()
            html = fetch(s, url, verbose=False)
            m = re.search(r"<title>(.*?)</title>", html, re.S | re.I)
            t = clean_site_title(m.group(1) if m else "")
            name = safe_name(t) if t else ""
        except (requests.RequestException, OSError, ValueError):
            name = ""
    if not name:
        m = re.search(r"/(\d+)\.html?$", url)
        name = f"专题_{m.group(1) if m else '自定义'}"
    m = re.search(r"/(\d+)\.html?$", url)
    key = m.group(1) if m else name
    return {"name": name, "url": url, "key": key}


# ---------------------------------------------------------------------------
# 会话
# ---------------------------------------------------------------------------
def make_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": UA,
        "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://www.gaokzx.com/",
    })
    retry = Retry(total=4, backoff_factor=1.2,
                  status_forcelist=[429, 500, 502, 503, 504],
                  allowed_methods=["GET", "HEAD"])
    ad = HTTPAdapter(max_retries=retry, pool_connections=16, pool_maxsize=16)
    # 站点全站 HTTPS（含 CDN 直链），http 链接走 requests 默认适配器即可
    s.mount("https://", ad)
    # 站点证书链有问题（不加 -k / verify=False 会 SSL 握手失败），这里显式放宽
    s.verify = False
    return s


def safe_name(s, max_len=140):
    s = BAD_FS.sub("_", (s or "").strip())
    s = re.sub(r"\s+", " ", s)
    s = s.strip(" .")
    return (s[:max_len]).strip(" .") or "unnamed"


def ctx_prefix(rec):
    """由上下文生成便于归档的前缀：东城区_北京五中_化学试题"""
    parts = []
    for g in rec.get("group", []):
        g = safe_name(g)
        if g and g not in parts and not g.endswith(("试题", "答案")):
            parts.append(g)
        elif g and g not in parts and len(parts) < 1:
            parts.append(g)
    return "_".join(parts[:3])


# ---------------------------------------------------------------------------
# 抓取层
# ---------------------------------------------------------------------------
# 试卷类型别名表：键=归一后主名，值=归一后别名元组。
# 语义：用户输入命中键时，词表额外并入这些别名（子串匹配，故"北京"能命中
# "北京卷/北京各区"等；反之输入别名也能命中主名记录）。
_PAPER_TYPE_ALIASES = {
    # 反例警示：不要加 "全国I卷": ("全国I",) —— "全国I" 是 "全国II" 的子串，
    # 去卷别名会串味（第 1 步实测）。
    "北京卷": ("北京",),
    # 后续按站点实际"试卷类型"列取值扩展，如：
    # "全国甲卷": ("甲卷",),
    # "新高考I卷": ("新高考1卷",),
}


def _build_paper_terms(paper_type):
    """把 --paper-type 原始输入转成「归一化子串词表」（含别名展开）。

    - None/空串/全空白 -> 返回 []（调用方据此判定"不筛选"，非"全不匹配"）
    - 支持多值分隔：英文/中文逗号、顿号、斜杠、空白
    - 每个 term 过 paper_type_key 归一（去空白/罗马转拉丁/全角转半角/casefold），
      与 record_matches 内部归一保持一致；空归一结果丢弃；去重保序
    """
    if not paper_type:
        return []
    if isinstance(paper_type, str):
        parts = [p for p in re.split(r"[,，、/\s]+", paper_type) if p]
    else:
        parts = [str(p) for p in paper_type]
    terms, seen = [], set()

    def _add(x):
        k = paper_type_key(x)
        if k and k not in seen:
            seen.add(k)
            terms.append(k)

    for p in parts:
        _add(p)
        for key in (p, paper_type_key(p)):          # 原键 + 归一键双查，容错
            for alias in _PAPER_TYPE_ALIASES.get(key, ()):
                _add(alias)
    return terms


def fetch(session, url, verbose=False, expect_binary=False, timeout=40):
    t0 = time.time()
    r = session.get(url, timeout=timeout)
    dt = time.time() - t0
    if verbose or r.status_code != 200:
        LOG(f"    [HTTP] {r.status_code} {len(r.content):>9}B {dt:5.2f}s  {url}")
    r.raise_for_status()
    if expect_binary:
        return r.content
    # 站点声明 utf-8；个别老页面是 gbk，做个兜底
    enc = r.encoding
    if not enc or enc.lower() in ("iso-8859-1",):
        enc = "utf-8"
    try:
        return r.content.decode(enc)
    except UnicodeDecodeError:
        return r.content.decode("gbk", "replace")


def collect_targets(session, topic_key, verbose=False, cfg=None, paper_terms=None):
    """抓专题页 -> 解析表格 -> 目标详情页列表。

    cfg 非空时直接用它（--url 自定义专题），否则从 TOPICS[topic_key] 取。
    """
    cfg = cfg or TOPICS[topic_key]
    key = cfg.get("key", topic_key)
    LOG(f"\n=== [采集] {cfg['name']}")
    LOG(f"    专题页: {cfg['url']}")
    html = fetch(session, cfg["url"], verbose)
    LOG(f"    专题页大小: {len(html)} 字符")

    recs = parse_topic(html)
    LOG(f"    解析出表格记录: {len(recs)} 条")

    if not recs:
        # 没有表格 → 该 URL 本身可能就是一个「详情页」（页内直接挂 PDF 附件）。
        # 站点既有 专题页→表格→详情页 的汇总页，也有单校/单科的内容页，
        # 两种形态都要能采，否则用户贴个详情页链接会静默空转。
        files = extract_files(html, include_images=False)
        if files:
            LOG(f"    本页无表格，但直接含 {len(files)} 个 PDF 附件 -> 按「详情页」处理")
            title = clean_site_title(detail_title(html)) or cfg["name"]
            rec = {"section": "", "group": [], "col": "", "text": title,
                   "url": abs_url(cfg["url"]), "row": 0, "col_idx": 0,
                   "topic": key}
            if paper_terms and not record_matches(rec, paper_terms):
                LOG(f"    [试卷类型] 详情页标题不匹配 {paper_terms} -> 跳过")
                return []
            return [rec]
        LOG("    [警告] 本页既无表格记录、也未发现 PDF 附件，无法推进。")
        return []

    targets, seen = [], {}
    for r in recs:
        url = abs_url(r["url"])
        if url in seen:                     # 同一页多个入口，合并上下文
            seen[url]["text"] += " | " + r["text"]
            continue
        rec = dict(r)
        rec["url"] = url
        rec["topic"] = key
        seen[url] = rec
        targets.append(rec)
    if paper_terms:
        _before = len(targets)
        targets = [t for t in targets if record_matches(t, paper_terms)]
        LOG(f"    [试卷类型] 过滤 {paper_terms}: {_before} -> {len(targets)}")
    LOG(f"    去重后目标详情页: {len(targets)} 个")
    if verbose and targets:
        LOG("    [变量追踪] 前 3 条解析结果:")
        for t in targets[:3]:
            LOG("      " + json.dumps(
                {k: t[k] for k in ("section", "group", "col", "text", "url")},
                ensure_ascii=False))
    return targets


def resolve_pdf(session, rec, verbose=False):
    """抓详情页 -> 取出 PDF 直链。返回 (rec, [pdf_item, ...])。

    页面无 PDF 附件但本身还有表格（即「中间汇总页」，如总汇总页下的
    年级汇总页）时，把表格记录写进 rec["drill_out"]，由 run() 继续下钻展开。
    """
    try:
        html = fetch(session, rec["url"], verbose)
    except requests.RequestException as e:
        rec["error"] = f"详情页抓取失败: {e}"
        return rec, []

    files = extract_files(html, include_images=False)
    rec["detail_title"] = detail_title(html)
    if verbose:
        LOG(f"    [解析] {rec['url']} -> 附件 {len(files)} 个")
        for f in files:
            LOG(f"        {f['fileName']}  <=  {f['fileUrl']}")
    if not files:
        # 无附件：可能本身是「中间汇总页」（总汇总 -> 年级汇总 -> 试卷详情）。
        # 页里还有表格记录就交回调用方继续下钻；表格也没有才算真「无附件」。
        subs = parse_topic(html)
        if subs:
            rec["drill_out"] = subs
            if verbose:
                LOG(f"    [下钻] 无 PDF 附件，但含 {len(subs)} 条表格记录 -> 按中间汇总页展开")
        else:
            rec["error"] = "未找到 PDF 附件"
    return rec, files


# ---------------------------------------------------------------------------
# 下载
# ---------------------------------------------------------------------------
def _pdf_sanity(path):
    """已存在文件的完整性校验（跳过下载前必过）：非零字节 + 头部 %PDF 魔数 + 尾部含 %%EOF。
    任一不满足即判为损坏/截断 -> 返回 False，调用方会删除重下。任何 IO 异常也返回 False。"""
    try:
        size = os.path.getsize(path)
        if size <= 0:
            return False
        with open(path, "rb") as fh:
            head = fh.read(1024)
            if not head.lstrip(b"\xef\xbb\xbf\r\n\t ").startswith(b"%PDF-"):
                return False
            # 尾部 1KB 内应出现 %%EOF（容忍换行/尾部空白）
            try:
                fh.seek(max(0, size - 1024))
                tail = fh.read()
            except OSError:
                return False
        return b"%%EOF" in tail
    except OSError:
        return False


def download_one(session, item, dest_dir, filename=None, verbose=False):
    os.makedirs(dest_dir, exist_ok=True)
    url = item["fileUrl"]
    raw_name = filename or item["fileName"] or unquote(os.path.basename(urlparse(url).path))
    if not raw_name.lower().endswith(".pdf"):
        raw_name = safe_name(raw_name) + ".pdf"
    out_name = safe_name(raw_name)
    if not out_name.lower().endswith(".pdf"):
        out_name += ".pdf"

    path = os.path.join(dest_dir, out_name)
    if os.path.exists(path):
        if _pdf_sanity(path):
            try:
                return {"status": "已存在", "path": path, "size": os.path.getsize(path),
                        "url": url, "fileName": out_name}
            except OSError:
                pass
        else:
            # 已存在但零字节/损坏/截断 -> 删除后重下（坏文件必须重下）
            LOG(f"    [!损坏] {out_name} 已存在但校验不通过（零字节/非PDF/无EOF）-> 删除重下")
            try:
                os.remove(path)
            except OSError:
                pass

    tmp = path + ".part"
    try:
        t0 = time.time()
        r = session.get(url, timeout=90, stream=True)
        r.raise_for_status()
        total = int(r.headers.get("Content-Length") or 0)
        got = 0
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(65536):
                if chunk:
                    f.write(chunk)
                    got += len(chunk)
        if total and got != total:
            raise IOError(f"长度不符: 收到 {got} / 期望 {total}")
        os.replace(tmp, path)
        dt = time.time() - t0
        if verbose:
            LOG(f"    [下载] {out_name}  {got/1048576:.2f}MB  {dt:.1f}s")
        return {"status": "已下载", "path": path, "size": got, "url": url,
                "fileName": out_name, "seconds": round(dt, 2)}
    except (requests.RequestException, OSError, ValueError) as e:
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass
        return {"status": "失败", "path": path, "size": 0, "url": url,
                "fileName": out_name, "error": str(e)}


def plan_filename(rec, item, used):
    """
    给每个 PDF 定一个确定性的文件名。

    为什么要做：站点给的文件名可能很泛（"期末试题.pdf"），
    不同区/学校之间会撞名；多个线程同时写就互相覆盖了。
    所以在**主线程**里一次性算好，撞名就加序号 —— 不靠 exists() 竞争。
    """
    raw = item["fileName"] or unquote(os.path.basename(urlparse(item["fileUrl"]).path))
    stem = safe_name(re.sub(r"\.pdf$", "", raw, flags=re.I))

    # 文件名里如果没体现"区/学校"，就把上下文前缀补上
    prefix = ctx_prefix(rec)
    if prefix and not any(p and p in stem for p in prefix.split("_")[:1]):
        stem = f"{prefix}_{stem}"

    name = stem + ".pdf"
    if name in used:
        n = 2
        while f"{stem}_{n}.pdf" in used:
            n += 1
        name = f"{stem}_{n}.pdf"
    used.add(name)
    return name


# ---------------------------------------------------------------------------
# 清单
# ---------------------------------------------------------------------------
HEADERS = ["序号", "专题", "章节", "区/学校", "列", "链接文字", "详情页",
           "PDF文件名", "PDF直链", "状态", "大小(MB)", "本地路径", "备注"]

# 表头配色（十六进制色值：FFFFFF 白字 / 3598DB 蓝底，非词典词会被拼写检查误报）
# noinspection SpellCheckingInspection
HDR_FG, HDR_BG = "FFFFFF", "3598DB"


def write_manifest(rows, path):
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "资源清单"
    ws.append(HEADERS)
    hf = Font(bold=True, color=HDR_FG)
    fill = PatternFill("solid", fgColor=HDR_BG)
    for c in ws[1]:
        c.font = hf
        c.fill = fill
        c.alignment = Alignment(horizontal="center", vertical="center")

    for i, r in enumerate(rows, 1):
        ws.append([
            i, r.get("topic_name", ""), r.get("section", ""),
            " / ".join(r.get("group", [])), r.get("col", ""), r.get("text", ""),
            r.get("url", ""), r.get("fileName", ""), r.get("pdf_url", ""),
            r.get("status", ""), round(r.get("size", 0) / 1048576, 2),
            r.get("path", ""), r.get("error", "") or r.get("note", ""),
        ])

    widths = [6, 34, 40, 20, 9, 16, 44, 40, 60, 9, 10, 60, 30]
    for i, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(i)].width = w
    ws.freeze_panes = "A2"
    wb.save(path)
    return path


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def _targets_fingerprint(targets):
    """对目标页 URL 集合取 sha1[:16] 指纹（排序后拼接，不受发现顺序影响）。空集则返 ""。
    专题页目标增/删/换页都会导致 fp 变化 -> 下游判断是否需要重置 offset 并换 run_id。"""
    import hashlib
    urls = sorted({str(t.get("url") or "") for t in (targets or []) if t.get("url")})
    if not urls:
        return ""
    return hashlib.sha1("\n".join(urls).encode("utf-8", errors="replace")).hexdigest()[:16]


def _apply_offset_reset(offset, total_targets, expected_fp, actual_fp):
    """纯函数：判定是否应因历史偏移越界/目标集合变化而强制重置 offset。
    返回 (new_offset, reason, offset_before)；reason="" 代表无需重置。
      - expected_fp 已存但≠ actual_fp  -> reason='fingerprint_changed' （专题结构变化，新旧集合不同）
      - offset > total_targets (且 total>0)      -> reason='offset_out_of_range'   （历史偏移越过新目标总数）
      - offset == total_targets                   -> **不**重置（属“自然到末尾”，交给上层当正常结束处理）
    无任何集合信息（total=0 且 fp 缺失）时保守不重置，避免无目标时把真·空误判为重置。"""
    off_before = int(offset or 0)
    try:
        tot = int(total_targets or 0)
    except (TypeError, ValueError):
        tot = 0
    if off_before <= 0:
        return (off_before, "", off_before)
    if expected_fp and actual_fp and str(expected_fp) != str(actual_fp):
        return (0, "fingerprint_changed", off_before)
    if tot > 0 and off_before > tot:
        return (0, "offset_out_of_range", off_before)
    return (off_before, "", off_before)


def run(topic_keys, limit=0, dry_run=False, verbose=False, debug=False,
        workers=4, delay=0.35, custom_cfgs=None, db_path=None, batch_id=None,
        offset=0, expected_fp=None, paper_type=None):
    if ledgerio is not None and db_path:
        ledgerio.configure(db_path)     # 台账跟随调用方指定的库（压测库/生产库）
    if bc is not None and db_path:
        bc.configure(db_path)           # 批次状态表跟随同一库
    session = make_session()
    paper_terms = _build_paper_terms(paper_type)
    if paper_terms:
        LOG(f"[试卷类型] paper_type={paper_type!r} -> 归一化词表 {paper_terms}")
    all_rows, all_targets = [], []

    # topic key -> 配置。自定义 --url 的配置与预设 TOPICS 合并，统一查询。
    cfg_map = dict(TOPICS)
    for c in (custom_cfgs or []):
        cfg_map[c["key"]] = c

    for tk in topic_keys:
        all_targets.extend(collect_targets(session, tk, verbose,
                                           cfg=cfg_map.get(tk),
                                           paper_terms=paper_terms))

    # 切片前捕获目标总数（供分批流水线判定结束条件 offset>=总目标；不受 --offset/--limit 影响）。
    total_targets = len(all_targets)

    # 目标集合指纹 + 历史偏移越界/指纹变化 → 自动重置 offset=0（修历史坑：专题页目标变化后旧 offset
    # 越过新 total，曾见 offset=168 > 新 total=165 → 切片空 → rc=2 空跑不产出 zip）。
    target_fp = _targets_fingerprint(all_targets)
    offset_before = int(offset or 0)
    offset, offset_reset_reason, offset_before = _apply_offset_reset(
        offset, total_targets, expected_fp, target_fp)
    if offset_reset_reason:
        LOG(f">>> [偏移重置] 原因={offset_reset_reason}, "
            f"offset_before={offset_before}, total_targets={total_targets}, "
            f"expected_fp={expected_fp or '-'}, actual_fp={target_fp or '-'}, offset_after={offset}")

    if offset:
        all_targets = all_targets[offset:]
        LOG(f"\n[偏移] 跳过前 {offset} 个目标（--offset，分批流水线用）")
    if limit:
        all_targets = all_targets[:limit]
        if offset:
            LOG(f"[限制] 本批处理第 {offset + 1}~{offset + len(all_targets)} 个目标"
                f"（共 {len(all_targets)} 个）")
        else:
            LOG(f"\n[限制] 本次只处理前 {len(all_targets)} 个目标")

    if dry_run:
        LOG(f"\n[干跑] 共 {len(all_targets)} 个目标详情页，未发起详情页请求。")
        out = LIST_DIR or os.path.join(OUT_ROOT, "清单")
        os.makedirs(out, exist_ok=True)
        p = write_manifest([dict(t, topic_name=cfg_map[t["topic"]]["name"])
                            for t in all_targets],
                           os.path.join(out, f"专题清单_仅目录_{datetime.now():%Y%m%d_%H%M}.xlsx"))
        LOG(f"[清单] {p}")
        result = {"batch_id": batch_id or "", "target_pages": len(all_targets),
                  "total_targets": total_targets,
                  "pdf_count": 0, "downloaded": 0, "skipped": 0, "failed": 0,
                  "offset": offset, "dry_run": True, "empty": len(all_targets) == 0,
                  "target_fp": target_fp, "offset_before": offset_before,
                  "offset_reset_reason": offset_reset_reason}
        LOG("RESULT" + json.dumps(result, ensure_ascii=False))
        return [], result

    # ---- 阶段一：并发解析目标页，拿到 PDF 直链 ----
    # 站点两种形态都要能采：
    #   两层：专题页 ->（表格）-> 详情页（挂 PDF 附件）      <- 旧专题
    #   三层：总汇总页 -> 年级汇总页 ->（表格）-> 试卷详情页  <- 147414 这类
    # 逐层解析：含附件 = 详情页，直接收下；无附件但仍有表格 = 中间汇总页，
    # 展开成下一层继续抓，直到拿到 PDF 或到下钻上限（MAX_DRILL_DEPTH）。
    # 历史坑：只认两层时把中间汇总页当详情页抓 -> 0 附件 -> 下载空转、全流程中止。
    LOG(f"\n=== [解析] 并发抓取 {len(all_targets)} 个详情页（线程 {workers}）")
    resolved = []
    frontier = list(all_targets)
    visited = {t["url"] for t in frontier}
    layer = 0
    while frontier and layer < MAX_DRILL_DEPTH:
        layer += 1
        if layer > 1:
            LOG(f"    --- [下钻] 第 {layer} 层：{len(frontier)} 个中间汇总页")
        next_frontier, done = [], 0
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures_map = {ex.submit(resolve_pdf, session, t, verbose): t for t in frontier}
            for fu in as_completed(futures_map):
                rec, files = fu.result()
                done += 1
                if files:
                    resolved.append((rec, files))
                elif rec.get("drill_out"):
                    # 中间汇总页 -> 子记录继承主题；section 缺省时用父页标题，
                    # 多层站点据此按「年级汇总页」分子目录归档。
                    for s in rec["drill_out"]:
                        s["topic"] = s.get("topic") or rec.get("topic") or ""
                        s["url"] = abs_url(s["url"])
                        if not s.get("section"):
                            s["section"] = clean_site_title(rec.get("detail_title") or "")
                    next_frontier.extend(rec["drill_out"])
                else:
                    resolved.append((rec, []))   # 真无附件，清单里体现
                if done % 20 == 0 or done == len(frontier):
                    LOG(f"    进度 {done}/{len(frontier)}")
                time.sleep(delay / workers)
        # 跨层去重（子页面可能回链父级/兄弟级）+ limit 按「最终目标数」裁剪
        nxt = []
        for s in next_frontier:
            u = s.get("url") or ""
            if not u or u in visited:
                continue
            visited.add(u)
            nxt.append(s)
        if limit:
            room = max(0, limit - len(resolved))
            if len(nxt) > room:
                nxt = nxt[:room]
        frontier = nxt
    if frontier:
        LOG(f"    [下钻] 已达最大层数 {MAX_DRILL_DEPTH}，剩余 {len(frontier)} 个页面按「无附件」记录")
        for t in frontier:
            resolved.append((t, []))

    if debug:
        LOG("\n[断点] 详情页解析完成，进入 pdb；可用变量：resolved / all_targets")
        import pdb
        pdb.set_trace()

    # ---- 阶段二：下载 ----
    LOG(f"\n=== [下载]")
    # 同一个 PDF 直链可能被多条记录引用（站点把链接复用/贴错），只下一次
    used_names, jobs, url_job = set(), [], {}
    job_names = {}
    for rec, files in resolved:
        if not files:
            all_rows.append(dict(rec, status="无附件", size=0))
            continue
        sub = safe_name(rec.get("section", "") or "未分章节")
        dest = os.path.join(OUT_ROOT, cfg_map[rec["topic"]]["name"], sub)
        for f in files:
            u = f["fileUrl"]
            if u in url_job:
                # 复用已计划的结果；上下文记在清单里以便溯源
                continue
            per_dir_used = used_names  # 全局去重，同一目录内也不会撞
            name = plan_filename(rec, f, per_dir_used)
            url_job[u] = len(jobs)
            jobs.append((rec, f, dest, name))
            job_names[u] = name

    LOG(f"    待下载 PDF: {len(jobs)} 个（表格引用 {sum(len(f) for _, f in resolved)} 条）")

    # 批次登记：为每个待下载 PDF 分配 file_id 并写入 manifest；
    # 同批写 file_steps 的 download=started（每个状态变化都落库）。
    fid_of = {}
    if bc is not None and batch_id:
        entries = [{"path": os.path.join(d, nm), "name": nm}
                   for _rec, _f, d, nm in jobs]
        for _r in bc.register_files(batch_id, entries):
            fid_of[bc.norm_key(_r["path"])] = _r["file_id"]
        for _rec, _f, d, nm in jobs:
            _fp = os.path.join(d, nm)
            _fid = fid_of.get(bc.norm_key(_fp))
            if _fid:
                bc.db_step(batch_id, _fid, bc.STEP_DOWNLOAD, bc.ST_STARTED, dst=_fp)

    ok = fail = skip = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures_map = {ex.submit(download_one, session, f, d, nm, verbose): (rec, f, d, nm)
                       for rec, f, d, nm in jobs}
        for i, fu in enumerate(as_completed(futures_map), 1):
            rec, f, d, nm = futures_map[fu]
            res = fu.result()
            if res["status"] == "已下载":
                ok += 1
            elif res["status"] == "已存在":
                skip += 1
            else:
                fail += 1
            # 台账：每个 PDF 的落地路径与状态（dry-run 不进此分支；
            # 「已存在」= 本轮未发生实际动作，不落账，避免重复噪声）
            if ledgerio is not None and res["status"] != "已存在":
                _note = str(res["status"])
                if res.get("url"):
                    _note += f" | {res['url']}"
                if res.get("error"):
                    _note += f" | {res['error']}"
                ledgerio.log("download",
                             "DONE" if res["status"] == "已下载" else "FAIL",
                             dst_path=res.get("path") or None,
                             size_bytes=res.get("size") or None,
                             note=_note)
            # 批次状态：download 终态落 file_steps（官方动作：ok / 已存在 skip / fail）
            if bc is not None and batch_id:
                _fp = os.path.join(d, nm)
                _fid = fid_of.get(bc.norm_key(_fp))
                if _fid:
                    _st = {"已下载": bc.ST_OK, "已存在": bc.ST_SKIP}.get(
                        res["status"], bc.ST_FAIL)
                    _err = (str(res.get("error") or res["status"])[:200]
                            if _st == bc.ST_FAIL else "")
                    # ok 时记录下载产物内容哈希：下游三条件跳过的缓存键 + 导出 SHA 列
                    _sha = ""
                    if _st == bc.ST_OK:
                        try:
                            import hashlib
                            with open(res.get("path") or _fp, "rb") as _fh:
                                _sha = hashlib.sha256(_fh.read()).hexdigest()
                        except OSError:
                            _sha = ""
                    bc.db_step(batch_id, _fid, bc.STEP_DOWNLOAD, _st,
                               dst=(res.get("path") or _fp), error=_err,
                               src_sha=_sha or None)
            all_rows.append({
                "topic_name": cfg_map[rec["topic"]]["name"],
                "section": rec.get("section", ""),
                "group": rec.get("group", []),
                "col": rec.get("col", ""),
                "text": rec.get("text", ""),
                "url": rec.get("url", ""),
                "fileName": res.get("fileName") or nm,
                "pdf_url": f["fileUrl"],
                "status": res["status"],
                "size": res.get("size", 0),
                "path": res.get("path", ""),
                "error": res.get("error", ""),
            })
            if i % 20 == 0 or i == len(jobs):
                LOG(f"    进度 {i}/{len(jobs)}  (成功 {ok} / 跳过 {skip} / 失败 {fail})")
            time.sleep(delay / workers)

    # ---- 阶段三：写清单 + 批次统计 + RESULT（供 pipeline 判定空结果）----
    out = LIST_DIR or os.path.join(OUT_ROOT, "清单")
    os.makedirs(out, exist_ok=True)
    p = write_manifest(all_rows,
                       os.path.join(out, f"资源清单_下载状态_{datetime.now():%Y%m%d_%H%M}.xlsx"))
    LOG(f"\n=== [完成] 下载 {ok} / 跳过 {skip} / 失败 {fail}")
    LOG(f"[清单] {p}")

    result = {"batch_id": batch_id or "",
              "target_pages": len(all_targets),
              "total_targets": total_targets,
              "pdf_count": len(jobs),
              "downloaded": ok, "skipped": skip, "failed": fail,
              "offset": offset,
              "dry_run": False,
              "target_fp": target_fp, "offset_before": offset_before,
              "offset_reset_reason": offset_reset_reason,
              "empty": (len(all_targets) == 0 or len(jobs) == 0)}
    if bc is not None and batch_id:
        bc.set_download_summary(batch_id, result)
        bc.set_batch_status(batch_id, "empty" if result["empty"] else "running")
    LOG("RESULT" + json.dumps(result, ensure_ascii=False))

    if debug:
        LOG("\n[断点] 全部流程结束，进入 pdb；可用变量：all_rows / jobs")
        import pdb
        pdb.set_trace()
    return all_rows, result


def main():
    ap = argparse.ArgumentParser(description="gaokzx.com 专题资源采集器")
    ap.add_argument("--topic", default="all", help="1 / 2 / all")
    ap.add_argument("--url", default=None,
                    help="直接指定专题页 URL（替代 --topic）。"
                         "专题名自动取页面 <title>；可配合 --name 覆盖")
    ap.add_argument("--name", default=None,
                    help="配合 --url：显式指定专题目录名")
    ap.add_argument("--limit", type=int, default=0,
                    help="只处理前 N 个最终目标（多层汇总按下钻后的详情页计）")
    ap.add_argument("--offset", type=int, default=0,
                    help="跳过前 N 个最终目标（分批流水线：--offset 24 --limit 24 = 第 2 批）")
    ap.add_argument("--expected-fp", default=None,
                    help="目标页 URL 集合的 sha1[:16] 指纹（游标上次落库值）；与本次实际集合不一致时，"
                         "下载器会自动将 offset 重置为 0 并在 RESULT 里上报 offset_reset_reason=fingerprint_changed")
    ap.add_argument("--dry-run", action="store_true", help="只出清单不下载")
    ap.add_argument("-v", "--verbose", action="store_true", help="打印中间变量")
    ap.add_argument("--debug", action="store_true", help="关键位置进入 pdb 断点")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--delay", type=float, default=0.35, help="每个请求基础延迟(秒)")
    ap.add_argument("--paper-type", default=None,
                    help="只采集指定试卷类型（如 北京卷 / 北京；可逗号分隔多值）。"
                         "缺省=不筛选。匹配基于表格『试卷类型』列与详情页标题，"
                         "已做罗马数字/全角/大小写归一，并做别名展开（北京卷→北京）")
    ap.add_argument("--db-path", default=None,
                    help="台账库路径（默认生产库 pipeline.db；pipeline.py 自动传）")
    ap.add_argument("--batch-id", default=None,
                    help="批次号（pipeline.py 传入）；缺省时自动创建独立批次")
    ap.add_argument("--out-root", default=None,
                    help="PDF 落盘根目录（默认 D:\\work\\文库资源）；--test-root 隔离时 pipeline 传入隔离目录，避免下载污染生产")
    ap.add_argument("--list-dir", default=None,
                    help="清单 xlsx 输出目录（默认 <out-root>\\清单）；隔离时随 out-root 一起落隔离目录")
    args = ap.parse_args()

    # 隔离支持：--out-root/--list-dir 覆盖模块级硬编码路径（不传则保持原生产行为）。
    global OUT_ROOT, LIST_DIR
    if args.out_root:
        OUT_ROOT = str(args.out_root).strip()
    if args.list_dir:
        LIST_DIR = str(args.list_dir).strip()

    custom = None
    if args.url:
        cfg = topic_from_url(args.url, args.name)
        custom = [cfg]
        keys = [cfg["key"]]
        LOG(f"[URL] 自定义专题: {cfg['name']}")
        LOG(f"[URL] 专题页   : {cfg['url']}")
    else:
        keys = list(TOPICS) if args.topic == "all" else [args.topic]
        for k in keys:
            if k not in TOPICS:
                LOG(f"未知专题: {k}"); sys.exit(1)

    # ---- 批次初始化：调用方（pipeline）传 --batch-id；独立运行时自建 ----
    bid = args.batch_id
    _bname = cfg["name"] if (args.url and custom) else args.topic
    if bc is not None and bid and bc.load_manifest(bid) is None:
        bc.db_batch(bid, url=args.url or "", name=_bname)
    if bc is not None and not bid and not args.dry_run:
        _mf = bc.new_batch(url=args.url or "", name=_bname,
                           key=(cfg["key"] if (args.url and custom) else args.topic))
        bid = (_mf or {}).get("batch_id")
    if bid:
        LOG(f"[批次] batch_id = {bid}")

    try:
        _rows, result = run(keys, args.limit, args.dry_run, args.verbose, args.debug,
                            args.workers, args.delay, custom_cfgs=custom,
                            db_path=args.db_path, batch_id=bid,
                            offset=max(0, int(args.offset or 0)),
                            expected_fp=(args.expected_fp or None),
                            paper_type=(args.paper_type or None))
    except KeyboardInterrupt:
        LOG("\n[中断] 用户终止")
        sys.exit(1)
    # 顶层兜底：无法穷举的异常统一打印调用栈后以失败码退出
    # noinspection PyBroadException
    except Exception:
        traceback.print_exc()
        sys.exit(1)

    # 空结果保护（硬性）：解析到 0 条目标详情页、0 个待下载 PDF 时，
    # 不得继续后续流程 —— 以退出码 2 通知调用方（pipeline 会立即中止全流程）。
    if result and result.get("empty") and not result.get("dry_run"):
        LOG(f">>> [空结果] 未发现可下载 PDF"
            f"（target_pages={result['target_pages']}, pdf_count={result['pdf_count']}），"
            f"退出码 2")
        sys.exit(2)


if __name__ == "__main__":
    main()
