# -*- coding: utf-8 -*-
"""
底边广告清理器（写入！仅在用户明确确认后运行）
================================================
基于 scan_bottom_ads.py 的报告执行清理。**默认输出到新目录，绝不原地覆盖。**

清理动作：
  1) 纯广告页      -> 删除该页（PyMuPDF delete_page）
  2) 文本关键词    -> 对底部带内命中的 span 做 redact（白底填充 + 消除文字层）
  3) 二维码        -> 按报告 qr_rect（可解码确认的精确坐标）做白底 redact
  4) 隐形水印字    -> 删除水印文字对象（视觉无损，消除搜索命中），删后复检残留

v2 修改（依据真实事故复盘，2026-09-19）：
  A) 二维码从「空实现」（只计数不处理）改为**按精确坐标 redact**：只认
     scan v3 的「可解码确认」条目（带 qr_rect），疑似/旧报告条目一律跳过
     —— 旧报告里的二维码命中实测几乎全是形态学误报（汉字「共/页」、
     图形框、线路图），盲盖会把白块糊上正文。
  B) 隐形水印摘除加**删后自检**：remove_invisible_spans 返回
     (删除数, 残留数)，残留 >0 计入汇总告警（历史出现过单行式
     q/...TJ...Q 漏网），需人工复核。

用法：
    # 预览（只打印将要做的改动，不写文件）
    python clean_bottom_ads.py --report <报告.json> --dry-run

    # 真正执行，输出到新目录
    python clean_bottom_ads.py --report <报告.json> --out-root "D:\\work\\文库资源_已清广告" --apply

    # 只清理指定类型
    python clean_bottom_ads.py --report <r.json> --apply --types 隐形水印字,纯广告页
"""
import argparse
import binascii
import json
import os
import re
import shutil
import sys
import time
from datetime import datetime

import pymupdf as fitz

# 状态看板写入（D:\work\_status\statusio.py）——只加调用、失败静默
sys.path.insert(0, r"D:\work\_status")
try:
    import statusio
except Exception:
    statusio = None

# 文件处理台账（D:\work\_status\ledgerio.py）——阶段 6 产物的落点/状态落库；
# 纯标准库、失败静默，绝不影响清理。默认生产库，--db-path 可切换。
try:
    import ledgerio
except Exception:
    ledgerio = None

sys.stdout.reconfigure(errors="replace")

# 批次核心（同目录）——file_steps 状态落库 + manifest 路径更新；失败静默。
try:
    import batchcore as bcore
except Exception:
    bcore = None

DEFAULT_OUT = r"D:\work\文库资源_已清广告"
SRC_ROOT = r"D:\work\文库资源_去水印后"


def _tz_epoch(s):
    """ISO 时间串 -> epoch 秒（无时区视为本机）。解析失败返回 None。"""
    try:
        d = datetime.fromisoformat(str(s).strip())
    except (ValueError, TypeError):
        return None
    if d.tzinfo is None:
        d = d.astimezone()
    return d.timestamp()


def rel_of(path, roots):
    for r in roots:
        try:
            rel = os.path.relpath(path, r)
            if not rel.startswith(".."):
                return rel
        except Exception:
            continue
    return os.path.basename(path)


def redact_bottom_band(page, band, keywords, color=(1, 1, 1)):
    """对底部带内命中关键词的 span 做 redact。返回命中数量。"""
    h = page.rect.height
    y0 = h * band
    n = 0
    try:
        dct = page.get_text("dict")
    except Exception:
        return 0
    for b in dct.get("blocks", []):
        for l in b.get("lines", []):
            for s in l.get("spans", []):
                txt = s.get("text") or ""
                if not txt.strip():
                    continue
                try:
                    x0, sy0, x1, sy1 = s["bbox"]
                except Exception:
                    continue
                if sy0 < y0:          # 不在底部带
                    continue
                if any(k in txt for k in keywords):
                    rect = fitz.Rect(x0 - 1, sy0 - 1, x1 + 1, sy1 + 1)
                    page.add_redact_annot(rect, fill=color)
                    n += 1
    return n


# ---------------------------------------------------------------
# 隐形水印文字层：判定规则 + 摘除实现（经实测校准，2026-09-17）
# ---------------------------------------------------------------
# 重要背景（踩过的坑，务必先读）：
#
# 1) 去水印产物 = 「整页栅格图」+「一层全覆盖的隐形文字层」，且这层文字
#    全部是 alpha≈3 / color=16777215（纯白）—— **包括正常正文**。
#    所以「按 alpha/颜色/字号 筛隐形字」会把整篇文字层一起删掉，等于
#    摧毁 OCR 成果与全文搜索能力。报告里的"隐形水印字 267 处"严重低估
#    了这个问题（它只采样到带关键词的 span）。
#    → 必须按「文本内容」判定，而不是按透明度/字号。
#
# 2) 用 page.add_redact_annot() + apply_redactions() 摘除是**不安全**的：
#    实测会重绘所在文本块，导致同块内的正文 span bbox 漂移/合并，出现
#    正文丢字（如地理图轴标签『水平距离/米』、英语答案『26. A 27. C』）。
#
# 3) 正确做法：**直接改内容流**（content stream），按 `q ... Q` 块粒度
#    整块删除水印。理由：该 PDF 的每个 `q/BT/.../ET/Q` 块恰好就是一段
#    文字，块边界与 span 一致；删块不影响任何其它块。
#    实测（延庆英语）：删 32 水印块，水印残留 0，正文差异 0 页；
#    像素差异仅出现在水印自身 bbox 内（因水印有 ca=.01 的微弱可见度），
#    与正文区域零重叠 —— 即差异全部来自"水印被移除"本身。
#
# ⚠ 另一个坑：一个页面的内容由**多个 stream 拼接**而成
#    （page.get_contents() 可能返回 35 个 xref），必须逐个处理全部
#    stream，只处理第一个会漏掉所有文字。
#
# 真水印的文本特征（经全库枚举 + 字号分布实测）：
#   A) 含微信号串 bjgkzx / bigkzx / gaokzx / bjgkz / bj8kzx
#   B) 含「在线」且同时含站名变体词根（高考/京考/惊高/亭考/离考/写考…）
#   C) 含「在线」且字号 >= 18pt  —— 兜住被切成残片的『在线』
#      （实测：正文集中在 0-16pt；18pt 以上仅 185 个 span，其中含"在线"的
#       全部是水印。而试卷标题如『参考答案』『数 学』『高三英语』虽也是
#        大字，但不含"在线"，因此不会被 C 命中 —— 这是不敢放宽到
#        "大字即水印"的原因。）
#   D) 含站名词根 且字号 >= 18pt —— 兜住丢失"在线"字样的水印残片（实测
#        漏网样本『北京京考在』25pt，不含"在线"，A/B/C 全不命中）。
#        正文中的『北京考生』等词同样含词根，故必须绑定大字号；正文实测
#        最大 16pt、水印残片 25pt —— 两者可安全区分。
# 误伤排除：
#   · 几何题『点P在线段AC上』—— 含"在线"但不含站名词根、且字号 8-11pt
#     → A/B/C 都不命中 → 安全保留。
#   · 试卷大标题『参考答案』『化学』『地理』—— 不含"在线" → 安全保留。
_WX_RE = re.compile(r"(bjgkzx|bigkzx|gaokzx|bjgkz|bj8kzx)", re.I)
_SITE_ROOTS = ("高考", "京考", "惊高", "亭考", "离考", "写考", "老在", "请在",
               "完在", "价高", "帝高", "考在", "速高", "北惊", "上惊", "上京",
               "停考", "在城", "在石", "大考在线")
_BIG_SIZE = 18.0          # 水印字号下限（正文实测最大 16pt）

# 实测两种 q 块书写格式：
#   ① `q\n ... \nQ`（多行式，恢复页/cleaned 来源的流）
#   ② `q/xxx gs ... BT ... [<..>]TJ ET Q`（单行空格分隔式——原 ads 流的
#      格式；实测漏网样本『北京京考在』(p9 xref=33) 就在这种块内，
#      旧正则 q\s*\n 整块均不匹配）。
# Q 后需跟空白、/ 或流末，避免误吞 `/Q1` 之类的名称。
_Q_BLOCK_RE = re.compile(r"\bq[\s/](.*?)\sQ(?=[\s/]|$)", re.S)
_TJ_RE = re.compile(r"\[<([0-9A-Fa-f]+)>\]TJ")
_SIZE_RE = re.compile(r"/(\S+)\s+([\d.]+)\s+Tf")


def is_watermark_text(txt, size=None):
    """判断一段文字层文本是否为站点水印。

    size 为可选字号；传入后可启用"含在线且大字"规则（C），
    用于兜住被切成残片的『在线』。
    """
    t = (txt or "").strip()
    if not t:
        return False
    if _WX_RE.search(t):
        return True
    if "在线" in t:
        if any(k in t for k in _SITE_ROOTS):
            return True
        if size is not None and size >= _BIG_SIZE:
            return True
    # D) 站名词根 + 大字号：兜住丢失"在线"的残片（如『北京京考在』25pt）
    if size is not None and size >= _BIG_SIZE and any(k in t for k in _SITE_ROOTS):
        return True
    return False


def _hex_to_text(h):
    """PDF hex string（2 字节/字符）→ 文本。"""
    try:
        b = binascii.unhexlify(h)
        if len(b) % 2:
            b += b"\x00"
        return b.decode("utf-16-be", "replace")
    except Exception:
        return ""


def strip_watermark_blocks(raw):
    """从一段内容流里删掉水印所在的 `q...Q` 块。返回 (新字节, 删除块数)。

    非水印块**字节级原样保留**，因此对正文零影响。
    """
    try:
        txt = raw.decode("latin-1")
    except Exception:
        return raw, 0
    parts, last, removed = [], 0, 0
    for m in _Q_BLOCK_RE.finditer(txt):
        block = m.group(0)
        tj = _TJ_RE.search(block)
        if not tj:
            continue
        # 取块内字号（Tf 的第二个操作数），用于规则 C
        size = None
        sm = _SIZE_RE.search(block)
        if sm:
            try:
                size = float(sm.group(2))
            except Exception:
                size = None
        if is_watermark_text(_hex_to_text(tj.group(1)), size):
            parts.append(txt[last:m.start()])
            last = m.end()
            removed += 1
    if not removed:
        return raw, 0
    parts.append(txt[last:])
    return "".join(parts).encode("latin-1"), removed


def _count_wm_blocks(raw):
    """统计一段内容流里仍命中的水印块数（复刻 strip_watermark_blocks 判定）。"""
    try:
        txt = raw.decode("latin-1")
    except Exception:
        return 0
    n = 0
    for m in _Q_BLOCK_RE.finditer(txt):
        block = m.group(0)
        tj = _TJ_RE.search(block)
        if not tj:
            continue
        size = None
        sm = _SIZE_RE.search(block)
        if sm:
            try:
                size = float(sm.group(2))
            except Exception:
                size = None
        if is_watermark_text(_hex_to_text(tj.group(1)), size):
            n += 1
    return n


def remove_invisible_spans(doc, page_index):
    """摘除指定页的文字层水印。返回 (删除块数, 删后残留块数)。

    ⚠ 签名与旧版不同（现在需要 doc 才能改 stream）；调用处已同步更新。
    必须遍历 page.get_contents() 返回的**全部** stream，不能只取第一个。

    删后自检：以流的**实际内容**复检（update_stream 失败则用原字节），
    残留 >0 说明有漏网格式或写入失败，调用方计入 stats["invis_left"]
    供人工复核告警。
    """
    try:
        page = doc[page_index]
        xrefs = page.get_contents()
    except Exception:
        return 0, 0
    n = left = 0
    for xr in xrefs or []:
        try:
            raw = doc.xref_stream(xr)
        except Exception:
            continue
        if not raw:
            continue
        new_raw, rm = strip_watermark_blocks(raw)
        cur = raw
        if rm:
            try:
                doc.update_stream(xr, new_raw)
                n += rm
                cur = new_raw
            except Exception:
                pass
        # 删后复检（以流的实际内容统计）：>0 即仍有漏网水印块
        left += _count_wm_blocks(cur)
    return n, left


def process_one(src, dst, plan, band=0.85, do_types=None, inplace=False):
    """处理单个文件。plan = 该文件的 findings 列表。返回处理摘要 dict。

    inplace=True 时，dst 应等于 src，写完后原子替换原文件（备份由调用方负责）。
    """
    do_types = do_types or {"纯广告页", "文本关键词", "二维码", "隐形水印字"}

    ad_pages = sorted({x["page"] for x in plan if x["type"] == "纯广告页"}, reverse=True)
    kw_pages = {x["page"] for x in plan if x["type"] == "文本关键词"}
    invis_pages = {x["page"] for x in plan if x["type"] == "隐形水印字"}

    # 二维码（v2）：只处理 scan v3「可解码确认」且带 qr_rect 的条目。
    # 疑似（未解码）/旧版报告条目（无 confirmed 字段）一律跳过 ——
    # 旧报告命中实测几乎全是形态学误报（汉字/图形框），盲盖会白块糊页。
    qr_rects = {}
    qr_suspected = 0
    qr_norect = 0
    for x in plan:
        if x["type"] != "二维码":
            continue
        _r = x.get("qr_rect")
        if x.get("confirmed") and isinstance(_r, (list, tuple)) and len(_r) == 4:
            qr_rects.setdefault(x["page"], []).append(tuple(float(v) for v in _r))
        elif x.get("confirmed"):
            qr_norect += 1
        else:
            qr_suspected += 1

    stats = {"del_pages": 0, "redact_kw": 0, "redact_qr": 0, "invis": 0,
             "invis_left": 0, "qr_suspected": qr_suspected,
             "qr_norect": qr_norect}

    doc = fitz.open(src)
    try:
        n0 = doc.page_count

        # 1) 隐形水印字：摘除文字层里的站点水印块（改内容流，不重绘页面）
        #    ⚠ 必须扫全部页，不能只用报告里的 invis_pages ——
        #    实测水印遍布全书（如某文件 19 页全有），报告只标了采样页。
        if "隐形水印字" in do_types:
            for i in range(doc.page_count):
                _rm, _left = remove_invisible_spans(doc, i)
                stats["invis"] += _rm
                stats["invis_left"] += _left

        # 2) 文本关键词 / 二维码：底部带 redact
        if "文本关键词" in do_types or "二维码" in do_types:
            KWS = ["关注", "扫码", "微信", "公众号", "gaokzx", "北京高考在线",
                   "客服", "咨询", "免费领取", "http", "www.", "QQ群"]
            for i in range(doc.page_count):
                if (i + 1) in kw_pages and "文本关键词" in do_types:
                    stats["redact_kw"] += redact_bottom_band(doc[i], band, KWS)
                if "二维码" in do_types and (i + 1) in qr_rects:
                    # v2：按 scan 报告 qr_rect（页面坐标，仅可解码确认条目）
                    # 精确白盖。旧版这里没有 redact（只计数），且经实测旧
                    # 报告条目多为形态学误报（汉字/图形框），故无
                    # confirmed/qr_rect 的条目一律跳过。
                    for (_qx0, _qy0, _qx1, _qy1) in qr_rects[i + 1]:
                        try:
                            rr = fitz.Rect(_qx0 - 2, _qy0 - 2,
                                           _qx1 + 2, _qy1 + 2) & doc[i].rect
                            if rr.is_empty or rr.width < 2 or rr.height < 2:
                                continue
                            doc[i].add_redact_annot(rr, fill=(1, 1, 1))
                            stats["redact_qr"] += 1
                        except Exception:
                            pass

        # 3) 应用剩余 redact（关键词/二维码的标注）
        #    隐形水印那批已在 remove_invisible_spans 内部 apply 过了，
        #    这里只兜底处理本节新增的 annot；重复 apply 是幂等的。
        if "文本关键词" in do_types or "二维码" in do_types:
            for i in range(doc.page_count):
                try:
                    doc[i].apply_redactions(images=fitz.PDF_REDACT_IMAGE_NONE)
                except TypeError:
                    try:
                        doc[i].apply_redactions()
                    except Exception:
                        pass
                except Exception:
                    pass

        # 4) 删除纯广告页（从后往前，避免索引漂移）
        if "纯广告页" in do_types:
            for pg in ad_pages:
                idx = pg - 1
                if 0 <= idx < doc.page_count:
                    doc.delete_page(idx)
                    stats["del_pages"] += 1

        os.makedirs(os.path.dirname(dst), exist_ok=True)
        if inplace:
            # 原地安全替换：先写临时文件，校验通过后再原子替换原文件。
            # 备份由 main() 在调用前统一做（--backup-dir）。
            tmp = dst + ".cleaning.tmp"
            doc.save(tmp, garbage=4, deflate=True, clean=True)
        else:
            doc.save(dst, garbage=4, deflate=True, clean=True)
    finally:
        doc.close()

    if inplace:
        tmp = dst + ".cleaning.tmp"
        try:
            # os.replace 是原子替换（覆盖已存在的目标），Windows 上也可用。
            # 不用 shutil.move：目标已存在时它会走"复制到目录内"的分支而出错。
            os.replace(tmp, dst)
        except Exception as e:
            # 替换失败：清理临时文件，并把异常抛给调用方记录 [ERR]
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass
            raise RuntimeError(f"原地替换失败（原文件未改动）: {e}")

    stats["pages_before"] = n0
    stats["pages_after"] = n0 - stats["del_pages"]
    return stats


def _sha256_file(path):
    """文件内容 sha256（十六进制）；读失败返回 None。ads 三条件跳过用（输入哈希登记/比对）。"""
    import hashlib
    try:
        hs = hashlib.sha256()
        with open(str(path), "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                hs.update(chunk)
        return hs.hexdigest()
    except OSError:
        return None


def main():
    ap = argparse.ArgumentParser(description="底边广告清理器（写入模式）")
    ap.add_argument("--report", required=True, help="scan_bottom_ads.py 产出的 JSON 报告")
    ap.add_argument("--out-root", default=DEFAULT_OUT, help="输出根目录（默认新建 文库资源_已清广告）")
    ap.add_argument("--band", type=float, default=0.85)
    ap.add_argument("--types", default="", help="只清理这些类型（逗号分隔）；默认全部")
    ap.add_argument("--only-subdir", default=None, help="只处理路径含该子串的文件")
    ap.add_argument("--limit", type=int, default=None, help="只处理前 N 个（小批量实测用）")
    ap.add_argument("--inplace", action="store_true",
                    help="原地修改生产文件（默认写到 --out-root 新目录）。"
                         "强烈建议配合 --backup-dir 使用。")
    ap.add_argument("--backup-dir", default=None,
                    help="--inplace 时，先把原文件复制一份到此目录再改（强烈建议）")
    ap.add_argument("--dry-run", action="store_true", help="只预览，不写文件")
    ap.add_argument("--apply", action="store_true", help="真正执行（必须显式指定）")
    ap.add_argument("--db-path", default=None,
                    help="台账库路径（默认生产库 pipeline.db；pipeline.py 自动传）")
    ap.add_argument("--batch-id", default=None,
                    help="批次号（生产流程必传）：只接受同批新报告，"
                         "报告缺失/过旧/batch_id 不匹配立即中止（退出码 3）")
    ap.add_argument("--fail_exit", action="store_true",
                    help="有失败文件时以退出码 4 结束（pipeline 据此判定清理未全成功）")
    ap.add_argument("--force", action="store_true",
                    help="忽略断点续传三条件（DB ok+产物存在+输入哈希未变），全部重跑")
    args = ap.parse_args()

    if ledgerio is not None and args.db_path:
        ledgerio.configure(args.db_path)
    if bcore is not None and args.db_path:
        bcore.configure(args.db_path)

    if not args.dry_run and not args.apply:
        print(">>> 安全保护：请显式指定 --dry-run（预览）或 --apply（执行）。")
        return

    do_types = set(t.strip() for t in args.types.split(",") if t.strip()) or \
               {"纯广告页", "文本关键词", "二维码", "隐形水印字"}

    bid = (args.batch_id or "").strip() or None

    # 新报告必检（batch_id 硬校验）：报告缺失 / 过旧 / batch_id 不匹配 -> 立即中止。
    # 生产流程（--batch-id）下绝不再读固定的旧报告（底边广告检查报告.json）。
    if not os.path.isfile(args.report):
        print(f">>> [中止] 扫描报告不存在：{args.report}")
        print(f"    请先运行： python scan_bottom_ads.py --batch-id {bid or '<批次>'}")
        sys.exit(3)

    with open(args.report, "r", encoding="utf-8") as f:
        rep = json.load(f)
    results = rep.get("results", {})

    if bid:
        _rep_bid = str(rep.get("batch_id") or "").strip()
        if _rep_bid != bid:
            print(f">>> [中止] 报告 batch_id 不匹配："
                  f"报告={_rep_bid or '(空)'}  当前批次={bid}")
            print(f"    请重新扫描： python scan_bottom_ads.py --batch-id {bid}")
            sys.exit(3)
        # 过旧判定：报告生成时间不得早于批次创建时间；mtime 超 48h 也视为过旧
        _m = _tz_epoch(str(rep.get("generated") or ""))
        _mf = bcore.load_manifest(bid) if bcore is not None else None
        _c = _tz_epoch(str((_mf or {}).get("created_at") or ""))
        if _m and _c and _m < _c:
            print(">>> [中止] 报告过旧（生成时间早于本批次创建时间），请重新扫描。")
            sys.exit(3)
        if _m and (time.time() - _m) > 48 * 3600:
            print(">>> [中止] 报告已超过 48 小时未更新，请重新扫描。")
            sys.exit(3)
    items = sorted(results.items())
    if args.only_subdir:
        items = [(p, v) for p, v in items if args.only_subdir in p]
    if args.limit:
        items = items[:args.limit]

    def _dst_of(p):
        _rel = rel_of(p, [SRC_ROOT, r"D:\work\文库资源"])
        return p if args.inplace else os.path.join(args.out_root, _rel)

    def _ads_cond_skip(p, dst):
        """ads 三条件断点跳过：该步 DB=ok 且 产物真实存在(>0字节) 且 sha256(输入)==登记值。
        老数据无哈希时补记并本轮放行（与 pipeline._skip_ok 同语义）。reconcile 回滚后
        状态不再是 ok -> 自然不跳过（被点名重跑）。"""
        if args.force or bcore is None or not bid:
            return False
        _fid = bcore.find_file_id(bid, p)
        if not _fid or not bcore.is_step_ok(bid, _fid, bcore.STEP_AD_CLEAN):
            return False
        try:
            if not (os.path.isfile(dst) and os.path.getsize(dst) > 0):
                return False
        except OSError:
            return False
        _stored = bcore.get_step_sha(bid, _fid, bcore.STEP_AD_CLEAN)
        _cur = _sha256_file(p)
        if _stored is None:
            if _cur:
                try: bcore.set_step_sha(bid, _fid, bcore.STEP_AD_CLEAN, _cur)
                except Exception: pass
            return True                       # 老数据补记后放行
        return _cur is not None and _cur == _stored

    if not args.force and bcore is not None and bid:
        _sk = 0
        _kept = []
        for _p, _v in items:
            if _ads_cond_skip(_p, _dst_of(_p)):
                _sk += 1
                continue
            _kept.append((_p, _v))
        items = _kept
        if _sk:
            print(f">>> [ads] 三条件断点跳过 {_sk} 个（DB ok + 产物存在 + 输入哈希未变；--force 可强跑）")

    # ---- 干净文件（本批次内、报告未命中的）也要进成品 ----
    # 否则「已清广告」只含曾命中并清理过的文件：打包会漏干净文件；
    # 全部干净时目录甚至不存在（打包源缺失，全流程无法完成）。
    clean_files = []
    if bid and bcore is not None and not args.inplace:
        try:
            _all, _seen = [], set()
            for _st in ("cleaned", "done"):
                for _fid, _p in bcore.scan_pdfs(_st, bid):
                    _k = bcore.norm_key(_p)
                    if _k in _seen:
                        continue
                    _seen.add(_k)
                    _all.append((_fid, _p))
            _hit = {bcore.norm_key(_p) for _p in results.keys()}
            clean_files = [(_fid, _p) for _fid, _p in _all
                           if bcore.norm_key(_p) not in _hit
                           and (not args.only_subdir or args.only_subdir in _p)]
            if not args.force:
                # 干净文件同样走三条件：已 ok+成品存在+输入未变 -> 不重复复制/不重复写库
                _cf_sk = 0
                _cf_kept = []
                for _fid, _p in clean_files:
                    if _ads_cond_skip(_p, _dst_of(_p)):
                        _cf_sk += 1
                        continue
                    _cf_kept.append((_fid, _p))
                clean_files = _cf_kept
                if _cf_sk:
                    print(f">>> [ads] 干净文件三条件跳过 {_cf_sk} 个（成品已存在且输入未变）")
        except Exception:
            clean_files = []

    print("=" * 72)
    print("底边广告清理" + ("（预览 dry-run，不写文件）" if args.dry_run else "（执行 apply）"))
    print("=" * 72)
    print(f"  报告文件 : {args.report}")
    print(f"  清理类型 : {', '.join(sorted(do_types))}")
    if args.inplace:
        print(f"  写入模式 : 原地修改（备份 -> {args.backup_dir or '未指定!'}）")
    else:
        print(f"  输出根目录: {args.out_root}")
    print(f"  待处理   : {len(items)} 个文件")
    print("=" * 72)

    if args.apply and args.inplace and not args.backup_dir:
        print(">>> 安全保护：--inplace 必须同时指定 --backup-dir（先备份再改）。")
        return
    if args.apply and args.inplace and args.backup_dir:
        os.makedirs(args.backup_dir, exist_ok=True)
        print(f">>> 备份目录已就绪：{args.backup_dir}")

    tot = {"files": 0, "del_pages": 0, "redact_kw": 0, "redact_qr": 0, "invis": 0,
           "invis_left": 0, "qr_suspected": 0, "backup": 0,
           "fail": 0, "copied": 0}
    # 批次状态：ad_clean started（dry-run 不落库——预览不算实际动作）
    fid_of = {}
    if bcore is not None and bid and not args.dry_run:
        for _p, _plan in items:
            _fid = bcore.find_file_id(bid, _p)
            if _fid:
                fid_of[_p] = _fid
                bcore.db_step(bid, _fid, bcore.STEP_AD_CLEAN, bcore.ST_STARTED, src=_p)
    if statusio:
        statusio.begin_step("ads", total=len(items), out_dir=args.out_root,
                            note=("dry-run" if args.dry_run else
                                  "原地修改" if args.inplace else "输出新目录"))
    for p, plan in items:
        if args.inplace:
            rel = rel_of(p, [SRC_ROOT, r"D:\work\文库资源"])
            dst = p
        else:
            rel = rel_of(p, [SRC_ROOT, r"D:\work\文库资源"])
            dst = os.path.join(args.out_root, rel)
        if args.dry_run:
            types = {}
            for x in plan:
                types[x["type"]] = types.get(x["type"], 0) + 1
            print(f"  [预览] {rel[:70]}")
            print(f"          {types}")
            if statusio:
                statusio.update_detail("ads", p, os.path.basename(p), "SKIP",
                                       note=f"dry-run 预览 {types}")
            tot["files"] += 1
            tot["del_pages"] += types.get("纯广告页", 0)
            continue
        try:
            # 原地模式：先备份
            if args.inplace and args.backup_dir:
                bdst = os.path.join(args.backup_dir, rel)
                os.makedirs(os.path.dirname(bdst), exist_ok=True)
                if not os.path.exists(bdst):
                    shutil.copy2(p, bdst)
                    tot["backup"] += 1
            st = process_one(p, dst, plan, args.band, do_types, inplace=args.inplace)
            tot["files"] += 1
            tot["del_pages"] += st["del_pages"]
            tot["redact_kw"] += st["redact_kw"]
            tot["redact_qr"] += st["redact_qr"]
            tot["invis"] += st["invis"]
            tot["invis_left"] += st.get("invis_left", 0)
            tot["qr_suspected"] += st.get("qr_suspected", 0)
            if ledgerio is not None:
                ledgerio.log("ads", "DONE", src_path=p, dst_path=dst,
                             pages=st["pages_after"],
                             note=f"删页{st['del_pages']} 遮盖{st['redact_kw']} "
                                  f"二维码{st['redact_qr']} 水印块{st['invis']} "
                                  f"（页 {st['pages_before']}->{st['pages_after']}）")
            if statusio:
                statusio.update_detail("ads", p, os.path.basename(p), "DONE",
                                       src_pages=st["pages_before"],
                                       dst_pages=st["pages_after"],
                                       note=f"删页{st['del_pages']} 遮盖{st['redact_kw']} "
                                            f"二维码{st['redact_qr']} 水印块{st['invis']}")
            print(f"  [OK] {os.path.basename(p)[:62]} -> 删页{st['del_pages']} "
                  f"遮盖{st['redact_kw']} 二维码{st['redact_qr']} 水印块{st['invis']} "
                  f"页{st['pages_before']}->{st['pages_after']}")
            if bcore is not None and bid and fid_of.get(p):
                bcore.db_step(bid, fid_of[p], bcore.STEP_AD_CLEAN, bcore.ST_OK,
                              src=p, dst=dst, src_sha=_sha256_file(p))
                if not args.inplace:
                    bcore.update_file_path(bid, fid_of[p], dst)
        except Exception as e:
            tot["fail"] += 1
            print(f"  [ERR] {os.path.basename(p)[:62]} -> {type(e).__name__}: {e}")
            if bcore is not None and bid and fid_of.get(p):
                bcore.db_step(bid, fid_of[p], bcore.STEP_AD_CLEAN, bcore.ST_FAIL,
                              src=p, error=f"{type(e).__name__}: {e}")
            if ledgerio is not None:
                ledgerio.log("ads", "FAIL", src_path=p,
                             note=f"{type(e).__name__}: {e}"[:180])
            if statusio:
                statusio.update_detail("ads", p, os.path.basename(p), "FAIL",
                                       note=f"{type(e).__name__}: {e}"[:180])

    # ---- 干净文件：无需清理，直接复制进成品（保持「已清广告」为完整批次集） ----
    for _fid, p in clean_files:
        rel = rel_of(p, [SRC_ROOT, r"D:\work\文库资源"])
        dst = os.path.join(args.out_root, rel)
        if args.dry_run:
            print(f"  [预览] 干净 {rel[:70]}（无需清理）")
            if statusio:
                statusio.update_detail("ads", p, os.path.basename(p), "SKIP",
                                       note="dry-run 干净（无需清理）")
            continue
        try:
            os.makedirs(os.path.dirname(dst) or args.out_root, exist_ok=True)
            if not (os.path.exists(dst) and os.path.getsize(dst) > 0):
                shutil.copy2(p, dst)
            tot["copied"] += 1
            if ledgerio is not None:
                ledgerio.log("ads", "DONE", src_path=p, dst_path=dst,
                             note="干净（无需清理）")
            if statusio:
                statusio.update_detail("ads", p, os.path.basename(p), "DONE",
                                       note="干净（无需清理）")
            print(f"  [OK] {os.path.basename(p)[:62]} -> 干净（无需清理）")
            if bcore is not None and bid and _fid:
                bcore.db_step(bid, _fid, bcore.STEP_AD_CLEAN, bcore.ST_OK,
                              src=p, dst=dst, src_sha=_sha256_file(p))
                bcore.update_file_path(bid, _fid, dst)
        except Exception as e:
            tot["fail"] += 1
            print(f"  [ERR] {os.path.basename(p)[:62]} -> {type(e).__name__}: {e}")
            if bcore is not None and bid and _fid:
                bcore.db_step(bid, _fid, bcore.STEP_AD_CLEAN, bcore.ST_FAIL,
                              src=p, error=f"{type(e).__name__}: {e}")
            if ledgerio is not None:
                ledgerio.log("ads", "FAIL", src_path=p,
                             note=f"{type(e).__name__}: {e}"[:180])

    print()
    print("=" * 72)
    print(f"  处理文件 : {tot['files']}")
    print(f"  干净复制 : {tot['copied']}")
    print(f"  失败     : {tot['fail']}")
    print(f"  删除广告页: {tot['del_pages']}")
    print(f"  遮盖文本 : {tot['redact_kw']}")
    print(f"  遮盖二维码: {tot['redact_qr']}"
          + (f"（疑似跳过 {tot['qr_suspected']}）" if tot["qr_suspected"] else ""))
    print(f"  清除水印块: {tot['invis']}"
          + (f" ⚠ 删后残留 {tot['invis_left']}，需人工复核！"
             if tot["invis_left"] else ""))
    if args.dry_run:
        print("  (预览模式，未写入任何文件)")
    elif args.inplace:
        print(f"  已原地修改 {tot['files']} 个文件，备份 {tot['backup']} 个 -> {args.backup_dir}")
    else:
        print(f"  输出目录 : {args.out_root}")
    print("=" * 72)
    if statusio:
        statusio.end_step("ads", status=("SKIP" if args.dry_run else None),
                          note=f"文件 {tot['files']} / 失败 {tot['fail']} / "
                               f"删页 {tot['del_pages']} / 二维码 {tot['redact_qr']} / "
                               f"水印块 {tot['invis']}")

    # RESULT：供 pipeline 解析（dry-run 不算正式成功，不写 ok 由状态层保证）
    print("RESULT" + json.dumps(
        {"batch_id": bid or "", "files": tot["files"], "copied": tot["copied"],
         "fail": tot["fail"],
         "dry_run": bool(args.dry_run), "out_root": args.out_root},
        ensure_ascii=False))
    if args.fail_exit and tot["fail"]:
        sys.exit(4)


if __name__ == "__main__":
    main()
