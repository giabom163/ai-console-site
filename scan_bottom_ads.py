# -*- coding: utf-8 -*-
"""
底边广告残留扫描器 v3（只读）
================================
扫描 PDF 页面**底部区域**的推广内容，输出建议清单。**绝不修改任何 PDF。**

相比 v1 的改进（针对"源/成品 PDF 文字层为空"的真实情况）：
  1) 不再只看"最后 1 页"——对**每一页**都做底部带文本检测；
  2) 文本层为空时自动降级为**渲染图密排中文 OCR**（RapidOCR/onnxruntime），
     否则论文级别的空文字层会导致 100% 漏检（v1 在源文件上就漏了个干净）；
  3) 增加"页脚语料"全局搜索：整页文字里出现强签名即高置信命中，
     避免漏掉位于页面中下部的页脚推广行；
  4) 进度条 + 边扫边展示命中，便于人工核对。

v3 修复（依据真实误删事故复盘，2026-09-19）：
  A) "纯广告页"判定引入复核 verify_ad_page（宁可漏报，不可误删）：
     旧版只看文字层（含签名/极短 + 无试题词）就把"整页栅格图 + 少量
     水印残余文字层"的正文页判成纯广告页——实测导致 18 页正文被删。
     新版：先要求广告证据；证据不足的嫌疑页做整页 OCR 复核，
     含试题内容的、或既非试题也非广告的一律否决。
  B) "二维码"只认"可解码确认"（多尺度 detectAndDecode）：
     旧版 detect()+形态学兜底误报严重——实测把单个汉字（"共""页"）、
     图形框、线路图判成二维码。未解码的仅记"疑似"提示，不参与清理。

检测三类：
  A) 文本关键词 —— 底部区域文字/OCR 结果命中广告词
  B) 二维码     —— 底部区域检测二维码（OpenCV QRCodeDetector + 形态学兜底）
  C) 纯广告页   —— 某页整页几乎无试题内容、只有推广信息

用法：
    python scan_bottom_ads.py                     # 扫默认两个目录（全量）
    python scan_bottom_ads.py --root <目录>        # 指定目录（可多次）
    python scan_bottom_ads.py --band 0.85         # 底部带起始比例（默认 0.85 = 底部 15%）
    python scan_bottom_ads.py --limit 20          # 只扫前 20 个（快速验证）
    python scan_bottom_ads.py --no-qr             # 跳过二维码检测（更快）
    python scan_bottom_ads.py --no-ocr            # 不做 OCR 兜底（最快，但可能漏检）
    python scan_bottom_ads.py --only-subdir XXX   # 只扫路径含该子串的文件
"""
import argparse
import json
import os
import re
import sys
from datetime import datetime

import pymupdf as fitz

sys.stdout.reconfigure(errors="replace")

# 批次核心（同目录）——生产模式（--batch-id）下只扫本批次单，禁止全目录扫描。
try:
    import batchcore as bc
except Exception:
    bc = None

# ---------------------------------------------------------------------------
# 广告关键词（按类别，便于报告里说明命中原因）
# ---------------------------------------------------------------------------
AD_KEYWORDS = {
    "社群": ["扫码", "长按识别", "QQ群", "qq群", "微信群", "入群", "加群", "扫一扫"],
    "公众号": ["关注", "公众号", "官方微信", "京考一点通", "订阅号", "服务号", "微信号"],
    "引流": ["下载更多", "更多试题", "免费领取", "领取", "点击下载", "查看完整", "获取更多",
             "试题下载", "试卷下载", "更多资料", "全部试题"],
    "客服": ["客服", "咨询", "联系我们", "热线", "联系方式"],
    "网址": ["http", "www.", "gaokzx", ".com", ".cn", "网址", "网址：", "官网"],
}
# 展平成 (词, 类别)
_KW_FLAT = [(w, cat) for cat, ws in AD_KEYWORDS.items() for w in ws]

# 微信号串（站点水印固定特征）
_WX_RE = re.compile(r"(bjgkzx|bigkzx|gaokzx|bjgkz|bj8kzx)", re.I)

# 站点专属强签名（命中即高置信）
AD_SIG = ("北京高考在线平台简介", "专注北京高中升学", "gaokzx", "北京高考在线")
FOOTER_PROMO_KEYWORDS = ("关注北京高考在线", "北京高考在线官方微信", "扫码关注", "长按识别")

# 弱关键词（单独出现不足以定罪，需 ≥2 个或与强签名同现）
WEAK_KW = {"关注", "咨询", "领取", "获取", "网址", "官网", "热线", "联系我们", "微信号"}
# 强关键词（单独出现即算命中）
STRONG_KW = {"扫码", "扫一扫", "长按识别", "QQ群", "qq群", "微信群", "入群", "加群",
             "公众号", "官方微信", "京考一点通", "订阅号", "服务号",
             "下载更多", "更多试题", "免费领取", "点击下载", "查看完整",
             "客服", "http", "www.", "gaokzx", ".com", ".cn", "试题下载", "试卷下载"}

# 广告内容密度用的词表（长词优先，替换时避免重叠重复计数）
_AD_DENSITY_WORDS = sorted({w for w, _c in _KW_FLAT}
                           | set(AD_SIG) | set(FOOTER_PROMO_KEYWORDS)
                           | {"北京高考在线", "高考在线", "bjgkzx"},
                           key=len, reverse=True)

# 试题特征词：用于判断"某页是否还是试题内容"（避免把正文页误判为纯广告页）
EXAM_HINTS = ("试题", "答案", "解析", "选择题", "填空题", "解答题", "本试卷", "考试时间",
              "第", "题", "分", "下列", "已知", "求证", "计算", "（", "）", "（1）")

# ---------------------------------------------------------------------------
# 依赖（缺失自动降级）
# ---------------------------------------------------------------------------
_CV2 = _NP = None
try:
    import cv2 as _CV2
    import numpy as _NP
except Exception:
    _CV2 = _NP = None

_OCR = None          # RapidOCR 实例（懒加载）
_OCR_TRIED = False


def _get_ocr():
    """懒加载 RapidOCR（onnxruntime CPU 版）。失败返回 None。"""
    global _OCR, _OCR_TRIED
    if _OCR_TRIED:
        return _OCR
    _OCR_TRIED = True
    try:
        from rapidocr_onnxruntime import RapidOCR
        _OCR = RapidOCR()
    except Exception:
        _OCR = None
    return _OCR


def _ocr_text(pix, min_conf=0.5):
    """对 Pixmap 做密排中文 OCR，返回拼接文本。失败返回 ''。"""
    eng = _get_ocr()
    if eng is None or _NP is None or _CV2 is None:
        return ""
    try:
        img = _NP.frombuffer(pix.samples, dtype=_NP.uint8).reshape(pix.height, pix.width, pix.n)
        if pix.n == 4:
            img = _CV2.cvtColor(img, _CV2.COLOR_RGBA2BGR)
        elif pix.n == 3:
            img = _CV2.cvtColor(img, _CV2.COLOR_RGB2BGR)
        elif pix.n == 1:
            img = _CV2.cvtColor(img, _CV2.COLOR_GRAY2BGR)
        res, _ = eng(img)
        if not res:
            return ""
        parts = []
        for item in res:
            try:
                box, txt, conf = item[0], item[1], float(item[2])
            except Exception:
                continue
            if conf >= min_conf and txt:
                parts.append(txt)
        return " ".join(parts)
    except Exception:
        return ""


def _ocr_full_page(page, dpi=130):
    """整页渲染 + OCR（纯广告页复核用）。失败返回 ''。"""
    try:
        pix = page.get_pixmap(dpi=dpi, alpha=False)
    except Exception:
        return ""
    return _ocr_text(pix, min_conf=0.5)


# ---------------------------------------------------------------------------
# 检测逻辑
# ---------------------------------------------------------------------------
def classify_text(t):
    """返回 (强命中列表, 弱命中列表, 类别集合)。"""
    strong, weak, cats = [], [], set()
    for w, cat in _KW_FLAT:
        if w in t:
            cats.add(cat)
            if w in STRONG_KW:
                strong.append(w)
            else:
                weak.append(w)
    return strong, weak, cats


def ad_density(text):
    """估算文本中"广告内容"占比。返回 (广告字符数, 占比)。

    用最长词优先的占位替换法统计被广告词/微信号串覆盖的字符数，
    避免重叠词带来的重复计数。用于"纯广告页"的纯文字层证据。
    """
    t = text or ""
    if not t:
        return 0, 0.0
    marked = _WX_RE.sub(lambda m: "\x01" * (m.end() - m.start()), t)
    for w in _AD_DENSITY_WORDS:
        if w in marked:
            marked = marked.replace(w, "\x01" * len(w))
    cnt = marked.count("\x01")
    return cnt, cnt / len(t)


def has_strong_sig(t):
    return any(s in t for s in AD_SIG) or any(s in t for s in FOOTER_PROMO_KEYWORDS)


def looks_like_exam(t):
    """粗略判断该页是否含试题内容。"""
    return sum(1 for k in EXAM_HINTS if k in t) >= 3


def detect_qr_in_pix(pix, cv2, np):
    """在 PyMuPDF Pixmap 上检测二维码（v3：只认"可解码"为确认）。

    背景：旧版 detect()+形态学兜底误报严重——实测把单个汉字（"共""页"）、
    图形框、线路图判成二维码。新版策略：
      1) 多尺度 detectAndDecode：解码成功 → confirmed（供清理）；
      2) detect 定位命中但均无法解码 → suspected（仅提示，不清理）；
      3) 删除形态学兜底（误报来源）。

    返回 (status, n, decoded, pts)：
      status 为 "confirmed" / "suspected" / ""；
      decoded 为解码文本（suspected 时为空串）；
      pts 为确认时的四角点（原图渲染坐标，ndarray 4x2）。
    """
    try:
        img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)
        if pix.n == 4:
            img = cv2.cvtColor(img, cv2.COLOR_RGBA2BGR)
        elif pix.n == 3:
            img = cv2.cvtColor(img, cv2.COLOR_RGB2BGR)
        else:
            img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)
    except Exception:
        return "", 0, "", None

    det = cv2.QRCodeDetector()
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    # 1) 解码确认（多尺度：小二维码靠放大、旋转靠检测器自带容错）
    for scale in (1.0, 1.5, 2.0, 2.5):
        g = gray if scale == 1.0 else cv2.resize(
            gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
        try:
            data, pts, _ = det.detectAndDecode(cv2.cvtColor(g, cv2.COLOR_GRAY2BGR))
            if data and pts is not None and len(pts) > 0:
                try:
                    pts_orig = pts / scale
                except Exception:
                    pts_orig = pts
                # cv2 返回 (1,4,2)，归一为 (4,2) 供调用方按点遍历
                pts_orig = np.asarray(pts_orig, dtype=float).reshape(-1, 2)
                return "confirmed", 1, str(data).strip(), pts_orig
        except Exception:
            pass
    # 2) 疑似（只定位不解码）——仅提示，不参与清理
    for scale in (1.0, 1.5):
        g = gray if scale == 1.0 else cv2.resize(
            gray, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)
        try:
            ok, pts = det.detect(cv2.cvtColor(g, cv2.COLOR_GRAY2BGR))
            if ok and pts is not None and len(pts) > 0:
                return "suspected", 1, "", None
        except Exception:
            pass
    return "", 0, "", None


def verify_ad_page(page, full_text, do_ocr=True):
    """"纯广告页"复核（v3 修复核心，防误删正文页）。

    事故背景：旧判定 `(has_sig or is_tiny) and not examish` 只看文字层——
    对"整页栅格图 + 少量水印残余文字层"的文件，正文页（文字层只有
    页脚"第N页,共M页"，或只有零星水印残字）被大量误判为纯广告页，
    实测致 18 页正文在清理阶段被删。

    复核原则：宁可漏报，不可误删。
      1) 文字层无任何广告证据（签名/广告词）→ 否决
         （"第N页/共M页"类图文页页脚在此拦下，无需 OCR）；
      2) 文字层广告密度 ≥0.6 且文本 ≥30 字 → 确认（纯文字型广告页）；
      3) 其余嫌疑页做整页 OCR 复核（如可用）：
         - OCR 出试题特征 → 否决（是正文页）；
         - OCR 广告证据充分（词≥2 且短文本 / 密度≥0.5 / 签名&短）→ 确认；
         - 其余 → 否决（保守）；
      4) OCR 不可用 → 否决（保守）。

    返回 (is_ad, detail)。
    """
    t = (full_text or "").strip()
    has_sig = has_strong_sig(t)
    strong, weak, _cats = classify_text(t)
    has_ad_evidence = has_sig or bool(strong) or (len(set(weak)) >= 2)
    if not has_ad_evidence:
        return False, "无广告证据（图文页页脚/短文本）"
    ad_chars, ratio = ad_density(t)
    if len(t) >= 30 and ratio >= 0.60:
        return True, f"文字层广告密度 {ratio:.0%}（{ad_chars}字）"
    if not do_ocr:
        return False, "OCR 未启用，保守不判"
    o = _ocr_full_page(page)
    if not o:
        return False, "OCR 无结果，保守不判"
    if looks_like_exam(o):
        return False, "OCR 检出试题内容"
    o_strong, o_weak, _ = classify_text(o)
    o_ad_cnt = len(set(o_strong)) + len(set(o_weak))
    o_sig = has_strong_sig(o)
    o_chars, o_ratio = ad_density(o)
    if (o_ad_cnt >= 2 and len(o) < 80) or (len(o) >= 30 and o_ratio >= 0.5) \
            or (o_sig and len(o) < 80):
        return True, f"OCR 广告证据 {o_ad_cnt} 项：{o[:26]}"
    return False, "OCR 未见明确广告内容"


def find_invisible_watermark_spans(page, min_alpha=8, min_size=12.0, only_ad_words=True):
    """检测"不可见水印文字层残渣"。

    去水印处理后，页面里常残留 alpha≈3/255、color=白、字号 20+pt 的
    文字对象 —— 肉眼看不见，但 **PDF 文本搜索/复制仍会命中**（SEO 式水印）。
    返回 [(text, alpha, size, bbox), ...]。

    only_ad_words=True 时只保留含广告/站点关键词的 span，避免把正文捞进来。
    """
    out = []
    try:
        dct = page.get_text("dict")
    except Exception:
        return out
    for b in dct.get("blocks", []):
        for l in b.get("lines", []):
            for s in l.get("spans", []):
                txt = (s.get("text") or "").strip()
                if not txt:
                    continue
                try:
                    alpha = int(s.get("alpha", 255))
                except Exception:
                    alpha = 255
                size = float(s.get("size") or 0)
                color = int(s.get("color") or 0)
                # 条件：低透明度（几乎不可见）或 白色，且字号偏大
                invisible = (alpha <= min_alpha or color == 16777215) and size >= min_size
                if not invisible:
                    continue
                if only_ad_words:
                    low = txt.lower()
                    if not any(k in txt for k in ("高考在线", "gaokzx", "bjgkzx",
                                                  "京考", "关注", "公众号", "扫码")) and \
                       "gaokzx" not in low:
                        continue
                out.append((txt, alpha, size, s.get("bbox")))
    return out


def scan_file(path, band=0.85, do_qr=True, do_ocr=True, max_qr_pages=4):
    """扫描单个 PDF。返回 findings 列表。"""
    findings = []
    try:
        d = fitz.open(path)
    except Exception as e:
        return [{"err": f"打开失败: {e}"}]

    try:
        npages = len(d)
        qr_checked = 0
        ocr_used = 0
        page_texts = []
        for i in range(npages):
            try:
                page = d[i]
                h = page.rect.height
                w = page.rect.width
                clip = fitz.Rect(0, h * band, w, h)

                # --- A) 底部文本关键词（文字层） ---
                try:
                    txt = page.get_text("text", clip=clip).strip()
                except Exception:
                    txt = ""

                ocr_txt = ""
                # 文字层为空且允许 OCR -> 渲染底部带做密排中文 OCR 兜底
                if not txt and do_ocr and ocr_used < 6:
                    try:
                        pix = page.get_pixmap(clip=clip, dpi=200, alpha=False)
                        ocr_txt = _ocr_text(pix)
                        ocr_used += 1
                    except Exception:
                        ocr_txt = ""

                merged = (txt + " " + ocr_txt).strip()
                if merged:
                    strong, weak, cats = classify_text(merged)
                    src = "文字层" if txt else ("OCR" if ocr_txt else "")
                    if strong or (len(set(weak)) >= 2):
                        findings.append({
                            "page": i + 1,
                            "type": "文本关键词",
                            "detail": "/".join(sorted(set(strong + weak))[:6]),
                            "cats": "/".join(sorted(cats)),
                            "src": src,
                            "text": merged[:140].replace("\n", " "),
                        })

                # --- 收集整页文字，末尾统一做签名搜索 ---
                try:
                    page_texts.append(page.get_text("text"))
                except Exception:
                    page_texts.append("")

                # --- C) 纯广告页（逐页判断；v3 复核防误删正文页） ---
                full = page_texts[-1].strip()
                if full:
                    has_sig = has_strong_sig(full)
                    is_tiny = len(full) < 40
                    examish = looks_like_exam(full)
                    # 旧版触发条件仅作"嫌疑信号"，是否成立由 verify_ad_page 复核
                    if (has_sig and not examish) or (is_tiny and not examish):
                        is_ad, why = verify_ad_page(page, full, do_ocr=do_ocr)
                        if is_ad and not any(
                                f["page"] == i + 1 and f["type"] == "文本关键词"
                                for f in findings):
                            findings.append({
                                "page": i + 1,
                                "type": "纯广告页",
                                "detail": f"复核确认：{why}",
                                "cats": "整页",
                                "src": "文字层+OCR" if do_ocr else "文字层",
                                "text": full[:140].replace("\n", " "),
                            })

                # --- D) 不可见水印文字层残渣（搜"北京高考在线"会命中的隐形字） ---
                try:
                    spans = find_invisible_watermark_spans(page)
                    if spans:
                        txts = [t for t, _a, _s, _b in spans]
                        findings.append({
                            "page": i + 1,
                            "type": "隐形水印字",
                            "detail": f"{len(spans)} 处 alpha≈{min(a for _t, a, _s, _b in spans)}/{int(max(s for _t, _a, s, _b in spans))}pt",
                            "cats": "文字层残渣",
                            "src": "文字层",
                            "text": " ".join(txts)[:140],
                        })
                except Exception:
                    pass

                # --- B) 二维码（底部带，限页数省时；v3 只认可解码确认） ---
                if do_qr and qr_checked < max_qr_pages:
                    try:
                        pix = page.get_pixmap(clip=clip, dpi=110, alpha=False)
                        status, n, decoded, pts = detect_qr_in_pix(pix, _CV2, _NP)
                        qr_checked += 1
                        if status:
                            rec = {
                                "page": i + 1,
                                "type": "二维码",
                                "cats": "图形",
                                "src": "图像",
                                "text": decoded[:80],
                            }
                            if status == "confirmed":
                                # 四角点（渲染像素，已归一为 (4,2)）→ PDF 页面坐标矩形
                                s = 72.0 / 110.0
                                _pts = _NP.asarray(pts, dtype=float).reshape(-1, 2)
                                xs = [float(q[0]) * s for q in _pts]
                                ys = [float(q[1]) * s + clip.y0 for q in _pts]
                                rec["detail"] = (f"确认 {n} 个（可解码"
                                                 + (f"：{decoded[:20]}" if decoded else "") + "）")
                                rec["qr_rect"] = [min(xs), min(ys), max(xs), max(ys)]
                                rec["confirmed"] = True
                            else:
                                rec["detail"] = "疑似 1 个（未解码，仅提示不处理）"
                                rec["confirmed"] = False
                            findings.append(rec)
                    except Exception:
                        pass
            except Exception:
                continue
    finally:
        d.close()
    return findings


def iter_pdfs(root):
    for dirpath, _, files in os.walk(root):
        for fn in sorted(files):
            if fn.lower().endswith(".pdf"):
                yield os.path.join(dirpath, fn)


# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description="底边广告残留扫描器 v2（只读，不改文件）")
    ap.add_argument("--root", action="append", default=None,
                    help="要扫描的目录（可多次指定）。默认扫 文库资源_去水印后 与 文库资源_已处理")
    ap.add_argument("--band", type=float, default=0.85,
                    help="底部带起始比例（默认 0.85 = 底部 15%%）")
    ap.add_argument("--limit", type=int, default=0, help="只扫前 N 个文件（快速验证）")
    ap.add_argument("--no-qr", action="store_true", help="跳过二维码检测（更快）")
    ap.add_argument("--no-ocr", action="store_true", help="跳过 OCR 兜底（更快，但可能漏检）")
    ap.add_argument("--out", default=None, help="报告输出路径（默认写到项目目录）")
    ap.add_argument("--only-subdir", default=None, help="只扫路径含该子串的文件")
    ap.add_argument("--batch-id", default=None,
                    help="批次号：只扫描该批次 manifest 里的文件（生产流程必传）；"
                         "报告默认输出 reports/bottom_ads_<batch_id>.json")
    ap.add_argument("--file-id", default=None, dest="file_id",
                    help="只扫描指定 file_id（逗号分隔可多个）——单文件调度（看板）使用")
    args = ap.parse_args()

    bid = (args.batch_id or "").strip() or None
    fids = {x.strip().upper() for x in (args.file_id or "").split(",")
            if x.strip()} or None

    roots = args.root or [
        r"D:\work\文库资源_去水印后",
        r"D:\work\文库资源_已处理",
    ]
    print("=" * 72)
    print("底边广告残留扫描 v3（只读模式 —— 不会修改任何 PDF）")
    print("=" * 72)
    print(f"  检测带   : 底部 {int((1 - args.band) * 100)}%（y >= {args.band} * 页高）")
    print(f"  二维码   : {'跳过' if args.no_qr else '检测（解码确认 + 疑似提示）'}")
    print(f"  OCR 兜底 : {'跳过' if args.no_ocr else '启用（RapidOCR 密排中文）'}")

    files = []
    if bid:
        # 生产模式：只扫本批次 manifest 里的文件（禁止全目录扫描）
        if bc is None:
            print(">>> batchcore 不可用，无法按批次扫描（中止）。")
            sys.exit(3)
        for stage in ("cleaned", "done"):
            for _fid, p in bc.scan_pdfs(stage, bid):
                if fids is not None and str(_fid or "").strip().upper() not in fids:
                    continue
                if p not in files:
                    files.append(p)
        _scope = f"，限 {len(fids)} 个 file_id" if fids else ""
        print(f"  批次     : {bid}（manifest 内待扫文件 {len(files)} 个{_scope}）")
    else:
        print(f"  目录     :")
        for r in roots:
            print(f"    - {r}  {'[存在]' if os.path.isdir(r) else '[不存在]'}")
        for r in roots:
            if not os.path.isdir(r):
                continue
            for f in iter_pdfs(r):
                if args.only_subdir and args.only_subdir not in f:
                    continue
                files.append(f)
    if args.limit:
        files = files[:args.limit]
    print(f"  待扫文件 : {len(files)} 个")
    print("=" * 72)
    if not files:
        print(">>> 没有可扫描的文件。")
        return

    try:
        from rich.progress import (BarColumn, Progress, TextColumn,
                                   TimeElapsedColumn)
        prog = Progress(TextColumn("[progress.description]{task.description}"),
                        BarColumn(), TextColumn("{task.completed}/{task.total}"),
                        TimeElapsedColumn())
        use_rich = True
    except Exception:
        use_rich = False

    results = {}
    hits_live = 0
    if use_rich:
        with prog:
            task = prog.add_task("扫描", total=len(files))
            for f in files:
                r = scan_file(f, args.band, not args.no_qr, not args.no_ocr)
                results[f] = r
                if r and not any("err" in x for x in r):
                    hits_live += 1
                    prog.update(task, description=f"扫描 [命中 {hits_live}]")
                prog.advance(task)
    else:
        for i, f in enumerate(files, 1):
            r = scan_file(f, args.band, not args.no_qr, not args.no_ocr)
            results[f] = r
            if r and not any("err" in x for x in r):
                hits_live += 1
            if i % 10 == 0 or i == len(files):
                print(f"  [{i}/{len(files)}] 扫描中... 命中 {hits_live}")

    hit_files = {p: v for p, v in results.items() if v and not any("err" in x for x in v)}
    err_files = {p: v for p, v in results.items() if v and any("err" in x for x in v)}
    clean = len(results) - len(hit_files) - len(err_files)

    by_type = {"文本关键词": 0, "二维码": 0, "纯广告页": 0, "隐形水印字": 0}
    by_cat = {}
    for v in hit_files.values():
        for x in v:
            by_type[x["type"]] = by_type.get(x["type"], 0) + 1
            by_cat[x.get("cats", "")] = by_cat.get(x.get("cats", ""), 0) + 1

    print()
    print("=" * 72)
    print("扫描结果汇总")
    print("=" * 72)
    print(f"  扫描文件   : {len(results)}")
    print(f"  命中广告   : {len(hit_files)} 个文件")
    print(f"    文本关键词: {by_type.get('文本关键词', 0)} 处")
    print(f"    二维码    : {by_type.get('二维码', 0)} 处")
    print(f"    纯广告页  : {by_type.get('纯广告页', 0)} 处")
    print(f"  干净       : {clean} 个文件")
    print(f"  打不开/异常: {len(err_files)} 个文件")

    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    if bid:
        # 生产模式：报告固定落 reports/bottom_ads_<batch_id>.*（clean 端做 batch_id 校验）
        _jdef = str(bc.batch_report_path(bid)) if bc is not None else \
            rf"D:\work\gaokzx\reports\bottom_ads_{bid}.json"
        jout = args.out or _jdef
        if jout.endswith(".json"):
            out = jout[:-5] + ".txt"
        else:
            out = jout
            jout = out + ".json"
    else:
        out = args.out or rf"D:\work\gaokzx\底边广告检查报告_{stamp}.txt"
        jout = out.replace(".txt", ".json")
    L = []
    L.append("=" * 78)
    L.append("底边广告残留检查报告（只读扫描，未修改任何文件）")
    L.append(f"生成时间: {datetime.now():%Y-%m-%d %H:%M:%S}")
    L.append(f"检测带  : 底部 {int((1 - args.band) * 100)}%（y >= {args.band} * 页高）")
    L.append(f"OCR 兜底: {'启用' if not args.no_ocr else '关闭'}")
    L.append("=" * 78)
    L.append("")
    L.append(f"扫描文件总数 : {len(results)}")
    L.append(f"命中广告     : {len(hit_files)}")
    L.append(f"干净         : {clean}")
    L.append(f"异常         : {len(err_files)}")
    L.append("")
    L.append(f"文本关键词命中: {by_type.get('文本关键词', 0)} 处")
    L.append(f"二维码命中    : {by_type.get('二维码', 0)} 处")
    L.append(f"纯广告页命中  : {by_type.get('纯广告页', 0)} 处")
    L.append(f"隐形水印字命中: {by_type.get('隐形水印字', 0)} 处（alpha≈3 肉眼不可见，但文本搜索会命中）")
    L.append("")
    L.append("按命中原因分类:")
    for k, v in sorted(by_cat.items(), key=lambda z: -z[1]):
        L.append(f"    {k or '(未分类)':<18} {v} 处")
    L.append("")
    L.append("=" * 78)
    L.append("明细（按文件）")
    L.append("=" * 78)
    for p in sorted(hit_files):
        L.append("")
        L.append(f"■ {p}")
        for x in sorted(hit_files[p], key=lambda z: (z.get("page", 0), z.get("type", ""))):
            L.append(f"    第 {x.get('page', 0):>3} 页 | {x.get('type', ''):<6} | "
                     f"{x.get('src', ''):<4} | {x.get('detail', '')}")
            if x.get("text"):
                L.append(f"              摘要: {x['text']}")
    if err_files:
        L.append("")
        L.append("=" * 78)
        L.append("异常文件")
        L.append("=" * 78)
        for p, v in sorted(err_files.items()):
            for x in v:
                L.append(f"  {p}")
                L.append(f"      {x.get('err', '')}")
    L.append("")
    L.append("=" * 78)
    L.append("建议操作口径（供第三步决策参考）")
    L.append("=" * 78)
    L.append("  纯广告页   -> 建议「删除该页」（已复核：需广告证据/整页 OCR 确认）")
    L.append("  文本关键词 -> 建议「redact 该区域」（底部遮盖推广文字）")
    L.append("  二维码     -> 建议「redact 该区域」（仅处理可解码确认项；疑似仅提示）")
    L.append("  隐形水印字 -> 建议「删除该文字对象」（视觉无损，消除搜索命中）")
    L.append("  仅页码/无意义短文本 -> 忽略")

    with open(out, "w", encoding="utf-8") as f:
        f.write("\n".join(L))

    jout = jout if bid else out.replace(".txt", ".json")
    with open(jout, "w", encoding="utf-8") as f:
        json.dump({"generated": datetime.now().isoformat(),
                   "batch_id": bid or "",
                   "band": args.band,
                   "total": len(results),
                   "hit_files": len(hit_files),
                   "clean": clean,
                   "err_files": len(err_files),
                   "by_type": by_type,
                   "by_cat": by_cat,
                   "results": {k: v for k, v in hit_files.items()}},
                  f, ensure_ascii=False, indent=2)

    print()
    print(f"  报告(TXT): {out}")
    print(f"  报告(JSON): {jout}")
    print()
    print(">>> 未修改任何 PDF。请查看报告后决定清理范围。")


if __name__ == "__main__":
    main()
