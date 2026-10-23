#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""gaokzx 流水线 · 文件整理与规范重命名（prename）—— 2026-10-21 自 pipeline.py 拆出。

职责：PDF 文件名解析（年份/区/学校/年级/月份/考试类型/科目/答案）、
规范重命名（plan_renames/apply_renames/rollback_renames）、冲突检测与 Excel 报告。
依赖 pcore；WM_* 常量与去水印实现见 pwm.py。
"""
from pathlib import Path
import json
import os
import re
import sys
from pcore import (AUTO_MODE, OCR_REPO, STAGE_CLEANED, WORK, _SEP, _bstep, _resolve_bid, _scan, _sha256_file, _wskip, bc, iter_pdfs, ledgerio, log_err, log_ok, statusio)
from pcore import (
    AUTO_MODE,
    OCR_REPO,
    STAGE_CLEANED,
    STEP_RENAME,
    WORK,
    _SEP,
    _bstep,
    _resolve_bid,
    _scan,
    _sha256_file,
    _wskip,
    bc,
    iter_pdfs,
    ledgerio,
    log_err,
    log_ok,
    statusio,
)

# 文件整理与重命名（防覆盖）：L1 文件名正则 -> L2 PDF 首页文字层 -> L3 OCR 兜底（可选）；默认
# dry-run，重名自动 _2/_3 尾缀，改名前写映射表可回滚。
_SUBJECTS = ["语文", "数学", "英语", "物理", "化学", "生物", "历史", "地理", "政治",
             "思想政治", "理科综合", "文科综合", "理综", "文综", "信息技术", "通用技术"]
_GRADES = ["高三", "高二", "高一", "初中", "小学"]
_EXAM_TYPES = ["月考", "期中", "期末", "一模", "二模", "三模", "四模", "开学考",
               "开学考试", "联考", "模拟", "调研", "统练", "周测", "阶段测试",
               "质量检测", "适应性测试", "学业水平", "竞赛",
               "考前热身", "热身卷", "热身", "保温卷", "保温", "考前练", "冲刺", "预测试"]
# 答案状态：只认“显式关键词”，删去光杆“答案”（“试题及答案”这类汇总标题不得把单份试卷默认补成有答案）。
_ANSWER_STATES = [("无答案", ["无答案", "不含答案", "学生版", "无解答", "空白卷"]),
                  ("有答案", ["有答案", "含答案", "附答案", "教师版", "解析版", "有解析", "答案解析", "答案版"])]

_DISTRICTS = ["海淀", "朝阳", "西城", "东城", "丰台", "石景山", "门头沟", "房山",
              "通州", "顺义", "昌平", "大兴", "怀柔", "平谷", "密云", "延庆",
              "燕山", "经开区"]
# 学校白名单（正文兜底时的唯一可信来源）：来自全库 763 个真实 PDF 文件名的频次统计
# （出现≥2 次的 99 个校名）+ 常见学校人工补充。规则：stem（下载命名，可信）里抽校名
# 走结构模式（宽松 + 噪声过滤）；meta（PDF 正文，不可信）兜底**只认白名单命中**——
# 正文里"十一届三中全会 / 技术的对立统一中 / 多所高校规范大学"这类短语绝不会通过
# 边界与虚词校验（历史坑：正文校名取最长，覆盖了文件名里的真校名）。
_SCHOOL_WHITELIST = (
    # —— 来自全库真实文件名的频次校名 ——
    "十二中", "十五中", "八十中", "十八中", "十九中", "五十五中", "十四中",
    "一零一中", "一六一中", "一六六中", "二中", "四中", "一中", "八中", "九中",
    "人大附中", "北大附中", "清华附中", "北师大实验中学", "北师大二附中",
    "北师大附中", "首师大附中", "理工大附中", "交大附中", "民大附中",
    "中关村中学", "八一学校", "景山学校", "汇文中学", "广渠门中学",
    "东直门中学", "牛栏山一中", "和平街一中", "延庆一中", "海淀进修学校",
    "北京中学",
    # —— 常见学校人工补充 ——
    "十一学校", "一七一中学", "五十七中", "五十六中", "育英学校", "育英中学",
    "海淀实验中学", "陈经纶中学", "日坛中学", "工大附中", "三里屯一中",
    "潞河中学", "运河中学", "大峪中学", "京源学校", "北航实验学校", "石油附中",
    "科大附中", "地大附中", "矿院附中", "玉渊潭中学", "海淀外国语",
    "建华实验学校", "清华实验学校", "北大附中实验学校", "海淀教师进修学校",
)
# 长词优先（先命中"北京十二中"，而不是"十二中"截断）
_SCHOOL_WL_SORTED = tuple(sorted(_SCHOOL_WHITELIST, key=len, reverse=True))
# 白名单命中时的边界校验：pre 为虚词或"届级高初"（"十一届三中"的"届"、"高二中"的"高"）、
# post 为"全会考期"（"三中全会"的"全"、"中考"的"考"）一律判假。宁可漏检不可错配。
_WL_PRE_BAD = set("统的会与及各共被在为是并了等有对从向把让使得过着们这那些多届级高初")
_WL_POST_BAD = set("全会考期")

def _wl_boundary_ok(text, i, w):
    """白名单命中词的边界是否干净（前后字符不构成正文短语）。"""
    pre = text[i - 1] if i > 0 else ""
    post = text[i + len(w)] if i + len(w) < len(text) else ""
    if pre and pre in _WL_PRE_BAD: return False
    if post and post in _WL_POST_BAD: return False
    return True

def _school_wl_hit(text):
    """在 text 里找白名单校名（最长命中，含回补"北京"前缀）；无命中返回 ""。
    只用于 meta 兜底与终检——stem 抽取不走这里（见 _extract_school）。"""
    if not text: return ""
    best = ""
    for w in _SCHOOL_WL_SORTED:
        start = 0
        while True:
            i = text.find(w, start)
            if i < 0: break
            if _wl_boundary_ok(text, i, w):
                cand = ("北京" + w) if text[max(0, i - 2):i] == "北京" else w
                if len(cand) > len(best): best = cand
                break                       # 该词命中一次即可，省时间
            start = i + 1
    return best

# 学校名后缀（用于"北京市XX中学/XX附中/第N中学"这类结构）
_SCHOOL_TAILS = (r"(?:中学|学校|附中|附小|小学|初中|高中|职业学院|大学|学院|"
                 r"回民中学|实验中学|育英中学|潞河中学|进修学校)")

# "数字+中"模式（二中/八十中/一零一中/第101中学…）：模式驱动非白名单，任何"（北京市/第）+数字+中"或"北京市XX+学校后缀"都能抽（旧白名单静默丢校名）。
_SCHOOL_NUM_RX = re.compile(
    r"(?:(?:北京市?|第)\s*)?"
    r"((?:[零一二三四五六七八九十百]{1,4}|\d{1,4})中(?:学|附中)?)"
)
_SCHOOL_TAIL_RX = re.compile(r"((?:北京市?)?[\u4e00-\u9fa5]{2,10}?" + _SCHOOL_TAILS + r")")

# stem 模式的噪声词：正文短语误入候选时直接丢弃（历史坑："技术的对立统一中"含"的"、
# "多所高校规范大学"含"规范/高校"、"十一届三中"后跟"全会"）。
_SCHOOL_BAD_SUB = ("的", "全会", "统一", "规范", "高校", "被", "为", "是",
                   "并", "了", "等", "各", "多", "共", "含", "及", "与", "有",
                   "在", "对", "从", "向", "把", "让", "使", "得", "着", "过",
                   "期末", "期中", "月考", "开学", "毕业", "考试", "答案", "试题",
                   "中考", "高考", "联考", "统考", "模拟")

def _extract_school(hay, mode="stem"):
    """抽取学校名。mode 决定信任级别：
      "stem"：下载文件名（站点给的标题，可信）——结构三模式 + 白名单合并、取最长，
              带噪声过滤；
      "meta"：PDF 正文（不可信）——**只认白名单命中**（含边界校验），绝不泛化。
    历史教训：旧版对 全文 取最长，"十一届三中全会 / 技术的对立统一中 / 多所高校规范大学"
    把文件名里真正的校名（十五中/广渠门中学/北京十二中）覆盖掉——正文永远不能压倒文件名。
    三层策略（stem 模式）：1) 中文前缀 + 数字/中文数字 + 中(学)?（牛栏山一中/铁二中/八十中）；
    2) （北京）? + XX + 学校类后缀（清华附中/潞河中学）；3) （北京）? + 数字 + 中(学)（二中/九中/101中学）。
    结果的"北京"前缀先剥掉。"""
    if mode == "meta":
        return _school_wl_hit(hay)
    def _strip_bj(s):
        if s.startswith("北京市"): s = s[3:]
        elif s.startswith("北京"): s = s[2:]
        # 区前缀（"海淀区人大附中"来自旧命名）也剥掉，区信息由 region 字段独立表达
        m = re.match(r"^([\u4e00-\u9fa5]{1,3})区", s)
        if m and m.group(1) in _DISTRICTS: s = s[len(m.group(1)) + 1:]
        return s
    cands = []
    # 1) 带中文前缀的数字型：牛栏山一中 / 十里堡二中 / 铁二中
    for m in re.finditer(
            r"((?:北京市?|第)?[\u4e00-\u9fa5]{0,6}?"
            r"(?:[零一二三四五六七八九十百]{1,4}|\d{1,4})中(?:学|附中)?)", hay):
        cands.append(m.group(1))
    # 2) 后缀型：清华附中 / 中关村中学 / 潞河中学
    for m in _SCHOOL_TAIL_RX.finditer(hay): cands.append(m.group(1))
    # 3) 纯数字型兜底：二中 / 九中
    for m in _SCHOOL_NUM_RX.finditer(hay): cands.append(m.group(1))
    if not cands: return ""
    # 去噪：含虚词/正文短语的直接丢弃（不做前缀裁剪，宁可不要）
    cleaned = [c for c in cands if c and not any(b in c for b in _SCHOOL_BAD_SUB)]
    cleaned = [_strip_bj(c) for c in cleaned]
    cleaned = [c for c in cleaned if c and len(c) >= 2 and c != "北京"]
    if not cleaned: return ""
    # 冲突时优先更长的（信息更全）；长度相同取先出现的
    return max(cleaned, key=lambda s: (len(s), -hay.find(s)))

# 科目匹配"位置优先"：不能按 _SUBJECTS 列表顺序决胜负（与文字位置无关），否则延庆地理卷因正文出现
# "历史"二字被判成历史、生物卷因"化学"被判成化学；改为取 hay 中起始位置最靠前的科目，同位置取更长者。
_SUBJECT_ALIASES = (
    ("思想政治", "思想政治"),
    ("理科综合", "理科综合"),
    ("文科综合", "文科综合"),
    ("信息技术", "信息技术"),
    ("通用技术", "通用技术"),
    ("理综", "理综"),
    ("文综", "文综"),
    ("语文", "语文"),
    ("数学", "数学"),
    ("英语", "英语"),
    ("物理", "物理"),
    ("化学", "化学"),
    ("生物", "生物"),
    ("历史", "历史"),
    ("地理", "地理"),
    ("政治", "政治"),
)

def _pick_subject(hay):
    """在 hay 中挑科目：按**出现位置**决胜负，位置相同取词更长者。返回 "" 表示没命中。"""
    best, best_pos, best_len = "", None, 0
    for key, norm in _SUBJECT_ALIASES:
        pos = hay.find(key)
        if pos < 0: continue
        if best_pos is None or pos < best_pos or (pos == best_pos and len(key) > best_len):
            best, best_pos, best_len = norm, pos, len(key)
    return best

# 年份合法区间（校验门第一道）：明显越界的一律不采用
_YEAR_MIN, _YEAR_MAX = 2010, 2035

def _norm_year(text):
    """在 text 里找合法年份（2010~2035）；找不到返回 ""。"""
    for m in re.finditer(r"(20\d{2})", text or ""):
        if _YEAR_MIN <= int(m.group(1)) <= _YEAR_MAX:
            return m.group(1)
    return ""

def _meta_title_text(meta):
    """正文**标题区**：剔除“第N页/共M页”水印行后取前 3 个有效行并折叠字间空格。
    科目必须只看标题，不能扫整页：标题科目字常带空格（“语    文”），而题干/解析里的
    科目标词（“描述…的物理量”“降低化学反应…”）扫整页会造成假冲突，阻断合法改名。"""
    if not meta:
        return ""
    keep = []
    for l in (x.strip() for x in meta.splitlines()):
        if not l:
            continue
        if re.match(r"^第\s*\d+\s*页\s*/\s*共\s*\d+\s*页$", l):
            continue                      # 页码水印行不算标题区
        keep.append(l)
        if len(keep) >= 3:
            break
    return re.sub(r"\s+", "", "\n".join(keep))


def parse_pdf_name(text, meta_text=""):
    """从文件名（stem）与首页文字（meta）抽取规范字段，返回 dict。
    **字段信任级别（用户规则：原名优先，正文绝不覆盖关键字段）**：
      - 所有字段一律 **stem 优先**；
      - meta 仅当 stem 完全缺失时兜底：school 必须白名单命中（_extract_school mode="meta"），
        region 必须白名单命中，year/grade/subject/answer 为强词典/正则约束；
      - month/exam **不从 meta 兜底**（正文里的"6月""期中"等短语不得改写原文件名语义）。
    历史教训：旧版把 stem+meta 拼成单一 hay 统一正则——正文"十一届三中全会/技术的对立统一中/
    多所高校规范大学"被当成学校名，覆盖了文件名里真正的校名（十五中/广渠门中学/北京十二中）；
    科目按列表序取"首个命中"（正文出现"历史"二字，地理卷就被判成历史卷）。"""
    stem = text or ""
    meta = meta_text or ""
    hay = f"{stem}\n{meta}" if meta else stem
    out = {}
    # ---- 年：文件名优先；正文兜底也必须过 2010~2035 校验 ----
    out["year"] = _norm_year(stem) or _norm_year(meta)
    out["grade"] = (next((g for g in _GRADES if g in stem), "") or
                    next((g for g in _GRADES if g in meta), ""))
    # ---- 区：只在文件名里找；找不到再退回正文但必须命中已知区名（文件名"XX区"非贪婪取 1-3 汉字）----
    m = re.search(r"([\u4e00-\u9fa5]{1,3})区", stem)
    if m and m.group(1) in _DISTRICTS: out["region"] = m.group(1) + "区"
    else:
        # 2) 文件名里的裸区名（"延庆"这种不带"区"字的写法）
        hit = next((d for d in _DISTRICTS if d in stem), "")
        if hit: out["region"] = hit + ("区" if hit != "燕山" else "")
        else:
            # 3) 正文兜底：**必须命中已知区名白名单**，绝不接受任意"XX区"
            m = re.search(r"([\u4e00-\u9fa5]{1,3})区", meta)
            if m and m.group(1) in _DISTRICTS: out["region"] = m.group(1) + "区"
            else:
                hit = next((d for d in _DISTRICTS if d in meta), "")
                out["region"] = hit + ("区" if hit and hit != "燕山" else "")
    # ---- 学校：文件名结构抽取优先；正文兜底**只认白名单**（信任级别差异见 _extract_school）----
    out["school"] = _extract_school(stem, mode="stem") or _extract_school(meta, mode="meta")
    if out["school"] and out["region"] and out["school"].replace("北京", "") == out["region"]:
        out["school"] = ""          # "北京海淀"被判成学校 -> 丢弃，交给 region 字段
    # ---- 月份 / 考试类型：**只认文件名**（正文里的"6月""期中"不得改写语义）----
    m = re.search(r"(\d{1,2})月", stem)
    out["month"] = (m.group(1) + "月") if m else ""
    out["exam"] = next((e for e in _EXAM_TYPES if e in stem), "")
    # ---- 科目（用户规则：正文首页标题为准，文件名前缀仅辅助；二者冲突 -> 记 conflict 拒绝改名）----
    # meta 只取标题区（前 3 有效行、折叠字间空格），题干/解析里的科目标词不得参与判冲突。
    sub_stem = _pick_subject(stem)
    sub_meta = _pick_subject(_meta_title_text(meta))
    if sub_stem and sub_meta and sub_stem != sub_meta:
        out["subject"] = sub_meta               # 正文优先取值
        out["subject_conflict"] = True          # 但冲突：_rename_guard 据此拒绝改名
        out["subject_in_name"] = sub_stem       # 留存文件名前缀科目（冲突审核报表用）
    else:
        out["subject"] = sub_meta or sub_stem    # 正文有则用正文，否则文件名
        out["subject_conflict"] = False
        out["subject_in_name"] = ""
    # ---- 答案（用户规则：只从“文件名显式关键词”抽取，绝不默认补“有答案”；无关键词留空）----
    ans = ""
    for label, keys in _ANSWER_STATES:
        if any(k in stem for k in keys):        # 仅 stem：正文/题干里的“答案”不作数
            ans = label
            break
    if any(k in stem for k in ("无答案", "不含答案", "无解答", "学生版", "空白卷")):
        ans = "无答案"                           # 显式负向优先，杜绝“无答案被补成有答案”
    out["answer"] = ans
    out["raw"] = text
    return out

def _pdf_first_page_text(path, pages=1):
    try:
        import pymupdf as fitz
        with fitz.open(path) as d: return "\n".join(d[i].get_text() for i in range(min(pages, d.page_count)))
    except Exception: return ""

def build_norm_name(f, fallback_stem=""):
    """按字段拼规范化文件名；字段太少就保留原名（不乱改）。"""
    parts = []
    if f.get("year"): parts.append(f["year"])
    if "北京" not in (f.get("school") or "") and not f.get("region"): parts.append("北京")
    for k in ("region", "school", "grade", "month", "exam", "subject", "answer"):
        if f.get(k): parts.append(f[k])
    name = re.sub(r'[\\/:*?"<>|\r\n\t]', "_", "".join(parts)).strip(" .")
    return name if len(name) >= 6 else fallback_stem

def _rename_guard(f, stem):
    """重命名输出前的硬校验（用户规则：区/学校/科目/年份必须命中词典，否则回退原文件名）。
    返回 (ok, reason)；ok=False 时调用方保留原文件名（changed=False，建议清单里可见原因）。"""
    y = str(f.get("year") or "")
    if y and not (_YEAR_MIN <= int(y) <= _YEAR_MAX):
        return False, f"年份越界({y})"
    rg = str(f.get("region") or "")
    if rg and rg.rstrip("区") not in _DISTRICTS:
        return False, f"区名未命中词典({rg})"
    sj = str(f.get("subject") or "")
    if sj and sj not in _SUBJECTS:
        return False, f"科目未命中词典({sj})"
    if f.get("subject_conflict"):
        # 文件名前缀科目与正文首页科目不一致：不自动改名，保留原名交人工（杜绝“物理被改成数学”）
        return False, "科目冲突(文件名前缀≠正文)，拒绝改名"
    sc = str(f.get("school") or "")
    if sc:
        # 学校：要么在 stem 里实打实出现（含去"北京"形式），要么白名单命中（meta 抽取已保证）
        _sc_ok = (sc in stem or sc.replace("北京", "") in stem or
                  _school_wl_hit(sc) == sc)
        if not _sc_ok:
            return False, f"学校未通过校验({sc})"
    if not (y or rg or sc):
        return False, "关键字段不足(年份/区/学校均缺)"
    return True, ""

def unique_path(dst: Path):
    """目标已存在则返回 _1/_2/_3... 尾缀路径。绝不返回已存在的路径（防覆盖）。"""
    if not dst.exists(): return dst
    stem, suf, parent = dst.stem, dst.suffix, dst.parent
    n = 1
    while True:
        cand = parent / f"{stem}_{n}{suf}"
        if not cand.exists(): return cand
        n += 1

def plan_renames(root, limit=None, use_text=True, use_ocr=False, recursive=True, files=None):
    """生成改名建议清单（不改任何文件）。[!] 必须走 iter_pdfs() 而非 root.glob()——只有 iter_pdfs
    内部才有 ONLY_SUBDIR（--only-subdir/--url 自动限定）过滤，直接扫盘曾导致整库（含旧库）被一起
    改名。recursive=False 退化为只扫根目录一层；files 非 None 时直接用（批次清单已限定）。"""
    root = Path(root)
    # iter_pdfs() 产出 str 路径（供 glob/subprocess 用），这里要 Path 才能取 .stem/.name/.with_name。
    if files is not None: files = [Path(x) for x in files]
    elif recursive: files = [Path(x) for x in iter_pdfs(str(root))]
    else: files = sorted(root.glob("*.pdf"))
    if limit: files = files[:limit]
    plan = []
    guard_skips = []
    for p in files:
        stem = p.stem
        meta = _pdf_first_page_text(p) if use_text else ""
        f = parse_pdf_name(stem, meta)
        # L3：前两层缺关键字段且用户显式开启时才跑 OCR（贵）
        if use_ocr and (not f["subject"] or not f["year"]):
            try:
                f2 = _ocr_one_text(p)
                if f2: f = parse_pdf_name(stem, meta + "\n" + f2)
            except Exception: pass
        # 校验门：区/学校/科目/年份必须命中词典，不通过 -> 保留原文件名（不改名）
        _ok, _why = _rename_guard(f, stem)
        new_stem = build_norm_name(f, fallback_stem=stem)
        # 信息保真：stem 里出现的考试限定词必须全部保留到新名，丢任一 -> 拒绝改名（杜绝“丢三模/考前热身/保温卷”）。
        if _ok and new_stem != stem:
            _lost = [e for e in _EXAM_TYPES if e in stem and e not in new_stem]
            if _lost:
                _ok = False
                _why = (_why + "；" if _why else "") + f"限定词丢失({','.join(_lost)})"
        if not _ok:
            new_stem = stem
            guard_skips.append((p.name, _why))
        plan.append({"src": str(p), "old": p.name, "new_stem": new_stem,
                     "dst": str(p.with_name(new_stem + p.suffix)),
                     "changed": new_stem != stem,
                     "guard": ("" if _ok else _why),
                     "fields": {k: v for k, v in f.items() if k != "raw"}})
    if guard_skips:
        print(f"    [校验] {len(guard_skips)} 个文件未通过字段校验，保留原文件名：")
        for _n, _w in guard_skips[:12]:
            print(f"      - {_n}  ({_w})")
        if len(guard_skips) > 12: print(f"      ... 其余 {len(guard_skips) - 12} 个略")
    return plan

def _ocr_one_text(path):
    """L3 OCR 兜底：调 OCR 仓库给单文件加层后读回文字。仅用户显式开启时使用。"""
    sys.path.insert(0, str(OCR_REPO))
    from BatchOcrPdfAddTextLayer import check_pdf_ocr_add_text
    check_pdf_ocr_add_text(path)
    return _pdf_first_page_text(path, pages=2)
def _same_content(src, dst):
    """内容是否一致：先比大小（廉价），大小相同再比 sha256。两者都一致才判重复。"""
    try:
        if Path(src).stat().st_size != Path(dst).stat().st_size:
            return False
    except OSError:
        return False
    hs, hd = _sha256_file(src), _sha256_file(dst)
    return hs is not None and hs == hd


def _fmt_rename_fields(fields):
    """将抽取字段拼为人读串（供“原名 -> 新名 -> 抽取字段”三段式日志）。"""
    fields = fields or {}
    _keys = [("year", "年"), ("region", "区"), ("school", "校"), ("grade", "年级"),
             ("month", "月"), ("exam", "考试"), ("subject", "科目"), ("answer", "答案")]
    s = " ".join(f"{lab}={fields[k]}" for k, lab in _keys if fields.get(k))
    if fields.get("subject_conflict"):
        s += " [!科目冲突]"
    return s or "(无)"


# 改名冲突审核工作表（真事故文件出口：不只留日志，逐条可审可处理）；测试隔离模式写入隔离目录
RENAME_CONFLICT_XLSX = WORK / "文库资源" / "_报告" / "rename_conflicts.xlsx"


def _collect_rename_conflicts(plan):
    """从改名计划提取冲突/拒绝条目：科目冲突（文件名前缀≠正文标题）与目标撞名类拒绝。
    每行：原名 / 正文识别科目 / 文件名科目 / 冲突字段 / 建议新名(正文优先) / 状态。"""
    rows = []
    for item in plan:
        fd = item.get("fields") or {}
        g = item.get("guard") or ""
        is_conf = bool(fd.get("subject_conflict")) or ("冲突" in g)
        if not is_conf:
            continue
        stem = Path(item["old"]).stem
        f2 = {k: v for k, v in fd.items() if k not in ("raw",)}
        f2["subject_conflict"] = False
        try:
            sug = build_norm_name(f2, fallback_stem=stem)
        except Exception:
            sug = ""
        if sug == stem:
            sug = ""                      # 建议名与原名相同 = 无可建议
        if item.get("by_body_fix"):
            status = "已按正文强改（冲突留档）"
        elif fd.get("subject_conflict"):
            status = "拒绝改名：科目冲突，待人工复核"
        else:
            status = f"保留原名：{g}"
        rows.append({"old": item["old"], "src": item["src"],
                     "body": fd.get("subject") or "(未识别)",
                     "name_sub": fd.get("subject_in_name") or _pick_subject(stem) or "(无)",
                     "conflict": g or "科目冲突(文件名前缀≠正文)",
                     "suggest": (sug + ".pdf") if sug else "(无建议)",
                     "status": status})
    return rows


def write_rename_conflicts_xlsx(plan, out_path=None):
    """生成/刷新 _报告\\rename_conflicts.xlsx（每次改名步骤全量覆写，含本轮处理结果）。
    无冲突也写表（单行“本轮无冲突”），避免旧报表误导人工审核。失败只告警不阻断主流程。"""
    rows = _collect_rename_conflicts(plan)
    p = Path(out_path) if out_path else RENAME_CONFLICT_XLSX
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill
        p.parent.mkdir(parents=True, exist_ok=True)
        wb = Workbook()
        ws = wb.active
        ws.title = "改名冲突审核"
        ws.append(["原名", "正文识别科目", "文件名科目", "冲突字段/原因", "建议新名(按正文)", "状态", "当前路径"])
        for c in ws[1]:
            c.font = Font(bold=True)
            c.fill = PatternFill("solid", fgColor="DDEBF7")
        for r in rows:
            ws.append([r["old"], r["body"], r["name_sub"], r["conflict"],
                       r["suggest"], r["status"], r["src"]])
        if not rows:
            ws.append(["(本轮无冲突)", "", "", "", "", "无待人工复核条目", ""])
        for col, w in (("A", 46), ("B", 12), ("C", 12), ("D", 34), ("E", 46), ("F", 26), ("G", 60)):
            ws.column_dimensions[col].width = w
        wb.save(str(p))
        if rows:
            print(f">>> [rename] 冲突审核表已生成：{p}（{len(rows)} 条待人工复核，"
                  "可用 --step rename --rename-apply --fix-conflicts-by-body 按正文强制改名）")
        return len(rows)
    except Exception as e:
        print(f">>> [rename] [!] 冲突审核表写入失败（不阻断）：{type(e).__name__}: {e}")
        return 0


def show_rename_conflicts():
    """菜单[v]：展示改名冲突审核表内容（无文件/无冲突给中文提示）。"""
    p = RENAME_CONFLICT_XLSX
    print("\n" + _SEP)
    print(f"改名冲突审核表：{p}")
    print(_SEP)
    if not p.exists():
        print("  尚未生成过冲突表（跑过 --step rename 后若有冲突会自动生成）。")
        return
    try:
        from openpyxl import load_workbook
        wb = load_workbook(str(p), read_only=True)
        ws = wb.active
        n = 0
        for row in ws.iter_rows(values_only=True):
            vals = ["" if v is None else str(v) for v in row]
            if all(not v for v in vals):
                continue
            n += 1
            if n == 1:
                print("  " + " | ".join(vals))
                print("  " + "-" * 64)
                continue
            print("  " + " | ".join(v[:36] for v in vals))
        wb.close()
        if n <= 1:
            print("  （无冲突记录）")
        else:
            print(f"\n  共 {n - 1} 条。处理入口：python pipeline.py --step rename --rename-apply "
                  "--fix-conflicts-by-body（人工确认后按正文强制改名并留档）")
    except Exception as e:
        print(f"  [!] 读取失败：{type(e).__name__}: {e}")


def _promote_conflicts_by_body(plan):
    """--fix-conflicts-by-body：把科目冲突条目转为「正文优先」改名建议（用户已确认模式）。
    重过 guard + 限定词保护，不通过的不提升。返回提升条数。"""
    n = 0
    for item in plan:
        fd = item.get("fields") or {}
        if not fd.get("subject_conflict"):
            continue
        stem = Path(item["src"]).stem
        f2 = {k: v for k, v in fd.items() if k != "raw"}
        f2["subject_conflict"] = False
        ok2, why2 = _rename_guard(f2, stem)
        new_stem = build_norm_name(f2, fallback_stem=stem)
        if ok2:
            _lost = [e for e in _EXAM_TYPES if e in stem and e not in new_stem]
            if _lost:
                ok2 = False
        if not ok2 or new_stem == stem:
            continue
        item["new_stem"] = new_stem
        item["dst"] = str(Path(item["src"]).with_name(new_stem + Path(item["src"]).suffix))
        item["changed"] = True
        item["guard"] = ""
        item["by_body_fix"] = True
        n += 1
    return n


def apply_renames(plan, dry=True, log_path=None, bid=None, fid_of=None):
    """执行改名（内容感知）：理想名未占用则直接改名；理想名已占用时按内容哈希判定——
    内容相同 -> 判重复，源文件移入 _duplicates；内容不同 -> 判冲突，保留原文件名不改名。
    绝不自动把已有 _1 升级成 _2。返回 (改动数, 映射列表)，映射写 JSON 便于回滚。
    bid/fid_of 提供时同步写 pipeline.db 的 file_steps（rename started/ok/fail/skip）并推进 manifest
    路径；dry 模式不写库。"""
    fid_of = fid_of or {}
    mapping, n = [], 0
    for item in plan:
        if not item["changed"]: continue
        src = Path(item["src"])
        if not src.exists():
            log_err(item["old"], "源文件不存在，跳过")
            _wskip(bid, fid_of.get(item["src"]), STEP_RENAME, item["src"], dry, "源文件不存在")
            statusio.update_detail("rename", item["src"], item["old"], "FAIL", note="源文件不存在")
            continue
        ideal = Path(item["dst"])
        # 冲突预检：理想名已被占用时按内容哈希判定重复/冲突；绝不把已有 _1 自动升级成 _2。
        if ideal.exists():
            if _same_content(src, ideal):
                # 内容相同 -> 判重复：源文件移入 _duplicates（不生成 _N，不占正规名）
                dupdir = ideal.parent / "_duplicates"
                move_to = unique_path(dupdir / ideal.name)
                if dry:
                    print(f"  {item['old']}\n      [=重复] 与已存在的 {ideal.name} 内容一致 -> 移入 _duplicates")
                    statusio.update_detail("rename", item["src"], item["old"], "SKIP", note="重复（同哈希）-> _duplicates")
                    continue
                try: dupdir.mkdir(parents=True, exist_ok=True)
                except OSError: pass
                _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "started", src=item["src"])
                try:
                    os.replace(str(src), str(move_to))
                    log_ok(f"{item['old']} -> [重复] _duplicates/{move_to.name}")
                    ledgerio.log("rename", "SKIP", src_path=str(src), dst_path=str(move_to),
                                 note="内容重复 -> _duplicates")
                    _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "ok", src=item["src"],
                           dst=str(move_to), error="dup->_duplicates")
                    # 回填上游各步登记的旧路径，防 dst 悬空（reconcile 误回滚根因）
                    if bc is not None and bid and fid_of.get(item["src"]):
                        try: bc.db_repath(bid, fid_of[item["src"]], item["src"], str(move_to))
                        except Exception: pass
                    statusio.update_detail("rename", item["src"], item["old"], "DONE", note="重复同哈希 -> _duplicates")
                except OSError as e:
                    log_err(item["old"], f"去重移动失败: {e}")
                    _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "fail", src=item["src"], error=f"去重移动失败: {e}")
                    statusio.update_detail("rename", item["src"], item["old"], "FAIL", note=f"去重移动失败: {e}")
                continue
            # 内容不同 -> 冲突：保留原文件名、不改名、不生成 _N，交人工复核
            if dry:
                print(f"  {item['old']}\n      [!冲突] 目标 {ideal.name} 已存在且内容不同 -> 保留原文件名")
                statusio.update_detail("rename", item["src"], item["old"], "SKIP", note=f"冲突:{ideal.name}")
                continue
            _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "started", src=item["src"])
            _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "skip", src=item["src"],
                   dst=str(ideal), error="目标已存在且内容不同（保留原名）")
            statusio.update_detail("rename", item["src"], item["old"], "SKIP",
                                   note=f"冲突（{ideal.name} 内容不同）保留原名")
            print(f"    [!冲突] {item['old']} 目标 {ideal.name} 已存在且内容不同 -> 保留原名，未改名")
            continue
        # 理想名未被占用：正常改名（无撞名就不需要 _N 尾缀）
        dst = ideal
        if dry:
            print(f"  {item['old']}\n      -> {dst.name}\n      抽取字段: {_fmt_rename_fields(item.get('fields'))}")
            statusio.update_detail("rename", item["src"], item["old"], "SKIP", note=f"dry-run 建议 -> {dst.name}")
            n += 1
            continue
        _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "started", src=item["src"])
        try:
            os.replace(str(src), str(dst))
            mapping.append({"from": str(src), "to": str(dst)})
            log_ok(f"{item['old']} -> {dst.name}")
            print(f"      抽取字段: {_fmt_rename_fields(item.get('fields'))}")
            try: _sz = dst.stat().st_size
            except OSError: _sz = None
            ledgerio.log("rename", "DONE", src_path=str(src), dst_path=str(dst),
                         size_bytes=_sz, note=f"{src.name} -> {dst.name}")
            _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "ok", src=item["src"], dst=str(dst))
            if bc is not None and bid and fid_of.get(item["src"]):
                try: bc.update_file_path(bid, fid_of[item["src"]], str(dst))
                except Exception: pass
                # 回填上游各步（watermark/ocr/qc）登记的旧路径为新名：从根上杜绝 dst 悬空
                try: bc.db_repath(bid, fid_of[item["src"]], item["src"], str(dst))
                except Exception: pass
            statusio.update_detail("rename", item["src"], item["old"], "DONE",
                                   src_bytes=_sz, dst_bytes=_sz,
                                   note=f"-> {dst.name}")
            n += 1
        except OSError as e:
            log_err(item["old"], f"改名失败: {e}")
            ledgerio.log("rename", "FAIL", src_path=str(item["src"]), note=f"改名失败: {e}")
            _bstep(bid, fid_of.get(item["src"]), STEP_RENAME, "fail", src=item["src"], error=f"改名失败: {e}")
            statusio.update_detail("rename", item["src"], item["old"], "FAIL", note=f"改名失败: {e}")
    if not dry and mapping and log_path:
        Path(log_path).parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "w", encoding="utf-8") as fh: json.dump(mapping, fh, ensure_ascii=False, indent=1)
        print(f">>> 改名映射已写入 {log_path}（回滚：--step rollback-rename）")
    return n, mapping

def rollback_renames(log_path, dry=False):
    """按改名映射回滚（把 to 改回 from）。安全要求（都是踩过的坑）：支持 dry=True 逐条预演不落盘；
    目标名已存在时**跳过并告警**（绝不覆盖已有文件）；源名不存在时**跳过并告警**（不在预期状态，
    需人工核对）；返回 (成功数, 跳过数, 失败数)，全部打印，不静默。"""
    with open(log_path, encoding="utf-8") as fh: mapping = json.load(fh)
    ok = skip = fail = 0
    for m in reversed(mapping):
        cur, tgt = Path(m["to"]), Path(m["from"])
        if not cur.exists():
            print(f"  [跳过] 当前文件不存在: {cur.name}")
            skip += 1
            continue
        if tgt.exists():
            print(f"  [跳过] 回滚目标已存在，拒覆盖: {tgt.name}")
            skip += 1
            continue
        if dry:
            print(f"  {cur.name}\n      -> 回滚为 {tgt.name}")
            ok += 1
            continue
        try:
            tgt.parent.mkdir(parents=True, exist_ok=True)
            os.replace(str(cur), str(tgt))
            print(f"  [OK] {cur.name} -> {tgt.name}")
            ok += 1
        except OSError as e:
            print(f"  [失败] {cur.name}: {e}")
            fail += 1
    tag = "(dry-run)" if dry else ""
    print(f">>> {tag} 回滚：成功 {ok} / 跳过 {skip} / 失败 {fail}")
    return ok, skip, fail

def _ask_rename_choice():
    """重命名三选：1=生成建议清单 2=应用重命名 3=跳过（默认 1，零风险）。
    AUTO_MODE：自动选 2（应用重命名），重名仍自动加 _1/_2 尾缀，绝不覆盖。"""
    if AUTO_MODE:
        print("  [自动模式] 重命名操作 -> [2] 应用重命名（自动接受建议）")
        return "apply"
    print("\n请选择重命名操作：")
    print("  [1] 生成建议清单（只预览，不改名）")
    print("  [2] 应用重命名（真改名；重名自动加 _1/_2 尾缀，绝不覆盖）")
    print("  [3] 跳过重命名")
    try: c = input("请输入 [1/2/3]: ").strip() or "1"
    except (EOFError, KeyboardInterrupt):
        print()
        c = "1"
    return {"1": "list", "2": "apply", "3": "skip"}.get(c, "list")

def step_rename(_con=None, root=None, limit=None, dry=True, use_ocr=False,
                use_text=True, log_path=None, batch_id=None, ask=False,
                fix_conflicts=False):
    """重命名步骤。批次模式：文件范围来自当前批次 manifest（scan_pdfs("cleaned")），禁止全目录
    扫描；显式传 root 时为手工模式。ask=True 时询问三选：建议清单/应用/跳过；跳过也写
    pipeline.db: rename skipped，并提示打包文件名可能未优化。
    fix_conflicts=True（--fix-conflicts-by-body）：科目冲突条目转为「正文优先」改名建议，
    随本轮一并应用（CLI 显式传参即人工确认；仅单步 rename 可用，绝不参与 auto/all）；
    无论哪种模式，冲突审核表 _报告\\rename_conflicts.xlsx 均刷新留档。"""
    bid = _resolve_bid(batch_id)
    fid_of, files = {}, None
    if root:
        base = Path(root)
        if bid:
            print(">>> [rename] 手工模式（显式 root）：不按批次 manifest 过滤")
            bid = None
    else:
        base = STAGE_CLEANED
        if bc is not None and bid:
            pairs = _scan("cleaned", bid)
            files = [p for _f, p in pairs]
            fid_of = {p: _f for _f, p in pairs}
            if not files:
                print(f">>> [rename] 本批次在「去水印后」目录没有 PDF（批次 {bid}）。")
                print("        若刚去过水印，请确认产物已生成；否则先跑 步骤2 去水印。")
                return 0
        else:
            print(">>> [rename] 没有当前批次（_批次/current.json 不存在）。")
            print("        请先执行 步骤1 下载 + 步骤2 去水印，或显式传 --rename-root <目录>。")
            return 0
    print(f">>> [rename] 根目录 {base}（{'dry-run 预览' if dry else '正式执行'}）")
    if not base.is_dir():
        log_err(str(base), "目录不存在")
        return 0
    plan = plan_renames(base, limit=limit, use_text=use_text, use_ocr=use_ocr, files=files)
    if fix_conflicts and not dry:
        _np = _promote_conflicts_by_body(plan)
        print(f">>> [rename] --fix-conflicts-by-body：{_np} 条科目冲突转为正文优先改名建议（随本轮应用并留档）")
    elif fix_conflicts and dry:
        print(">>> [rename] [!] --fix-conflicts-by-body 需配合 --rename-apply 才生效（当前 dry-run 仅预览不改名）")
    # 冲突审核表：无论有无改名建议都刷新（含 by_body_fix 状态），真事故文件逐条可审可处理
    write_rename_conflicts_xlsx(plan)
    changed = [x for x in plan if x["changed"]]
    print(f"    共扫描 {len(plan)} 个，建议改名 {len(changed)} 个")
    statusio.begin_step("rename", total=len(changed), out_dir=str(base))
    if not changed:
        print("    无需改名。")
        for item in plan: _wskip(bid, fid_of.get(item["src"]), STEP_RENAME, item["src"], dry, "无需改名")
        statusio.end_step("rename", note="无需改名")
        return 0
    for item in changed[:50]:
        got = " ".join(v for v in item["fields"].values() if v)
        print(f"  · {item['old']}")
        print(f"      -> {item['new_stem']}.pdf")
        print(f"         识别字段: {got}")
    if len(changed) > 50: print(f"  ... 其余 {len(changed) - 50} 个略")
    # ---- 决策：三选（ask=True 时必须明确询问；否则按 dry 参数） ----
    mode = "apply" if not dry else "list"
    if ask: mode = _ask_rename_choice()
    if mode == "skip":
        print(">>> [rename] 已跳过重命名（pipeline.db: rename skipped）。")
        print("    提示：打包文件名可能未优化（仍使用原始文件名）。")
        for item in plan: _wskip(bid, fid_of.get(item["src"]), STEP_RENAME, item["src"], False, "用户选择跳过重命名")
        statusio.end_step("rename", status="SKIP", note="用户选择跳过（skipped）")
        return 0
    if mode == "list":
        print(">>> [rename] 仅生成建议清单，未执行改名（pipeline.db 不写 ok）。")
        # 看板：dry-run 也逐条记 SKIP 明细（不落盘任何文件）
        for _x in changed:
            statusio.update_detail("rename", _x["src"], _x["old"], "SKIP", note=f"建议清单 -> {_x['new_stem']}.pdf")
        statusio.end_step("rename", status="SKIP", note="建议清单（未改名）")
        print("    提示：加 --rename-apply 才会真正改名；或在向导里选 [2] 应用重命名。")
        return len(changed)
    # ---- mode == "apply"：正式改名 ----
    n, _ = apply_renames(plan, dry=False, log_path=log_path, bid=bid, fid_of=fid_of)
    for item in plan:   # 无需改动的文件落 skip 终态（状态表完整）
        if not item["changed"]: _wskip(bid, fid_of.get(item["src"]), STEP_RENAME, item["src"], False, "无需改名")
    print(f">>> [rename] 完成 {n} 个（重名自动加 _1/_2 尾缀，绝不覆盖）")
    statusio.end_step("rename", note=f"完成 {n} 个")
    return n
