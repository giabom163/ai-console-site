# -*- coding: utf-8 -*-
"""
gaokzx.com 采集器 · 解析层
==========================

站点结构（2026-09 实测）
-----------------------
1) 专题页  https://www.gaokzx.com/gk/shitiku/<id>.html
   只是个"目录页"，正文里放 <table>，每行是 [区][学校][科目试题链接]。
   * 专题1(145733)：3 列 —— 区 | 中学名称 | 月考试题
   * 专题2(151404)：5 列 —— 城区 | 科目 | 2026.1 | 2025.1 | 2024.1
   表格里大量用 rowspan（"东城区"跨 5 行），必须做 rowspan 展开。

2) 详情页  https://www.gaokzx.com/gk/shitiku/<id>.html
   正文是题目内容；真正的附件地址藏在
   <script id="__NUXT_DATA__"> 里的 Nuxt/devalue 载荷中：

       {"fileName":854,"show":20,"fileUrl":855}
       "2026北京育才学校高三3月月考数学   有答案.pdf"      <- 下标 854
       "https://cdn.gaokzx.com/zixunzhan/....pdf"       <- 下标 855

   附件都挂在 data["id<ID>-columnData/shitiku-pcTypetrue/
   informationManagement/app/info"].contentModule[] 下。
   contentModule 里既有 .pdf（真附件）也有 .png（"点击进群"推广图），
   所以必须按扩展名过滤。

devalue 编码规则
----------------
* 顶层是一个扁平数组 payload[]；
* 数组每一项要么是字面量，要么是「容器描述」：
    对象 {"key": <下标>, ...}     数组 [<下标>, <下标>, ...]
* 关键：容器里的每个值都是**下标**，字符串/数字也不例外
  （devalue 会去重后全塞进扁平数组，互相用下标引用）；
* 负数是特殊值：-1=undefined  -2=空洞  -3=NaN  -4=+Inf  -5=-Inf  -6=-0；
* ["Reactive", n] 之类标记：真实内容是下标 n 指向的值。
"""
import json
import re
from html import unescape

PAYLOAD_RE = re.compile(r'<script[^>]*id="__NUXT_DATA__"[^>]*>(.*?)</script>', re.S)
REACTIVE_MARKERS = {"Reactive", "ShallowReactive", "Ref", "ShallowRef", "EmptyRef", "Raw"}
FUNC_MARKERS = {"Date", "BigInt", "RegExp", "Set", "Map", "URL", "URLSearchParams"}


# --------------------------------------------------------------------------
# devalue 解码
# --------------------------------------------------------------------------
class Devalue:
    UNDEFINED, HOLE, NAN, POS_INF, NEG_INF, NEG_ZERO = -1, -2, -3, -4, -5, -6

    def __init__(self, payload):
        self.p = payload
        self._memo = {}

    def get(self, idx, _depth=0):
        if not isinstance(idx, int):
            return idx
        if idx == self.NAN:
            return float("nan")
        if idx == self.POS_INF:
            return float("inf")
        if idx == self.NEG_INF:
            return float("-inf")
        if idx == self.NEG_ZERO:
            return 0
        if idx < 0 or idx >= len(self.p):
            return None
        if idx in self._memo:
            return self._memo[idx]
        if _depth > 80:
            return None
        self._memo[idx] = None          # 先占位，压住环形引用
        v = self._resolve(self.p[idx], _depth)
        self._memo[idx] = v
        return v

    def _resolve(self, v, _depth):
        if isinstance(v, (str, bool, int, float)) or v is None:
            return v
        if isinstance(v, list):
            if not v:
                return []
            head = v[0]
            if isinstance(head, str) and head in REACTIVE_MARKERS | FUNC_MARKERS:
                return self.get(v[1], _depth + 1) if len(v) > 1 else None
            return [self.get(x, _depth + 1) for x in v]
        if isinstance(v, dict):
            return {k: self.get(x, _depth + 1) for k, x in v.items()}
        return v

    def root(self):
        return self.get(0)


def load_payload(html):
    m = PAYLOAD_RE.search(html)
    if not m:
        return None
    try:
        return Devalue(json.loads(m.group(1)))
    except (ValueError, TypeError, RecursionError):
        return None


def walk(node, path="", _seen=None, containers=True):
    """
    深度遍历还原后的对象，产出 (路径, 值)。带环保护。

    注意：containers=True 时 dict / list 节点**自己也会被吐出**。
    这是必须的 —— 找 {"fileName":..,"fileUrl":..} 这种结构要判字典节点，
    只吐叶子（标量）的话条件永远不成立（踩过这个坑）。
    """
    if _seen is None:
        _seen = set()
    if isinstance(node, (dict, list)):
        if id(node) in _seen:
            return None      # 生成器内与裸 return 等价，显式写出以保持返回形式一致
        _seen.add(id(node))
    if isinstance(node, dict):
        if containers:
            yield path, node
        for k, v in node.items():
            yield from walk(v, f"{path}.{k}", _seen, containers)
    elif isinstance(node, list):
        if containers:
            yield path, node
        for i, v in enumerate(node):
            yield from walk(v, f"{path}[{i}]", _seen, containers)
    else:
        yield path, node


# --------------------------------------------------------------------------
# 详情页：抽附件
# --------------------------------------------------------------------------
PDF_EXT = (".pdf",)
IMG_EXT = (".png", ".jpg", ".jpeg", ".gif", ".webp")


def extract_files(html, include_images=False):
    """
    抽出详情页的附件。返回 [{"fileName","fileUrl","kind","path"}]

    kind: pdf  = 真正要下载的试卷
          image= 站点推广图（"点击进群领取资料.png" 之类），默认丢弃
    """
    dv = load_payload(html)
    if dv is None:
        return []
    out, seen = [], set()
    for path, v in walk(dv.root()):
        if not isinstance(v, dict) or "fileUrl" not in v:
            continue
        url = v.get("fileUrl")
        if not isinstance(url, str) or not url.startswith("http"):
            continue
        low = url.lower().split("?")[0]
        if low.endswith(PDF_EXT):
            kind = "pdf"
        elif low.endswith(IMG_EXT):
            kind = "image"
        else:
            kind = "other"
        if kind != "pdf" and not include_images:
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append({
            "fileName": (v.get("fileName") or "").strip(),
            "fileUrl": url,
            "kind": kind,
            "path": path,
        })
    return out


def detail_title(html):
    m = re.search(r"<title>(.*?)</title>", html, re.S)
    return unescape(m.group(1)).strip() if m else ""


def detail_text(html):
    """详情页正文纯文本（用于文件名兜底 / 交叉核对）。"""
    m = re.search(r'<div[^>]*class="[^"]*(?:article|content|detail)[^"]*"[^>]*>(.*)', html, re.S | re.I)
    seg = m.group(1) if m else html
    seg = re.sub(r"<script.*?</script>", "", seg, flags=re.S | re.I)
    seg = re.sub(r"<style.*?</style>", "", seg, flags=re.S | re.I)
    t = re.sub(r"<[^>]+>", "\n", seg)
    return unescape(t)


# --------------------------------------------------------------------------
# 专题页：抽表格（rowspan/colspan 展开）
# --------------------------------------------------------------------------
CELL_RE = re.compile(r"<t([dh])\b([^>]*)>(.*?)</t\1>", re.S | re.I)
ROW_RE = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.S | re.I)
ATTR_RE = re.compile(r'(\w[\w-]*)\s*=\s*"([^"]*)"')


def _attrs(s):
    return {k.lower(): v for k, v in ATTR_RE.findall(s)}


def table_grid(html, table_index=0):
    """
    把第 table_index 张表展开成二维网格（rowspan/colspan 已铺平）。
    返回 [[cell_html, ...], ...]；同一个合并单元格会在它覆盖的每格重复出现。
    """
    tables = re.findall(r"<table\b.*?</table>", html, re.S | re.I)
    if table_index >= len(tables):
        return []
    return grid_from_table(tables[table_index])


def grid_from_table(tbl):
    grid = []
    pending = {}   # (row, col) -> (cell_html, attrs, 剩余行数)

    for r, row_html in enumerate(ROW_RE.findall(tbl)):
        while len(grid) <= r:
            grid.append([])
        row = grid[r]
        c = 0

        def fill_pending(r, c):
            """把上面 rowspan 残留的格子补到当前行。"""
            while (r, c) in pending:
                cell_html, attrs, left = pending.pop((r, c))
                while len(row) <= c:
                    row.append("")
                row[c] = cell_html
                if left - 1 > 0:
                    pending[(r + 1, c)] = (cell_html, attrs, left - 1)
                c += 1
            return c

        c = fill_pending(r, 0)
        for ttag, attr_s, inner in CELL_RE.findall(row_html):
            a = _attrs(attr_s)
            rowspan = int(a.get("rowspan", 1) or 1)
            colspan = int(a.get("colspan", 1) or 1)
            if ttag.lower() == "d":
                c = fill_pending(r, c)
            while len(row) <= c:
                row.append("")
            for k in range(colspan):
                while len(row) <= c + k:
                    row.append("")
                row[c + k] = inner
            if rowspan > 1:
                for k in range(colspan):
                    pending[(r + 1, c + k)] = (inner, a, rowspan - 1)
            c += colspan
        fill_pending(r, c)
    return grid


def cell_links(cell_html):
    """取单元格里的 (显示文本, href) 列表。"""
    links = []
    for m in re.finditer(r'<a\b[^>]*href="([^"]+)"[^>]*>(.*?)</a>', cell_html, re.S | re.I):
        href, inner = m.group(1), m.group(2)
        txt = unescape(re.sub(r"<[^>]+>", "", inner)).replace("\xa0", " ")
        txt = re.sub(r"\s+", " ", txt).strip()
        links.append((txt or "(无标题)", href))
    return links


def cell_text(cell_html):
    t = re.sub(r"<[^>]+>", " ", cell_html)
    return re.sub(r"\s+", " ", unescape(t)).replace("\xa0", " ").strip()


def parse_topic(html):
    """
    解析专题页 -> [ {"section","group","col","text","url","row","col_idx"} ]

    一个专题页可能有**多张表**（专题1 就是 8 张 = 8 个月），
    所以逐表解析，并把紧邻表格上方的 <h2> 作为 section 归属。

    设计取向：**表头感知 + 左邻上下文**，不写死列号。
    站点加列/换顺序都不会崩 —— 这正是手写正则"规则一变就失效"的病根。
    """
    out = []
    for m in re.finditer(r"<table\b.*?</table>", html, re.S | re.I):
        tbl = m.group(0)
        grid = grid_from_table(tbl)
        if not grid:
            continue
        section = _preceding_h2(html, m.start())
        out.extend(_records_from_grid(grid, section))

    # 去重：key 里带上 text —— 同一 URL 可能被表格里不同行引用
    # （站点自己贴错链接的情况，如"化学试题"和"数学试题"都指向同一页）。
    # 那种要保留成两条记录，让清单能看出站点的数据问题；
    # 只有 rowspan 展开造成的**完全重复**（同上下文同文字）才合并。
    merged = {}
    order = []
    for x in out:
        key = (x["url"], tuple(x["group"]), x["col"], x["section"], x["text"])
        if key in merged:
            continue
        merged[key] = x
        order.append(key)
    return [merged[k] for k in order]


def _preceding_h2(html, pos):
    hs = re.findall(r"<h2[^>]*>(.*?)</h2>", html[:pos], re.S | re.I)
    if not hs:
        return ""
    t = re.sub(r"<[^>]+>", "", hs[-1])
    return re.sub(r"\s+", " ", unescape(t)).strip()


def _records_from_grid(grid, section=""):
    # 找表头行：取「第一个含链接的行」的**上一行**。
    # 不要简单取第一行 —— 专题2 的 <thead> 是个 colspan=5 的大标题，
    # 真正的列名（城区/科目/2026.1/...）在它下面一行。
    first_link_row = None
    for i, row in enumerate(grid):
        if any("<a" in (c or "").lower() for c in row):
            first_link_row = i
            break

    header, header_row_i = [], -1
    if first_link_row is not None:
        for j in range(first_link_row - 1, -1, -1):
            if not grid[j]:
                continue
            if any("<a" in (c or "").lower() for c in grid[j]):
                continue
            if not any(cell_text(c) for c in grid[j]):
                continue
            header = [cell_text(c) for c in grid[j]]
            header_row_i = j
            break

    records = []
    for r, row in enumerate(grid):
        if r <= header_row_i:
            continue
        for c, cell in enumerate(row):
            if not cell or "<a" not in cell.lower():
                continue
            links = cell_links(cell)
            if not links:
                continue
            left = []
            for k in range(c):
                t = cell_text(row[k]) if k < len(row) else ""
                if t and t not in left:
                    left.append(t)
            col_name = header[c] if c < len(header) else ""
            for txt, href in links:
                records.append({
                    "section": section,
                    "group": left,
                    "col": col_name,
                    "text": txt,
                    "url": href,
                    "row": r,
                    "col_idx": c,
                })
    return records


def abs_url(href, base="https://www.gaokzx.com"):
    if href.startswith("//"):
        return "https:" + href
    if href.startswith("/"):
        return base + href
    return href


if __name__ == "__main__":
    import sys
    f = sys.argv[1] if len(sys.argv) > 1 else "d1.html"
    html = open(f, encoding="utf-8").read()
    print("标题 :", detail_title(html))
    print("附件 :")
    for it in extract_files(html, include_images=True):
        print(f"   [{it['kind']:5}] {it['fileName']}  ->  {it['fileUrl']}")
    recs = parse_topic(html)
    print(f"表格记录数: {len(recs)}")
    for x in recs[:10]:
        print("   ", x["group"], "|", x["col"], "|", x["text"], "|", x["url"])
