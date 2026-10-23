#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""gaokzx 流水线 · 去水印（pwm）—— 2026-10-21 自 pipeline.py 拆出。

职责：WM_* 判据常量、图层水印无损删除、像素水印局部块修补白化、广告页剔除、
step_watermark 调度（多进程子进程 wm_worker.py 隔离 C 层崩溃）。
_wm_process_one 由 wm_worker.py 通过 pipeline re-export 调用（pipeline.py 保持兼容导出）。
依赖 pcore；文件重命名见 prename.py。
"""
from pathlib import Path
import json
import os
import shutil
import subprocess
import time
from pcore import (
    OUT,
    PY,
    ROOT,
    STAGE_ADS,
    STAGE_CLEANED,
    STAGE_DONE,
    STAGE_PENDING,
    STEP_WATERMARK,
    _acquire_lock,
    _ask_apply,
    _bstep,
    _file_present,
    _manifest_curpath,
    _merge_retry,
    _release_lock,
    _resolve_bid,
    _retry_mark_done,
    _scan,
    _sha256_file,
    _skip_ok,
    _wf_bar,
    _wf_install_sigint,
    _wskip,
    bc,
    iter_pdfs,
    ledgerio,
    mark,
    statusio,
)

WM_WORKER = ROOT / "wm_worker.py"      # 单文件去水印子进程入口（隔离 C 层崩溃 + 支持并发）
# 注：OCR_WORKER（wm_ocr_batch_worker.py 入口）服务 step_ocr，定义在 pipeline.py。

WM_DETECT_MIN = 0.001    # 水印检测阈值（灰/浅蓝双通道并集像素占比）；旧版 0.8% 仅灰通道，漏检大量浅蓝水印
WM_BLUE_CLOSE = 101      # 蓝通道闭运算核：桥接水印被逐页差异内容切断的长断段
WM_EXT_BREL = 25         # 蓝扩展区的蓝偏下限（B-R）：越大越只清"纯蓝"，保护插画/照片里的蓝灰色
WM_CORE_MIN = 90# 蓝核心区白化的亮度下限。
                         # 2026-10-04 实测改185 -> 90：残留像素 89% 灰度 < 185（深蓝描边典型 (71,94,112)），
                         # 卡 185 会把水印笔画芯整片留下。配合下面的"遮罩外纯正文 0损伤"实测（见remove_watermark_raster）。
WM_GRAY_MIN = 185        # 灰通道白化的灰度下限。**保持 185，不要下调**（2026-10-04 实测）：
                         # 灰通道靠"跨页一致的中间调灰"抓水印，但正文汉字的抗锯齿边缘同样是
                         # 中间调灰。下调到 90 会把正文笔画边缘整片框进遮罩 -> 缺字破图
                         # （实测正文墨水掉 13.75%，第 7/8/12/13 题明显缺笔画）。
                         # 蓝通道的目标像素天然偏色（R/G/B 明显不等），不需要靠亮度下限来排除正文。
WM_EXT_MIN = 90          # 蓝扩展区白化的亮度下限（2026-10-04 下调）。
                         # 蓝通道判据含"偏蓝"条件(R>70, B-R>25, B-G>2)，正文墨迹近中性不会被框入，
                         # 因此亮度下限可放宽以清掉深蓝描边；实测遮罩外纯正文损伤 0.0000%。
# 橙色 logo 型水印：实测主色 (243,216,174) / (240,216,168)，R-B≈+72、R-G≈+24。
# 旧版无橙通道 -> 残留全落在橙图标上（占残留 100%）；曾试 R-G>28 判据，实测 R-G 仅 +27，差1 单位漏掉。
WM_ORANGE_BREL = 40     # 橙判据的 R-B 下限
WM_ORANGE_RMIN = 190    # 橙判据的 R 下限（排除深色正文笔画）
WM_ORANGE_GMIN = 140    # 橙判据的 G 下限
WM_ORANGE_RG= 14       # 橙判据的 R-G 下限（实测橙图标 R-G 分布 24~32，取 14 留足余量）
# 方法：跨页重复检测 + 像素级白化（栅格重建）+ 后续 OCR 回补文字层。两代水印两通道：1) 灰色：
#   跨页一致 + 中间调灰 + 中央区（排页眉/页脚防误伤）；2) 浅蓝：跨页一致 + 偏色（B>G>R）全页生效
#   （旧版只测灰度曾漏检）。干净文件直接跳过不动原文件。
def _render_rgb(pdf, scale=1.2, max_pages=10):
    """渲染前 N 页为 RGB float32 栈（尺寸归一化到首页）。"""
    import pymupdf as fitz
    import numpy as np
    import cv2
    doc = fitz.open(pdf)
    out = []
    ref = None
    for i in range(min(len(doc), max_pages)):
        pix = doc[i].get_pixmap(matrix=fitz.Matrix(scale, scale))
        arr = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)[..., :3]
        a = arr.astype(np.float32)
        if ref is None: ref = a.shape[:2]
        elif a.shape[:2] != ref:       # 不同页尺寸不一致 -> 归一化到首页尺寸，保证可跨页堆叠
            a = cv2.resize(a, (ref[1], ref[0]))
        out.append(a)
    doc.close()
    return np.stack(out) if out else None

def _wm_masks(stack):
    """由多页 RGB 栈计算低分辨率水印遮罩。返回 (gray_mask, blue_mask)。"""
    import numpy as np
    if stack is None: raise ValueError("_wm_masks: 无有效页栈")
    std = stack.std(axis=0).mean(axis=2)
    mean = stack.mean(axis=0)
    mean_gray = mean.mean(axis=2)
    rr, gg, bb = mean[..., 0], mean[..., 1], mean[..., 2]   # 均值图取色：中位数会丢掉"部分页偏淡"的水印行
    h, w = std.shape
    consistency = std < 16                      # 跨页几乎不变（核心约束）
    mid = (mean_gray > 110) & (mean_gray < 235)  # 中间调灰（排除纯黑字/纯白底）
    central = np.zeros_like(consistency)
    central[int(h * 0.06):int(h * 0.94), int(w * 0.06):int(w * 0.94)] = True   # 灰通道排除页眉页脚
    gray_mask = consistency & mid & central
    # 浅蓝水印：B 明显高于 R/G（半透明蓝叠白底）、R 不太低（排除深蓝 logo/正文）；判据比灰通道宽但"跨页一致"仍核心。
    blue_mask = consistency & (bb - rr > 8) & (bb > 110) & (rr > 70) & (bb - gg > 2)
    return gray_mask, blue_mask

def detect_watermark(pdf):
    """返回 (是否疑似水印, 水印像素占比)；干净文件返回 False。
    灰度与浅蓝双通道取并集（浅蓝水印常极淡，仅灰度会漏检），再做 3x3 开运算滤掉
    孤立的细碎一致性像素（文字抗锯齿噪声/修复残留），只保留成片水印区域。"""
    import cv2
    import numpy as np
    stack = _render_rgb(pdf, scale=1.2, max_pages=10)
    if stack is None or len(stack) < 2: return False, 0.0
    gray_mask, blue_mask = _wm_masks(stack)
    m = (gray_mask | blue_mask).astype(np.uint8)
    m = cv2.morphologyEx(m, cv2.MORPH_OPEN, cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (3, 3)))
    frac = float(m.mean())
    return frac > WM_DETECT_MIN, frac

def _strip_watermark_artifacts(raw):
    """删除内容流中所有 /Artifact ... /Subtype /Watermark ... BDC ... EMC 块。
    平衡扫描，只删标记了 /Watermark 的工件，避免误伤其他内容。返回 (new_str, removed_count)。"""
    out, i, n, removed = [], 0, len(raw), 0
    while i < n:
        bdc = raw.find("BDC", i)
        if bdc == -1:
            out.append(raw[i:]); break
        dict_end = raw.rfind(">>", i, bdc)
        if dict_end == -1:
            out.append(raw[i:]); break
        dict_start = raw.rfind("<<", i, dict_end)
        if dict_start == -1:
            out.append(raw[i:]); break
        if "/Watermark" in raw[dict_start:bdc + 3]:
            depth, p = 1, bdc + 3
            while p < n and depth > 0:
                nb, ne = raw.find("BDC", p), raw.find("EMC", p)
                if ne == -1:
                    p = n; break
                if nb != -1 and nb < ne:
                    depth += 1; p = nb + 3
                else:
                    depth -= 1; p = ne + 3
            out.append(raw[i:dict_start])   # 从 << 到 EMC 整段删除
            removed += 1
            i = p
        else:
            out.append(raw[i:bdc + 3]); i = bdc + 3
    return "".join(out), removed

def has_layer_watermark(pdf):
    """快速判据：任一页内容流出现 /Subtype /Watermark 标记（独立水印图层）。"""
    import pymupdf as fitz
    try: d = fitz.open(pdf)
    except Exception: return False
    try:
        for i in range(len(d)):
            raw = d[i].read_contents().decode("latin-1", "replace")
            if "/Watermark" in raw and "BDC" in raw: return True
        return False
    finally: d.close()

def remove_watermark_layer(pdf, out_pdf):
    """无损删除图层水印：仅剔出页面内容流里的 Watermark 工件块，不栅格化、不丢文字层；
    save(garbage=4) 回收被孤立的水印 XObject。返回真正移除的块数
    （0 表示结构异常，调用方应退回像素级栅格方案）。原文件不动。"""
    import pymupdf as fitz
    d = fitz.open(pdf)
    total = 0
    for i in range(len(d)):
        for xref in d[i].get_contents():
            raw_b = d.xref_stream(xref) or b""
            raw = raw_b.decode("latin-1", "replace")
            if "/Watermark" not in raw or "BDC" not in raw: continue
            new, removed = _strip_watermark_artifacts(raw)
            if removed:
                total += removed
                d.update_stream(xref, new.encode("latin-1"))
    if total == 0:
        d.close()
        return 0
    # ---- 清理悬空 ExtGState 引用（防下游 MuPDF error: cannot find ExtGState resource 'GSx'）----
    # 原理：删 /Watermark 工件块时，块内若定义过 ExtGState（通常 /GS1.../GSn），
    # 页面其他内容仍可能 ` /GSx gs ` 引用已失效资源。这里扫描每页内容流，
    # 收集所有 /GSxxx 名字；与页面 Resources.ExtGState 做差集；对差集里的名字，
    # 直接从内容流删掉 ` /GSxxx gs ` 指令（等价于 no-op，不影响渲染）。
    try:
        import re as _re
        _GS_REF = _re.compile(rb"/(GS\d+)\s+gs\b")
        for _pi in range(len(d)):
            _page = d[_pi]
            try:
                _raw = _page.read_contents()
            except Exception:
                continue
            if not _raw or b"/GS" not in _raw:
                continue
            _refs = set(_GS_REF.findall(_raw))
            if not _refs:
                continue
            # 页面 Resources.ExtGState 现状（缺失即视为悬空）
            _have = set()
            try:
                _res = _page.get_resources() or {}
                _ext = _res.get("ExtGState") or {}
                _have = {str(k).lstrip("/").encode("latin-1", "replace") for k in _ext}
            except Exception:
                _have = set()
            _missing = _refs - _have
            if not _missing:
                continue
            _new = _raw
            for _m in _missing:
                # 删 ` /GSx gs`（保留其他空格/换行，删前后紧邻的单个空格避免残留双空格）
                _new = _re.sub(rb"\s*/" + _m + rb"\s+gs\b", b"", _new)
            if _new != _raw:
                try:
                    _page.set_contents(_new)
                except Exception:
                    # set_contents 不可用：退回 update_stream 逐个 contents xref
                    try:
                        for _xr in _page.get_contents():
                            _s = d.xref_stream(_xr) or b""
                            if not _s:
                                continue
                            _s2 = _s
                            for _m in _missing:
                                _s2 = _re.sub(rb"\s*/" + _m + rb"\s+gs\b", b"", _s2)
                            if _s2 != _s:
                                d.update_stream(_xr, _s2)
                    except Exception:
                        pass
    except Exception:
        pass   # 清理失败绝不阻断（宁可留悬空引用，也不能让整步骤崩）
    d.save(out_pdf, deflate=True, garbage=4)
    d.close()
    return total

# 局部块贴图参数：差分掩码按 128px 网格聚合 + 膨胀合并相邻水印块；单页补丁数超过 60 或
# 补丁总面积超过页面 45% 时才退回整页栅格（此时也仅"盖图"，原页内容流与文字层保留）。
WM_PATCH_GRID = 128
WM_PATCH_PAD = 10
WM_PATCH_MAX_N = 60
WM_PATCH_MAX_FRAC = 0.45

def _diff_boxes(diff, grid=WM_PATCH_GRID, pad=WM_PATCH_PAD):
    """把差分掩码（bool 二维）聚合成贴块矩形列表 [(x0,y0,x1,y1), ...]（像素，已加 pad 外扩）。
    算法：网格化 -> 含差分的格标记 -> 膨胀 1 格合并相邻块 -> 连通域取包围盒。
    统一网格法（而非逐连通域）：限制贴块数量、天然合并散字水印，成本可控。"""
    import cv2
    import numpy as np
    h, w = diff.shape
    if not diff.any(): return []
    rows = (h + grid - 1) // grid
    cols = (w + grid - 1) // grid
    cell = cv2.resize(diff.astype(np.uint8), (cols, rows),
                      interpolation=cv2.INTER_AREA) > 0
    cell_u8 = cv2.dilate(cell.astype(np.uint8), np.ones((3, 3), np.uint8), iterations=1)
    n, lab = cv2.connectedComponents(cell_u8, connectivity=8)
    boxes = []
    for k in range(1, n):
        ys, xs = np.where(lab == k)
        x0, x1 = int(xs.min()), int(xs.max())
        y0, y1 = int(ys.min()), int(ys.max())
        boxes.append((max(0, x0 * grid - pad), max(0, y0 * grid - pad),
                      min(w, (x1 + 1) * grid + pad), min(h, (y1 + 1) * grid + pad)))
    return boxes

def remove_watermark_raster(pdf, out_pdf, scale=2.0):
    """像素级去水印（局部块修补版）：灰度+浅蓝双通道遮罩标记水印像素，**只在掩码命中区域**
    做白化重绘小块、贴回原页——原页面内容流与文字层全部保留（不再整页栅格重建、不再丢文字层）。
    仅当单页补丁过多/占比过大时才退回整页栅格；即便整页栅格也只是在原页上盖一层图，
    文字层仍在（OCR 阶段按页判断文字层，自然只补真正缺层的页）。
    返回 dict：{mode: patch|full|copy, raster_pages: [整页栅格页号...], n_patch: 总块数, note}。
    原文件不动，成果写到 out_pdf；同路径调用（in-place）也安全（临时文件 + 原子替换）。"""
    import pymupdf as fitz
    import cv2
    import numpy as np
    import tempfile
    rep = {"mode": "patch", "raster_pages": [], "n_patch": 0, "note": "",
           "wm_px_total": 0, "wm_frac": 0.0}
    stack = _render_rgb(pdf, scale=1.2, max_pages=10)
    if stack is None or len(stack) < 2:      # 空/单页 PDF 无跨页信息可判 -> 原件拷贝
        rep["mode"] = "copy"
        if os.path.abspath(pdf) != os.path.abspath(out_pdf):
            shutil.copy2(pdf, out_pdf)
        return rep
    gray_mask, blue_mask = _wm_masks(stack)
    # 跨页一致性只命中"白底上的水印芯"（压在内容上的随底色变化，不具一致性）。先轻度膨胀盖抗锯齿
    # 边缘，再闭运算桥接断段（蓝通道更长）。白化两档：核心区宽松、扩展区收紧，保护深色像素（防"静止"等被抹）。
    kern = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (5, 5))
    kern_c13 = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (13, 13))
    kern_cb = cv2.getStructuringElement(cv2.MORPH_ELLIPSE, (WM_BLUE_CLOSE, WM_BLUE_CLOSE))
    gray_mask = cv2.dilate(gray_mask.astype(np.uint8), kern, iterations=1)
    gray_mask = cv2.morphologyEx(gray_mask, cv2.MORPH_CLOSE, kern_c13)
    gray_mask = cv2.dilate(gray_mask, kern, iterations=1)
    blue_core = cv2.dilate(blue_mask.astype(np.uint8), kern, iterations=1)
    blue_big = cv2.morphologyEx(blue_core, cv2.MORPH_CLOSE, kern_cb)
    blue_big = cv2.dilate(blue_big, kern, iterations=1)
    doc = fitz.open(pdf)
    out = fitz.open()
    out.insert_pdf(doc)                      # 关键：原页原样保留（内容流 + 文字层 + 注释）
    for i in range(len(doc)):
        pix = doc[i].get_pixmap(matrix=fitz.Matrix(scale, scale))
        arr = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)[..., :3].copy()
        h, w = arr.shape[:2]
        mg = cv2.resize(gray_mask, (w, h), interpolation=cv2.INTER_NEAREST) > 0
        core = cv2.resize(blue_core, (w, h), interpolation=cv2.INTER_NEAREST) > 0
        big = cv2.resize(blue_big, (w, h), interpolation=cv2.INTER_NEAREST) > 0
        ext = big & ~core
        vals = arr.mean(axis=2)
        ai = arr.astype(np.int16)
        rr, gg, bb = ai[..., 0], ai[..., 1], ai[..., 2]
        # 全分辨率下的浅蓝判据（比检测稍宽，覆盖笔画边缘过渡）
        # 注：rr>70 这一条是正文保护的关键——正文墨迹的抗锯齿边缘虽然也是中间调，
        # 但近中性（B-R 很小），会被"偏蓝"条件挡掉。
        bluish = (bb - rr > 12) & (bb > 110) & (rr > 70) & (bb - gg > 2)
        bluish_ext = (bb - rr > WM_EXT_BREL) & (bb > 110) & (rr > 70) & (bb - gg > 2)
        # 橙色 logo 型水印（2026-10-04 新增）：站标 logo 是橙色方块 + 橙色文字，只判蓝会整片残留。
        orange = ((rr - bb > WM_ORANGE_BREL) & (rr > WM_ORANGE_RMIN)
                  & (gg > WM_ORANGE_GMIN) & (rr - gg > WM_ORANGE_RG))
        # 1) 灰通道区：中间调灰（>WM_GRAY_MIN=185）置白。
        #    2026-10-04 关键修正：灰通道必须加"偏色排除"。
        #      灰通道靠"跨页一致的中间调灰"抓水印，但正文汉字的抗锯齿边缘同样是中间调灰，
        #      一旦被框进遮罩就会连笔画一起抹掉 -> 缺字破图（实测正文墨水掉 12.97%）。
        #      真实水印（无论蓝还是橙）都显著偏色；正文墨迹近中性（R≈G≈B）。
        #      故要求灰通道命中像素同时满足"明显偏蓝"或"明显偏橙"，中性灰一律不动。
        mg_hit = (mg & (vals > WM_GRAY_MIN) & ((bb - rr > 10) | (vals <= 235))
                  & ((bb - rr > 6) | (rr - bb > 12)))
        # 2) 蓝核心区：仅"偏蓝够亮（>WM_CORE_MIN=90）"像素 —— 实测吃墨迹 0；
        # 3) 蓝扩展区：同上（>WM_EXT_MIN=90）；
        # 4) 橙通道：logo 专用，实测吃墨迹 25（可忽略）。
        wm_px = (mg_hit | (core & bluish & (vals > WM_CORE_MIN)) | (ext & bluish_ext & (vals > WM_EXT_MIN))
                 | ((core | ext) & orange))
        if not wm_px.any():
            continue                          # 本页无水印像素 -> 页面不动（零成本）
        rep["wm_px_total"] += int(wm_px.sum())
        rep["wm_frac"] = rep["wm_px_total"] / float(w * h * max(1, len(doc)))
        inp = arr.copy()
        inp[wm_px] = 255
        diff = (inp != arr).any(axis=2)
        boxes = _diff_boxes(diff)
        pg = out[i]
        page_w, page_h = pg.rect.width, pg.rect.height
        # 换算比例：像素 -> 页面 pt（用实际 pix 尺寸，避免 round 误差）
        sx, sy = page_w / float(w), page_h / float(h)
        frac = (sum((b[2] - b[0]) * (b[3] - b[1]) for b in boxes)
                / float(w * h)) if boxes else 0.0
        # 2026-10-04 修复「静默零补丁」：_diff_boxes 在 scale>=2.0 时几乎必返空
        #   （WM_PATCH_GRID=128 网格 + INTER_AREA降采样会把稀疏水印笔画抹平，
        #     每格平均命中<0.5 -> 全部格子判空 -> boxes=[] -> 零补丁 -> 函数仍"成功"返回）。
        #   历史后果：整批文件去水印实际未生效，但日志报 73/73 成功。
        #   对策：boxes 为空但确有水印像素时，退整页栅格（绝不静默跳过）。
        if (not boxes) or len(boxes) > WM_PATCH_MAX_N or frac > WM_PATCH_MAX_FRAC:
            # 补丁过多/占比过大：整页栅格（仍是"原页 + 盖图"，文字层保留）
            rgb = inp[..., ::-1]
            _ret, buf = cv2.imencode(".png", rgb, [cv2.IMWRITE_PNG_COMPRESSION, 2])
            pg.insert_image(pg.rect, stream=buf.tobytes())
            rep["raster_pages"].append(i)
            continue
        for (x0, y0, x1, y1) in boxes:
            patch = inp[y0:y1, x0:x1, ::-1]   # BGR
            _ret, buf = cv2.imencode(".png", patch, [cv2.IMWRITE_PNG_COMPRESSION, 2])
            pg.insert_image(fitz.Rect(x0 * sx, y0 * sy, x1 * sx, y1 * sy),
                            stream=buf.tobytes())
            rep["n_patch"] += 1
    if rep["raster_pages"]:
        rep["mode"] = "full"
        rep["note"] = f"{len(rep['raster_pages'])} 页整页栅格（原文字层仍在）"
    # 2026-10-04 新增验收字段：白化像素总数 + 覆盖率。
    # 判据：单文件 wm_px 覆盖率应 >= 0.5%（低于此说明遮罩没建成，水印其实没清）。
    # 历史教训：n_patch=0 也不代表失败（可能走整页栅格），但 wm_px=0 一定说明没干活。
    if rep["wm_px_total"]:
        rep["note"] = (rep["note"] + " | " if rep["note"] else "") + \
            f"白化 {rep['wm_px_total']} 像素，覆盖 {rep['wm_frac']:.2%}"
    # 统一临时文件 + 原子替换：out_pdf 可能与 pdf 同路径（in-place），防 Windows 文件锁
    _fd, _tmp = tempfile.mkstemp(suffix=".pdf", dir=str(os.path.dirname(out_pdf) or "."))
    os.close(_fd)
    try:
        out.save(_tmp, deflate=True, garbage=4)
    finally:
        out.close()
        doc.close()
    os.replace(_tmp, out_pdf)
    return rep
# ------------------------- 步骤 4（前置）：末尾广告页 / 页脚推广行（无损） -------------------------
# 两者都"低风险、不碰像素"：* 末尾广告页：源文件末 1~N 页为站点推广页（"北京高考在线平台简介…"/"专注北京
#   高中升学…gaokzx…"），在【源】上按文字签名定页数，对【输出 PDF】无损删末 N 页（输出与源页数一致）。
# * 页脚推广行：少数矢量文件底部"关注北京高考在线官方微信：京考一点通…"矢量文字用 redact 标注 + apply_redactions()
#   无损覆盖（栅格化输出无文字层 -> no-op）。
# 识别必须在【源】上做（栅格化后文字层判定失效）；签名只用推广页专属词，'北京高考在线'/'京考一点通' 绝不可纳入
#   （否则尾页向前扫描误删正文页，已实测误删 8~15 页）。
AD_SIG = ("北京高考在线平台简介", "专注北京高中升学", "gaokzx")
FOOTER_PROMO_KEYWORDS = ("关注北京高考在线", "北京高考在线官方微信")

def _safe_save_replace(d, out_pdf):
    """PyMuPDF 同路径保存有潜在问题，统一先写临时文件再原子替换。
    注意：必须在关闭文档(释放对 out_pdf 的句柄)后再 replace，否则 Windows 下因文件被占用而 PermissionError。"""
    import tempfile
    import os as _os
    fd, tmp = tempfile.mkstemp(suffix=".pdf", dir=str(_os.path.dirname(out_pdf) or "."))
    _os.close(fd)
    try: d.save(tmp, deflate=True, garbage=4)
    finally: d.close()
    _os.replace(tmp, out_pdf)

def count_trailing_ad_pages(src):
    """在【源文件】上探测末尾广告页数量（尾页向前，遇首个非广告页即止）。
    广告页特征：页内文字含推广签名（站点名/域名/公众号）且文字量在正常推广区间。
    返回广告页数（0 表示无）。栅格化后的输出整页变成一张大图，会破坏文字层判定，故必须在源上探测。"""
    import pymupdf as fitz
    try: d = fitz.open(src)
    except Exception: return 0
    try:
        n = len(d)
        cnt = 0
        for i in range(n - 1, -1, -1):
            try: text = d[i].get_text()
            except Exception: break
            t = text.strip()
            # 仅用下界过滤空白/极短页（上界已移除——AD_SIG 才是真判据，且避免超长推广页被上界误判漏删）。
            if len(t) < 30: break
            if not any(sig in t for sig in AD_SIG): break
            cnt += 1
        return cnt
    finally: d.close()

def delete_trailing_ad_pages(out_pdf, n_ad):
    """从已生成的【输出 PDF】末尾删除 n_ad 页（广告页）。无损（仅删页，不碰其余页像素）。
    输出与源页数一致，故删源末 n_ad 页 == 删输出末 n_ad 页。返回实际删除数（0 表示无操作）。"""
    if n_ad <= 0: return 0
    import pymupdf as fitz
    import os as _os
    if not _os.path.exists(out_pdf): return 0
    d = fitz.open(out_pdf)
    try:
        if n_ad >= len(d): return 0
        for _ in range(n_ad): d.delete_page(len(d) - 1)
        _safe_save_replace(d, out_pdf)
        d = None
        return n_ad
    finally:
        if d is not None: d.close()

def remove_footer_promo(pdf, out_pdf):
    """无损删除矢量页脚推广行：对页脚区域(y>0.86*页高)命中推广关键词的文字 span
    加 redact 标注并 apply_redactions()（白底覆盖）。只动矢量文字、不碰栅格/烧入内容；
    栅格化输出无文字层 => 自动 no-op。返回是否改动。"""
    import pymupdf as fitz
    d = fitz.open(pdf)
    changed = False
    try:
        for pg in d:
            try: blocks = pg.get_text("dict")["blocks"]
            except Exception: continue
            page_h = pg.rect.height
            for b in blocks:
                if b.get("type") != 0:   # 仅处理文字块
                    continue
                for line in b.get("lines", []):
                    for span in line.get("spans", []):
                        y0 = span["bbox"][1]
                        txt = span["text"]
                        if y0 > page_h * 0.86 and any(k in txt for k in FOOTER_PROMO_KEYWORDS):
                            pg.add_redact_annot(fitz.Rect(*span["bbox"]), fill=(1, 1, 1))
                            changed = True
        if changed:
            for pg in d: pg.apply_redactions()
            _safe_save_replace(d, out_pdf)
            d = None
    finally:
        if d is not None: d.close()
    return changed

def _wm_process_one(pdf, out_root, src_root=None, dry=False):
    """单文件完整去水印：探测（图层/像素/广告页）+ 路线分流 + 落盘。**不写 DB**。专供 wm_worker.py
    在独立子进程里调用——子进程本身即崩溃隔离边界，故直接调用各探测函数（不再嵌套 _probe_source
    的二级子进程）。返回 dict：file/name/status(ok|none|fail|dry)/route/frac/n_ad/removed/footer/ad_del/err。"""
    src_root = Path(src_root) if src_root else OUT
    rel = os.path.relpath(pdf, str(src_root))
    outp = os.path.join(str(out_root), rel)
    name = os.path.basename(pdf)[:38]
    has_layer = has_layer_watermark(pdf)
    ok, frac = detect_watermark(pdf)
    n_ad = count_trailing_ad_pages(pdf)
    base = {"file": pdf, "name": name, "frac": round(float(frac), 6), "n_ad": int(n_ad)}
    if dry:
        route = "layer" if has_layer else ("raster" if ok else "none")
        return {**base, "dry": True, "route": route, "status": "dry"}
    os.makedirs(os.path.dirname(outp), exist_ok=True)
    # 路线 1：图层水印 -> 无损删（不栅格化、保留文字层）
    if has_layer:
        try: removed = remove_watermark_layer(pdf, outp)
        except Exception: removed = 0
        if removed > 0:
            footer = bool(remove_footer_promo(outp, outp))
            # 图层删后复检：仍检出像素水印（烧入式）则再用局部块修补白化（保留原文字层）。
            try: ok2, _ = detect_watermark(outp)
            except Exception: ok2 = False
            _rep2 = {}
            if ok2:
                try: _rep2 = remove_watermark_raster(outp, outp)
                except Exception: _rep2 = {}
            ad = delete_trailing_ad_pages(outp, n_ad)
            return {**base, "route": "layer", "status": "ok",
                    "removed": removed, "raster_followup": bool(ok2),
                    "footer": footer, "ad_del": ad,
                    "n_patch": _rep2.get("n_patch"),
                    "raster_pages": _rep2.get("raster_pages") or []}
        # 移除为 0 -> 结构异常，落到下方像素级兜底
    # 路线 2：无水印（也不含可删图层水印）
    if not has_layer and not ok:
        # 即便确属无水印，也要把源文件拷贝进产物目录，保证产物是完整 581 镜像
        shutil.copy2(pdf, outp)
        ad = delete_trailing_ad_pages(outp, n_ad)
        if n_ad > 0: return {**base, "route": "copy_ad", "status": "ok", "ad_del": ad}
        return {**base, "route": "none", "status": "ok", "ad_del": ad}
    # 路线 3：像素水印（烧入扫描图）-> 局部块修补白化（保留原页内容流与文字层；仅补丁
    # 过多/占比过大时才整页盖图，且页号写入 raster_pages 供日志追溯）。
    _rep3 = remove_watermark_raster(pdf, outp)
    ad = delete_trailing_ad_pages(outp, n_ad)
    return {**base, "route": "raster", "status": "ok", "ad_del": ad,
            "wm_mode": _rep3.get("mode"), "n_patch": _rep3.get("n_patch"),
            "raster_pages": _rep3.get("raster_pages") or []}

def _wm_consume(con, f, res, err, dry):
    """处理子进程返回结果：打印日志 +（非 dry）写 DB 状态。返回 ok/none/fail/dry。"""
    name = os.path.basename(f)[:38]
    if res is None:
        if not dry: mark(con, f, "wm", "fail")
        print(f"[WM] [!] 失败 {name} | {err}")
        return "fail"
    route, st = res.get("route"), res.get("status")
    pct = float(res.get("frac") or 0.0) * 100
    if dry:
        if route == "layer": r = "图层水印 -> 无损删除"
        elif route == "raster": r = f"像素水印(占比 {pct:.2f}%) -> 局部块修补"
        else: r = f"无水印(占比 {pct:.2f}%) 跳过"
        ad_note = f" + 删末尾广告 {res['n_ad']} 页" if res.get("n_ad") else ""
        print(f"[WM] {r}{ad_note} {name}")
        return "dry"
    if st == "ok":
        if route == "layer":
            print(f"[WM] 图层水印 -> 无损删除(移除 {res.get('removed')} 块) {name}")
            if res.get("n_patch"): print(f"[WM]   + 烧入残余局部块修补 {res['n_patch']} 块 {name}")
            if res.get("footer"): print(f"[WM]   + 页脚推广行 矢量删除 {name}")
            if res.get("ad_del"): print(f"[WM]   + 删末尾广告页 {res['ad_del']} 页 {name}")
        elif route == "raster":
            # 局部块修补为主（保留文字层）；整页盖图仅少数页且页号可追溯
            _np = res.get("n_patch")
            _rp = res.get("raster_pages") or []
            _how = (f"局部块修补 {_np} 块" if _np else "白化")
            if _rp: _how += f"，{len(_rp)} 页整页盖图(页号 {','.join(str(x + 1) for x in _rp[:8])}{'...' if len(_rp) > 8 else ''})"
            print(f"[WM] 检测到像素水印(占比 {pct:.2f}%) -> {_how} {name}")
            if res.get("ad_del"): print(f"[WM]   + 删末尾广告页 {res['ad_del']} 页 {name}")
        else:  # copy_ad
            print(f"[WM] 无水印但有广告页 {res.get('n_ad')} 页 -> 拷贝+无损删页 {name}")
        mark(con, f, "wm", "ok")
        return "ok"
    if st == "none":
        print(f"[WM] 无水印(占比 {pct:.2f}%) 跳过 {name}")
        mark(con, f, "wm", "none")
        return "none"
    mark(con, f, "wm", "fail")
    print(f"[WM] [!] 失败({route}) {name} {str(res.get('err', ''))[:200]}")
    return "fail"
def step_watermark(con, out_root=None, limit=None, dry=False, force=False,
                   workers=6, src=None, hardest=None, force_scope="all",
                   batch_id=None, ask_apply=True):
    """去水印主步骤。每个文件在独立子进程里完整处理（probe+route+落盘），workers>1 并发跑：隔离 C 层崩溃又吃满
    多核；DB 状态统一在父进程写（SQLite 单写者）。src/hardest 用于小批量压测（指定源目录 / 只取最大的 N 个）。
    批次模式：文件范围来自当前批次 manifest（scan_pdfs("pending", batch_id)）；只有显式传 --src 时退化为手工目录
    模式。dry-run 预览后询问"是否正式执行？[y/N]"（默认 N），确认后才真跑；只有真跑成功才写 ok（预览不写）。
    force_scope："all"=--force 作用所有文件（整库重跑，慎用）；"new"=只强跑「产物尚不存在」的增量文件（已处理照常跳过）。"""
    import concurrent.futures as cf
    import time
    import json
    if not _acquire_lock():
        print(">>> [wm] 已有其它 step_watermark 实例在运行（lock 未过期），本实例退出以避免并发写冲突")
        return
    try:
        # ---- 取文件范围：批次 manifest 优先（禁止全目录扫描） ----
        bid = _resolve_bid(batch_id)
        fid_of = {}
        if src:
            # 手工/压测模式：显式源目录（--src），不受批次限制；不写批次状态
            print(">>> [wm] [!] 手工模式（--src 显式指定）：不按批次 manifest 过滤，仅用于压测")
            bid = None
            src_root = Path(src)
            files = list(iter_pdfs(str(src_root)))
        else:
            src_root = STAGE_PENDING
            if bc is not None and bid:
                pairs = _scan("pending", bid)
                files = [pp for _f, pp in pairs]
                fid_of = {pp: _f for _f, pp in pairs}
                print(f">>> [wm] 批次 {bid}：待处理目录命中 {len(files)} 个 PDF（manifest 限定）")
                if not files:
                    print(">>> [wm] 本批次在「待处理」目录没有可处理的 PDF" "（可能已全部流转到下一阶段，或尚未下载）。")
                    print(f"        当前批次: {bid}")
                    return
            else:
                print(">>> [wm] 没有当前批次（_批次/current.json 不存在）。")
                print("        请先执行 步骤1 抓取下载（会自动创建批次），或显式传 --src <目录>。")
                return
        out_root = Path(out_root) if out_root else (src_root.parent / (src_root.name + "_去水印后"))
        os.makedirs(str(out_root), exist_ok=True)
        if hardest: files = sorted(files, key=lambda p: os.path.getsize(p), reverse=True)[: int(hardest)]
        # 原地链当前权威路径：下游 rename 改过名的文件，产物判定回落 manifest 当前名（与 reconcile 一致）
        _curpath = _manifest_curpath(bid) if (bc is not None and bid) else {}
        _outp_norm = os.path.normcase(str(out_root)) + os.sep
        todo, skip = [], 0
        skipped_set = set()   # 看板重试并入时用于修正 skip 计数
        for f in files:
            try:
                rel = os.path.relpath(f, str(src_root))
                prod = out_root / rel
                _prod = str(prod) if (prod.exists() and prod.stat().st_size > 0) else None
            except OSError:
                _prod = None
            if _prod is None:
                # 登记旧名不存在：若该文件当前权威路径已在去水印目录内（被 rename 改过名），视为产物存在
                _cp = _curpath.get(fid_of.get(f))
                if _cp and os.path.normcase(_cp).startswith(_outp_norm) and _file_present(_cp):
                    _prod = _cp
            if force and force_scope == "new":
                # 增量模式：产物已存在且非空 => 视为已处理，跳过（不重跑旧成果）
                if _prod:
                    skip += 1
                    skipped_set.add(f)
                    _wskip(bid, fid_of.get(f), STEP_WATERMARK, f, dry, "产物已存在（增量跳过）")
                    continue
            elif _skip_ok(bid, fid_of.get(f), STEP_WATERMARK, _prod or f,
                          extra_ok=bool(_prod), curpath=_curpath) and not force:
                # 三条件（DB ok + 产物存在 + sha 一致）才跳过；FORCE_MISSING 时缺失产物不跳过
                skip += 1
                skipped_set.add(f)
                _wskip(bid, fid_of.get(f), STEP_WATERMARK, f, dry, "断点续传跳过（三条件）")
                continue
            todo.append(f)
            if limit is not None and len(todo) >= limit: break
        # 看板重试队列：被点名的文件强制并入（绕过断点续传/产物已存在跳过，不受 --limit 影响）
        retry_items, skip = _merge_retry(
            "wm", (src_root, STAGE_CLEANED, STAGE_DONE, STAGE_ADS), todo, skipped_set, skip)
        workers = max(1, int(workers or 1))
        print(f">>> [wm] 待处理 {len(todo)}，已跳过 {skip}，并发 {workers}；" f"源: {src_root}；输出: {out_root}")
        if not todo:
            # 空跑防呆：不要只说"完成 0"，要明确告诉用户这是"被跳过"而不是"没问题"
            if skip:
                print(f">>> [wm] [!] 全部 {skip} 个文件都因断点续传被跳过（真跑 0 个）。")
                print(f"        如需强制重跑验证：加 --force")
                print(f"        例： python pipeline.py --step watermark --limit 3 --force")
            else:
                print(f">>> [wm] 源目录 {src_root} 下没有 PDF 文件（真跑 0 个）。")
                print(f"        请确认目录是否正确，或先跑 --step download 下载。")
            return
        statusio.begin_step("wm", total=len(todo), skipped=skip, out_dir=str(out_root), note=f"并发 {workers}")
        def _mark_running(f):
            """看板 + 批次状态：dispatch 时先标 RUNNING/started（失败静默）。"""
            if bc is not None and bid and fid_of.get(f) and not dry:
                bc.db_step(bid, fid_of[f], STEP_WATERMARK, bc.ST_STARTED, src=f)
            try: statusio.update_detail("wm", f, os.path.basename(f), "RUNNING", note="处理中")
            except Exception: pass
        def run_one(f):
            _t_one = time.time()        # 看板：单文件耗时
            cmd = [str(PY), str(WM_WORKER), f, str(out_root), str(src_root), "1" if dry else "0"]
            try: r = subprocess.run(cmd, capture_output=True, timeout=1800)
            except Exception as e: return f, None, f"launch/timeout: {e}"
            if r.returncode != 0:
                tail = r.stderr.decode("utf-8", "replace")[-300:]
                return f, None, f"exit {r.returncode}: {tail}"
            out = r.stdout.decode("utf-8", "replace")
            i = out.find("RESULT")
            if i < 0: return f, None, "no RESULT"
            try:
                res = json.loads(out[i + 6:])
                if isinstance(res, dict):   # 看板：回填单文件耗时（不改子进程协议）
                    res.setdefault("elapsed_sec", round(time.time() - _t_one, 2))
                return f, res, None
            except Exception as e: return f, None, f"bad json: {e}"
        def _wm_stat(fpath, r, res, err):
            """看板 + 台账：单文件结果写进 detail_wm.json 与 file_events。台账里 dst_path = 产物
            绝对路径（去水印后的存放位置）；dry-run(SKIP) 不算实际事件，不落台账。"""
            res = res or {}
            _st = {"ok": "DONE", "none": "DONE", "fail": "FAIL", "dry": "SKIP"}.get(r, "WARN")
            _route = res.get("route", "")
            _note = ""
            if r in ("ok", "dry"):
                if _route == "layer": _note = f"图层水印→无损删除({res.get('removed', '?')}块)"
                elif _route == "raster":
                    _np = res.get("n_patch"); _rp = res.get("raster_pages") or []
                    _note = (f"像素水印→局部块修补({_np}块)" if _np else "像素水印→白化")
                    if _rp: _note += f"；整页盖图{len(_rp)}页(文字层保留)"
                elif _route == "copy_ad": _note = "无水印+删广告页"
                else: _note = "无水印"
                if res.get("ad_del"): _note += f"；删广告页{res['ad_del']}页"
            elif r == "fail": _note = str(res.get("err") or err or "")[:160]
            _outp = None
            _dst_sz = _src_sz = None
            try:
                _rel = os.path.relpath(fpath, str(src_root))
                _outp = os.path.join(str(out_root), _rel)
                _dst_sz = os.path.getsize(_outp) if os.path.exists(_outp) else None
                _src_sz = os.path.getsize(fpath) if os.path.exists(fpath) else None
            except OSError: pass
            if _st != "SKIP":
                try:
                    ledgerio.log("wm", _st, src_path=fpath,
                                 dst_path=(_outp if _outp and os.path.exists(_outp)
                                           else None),
                                 size_bytes=_src_sz,
                                 elapsed_sec=res.get("elapsed_sec"),
                                 note=_note)
                except Exception: pass
            # 批次状态（file_steps）：只有正式执行的真实结果才写（ok/fail）；成功后同步推进 manifest 路径。
            if (bc is not None and bid and fid_of.get(fpath) and not dry):
                _ok = r in ("ok", "none")
                _dstp = _outp if (_outp and os.path.exists(_outp)) else None
                _bstep(bid, fid_of[fpath], STEP_WATERMARK,
                       bc.ST_OK if _ok else bc.ST_FAIL,
                       src=fpath, dst=_dstp,
                       src_sha=(_sha256_file(_dstp) if (_ok and _dstp) else None),
                       error=("" if _ok else _note))
                if _dstp:
                    try: bc.update_file_path(bid, fid_of[fpath], _dstp)
                    except Exception: pass
            try:
                statusio.update_detail("wm", fpath, os.path.basename(fpath), _st,
                                       elapsed_sec=res.get("elapsed_sec"),
                                       src_bytes=_src_sz, dst_bytes=_dst_sz,
                                       note=_note)
            except Exception: pass
        while True:
            t0 = time.time()
            tally = {"ok": 0, "none": 0, "fail": 0}
            _retry_processed = set()    # 看板重试：实际派发过的文件（中断未派发的不算）
            if dry: print(f">>> [wm] (dry-run) 预览中（{len(todo)} 个）：只探测不写产物、不落库")
            # 进度条 + Ctrl+C 安全中断（第一次按下让当前文件收尾后停，状态已逐条落库）
            stop = _wf_install_sigint("去水印")
            bar = _wf_bar(len(todo), "去水印", enabled=not dry)
            with bar as _b:
                if workers == 1:
                    for f in todo:
                        if stop():
                            print("    [!] 收到中断信号，停止派发新文件")
                            break
                        _mark_running(f)
                        rf, res, err = run_one(f)
                        _retry_processed.add(rf)
                        r = _wm_consume(con, rf, res, err, dry)
                        _wm_stat(rf, r, res, err)
                        if not dry: tally[r] = tally.get(r, 0) + 1
                        _b.update(1, label=os.path.basename(f),
                                  ok=(r == "ok"), name=(None if dry else os.path.basename(f)))
                else:
                    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
                        futs = {}
                        for f in todo:
                            _mark_running(f)
                            futs[ex.submit(run_one, f)] = f
                        for fut in cf.as_completed(futs):
                            f = futs[fut]
                            rf, res, err = fut.result()
                            _retry_processed.add(f)
                            r = _wm_consume(con, rf, res, err, dry)
                            _wm_stat(rf, r, res, err)
                            if not dry: tally[r] = tally.get(r, 0) + 1
                            _b.update(1, label=os.path.basename(f),
                                      ok=(r == "ok"), name=(None if dry else os.path.basename(f)))
                            if stop():
                                # 不再等待剩余任务，但已派发的让它自然结束
                                pass
            # 收尾自检：每个源文件在产物目录都应存在对应文件（none 类也会拷贝，见 _wm_process_one 路线 2）。
            missing_out = [
                f for f in files
                if not os.path.exists(os.path.join(str(out_root), os.path.relpath(f, str(src_root))))
            ]
            if dry:
                print(f">>> [wm] (dry-run) 预告 {len(todo)} 个，已跳过 {skip}；输出目录: {out_root}")
                # 用户要求：dry-run 预览后必须询问是否正式执行（默认 N）；确认才真跑，只有真跑成功才写 ok。
                if ask_apply and _ask_apply("去水印"):
                    print(">>> [wm] 确认正式执行 —— 开始真跑（写产物 + 写 pipeline.db）")
                    dry = False
                    continue
                print(">>> [wm] 仅预览，未正式执行（pipeline.db 不写 ok）。")
                break
            print(f">>> [wm] 完成 {tally['ok']}，无水印 {tally['none']}，失败 {tally['fail']}，"
                  f"已跳过 {skip}，耗时 {time.time() - t0:.1f}s；输出目录: {out_root}")
            if missing_out:
                print(f">>> [wm] 自检告警：{len(missing_out)} 个源文件在产物目录缺失：")
                for f in missing_out[:30]: print("   MISSING", f)
            else: print(f">>> [wm] 自检通过：{len(files)} 个源文件均有对应产物")
            break
        if retry_items and not dry:   # 看板重试：已处理到的项标记 done（dry-run 不动队列）
            _retry_mark_done("wm", [r for p, r in retry_items if p in _retry_processed])
        # 看板：正常收尾（dry-run 记 SKIP）
        statusio.end_step("wm", status=("SKIP" if dry else None),
                          note=("dry-run 预览" if dry else
                                f"完成 {tally['ok']} / 无水印 {tally['none']} / "
                                f"失败 {tally['fail']}"))
    except BaseException:   # 看板：异常中断也把状态落成 FAIL，避免看板永远 RUNNING
        statusio.end_step("wm", status="FAIL", note="步骤异常中断（详见控制台）")
        raise
    finally: _release_lock()
