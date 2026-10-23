# -*- coding: utf-8 -*-
"""阶段2 问题3 整专题合并单包 —— 离线单元测试（不联网、绝不碰生产 pipeline.db）。

覆盖：
  A. _pack_mode：默认 topic；GAOKZX_PACK_MODE=batch 覆盖；大小写/空白鲁棒。
  B. step_pack_topic：跨 run 多批次按 manifest file_id 枚举 + resolve 定位，按专题分组，
     一专题一 zip，逐 (bid,fid) 写 step=7 ok（dst=所属专题 zip）。用注入式 FakeBC，不依赖真实台账。
  C. _batch_zips_exist：批次级命中；批次级空 -> run 级回退命中兄弟批次 zip（用真实 batchcore + 临时库）。
  D. step_pack 分派路由：mode/单文件调度 -> legacy/topic 的选择。

运行：python test_topic_pack.py   全 PASS 退出码 0，否则 1。
"""
import contextlib
import os
import sqlite3
import sys
import tempfile
import zipfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import batchcore as bc
import pcore
import ppack

_RESULTS = []


def check(name, cond, detail=""):
    _RESULTS.append(bool(cond))
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f"  — {detail}" if detail and not cond else ""))


# ---------------- A. _pack_mode ----------------
def test_pack_mode():
    print("A. _pack_mode")
    old = os.environ.get("GAOKZX_PACK_MODE")
    try:
        os.environ.pop("GAOKZX_PACK_MODE", None)
        check("默认 = topic", ppack._pack_mode() == "topic", ppack._pack_mode())
        os.environ["GAOKZX_PACK_MODE"] = "batch"
        check("显式 batch", ppack._pack_mode() == "batch")
        os.environ["GAOKZX_PACK_MODE"] = "  TOPIC  "
        check("大小写+空白鲁棒 -> topic", ppack._pack_mode() == "topic", ppack._pack_mode())
    finally:
        if old is None:
            os.environ.pop("GAOKZX_PACK_MODE", None)
        else:
            os.environ["GAOKZX_PACK_MODE"] = old


# ---------------- C. step_pack_topic（FakeBC 注入） ----------------
class _FakeBC:
    ST_PENDING_ACCEPT = "pending_accept"
    ST_OK = "ok"

    def __init__(self, ads, mapping, runs, steps_sink):
        self.ads = ads
        self.mapping = mapping          # (bid, fid) -> abs path
        self.runs = runs                # bid -> run_id
        self.by_run = {}                # run_id -> [bid,...]
        for b, r in runs.items():
            self.by_run.setdefault(r, []).append(b)
        self.steps = steps_sink         # list[(bid,fid,step,status,src,dst,sha)]

    def norm_key(self, p):
        return os.path.normcase(os.path.abspath(str(p)))

    def is_test_path(self, p):
        return False

    def resolve_file_path(self, bid, fid):
        return self.mapping.get((bid, str(fid)))

    def load_manifest(self, bid):
        recs = [{"file_id": f, "path": ""} for (b, f) in self.mapping if b == bid]
        return {"files": recs}

    def db_run_of_batch(self, bid):
        return self.runs.get(bid)

    def db_batches_of_run(self, run_id):
        return list(self.by_run.get(run_id, []))

    def db_step(self, bid, fid, step, status, src=None, dst=None, error="", src_sha=None):
        self.steps.append((bid, str(fid), int(step), status, src, dst, src_sha))
        return True


def test_step_pack_topic():
    print("C. step_pack_topic")
    root = Path(tempfile.mkdtemp(prefix="tpk_"))
    ads = root / "ads"
    packed = root / "packed"
    cleaned = root / "cleaned"
    for t in ("ads", "packed", "cleaned"):
        (root / t).mkdir()

    # 两个批次属同一 run，文件分散在两个专题目录（topicA 有两层子目录、topicB 单层）
    def mkfile(rel):
        fp = ads / rel
        fp.parent.mkdir(parents=True, exist_ok=True)
        fp.write_bytes(b"%PDF-" + rel.encode())
        return str(fp)

    m = {
        ("B1", "F0001"): mkfile(os.path.join("topicA", "海淀", "a1.pdf")),
        ("B1", "F0002"): mkfile(os.path.join("topicA", "a2.pdf")),
        ("B2", "F0003"): mkfile(os.path.join("topicA", "朝阳", "a3.pdf")),   # 专题A 跨到 B2
        ("B2", "F0004"): mkfile(os.path.join("topicB", "b1.pdf")),
    }
    runs = {"B1": "RUN1", "B2": "RUN1"}
    steps = []

    saved = (ppack.bc, pcore.bc, ppack.STAGE_ADS, ppack.STAGE_PACKED, ppack.STAGE_CLEANED,
             pcore.ONLY_FILE_IDS, ppack._LAST_PACK_ZIP, ppack.statusio, ppack.ledgerio, ppack._wf_bar)
    try:
        ppack.bc = _FakeBC(ads, m, runs, steps)
        pcore.bc = ppack.bc          # _bstep/_resolve_bid 在 pcore 内解析 pcore.bc，须同步注入
        ppack.STAGE_ADS, ppack.STAGE_PACKED, ppack.STAGE_CLEANED = ads, packed, cleaned
        pcore.ONLY_FILE_IDS = None
        ppack._LAST_PACK_ZIP = None
        # 打桩外部副作用模块（看板/台账），避免写真实 json/库
        class _Null:
            def __getattr__(self, _n):
                return lambda *a, **k: None
        ppack.statusio = _Null()
        ppack.ledgerio = _Null()
        ppack._wf_bar = lambda *a, **k: contextlib.nullcontext(type("B", (), {"update": lambda *a, **k: None})())

        made = ppack.step_pack_topic(None, None, None, dry=False, batch_id="B1", ask_apply=False)
    finally:
        (ppack.bc, pcore.bc, ppack.STAGE_ADS, ppack.STAGE_PACKED, ppack.STAGE_CLEANED,
         pcore.ONLY_FILE_IDS, ppack._LAST_PACK_ZIP, ppack.statusio, ppack.ledgerio, ppack._wf_bar) = saved

    check("产出 2 个专题 zip", isinstance(made, list) and len(made) == 2, str(made))
    zn = {Path(z).name for z in (made or [])}
    check("zip 名 = topicA.zip / topicB.zip", zn == {"topicA.zip", "topicB.zip"}, str(zn))
    ta = next((z for z in (made or []) if Path(z).name == "topicA.zip"), None)
    tb = next((z for z in (made or []) if Path(z).name == "topicB.zip"), None)
    if ta:
        with zipfile.ZipFile(ta) as zf:
            a_names = {os.path.normpath(n) for n in zf.namelist()}
        check("topicA.zip 含 3 个 PDF（跨 B1/B2）",
              len({n for n in a_names if n.lower().endswith(".pdf")}) == 3, str(a_names))
        check("topicA.zip 保留 海淀/a1.pdf 层级",
              os.path.normpath(os.path.join("topicA", "海淀", "a1.pdf")) in a_names, str(a_names))
    if tb:
        with zipfile.ZipFile(tb) as zf:
            b_names = {os.path.normpath(n) for n in zf.namelist()}
        check("topicB.zip 含 1 个 PDF", len(b_names) == 1, str(b_names))
    # step=7 落库：每个 (bid,fid) 一条 pack ok，dst 指向其所属专题 zip
    ok7 = [s for s in steps if s[2] == ppack.STEP_PACK and s[3] == "ok"]
    got = {(s[0], s[1]) for s in ok7}
    check("4 个文件均写 step=7 ok", got == set(m.keys()), str(got))
    dstA = {s[5] for s in ok7 if Path(s[5]).name == "topicA.zip"}
    dstB = {s[5] for s in ok7 if Path(s[5]).name == "topicB.zip"}
    check("topicA 文件的 dst 指向 topicA.zip", len(dstA) == 1, str(dstA))
    check("topicB 文件的 dst 指向 topicB.zip", len(dstB) == 1, str(dstB))
    # 每个文件都应写 pending_accept(step8)
    acc = {(s[0], s[1]) for s in steps if s[2] == ppack.STEP_ACCEPT and s[3] == "pending_accept"}
    check("4 个文件均置 pending_accept", acc == set(m.keys()), str(acc))


# ---------------- D. _batch_zips_exist run 级回退（真实 batchcore + 临时库） ----------------
def test_batch_zips_fallback():
    print("D. _batch_zips_exist run 级回退")
    tmp = Path(tempfile.mkdtemp(prefix="bzpk_"))
    dbp = tmp / "t.db"
    bc.configure(str(dbp))
    saved_db, saved_bc = pcore.DB, ppack.bc
    try:
        ppack.bc = bc
        pcore.DB = dbp
        run = bc.new_batch_id("run")
        b1 = bc.new_batch_id("batch")
        b2 = bc.new_batch_id("batch")
        bc.db_batch(b1, run_id=run)
        bc.db_batch(b2, run_id=run)
        zfile = tmp / "topic.zip"
        with zipfile.ZipFile(zfile, "w") as zf:
            zf.writestr("x.pdf", b"%PDF-")
        # 只有 b1 写 step=7 ok；b2 不写
        bc.db_step(b1, "F0001", ppack.STEP_PACK, "ok", dst=str(zfile))
        r1 = ppack._batch_zips_exist(b1)
        r2 = ppack._batch_zips_exist(b2)
        check("b1 批次级直接命中", r1 == [str(zfile)], str(r1))
        check("b2 批次级空 -> run 级回退命中 b1 的 zip", r2 == [str(zfile)], str(r2))
        # zip 文件删除后，run 级回退也应因物理不存在而返回空
        zfile.unlink()
        check("zip 物理消失后回退返回空", ppack._batch_zips_exist(b2) == [], "应空")
    finally:
        pcore.DB, ppack.bc = saved_db, saved_bc


def test_dispatcher_routing():
    print("E. step_pack 分派路由")
    saved_bc, saved_only, saved_mode = ppack.bc, pcore.ONLY_FILE_IDS, os.environ.get("GAOKZX_PACK_MODE")
    calls = {"legacy": 0, "topic": 0}
    saved_fn = (ppack.step_pack_legacy, ppack.step_pack_topic)
    try:
        ppack.bc = object()  # 非 None，避开入口短路
        ppack.step_pack_legacy = lambda *a, **k: calls.__setitem__("legacy", calls["legacy"] + 1) or "L"
        ppack.step_pack_topic = lambda *a, **k: calls.__setitem__("topic", calls["topic"] + 1) or "T"
        pcore.ONLY_FILE_IDS = None
        os.environ["GAOKZX_PACK_MODE"] = "batch"
        ppack.step_pack(batch_id="Bx")
        check("mode=batch -> 走 legacy", calls == {"legacy": 1, "topic": 0}, str(calls))
        calls.update(legacy=0, topic=0)
        os.environ["GAOKZX_PACK_MODE"] = "topic"
        ppack.step_pack(batch_id="Bx")
        check("mode=topic 且非单文件 -> 走 topic", calls == {"legacy": 0, "topic": 1}, str(calls))
        calls.update(legacy=0, topic=0)
        pcore.ONLY_FILE_IDS = {"F0001"}          # 单文件调度
        ppack.step_pack(batch_id="Bx")
        check("单文件调度(ONLY_FILE_IDS) 恒走 legacy（不受开关影响）", calls == {"legacy": 1, "topic": 0}, str(calls))
    finally:
        ppack.bc, pcore.ONLY_FILE_IDS = saved_bc, saved_only
        ppack.step_pack_legacy, ppack.step_pack_topic = saved_fn
        if saved_mode is None:
            os.environ.pop("GAOKZX_PACK_MODE", None)
        else:
            os.environ["GAOKZX_PACK_MODE"] = saved_mode


def main():
    test_pack_mode()
    test_step_pack_topic()
    test_batch_zips_fallback()
    test_dispatcher_routing()
    ok = all(_RESULTS)
    print(f"\n结果：{sum(_RESULTS)}/{len(_RESULTS)} 通过  =>", "ALL PASS" if ok else "HAS FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
