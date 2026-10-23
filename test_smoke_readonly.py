# -*- coding: utf-8 -*-
"""gaokzx 只读冒烟测试网（P0）—— pflow/batchcore/dashboard 拆分前的回归安全网。

铁律：
  * 绝不联网；
  * 绝不写生产库/生产文件 —— 进程启动即设 GAOKZX_TEST_ROOT 把 DB/_批次/logs 全重定向到临时目录；
  * 只跑只读路径（--help / check-env 语义 / 纯函数 / 隔离库 CRUD）。

覆盖：
  A. CLI --help：退出码 0 + 30 个关键参数注册在位（argparse 注册段零改动哨兵）
  B. 模块依赖链 import + pipeline re-export 符号完整性（拆分兼容性哨兵）
  C. pcore 纯函数：_sha256_file / clean_text / _norm_py / iter_pdfs+ONLY_SUBDIR /
     _scan+ONLY_FILE_IDS / _ask AUTO_MODE（跨模块可变全局哨兵）
  D. ppack._pack_mode：默认 topic / 环境变量覆盖
  E. prename.parse_pdf_name / build_norm_name：真实样本字段断言（探针固化值）
  F. batchcore 隔离库全流程：new_batch/register_files/db_step/is_step_ok/manifest
     roundtrip/set_current/scan_pdfs 正负例/db_pending_batches/batch_accept 三态
     （空返回/验收1条/幂等重跑）/db_batches_of_run/set_batch_status

运行：python test_smoke_readonly.py   全 PASS 退出码 0，否则 1。
"""
import contextlib
import os
import subprocess
import sys
import tempfile

# ---- 隔离必须在任何项目模块 import 之前（batchcore/pcore 模块加载即固化路径）----
_ISO = tempfile.mkdtemp(prefix="gaokzx_smoke_")
os.environ["GAOKZX_TEST_ROOT"] = _ISO
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import batchcore as bc
import pcore
import ppack
import prename

_GAOKZX = os.path.dirname(os.path.abspath(__file__))
_RESULTS = []


def check(name, cond, detail=""):
    _RESULTS.append(bool(cond))
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f"  — {detail}" if detail and not cond else ""))


# ---------------- A. CLI --help ----------------
def test_help():
    print("A. CLI --help")
    r = subprocess.run([sys.executable, os.path.join(_GAOKZX, "pipeline.py"), "--help"],
                       capture_output=True, text=True, encoding="utf-8", errors="replace")
    check("--help 退出码 0", r.returncode == 0, r.stderr[:200])
    must_have = ["--step", "--topic", "--url", "--limit", "--ocr-workers", "--wm-workers",
                 "--auto", "--start-stage", "--test-root", "--chunk", "--file-id", "--batch-id",
                 "--force", "--force-scope", "--only-subdir", "--dry-run", "--check",
                 "--ensure-dirs", "--before", "--roots", "--include-backup", "--paper-type",
                 "--year-type", "--skip-ads", "--pack-root", "--rename-apply", "--rename-ocr",
                 "--fix-conflicts-by-body", "--force-missing", "--force-full-run", "--move",
                 "--install", "--hardest", "--src", "--device", "--fp16", "--ocr-threads",
                 "--ocr-batch", "--ads-report", "--ads-out", "--rename-root", "--qc-root",
                 "--ocr-root", "--pack-out", "--name", "--workers", "--delay",
                 "--force-rerun-failed", "--ask-on-empty", "--ask-on-stage-failure",
                 "--ask-on-resume", "--include-db-stale", "--include-db-ok", "--apply"]
    missing = [m for m in must_have if m not in r.stdout]
    check(f"{len(must_have)} 个关键参数全注册", not missing, str(missing))
    check("--help 输出含 usage 段", "usage: pipeline.py" in r.stdout)


# ---------------- B. 模块依赖链 + 符号完整性 ----------------
def test_imports():
    print("B. 模块依赖链 + 符号完整性")
    import pipeline as P
    import pflow
    syms = [
        ("P.main", P.main), ("P.run_wizard", P.run_wizard),
        ("P.step_watermark", P.step_watermark), ("P.step_rename", P.step_rename),
        ("P.step_ocr", P.step_ocr), ("P.step_qc", P.step_qc),
        ("P.step_ads", P.step_ads), ("P.step_pack", P.step_pack),
        ("P.step_discover_download", P.step_discover_download),
        ("P.step_export_excel", P.step_export_excel), ("P.step_cleanup", P.step_cleanup),
        ("P.run_auto", P.run_auto), ("P.run_step_safe", P.run_step_safe),
        ("P.db", P.db), ("P.ensure_dirs", P.ensure_dirs),
        ("P.step_check_env", P.step_check_env),
    ]
    bad = [n for n, f in syms if not callable(f)]
    check(f"{len(syms)} 个 re-export 符号可调用", not bad, str(bad))
    check("wm_worker 兼容：P._wm_process_one 可达", callable(P._wm_process_one))
    check("pflow.run_wizard 与 P 同一对象", P.run_wizard is pflow.run_wizard)


# ---------------- C. pcore 纯函数 + 可变全局 ----------------
def test_pcore_utils():
    print("C. pcore 纯函数 + 可变全局")
    from pathlib import Path
    tmp = Path(tempfile.mkdtemp(prefix="pcore_smoke_"))
    f1 = tmp / "a.pdf"
    f1.write_bytes(b"%PDF-1.4 same")
    f2 = tmp / "b.pdf"
    f2.write_bytes(b"%PDF-1.4 same")
    f3 = tmp / "c.pdf"
    f3.write_bytes(b"%PDF-1.4 DIFF")
    h1, h2, h3 = pcore._sha256_file(str(f1)), pcore._sha256_file(str(f2)), pcore._sha256_file(str(f3))
    check("_sha256_file 同内容同哈希", h1 == h2 and h1 and len(h1) == 64)
    check("_sha256_file 不同内容不同哈希", h1 != h3)
    check("clean_text 去控制字符", pcore.clean_text("a\x00b", 10) == "ab")
    check("_norm_py 返回规范路径", isinstance(pcore._norm_py(sys.executable), str))
    # iter_pdfs + ONLY_SUBDIR
    (tmp / "sub").mkdir()
    (tmp / "sub" / "d.pdf").write_bytes(b"%PDF-")
    saved_sub = pcore.ONLY_SUBDIR
    try:
        pcore.ONLY_SUBDIR = "sub"
        got = list(pcore.iter_pdfs(tmp))
        check("iter_pdfs ONLY_SUBDIR 过滤", len(got) == 1 and got[0].endswith("d.pdf"), str(got))
        pcore.ONLY_SUBDIR = None
        got_all = list(pcore.iter_pdfs(tmp))
        check("iter_pdfs 无过滤返回全部", len(got_all) == 4, str(got_all))
    finally:
        pcore.ONLY_SUBDIR = saved_sub
    # _scan + ONLY_FILE_IDS（看板单文件调度哨兵）
    class FakeBC:
        STEP_DOWNLOAD = 1
        def scan_pdfs(self, stage, bid):
            return [("F0001", str(f1)), ("F0002", str(f3))]
        def is_test_path(self, p):
            return False
    saved_bc, saved_fids = pcore.bc, pcore.ONLY_FILE_IDS
    try:
        pcore.bc = FakeBC()
        pcore.ONLY_FILE_IDS = ["f0001"]        # 小写测大小写归一
        r1 = pcore._scan(1, "BX")
        check("_scan ONLY_FILE_IDS 过滤", len(r1) == 1 and r1[0][0] == "F0001", str(r1))
        pcore.ONLY_FILE_IDS = None
        r2 = pcore._scan(1, "BX")
        check("_scan 无过滤返回全批", len(r2) == 2, str(r2))
    finally:
        pcore.bc, pcore.ONLY_FILE_IDS = saved_bc, saved_fids
    # _ask AUTO_MODE
    saved_am = pcore.AUTO_MODE
    try:
        pcore.AUTO_MODE = True
        check("_ask AUTO_MODE 返回默认", pcore._ask("测试?", "n") == "n")
    finally:
        pcore.AUTO_MODE = saved_am


# ---------------- D. _pack_mode ----------------
def test_pack_mode():
    print("D. _pack_mode")
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


# ---------------- E. parse_pdf_name ----------------
def test_parse_name():
    print("E. parse_pdf_name / build_norm_name")
    cases = [
        ("2025北京一六一中高二开学考英语有答案",
         {"year": "2025", "grade": "高二", "school": "一六一中", "exam": "开学考",
          "subject": "英语", "answer": "有答案", "region": ""}),
        ("2026延庆区延庆一中高二开学考生物",
         {"year": "2026", "region": "延庆区", "school": "延庆一中", "grade": "高二",
          "exam": "开学考", "subject": "生物", "answer": ""}),
        ("2026北京交大附中高二开学考数学有答案",
         {"year": "2026", "school": "交大附中", "subject": "数学", "answer": "有答案"}),
        ("2025北京师大二附中高二下学期期中化学无答案",
         {"year": "2025", "school": "师大二附中", "exam": "期中", "subject": "化学",
          "answer": "无答案"}),
    ]
    for text, want in cases:
        r = prename.parse_pdf_name(text, "")
        bad = {k: (r.get(k), v) for k, v in want.items() if r.get(k) != v}
        check(f"parse({text[:18]}…)", not bad, str(bad))
    r = prename.parse_pdf_name("2025北京一六一中高二开学考英语有答案", "")
    nn = prename.build_norm_name(r, "fallback")
    check("build_norm_name 拼回规范名", nn == "2025北京一六一中高二开学考英语有答案", nn)


# ---------------- F. batchcore 隔离库全流程 ----------------
def test_batchcore_isolated():
    print("F. batchcore 隔离库全流程（GAOKZX_TEST_ROOT 隔离）")
    from pathlib import Path
    check("BATCH_DIR 已重定向到隔离目录", str(bc.BATCH_DIR).startswith(_ISO), str(bc.BATCH_DIR))
    check("DEFAULT_DB 已重定向到隔离目录", str(bc.DEFAULT_DB).startswith(_ISO), str(bc.DEFAULT_DB))

    stage_dir = bc.STAGE_ALIAS["pending"]
    stage_dir.mkdir(parents=True, exist_ok=True)
    pdf = stage_dir / "x.pdf"
    pdf.write_bytes(b"%PDF-1.4 fake")

    mf = bc.new_batch(url="http://smoke/1", name="冒烟", run_id="RUN_SMOKE")
    check("new_batch 返回 manifest", bool(mf and mf.get("batch_id")), str(mf)[:80])
    bid = mf["batch_id"]

    files = bc.register_files(bid, [{"path": str(pdf), "name": "x.pdf"}])
    check("register_files 分配 file_id", len(files) == 1 and files[0]["file_id"] == "F0001",
          str(files))
    fid = files[0]["file_id"]

    bc.db_step(bid, fid, 1, "ok", src=str(pdf))
    check("db_step + is_step_ok", bc.is_step_ok(bid, fid, 1) is True)
    check("未写步骤 is_step_ok=False", bc.is_step_ok(bid, fid, 2) is False)

    bc.save_manifest({"batch_id": bid, "files": [{"file_id": fid, "path": str(pdf)}]})
    mf2 = bc.load_manifest(bid)
    check("manifest roundtrip", mf2 and mf2["files"][0]["file_id"] == fid)

    bc.set_current(bid)
    check("set_current + current_batch_id", bc.current_batch_id() == bid)

    pairs = bc.scan_pdfs("pending", bid)
    check("scan_pdfs 命中（stage 前缀匹配）", len(pairs) == 1 and pairs[0][0] == fid, str(pairs))
    pairs_fid = bc.scan_pdfs("pending", bid, file_ids={fid})
    check("scan_pdfs file_ids 收窄", len(pairs_fid) == 1)
    pairs_none = bc.scan_pdfs("cleaned", bid)
    check("scan_pdfs 串 stage 目录不命中（负例）", pairs_none == [], str(pairs_none))

    pb = bc.db_pending_batches()
    check("db_pending_batches 空列表", pb == [], str(pb))

    r0 = bc.batch_accept(batch_ids=bid)
    check("batch_accept 无 pending 幂等空返回",
          r0 == {"ok": True, "accepted": 0, "skipped": 0, "errors": []}, str(r0))
    bc.db_step(bid, fid, 8, "pending_accept", src=str(pdf), dst=str(pdf.with_suffix(".zip")))
    r1 = bc.batch_accept(batch_ids=bid)
    check("batch_accept 验收 1 条", r1["ok"] and r1["accepted"] == 1, str(r1))
    r2 = bc.batch_accept(batch_ids=bid)
    check("batch_accept 重跑幂等（accepted=0 skipped=1）",
          r2["accepted"] == 0 and r2["skipped"] == 1, str(r2))
    check("验收后 db_pending_batches 清空", bc.db_pending_batches() == [])

    check("db_batches_of_run", bc.db_batches_of_run("RUN_SMOKE") == [bid])
    check("set_batch_status", bc.set_batch_status(bid, "accepted", note="smoke") is True)
    check("db_step_counts 可查", isinstance(bc.db_step_counts(bid), (list, dict)))


def main():
    test_help()
    test_imports()
    test_pcore_utils()
    test_pack_mode()
    test_parse_name()
    test_batchcore_isolated()
    ok = all(_RESULTS)
    print(f"\n结果：{sum(_RESULTS)}/{len(_RESULTS)} 通过  =>", "ALL PASS" if ok else "HAS FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
