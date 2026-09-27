#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
断点续跑游标越界/指纹变化的单元测试（纯函数级，无网络无 IO）。

覆盖 `gaokzx_download._apply_offset_reset` 与 `_targets_fingerprint` 两个纯函数，
分别对应用户验收的三条：
  1) offset > total_targets -> 自动重置 offset=0，reason=offset_out_of_range。
  2) offset == total_targets -> 不重置（视为自然到末尾，交由上层报 rc=0/退出码 0）。
  3) 目标 URL 集合增/删 -> fp 不一致 -> 重置并 reason=fingerprint_changed。

运行方式（任选其一）：
  python test_offset_reset.py            # 直接跑，退出码 0/非 0
  pytest test_offset_reset.py -v         # 若装了 pytest 亦可（无第三方依赖要求）
"""
import sys
import os

# 允许作为脚本直接运行（无需安装为包）
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from gaokzx_download import _apply_offset_reset, _targets_fingerprint  # noqa: E402


# ---------- _apply_offset_reset：三种边界 ----------

def test_offset_out_of_range_resets_to_zero():
    """场景：历史 offset=168 但目标集合重新解析后 total=165（专题页少了几篇）→ 自动重置为 0。"""
    new_off, reason, off_before = _apply_offset_reset(
        offset=168, total_targets=165, expected_fp=None, actual_fp=None)
    assert reason == "offset_out_of_range", f"expected offset_out_of_range, got {reason!r}"
    assert new_off == 0, f"expected 0, got {new_off}"
    assert off_before == 168, f"expected 168, got {off_before}"


def test_offset_equal_total_not_reset():
    """场景：offset=165 == total=165 → 视为"自然到末尾"，**不**重置（由上层按正常结束处理，rc 归 0）。
    这条防的是"每轮跑完最后一批又被强行拉到 offset=0 从头再跑"的错误行为。"""
    new_off, reason, off_before = _apply_offset_reset(
        offset=165, total_targets=165, expected_fp=None, actual_fp=None)
    assert reason == "", f"expected no reset, got reason={reason!r}"
    assert new_off == 165, f"expected offset unchanged 165, got {new_off}"
    assert off_before == 165


def test_offset_zero_never_resets_even_if_fp_changed():
    """场景：offset=0 时无论 fp 是否变化都不动（本来就在起点，重置等价）。"""
    new_off, reason, off_before = _apply_offset_reset(
        offset=0, total_targets=165, expected_fp="deadbeefdeadbeef", actual_fp="ffffffffffffffff")
    assert reason == ""
    assert new_off == 0
    assert off_before == 0


def test_fingerprint_change_resets_to_zero():
    """场景：目标集合内容变化（新增/删除/换 URL）→ 期望 fp 与实际 fp 不一致 → 重置为 0 并 reason=fingerprint_changed。
    即使 offset <= total 也应触发（专题页替换了一批新资源，旧游标已失去意义）。"""
    new_off, reason, off_before = _apply_offset_reset(
        offset=50, total_targets=165,
        expected_fp="abc123abc123abc1", actual_fp="xyz789xyz789xyz7")
    assert reason == "fingerprint_changed", f"got {reason!r}"
    assert new_off == 0
    assert off_before == 50


def test_fingerprint_match_offset_within_range_no_reset():
    """场景：fp 相同且 offset 在合理范围 → 保持原样。"""
    new_off, reason, _ = _apply_offset_reset(
        offset=24, total_targets=165,
        expected_fp="same_fp", actual_fp="same_fp")
    assert reason == ""
    assert new_off == 24


def test_fingerprint_change_has_priority_over_out_of_range():
    """fp 变化优先于 offset 越界（专题结构变，越界只是表象）。"""
    new_off, reason, _ = _apply_offset_reset(
        offset=168, total_targets=165,
        expected_fp="old_fp", actual_fp="new_fp")
    assert reason == "fingerprint_changed"
    assert new_off == 0


def test_no_targets_zero_offset_conservative_no_reset():
    """total=0 且 offset=0 时保守不重置（URL 真无目标，属 no_targets，非 offset_out_of_range）。"""
    new_off, reason, _ = _apply_offset_reset(
        offset=0, total_targets=0, expected_fp=None, actual_fp=None)
    assert reason == ""
    assert new_off == 0


def test_missing_expected_fp_skips_fp_check():
    """首次运行游标无 fp 记录（expected_fp 为 None/空）→ 不做 fp 比对，仅走 offset>total 分支。"""
    new_off, reason, _ = _apply_offset_reset(
        offset=50, total_targets=100, expected_fp=None, actual_fp="whatever")
    assert reason == ""
    assert new_off == 50


# ---------- _targets_fingerprint：稳定性 & 集合变化检测 ----------

def _mk(urls):
    return [{"url": u} for u in urls]


def test_fingerprint_stable_under_order_and_dupes():
    """目标 URL 顺序/重复不影响 fp（sorted + set 归一）。"""
    fp1 = _targets_fingerprint(_mk(["a/1", "a/2", "a/3"]))
    fp2 = _targets_fingerprint(_mk(["a/3", "a/1", "a/2"]))
    fp3 = _targets_fingerprint(_mk(["a/1", "a/1", "a/2", "a/3", "a/2"]))
    assert fp1 == fp2 == fp3
    assert len(fp1) == 16   # sha1[:16]


def test_fingerprint_changes_when_one_url_added():
    """增删一个目标 -> fp 必然变化（对应用户场景 4）。"""
    before = _targets_fingerprint(_mk(["a/1", "a/2", "a/3"]))
    after_add = _targets_fingerprint(_mk(["a/1", "a/2", "a/3", "a/4"]))
    after_del = _targets_fingerprint(_mk(["a/1", "a/2"]))
    assert before != after_add
    assert before != after_del


def test_fingerprint_empty_for_no_targets():
    """无目标 -> fp 为空串（触发时不做变化判定，走 total=0 分支）。"""
    assert _targets_fingerprint([]) == ""
    assert _targets_fingerprint(None) == ""


# ---------- 端到端场景（模拟用户报告的现象） ----------

def test_scenario_159032_offset_168_total_165():
    """用户报的现场：URL 159032 重新解析出 total=165，游标里 offset=168 -> 应重置并 reason=offset_out_of_range。"""
    # 无 fp（历史游标未记录 fp）→ 走 total 越界分支
    new_off, reason, off_before = _apply_offset_reset(168, 165, None, "abc123abc123abc1")
    assert reason == "offset_out_of_range"
    assert new_off == 0 and off_before == 168


def test_scenario_target_set_shrunk_triggers_fp_change():
    """同一场景，若历史游标已带 fp：专题从 168 篇缩到 165 篇，URL 集合变化 -> reason=fingerprint_changed。"""
    old_fp = _targets_fingerprint(_mk([f"p/{i}" for i in range(168)]))
    new_fp = _targets_fingerprint(_mk([f"p/{i}" for i in range(165)]))
    assert old_fp != new_fp
    new_off, reason, _ = _apply_offset_reset(168, 165, old_fp, new_fp)
    assert reason == "fingerprint_changed"
    assert new_off == 0


# ---------- 简易 runner（无 pytest 也能跑） ----------

def _run_all():
    tests = [(k, v) for k, v in sorted(globals().items())
             if k.startswith("test_") and callable(v)]
    passed = failed = 0
    for name, fn in tests:
        try:
            fn()
            passed += 1
            print(f"  PASS  {name}")
        except AssertionError as e:
            failed += 1
            print(f"  FAIL  {name}  -> {e}")
        except Exception as e:
            failed += 1
            print(f"  ERR   {name}  -> {type(e).__name__}: {e}")
    print(f"\n{passed} passed, {failed} failed (total {len(tests)})")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(_run_all())
