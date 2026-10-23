#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""gaokzx 批次核心 · 批量验收（bcaccept）—— 2026-10-22 自 batchcore.py 拆出。

职责：待验收批次列举（db_pending_batches）+ 批量状态验收（batch_accept——
pending_accept -> accepted 的唯一合法迁移；BEGIN IMMEDIATE 事务、幂等重跑、
批次级状态推进、manifest 同步）。
依赖：batchcore 的 _lock/_conn/_dbg/_now_iso/ST_ACCEPTED/load_manifest/save_manifest/
db_batches_of_run，均在函数内 import batchcore as bc 取得——batchcore 末尾
re-export 本模块，模块级 import 会成环，切勿改回模块级。
被 batchcore re-export 后经 `import batchcore as bc` 使用；pflow/dashboard 亦经 bc 使用。
"""
import sqlite3


# ---------------------------------------------------------------------------
# 批量验收（pending_accept -> accepted）：事务化、幂等、统一入口
# ---------------------------------------------------------------------------
_ACCEPT_CHUNK = 400          # 分块 IN / executemany 单批量，防 SQLite 变量过多


def _chunks(seq, n):
    seq = list(seq)
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def db_pending_batches():
    """列出仍有「待验收」文件的批次 -> [{batch_id, run_id, pending, zip}]（按 batch_id 排序）。
    zip 取本批 file_steps step=8 pending_accept 行的 dst（打包时登记的 zip 路径，代表样例）。失败静默返回 []。"""
    import batchcore as bc      # 函数内 import：batchcore 末尾 re-export 本模块，模块级会成环
    try:
        with bc._lock:
            con = bc._conn()
            try:
                rows = con.execute(
                    "SELECT fs.batch_id, COUNT(*), "
                    "(SELECT b.run_id FROM batches b WHERE b.batch_id=fs.batch_id), "
                    "MAX(fs.dst) FROM file_steps fs "
                    "WHERE fs.step=8 AND fs.status='pending_accept' "
                    "GROUP BY fs.batch_id ORDER BY fs.batch_id").fetchall()
                return [{"batch_id": r[0], "pending": int(r[1] or 0),
                         "run_id": r[2] or "", "zip": r[3] or ""} for r in rows]
            finally:
                con.close()
    except Exception as e:
        bc._dbg("db_pending_batches", e)
        return []


def batch_accept(batch_ids=None, file_ids=None, run_id=None, operator="manual", pairs=None):
    """批量验收：把 file_steps step=8 的 pending_accept 迁移为 accepted（唯一允许的迁移）。

    圈定范围（多者可组合取并集，均为空则不动任何行、直接返回）：
      batch_ids —— 批次号列表/单个；run_id —— 该运行下全部批次（经 batches 反查 batch_id）；
      file_ids  —— 具体 file_id 列表/单个（注意：file_id 仅批内唯一，跨批会命中同名，仅在确定无歧义时用）；
      pairs     —— 精确 (batch_id, file_id) 对列表 [[b,f],...] 或 [{batch_id,file_id},...]（看板多选专用，最安全）。
    严格只碰 step=8 且 status='pending_accept' 的行；fail/error/rejected/已 accepted/其它 step 一律不动。
    事务：BEGIN IMMEDIATE；幂等（重复调用第二次 accepted=0）：已 accepted 行不进入候选、不被重写
      （skipped 仅计数）；accepted_at 用 COALESCE 保留首次验收时间不刷新；batches 级推进条件同理重复无害；
    分块 executemany 防变量过多。
    同步：写 accepted_at/accepted_by；某批范围内已无 pending_accept 时把 batches.status 推进为 accepted 并记批次级 accepted_at/by。
    返回 {"ok": bool, "accepted": n, "skipped": m, "errors": [...]}（skipped=范围内 step=8 但非 pending_accept 的存量行，未受影响）。
    说明：这是“状态验收”（DB 位）；把文件物理归档移入「文库资源_completed」仍由 pipeline.step_accept 逐文件执行。"""
    import batchcore as bc      # 函数内 import：batchcore 末尾 re-export 本模块，模块级会成环
    res = {"ok": True, "accepted": 0, "skipped": 0, "errors": []}
    # ---- 归一化入参 ----
    def _as_list(x):
        if x is None:
            return []
        if isinstance(x, (list, tuple, set)):
            return [str(i).strip() for i in x if str(i).strip()]
        s = str(x).strip()
        return [s] if s else []
    bs = _as_list(batch_ids)
    fs = [f.upper() for f in _as_list(file_ids)]
    prs = []
    for it in (pairs or []):
        if isinstance(it, dict):
            b, f = str(it.get("batch_id") or "").strip(), str(it.get("file_id") or "").strip().upper()
        else:
            try:
                b, f = str(it[0]).strip(), str(it[1]).strip().upper()
            except (IndexError, TypeError):
                continue
        if b and f and (b, f) not in prs:
            prs.append((b, f))
    rid = (str(run_id).strip() if run_id else "")
    if rid:
        for b in bc.db_batches_of_run(rid):
            if b and b not in bs:
                bs.append(b)
    if not bs and not fs and not prs:
        # 未指定任何范围（或 run_id 下无批次）：不动任何行，正常空返回（幂等，绝不报错、绝不闪退）。
        # 满足 [1] 一键全自动收尾对空/无待验收批次的安全调用：ok=True, accepted=0, skipped=0, errors=[]。
        return res
    by = str(operator or "manual").strip()[:32] or "manual"
    try:
        with bc._lock:
            con = bc._conn()
            try:
                con.isolation_level = None      # 手动事务：交给下面的 BEGIN IMMEDIATE/COMMIT，禁模块隐式开事务
                con.execute("BEGIN IMMEDIATE")
                # 1) 命中范围内的 step=8 候选行（pending_accept 才改）
                cand = []            # (batch_id, file_id)
                scope_batches = set()
                for grp in _chunks(bs, _ACCEPT_CHUNK):
                    ph = ",".join("?" * len(grp))
                    for b, f in con.execute(
                            "SELECT batch_id,file_id FROM file_steps "
                            "WHERE step=8 AND status='pending_accept' "
                            f"AND batch_id IN ({ph})", grp).fetchall():
                        if (b, f) not in cand:
                            cand.append((b, f))
                        scope_batches.add(b)
                if fs:
                    for grp in _chunks(fs, _ACCEPT_CHUNK):
                        ph = ",".join("?" * len(grp))
                        for b, f in con.execute(
                                "SELECT batch_id,file_id FROM file_steps "
                                "WHERE step=8 AND status='pending_accept' "
                                f"AND file_id IN ({ph})", grp).fetchall():
                            if (b, f) not in cand:
                                cand.append((b, f))
                            scope_batches.add(b)
                if prs:
                    for grp in _chunks(prs, _ACCEPT_CHUNK):
                        conds, vals = [], []
                        for (b, f) in grp:
                            conds.append("(batch_id=? AND file_id=?)"); vals += [b, f]
                        for b, f in con.execute(
                                "SELECT batch_id,file_id FROM file_steps "
                                "WHERE step=8 AND status='pending_accept' AND ("
                                + " OR ".join(conds) + ")", vals).fetchall():
                            if (b, f) not in cand:
                                cand.append((b, f))
                            scope_batches.add(b)
                # 2) skipped：范围内 step=8 但当前非 pending_accept 的存量行（仅统计，不动）
                for grp in _chunks(bs, _ACCEPT_CHUNK):
                    ph = ",".join("?" * len(grp))
                    for (n,) in con.execute(
                            "SELECT COUNT(*) FROM file_steps "
                            "WHERE step=8 AND status<>'pending_accept' "
                            f"AND batch_id IN ({ph})", grp).fetchall():
                        res["skipped"] += int(n or 0)
                if prs:
                    for grp in _chunks(prs, _ACCEPT_CHUNK):
                        conds, vals = [], []
                        for (b, f) in grp:
                            conds.append("(batch_id=? AND file_id=?)"); vals += [b, f]
                        for (n,) in con.execute(
                                "SELECT COUNT(*) FROM file_steps "
                                "WHERE step=8 AND status<>'pending_accept' AND ("
                                + " OR ".join(conds) + ")", vals).fetchall():
                            res["skipped"] += int(n or 0)
                # 3) 逐块按主键 UPSERT 状态（幂等：WHERE 再校验 pending_accept）
                now = bc._now_iso()
                for grp in _chunks(cand, _ACCEPT_CHUNK):
                    cur = con.executemany(
                        "UPDATE file_steps SET status=?, accepted_at=COALESCE(accepted_at, ?), "
                        "accepted_by=?, updated_at=? "
                        "WHERE batch_id=? AND file_id=? AND step=8 AND status='pending_accept'",
                        [(bc.ST_ACCEPTED, now, by, now, b, f) for (b, f) in grp])
                    # executemany 的 rowcount 为本批累计影响行数（SELECT changes() 只反映最后一条，会少算）
                    res["accepted"] += int(cur.rowcount or 0)
                # 4) 批次级推进：范围内 pending_accept 清空、且无 rejected/exec_fail、确有 accepted
                #    才把 batches.status 推进为 accepted（仍有被驳回/执行失败文件的批次不整体判定为已验收）
                for b in sorted(scope_batches):
                    row = con.execute(
                        "SELECT SUM(status='pending_accept'), SUM(status='rejected'), "
                        "SUM(status='exec_fail'), SUM(status='accepted') "
                        "FROM file_steps WHERE batch_id=? AND step=8", (b,)).fetchone()
                    pend, rej, exf, acc = (int(x or 0) for x in (row or (0, 0, 0, 0)))
                    if pend == 0 and rej == 0 and exf == 0 and acc > 0:
                        try:
                            con.execute(
                                "UPDATE batches SET status=?, accepted_at=COALESCE(accepted_at,?), "
                                "accepted_by=?, updated_at=? WHERE batch_id=?",
                                ("accepted", now, by, now, b))
                        except sqlite3.Error:
                            pass
                con.commit()
            except Exception as e:
                try:
                    con.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                res["ok"] = False
                res["accepted"] = 0
                res["errors"].append(f"{type(e).__name__}: {e}")
            finally:
                con.close()
        # 5) manifest 同步：验收成功的批次，若已无 pending_accept，把 manifest.status 也落 accepted
        if res["ok"] and res["accepted"]:
            for b in sorted(scope_batches):
                try:
                    mf = bc.load_manifest(b)
                    if mf and str(mf.get("status") or "") != "accepted":
                        mf["status"] = "accepted"
                        bc.save_manifest(mf)
                except Exception as e:
                    bc._dbg("batch_accept.manifest", e)
        return res
    except Exception as e:
        res["ok"] = False
        res["errors"].append(f"{type(e).__name__}: {e}")
        return res
