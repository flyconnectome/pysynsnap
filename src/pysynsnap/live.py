"""Rows at times after the newest snapshot (see docs/spec.md).

Stateless: every call starts from a published snapshot and applies the edits
since. The aedes R client adds a per-session cache and a cheaper path for
small queries; both are optimisations of the same steps.
"""

from __future__ import annotations

import uuid

import duckdb
import numpy as np
import pandas as pd

from .cave import delta_roots, get_roots
from .snapshot import ROW_COLS, Snapshot, roots_where
from .times import now, parse_time


def _tmp(prefix: str) -> str:
    return f"synsnap_{prefix}_{uuid.uuid4().hex[:12]}"


def rows_at(snap: Snapshot, client, timestamp="now", pre=None, post=None,
            details: bool = False, overlap: float = 600,
            tag: str | None = None) -> duckdb.DuckDBPyRelation:
    """Rows (``id, pre_root, post_root``) at any time at or after a snapshot.

    Parameters
    ----------
    snap : Snapshot
    client : CAVEclient or its chunkedgraph
    timestamp : time or "now"
    pre, post : optional root ids to restrict to. The roots must be valid at
        ``timestamp``.
    details : join the static columns (supervoxels, positions, size)
    overlap : seconds of edits before the snapshot to re-read, for edits
        that only became visible after it was made
    tag : snapshot to start from. Defaults to the newest published (or full)
        snapshot at or before ``timestamp``.
    """
    T = now() if timestamp == "now" else parse_time(timestamp)
    tag = tag or snap.at(T, locals=False)
    if tag is None:
        raise LookupError(f"No snapshot at or before {T}")
    t0 = snap.meta(tag)["timestamp"]
    if T < t0 - pd.Timedelta(seconds=1):
        raise ValueError(f"{T} is before snapshot {tag!r}")
    side, where, changed = roots_where(pre, post)
    con = snap.con

    if T <= t0:
        sql = snap.rows_sql(tag, side=side, where=where, changed=changed)
    else:
        old, _ = delta_roots(client, t0 - pd.Timedelta(seconds=overlap), T)
        sql = _overlay(snap, tag, old, T, client, side, where, changed)
    if details:
        sql = f"SELECT * FROM ({sql}) JOIN {snap.static_sql()} USING (id)"
    return con.sql(sql)


def _overlay(snap, tag, old, T, client, side, where, changed) -> str:
    """SQL for the rows of ``tag`` with every side on an expired root looked
    up again at T. Leaves temp tables on the connection."""
    con = snap.con
    if not len(old):
        return snap.rows_sql(tag, side=side, where=where, changed=changed)
    exp = _tmp("expired")
    con.register(exp, pd.DataFrame({"root": np.asarray(old, np.int64)}))
    inexp = lambda s: f"{s}_root IN (SELECT root FROM {exp})"
    chg = _tmp("changed")
    # each side read from the copy sorted by it, so the filter prunes
    con.execute(f"""CREATE TEMP TABLE {chg} AS
      SELECT c.id, c.pre_root, c.post_root, s.pre_sv, s.post_sv,
        {inexp('c.pre')} AS pre_stale, {inexp('c.post')} AS post_stale
      FROM (SELECT DISTINCT * FROM (
        {snap.rows_sql(tag, side='pre', where=inexp('pre'))} UNION ALL
        {snap.rows_sql(tag, side='post', where=inexp('post'))})) c
      JOIN {snap.static_sql()} s USING (id)""")
    con.unregister(exp)
    sv = con.sql(f"""SELECT pre_sv AS sv FROM {chg} WHERE pre_stale
      UNION SELECT post_sv FROM {chg} WHERE post_stale ORDER BY sv""").df().sv
    nr = _tmp("roots")
    con.register(nr, pd.DataFrame({"sv": sv.to_numpy(np.int64),
                                   "root": get_roots(client, sv, T)}))
    upd = _tmp("updated")
    con.execute(f"""CREATE TEMP TABLE {upd} AS SELECT c.id,
        CASE WHEN c.pre_stale THEN p.root ELSE c.pre_root END AS pre_root,
        CASE WHEN c.post_stale THEN q.root ELSE c.post_root END AS post_root
      FROM {chg} c LEFT JOIN {nr} p ON c.pre_sv = p.sv
      LEFT JOIN {nr} q ON c.post_sv = q.sv""")
    con.unregister(nr)
    w = "" if where is None else f" WHERE {where}"
    return f"""SELECT {ROW_COLS} FROM ({snap.rows_sql(tag, side=side, where=where,
                                                  changed=changed)})
        WHERE id NOT IN (SELECT id FROM {chg})
      UNION ALL SELECT {ROW_COLS} FROM {upd}{w}"""
