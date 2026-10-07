"""Sketch: snapshot logs as Delta Lake tables (see docs/proposal.md).

A store at ``uri`` has two Delta tables:

``<uri>/bases``
    full snapshots: ``version``, ``t``, ``id``, ``pre_root``, ``post_root``,
    partitioned by ``version`` and sorted by ``pre_root`` within it.
``<uri>/log``
    one append per update run: ``id``, ``t``, ``pre_root``, ``post_root``,
    ``old_pre``, ``old_post``, ``day``; partitioned by ``day``. Only a side
    that changed is filled in. A run with no changes still commits (with no
    rows), so its time is recorded.

Each commit carries ``synsnap.*`` metadata: the run time, and for version runs
and bases, the version. The state at time T is the newest base at or before
T, updated by the log rows with base time < t <= T, folded as in
docs/spec.md. It is exact at the times in :func:`checkpoints`; other times
need the chunkedgraph (see live.py).

Nothing here depends on the materialization engine: an update run only needs
to hand over the rows it changed and its time.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import pyarrow as pa
from deltalake import CommitProperties, DeltaTable, write_deltalake

from .times import format_time, parse_time

LOG_SCHEMA = pa.schema([
    ("id", pa.int64()), ("t", pa.timestamp("us", tz="UTC")),
    ("pre_root", pa.int64()), ("post_root", pa.int64()),
    ("old_pre", pa.int64()), ("old_post", pa.int64()),
    ("day", pa.date32()),
])
BASE_SCHEMA = pa.schema([
    ("version", pa.int64()), ("t", pa.timestamp("us", tz="UTC")),
    ("id", pa.int64()), ("pre_root", pa.int64()), ("post_root", pa.int64()),
])

FOLD = """arg_max(pre_root, t) FILTER (WHERE pre_root IS NOT NULL) AS pre_root,
    arg_max(post_root, t) FILTER (WHERE post_root IS NOT NULL) AS post_root"""


def _meta(**kw) -> CommitProperties:
    return CommitProperties(custom_metadata={
        f"synsnap.{k}": str(v) for k, v in kw.items() if v is not None})


def _ts_sql(t) -> str:
    return f"TIMESTAMPTZ '{format_time(t).removesuffix(' UTC')}+00'"


def _table(uri, name, storage_options):
    return DeltaTable(f"{uri}/{name}", storage_options=storage_options)


def log_rows(before: pd.DataFrame, after: pd.DataFrame, t) -> pa.Table:
    """Log rows for one run from the roots of the rows it looked up.

    ``before`` and ``after`` have ``id``, ``pre_root``, ``post_root`` for the
    same synapses before and after the run. Sides that did not change are
    NULL; rows with no change are dropped.
    """
    t = parse_time(t)
    d = before.merge(after, on="id", suffixes=("_old", ""), validate="1:1")
    pre = d.pre_root != d.pre_root_old
    post = d.post_root != d.post_root_old
    keep = pre | post
    d, pre, post = d[keep], pre[keep], post[keep]
    n = len(d)
    col = lambda x, m: pa.array(x.where(m), pa.int64(), from_pandas=True)
    return pa.table({
        "id": pa.array(d.id, pa.int64()),
        "t": pa.array([t] * n, pa.timestamp("us", tz="UTC")),
        "pre_root": col(d.pre_root, pre),
        "post_root": col(d.post_root, post),
        "old_pre": col(d.pre_root_old, pre),
        "old_post": col(d.post_root_old, post),
        "day": pa.array([t.date()] * n, pa.date32()),
    }, schema=LOG_SCHEMA).sort_by("id")


def append_run(uri: str, rows: pa.Table, t, version: int | None = None,
               storage_options: dict | None = None) -> None:
    """Commit one update run's log rows (possibly none) at time ``t``.

    Exactly one commit per run, made after all its lookups are done.
    """
    write_deltalake(f"{uri}/log", rows.cast(LOG_SCHEMA), mode="append",
                    partition_by=["day"], storage_options=storage_options,
                    commit_properties=_meta(t=format_time(t), kind="run",
                                            version=version))


def write_base(uri: str, rows: pd.DataFrame | pa.Table, t, version: int,
               storage_options: dict | None = None) -> None:
    """Add a full snapshot (``id``, ``pre_root``, ``post_root``) at a version."""
    con = duckdb.connect()
    con.register("rows", rows)
    tbl = con.sql(f"""SELECT {int(version)}::BIGINT AS version,
        {_ts_sql(t)} AS t, id::BIGINT AS id,
        pre_root::BIGINT AS pre_root, post_root::BIGINT AS post_root
      FROM rows ORDER BY pre_root, id""").arrow()
    if isinstance(tbl, pa.RecordBatchReader):
        tbl = tbl.read_all()
    write_deltalake(f"{uri}/bases", tbl.cast(BASE_SCHEMA), mode="append",
                    partition_by=["version"], storage_options=storage_options,
                    commit_properties=_meta(t=format_time(t), kind="base",
                                            version=version))


def checkpoints(uri: str, storage_options: dict | None = None) -> pd.DataFrame:
    """Times the store answers exactly (``t``, ``kind``, ``version``), oldest
    first: bases, and runs not folded away by :func:`collapse_day`."""
    rows = []
    for name in ("bases", "log"):
        for h in _table(uri, name, storage_options).history():
            if "synsnap.kind" in h:
                rows.append({"t": h.get("synsnap.t"), "kind": h["synsnap.kind"],
                             "version": h.get("synsnap.version"),
                             "collapsed": h.get("synsnap.collapsed")})
    df = pd.DataFrame(rows, columns=["t", "kind", "version", "collapsed"])
    df["t"] = [parse_time(t) if t else pd.NaT for t in df.t]
    df["version"] = pd.to_numeric(df.version).astype("Int64")
    # a collapse keeps only the day's last run with changes, plus runs after it
    for day, t in zip(df.collapsed, df.t):
        if isinstance(day, str):
            gone = (df.kind == "run") & (df.t.dt.date == pd.Timestamp(day).date())
            if pd.notna(t):
                gone &= df.t < t
            df = df[~gone]
    df = df[df.kind != "collapse"]
    return (df.drop(columns="collapsed").sort_values("t")
            .reset_index(drop=True))


def state_sql(con: duckdb.DuckDBPyConnection, uri: str, T,
              storage_options: dict | None = None) -> str:
    """SQL giving ``id, pre_root, post_root`` at time ``T``. Registers the
    tables on ``con`` as ``synsnap_bases`` and ``synsnap_log``."""
    con.register("synsnap_bases",
                 _table(uri, "bases", storage_options).to_pyarrow_dataset())
    con.register("synsnap_log",
                 _table(uri, "log", storage_options).to_pyarrow_dataset())
    b = con.sql(f"""SELECT version, t FROM synsnap_bases
        WHERE t <= {_ts_sql(T)} GROUP BY ALL ORDER BY t DESC LIMIT 1""").fetchone()
    if b is None:
        raise LookupError(f"No base at or before {parse_time(T)}")
    version, tb = b
    return f"""SELECT b.id,
        coalesce(f.pre_root, b.pre_root) AS pre_root,
        coalesce(f.post_root, b.post_root) AS post_root
      FROM (SELECT id, pre_root, post_root FROM synsnap_bases
            WHERE version = {int(version)}) b
      LEFT JOIN (SELECT id, {FOLD} FROM synsnap_log
        WHERE t > {_ts_sql(tb)} AND t <= {_ts_sql(T)}
        GROUP BY id) f USING (id)"""


def compact(uri: str, storage_options: dict | None = None) -> None:
    """Bin-pack small log files. Keeps every row, so every run stays exact.

    Do not vacuum with zero retention: readers and the next run may still be
    using the old files.
    """
    _table(uri, "log", storage_options).optimize.compact()


def collapse_day(uri: str, day, storage_options: dict | None = None) -> None:
    """Rewrite one finished day of the log as one row per synapse, at the
    day's last change. Earlier runs that day are then no longer exact."""
    day = pd.Timestamp(day).date()
    dt = _table(uri, "log", storage_options)
    con = duckdb.connect()
    con.register("log", dt.to_pyarrow_dataset())
    t_last = con.sql(f"SELECT max(t) FROM log WHERE day = DATE '{day}'"
                     ).fetchone()[0]
    if t_last is None:
        return
    # a side that changed and changed back is no change
    tbl = con.sql(f"""WITH f AS (SELECT id, {FOLD},
        arg_min(old_pre, t) FILTER (WHERE old_pre IS NOT NULL) AS old_pre,
        arg_min(old_post, t) FILTER (WHERE old_post IS NOT NULL) AS old_post
        FROM log WHERE day = DATE '{day}' GROUP BY id),
      g AS (SELECT *, pre_root IS DISTINCT FROM old_pre AS dpre,
          post_root IS DISTINCT FROM old_post AS dpost FROM f)
      SELECT id, {_ts_sql(t_last)} AS t,
        CASE WHEN dpre THEN pre_root END AS pre_root,
        CASE WHEN dpost THEN post_root END AS post_root,
        CASE WHEN dpre THEN old_pre END AS old_pre,
        CASE WHEN dpost THEN old_post END AS old_post,
        DATE '{day}' AS day
      FROM g WHERE dpre OR dpost ORDER BY id""").arrow()
    if isinstance(tbl, pa.RecordBatchReader):
        tbl = tbl.read_all()
    write_deltalake(dt, tbl.cast(LOG_SCHEMA), mode="overwrite",
                    predicate=f"day = '{day}'",
                    commit_properties=_meta(kind="collapse", collapsed=day,
                                            t=format_time(t_last)))
