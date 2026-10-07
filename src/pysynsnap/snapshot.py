"""Reading a local snapshot folder (see docs/spec.md)."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import duckdb
import pandas as pd

from .times import parse_time

ROW_COLS = "id, pre_root, post_root"


def sql_str(x) -> str:
    return "'" + str(x).replace("'", "''") + "'"


class Snapshot:
    """A local snapshot folder.

    Parameters
    ----------
    root : str or Path
        Folder holding ``static.parquet`` and one subfolder per tag.
    con : duckdb.DuckDBPyConnection, optional
        Connection to run queries on. Defaults to a new in-memory one.
    """

    def __init__(self, root, con: duckdb.DuckDBPyConnection | None = None):
        self.root = Path(root).expanduser().resolve()
        if not self.root.is_dir():
            raise FileNotFoundError(f"No snapshot folder at {self.root}")
        self.con = con if con is not None else duckdb.connect()

    def __repr__(self):
        return f"Snapshot({str(self.root)!r})"

    # -- metadata ----------------------------------------------------------

    def path(self, *parts) -> Path:
        return self.root.joinpath(*parts)

    def meta(self, tag: str) -> dict:
        f = self.path(tag, "meta.json")
        if not f.exists():
            raise KeyError(f"No snapshot {tag!r} in {self.root}")
        m = json.loads(f.read_text())
        m.setdefault("base", None)
        if m.get("parent") is None:
            m["parent"] = m["base"]
        m.setdefault("kind", None)
        m["timestamp"] = parse_time(m["timestamp"])
        return m

    def tags(self, all: bool = False) -> pd.DataFrame:
        """Snapshots, oldest first.

        Checkpoints whose chain back to their base is incomplete are left out
        unless ``all``; their ``usable`` column is False.
        """
        tags = sorted(p.parent.name for p in self.root.glob("*/meta.json"))
        metas = [self.meta(t) for t in tags]
        # older checkpoints without a log (aedes converts them) are unusable
        old = {m["tag"] for m in metas
               if m["base"] and not self.path(m["tag"], "log.parquet").exists()}
        df = pd.DataFrame(
            [{k: m.get(k) for k in ("tag", "timestamp", "base", "parent", "kind")}
             for m in metas],
            columns=["tag", "timestamp", "base", "parent", "kind"])
        df = df.sort_values("timestamp", kind="stable").reset_index(drop=True)
        # parents are always older, so one pass in time order settles each chain
        ok: dict[str, bool] = {}
        bases = dict(zip(df.tag, df.base))
        for tag, base, parent in zip(df.tag, df.base, df.parent):
            if not base:
                ok[tag] = True
                continue
            pbase = bases.get(parent)
            ok[tag] = (tag not in old and ok.get(parent, False)
                       and (pbase == base or (parent == base and not pbase)))
        df["usable"] = df.tag.map(ok).astype(bool)
        return df if all else df[df.usable].reset_index(drop=True)

    def chain(self, tag: str) -> list[str]:
        """Checkpoints from ``tag`` back to (not including) its base, newest
        first; empty for a full snapshot."""
        m = self.meta(tag)
        chain = []
        while m["base"]:
            if not self.path(m["tag"], "log.parquet").exists():
                raise ValueError(f"Snapshot {m['tag']!r} has no log.parquet")
            chain.append(m["tag"])
            if m["parent"] == m["base"]:
                break
            m2 = self.meta(m["parent"])
            if m2["base"] != m["base"]:
                raise ValueError(f"Snapshot {m['tag']!r} has parent "
                                 f"{m['parent']!r} with a different base")
            m = m2
        return chain

    def latest(self) -> str:
        tags = self.tags()
        if not len(tags):
            raise LookupError(f"No snapshots in {self.root}")
        return tags.tag.iloc[-1]

    def at(self, when: dt.datetime | str = "now", tol: float = 1.0,
           locals: bool = True) -> str | None:
        """Newest snapshot at or before ``when``, allowing ``tol`` seconds.

        With ``locals=False``, local checkpoints only count at ``when`` itself:
        they were made without waiting for late edits, so anything worked out
        for a later time should start from a published snapshot.
        """
        tags = self.tags()
        exact = pd.Series(False, index=tags.index)
        if when != "now":
            when = parse_time(when)
            age = (when - tags.timestamp).dt.total_seconds()
            tags, exact = tags[age >= -tol], (age.abs() <= tol)[age >= -tol]
        if not locals:
            tags = tags[~((tags.kind == "local") & tags.base.notna()) | exact]
        return tags.tag.iloc[-1] if len(tags) else None

    # -- rows --------------------------------------------------------------

    def ids_file(self, tag: str, side: str | None = None) -> Path:
        if side == "post":
            f = self.path(tag, "by_post.parquet")
            if f.exists():
                return f
        return self.path(tag, "by_pre.parquet")

    def static_sql(self) -> str:
        return f"read_parquet({sql_str(self.path('static.parquet'))})"

    def rows_sql(self, tag: str | None = None, side: str | None = None,
                 where: str | None = None, changed: str | None = None) -> str:
        """SQL giving ``id, pre_root, post_root`` at snapshot ``tag``.

        ``where`` may only refer to ``pre_root`` and ``post_root``. With
        ``side``, reads the copy sorted by that root, so a ``where`` on it
        skips row groups. The result is a plain SELECT, so it can be UNIONed.

        ``changed`` picks the changed rows that might match ``where``. A side
        that did not change is NULL in the folded log, so it must hold when
        either side matches; defaults to ``where``, which is right when it
        refers to one side only. :func:`roots_where` gives both.
        """
        tag = tag or self.latest()
        w = "" if where is None else f" WHERE {where}"
        changed = changed or where
        m = self.meta(tag)
        if not m["base"]:
            return f"SELECT {ROW_COLS} FROM {sql_str(self.ids_file(tag, side))}{w}"
        base = sql_str(self.ids_file(m["base"], side))
        logs = ", ".join(sql_str(self.path(t, "log.parquet"))
                         for t in self.chain(tag))
        fold = f"""SELECT id,
            arg_max(pre_root, t) FILTER (WHERE pre_root IS NOT NULL) AS pre_root,
            arg_max(post_root, t) FILTER (WHERE post_root IS NOT NULL) AS post_root
          FROM read_parquet([{logs}]) GROUP BY id"""
        # base rows matching `where` before or after the changes
        b = base if where is None else f"""(SELECT {ROW_COLS} FROM {base}
            WHERE {where} UNION SELECT {ROW_COLS} FROM {base}
            WHERE id IN (SELECT id FROM synsnap_f WHERE {changed}))"""
        return f"""SELECT {ROW_COLS} FROM (WITH synsnap_f AS ({fold})
          SELECT b.id,
            coalesce(f.pre_root, b.pre_root) AS pre_root,
            coalesce(f.post_root, b.post_root) AS post_root
          FROM {b} b LEFT JOIN synsnap_f f USING (id)){w}"""

    def rows(self, tag: str | None = None, pre=None, post=None,
             details: bool = False) -> duckdb.DuckDBPyRelation:
        """Rows at snapshot ``tag`` (default: newest) as a DuckDB relation.

        ``pre`` and ``post`` restrict to synapses with those pre- or
        postsynaptic roots. ``details`` joins the static columns.
        """
        side, where, changed = roots_where(pre, post)
        sql = self.rows_sql(tag, side=side, where=where, changed=changed)
        if details:
            sql = f"SELECT * FROM ({sql}) JOIN {self.static_sql()} USING (id)"
        return self.con.sql(sql)


def ids_sql(x) -> str:
    return ",".join(str(int(i)) for i in x)


def roots_where(pre=None, post=None):
    """(side, where, changed) restricting rows to given pre and/or post
    roots; see :meth:`Snapshot.rows_sql`."""
    parts, side = [], None
    if pre is not None:
        parts.append(f"pre_root IN ({ids_sql(pre) or 'NULL'})")
        side = "pre"
    if post is not None:
        parts.append(f"post_root IN ({ids_sql(post) or 'NULL'})")
        side = side or "post"
    if not parts:
        return side, None, None
    return side, " AND ".join(parts), " OR ".join(parts)
