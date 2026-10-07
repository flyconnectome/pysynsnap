"""A synthetic segmentation with known history, a fake chunkedgraph for it
and snapshot folders written from it."""

from __future__ import annotations

import json
from itertools import count

import numpy as np
import pandas as pd
import pytest

from pysynsnap.times import format_time, parse_time

T0 = parse_time("2026-01-01 00:00:00 UTC")
N_SV, N_SYN = 40, 300


class World:
    """Supervoxels with roots changing at known edit times."""

    def __init__(self, seed=1, n_edits=60, days=4):
        rng = np.random.default_rng(seed)
        self._ids = count(1000)
        root = np.repeat([next(self._ids) for _ in range(N_SV // 4)], 4)
        self.history = [(T0 - pd.Timedelta(days=1), root.copy())]
        self.edits = []  # (t, expired, created)
        times = sorted(T0 + pd.to_timedelta(rng.uniform(0, days * 86400, n_edits),
                                            unit="s").round("ms"))
        for t in times:
            if rng.random() < 0.5:  # merge two roots
                a, b = rng.choice(np.unique(root), 2, replace=False)
                new = next(self._ids)
                root[np.isin(root, [a, b])] = new
                self.edits.append((t, [a, b], [new]))
            else:  # split one root in two
                a = rng.choice(np.unique(root))
                svs = np.flatnonzero(root == a)
                if len(svs) < 2:
                    continue
                half = rng.permutation(svs)[: len(svs) // 2]
                n1, n2 = next(self._ids), next(self._ids)
                root[svs] = n1
                root[half] = n2
                self.edits.append((t, [a], [n1, n2]))
            self.history.append((t, root.copy()))
        sv = np.arange(1, N_SV + 1, dtype=np.int64) + 10**15
        self.sv = sv
        self.static = pd.DataFrame({
            "id": np.arange(1, N_SYN + 1, dtype=np.int32),
            "pre_sv": rng.choice(sv, N_SYN), "post_sv": rng.choice(sv, N_SYN),
            "x": rng.integers(0, 1000, N_SYN), "y": rng.integers(0, 1000, N_SYN),
            "z": rng.integers(0, 1000, N_SYN), "size": rng.integers(1, 50, N_SYN),
        })

    def roots(self, sv, t):
        t = parse_time(t)
        r = [h for ht, h in self.history if ht <= t][-1]
        return r[np.asarray(sv, np.int64) - 10**15 - 1]

    def rows(self, t) -> pd.DataFrame:
        s = self.static
        return pd.DataFrame({"id": s.id.astype(np.int64),
                             "pre_root": self.roots(s.pre_sv, t),
                             "post_root": self.roots(s.post_sv, t)})


class FakeChunkedgraph:
    def __init__(self, world):
        self.w = world
        self.calls = []

    def get_roots(self, supervoxel_ids, timestamp=None, stop_layer=None):
        self.calls.append(("get_roots", len(supervoxel_ids)))
        return self.w.roots(supervoxel_ids, timestamp).astype(np.uint64)

    def get_delta_roots(self, timestamp_past, timestamp_future=None):
        a, b = parse_time(timestamp_past), parse_time(timestamp_future)
        self.calls.append(("get_delta_roots", a, b))
        ed = [e for e in self.w.edits if a <= e[0] < b]
        old = np.array([r for e in ed for r in e[1]], np.int64)
        new = np.array([r for e in ed for r in e[2]], np.int64)
        return old, new


def write_snapshots(world, root, tags):
    """Write ``tags`` [(tag, t, kind, parent)] to ``root``; the first is the
    full snapshot and the rest are checkpoints on it."""
    root.mkdir(parents=True, exist_ok=True)
    world.static.sort_values("id").to_parquet(root / "static.parquet", index=False)
    (root / "static.json").write_text(json.dumps({"rows": N_SYN}))
    base = tags[0][0]
    prev = {}
    for tag, t, kind, parent in tags:
        d = root / tag
        d.mkdir()
        cur = world.rows(t)
        meta = {"tag": tag, "timestamp": format_time(t), "kind": kind,
                "rows": len(cur)}
        if tag == base:
            cur.sort_values(["pre_root", "id"]).to_parquet(
                d / "by_pre.parquet", index=False)
            cur.sort_values(["post_root", "id"]).to_parquet(
                d / "by_post.parquet", index=False)
        else:
            old = prev[parent]
            m = cur.merge(old, on="id", suffixes=("", "_old"))
            dp, dq = m.pre_root != m.pre_root_old, m.post_root != m.post_root_old
            m = m[dp | dq]
            dp, dq = dp[m.index], dq[m.index]
            log = pd.DataFrame({
                "id": m.id, "t": parse_time(t),
                "pre_root": m.pre_root.where(dp).astype("Int64"),
                "post_root": m.post_root.where(dq).astype("Int64"),
                "old_pre": m.pre_root_old.where(dp).astype("Int64"),
                "old_post": m.post_root_old.where(dq).astype("Int64")})
            log.to_parquet(d / "log.parquet", index=False)
            meta.update(base=base, parent=parent)
        if tag == base:
            meta.update(base=None, parent=None, kind="version", version=1)
        (d / "meta.json").write_text(json.dumps(meta))
        prev[tag] = cur


TAGS = [("v1", T0, "version", None),
        ("c1", T0 + pd.Timedelta(days=1), "regular", "v1"),
        ("c2", T0 + pd.Timedelta(days=2), "regular", "c1"),
        ("c3", T0 + pd.Timedelta(days=2, hours=6), "local", "c2")]


@pytest.fixture(scope="session")
def world():
    return World()


@pytest.fixture
def cg(world):
    return FakeChunkedgraph(world)


@pytest.fixture(scope="session")
def snapdir(world, tmp_path_factory):
    root = tmp_path_factory.mktemp("snap")
    write_snapshots(world, root, TAGS)
    return root


def as_set(df) -> set:
    return set(map(tuple, df[["id", "pre_root", "post_root"]]
                   .astype("int64").to_numpy()))
