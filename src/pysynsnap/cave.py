"""The two chunkedgraph calls snapshots need, on top of caveclient.

Functions take either a ``CAVEclient`` or its ``chunkedgraph`` (anything with
``get_roots`` and ``get_delta_roots``), which keeps them easy to test.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import numpy as np
import pandas as pd

from .times import parse_time


def chunkedgraph(client):
    return getattr(client, "chunkedgraph", client)


def delta_roots(client, past, future,
                max_interval: float = 86400) -> tuple[np.ndarray, np.ndarray]:
    """Roots expired and created between ``past`` and ``future`` (inclusive).

    CAVE counts edits before ``timestamp_future``, but a root lookup at
    ``future`` already sees an edit made exactly then, so the window is
    extended by 1 ms (edit times are whole ms). Long windows are fetched in
    ``max_interval`` second pieces.
    """
    cg = chunkedgraph(client)
    t, end = parse_time(past), parse_time(future) + pd.Timedelta(milliseconds=1)
    step = pd.Timedelta(seconds=max_interval)
    old, new = [], []
    while t < end:
        te = min(end, t + step)
        o, n = cg.get_delta_roots(t.to_pydatetime(), te.to_pydatetime())
        old.append(np.asarray(o, dtype=np.int64))
        new.append(np.asarray(n, dtype=np.int64))
        t = te
    cat = lambda x: np.unique(np.concatenate(x)) if x else np.array([], np.int64)
    return cat(old), cat(new)


def get_roots(client, sv, timestamp, chunksize: int = 100_000,
              threads: int = 4) -> np.ndarray:
    """Roots of supervoxels ``sv`` at ``timestamp`` as int64, in parallel
    chunks."""
    cg = chunkedgraph(client)
    sv = np.asarray(sv, dtype=np.int64)
    if not len(sv):
        return np.array([], np.int64)
    ts = parse_time(timestamp).to_pydatetime()
    chunks = [sv[i:i + chunksize] for i in range(0, len(sv), chunksize)]

    def f(c):
        r = np.asarray(cg.get_roots(c, timestamp=ts)).astype(np.int64)
        if len(r) != len(c):
            raise RuntimeError("supervoxel lookup returned the wrong length")
        return r

    with ThreadPoolExecutor(max_workers=threads) as ex:
        return np.concatenate(list(ex.map(f, chunks)))
