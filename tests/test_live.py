import numpy as np
import pandas as pd
import pytest

from conftest import TAGS, as_set
from pysynsnap import Snapshot, rows_at


@pytest.mark.parametrize("hours", [0, 0.5, 7, 30, 54, 60, 90])
def test_rows_at_matches_truth(snapdir, world, cg, hours):
    T = TAGS[1][1] + pd.Timedelta(hours=hours)
    got = rows_at(Snapshot(snapdir), cg, T).df()
    assert as_set(got) == as_set(world.rows(T))


def test_rows_at_filtered(snapdir, world, cg):
    T = TAGS[2][1] + pd.Timedelta(hours=20)
    truth = world.rows(T)
    some = truth.pre_root.unique()[:3]
    got = rows_at(Snapshot(snapdir), cg, T, pre=some).df()
    assert as_set(got) == as_set(truth[truth.pre_root.isin(some)])
    post = truth.post_root.unique()[:3]
    got = rows_at(Snapshot(snapdir), cg, T, post=post, details=True).df()
    assert as_set(got) == as_set(truth[truth.post_root.isin(post)])
    assert "pre_sv" in got.columns


def test_rows_at_skips_local_checkpoint(snapdir, cg):
    T = TAGS[3][1] + pd.Timedelta(hours=1)
    rows_at(Snapshot(snapdir), cg, T)
    _, a, b = next(c for c in cg.calls if c[0] == "get_delta_roots")
    assert a == TAGS[2][1] - pd.Timedelta(seconds=600)


def test_delta_roots_chunked(cg):
    from pysynsnap.cave import delta_roots
    a = TAGS[0][1]
    old, new = delta_roots(cg, a, a + pd.Timedelta(days=3), max_interval=3600)
    assert sum(c[0] == "get_delta_roots" for c in cg.calls) == 73
    want = {r for t, o, _ in cg.w.edits if a <= t <= a + pd.Timedelta(days=3)
            for r in o}
    assert set(old) == want and old.dtype == np.int64


def test_get_roots_chunked(world, cg):
    from pysynsnap.cave import get_roots
    sv = np.repeat(world.sv, 3)
    r = get_roots(cg, sv, TAGS[1][1], chunksize=7, threads=3)
    assert (r == world.roots(sv, TAGS[1][1])).all() and r.dtype == np.int64


def test_before_snapshot_errors(snapdir, cg):
    with pytest.raises(LookupError):
        rows_at(Snapshot(snapdir), cg, TAGS[0][1] - pd.Timedelta(hours=1))
