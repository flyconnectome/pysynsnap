import duckdb
import pandas as pd
import pytest

pytest.importorskip("deltalake")

from conftest import T0, as_set  # noqa: E402
from pysynsnap import delta  # noqa: E402

RUNS = [T0 + pd.Timedelta(hours=h) for h in range(0, 72, 3)]


def state(uri, T):
    con = duckdb.connect()
    return con.sql(delta.state_sql(con, uri, T)).df()


@pytest.fixture
def store(world, tmp_path):
    uri = str(tmp_path / "store")
    delta.write_base(uri, world.rows(RUNS[0]), RUNS[0], version=1)
    for a, b in zip(RUNS, RUNS[1:]):
        rows = delta.log_rows(world.rows(a), world.rows(b), b)
        delta.append_run(uri, rows, b)
    return uri


def test_state_at_runs(world, store):
    for T in RUNS[::4] + [RUNS[-1]]:
        assert as_set(state(store, T)) == as_set(world.rows(T))


def test_checkpoints_record_empty_runs(world, store):
    cp = delta.checkpoints(store)
    assert list(cp.kind) == ["base"] + ["run"] * (len(RUNS) - 1)
    assert list(cp.t) == RUNS
    n = delta.DeltaTable(f"{store}/log").to_pyarrow_table().num_rows
    assert n < sum(len(world.rows(t)) for t in RUNS)


def test_second_base(world, store):
    v2 = RUNS[8]
    delta.write_base(store, world.rows(v2), v2, version=2)
    assert as_set(state(store, RUNS[10])) == as_set(world.rows(RUNS[10]))
    assert as_set(state(store, RUNS[5])) == as_set(world.rows(RUNS[5]))


def test_compact_keeps_runs(world, store):
    delta.compact(store)
    for T in RUNS[1::5]:
        assert as_set(state(store, T)) == as_set(world.rows(T))


def test_collapse_day(world, store):
    day = (T0 + pd.Timedelta(days=1)).date()
    before = delta.checkpoints(store)
    delta.collapse_day(store, day)
    after = delta.checkpoints(store)
    assert len(after) < len(before)
    in_day = after[after.t.dt.date == day]
    assert len(in_day) >= 1
    for T in list(in_day.t) + [RUNS[-1], RUNS[5]]:
        assert as_set(state(store, T)) == as_set(world.rows(T))


def test_no_base_errors(store):
    with pytest.raises(LookupError):
        state(store, T0 - pd.Timedelta(hours=1))
