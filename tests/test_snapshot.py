import json

import pandas as pd
import pytest

from conftest import TAGS, as_set
from pysynsnap import Snapshot


def test_tags_and_chain(snapdir):
    s = Snapshot(snapdir)
    assert list(s.tags().tag) == [t[0] for t in TAGS]
    assert s.chain("v1") == []
    assert s.chain("c3") == ["c3", "c2", "c1"]
    assert s.latest() == "c3"


def test_at(snapdir):
    s = Snapshot(snapdir)
    t3 = TAGS[3][1]
    assert s.at(TAGS[0][1] - pd.Timedelta(hours=1)) is None
    assert s.at(t3 + pd.Timedelta(hours=1)) == "c3"
    # local checkpoints only count at their own time
    assert s.at(t3 + pd.Timedelta(hours=1), locals=False) == "c2"
    assert s.at(t3, locals=False) == "c3"


@pytest.mark.parametrize("tag", [t[0] for t in TAGS])
def test_rows_match_truth(snapdir, world, tag):
    s = Snapshot(snapdir)
    t = dict((x[0], x[1]) for x in TAGS)[tag]
    truth = world.rows(t)
    assert as_set(s.rows(tag).df()) == as_set(truth)
    some = truth.pre_root.unique()[:3]
    got = s.rows(tag, pre=some).df()
    assert as_set(got) == as_set(truth[truth.pre_root.isin(some)])
    post = truth.post_root.unique()[:2]
    got = s.rows(tag, pre=some, post=post).df()
    want = truth[truth.pre_root.isin(some) & truth.post_root.isin(post)]
    assert as_set(got) == as_set(want)


def test_details(snapdir, world):
    d = Snapshot(snapdir).rows("c1", details=True).df()
    assert {"pre_sv", "post_sv", "x", "size"} <= set(d.columns)
    assert len(d) == len(world.static)


def test_broken_chain_unusable(snapdir, tmp_path):
    import shutil
    shutil.copytree(snapdir, tmp_path / "s")
    (tmp_path / "s" / "c1" / "log.parquet").unlink()
    s = Snapshot(tmp_path / "s")
    assert list(s.tags().tag) == ["v1"]
    assert not s.tags(all=True).set_index("tag").usable["c2"]


def test_meta_defaults(snapdir):
    m = Snapshot(snapdir).meta("c1")
    assert m["parent"] == "v1" and m["timestamp"] == TAGS[1][1]
    raw = json.loads((snapdir / "v1" / "meta.json").read_text())
    assert raw["kind"] == "version"
