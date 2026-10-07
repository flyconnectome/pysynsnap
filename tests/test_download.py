import json
import shutil

import pytest

from conftest import as_set
from pysynsnap import Snapshot, download
from pysynsnap.download import md5


def publish(src, dest, tags):
    """A published folder with a format 2 manifest listing ``tags``."""
    dest.mkdir()
    files = []
    for p in ["static.parquet", "static.json"] + [
            f"{t}/{f.name}" for t in tags for f in sorted((src / t).iterdir())]:
        (dest / p).parent.mkdir(exist_ok=True)
        shutil.copy(src / p, dest / p)
        files.append({"path": p, "size": (dest / p).stat().st_size,
                      "md5": md5(dest / p)})
    s = Snapshot(src)
    snaps = [{"tag": t, "base": s.meta(t)["base"], "parent": s.meta(t)["parent"]}
             for t in tags]
    (dest / "manifest.json").write_text(json.dumps(
        {"format": 2, "latest": tags[-1], "snapshots": snaps, "files": files}))
    return dest.as_uri()


def test_download_round_trip(snapdir, tmp_path):
    url = publish(snapdir, tmp_path / "pub", ["v1", "c1", "c2"])
    done = download(url, tmp_path / "local")
    assert sorted(done) == ["c1", "c2", "v1"]
    a, b = Snapshot(snapdir), Snapshot(tmp_path / "local")
    assert as_set(a.rows("c2").df()) == as_set(b.rows("c2").df())
    assert not (tmp_path / "local" / ".staging" / "download").exists()
    # nothing new the second time
    assert download(url, tmp_path / "local") == []


def test_download_incremental_and_not_full(snapdir, tmp_path):
    url = publish(snapdir, tmp_path / "pub", ["v1", "c1"])
    assert download(url, tmp_path / "local", full=False) == []
    assert sorted(download(url, tmp_path / "local")) == ["c1", "v1"]
    url2 = publish(snapdir, tmp_path / "pub2", ["v1", "c1", "c2"])
    assert download(url2, tmp_path / "local", full=False) == ["c2"]


def test_download_refuses_other_static(snapdir, tmp_path):
    url = publish(snapdir, tmp_path / "pub", ["v1"])
    (tmp_path / "local").mkdir()
    (tmp_path / "local" / "static.json").write_text("{}")
    with pytest.raises(ValueError, match="different static"):
        download(url, tmp_path / "local")


def test_download_md5_mismatch(snapdir, tmp_path):
    url = publish(snapdir, tmp_path / "pub", ["v1"])
    (tmp_path / "pub" / "v1" / "by_pre.parquet").write_bytes(b"x")
    with pytest.raises(IOError, match="md5"):
        download(url, tmp_path / "local")
