"""Downloading published snapshots (manifest format 2, see docs/spec.md)."""

from __future__ import annotations

import hashlib
import json
import shutil
import urllib.request
from pathlib import Path

import pandas as pd

from .snapshot import Snapshot


def manifest_url(url: str) -> str:
    return url if url.endswith(".json") else url.rstrip("/") + "/manifest.json"


def read_manifest(url: str) -> dict:
    with urllib.request.urlopen(manifest_url(url)) as r:
        m = json.load(r)
    if m.get("format") != 2:
        raise ValueError("Unsupported snapshot manifest format")
    return m


def _rows(table) -> list[dict]:
    """jsonlite writes data.frames as a list of row objects."""
    if isinstance(table, dict):  # column-wise
        keys = list(table)
        return [dict(zip(keys, v)) for v in zip(*table.values())]
    return list(table)


def md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def _fetch(url: str, dest: Path, want_md5: str) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with urllib.request.urlopen(url) as r, open(dest, "wb") as f:
        shutil.copyfileobj(r, f, 1 << 20)
    if md5(dest) != want_md5:
        dest.unlink()
        raise IOError(f"md5 mismatch for {url.rsplit('/', 1)[-1]}")


def download(url: str, root, full: bool = True) -> list[str]:
    """Download the snapshots listed in the manifest at ``url`` into ``root``.

    Existing snapshots are never changed: a tag already in ``root`` (with the
    same base) is skipped, and different static data is an error. Each
    snapshot comes after its parent, every file is checked against its md5
    and each ``meta.json`` is placed last. With ``full=False``, full snapshots
    not already in ``root`` (and checkpoints on them) are skipped. Returns the
    tags downloaded.
    """
    root = Path(root).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    m = read_manifest(url)
    base_url = manifest_url(url).rsplit("/", 1)[0] + "/"
    files = {f["path"]: f for f in _rows(m["files"])}
    stage = root / ".staging" / "download"
    stage.mkdir(parents=True, exist_ok=True)

    def get(paths):
        for p in paths:
            _fetch(base_url + p, stage / p, files[p]["md5"])
        # in order, so meta.json / static.json go last
        for p in paths:
            (root / p).parent.mkdir(parents=True, exist_ok=True)
            (stage / p).replace(root / p)

    sj = root / "static.json"
    if sj.exists():
        if md5(sj) != files["static.json"]["md5"]:
            raise ValueError(f"The snapshots in {root} were built from different "
                             "static data; use a new folder")
    else:
        get(["static.parquet", "static.json"])

    snaps = {s["tag"]: s for s in _rows(m["snapshots"])}
    local = Snapshot(root).tags()
    have = {t for t, b in zip(local.tag, local.base)
            if t in snaps and snaps[t].get("base") == (None if pd.isna(b) else b)}
    todo = [t for t in snaps if t not in have]
    if not full:
        todo = [t for t in todo if snaps[t].get("base")]
    done: list[str] = []
    while todo:
        ready = [t for t in todo if not snaps[t].get("base")
                 or snaps[t]["parent"] in have | set(done)]
        if not ready:
            if not full:
                break
            raise ValueError(f"Published snapshots {todo} need snapshots that "
                             "are not published")
        for t in ready:
            ps = [p for p in files if p.startswith(t + "/")]
            # meta.json last
            get(sorted(ps, key=lambda p: p.endswith("/meta.json")))
        done += ready
        todo = [t for t in todo if t not in ready]
    shutil.rmtree(stage, ignore_errors=True)
    return done
