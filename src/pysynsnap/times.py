"""Time handling. Snapshot times are UTC, kept to whole milliseconds."""

from __future__ import annotations

import datetime as dt

import pandas as pd

UTC = dt.timezone.utc


def parse_time(x) -> pd.Timestamp:
    """A tz-aware UTC Timestamp from text like ``2026-10-06 14:12:03.123 UTC``,
    a datetime or a Timestamp. Naive values are taken as UTC."""
    if isinstance(x, str):
        x = x.removesuffix(" UTC").replace("T", " ")
    t = pd.Timestamp(x)
    return t.tz_localize(UTC) if t.tzinfo is None else t.tz_convert(UTC)


def format_time(t) -> str:
    """Exact text of a time to the microsecond, as written to meta.json."""
    return parse_time(t).strftime("%Y-%m-%d %H:%M:%S.%f UTC")


def now() -> pd.Timestamp:
    return pd.Timestamp.now(tz=UTC)
