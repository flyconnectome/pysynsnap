"""Experimental: versioned CAVE synapse root-id snapshots.

The format is shared with the aedes R package; see docs/spec.md.
"""

from .download import download
from .live import rows_at
from .snapshot import Snapshot

__all__ = ["Snapshot", "download", "rows_at"]
__version__ = "0.0.1"
