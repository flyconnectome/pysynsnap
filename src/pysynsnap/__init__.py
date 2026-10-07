"""Experimental: versioned CAVE synapse root-id snapshots.

The format is shared with the aedes R package; see docs/spec.md.
"""

from .download import download
from .snapshot import Snapshot

__all__ = ["Snapshot", "download"]
__version__ = "0.0.1"
