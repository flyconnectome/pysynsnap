# pysynsnap

> **Experimental.** This repository is principally an experiment to work out
> a re-usable specification for publishing and reading versioned synapse
> root ids from [CAVE](https://github.com/CAVEconnectome). The code is a
> sketch to test that specification, not a supported package. Formats and
> APIs will change without notice.

CAVE synapse tables are large, but only their root ids change, and only a
little each day. The [aedes](https://github.com/flyconnectome/aedes) R package
keeps a local copy split into static data (written once), occasional full
snapshots of the roots, and small daily change logs. With the chunkedgraph it
can then answer queries at any time, including now, in seconds.

This repository holds:

- [docs/spec.md](docs/spec.md): the snapshot format as aedes writes it.
- [docs/proposal.md](docs/proposal.md): a draft proposal for the CAVE
  materialization engine to write the same data as part of its update runs,
  including a Delta Lake profile and alternatives.
- `pysynsnap`: a small Python reference reader on top of DuckDB and
  caveclient, plus a sketch of a Delta Lake writer (`pysynsnap.delta`).

## Install

```bash
pip install "pysynsnap[cave,delta] @ git+https://github.com/flyconnectome/pysynsnap"
```

## Use

```python
import pysynsnap as ss
from caveclient import CAVEclient

ss.download(url, "~/synsnap")         # url of a published snapshot folder
snap = ss.Snapshot("~/synsnap")
snap.tags()                           # snapshots, oldest first
snap.rows(pre=[...]).df()             # rows at the newest snapshot

client = CAVEclient("<datastack>")
ss.rows_at(snap, client, "now", post=[...]).df()  # rows at any later time
```

## Contributing

Issues and pull requests are welcome, especially on the specification. Tests
use only synthetic data:

```bash
pip install -e ".[dev]"
pytest
```

## Licence

MIT
