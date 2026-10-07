# Alternative framings for the dynamic data

Ben: this was just me asking for outside of the box alternatives and other table formats
that might handle various aspects of what we've been discussing. Feel free to ignore for now.

---

These are alternatives to "base table + change log". They build on the
two-axis view in [compaction.md](compaction.md).

## 1. Root membership facts

A root id is a fixed set of supervoxels and is never reused. So "this side of
synapse `id` is on root `R`" holds for R's whole lifetime, and it does not
need a time attached. The base and the change log can be replaced by two
tables.

### `membership`

One row per synapse side and each root that side has ever been on.
Append-only; rows are never updated or collapsed.

| Column | Type | Meaning |
|---|---|---|
| `id` | BIGINT | synapse id |
| `side` | `pre` / `post` | side of the synapse |
| `root` | BIGINT | a root that side has been on |

Sorted by `(root, side, id)`.

- At a rebase: two rows per synapse, from the base.
- Each run: one row per changed side, i.e. the non-NULL `pre_root` /
  `post_root` values of that run's log, unpivoted.

Example: synapse 7 starts with pre on root A and post on root X; later A is
merged into B.

| id | side | root |
|---|---|---|
| 7 | pre | A |
| 7 | post | X |
| 7 | pre | B |

### `roots`

One row per root in `membership`. This is the only table with times.

| Column | Meaning |
|---|---|
| `root` | root id |
| `created` | when the root came into existence |
| `expired` | when it was replaced; NULL if still alive |

### Queries

- **By root (no time needed).** `WHERE root IN (...) AND side = 'pre'` gives
  a neuron's outputs; `side = 'post'` gives its inputs. The result is complete
  for any root that was alive at the base or at some run, because every side
  on a new root is logged at the first run after the root is created.
- **At a time T.** A side's root at T is its row whose root has
  `created <= T < expired`. This is needed for partner roots and for
  full-table states.
- **After the newest run.** As before: `get_delta_roots` and `get_roots` from
  the chunkedgraph.

### Trade-offs

- One sort order serves both inputs and outputs, so `by_pre` and `by_post`
  copies are not needed.
- Rows are never collapsed, so the "which rows to keep" axis disappears.
  Retention is just dropping roots that expired before the oldest supported
  time. File-level compaction still applies.
- Size: about 2 × synapses at the base, plus the log rows without `t` and
  `old_*`.
- `roots` must be kept correct. Its times come from the chunkedgraph, or
  approximately from run times.

## 2. Supervoxel-keyed instead of synapse-keyed

Edits only change which root a supervoxel maps to. One `sv → root` table,
limited to supervoxels referenced by an annotation, could serve every
annotation table in a datastack. Each annotation table then holds supervoxel
columns and never changes. Reads add one join. Framing 1 works the same way
with `sv` in place of `(id, side)`.

This is a choice of scope (per table or per datastack), independent of
framing 1.

## 3. Table formats

| Format | Relevant feature | Caveat |
|---|---|---|
| Delta | atomic appends, file statistics, broad reader support | retention (`VACUUM`) is by age only |
| Hudi merge-on-read | base files plus change files merged on read, with scheduled compaction: the base + log model built in | narrower reader support |
| Iceberg | named tags pin chosen snapshots while others expire; equality deletes give cheap upserts by `id` | reader support for equality deletes varies |
| DuckLake | small commits stored in its catalog database, avoiding many small files | remote readers need access to the catalog database, not just the bucket |

Framing 1 is append-only and needs no merge semantics, so plain Delta is
enough for it.
