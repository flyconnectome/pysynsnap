# Compaction trade-offs for the change log

Treat the changes since the last base as one append-only Delta table. Each row
is a fact: at time `t`, this side of synapse `id` had root `r`. The state at
time T is the base plus the latest row per `id` and side with `t <= T`. The
write side has two independent choices.

## Axis 1: which rows to keep (lossy)

Collapsing a time range keeps only the latest row per `id` and side in that
range. Time points inside the range can no longer be reconstructed.

| Policy | Exact time points | Rows read for "now" |
|---|---|---|
| Keep every row until rebase | every run | all changes since base |
| Collapse by age (e.g. per run for 7 days, then per day, then per version) | recent runs, then coarser | about one row per changed side, plus recent detail |
| Collapse continuously | only the newest | one row per changed side |

To keep chosen time points under continuous collapse (e.g. versions, dailies),
store a frozen collapsed copy for each one. This bounds a read at base + 2
files, at the cost of duplicated rows and files that are rewritten.

A rebase is the full collapse into a new base. It is the same at every point
on this axis.

## Axis 2: file-level compaction (lossless)

The rows are unchanged; only their files change, so `t` survives and every
kept time point stays exact.

| Option | Effect |
|---|---|
| None | one or more small files per run; reads open many files |
| `OPTIMIZE` (bin-packing) | fewer, larger files |
| `OPTIMIZE` with sorting or Z-order on root columns | file statistics let readers skip files when filtering by root |

Commits that remove files break Delta version time travel only once `VACUUM`
deletes those files. Filtering on `t` still works. Do not vacuum with zero
retention while readers or the next run may hold old files.

## Consequences

- Time travel uses the `t` column, not Delta versions, so the axes are
  independent: any row policy can be combined with any file option.
- A local mirror keeps up cheaply when files are immutable and new files are
  small. Both kinds of compaction rewrite files (see [Mirroring](#mirroring)).
- A one-off remote reader benefits most from few rows (axis 1) and few,
  well-sorted files (axis 2).

## Mirroring

Every compaction is a commit that removes files and adds new ones. Delta
flags file compaction `dataChange: false`. Row collapse and rebase are
`dataChange: true`.

| Mirror | How | Effect of remote compaction |
|---|---|---|
| Physical | copy new data files, then new `_delta_log/` entries (e.g. `rclone copy`) | rewritten files are downloaded again |
| Logical | append remote rows with `t >` local watermark to a local table (or read Delta's change data feed) | file compaction is skipped; collapses of rows already held can be ignored |
| Lazy cache | download files on first read | old cached files become unused; new files are cache misses |

A logical pull is cheap only if files can be skipped by `t`. Sorting the whole
table by root spreads `t` across every file. Partitioning by day and sorting
by root within each day serves both root filters and incremental pulls.

## Expected scale

- Change files: about 1-10 MB per day.
- Rebase: monthly to quarterly, so the change table peaks at roughly
  30 MB-1 GB. A base is a few GB.

At this scale:

- **Rows (axis 1).** Keeping every row until rebase costs at most about 1 GB.
  Collapsing would save part of that but lose time points, which is probably
  not worth it.
- **Files (axis 2).** Hourly runs give hundreds to a few thousand small files
  per rebase period. Merging each finished day into one file sorted by root
  fixes this and touches at most about 10 MB per day.
- **Mirroring.** A physical mirror is enough. Daily compaction re-downloads
  at most a day's data. Even rewriting the whole change table costs less than
  a rebase, which every mirror downloads anyway.
