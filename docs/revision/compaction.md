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
  small. Both kinds of compaction rewrite files, so a mirror has to download
  the rewritten files again.
- A one-off remote reader benefits most from few rows (axis 1) and few,
  well-sorted files (axis 2).
