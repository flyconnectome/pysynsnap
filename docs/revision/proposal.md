# Proposal: synapse snapshots as a materialization output

> **Status: experimental draft for discussion.** Nothing here is agreed or
> implemented in CAVE. The aim is to work out a format that could be shared
> by the CAVE materialization engine and by clients such as
> [aedes](https://github.com/flyconnectome/aedes).

## The idea

Each materialization update run already finds rows whose root ids have
expired and looks up their new roots. Those changed rows are exactly what a
snapshot checkpoint log contains (see [spec.md](spec.md)). If each run also
published its changed rows, the materialization dump bucket would carry a
complete, time-indexed history of root ids. Clients could then rebuild the
synapse table's roots at any run time from a base dump plus that log, and
reach any later time with the chunkedgraph.

- Base and static data are parquet dumps the materialization engine can
  already write.
- The log is small: of order 1 MB per day for a ~10⁸ synapse table.
- Clients need only DuckDB (or another parquet / Delta reader) plus
  caveclient for times after the newest run.

Today a separate "Lab server" does this for aedes: it runs hourly, makes one
checkpoint a day plus one at each materialization version, and publishes them
over https in the format in [spec.md](spec.md).

## Writer guarantees

Whatever the packaging, for two consecutive runs at T1 < T2 the output for T2
must be:

1. **Complete.** Every synapse side whose root at T2 differs from its root at
   T1 has a row.
2. **Correct.** Each value is `get_roots(sv, timestamp=T2)` of that side's
   supervoxel. Unchanged sides are NULL, and `old_*` equals the state at T1.
3. **Settled.** All edits up to T2 are visible when the run reads them: run T2
   well in the past, or re-read delta roots from T1 minus an overlap.
4. **Exact times.** `t` is UTC to the millisecond; a version run uses the
   version's exact timestamp. `get_delta_roots` excludes an edit made exactly
   at its end time, while `get_roots` at that time includes it, so the delta
   window should end 1 ms after T2.
5. **Atomic.** A run's rows appear all at once or not at all.

Comparing an independent implementation (such as the Lab server's) with the
materialization output at shared times, e.g. each version, is a cheap
continuous check.

## Where it would hook into MaterializationEngine

From a read of MaterializationEngine v5.30.0 (paths under
`materializationengine/`). Please correct anything we have misread.

| Step | Code | Notes |
|---|---|---|
| Periodic run | `workflows/update_database_workflow.py` `update_database_workflow` | sets the run time T (`materialization_time_stamp`) and chains ingest, missing roots and root update per table |
| Expired roots | `workflows/update_root_ids.py` `get_expired_root_ids_from_pcg`, `lookup_expired_root_ids` | `get_delta_roots(last_updated, T)` |
| Rows to update | `get_supervoxel_id_queries` | one query per root column, so pre and post are separate tasks |
| New roots | `get_new_root_ids`, `lookup_new_root_ids` | `get_roots(sv, T)` then `bulk_update_mappings` by id; the old root is in the dataframe just before it is overwritten |
| Run recorded | `shared_tasks.py` `update_metadata` | sets `SegmentationMetadata.last_updated = T`; the only record of a run |
| Version freeze | `workflows/complete_workflow.py` | runs the same chain at the version time, then copies the database |
| Existing Delta export | `workflows/deltalake_export.py` | full, write-once table per version |

The natural capture point is `get_new_root_ids`: it has, per batch, the id,
old root, supervoxel and new root for one side, plus T. Rows should be kept
only where old != new (rows that had no root, and the "look up all roots"
mode, would otherwise appear as changes). Many small concurrent Delta commits
would contend, so tasks should stage their rows and one step after
`update_metadata` should commit them once per run.

### Things to keep an eye on

The update run already does nearly all of the work. A change log makes a few
timing details more visible than they are for the live database, because the
log records each run's result for good. None of these are known problems;
they are just worth bearing in mind when wiring things up:

- **Edits that are listed late.** Each window runs from `last_updated` to T.
  The chunkedgraph occasionally lists an edit a little after it was made, so
  starting each window a few minutes earlier (aedes uses 10) would pick these
  up. The update is idempotent, so the extra cost is small.
- **Runs that coincide.** It may be worth making sure an hourly update and a
  version build don't run at the same time, so that a version and the log
  always agree on the roots at its time.
- **First run for a new table.** When `last_updated` is not yet set, the
  window starts 5 days back, which is worth knowing for tables whose roots
  were looked up earlier than that.
- **Time formats.** Run times are parsed as `"%Y-%m-%d %H:%M:%S.%f"`; writing
  them in exactly that form (e.g. always with microseconds, no time zone) keeps
  that parsing happy.
- **Manually triggered runs.** These use a different lock key from scheduled
  ones, so it helps to avoid starting one while a scheduled run is going.

## Packaging options

### A. Append-only Delta log (sketched in `pysynsnap.delta`)

- `<uri>/bases`: full `id, pre_root, post_root` per version, partitioned by
  version, sorted by `pre_root`.
- `<uri>/log`: one commit per run, rows as in spec.md plus a `day`
  partition. A run with no changes still commits, so its time is recorded in
  the commit metadata (`synsnap.t`, `synsnap.kind`, `synsnap.version`).
- State at T = newest base at or before T, plus the log rows with
  base time < t <= T, folded as in spec.md.
- Compaction: `OPTIMIZE` keeps every run exact; collapsing a finished day to
  one row per synapse keeps only that day's last run exact. Do not vacuum with
  zero retention while readers or the next run may hold old files.

### B. Cumulative overlays

Each delta file holds every row changed since its anchor (a base or a
version), so a read is at most base + two files, and newer files replace older
ones. A cumulative file is a fold of the chain of logs since its anchor, so
the two options hold the same information; they differ in who folds and when.

| | Chain of logs (A, aedes) | Cumulative overlays (B) |
|---|---|---|
| Files per read | base + n logs | base + 2 |
| Writer cost per run | new rows only | rewrite the file since the anchor |
| Mirror following daily | downloads one day's log | re-downloads a growing file |
| Cold start at "now" | base + all logs since base | base + one deduplicated file |
| Past time points | free (every checkpoint) | one file per kept time point |
| Files | immutable | live file overwritten |

Chains suit clients that keep a local mirror; cumulative files suit one-off
remote readers. Both can be served: keep the immutable per-run logs as the
record, and periodically publish a folded file (anchor to watermark) as a
derived shortcut that cold readers use in place of the logs before it.

## Open questions

- Does the update run look up new roots for every supervoxel on an expired
  root at one run time? If so its output is the log almost as is.
- What cadence and settling delay are acceptable for the chunkedgraph?
- Should log rows carry both roots (simpler to fold in any engine) or only the
  changed side (as now, which also records which side changed)?
- Which tables have static annotations, and how should deleted or new
  annotations be recorded?
