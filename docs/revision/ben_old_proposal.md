# Statement of Work: Incremental Root-ID Publishing for the CAVE Delta Lake Export

**Status:** Draft for discussion · **Prepared by:** Ben · **Date:** October 2026

---

## 1. Background

The CAVE MaterializationEngine currently exports each materialization version as a Delta Lake table (MaterializationEngine#220). Each export is a full copy of the table, written once for each indexed column, so every version rewrites columns that never change, such as supervoxel IDs, positions and sizes. Exports exist only at materialization versions, so the export can't answer questions about the present.

Greg Jefferis's `aedes` package (flyconnectome/aedes; see PR #38 and the *Local Synapse Snapshots* vignette) shows that a different layout is practical. Static columns are stored once. A base table holds only `id, pre_root, post_root`. Small change logs, about 1 MB per day for the Aedes synapse table, are layered on top. Times after the newest snapshot are reached by calling `get_delta_roots` and re-looking-up only the supervoxels that changed. This gives fast local queries at any materialization version and up to the second, at a small incremental cost.

This SOW proposes building the same capability into the CAVE export itself. The aims are to keep the remote queryability of Delta, make each new time point cost roughly the size of its edits, and add a near-real-time live layer. The work runs in shadow alongside the existing export until it has been shown to be correct.

## 2. Objectives

1. Split the export into a static part, written once, and a dynamic part holding root IDs. The split follows the existing annotation/segmentation table divide.
2. Produce a near-real-time stream of changed rows, at an interval of about 5 minutes, from the newest materialization version up to the present.
3. Publish that stream in a form that remote and local readers can merge cheaply and safely.
4. Compact the stream into version deltas and daily deltas whenever a version is minted, so that any supported time point can be read as at most three layers on top of the static data.
5. Run in parallel with the existing materialization and export, with automated correctness checks, before anything is made public.

**Supported time points.** The live state, a daily state for each of the last 7 days, and every blessed materialization version. Arbitrary historical times beyond these are out of scope. Clients that need them can compute them from CAVE deltas, as `aedes` does.

## 3. Proposed Design (summary)

A read at time *T* merges a fixed stack:

| Layer | Contents | Lifetime |
| --- | --- | --- |
| Static | Annotation columns plus supervoxel IDs, bucketed on `id` | Written once at t0 |
| Root base *B* | Full `id, pre_root, post_root` at some version, clustered on root (pre- and post-sorted copies) | Replaced only on rebase |
| Version delta *B → v* | Every row whose roots changed between *B* and version *v*, cumulative from *B* | Kept as long as version *v* is kept |
| Live / daily delta *v → T* | Every row whose roots changed between *v* and *T*, cumulative from *v* | Live file is reset at each version; dailies are kept 7 days |

Deltas are cumulative from their anchor, not chained to the previous delta. Selecting files gives time travel. The row rule is a simple override: a delta row replaces a base row with the same `id`. Read depth is therefore bounded at base plus two deltas, whatever the history. Published files are immutable and listed in a manifest, which is written last.

A filtered read takes this form, which stays correct when filtering on root columns:

```sql
SELECT * FROM base   WHERE pre_root IN (...) AND id NOT IN (SELECT id FROM vdelta UNION SELECT id FROM ldelta)
UNION ALL
SELECT * FROM vdelta WHERE pre_root IN (...) AND id NOT IN (SELECT id FROM ldelta)
UNION ALL
SELECT * FROM ldelta WHERE pre_root IN (...)
```

Delta Lake is used for its packaging (atomic commits, file statistics, remote range reads, ecosystem readers) rather than for time travel. Delta's age-based VACUUM can't keep particular versions while expiring the time points around them, and doing live updates as `MERGE` with deletion vectors would touch base-file metadata on every update.

## 4. Scope of Work

### WP1. Static/dynamic split at t0

Extend the current export so it writes the static and dynamic parts separately. Static is the annotation columns plus the segmentation table's supervoxel IDs, bucketed on `id`. Dynamic is root IDs only, as pre- and post-sorted copies. A defined class of tables is treated as having static annotations. The delta schema still reserves room for tombstones and new `id`s, so annotation-side changes can be supported later without a format change.

*Deliverables:* split writer in the export pipeline; schema and manifest specification; a first t0 export of one production table (synapses).

### WP2. Morsel generation inside MaterializationEngine

Morsels are produced by MaterializationEngine's existing live-database update, not by a separate service. That workflow already does the work a morsel needs: it finds expired roots with `get_delta_roots`, selects the rows of the live segmentation table that reference them, re-looks-up their supervoxels with `get_roots`, and writes the new roots back. It also runs at the point where versions are minted. WP2 adds capture and emission around that code and changes as little of it as possible.

**Existing code reused as-is.** The new step is added to the `update_database_workflow` chain (in `workflows/update_database_workflow.py`), which runs `ingest_new_annotations_workflow`, `find_missing_root_ids_workflow` and `update_root_ids_workflow` for each table. The `update_root_ids_workflow` path (in `workflows/update_root_ids.py`) works like this:

1. `get_expired_root_ids_from_pcg` / `lookup_expired_root_ids` call `get_delta_roots(last_updated_time_stamp, materialization_time_stamp)`.
2. `get_supervoxel_id_queries` selects `id, *_root_id, *_supervoxel_id` from the live segmentation table where the root is expired or NULL.
3. `get_new_root_ids` / `lookup_new_root_ids` call `get_roots` at `materialization_time_stamp` and write the results with `bulk_update_mappings`.
4. `update_metadata` advances `last_updated_time_stamp`.

`_run_complete_workflow` runs the same chain before freezing a new version. A minted version's root table is therefore exactly the live table after that cycle.

Because the filter runs against the live segmentation table, it already runs against the current head state. That handles synapses whose roots change more than once in a day, so no separate head needs to be maintained.

**Additions.**

*(a) Change capture.* In each task that writes roots to the live segmentation table (`get_new_root_ids`, the missing-root lookup used by `find_missing_root_ids_workflow`, and new-annotation ingestion), insert `(cycle_ts, id, column, new_root)` into a per-table changelog table in the same database session as the existing write. Capture is then transactional with the live update: a row is logged exactly when it is changed, and a retried Celery task logs again idempotently. `cycle_ts` is the cycle's `materialization_time_stamp`.

*(b) Emit task.* Add an `emit_morsel` task to the chain after `update_metadata`, so it runs only once the whole cycle has finished. It reads the changelog ids for `cycle_ts` and reads full current `id, pre_root, post_root` rows for those ids from the live segmentation table. This turns the per-column updates into whole rows. It then writes the morsel Parquet to the audit bucket and hands it to the WP3 publisher. The `LockedTask` lock on `update_database_workflow` already stops the next cycle from writing while emission runs. The morsel's watermark is `cycle_ts`. In the cycle run by `_run_complete_workflow`, the morsel is also tagged with the new version number, which gives WP4 its minting trigger.

*(c) Overlap window.* As far as we can tell from the code, `get_delta_roots` is called starting exactly at `last_updated_time_stamp`. That means an edit which becomes visible in the chunkedgraph after a cycle has queried past its timestamp is never picked up, by the live database or by any version frozen from it. The fix is to query from `last_updated_time_stamp − overlap` (about 10 minutes). Roots in the overlap that were already processed no longer appear in the live table, so the extra work is small. This change benefits the live database regardless of the export, so it should be confirmed and, if needed, proposed upstream as its own change.

*(d) Cadence.* By default the update runs hourly, during set hours on weekdays (the `Update Live Database` beat schedule). The morsel interval is whatever the cycle interval is. The 5-minute target becomes a schedule change, which is feasible only if cycle duration and chunkedgraph load allow it, and WP2 measures both. Moving from hourly to around 15 minutes is a reasonable first step.

*Deliverables:* changelog table and capture hooks in the existing write paths; `emit_morsel` task; the overlap change, as a separate PR; a raw morsel audit log in a TTL bucket; measurements of cycle duration, `get_roots` volume and morsel size at the tested cadences.

### WP3. Live publishing

Publish a single cumulative live file per version, `live/v<N>/cumulative.parquet`, overwritten atomically after each interval. Its Parquet key-value metadata and its path record its base, its version and its watermark. A pointer file (`current.json`) is updated last when the version rolls over, so readers never pair mismatched layers. Readers poll with conditional GETs on the ETag. If the cumulative file grows too large, a short tail of recent morsels can sit on top of it and be folded in hourly.

The other publishing options considered were a JSON endpoint, appending every 5 minutes to the Delta table, and a staging bucket of per-interval files. They are recorded in §8 for discussion.

*Deliverables:* publisher; manifest and pointer specification; reference reader in Python, using DuckDB and/or polars.

### WP4. Minting, compaction and retention

When a version is minted, freeze the cumulative live file as of the mint cycle and fold it into the version delta. Because that cycle is the same live update `_run_complete_workflow` runs before freezing, the result matches the frozen version by construction. Edits that become visible late after a mint are picked up by the next cycle's overlap window, just as they are for the frozen version's successors. Daily states are kept as frozen live files and expire after 7 days. Each blessed version keeps its version delta. When the version delta exceeds a threshold, provisionally 5–10% of rows, a new base is written. Old bases are reference-counted until no retained version depends on them. Long-lived blessed versions can be given a standalone root table so they don't keep an old base alive.

Compaction never rewrites the base or the static part. Local mirrors therefore stay in sync by following the manifest: they download added files and delete removed ones, without replaying compaction. The only large client transfer is a rebase, and that covers the root table only.

*Deliverables:* compaction job triggered by version minting; retention and garbage-collection job; rebase procedure.

### WP5. Shadow run and validation

Run WP1–WP4 alongside the existing materialization and export (WP2 runs inside it, behind a feature flag), for at least several weeks covering multiple version mints, with no public exposure.

*Deliverables:* automated validation suite (§6); monitoring dashboard; shadow-run report with a go/no-go recommendation.

### WP6. Client access (optional, after the shadow run)

Provide a client reader, a local mirror tool and documentation. Coordinate with the `aedes` team so that Greg's live-head logic can sit on top of the published layers instead of a lab-server snapshot.

### Out of scope

Arbitrary-time history beyond the supported time points. Changes to MaterializationEngine's existing versioning or retention. Tables with mutable annotations, beyond reserving schema space for them. Retiring the existing export, which will be decided after the shadow run.

## 5. Phasing

| Phase | Work packages | Exit criterion |
| --- | --- | --- |
| 0. Spec | Decisions D1–D6 below; schema and manifest spec | Agreed design |
| 1. Split | WP1 | t0 split export matches the current export row for row |
| 2. Live | WP2, WP3 | Morsels emitted from every live-update cycle on LTV; cadence reduced from hourly without degrading the live update |
| 3. Compaction | WP4 | Clean version mints and rebases in shadow |
| 4. Shadow | WP5 | Validation suite green across multiple versions |
| 5. Release | WP6 | Go decision; public documentation |

Effort and staffing are to be agreed in discussion.

## 6. Validation and Acceptance Criteria

1. **Version equality.** At every minted version, (base + version delta) equals MaterializationEngine's independently materialized root table for that version exactly. This is checked by a hash aggregate per `id` bucket. Any mismatch is treated as a bug in morsel generation.
2. **Live agreement.** For sampled neurons spanning small to very large, live-layer answers match `live_live_query` at the same timestamp.
3. **Settling.** We measure the disagreement between a provisional watermark and the same watermark recomputed after the settling delay. This also measures how often the live database itself misses late edits, with and without the overlap change.
4. **Performance.** We track morsel latency, morsel and cumulative file sizes, `get_roots` volume per interval, read latency for a few neurons and for thousands of neurons (remote and local), and how quickly the rebase threshold is approached.
5. **Client sync.** A local mirror stays consistent through version mints and a rebase, transferring only added files.

## 7. Assumptions and Dependencies

For the in-scope table class, annotation columns and supervoxel IDs are immutable after detection. `get_delta_roots` and `get_roots` are fast enough to sustain a 5-minute cadence, though the load will be bursty: an edit to a large neuron flags all of its synapses. The LTV test cluster and a Ray/Daft deployment are available for development. Object storage (GCS or S3) supports atomic overwrites and conditional GETs. The live-update workflow runs for every table in scope (it does not skip large tables by default), and `_run_complete_workflow` runs the same update chain immediately before freezing, so the final morsel of each version marks the mint. Adding a changelog insert to the existing write transactions has an acceptable cost on Postgres.

## 8. Decisions for Discussion

| # | Decision | Options | Current leaning |
| --- | --- | --- | --- |
| D1 | Live publishing channel | JSON endpoint; append to Delta every Δ; staging bucket of per-interval Parquet; single cumulative overwritten file | Cumulative file, plus staging morsels as an audit log |
| D2 | Live-update cadence | Hourly (current default); \~15 min; \~5 min | Measure cycle duration and chunkedgraph load; step down from hourly |
| D3 | Use of Delta semantics | Delta time travel (MERGE with deletion vectors); validity intervals per row; cumulative overlays in a manifest | Cumulative overlays; Delta for packaging only |
| D4 | Watermark policy | Overlap length; settling delay; whether live readers see unsettled data; whether the overlap change goes upstream independently | \~10 min overlap upstreamed as a standalone fix; \~1 h settling; live readers may see unsettled data, clearly labelled |
| D5 | Rebase trigger | Fraction of rows changed; fixed interval; aligned with versions | Row fraction (5–10%), aligned with the next version |
| D6 | Table classes | Which tables are treated as static-annotation; tombstone support from day one | Synapse tables first; reserve schema space for tombstones |

## 9. Risks

| Risk | Mitigation |
| --- | --- |
| Chunkedgraph load from bursty `get_roots` calls | No extra load at the current cadence, since morsels reuse calls the live update already makes; increase cadence only as measurements allow |
| Late-visible edits produce wrong states | Overlap window; settled watermarks for minting; settling check in validation |
| Large edits inflate the live file within a day | Tail of recent morsels with hourly folding; size monitoring |
| Reader support for the override merge is inconsistent across engines | Reference reader; SQL view; tests against DuckDB, polars and delta-rs |
| Static assumption violated for some tables | Restrict table class; schema space reserved for annotation changes |

## 10. Acknowledgements

The core approach of splitting static from dynamic data, using change logs over a base, and computing live heads from `get_delta_roots` follows Greg Jefferis's implementation in `flyconnectome/aedes`.