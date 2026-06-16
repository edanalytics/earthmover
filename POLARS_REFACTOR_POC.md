# Earthmover: Dask → Polars backend (proof of concept)

**Goal.** Replace Earthmover's Dask (+pandas) dataframe backend with **Polars** (lazy
`LazyFrame` + streaming engine) to reduce the peak memory that causes production OOMs —
especially on `melt`. Priority: demonstrate the memory win; behavior is kept as close to
identical as practical (and currently matches).

## TL;DR

- **Correctness:** the bundled suite (`earthmover -t`) passes **byte-for-byte** against the
  committed expected outputs, and **10/10** offline example projects produce output identical
  (sorted) to the Dask backend. (The one nominal diff, `01_simple`, is an intentionally
  *random* Jinja value.)
- **Memory (the `melt` explosion you flagged):** melting a wide table multiplies rows by the
  number of value columns. Polars streams it; pandas/Dask materialize it.

  | Workload (melt 1M rows × 50 value cols → 50M long rows) | Peak RSS |
  |---|---|
  | Polars `unpivot` → stream to disk (2 threads) | **~0.6 GB** |
  | pandas `melt` (materialized — what Dask must do) | **~4.7 GB** |

  And the **slope** is the real story (peak RSS as the wide table grows, ×50 explosion):

  | wide rows | Polars (stream) | pandas/Dask (materialize) |
  |---|---|---|
  | 1M (→50M) | ~0.60 GB | ~4.65 GB |
  | 2M (→100M) | ~0.75 GB | ~9.5 GB |
  | 3M (→150M) | ~0.9 GB (est.) | ~14 GB → **OOM** |

  Polars is ~flat (true streaming); materialization is linear and OOMs by ~3M wide rows on a
  16 GB box.

- **End-to-end under a hard 2 GB memory cap** (full `earthmover run`, melt 1M×50 → 50M rows,
  rendered to JSONL):

  | Backend | Result under 2 GB cap | Peak RSS | Wall | Rows written |
  |---|---|---|---|---|
  | Dask | **OOM-killed (SIGKILL)** | pinned at 2.0 GB | 3:53 (died) | **0** |
  | Polars (2 threads) | **completed** | **0.77 GB** | 17:08 | **50,000,000** |

  Polars runs the exact pipeline that Dask cannot fit in 2 GB, at ~0.77 GB peak. It is slower
  here because the bottleneck is per-row Jinja rendering of 50M rows with threads pinned to 2
  (a memory-conservative setting) — but it *succeeds within the budget*, which is the point.
  With more threads/memory it is also faster than Dask on equivalent work.

## What changed

Backend swap touched 10 files; the rest of Earthmover is engine-agnostic:
`earthmover/__init__.py`, `earthmover.py`, `nodes/{node,source,destination,transformation}.py`,
`operations/{operation,column,row,dataframe,groupby}.py`, plus `util.py` and `requirements.txt`.

Design:
- Every node's `data` is a `pl.LazyFrame`; the pipeline stays lazy until the destination.
- **Destination streams output**: the transformed (pre-render) frame is spilled to a temporary
  Arrow file via `sink_ipc` (no Python UDF → streams with bounded memory), then read back in
  row-batches and Jinja-rendered to the output file. Peak memory is independent of output size.
- **Pandas-semantics operations preserved** by running pandas inside
  `LazyFrame.map_batches(..., streamable=True)` (the 1:1 analog of Dask `map_partitions`):
  `filter_rows` (pandas `.query` syntax), `flatten`, `group_by` agg lambdas. Native Polars is
  used for I/O, joins, unions, `melt`/`unpivot`, sorts, distinct, column ops.
- **Jinja per-row rendering** uses `pl.struct(pl.all()).map_elements(...)`; Polars `null` is
  coerced to NaN so the `{% if value!=value %}` idiom still works.
- CSV/TSV read via streaming `pl.scan_csv(infer_schema_length=0, missing_utf8_is_empty_string=True)`
  (all-string, empties preserved); multi-line headers / non-UTF-8 fall back to pandas.
- `repartition` / `chunksize` are accepted but warn (no-op under Polars). Python ≥3.10; Dask removed.

## Methodology & honesty caveats

- Peak RSS via `/usr/bin/time -v`; hard caps via `systemd-run --user --scope -p MemoryMax=…`
  (verified it OOM-kills at the limit). Host: 16 GB RAM, 20 cores, WSL2.
- **Data is synthetic.** It uses the repo's own perf-test schema (`big_attendance`) and a wide
  assessment-style table — a fair proxy, but **not** your production failure reproduced. Happy
  to re-run on a real OOM-repro config/dataset.
- **Threads matter a lot.** Polars allocates per-thread arenas; its memory *floor* scales with
  core count. On a simple map pipeline at 5M rows, Polars at 20 threads used *more* peak RSS
  than Dask (1.2 GB vs 0.9 GB) while being ~2× faster; at 2 threads the floor dropped to
  ~0.3 GB. The benchmarks above pin Polars to 2 threads (a realistic memory-constrained
  setting). Polars trades memory for speed via parallelism — and that tradeoff is tunable.
- Where Polars currently **materializes** (and so does not yet help memory): `group_by`
  aggregations, `pivot`, and `expect` collect the frame (these reuse pandas for fidelity).
  `map_values` uses `Expr.replace`, which is memory-heavy (~+0.4 GB at 5M rows) — a candidate
  for a lighter idiom. These are follow-ups, not blockers; the headline `melt` path streams.

## Follow-up: streaming `group_by`, native `pivot`, and the melt-chunking investigation

(Separate branch `feature/melt-memory`, off `feature/polars`.)

**The real melt OOM is `melt -> group_by`, not melt alone.** Polars' `unpivot` already
streams melt with bounded memory (≈0.6–1.0 GB whether 50 or 4000 value columns; verified up to
200M output rows). The explosion came from `group_by`, which previously did
`collect().to_pandas()` — materializing the entire post-melt explosion.

Discriminating test (melt 100K×1000 → 100M rows, then `group_by` count):

| group_by implementation | Peak RSS |
|---|---|
| old (`collect().to_pandas()`) | **11.9 GB** |
| **native `pl.group_by().agg()` (streaming)** | **0.46 GB** |

So `group_by` was ported to native Polars `group_by().agg(...)`:
- count/min/max/sum/mean/std/var → native reductions (hold per-group state, stream).
- `agg`/`json_array_agg` → per-group list aggregation, collapsed to strings post-group_by
  (these inherently retain each group's values, but the grouping still streams).
- Numeric results re-formatted to match the prior pandas output (integral → `34`, not `34.0`).
- Verified **byte-identical** to the Dask backend on `earthmover -t` and example projects
  `03_groupby` and `03a_groupby_with_rank` (which exercise min/max/mean/agg/json_array_agg).

**`pivot`** now uses Polars' native Arrow-backed `pivot` (with `sort_columns=True` to match
pandas' sorted output) instead of a `to_pandas().pivot_table()` round-trip. It still
materializes (pivot must see all rows), but in the more efficient engine. Tests pass identically.

**Melt column-chunking: investigated and intentionally NOT shipped.** Chunking the value
columns and `pl.concat`-ing the sub-melts *increased* peak memory (0.8 GB → 2.8 GB → 4 GB as
chunks shrank) because it breaks streaming and re-scans the source. Polars' single `unpivot` is
already the bounded-memory path; chunking can't beat it. (Data on request.)

**Known remaining overhead (follow-up):** on *very wide* sources the row-level "drop all-empty
rows" filter builds an `all_horizontal` predicate over every column, which adds ~1.8 GB on a
1002-column table (full earthmover `melt -> group_by` lands at ~2.3 GB vs the group_by's own
0.45 GB). Worth a lighter formulation, but separate from this change.

## Reproduce

```
# correctness
.venv-polars/bin/earthmover -t
# melt benchmark project lives in _bench_melt/ (wide CSV via /tmp/gen_wide.py)
```
