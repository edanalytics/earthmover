# Earthmover uses Polars as its dataframe backend.
#
# Historically the backend was Dask (+ pandas); it was swapped to Polars to
# reduce peak memory usage and OOM errors in production by leaning on Polars'
# Arrow-backed columnar storage and its lazy/streaming execution engine.
#
# A small amount of pandas is still used internally for row-wise Jinja
# rendering and a few operations whose pandas semantics we preserve exactly
# (e.g. `filter_rows`' query syntax); those run inside bounded, streaming
# `LazyFrame.map_batches` calls.

import polars as pl

# Be permissive about Jinja-produced/empty values: we never want Polars to
# raise on a column that mixes types during a Python UDF round-trip.
pl.Config.set_fmt_str_lengths(1000)
