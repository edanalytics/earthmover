import json
import polars as pl
import re

from earthmover.operations.operation import Operation

from typing import Dict, List, Tuple
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from polars import LazyFrame as DataFrame


class GroupByWithRankOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'group_by_columns', 'rank_column',
    )

    GROUPED_COL_NAME = "____grouped_col____"
    GROUPED_COL_SEP = "_____"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.group_by_columns = self.error_handler.assert_get_key(self.config, 'group_by_columns', dtype=list)
        self.rank_column = self.error_handler.assert_get_key(self.config, 'rank_column', dtype=str)

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """

        :return:
        """
        super().execute(data, **kwargs)

        if not set(self.group_by_columns).issubset(data.collect_schema().names()):
            self.error_handler.throw(
                "one or more specified group-by columns not in the dataset"
            )
            raise

        # 0-based position within each group, in row order (equivalent to pandas `cumcount()`).
        data = data.with_columns(
            pl.int_range(0, pl.len()).over(self.group_by_columns).alias(self.rank_column)
        )

        return data


class GroupByOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition',
        'group_by_columns', 'create_columns',
    )

    COLUMN_REQ_AGG_TYPES = [
        "agg", "aggregate",
        "max", "maximum",
        "min", "minimum",
        "sum",
        "mean", "avg",
        "std", "stdev", "stddev",
        "var", "variance"
    ]

    GROUP_SIZE_COL = "__GROUP_SIZE__"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.group_by_columns    = self.error_handler.assert_get_key(self.config, 'group_by_columns', dtype=list)
        self.create_columns_dict = self.error_handler.assert_get_key(self.config, 'create_columns', dtype=dict)

    # Aggregations computed as native Polars numeric reductions (streaming-friendly).
    # Their results are formatted back to strings to match the previous pandas output.
    NUMERIC_AGG_TYPES = (
        "sum", "min", "minimum", "max", "maximum",
        "mean", "avg", "std", "stdev", "stddev", "var", "variance",
    )

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """
        Group-by is implemented with native Polars `group_by().agg(...)` so it streams: the
        reducing aggregations (count/min/max/sum/mean/std/var) hold only per-group state
        rather than materializing every row, which is what makes large `melt -> group_by`
        pipelines fit in memory. (`agg`/`json_array_agg` must collect each group's values by
        nature, but the grouping itself still streams.)

        :return:
        """
        super().execute(data, **kwargs)

        data_columns = data.collect_schema().names()
        if not set(self.group_by_columns).issubset(data_columns):
            self.error_handler.throw(
                "one or more specified group-by columns not in the dataset"
            )
            raise

        agg_exprs = []
        numeric_cols = []           # result columns to re-format as ints/floats like pandas
        list_join_cols = {}         # name -> separator (for `agg`/`aggregate`)
        json_cols = {}              # name -> separator/"str" flag (for `json_array_agg`)

        for new_col_name, func in self.create_columns_dict.items():

            _pieces = re.findall(
                r"([A-Za-z0-9_]*)\(([A-Za-z0-9_]*)?,?(.*)?\)",
                func
            )[0]

            # User can pass in 1, 2, or 3 pieces. We want to default undefined pieces to empty strings.
            _pieces = list(_pieces) + ["", ""]  # Clever logic to simplify unpacking.
            _agg_type, _col, _sep, *_ = _pieces  # Unpack the pieces, adding blanks as necessary.

            #
            if _agg_type in self.COLUMN_REQ_AGG_TYPES:

                if _col == "":
                    self.error_handler.throw(
                        f"aggregation function `{_agg_type}`(column) missing required column"
                    )

                if _col not in data_columns:
                    self.error_handler.throw(
                        f"aggregation function `{_agg_type}`({_col}) refers to a column {_col} which does not exist"
                    )

            expr = self._build_agg_expr(_agg_type, _col, _sep, new_col_name)
            if expr is None:
                self.error_handler.throw(
                    f"invalid aggregation function `{_agg_type}` in `group_by` operation"
                )
                raise

            agg_exprs.append(expr)
            if _agg_type in self.NUMERIC_AGG_TYPES:
                numeric_cols.append(new_col_name)
            elif _agg_type in ("agg", "aggregate"):
                list_join_cols[new_col_name] = _sep
            elif _agg_type == "json_array_agg":
                json_cols[new_col_name] = _sep

        result = data.group_by(self.group_by_columns).agg(agg_exprs)

        # Post-process the (now small) grouped result. `agg`/`json_array_agg` were aggregated
        # into per-group lists above; collapse them to strings here, preserving input order.
        for name, sep in list_join_cols.items():
            result = result.with_columns(pl.col(name).list.join(sep).alias(name))

        for name, sep in json_cols.items():
            result = result.with_columns(
                pl.col(name).map_elements(self._make_json_formatter(sep), return_dtype=pl.Utf8).alias(name)
            )

        # Format numeric results to match pandas: integral values render without a trailing
        # ".0" (e.g. min 34, not 34.0), non-integral as their usual float repr.
        for name in numeric_cols:
            result = result.with_columns(
                pl.col(name).map_elements(self._format_numeric, return_dtype=pl.Utf8).alias(name)
            )

        return result

    def _build_agg_expr(self, agg_type: str, column: str, separator: str, alias: str):
        """Map an aggregation function name to a native Polars aggregation expression."""
        if agg_type in ("count", "size"):
            return pl.len().alias(alias)

        if agg_type in ("agg", "aggregate", "json_array_agg"):
            # Collect each group's values (in input order); collapsed to a string post-agg.
            return pl.col(column).alias(alias)

        if agg_type in ("str_min", "str_minimum"):
            return pl.col(column).min().alias(alias)
        if agg_type in ("str_max", "str_maximum"):
            return pl.col(column).max().alias(alias)

        numeric = pl.col(column).cast(pl.Float64, strict=False)
        if agg_type == "sum":
            return numeric.sum().alias(alias)
        if agg_type in ("min", "minimum"):
            return numeric.min().alias(alias)
        if agg_type in ("max", "maximum"):
            return numeric.max().alias(alias)
        if agg_type in ("mean", "avg"):
            # Matches the prior `to_numeric(col).sum() / len(group)`.
            return (numeric.sum() / pl.len()).alias(alias)
        if agg_type in ("std", "stdev", "stddev"):
            return numeric.std().alias(alias)
        if agg_type in ("var", "variance"):
            return numeric.var().alias(alias)

        return None

    @staticmethod
    def _format_numeric(value):
        """Render a numeric aggregate like pandas did: integral -> int string, else float."""
        if value is None:
            return None
        fvalue = float(value)
        if fvalue.is_integer():
            return str(int(fvalue))
        return str(fvalue)

    @staticmethod
    def _make_json_formatter(separator: str):
        """Build a per-group list -> JSON-array-string formatter matching the prior backend."""
        def _format(values):
            vals = [str(v) for v in values]
            if separator == "str":
                # compact JSON (no spaces), quoted strings: ["1","2"]
                return json.dumps(vals, separators=(",", ":"))
            # unquoted, comma-joined: [1,2]
            return "[" + ",".join(vals) + "]"
        return _format
