import dask.dataframe as dd
import functools
import pandas as pd
import re

from earthmover.operations.operation import Operation

from typing import Dict, List, Tuple
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from dask.dataframe.core import DataFrame


# ---------------------------------------------------------------------------
# Module-level aggregation helpers for custom (non-native) agg types.
# These must be top-level so they are picklable for Dask distributed workers.
# ---------------------------------------------------------------------------

def _agg_join(x, column, separator):
    return separator.join(x[column])

def _agg_json_array_agg(x, column, separator):
    if separator == "str":
        return x[column].to_json(orient="records")
    return f"[{','.join(x[column])}]"

def _agg_count(x):
    return len(x)


# ---------------------------------------------------------------------------
# Aggregation type classification
# ---------------------------------------------------------------------------

# These map earthmover agg-type names to their Dask/pandas equivalents.
# All of these can be handled by a single grouped.agg() call.
_NATIVE_AGG_MAP = {
    'sum'      : 'sum',
    'avg'      : 'mean',
    'mean'     : 'mean',
    'max'      : 'max',
    'maximum'  : 'max',
    'min'      : 'min',
    'minimum'  : 'min',
    'str_max'      : 'max',
    'str_maximum'  : 'max',
    'str_min'      : 'min',
    'str_minimum'  : 'min',
    'std'      : 'std',
    'stdev'    : 'std',
    'stddev'   : 'std',
    'var'      : 'var',
    'variance' : 'var',
}

# These aggregation types require the source column to be numeric first.
_NUMERIC_CONV_TYPES = {
    'sum', 'avg', 'mean',
    'max', 'maximum',
    'min', 'minimum',
    'std', 'stdev', 'stddev',
    'var', 'variance',
}


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

        if not set(self.group_by_columns).issubset(data.columns):
            self.error_handler.throw(
                "one or more specified group-by columns not in the dataset"
            )
            raise

        data[self.rank_column] = data.groupby(self.group_by_columns).cumcount().reset_index(drop=True)

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

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """
        Aggregate using a single grouped.agg() for all native aggregation types
        (sum, mean, count, min, max, std, var) so that Dask performs only ONE
        shuffle instead of one per output column.  Custom aggregations that
        cannot be expressed natively still use grouped.apply() as a fallback.
        """
        super().execute(data, **kwargs)

        if not set(self.group_by_columns).issubset(data.columns):
            self.error_handler.throw(
                "one or more specified group-by columns not in the dataset"
            )
            raise

        # ── 1. Parse every create_columns entry ──────────────────────────────
        parsed = {}  # new_col -> (_agg_type, _col, _sep)
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
                if not _col:
                    self.error_handler.throw(
                        f"aggregation function `{_agg_type}`(column) missing required column"
                    )

                if _col not in data.columns:
                    self.error_handler.throw(
                        f"aggregation function `{_agg_type}`({_col}) refers to column `{_col}` which does not exist"
                    )

            parsed[new_col_name] = (_agg_type, _col, _sep)

        # ── 2. Classify into native / count / custom ──────────────────────────
        # native   – handled by a single grouped.agg() call (optimal)
        # count    – handled by grouped.size() (no source column needed)
        # custom   – fallback to grouped.apply() (one shuffle per column)
        native = {}      # new_col -> (src_col, dask_fn_str)
        count_cols = []  # new_col names for count()/size() without a column
        custom = {}      # new_col -> (_agg_type, _col, _sep)

        for new_col, (_agg_type, _col, _sep) in parsed.items():
            if _agg_type in ('count', 'size') and not _col:
                count_cols.append(new_col)
            elif _agg_type in _NATIVE_AGG_MAP:
                native[new_col] = (_col, _NATIVE_AGG_MAP[_agg_type])
            else:
                custom[new_col] = (_agg_type, _col, _sep)

        grouped = data.groupby(self.group_by_columns)

        result = None

        # ── 4. Single .agg() call for all native aggregations ─────────────────
        #
        #  Strategy: give each (new_col, src_col, fn) its own uniquely-named
        #  temporary column in data before calling .agg().  This guarantees:
        #    • Flat column names in the result (no MultiIndex headaches).
        #    • Numeric conversion is applied ONLY to the temp copy, never to
        #      the original source column (which may be used by custom aggs
        #      that require string values, e.g. agg/aggregate).
        if native:
            temp_col_map = {}    # new_col -> temp_col_name
            agg_dict_flat = {}   # temp_col_name -> dask_fn_str

            for new_col, (src_col, dask_fn) in native.items():
                temp_col = f'__agg_{new_col}__'
                # Apply numeric conversion on the temp copy when needed,
                # leaving the original column untouched.
                if dask_fn in ('sum', 'mean', 'std', 'var') or (
                    dask_fn in ('max', 'min') and
                    parsed[new_col][0] in _NUMERIC_CONV_TYPES
                ):
                    data[temp_col] = dd.to_numeric(data[src_col], errors='coerce')
                else:
                    data[temp_col] = data[src_col]
                agg_dict_flat[temp_col] = dask_fn
                temp_col_map[new_col] = temp_col

            # ── ONE shuffle ──
            native_result = data.groupby(self.group_by_columns).agg(agg_dict_flat).reset_index()

            # Rename temp columns to user-specified new column names.
            rename_dict = {tmp: new for new, tmp in temp_col_map.items()}
            native_result = native_result.rename(columns=rename_dict)
            result = native_result

        # ── 5. Add count() / size() columns ───────────────────────────────────
        if count_cols:
            size_result = grouped.size().reset_index()
            size_result.columns = self.group_by_columns + ['__count__']
            # Rename the size column to the first count new_col; duplicate for any others.
            size_result = size_result.rename(columns={'__count__': count_cols[0]})
            for extra_col in count_cols[1:]:
                size_result[extra_col] = size_result[count_cols[0]]

            if result is None:
                result = size_result
            else:
                result = result.merge(size_result, on=self.group_by_columns, how='left')

        # ── 6. Custom aggs: fallback to grouped.apply() ───────────────────────
        if custom:
            # If we have no base result yet, seed it from grouped.size()
            if result is None:
                result = grouped.size().reset_index()
                result.columns = self.group_by_columns + [self.GROUP_SIZE_COL]

            for new_col, (_agg_type, _col, _sep) in custom.items():
                agg_fn = self._get_agg_fn(_agg_type, _col, _sep)
                if not agg_fn:
                    self.error_handler.throw(
                        f"invalid aggregation function `{_agg_type}` in `group_by` operation"
                    )

                meta = pd.Series(
                    dtype='object',
                    name=new_col,
                    index=pd.MultiIndex.from_tuples(
                        tuples=[(None,) * len(self.group_by_columns)],
                        names=self.group_by_columns
                    )
                )
                _computed = grouped.apply(agg_fn, meta=meta).reset_index()
                result = result.merge(_computed, how='left', on=self.group_by_columns)

            # Drop the seed size column if it was only added as a base
            if self.GROUP_SIZE_COL in result.columns:
                result = result.query(f"{self.GROUP_SIZE_COL} > 0")
                del result[self.GROUP_SIZE_COL]

        return result

    @staticmethod
    def _get_agg_fn(agg_type: str, column: str = "", separator: str = ""):
        """
        Return a picklable callable for the given *custom* aggregation type.
        Native types (sum, mean, count, min, max, std, var) are handled
        separately via grouped.agg() and should not reach this method.
        """
        agg_fn_mapping = {
            'agg'           : functools.partial(_agg_join, column=column, separator=separator),
            'aggregate'     : functools.partial(_agg_join, column=column, separator=separator),
            'json_array_agg': functools.partial(_agg_json_array_agg, column=column, separator=separator),
            # count is handled upstream; include here as a safe fallback
            'count'         : _agg_count,
            'size'          : _agg_count,
        }
        return agg_fn_mapping.get(agg_type)