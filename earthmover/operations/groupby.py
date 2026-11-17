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

def _agg_std(x, column):
    return pd.to_numeric(x[column]).std()

def _agg_var(x, column):
    return pd.to_numeric(x[column]).var()

def _apply_custom_aggs(group, agg_fns):
    """Compute every custom aggregation for one group, so all of them share a single grouped.apply() (one shuffle)."""
    return pd.Series({new_col: agg_fn(group) for new_col, agg_fn in agg_fns.items()}, dtype='object')


# ---------------------------------------------------------------------------
# Aggregation type classification
# ---------------------------------------------------------------------------

# These map earthmover agg-type names to their Dask/pandas equivalents.
# All of these can be handled by a single grouped.agg() call.
# (std/var are custom aggs instead: Dask's grouped std/var loses precision on large values,
#  e.g. var of [1e9+1, 1e9+2, 1e9+3, 1e9+4] is 0.0 in Dask but 1.25 in pandas.)
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
}

# These aggregation types require the source column to be numeric first.
_NUMERIC_CONV_TYPES = {
    'sum', 'avg', 'mean',
    'max', 'maximum',
    'min', 'minimum',
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
        Aggregate in two passes, regardless of how many columns are created:
          1. one grouped.agg() for native types (sum, avg, count, min, max)
          2. one grouped.apply() for custom types (agg, json_array_agg, std, var)
        (Previously each output column was its own grouped.apply(), i.e. one shuffle per column.)
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
        native = {}      # new_col -> (_agg_type, _col)
        count_cols = []  # new_col names for count()/size(): copies of the group size
        custom = {}      # new_col -> picklable agg function

        for new_col, (_agg_type, _col, _sep) in parsed.items():
            if _agg_type in _NATIVE_AGG_MAP:
                native[new_col] = (_agg_type, _col)
            elif _agg_type in ('count', 'size'):
                count_cols.append(new_col)
            else:
                agg_fn = self._get_agg_fn(_agg_type, _col, _sep)
                if not agg_fn:
                    self.error_handler.throw(
                        f"invalid aggregation function `{_agg_type}` in `group_by` operation"
                    )
                custom[new_col] = agg_fn

        # ── 3. One grouped.agg() for group size + all native aggregations ────
        # Each native aggregation gets its own temp column. This keeps result names flat, and
        # numeric conversion never touches the source column (custom aggs need its strings).
        agg_data = data.assign(**{self.GROUP_SIZE_COL: 1})
        agg_spec = {self.GROUP_SIZE_COL: 'size'}  # temp col -> Dask agg function
        temp_cols = {}                            # new_col -> temp col
        avg_cols = []                             # new cols computed as sum / group size

        for new_col, (_agg_type, _col) in native.items():
            temp_col = f'__agg_{new_col}__'
            dask_fn = _NATIVE_AGG_MAP[_agg_type]

            # avg divides by ALL rows in the group (blanks included), e.g. avg('2', '', '4') = 2.0
            if dask_fn == 'mean':
                dask_fn = 'sum'
                avg_cols.append(new_col)

            # Default errors='raise': blanks become NaN, other non-numeric values raise an error.
            if _agg_type in _NUMERIC_CONV_TYPES:
                agg_data = agg_data.assign(**{temp_col: dd.to_numeric(agg_data[_col])})
            else:
                agg_data = agg_data.assign(**{temp_col: agg_data[_col]})

            agg_spec[temp_col] = dask_fn
            temp_cols[new_col] = temp_col

        result = agg_data.groupby(self.group_by_columns).agg(agg_spec).reset_index()

        for new_col in avg_cols:
            result[temp_cols[new_col]] = result[temp_cols[new_col]] / result[self.GROUP_SIZE_COL]

        for new_col in count_cols:
            result[new_col] = result[self.GROUP_SIZE_COL]

        result = result.rename(columns={temp_col: new_col for new_col, temp_col in temp_cols.items()})

        # ── 4. One grouped.apply() for all custom aggregations ────────────────
        if custom:
            meta = pd.DataFrame(
                {new_col: pd.Series(dtype='object') for new_col in custom},
                index=pd.MultiIndex.from_tuples(
                    tuples=[(None,) * len(self.group_by_columns)],
                    names=self.group_by_columns
                )
            ).iloc[:0]
            custom_result = (data
                .groupby(self.group_by_columns)
                .apply(functools.partial(_apply_custom_aggs, agg_fns=custom), meta=meta)
                .reset_index()
            )
            result = result.merge(custom_result, how='left', on=self.group_by_columns)

        # Output columns in `create_columns` order (also drops the group-size column).
        return result[self.group_by_columns + list(self.create_columns_dict.keys())]

    @staticmethod
    def _get_agg_fn(agg_type: str, column: str = "", separator: str = ""):
        """
        Return a picklable callable for the given *custom* aggregation type.
        Native types (sum, avg, count, min, max) are handled by grouped.agg() instead.
        """
        agg_fn_mapping = {
            'agg'           : functools.partial(_agg_join, column=column, separator=separator),
            'aggregate'     : functools.partial(_agg_join, column=column, separator=separator),
            'json_array_agg': functools.partial(_agg_json_array_agg, column=column, separator=separator),
            'std'           : functools.partial(_agg_std, column=column),
            'stdev'         : functools.partial(_agg_std, column=column),
            'stddev'        : functools.partial(_agg_std, column=column),
            'var'           : functools.partial(_agg_var, column=column),
            'variance'      : functools.partial(_agg_var, column=column),
        }
        return agg_fn_mapping.get(agg_type)