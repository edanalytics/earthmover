import pandas as pd
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

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """
        Group-by is an inherently whole-frame aggregation. We materialize to pandas (the
        result of an aggregation is typically small) and reuse the established aggregation
        lambdas, which keeps behavior identical to the previous backend. The result is
        returned as a Polars LazyFrame so the rest of the pipeline stays lazy.

        :return:
        """
        super().execute(data, **kwargs)

        if not set(self.group_by_columns).issubset(data.collect_schema().names()):
            self.error_handler.throw(
                "one or more specified group-by columns not in the dataset"
            )
            raise

        pdf = data.collect().to_pandas()
        grouped = pdf.groupby(self.group_by_columns, sort=False)

        result = grouped.size().reset_index()
        result.columns = self.group_by_columns + [self.GROUP_SIZE_COL]

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

                if _col not in pdf.columns:
                    self.error_handler.throw(
                        f"aggregation function `{_agg_type}`({_col}) refers to a column {_col} which does not exist"
                    )

            agg_lambda = self._get_agg_lambda(_agg_type, _col, _sep)
            if not agg_lambda:
                self.error_handler.throw(
                    f"invalid aggregation function `{_agg_type}` in `group_by` operation"
                )

            _computed = grouped.apply(agg_lambda).reset_index()
            _computed.columns = self.group_by_columns + [new_col_name]
            result = result.merge(_computed, how="left", on=self.group_by_columns)

        result = result[result[self.GROUP_SIZE_COL] > 0]
        del result[self.GROUP_SIZE_COL]

        return pl.from_pandas(result).lazy()

    @staticmethod
    def _get_agg_lambda(agg_type: str, column: str = "", separator: str = ""):
        """

        :param agg_type:
        :param column:
        :param separator: usually a string to separate list elements, except in the case of json_array_agg where it specifies a data type
        :return:
        """
        agg_lambda_mapping = {
            'agg'      : lambda x: separator.join(x[column]),
            'aggregate': lambda x: separator.join(x[column]),
            'json_array_agg': lambda x: x[column].to_json(orient="records") if separator == "str" else f"[{','.join(x[column])}]",
            'avg'      : lambda x: pd.to_numeric(x[column]).sum() / max(1, len(x)),
            'count'    : lambda x: len(x),
            'max'      : lambda x: pd.to_numeric(x[column]).max(),
            'maximum'  : lambda x: pd.to_numeric(x[column]).max(),
            'str_max'      : lambda x: x[column].max(),
            'str_maximum'  : lambda x: x[column].max(),
            'mean'     : lambda x: pd.to_numeric(x[column]).sum() / max(1, len(x)),
            'min'      : lambda x: pd.to_numeric(x[column]).min(),
            'minimum'  : lambda x: pd.to_numeric(x[column]).min(),
            'str_min'      : lambda x: x[column].min(),
            'str_minimum'  : lambda x: x[column].min(),
            'size'     : lambda x: len(x),
            'std'      : lambda x: pd.to_numeric(x[column]).std(),
            'stdev'    : lambda x: pd.to_numeric(x[column]).std(),
            'stddev'   : lambda x: pd.to_numeric(x[column]).std(),
            'sum'      : lambda x: pd.to_numeric(x[column]).sum(),
            'var'      : lambda x: pd.to_numeric(x[column]).var(),
            'variance' : lambda x: pd.to_numeric(x[column]).var(),
        }
        return agg_lambda_mapping.get(agg_type)
