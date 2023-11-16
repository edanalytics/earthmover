import polars as pl
import re

from earthmover.nodes.operation import Operation

from typing import Dict, List, Tuple
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from dask.dataframe.core import DataFrame


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
        self.group_by_columns: List[str] = None
        self.create_columns_dict: Dict[str, str] = None

    def compile(self):
        """

        :return:
        """
        super().compile()
        self.group_by_columns    = self.error_handler.assert_get_key(self.config, 'group_by_columns', dtype=list)
        self.create_columns_dict = self.error_handler.assert_get_key(self.config, 'create_columns', dtype=dict)

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """
        Note: There is a bug in Dask Groupby operations.
        Index columns are overwritten by 'index' after index reset.

        :return:
        """
        super().execute(data, **kwargs)

        if not set(self.group_by_columns).issubset(data.columns):
            self.error_handler.throw(
                "one or more specified group-by columns not in the dataset"
            )
            raise

        agg_expressions: List[pl.Expr] = []

        for new_col_name, func_str in self.create_columns_dict.items():
            agg_type, col, sep = self.parse_groupby_string(func_str)

            #
            if agg_type in self.COLUMN_REQ_AGG_TYPES:

                if not col:
                    self.error_handler.throw(
                        f"aggregation function `{agg_type}(column)` missing required column"
                    )

                if col not in data.columns:
                    self.error_handler.throw(
                        f"aggregation function `{agg_type}({col})` refers to a column which does not exist"
                    )

            agg_expr = self.get_agg_expr(agg_type, col, sep)
            if agg_expr is None:
                self.error_handler.throw(
                    f"invalid aggregation function `{agg_type}` in `group_by` operation"
                )

            agg_expressions.append(agg_expr.alias(new_col_name))

        #
        return data.group_by(self.group_by_columns).agg(*agg_expressions)

    @staticmethod
    def parse_groupby_string(func_str: str) -> Tuple[str, str, str]:
        """

        :param func_str:
        :return:
        """
        func_regex: str = "([A-Za-z0-9_]*)\(([A-Za-z0-9_]*)?,?(.*)?\)"

        pieces = re.findall(func_regex, func_str)[0]
        pieces = list(pieces) + ["", ""]  # Clever logic to simplify unpacking.

        # User can pass in 1, 2, or 3 pieces. We want to default undefined pieces to empty strings.
        agg_type, col, sep, *_ = pieces  # Unpack the pieces, adding blanks as necessary.
        return agg_type, col, sep

    @staticmethod
    def get_agg_expr(agg_type: str, column: str = "", separator: str = ""):
        """

        :param agg_type:
        :param column:
        :param separator:
        :return:
        """
        agg_lambda_mapping = {
            'agg'        : pl.concat_str(pl.col(column), separator=separator),
            'aggregate'  : pl.concat_str(pl.col(column), separator=separator),
            'avg'        : pl.col(column).mean(),
            'count'      : pl.all().count(),
            'max'        : pl.col(column).cast(pl.Int32).max(),
            'maximum'    : pl.col(column).cast(pl.Int32).max(),
            'str_max'    : pl.col(column).max(),
            'str_maximum': pl.col(column).max(),
            'mean'       : pl.col(column).mean(),
            'min'        : pl.col(column).cast(pl.Int32).min(),
            'minimum'    : pl.col(column).cast(pl.Int32).min(),
            'str_min'    : pl.col(column).min(),
            'str_minimum': pl.col(column).min(),
            'size'       : pl.all().count(),
            'std'        : pl.col(column).cast(pl.Int32).std(),
            'stdev'      : pl.col(column).cast(pl.Int32).std(),
            'stddev'     : pl.col(column).cast(pl.Int32).std(),
            'sum'        : pl.col(column).cast(pl.Int32).sum(),
            'var'        : pl.col(column).cast(pl.Int32).var(),
            'variance'   : pl.col(column).cast(pl.Int32).var(),
        }
        return agg_lambda_mapping.get(agg_type)
