import pandas as pd
import re

from earthmover.operations.operation import Operation

from typing import Dict, List, Tuple
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from dask.dataframe.core import DataFrame


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

        #
        grouped = data.groupby(self.group_by_columns)

        result = grouped.size().reset_index()
        result.columns = self.group_by_columns + [self.GROUP_SIZE_COL]

        for new_col_name, func in self.create_columns_dict.items():

            _pieces = re.findall(
                "([A-Za-z0-9_]*)\(([A-Za-z0-9_]*)?,?(.*)?\)",
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

                if _col not in data.columns:
                    self.error_handler.throw(
                        f"aggregation function `{_agg_type}`({_col}) refers to a column {_col} which does not exist"
                    )

            agg_lambda = self._get_agg_lambda(_agg_type, _col, _sep)
            if not agg_lambda:
                self.error_handler.throw(
                    f"invalid aggregation function `{_agg_type}` in `group_by` operation"
                )

            #
            # ddf.apply() requires the index be defined, at least in structure.
            meta = pd.Series(
                dtype='object',
                name=new_col_name,
                index=pd.MultiIndex.from_tuples(
                    tuples=[(None,) * len(self.group_by_columns)],
                    names=self.group_by_columns
                )
            )

            _computed = grouped.apply(agg_lambda, meta=meta).reset_index()
            result = result.merge(_computed, how="left", on=self.group_by_columns)

        data = result.query(f"{self.GROUP_SIZE_COL} > 0")
        del data[self.GROUP_SIZE_COL]

        return data

    @staticmethod
    def _get_agg_lambda(agg_type: str, column: str = "", separator: str = ""):
        """

        :param agg_type:
        :param column:
        :param separator:
        :return:
        """
        agg_lambda_mapping = {
            'agg'      : lambda x: separator.join(x[column]),
            'aggregate': lambda x: separator.join(x[column]),
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

