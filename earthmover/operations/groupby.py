import pandas as pd
from dask import dataframe as dd
import re
from functools import partial

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
        # data = data.sort_values(by=self.group_by_columns).repartition(partition_size=self.target_partition_size)
        grouped = data.groupby(self.group_by_columns)

        result = grouped.size().reset_index()
        result.columns = self.group_by_columns + [self.GROUP_SIZE_COL]
        result = result.query(f"{self.GROUP_SIZE_COL} > 0")
        if self.earthmover.distributed:
            result = result.repartition(partition_size=self.target_partition_size)
        
        all_new_cols = {}
        for new_col_name, func in self.create_columns_dict.items():

            _pieces = re.findall(
                r"([A-Za-z0-9_]*)\(([A-Za-z0-9_]*)?,?(.*)?\)",
                func
            )[0]

            # User can pass in 1, 2, or 3 pieces. We want to default undefined pieces to empty strings.
            _pieces = list(_pieces) + ["", ""]  # Clever logic to simplify unpacking.
            all_new_cols[new_col_name] = _pieces
        
        # Special handling for a few numeric agg types, for better performance:
        # - count comes for free in `grouped`
        # - avg = sum()/count - so if we already computed sum(), no need to actually compute!
        create_new_cols = {}
        count_new_cols = {}
        avg_new_cols = {}
        for new_col_name, _pieces in all_new_cols.items():
            _agg_type, _col, _sep, *_ = _pieces  # Unpack the pieces, adding blanks as necessary.
            
            if _agg_type in ["count", "size"]:
                count_new_cols[new_col_name] = _pieces
                continue
            elif _agg_type in ["mean", "avg"]:
                sum_on_same_col = [
                    x for x in all_new_cols.keys() if all_new_cols[x][0]=="sum" and all_new_cols[x][1]==_col
                ]
                if len(sum_on_same_col)>0:
                    avg_new_cols[new_col_name] = _pieces
                    continue
            create_new_cols[new_col_name] = _pieces
        
        # now compute the columns needed:
        for new_col_name, _pieces in create_new_cols.items():
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
                
            agg_partial = self._get_agg_partial(_agg_type, _col, _sep)
            
            if not agg_partial:
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

            _computed = grouped.apply(agg_partial, meta=meta)
            _computed = _computed.reset_index()
            if self.earthmover.distributed:
                _computed = _computed.repartition(partition_size=self.target_partition_size)
            result = result.merge(_computed, how="left", on=self.group_by_columns)

        for new_col_name, _pieces in count_new_cols.items():
            result[new_col_name] = result[self.GROUP_SIZE_COL]
        
        for new_col_name, _pieces in avg_new_cols.items():
            _agg_type, _col, _sep, *_ = _pieces  # Unpack the pieces, adding blanks as necessary.
            sum_col = [
                x for x in all_new_cols.keys() if all_new_cols[x][0]=="sum" and all_new_cols[x][1]==_col
            ][0]
            result[new_col_name] = result[sum_col] / result[self.GROUP_SIZE_COL]

        # is this final repartition needed?    
        # if self.earthmover.distributed:
        #     result = result.repartition(partition_size=self.target_partition_size)

        del result[self.GROUP_SIZE_COL]
        return result

    @staticmethod
    def _get_agg_partial(agg_type: str, column: str = "", separator: str = ""):
        """

        :param agg_type:
        :param column:
        :param separator: usually a string to separate list elements, except in the case of json_array_agg where it specifies a data type
        :return:
        """
        agg_partial_mapping = {
            'agg'      : partial(GroupByOperation.aggregate, column, separator),
            'aggregate': partial(GroupByOperation.aggregate, column, separator),
            'json_array_agg': partial(GroupByOperation.json_array_agg, column, separator),
            'avg'      : partial(GroupByOperation.avg, column),
            'count'    : partial(len),
            'max'      : partial(GroupByOperation.maximum, column),
            'maximum'  : partial(GroupByOperation.maximum, column),
            'str_max'      : partial(GroupByOperation.str_maximum, column),
            'str_maximum'  : partial(GroupByOperation.str_maximum, column),
            'mean'     : partial(GroupByOperation.avg, column),
            'min'      : partial(GroupByOperation.minimum, column),
            'minimum'  : partial(GroupByOperation.minimum, column),
            'str_min'      : partial(GroupByOperation.str_minimum, column),
            'str_minimum'  : partial(GroupByOperation.str_minimum, column),
            'size'     : partial(len),
            'std'      : partial(GroupByOperation.stddev, column),
            'stdev'    : partial(GroupByOperation.stddev, column),
            'stddev'   : partial(GroupByOperation.stddev, column),
            'sum'      : partial(GroupByOperation.sum, column),
            'var'      : partial(GroupByOperation.variance, column),
            'variance' : partial(GroupByOperation.variance, column),
        }
        return agg_partial_mapping.get(agg_type)
    
    @staticmethod
    def aggregate(column, separator, x):
        return separator.join(x[column])
    
    @staticmethod
    def json_array_agg(column, separator, x):
        return x[column].to_json(orient="records") if separator == "str" else f"[{','.join(x[column])}]"
    
    @staticmethod
    def sum(column, x):
        return pd.to_numeric(x[column]).sum()
    
    @staticmethod
    def avg(column, x):
        return pd.to_numeric(x[column]).sum() / max(1, len(x))
    
    @staticmethod
    def minimum(column, x):
        return pd.to_numeric(x[column]).min()
    
    @staticmethod
    def maximum(column, x):
        return pd.to_numeric(x[column]).max()
    
    @staticmethod
    def str_minimum(column, x):
        return x[column].min()
    
    @staticmethod
    def str_maximum(column, x):
        return x[column].max()
    
    @staticmethod
    def stddev(column, x):
        return pd.to_numeric(x[column]).std()
    
    @staticmethod
    def variance(column, x):
        return pd.to_numeric(x[column]).var()