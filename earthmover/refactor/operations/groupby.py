import dask.dataframe as dd
import pandas as pd
import re

from earthmover.refactor.operations.operation import Operation


class GroupByWithCountOperation(Operation):
    """

    """
    GROUPED_COL_NAME = "____grouped_col____"
    GROUPED_COL_SEP = "_____"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.group_by_columns = None
        self.count_column = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, 'group_by_columns', list)
        self.group_by_columns = self.config['group_by_columns']

        self.error_handler.assert_key_exists_and_type_is(self.config, 'count_column', str)
        self.count_column = self.config['count_column']


    def verify(self):
        """

        :return:
        """
        super().verify()

        if not set(self.group_by_columns).issubset(self.data.columns):
            self.error_handler.throw(
                "one or more specified group-by columns not in the dataset"
            )
            raise


    def execute(self):
        """

        :return:
        """
        super().execute()

        self.data[self.GROUPED_COL_NAME] = self.data.apply(
            lambda x: self.GROUPED_COL_SEP.join([*self.group_by_columns])
        , axis=1, meta='str')

        self.data = (
            self.data
                .groupby(self.GROUPED_COL_NAME, sort=False)
                .size()
                .reset_index()
        )

        self.data[self.group_by_columns] = self.data[self.GROUPED_COL_NAME].str.split(
            self.GROUPED_COL_SEP, n=len(self.group_by_columns), expand=True
        )
        del self.data[self.GROUPED_COL_NAME]

        return self.data



class GroupByWithAggOperation(Operation):
    """

    """
    DEFAULT_AGG_SEP = ","

    GROUPED_COL_NAME = "____grouped_col____"
    GROUPED_COL_SEP = "_____"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.group_by_columns = None
        self.agg_column = None
        self.separator = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, 'group_by_columns', list)
        self.group_by_columns = self.config['group_by_columns']

        self.error_handler.assert_key_exists_and_type_is(self.config, 'agg_column', str)
        self.agg_column = self.config['agg_column']

        if 'separator' in self.config:
            self.error_handler.assert_key_exists_and_type_is(self.config, 'separator', str)
            self.separator = self.config['separator']
        else:
            self.separator = self.DEFAULT_AGG_SEP

    def verify(self):
        """

        :return:
        """
        super().verify()

        if not set(self.group_by_columns).issubset(self.data.columns):
            self.error_handler.throw(
                "one or more specified group-by columns not in the dataset"
            )
            raise


    def execute(self):
        """

        :return:
        """
        super().execute()

        self.data[self.GROUPED_COL_NAME] = self.data.apply(
            lambda x: self.GROUPED_COL_SEP.join([*self.group_by_columns])
            , axis=1, meta='str')

        _grouped = self.data.groupby(self.GROUPED_COL_NAME, sort=False)
        _grouped = _grouped[[self.agg_column]].agg(self.separator.join)

        self.data = _grouped.reset_index()

        self.data[self.group_by_columns] = self.data[self.GROUPED_COL_NAME].str.split(
            self.GROUPED_COL_SEP, n=len(self.group_by_columns), expand=True
        )
        del self.data[self.GROUPED_COL_NAME]

        return self.data



class GroupByOperation(Operation):
    """

    """
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

        self.group_by_columns = None
        self.create_columns_dict = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, 'group_by_columns', list)
        self.group_by_columns = self.config['group_by_columns']

        self.error_handler.assert_key_exists_and_type_is(self.config, "create_columns", dict)
        self.create_columns_dict = self.config['create_columns']


    def verify(self):
        """

        :return:
        """
        super().verify()

        if not set(self.group_by_columns).issubset(self.data.columns):
            self.error_handler.throw(
                "one or more specified group-by columns not in the dataset"
            )
            raise


    def execute(self):
        """
        Note: There is a bug in Dask Groupby operations.
        Index columns are overwritten by 'index' after index reset.
        https://stackoverflow.com/questions/62323279/how-do-keep-dask-dataframe-after-groupby-compute
        https://stackoverflow.com/questions/73604953/dask-groupby-columns-are-not-there-even-after-reset-index


        :return:
        """
        super().execute()

        #
        _grouped = self.data.groupby(self.group_by_columns)

        result = _grouped.size().reset_index()
        result.columns = self.group_by_columns + [self.GROUP_SIZE_COL]

        for new_col_name, func in self.create_columns_dict.items():
            if new_col_name == "__line__":
                continue

            _pieces = re.findall(
                "([A-Za-z0-9_]*)\(([A-Za-z0-9_]*)?,?(.*)?\)",
                func
            )[0]

            _pieces = list(_pieces) + ["", ""]  # Clever logic to simplify unpacking.
            _agg_type, _col, _sep, *_ = _pieces  # Unpack the pieces, adding blanks as necessary.
            # _agg_type, _col, _sep, *_ = _pieces + ["", ""]  # Unpack the pieces, adding blanks as necessary.

            #
            if _agg_type in self.COLUMN_REQ_AGG_TYPES:

                if _col == "":
                    self.error_handler.throw(
                        f"aggregation function `{_agg_type}`(column) missing required column"
                    )

                if _col not in result.columns:
                    self.error_handler.throw(
                        f"aggregation function `{_agg_type}`({_col}) refers to a column {_col} which does not exist"
                    )

            if not (agg_lambda := self._get_agg_lambda(_agg_type, _col, _sep)):
                self.error_handler.throw(
                    f"invalid aggregation function `{_agg_type}` in `group_by` operation"
                )

            # Groupbys require the index be defined, at least in structure.
            meta = pd.Series(
                dtype='object',
                name=new_col_name,
                index=pd.MultiIndex.from_tuples(
                    tuples=[(None,) * len(self.group_by_columns)],
                    names=self.group_by_columns
                )
            )

            #
            _computed = (
                _grouped
                    .apply(agg_lambda, meta=meta)
                    .reset_index()
            )

            result = result.merge(_computed, how="left", on=self.group_by_columns)

        self.data = result.query(f"{self.GROUP_SIZE_COL} > 0")
        del self.data[self.GROUP_SIZE_COL]

        return self.data


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
            'avg'      : lambda x: dd.to_numeric(x[column]).sum() / max(1, len(x)),
            'count'    : lambda x: len(x),
            'max'      : lambda x: dd.to_numeric(x[column]).max(),
            'maximum'  : lambda x: dd.to_numeric(x[column]).max(),
            'mean'     : lambda x: dd.to_numeric(x[column]).sum() / max(1, len(x)),
            'min'      : lambda x: dd.to_numeric(x[column]).min(),
            'minimum'  : lambda x: dd.to_numeric(x[column]).min(),
            'size'     : lambda x: len(x),
            'std'      : lambda x: dd.to_numeric(x[column]).std(),
            'stdev'    : lambda x: dd.to_numeric(x[column]).std(),
            'stddev'   : lambda x: dd.to_numeric(x[column]).std(),
            'sum'      : lambda x: dd.to_numeric(x[column]).sum(),
            'var'      : lambda x: dd.to_numeric(x[column]).var(),
            'variance' : lambda x: dd.to_numeric(x[column]).var(),
        }
        return agg_lambda_mapping.get(agg_type)
