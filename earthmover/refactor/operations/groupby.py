import abc
import re

import pandas as pd

from earthmover.refactor.operations.operation import Operation


class GenericGroupByOperation(Operation):
    """

    """
    GROUPED_COL_NAME = "____grouped_col____"
    GROUPED_COL_SEP  = "_____"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.source = None
        self.group_by_columns = None


    @abc.abstractmethod
    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, 'source', str)
        self.source = self.config['source']

        self.error_handler.assert_key_exists_and_type_is(self.config, 'group_by_columns', list)
        self.group_by_columns = self.config['group_by_columns']

        pass


    @abc.abstractmethod
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

        pass


    @abc.abstractmethod
    def execute(self):
        """

        :return:
        """
        super().execute()

        self.data = self.get_source_node(self.source).data
        self.verify()
        pass



    def _group_by(self, data):
        """

        :param data:
        :param cols:
        :return:
        """
        data[self.GROUPED_COL_NAME] = data.apply(
            lambda x: self.GROUPED_COL_SEP.join([*self.group_by_columns])
        , axis=1, meta='str')

        return data.groupby(self.GROUPED_COL_NAME, sort=False)


    def _revert_group_by(self, data):
        """

        :param data:
        :return:
        """
        data = data.reset_index()

        data[self.group_by_columns] = data[self.GROUPED_COL_NAME].str.split(
            self.GROUPED_COL_SEP, n=len(self.group_by_columns), expand=True
        )
        del data[self.GROUPED_COL_NAME]

        return data



class GroupByWithCountOperation(GenericGroupByOperation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.count_column = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, 'count_column', str)
        self.count_column = self.config['count_column']


    def execute(self):
        """

        :return:
        """
        super().execute()

        _grouped = self._group_by(self.data)
        _grouped = _grouped.size()
        return self._revert_group_by(_grouped)



class GroupByWithAggOperation(GenericGroupByOperation):
    """

    """
    DEFAULT_AGG_SEP = ","

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.agg_column = None
        self.separator = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, 'agg_column', str)
        self.agg_column = self.config['agg_column']

        if _separator := self.config.get('separator'):
            self.error_handler.assert_key_exists_and_type_is(self.config, 'separator', str)
            self.separator = _separator
        else:
            self.separator = self.DEFAULT_AGG_SEP


    def execute(self):
        """

        :return:
        """
        super().execute()

        _grouped = self._group_by(self.data)
        _grouped = _grouped[[self.agg_column]].agg(self.separator.join)
        return self._revert_group_by(_grouped)



class GroupByOperation(GenericGroupByOperation):
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

        self.create_columns_dict = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, "create_columns", dict)
        self.create_columns_dict = self.config['create_columns']


    def verify(self):
        """

        :return:
        """
        super().verify()
        pass


    def execute(self):
        """

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

            #
            _computed = (
                _grouped
                    .apply(agg_lambda)
                    .to_frame(new_col_name)
                    .reset_index()
            )
            result = result.merge(_computed, how="left", on=self.group_by_columns)

        result = result.query(f"{self.GROUP_SIZE_COL} > 0")
        del result[self.GROUP_SIZE_COL]

        return result


    @staticmethod
    def _get_agg_lambda(agg_type: str, column: str = "", separator: str = ""):
        """

        :param agg_type:
        :param column:
        :param separator:
        :return:
        """
        AGG_LAMBDA_MAPPING = {
            'agg'      : lambda x: separator.join(x[column]),
            'aggregate': lambda x: separator.join(x[column]),
            'avg'      : lambda x: pd.to_numeric(x[column]).sum() / max(1, len(x)),
            'count'    : lambda x: len(x),
            'max'      : lambda x: pd.to_numeric(x[column]).max(),
            'maximum'  : lambda x: pd.to_numeric(x[column]).max(),
            'mean'     : lambda x: pd.to_numeric(x[column]).sum() / max(1, len(x)),
            'min'      : lambda x: pd.to_numeric(x[column]).min(),
            'minimum'  : lambda x: pd.to_numeric(x[column]).min(),
            'size'     : lambda x: len(x),
            'std'      : lambda x: pd.to_numeric(x[column]).std(),
            'stdev'    : lambda x: pd.to_numeric(x[column]).std(),
            'stddev'   : lambda x: pd.to_numeric(x[column]).std(),
            'sum'      : lambda x: pd.to_numeric(x[column]).sum(),
            'var'      : lambda x: pd.to_numeric(x[column]).var(),
            'variance' : lambda x: pd.to_numeric(x[column]).var(),
        }
        return AGG_LAMBDA_MAPPING.get(agg_type)
