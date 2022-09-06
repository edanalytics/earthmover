import abc

import pandas as pd

from .operation import Operation


class GenericDataFrameOperation(Operation):
    """
    This class has alterative behavior to other operations.
    More than one source is always required, so `source_list` and `data_list` are used.
    Do NOT super() up to Operation.

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.source_list = None
        self.data_list = None


    @abc.abstractmethod
    def compile(self):
        """

        :return:
        """
        self.error_handler.assert_key_exists_and_type_is(self.config, 'sources', list)
        self.source_list = self.config['sources']
        pass

    @abc.abstractmethod
    def verify(self):
        """

        :return:
        """
        pass


    @abc.abstractmethod
    def execute(self):
        """

        :return:
        """
        self.data_list = self.source_list = [
            self.get_source_node(source).data for source in self.source_list
        ]
        pass



class JoinOperation(GenericDataFrameOperation):
    """

    """
    JOIN_TYPES = ["inner", "left", "right", "outer"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.join_type = None

        self.left_data = None
        self.left_cols = None
        self.left_keys = None

        self.right_data = None
        self.right_cols = None
        self.right_keys = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        # Check left keys
        if 'left_key' not in self.config and 'left_keys' not in self.config:
            self.error_handler.throw("must define `left_key` or `left_keys`")
            raise

        if _key := self.config.get('left_key'):
            self.error_handler.assert_key_type_is(self.config, 'left_key', 'str')
            self.left_keys = [_key]

        elif _keys := self.config.get('left_keys'):
            self.error_handler.assert_key_type_is(self.config, 'left_keys', 'str')
            self.left_keys = _keys
        else:
            self.error_handler.throw(
                "`left_key(s)` incorrectly specified (should be string or list of strings)"
            )
            raise


        # Check right keys
        if 'right_key' not in self.config and 'right_keys' not in self.config:
            self.error_handler.throw("must define `right_key` or `right_keys`")
            raise

        if _key := self.config.get('right_key'):
            self.error_handler.assert_key_type_is(self.config, 'right_key', 'str')
            self.right_keys = [_key]

        elif _keys := self.config.get('right_keys'):
            self.error_handler.assert_key_type_is(self.config, 'right_keys', 'str')
            self.right_keys = _keys
        else:
            self.error_handler.throw(
                "`right_key(s)` incorrectly specified (should be string or list of strings)"
            )
            raise


        # Check join type
        self.join_type = self.config.get('join_type')
        if not self.join_type:
            self.error_handler.throw("must define `join_type`")
            raise

        if self.join_type not in self.JOIN_TYPES:
            self.error_handler.throw(
                f"`join_type` must be one of [inner, left, right, outer], not `{self.join_type}`"
            )
            raise


        # Collect left and right datasets
        if len(self.source_list) != 2:
            self.error_handler.throw(
                f"`sources` must define exactly two sources to join"
            )
            raise

        self.left_data  = self.source_list[0].data
        self.right_data = self.source_list[1].data


        # Collect left columns to keep
        self.left_cols = self.left_data.columns

        if _keep_cols := self.config.get('left_keep_columns'):
            self.error_handler.assert_key_type_is(self.config, 'left_keep_columns', list)
            self.left_cols = _keep_cols

        elif _drop_cols := self.config.get('left_drop_columns'):
            self.error_handler.assert_key_type_is(self.config, 'left_drop_columns', list)

            # if any(self.left_keys.map(__contains__, _drop_cols)):
            if any(col in self.left_keys for col in _drop_cols):
                self.error_handler.throw(
                    "you may not `left_drop_columns` that are part of the `left_key(s)`"
                )
                raise
            
            else:
                self.left_cols = list(set(self.left_data.columns) - set(_drop_cols))

        else:
            self.error_handler.throw(
                "`left_keep_columns` or `left_drop_columns` incorrectly specified (should be list of strings)"
            )
            raise


        # Collect right columns to keep
        self.right_cols = self.right_data.columns

        if _keep_cols := self.config.get('right_keep_columns'):
            self.error_handler.assert_key_type_is(self.config, 'right_keep_columns', list)
            self.right_cols = _keep_cols

        elif _drop_cols := self.config.get('right_drop_columns'):
            self.error_handler.assert_key_type_is(self.config, 'right_drop_columns', list)

            # if any(self.right_keys.map(__contains__, _drop_cols)):
            if any(col in self.right_keys for col in _drop_cols):
                self.error_handler.throw(
                    "you may not `right_drop_columns` that are part of the `right_key(s)`"
                )
                raise

            else:
                self.right_cols = list(set(self.right_data.columns) - set(_drop_cols))

        else:
            self.error_handler.throw(
                "`right_keep_columns` or `right_drop_columns` incorrectly specified (should be list of strings)"
            )
            raise


    def verify(self):
        """

        :return:
        """
        super().verify()

        if not set(self.left_cols).issubset(self.left_data.columns):
            self.error_handler.throw(
                "columns in `left_keep_columns` are not defined in the dataset"
            )
            raise

        if not set(self.right_cols).issubset(self.right_data.columns):
            self.error_handler.throw(
                "columns in `right_keep_columns` are not defined in the dataset"
            )
            raise


    def execute(self):
        """

        :return:
        """
        super().execute()

        _left_data  = self.left_data[ list(set(self.left_cols).union(self.left_keys)) ]
        _right_data = self.right_data[ list(set(self.right_cols).union(self.right_keys)) ]

        try:
            return pd.merge(
                _left_data, _right_data, how=self.join_type,
                left_on=self.left_keys, right_on=self.right_keys
            )

        except Exception as err:
            self.error_handler.throw("error during `join` operation. Check your join keys?")
            raise



class UnionOperation(GenericDataFrameOperation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.header = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        if len(self.source_list) < 2:
            self.error_handler.throw('more than one source must be defined in `sources`')
            raise


    def verify(self):
        """

        :return:
        """
        _data_columns = set( self.data_list[0].columns )

        for data in self.data_list[1:]:
            if set(data.columns) != _data_columns:
                self.error_handler.throw('dataframes to union do not share identical columns')
                raise
        else:
            self.header = list(_data_columns)


    def execute(self):
        """

        :return:
        """
        _result = self.data_list[0]

        for data in self.data_list[1:]:
            try:
                _result = pd.concat([_result, data], ignore_index=True)
            except Exception as e:
                self.error_handler.throw("error during `union` operation... are sources same shape?")
                raise

        return _result
