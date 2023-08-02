import csv
import dask.dataframe as dd
import jinja2
import re
import string
import os
import pandas as pd

from earthmover.nodes.operation import Operation
from earthmover import util


class AddColumnsOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'debug', 'expect', 'operation',
        'columns',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_dict: dict = None

    def compile(self):
        """

        :return:
        """
        super().compile()
        self.columns_dict = self.assert_get_key(self.config, 'columns', dtype=dict)

    def execute(self):
        """

        :return:
        """
        super().execute()

        for col, val in self.columns_dict.items():

            # Apply the value as a static string if not obviously Jinja.
            if not util.contains_jinja(val):
                self.data[col] = val

            else:
                try:
                    template = util.build_jinja_template(val, macros=self.earthmover.macros)

                except Exception as err:
                    self.logger.critical(
                        f"syntax error in Jinja template for column `{col}` of `add_columns` operation ({err}):\n===> {val}"
                    )
                    raise

                self.data[col] = self.data.apply(
                    util.render_jinja_template, axis=1,
                    meta=pd.Series(dtype='str', name=col),
                    template=template,
                    template_str=val
                )

        return self.data


class ModifyColumnsOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'debug', 'expect', 'operation',
        'columns',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_dict: dict = None

    def compile(self):
        """

        :return:
        """
        super().compile()
        self.columns_dict = self.assert_get_key(self.config, 'columns', dtype=dict)

    def execute(self):
        """

        :return:
        """
        super().execute()

        for col, val in self.columns_dict.items():

            # Apply the value as a static string if not obviously Jinja.
            if not util.contains_jinja(val):
                self.data[col] = val

            else:
                try:
                    template = util.build_jinja_template(val, macros=self.earthmover.macros)

                except Exception as err:
                    self.logger.critical(
                        f"syntax error in Jinja template for column `{col}` of `modify_columns` operation ({err}):\n===> {val}"
                    )
                    raise

                # TODO: Allow user to specify string that represents current column value.
                self.data['value'] = self.data[col]

                self.data[col] = self.data.apply(
                    util.render_jinja_template, axis=1,
                    meta=pd.Series(dtype='str', name=col),
                    template=template,
                    template_str=val
                )

                del self.data["value"]

        return self.data


class DuplicateColumnsOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'debug', 'expect', 'operation',
        'columns',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_dict: dict = None

    def compile(self):
        """

        :return:
        """
        super().compile()
        self.columns_dict = self.assert_get_key(self.config, 'columns', dtype=dict)

    def execute(self):
        """

        :return:
        """
        super().execute()

        for old_col, new_col in self.columns_dict.items():

            if new_col in self.data.columns:
                self.logger.warning(
                    f"Duplicate column operation overwrites existing column `{new_col}`."
                )

            if old_col not in self.data.columns:
                self.logger.critical(
                    f"column {old_col} not present in the dataset"
                )

            self.data[new_col] = self.data[old_col]

        return self.data


class RenameColumnsOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'debug', 'expect', 'operation',
        'columns',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_dict: dict = None

    def compile(self):
        """

        :return:
        """
        super().compile()
        self.columns_dict = self.assert_get_key(self.config, 'columns', dtype=dict)

    def execute(self):
        """

        :return:
        """
        super().execute()

        for old_col, new_col in self.columns_dict.items():
            if new_col in self.data.columns:
                self.logger.warning(
                    f"Rename column operation overwrites existing column `{new_col}`."
                )
            if old_col not in self.data.columns:
                self.logger.critical(
                    f"column {old_col} not present in the dataset"
                )

        self.data = self.data.rename(columns=self.columns_dict)

        return self.data


class DropColumnsOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'debug', 'expect', 'operation',
        'columns',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_to_drop: list = None

    def compile(self):
        """

        :return:
        """
        super().compile()
        self.columns_to_drop = self.assert_get_key(self.config, 'columns', dtype=list)

    def execute(self):
        """

        :return:
        """
        super().execute()

        if not set(self.columns_to_drop).issubset(self.data.columns):
            self.logger.critical(
                "one or more columns specified to drop are not present in the dataset"
            )
            raise

        self.data = self.data.drop(columns=self.columns_to_drop)

        return self.data


class KeepColumnsOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'debug', 'expect', 'operation',
        'columns',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.header: list = None

    def compile(self):
        """

        :return:
        """
        super().compile()

        self.header = self.assert_get_key(self.config, 'columns', dtype=list)

    def execute(self):
        """

        :return:
        """
        super().execute()

        if not set(self.header).issubset(self.data.columns):
            self.logger.critical(
                "one or more columns specified to keep are not present in the dataset"
            )
            raise

        self.data = self.data[self.header]

        return self.data


class CombineColumnsOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'debug', 'expect', 'operation',
        'columns', 'new_column', 'separator',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_list: list = None
        self.new_column: str = None
        self.separator: str = None

    def compile(self):
        """

        :return:
        """
        super().compile()

        self.columns_list = self.assert_get_key(self.config, 'columns', dtype=list)
        self.new_column   = self.assert_get_key(self.config, 'new_column', dtype=str)

        self.separator = self.config.get('separator', "")

    def execute(self):
        """

        :return:
        """
        super().execute()

        if not set(self.columns_list).issubset(self.data.columns):
            self.logger.critical(
                f"one or more defined columns is not present in the dataset"
            )
            raise

        self.data[self.new_column] = self.data.apply(
            lambda x: self.separator.join(x[col] for col in self.columns_list),
            axis=1,
            meta=pd.Series(dtype='str', name=self.new_column)
        )

        return self.data



class MapValuesOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'debug', 'expect', 'operation',
        'column', 'columns', 'mapping', 'map_file',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_list: list = None
        self.map_file: str = None
        self.mapping: dict = None

    def compile(self):
        """

        :return:
        """
        super().compile()

        # Only 'column' or 'columns' can be populated
        _column  = self.assert_get_key(self.config, 'column', dtype=str, required=False)
        _columns = self.assert_get_key(self.config, 'columns', dtype=list, required=False)

        if bool(_column) == bool(_columns):  # Fail if both or neither are populated.
            self.logger.critical(
                "a `map_values` operation must specify either one `column` or several `columns` to convert"
            )
            raise

        self.columns_list = _columns or [_column]  # `[None]` evaluates to True

        #
        _mapping  = self.assert_get_key(self.config, 'mapping', dtype=dict, required=False)
        _map_file = self.assert_get_key(self.config, 'map_file', dtype=str, required=False)

        if _mapping:
            self.mapping = _mapping
        elif _map_file:
            self.mapping = self._read_map_file(_map_file)
        else:
            self.logger.critical(
                "must define either `mapping` (list of old_value: new_value) or a `map_file` (two-column CSV or TSV)"
            )
            raise

    def execute(self):
        """

        :return:
        """
        super().execute()

        if not set(self.columns_list).issubset(self.data.columns):
            self.logger.critical(
                "one or more columns to map are undefined in the dataset"
            )

        try:
            for _column in self.columns_list:
                self.data[_column] = self.data[_column].replace(self.mapping)

        except Exception as _:
            self.logger.critical(
                "error during `map_values` operation... check mapping shape and `column(s)`?"
            )

        return self.data

    def _read_map_file(self, file) -> dict:
        """

        :param file:
        :return:
        """
        sep = util.get_sep(file)


        try:
            with open(file, 'r', encoding='utf-8') as fp:
                _translations_list = list(csv.reader(fp, delimiter=sep))
                return dict(_translations_list[1:])
        
        except Exception as err:
            self.logger.critical(
                f"error reading `map_file` {file}: {err}"
            )
            raise



class DateFormatOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'debug', 'expect', 'operation',
        'column', 'columns', 'from_format', 'to_format',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_list: list = None
        self.from_format: str = None
        self.to_format: str = None

    def compile(self):
        """

        :return:
        """
        super().compile()

        self.from_format = self.assert_get_key(self.config, 'from_format', dtype=str)
        self.to_format   = self.assert_get_key(self.config, 'to_format', dtype=str)

        # Only 'column' or 'columns' can be populated
        _column  = self.assert_get_key(self.config, 'column', dtype=str, required=False)
        _columns = self.assert_get_key(self.config, 'columns', dtype=list, required=False)

        if bool(_column) == bool(_columns):  # Fail if both or neither are populated.
            self.logger.critical(
                "a `date_format` operation must specify either one `column` or several `columns` to convert"
            )
            raise

        self.columns_list = _columns or [_column]  # `[None]` evaluates to True

    def execute(self):
        """

        :return:
        """
        super().execute()

        if not set(self.columns_list).issubset(self.data.columns):
            self.logger.critical(
                "one or more columns to map are undefined in the dataset"
            )
            raise

        for _column in self.columns_list:
            try:
                self.data[_column] = (
                    dd.to_datetime(self.data[_column], format=self.from_format)
                        .dt.strftime(self.to_format)
                )

            except Exception as err:
                self.logger.critical(
                    f"error during `date_format` operation, `{_column}` column... check format strings? ({err})"
                )

        return self.data



class SnakeCaseColumnsOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'debug', 'expect', 'operation',
    )

    def execute(self) -> 'DataFrame':
        """

        :return:
        """
        super().execute()

        data_columns  = list(self.data.columns)
        snake_columns = list(map(self.to_snake_case, data_columns))

        if len(set(data_columns)) != len(set(snake_columns)):
            self.logger.critical(
                f"Snake case operation creates duplicate columns!\n"
                f"Columns before: {len(set(data_columns))}\n"
                f"Columns after : {len(set(snake_columns))}"
            )

        self.data = self.data.rename(columns=dict(zip(data_columns, snake_columns)))
        return self.data

    @staticmethod
    def to_snake_case(text: str):
        """
        Convert camelCase names to snake_case names.
        :param text: A camelCase string value to be converted to snake_case.
        :return: A string in snake_case.
        """
        punctuation_regex = re.compile("[" + re.escape(string.punctuation) + " ]")  # Include space

        text = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', text)
        text = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', text)
        text = punctuation_regex.sub("_", text)  # Replace any punctuation or spaces with underscores
        text = re.sub(r'_+', '_', text)  # Consolidate underscores
        text = re.sub(r'^_', '', text)  # Remove leading underscores
        return text.lower()
