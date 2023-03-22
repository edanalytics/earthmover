import csv
import dask.dataframe as dd
import jinja2
import os
import pandas as pd

from earthmover.nodes.operation import Operation
from earthmover import util


class AddColumnsOperation(Operation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(['columns'])

        self.columns_dict = None


    def compile(self):
        """

        :return:
        """
        super().compile()
        self.columns_dict = self.error_handler.assert_get_key(self.config, 'columns', dtype=dict)


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
                    template = jinja2.Environment(
                        loader=jinja2.FileSystemLoader(os.path.dirname('./'))
                    ).from_string(self.earthmover.state_configs['macros'] + val)

                except Exception as err:
                    self.error_handler.ctx.remove('line')
                    self.error_handler.throw(
                        f"syntax error in Jinja template for column `{col}` of `add_columns` operation ({err}):\n===> {val}"
                    )
                    raise

                self.data[col] = self.data.apply(
                    util.render_jinja_template, axis=1,
                    meta=pd.Series(dtype='str', name=col),
                    template=template,
                    template_str=val,
                    error_handler=self.error_handler
                )

        return self.data



class ModifyColumnsOperation(Operation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(['columns'])

        self.columns_dict = None


    def compile(self):
        """

        :return:
        """
        super().compile()
        self.columns_dict = self.error_handler.assert_get_key(self.config, 'columns', dtype=dict)


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
                    template = jinja2.Environment(
                        loader=jinja2.FileSystemLoader(os.path.dirname('./'))
                    ).from_string(self.earthmover.state_configs['macros'] + val)

                except Exception as err:
                    self.error_handler.ctx.remove('line')
                    self.error_handler.throw(
                        f"syntax error in Jinja template for column `{col}` of `modify_columns` operation ({err}):\n===> {val}"
                    )
                    raise

                # TODO: Allow user to specify string that represents current column value.
                self.data['value'] = self.data[col]

                self.data[col] = self.data.apply(
                    util.render_jinja_template, axis=1,
                    meta=pd.Series(dtype='str', name=col),
                    template=template,
                    template_str=val,
                    error_handler=self.error_handler
                )

                del self.data["value"]

        return self.data



class DuplicateColumnsOperation(Operation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(['columns'])

        self.columns_dict = None


    def compile(self):
        """

        :return:
        """
        super().compile()
        self.columns_dict = self.error_handler.assert_get_key(self.config, 'columns', dtype=dict)

    
    def verify(self):
        """
        
        :return:
        """
        _columns = set(self.data.columns)

        for old_col, new_col in self.columns_dict.items():

            if new_col in _columns:
                self.logger.warning(
                    f"Duplicate column operation overwrites existing column `{new_col}`."
                )

            if old_col not in _columns:
                self.error_handler.throw(
                    f"column {old_col} not present in the dataset"
                )
            
            _columns.remove(old_col)
            _columns.add(new_col)


    def execute(self):
        """

        :return:
        """
        super().execute()

        for old_col, new_col in self.columns_dict.items():

            self.data[new_col] = self.data[old_col]

        return self.data



class RenameColumnsOperation(Operation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(['columns'])

        self.columns_dict = None


    def compile(self):
        """

        :return:
        """
        super().compile()
        self.columns_dict = self.error_handler.assert_get_key(self.config, 'columns', dtype=dict)


    def verify(self):
        """
        
        :return:
        """
        _columns = set(self.data.columns)

        for old_col, new_col in self.columns_dict.items():

            if new_col in _columns:
                self.logger.warning(
                    f"Rename column operation overwrites existing column `{new_col}`."
                )

            if old_col not in _columns:
                self.error_handler.throw(
                    f"column {old_col} not present in the dataset"
                )
            
            _columns.remove(old_col)
            _columns.add(new_col)


    def execute(self):
        """

        :return:
        """
        super().execute()

        self.data = self.data.rename(columns=self.columns_dict)

        return self.data



class DropColumnsOperation(Operation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(['columns'])

        self.columns_to_drop = None


    def compile(self):
        """

        :return:
        """
        super().compile()
        self.columns_to_drop = self.error_handler.assert_get_key(self.config, 'columns', dtype=list)


    def verify(self):
        """

        :return:
        """
        super().verify()

        if not set(self.columns_to_drop).issubset(self.data.columns):
            self.error_handler.throw(
                "one or more columns specified to drop are not present in the dataset"
            )
            raise


    def execute(self):
        """

        :return:
        """
        super().execute()

        self.data = self.data.drop(columns=self.columns_to_drop)

        return self.data


class KeepColumnsOperation(Operation):
    """

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(['columns'])

        self.header = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.header = self.error_handler.assert_get_key(self.config, 'columns', dtype=list)


    def verify(self):
        """

        :return:
        """
        super().verify()

        if not set(self.header).issubset(self.data.columns):
            self.error_handler.throw(
                "one or more columns specified to keep are not present in the dataset"
            )
            raise


    def execute(self):
        """

        :return:
        """
        super().execute()

        self.data = self.data[self.header]

        return self.data


class CombineColumnsOperation(Operation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(['columns', 'new_column', 'separator'])

        self.columns_list = None
        self.new_column = None
        self.separator = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.columns_list = self.error_handler.assert_get_key(self.config, 'columns', dtype=list)
        self.new_column   = self.error_handler.assert_get_key(self.config, 'new_column', dtype=str)

        self.separator = self.config.get('separator', "")


    def verify(self):
        """

        :return:
        """
        super().verify()

        if not set(self.columns_list).issubset(self.data.columns):
            self.error_handler.throw(
                f"one or more defined columns is not present in the dataset"
            )
            raise

    def execute(self):
        """

        :return:
        """
        super().execute()

        self.data[self.new_column] = self.data.apply(
            lambda x: self.separator.join(x[col] for col in self.columns_list),
            axis=1,
            meta=pd.Series(dtype='str', name=self.new_column)
        )

        return self.data



class MapValuesOperation(Operation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(['column', 'columns', 'mapping', 'map_file'])

        self.columns_list = None
        self.map_file = None
        self.mapping = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        # Only 'column' or 'columns' can be populated
        _column  = self.error_handler.assert_get_key(self.config, 'column', dtype=str, required=False)
        _columns = self.error_handler.assert_get_key(self.config, 'columns', dtype=list, required=False)

        if bool(_column) == bool(_columns):  # Fail if both or neither are populated.
            self.error_handler.throw(
                "a `map_values` operation must specify either one `column` or several `columns` to convert"
            )
            raise

        self.columns_list = _columns or [_column]  # `[None]` evaluates to True

        #
        _mapping  = self.error_handler.assert_get_key(self.config, 'mapping', dtype=dict, required=False)
        _map_file = self.error_handler.assert_get_key(self.config, 'map_file', dtype=str, required=False)

        if _mapping:
            self.mapping = _mapping
        elif _map_file:
            self.mapping = self._read_map_file(_map_file)
        else:
            self.error_handler.throw(
                "must define either `mapping` (list of old_value: new_value) or a `map_file` (two-column CSV or TSV)"
            )
            raise


    def verify(self):
        """

        :return:
        """
        super().verify()

        if not set(self.columns_list).issubset(self.data.columns):
            self.error_handler.throw(
                "one or more columns to map are undefined in the dataset"
            )


    def execute(self):
        """

        :return:
        """
        super().execute()

        try:
            for _column in self.columns_list:
                self.data[_column] = self.data[_column].replace(self.mapping)

        except Exception as _:
            self.error_handler.throw(
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
            with open(file, 'r') as fp:
                _translations_list = list(csv.reader(fp, delimiter=sep))
                return dict(_translations_list[1:])
        
        except Exception as err:
            self.error_handler.throw(
                f"error reading `map_file` {file}: {err}"
            )
            raise



class DateFormatOperation(Operation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(['column', 'columns', 'from_format', 'to_format'])

        self.columns_list = None
        self.from_format = None
        self.to_format = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.from_format = self.error_handler.assert_get_key(self.config, 'from_format', dtype=str)
        self.to_format   = self.error_handler.assert_get_key(self.config, 'to_format', dtype=str)

        # Only 'column' or 'columns' can be populated
        _column  = self.error_handler.assert_get_key(self.config, 'column', dtype=str, required=False)
        _columns = self.error_handler.assert_get_key(self.config, 'columns', dtype=list, required=False)

        if bool(_column) == bool(_columns):  # Fail if both or neither are populated.
            self.error_handler.throw(
                "a `date_format` operation must specify either one `column` or several `columns` to convert"
            )
            raise

        self.columns_list = _columns or [_column]  # `[None]` evaluates to True


    def verify(self):
        """

        :return:
        """
        super().verify()

        if not set(self.columns_list).issubset(self.data.columns):
            self.error_handler.throw(
                "one or more columns to map are undefined in the dataset"
            )
            raise


    def execute(self):
        """

        :return:
        """
        super().execute()

        for _column in self.columns_list:
            try:
                self.data[_column] = (
                    dd.to_datetime(self.data[_column], format=self.from_format)
                        .dt.strftime(self.to_format)
                )

            except Exception as err:
                self.error_handler.throw(
                    f"error during `date_format` operation, `{_column}` column... check format strings? ({err})"
                )

        return self.data
