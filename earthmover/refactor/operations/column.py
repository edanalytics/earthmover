import abc
import csv

import jinja2

import os
import pandas as pd

from earthmover.refactor.operations.operation import Operation
from earthmover.refactor import util


class GenericColumnOperation(Operation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.source = None
        self.data = None


    @abc.abstractmethod
    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, 'source', str)
        self.source = self.config['source']
        pass


    def verify(self):
        """
        Verification is optional; therefore, this is not assigned as an abstract method.

        :return:
        """
        super().verify()
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



class AddColumnsOperation(GenericColumnOperation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.columns_dict = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, 'columns', dict)
        self.columns_dict = self.config['columns']


    def execute(self):
        """

        :return:
        """
        super().execute()

        for col, val in self.columns_dict.items():

            if col == "__line__":
                continue

            if util.contains_jinja(val):
                try:
                    template = jinja2.Environment(
                        loader=jinja2.FileSystemLoader(os.path.dirname('/'))
                    ).from_string(self.earthmover.state_configs['macros'] + val)

                except Exception as err:
                    self.error_handler.throw(
                        f"syntax error in Jinja template for column `{col}` of `add_columns` operation ({err})"
                    )
                    raise

                self.data = self.data.apply(
                    util.apply_jinja_template_to_row,
                    axis=1,
                    kwargs={
                        'template': template,
                        'col': col,
                        'error_handler': self.error_handler,
                    }
                )

            # Otherwise, assign a static value as the column.
            else:
                self.data[col] = val

        return self.data



class ModifyColumnsOperation(GenericColumnOperation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.columns_dict = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, 'columns', dict)
        self.columns_dict = self.config['columns']


    def execute(self):
        """

        :return:
        """
        super().execute()

        for col, val in self.columns_dict.items():

            if col == "__line__":
                continue

            # Apply the value as a static string if not obviously Jinja.
            if not util.contains_jinja(val):
                self.data[col] = val

            else:
                try:
                    template = jinja2.Environment(
                        loader=jinja2.FileSystemLoader(os.path.dirname('/'))
                    ).from_string(self.earthmover.state_configs['macros'] + val)

                except Exception as err:
                    self.error_handler.throw(
                        f"syntax error in Jinja template for column `{col}` of `add_columns` operation ({err})"
                    )
                    raise

                self.data = self.data.apply(
                    self._apply_jinja, axis=1, args=(template, col)
                )

        return self.data


    def _apply_jinja(self, row, template, col):
        """
        Extends util.apply_jinja_template_to_row().
        Adds the ability to reference current column value using `value` key.
        TODO: Let user specify name of column reference string.

        :param row:
        :param template:
        :param col:
        :return:
        """
        row["value"] = row[col]

        row = util.apply_jinja_template_to_row(row, template, col, error_handler=self.error_handler)

        del row["value"]

        return row



class DuplicateColumnsOperation(GenericColumnOperation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.columns_dict = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, "columns", dict)
        self.columns_dict = self.config['columns']


    def execute(self):
        """

        :return:
        """
        super().execute()

        for old_col, new_col in self.columns_dict.items():

            if old_col=="__line__":
                continue

            self.data[new_col] = self.data[old_col]

        return self.data



class RenameColumnsOperation(GenericColumnOperation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.columns_dict = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, "columns", dict)
        self.columns_dict = self.config['columns']


    def execute(self):
        """

        :return:
        """
        super().execute()

        return self.data.rename(columns=self.columns_dict)



class DropColumnsOperation(GenericColumnOperation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.columns_to_drop = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, "columns", list)
        self.columns_to_drop = self.config['columns']


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

        return self.data.drop(columns=self.columns_to_drop)



class KeepColumnsOperation(GenericColumnOperation):
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

        self.error_handler.assert_key_exists_and_type_is(self.config, "columns", list)
        self.header = self.config['columns']


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

        return self.data[self.header]



class CombineColumnsOperation(GenericColumnOperation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.columns_list = None
        self.new_column = None
        self.separator = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, "columns", list)
        self.columns_list = self.config['columns']

        self.error_handler.assert_key_exists_and_type_is(self.config, "new_column", str)
        self.new_column = self.config['new_column']

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
            (lambda x: self.separator.join(x[col] for col in self.columns_list))
        , axis=1)#, meta='str')

        return self.data



class MapValuesOperation(GenericColumnOperation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.columns_list = None
        self.mapping = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        #
        if _column := self.config.get('column'):
            self.error_handler.assert_key_type_is(self.config, 'column', str)
            self.columns_list = [_column]

        elif _columns := self.config.get('columns'):
            self.error_handler.assert_key_type_is(self.config, 'columns', list)
            self.columns_list = _columns

        else:
            self.error_handler.throw(
                "a `map_values` operation must specify either one `column` or several `columns` to convert"
            )
            raise

        #
        if _mapping := self.config.get('mapping'):
            self.error_handler.assert_key_type_is(self.config, "mapping", dict)
            self.mapping = _mapping

        elif _map_file := self.config.get('map_file'):
            self.error_handler.assert_key_type_is(self.config, "map_file", str)
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
                _translations_list = list(csv.DictReader(fp, delimiter=sep))
                return {key: val for trans in _translations_list for key, val in trans.items()}

        except Exception as err:
            self.error_handler.throw(
                f"error reading `map_file` {file}: {err}"
            )
            raise



class DateFormatOperation(GenericColumnOperation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.columns_list = None
        self.from_format = None
        self.to_format = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, 'from_format', str)
        self.from_format = self.config['from_format']

        self.error_handler.assert_key_exists_and_type_is(self.config, 'to_format', str)
        self.to_format = self.config['to_format']

        #
        if _column := self.config.get('column'):
            self.error_handler.assert_key_type_is(self.config, 'column', str)
            self.columns_list = [_column]

        elif _columns := self.config.get('columns'):
            self.error_handler.assert_key_type_is(self.config, 'columns', list)
            self.columns_list = _columns

        else:
            self.error_handler.throw(
                "a `date_format` operation must specify either one `column` or several `columns` to convert"
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
            raise


    def execute(self):
        """

        :return:
        """
        super().execute()

        for _column in self.columns_list:
            try:
                self.data[_column] = (
                    pd.to_datetime(self.data[_column], format=self.from_format)
                        .dt.strftime(self.to_format)
                )

            except Exception as err:
                self.error_handler.throw(
                    f"error during `date_format` operation, `{_column}` column... check format strings? ({err})"
                )
        return self.data
