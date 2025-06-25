import csv
import dask
import pandas as pd
import re
import string

from earthmover.operations.operation import Operation
from earthmover import util

from typing import Dict, List, Tuple
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from dask.dataframe.core import DataFrame


class AddColumnsOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition', 
        'columns',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_dict = self.error_handler.assert_get_key(self.config, 'columns', dtype=dict)

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """

        :return:
        """
        super().execute(data, **kwargs)

        for col, val in self.columns_dict.items():

            # Apply the value as a static string if not obviously Jinja.
            if not util.contains_jinja(val):
                data[col] = val

            else:
                try:
                    template = util.build_jinja_template(val, macros=self.earthmover.macros)

                except Exception as err:
                    self.error_handler.ctx.remove('line')
                    self.error_handler.throw(
                        f"syntax error in Jinja template for column `{col}` of `add_columns` operation ({err}):\n===> {val}"
                    )
                    raise

                data[col] = data.apply(
                    util.render_jinja_template, axis=1,
                    meta=pd.Series(dtype='str', name=col),
                    template=template,
                    template_str=val,
                    error_handler=self.error_handler
                )

        return data


class ModifyColumnsOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition', 
        'columns',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_dict = self.error_handler.assert_get_key(self.config, 'columns', dtype=dict)

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """

        :return:
        """
        super().execute(data, **kwargs)

        # Map each config column separately to expand wildcards one-by-one.
        for col, val in self.columns_dict.items():
            for data_column in self.match_wildcard_columns(data.columns, [col]):
                self.apply_jinja(data, data_column, val)

        return data
    
    def apply_jinja(self, data, col, val):
        # Apply the value as a static string if not obviously Jinja.
        if not util.contains_jinja(val):
            data[col] = val
            return  # End immediately if no jinja processing is required.
        
        try:
            template = util.build_jinja_template(val, macros=self.earthmover.macros)

        except Exception as err:
            self.error_handler.ctx.remove('line')
            self.error_handler.throw(
                f"syntax error in Jinja template for column `{col}` of `modify_columns` operation ({err}):\n===> {val}"
            )
            raise

        # TODO: Allow user to specify string that represents current column value.
        data['value'] = data[col]

        data[col] = data.apply(
            util.render_jinja_template, axis=1,
            meta=pd.Series(dtype='str', name=col),
            template=template,
            template_str=val,
            error_handler=self.error_handler
        )
        del data["value"]


class DuplicateColumnsOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition', 
        'columns',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_dict = self.error_handler.assert_get_key(self.config, 'columns', dtype=dict)

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """

        :return:
        """
        super().execute(data, **kwargs)

        for old_col, new_col in self.columns_dict.items():

            if new_col in data.columns:
                self.logger.warning(
                    f"Duplicate column operation overwrites existing column `{new_col}`."
                )

            if old_col not in data.columns:
                self.error_handler.throw(
                    f"column {old_col} not present in the dataset"
                )

            data[new_col] = data[old_col]

        return data


class RenameColumnsOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition', 
        'columns',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_dict = self.error_handler.assert_get_key(self.config, 'columns', dtype=dict)

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """

        :return:
        """
        super().execute(data, **kwargs)

        for old_col, new_col in self.columns_dict.items():
            if new_col in data.columns:
                self.logger.warning(
                    f"Rename column operation overwrites existing column `{new_col}`."
                )
            if old_col not in data.columns:
                self.error_handler.throw(
                    f"column {old_col} not present in the dataset"
                )

        data = data.rename(columns=self.columns_dict)

        return data


class DropColumnsOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition', 
        'columns',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_to_drop = self.error_handler.assert_get_key(self.config, 'columns', dtype=list)

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """

        :return:
        """
        super().execute(data, **kwargs)

        cols_to_discard = self.match_wildcard_columns(data.columns, self.columns_to_drop)
        if not set(cols_to_discard).issubset(data.columns):
            self.error_handler.throw(
                f"one or more columns specified to drop are not present in the dataset: {set(self.columns_to_drop).difference(data.columns)}"
            )
            raise

        # New functionality: do not raise an error if a column was not found in the dataset.
        data = data.drop(columns=cols_to_discard)

        return data


class KeepColumnsOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition', 
        'columns',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.header = self.error_handler.assert_get_key(self.config, 'columns', dtype=list)

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """

        :return:
        """
        super().execute(data, **kwargs)

        # Raise an error if a column specified to keep is absent from the dataset.
        cols_to_keep = self.match_wildcard_columns(data.columns, self.header, raise_on_unmatched=True)
        data = data[cols_to_keep]

        return data


class CombineColumnsOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition', 
        'columns', 'new_column', 'separator',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_list = self.error_handler.assert_get_key(self.config, 'columns', dtype=list)
        self.new_column   = self.error_handler.assert_get_key(self.config, 'new_column', dtype=str)
        self.separator = self.config.get('separator', "")

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """

        :return:
        """
        super().execute(data, **kwargs)

        # Columns are returned in order they were matched.
        # If more than one wildcard matches the same column, only the first is kept.
        # Raise an error if a column specified to combine is absent from the dataset.
        cols_to_combine = self.match_wildcard_columns(data.columns, self.columns_list, raise_on_unmatched=True)
        data[self.new_column] = data.apply(
            lambda x: self.separator.join(x[col] for col in cols_to_combine),
            axis=1,
            meta=pd.Series(dtype='str', name=self.new_column)
        )

        return data



class MapValuesOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition', 
        'column', 'columns', 'mapping', 'map_file',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

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

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """

        :return:
        """
        super().execute(data, **kwargs)

        try:
            # Raise an error if a column specified to combine is absent from the dataset.
            for data_column in self.match_wildcard_columns(data.columns, self.columns_list, raise_on_unmatched=True):
                data[data_column] = data[data_column].replace(self.mapping)

        except Exception as _:
            self.error_handler.throw(
                "error during `map_values` operation... check mapping shape and `column(s)`?"
            )

        return data

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
            self.error_handler.throw(
                f"error reading `map_file` {file}: {err}"
            )
            raise



class DateFormatOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition', 
        'column', 'columns', 'from_format', 'to_format', 'ignore_errors', 'exact_match',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.from_format = self.error_handler.assert_get_key(self.config, 'from_format', dtype=str)
        self.to_format   = self.error_handler.assert_get_key(self.config, 'to_format', dtype=str)
        self.ignore_errors   = self.error_handler.assert_get_key(self.config, 'ignore_errors', dtype=bool, required=False)
        self.exact_match   = self.error_handler.assert_get_key(self.config, 'exact_match', dtype=bool, required=False)

        # Only 'column' or 'columns' can be populated
        _column  = self.error_handler.assert_get_key(self.config, 'column', dtype=str, required=False)
        _columns = self.error_handler.assert_get_key(self.config, 'columns', dtype=list, required=False)

        if bool(_column) == bool(_columns):  # Fail if both or neither are populated.
            self.error_handler.throw(
                "a `date_format` operation must specify either one `column` or several `columns` to convert"
            )
            raise

        self.columns_list = _columns or [_column]  # `[None]` evaluates to True

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """

        :return:
        """
        super().execute(data, **kwargs)


        # Raise an error if a column specified to combine is absent from the dataset.
        for data_column in self.match_wildcard_columns(data.columns, self.columns_list, raise_on_unmatched=True):
            try:
                data[data_column] = (
                    dask.dataframe.to_datetime(data[data_column], format=self.from_format, exact=bool(self.exact_match), errors='coerce' if self.ignore_errors else 'raise')
                        .dt.strftime(self.to_format)
                )

            except Exception as err:
                self.error_handler.throw(
                    f"error during `date_format` operation, `{data_column}` column... check format strings? ({err})"
                )

        return data



class CaseColumnsOperation(Operation):
    """
    Generic casing operation to be overridden by child classes.
    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition', 
    )

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """

        :return:
        """
        super().execute(data, **kwargs)

        data_columns  = list(data.columns)
        cased_columns = list(map(self.apply_case, data_columns))

        if len(set(data_columns)) != len(set(cased_columns)):
            self.error_handler.throw(
                f"Casing operation creates duplicate columns!\n"
                f"Columns before: {len(set(data_columns))}\n"
                f"Columns after : {len(set(cased_columns))}"
            )

        data = data.rename(columns=dict(zip(data_columns, cased_columns)))
        return data

    @staticmethod
    def apply_case(text: str) -> str:
        """
        :param text: A string to apply the casing to.
        """
        raise NotImplementedError("Generic CaseColumnsOperation must be inherited by a child class.")

class SnakeCaseColumnsOperation(CaseColumnsOperation):
    """
    Convert camelCase names to snake_case names.
    """
    @staticmethod
    def apply_case(text: str) -> str:
        punctuation_regex = re.compile("[" + re.escape(string.punctuation) + " ]")  # Include space

        text = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', text)
        text = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', text)
        text = punctuation_regex.sub("_", text)  # Replace any punctuation or spaces with underscores
        text = re.sub(r'_+', '_', text)  # Consolidate underscores
        text = re.sub(r'^_', '', text)  # Remove leading underscores
        return text.lower()

class LowercaseColumnsOperation(CaseColumnsOperation):
    @staticmethod
    def apply_case(text: str) -> str:
        return text.lower()

class UppercaseColumnsOperation(CaseColumnsOperation):
    @staticmethod
    def apply_case(text: str) -> str:
        return text.upper()
