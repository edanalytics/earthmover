import os
import pandas as pd
import re

from earthmover.nodes.node import Node
from earthmover import util

from typing import Tuple


class Destination(Node):
    """

    """
    type: str = 'destination'
    mode: str = None  # Documents which class was chosen.
    allowed_configs: Tuple[str] = ('debug', 'expect', 'show_progress', 'repartition', 'source',)

    NULL_REPR: object = None  # Representation for Nones, NaNs, and NAs on output.
    STRING_DTYPES: Tuple[object] = ()  # Datatypes to be forced to strings on output (default none).

    def __new__(cls, *args, **kwargs):
        return object.__new__(FileDestination)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source: str = self.error_handler.assert_get_key(self.config, 'source', dtype=str)
        self.upstream_sources[self.source] = None

    @classmethod
    def cast_output_dtype(cls, value: object) -> object:
        """
        Helper method for casting row values to correct datatypes.
        Null-representation and dtype-to-string conversion differ by destination subclass.
        """
        if pd.isna(value):
            return cls.NULL_REPR
        
        if isinstance(value, cls.STRING_DTYPES):
            return str(value)
        
        return value


class FileDestination(Destination):
    """

    """
    mode: str = 'file'
    allowed_configs: Tuple[str] = (
        'debug', 'expect', 'show_progress', 'repartition', 'source',
        'template', 'extension', 'linearize', 'header', 'footer',
    )

    NULL_REPR: object = ""  # Templates use empty strings as nulls.
    STRING_DTYPES: Tuple[object] = (bool, int, float)  # All scalars are converted to strings in templates.

    EXP = re.compile(r"\s+")
    TEMPLATED_COL = "____OUTPUT____"
    DEFAULT_TEMPLATE = """{ {% for col, val in __row_data__.items() %}"{{ col }}": {{ val | tojson }}{% if not loop.last %}, {% endif %}{% endfor %} }"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.template = self.error_handler.assert_get_key(self.config, 'template', dtype=str, required=False, default=None)
        self.header = self.error_handler.assert_get_key(self.config, 'header', dtype=str, required=False, default=None)
        self.footer = self.error_handler.assert_get_key(self.config, 'footer', dtype=str, required=False, default=None)
        self.linearize = self.error_handler.assert_get_key(self.config, 'linearize', dtype=bool, required=False, default=True)
        self.extension = self.error_handler.assert_get_key(self.config, 'extension', dtype=str, required=False, default='')
        self.jinja_template = None  # Defined in execute()

        #config->extension is optional: if not present, we assume the destination name has an extension
        filename = f"{self.name}.{self.extension}" if self.extension else self.name
        self.file = os.path.join(self.earthmover.state_configs['output_dir'], filename)

    def execute(self, **kwargs):
        """
        
        :return:
        """
        super().execute(**kwargs)

        # Prepare the Jinja template for rendering rows.
        try:
            if self.template:
                # with open(self.template, 'r', encoding='utf-8') as fp:
                #     template_string = fp.read()
                template_string = ''
            else:
                template_string = self.DEFAULT_TEMPLATE

            # Replace multiple spaces with a single space to flatten templates.
            if self.linearize:
                template_string = self.EXP.sub(" ", template_string)

        except OSError as err:
            self.error_handler.throw(
                f"`template` file {self.template} cannot be opened ({err})"
            )
            raise

        except Exception as err:
            self.error_handler.throw(
                f"syntax error in Jinja template in `template` file {self.template} ({err})"
            )
            raise

        # this renders each row without having to itertuples() (which is much slower)
        # (meta=... below is how we prevent dask warnings that it can't infer the output data type)
        self.data = (
            self.upstream_sources[self.source].data
                .map_partitions(self.apply_render_to_partition, meta=pd.Series('str'))
        )

        # Repartition before writing, if specified.
        self.data = self.opt_repartition(self.data)

        # Verify the output directory exists.
        os.makedirs(os.path.dirname(self.file), exist_ok=True)

        # Write the optional header, each line, and the optional footer.
        with open(self.file, 'w+', encoding='utf-8') as fp:
            if self.header:
                fp.write(self.header)
        
        with open(self.file, 'a+', encoding='utf-8') as fp:
            fp.writelines(self.data.compute())
        # for partition in self.data.partitions:
            # lines = partition.compute().result()
            # with open(self.file, 'a+', encoding='utf-8') as fp:
            #     fp.writelines(lines)
            # partition = None  # Remove partition from memory immediately after write.
        
        with open(self.file, 'a+', encoding='utf-8') as fp:
            if self.footer:
                fp.write(self.footer)

        self.logger.debug(f"output `{self.file}` written")
        self.size = os.path.getsize(self.file)

    @classmethod
    def apply_render_to_partition(cls, partition):
        return partition.apply(cls.render_row, axis=1)
    
    @classmethod
    def render_row(cls, row: pd.Series):
        jinja_template = util.build_jinja_template(cls.template_string, macros=cls.earthmover.macros)
        print(row)
        row_data = {
            field: cls.cast_output_dtype(value)
            for field, value in row.to_dict().items()
        }
        row_data["__row_data__"] = row_data

        try:
            json_string = jinja_template.render(row_data) + "\n"

        except Exception as err:
            cls.error_handler.throw(
                f"error rendering Jinja template in `template` file {cls.template_file} ({err})"
            )
            raise

        return json_string
