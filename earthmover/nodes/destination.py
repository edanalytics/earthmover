import os
import pandas as pd
import re
import csv
import warnings
from functools import partial

from earthmover.nodes.node import Node
from earthmover import util
from earthmover.yaml_parser import JinjaEnvironmentYamlLoader

from typing import Tuple


class Destination(Node):
    """

    """
    type: str = 'destination'
    mode: str = None  # Documents which class was chosen.
    allowed_configs: Tuple[str] = ('debug', 'expect', 'require_rows', 'show_progress', 'repartition', 'source',)

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
    DEFAULT_TEMPLATE = """{ {% for col, val in __row_data__.pop('__row_data__').items() %}"{{ col }}": {{ val | tojson }}{% if not loop.last %}, {% endif %}{% endfor %} }"""

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
                template_string = JinjaEnvironmentYamlLoader.template_open_filepath(self.template, params=self.earthmover.params)
            else:
                template_string = self.DEFAULT_TEMPLATE

            # Replace multiple spaces with a single space to flatten templates.
            if self.linearize:
                template_string = self.EXP.sub(" ", template_string)
            else:
                template_string = template_string.strip("\r\n") + "\n"

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
            self.upstream_sources[self.source].data.repartition(partition_size="1MB")
                .map_partitions(partial(self.apply_render_row, template_string, self.render_row), meta=pd.Series('str'))
        )

        # Repartition before writing, if specified.
        self.data = self.opt_repartition(self.data)

        # Verify the output directory exists.
        os.makedirs(os.path.dirname(self.file), exist_ok=True)

        if os.path.exists(self.file): os.remove(self.file) # remove file (if exists)
        open(self.file, 'a').close() # touch file, so it exists

        # only load the first row if header/footer contain Jinja that might need it:
        if (
            (self.header and util.contains_jinja(self.header))
            or (self.footer and util.contains_jinja(self.footer))
        ):
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings("ignore", message="Insufficient elements for `head`")
                    # (use `npartitions=-1` because the first N partitions could be empty)
                    # (use self.upstream_sources because self.data is a single column, the rendered Jinja template)
                    first_row = self.upstream_sources[self.source].data.head(1, npartitions=-1).reset_index(drop=True).iloc[0]
            
            except IndexError:  # If no rows are present, build a representation of the row with empty values
                first_row = {col: "" for col in self.upstream_sources[self.source].data.columns}
                # first_row['__row_data__'] = first_row
                # first_row = pd.Series(first_row)
            # first_row_data = util.add_dunder_row_data(first_row)
        
        # Write the optional header, each line
        if self.header:
            with open(self.file, 'a', encoding='utf-8') as fp:
                if self.header and util.contains_jinja(self.header):
                    header_template = util.build_jinja_template(self.earthmover.macros + self.header)
                    rendered_template = self.render_row(first_row, template=header_template, template_string=self.header, dunder_row_data=True)
                    fp.write(rendered_template.strip("\r\n") + "\n")
                elif self.header: # no jinja
                    fp.write(self.header.strip("\r\n") + "\n")
        
        # Append data rows to file:
        # to_csv() - which is most efficient - unfortunately only works if `linearize: True`;
        # otherwise, we get an error about escapechar being required (since the non-linearized
        # data might contain newline chars)
        if self.linearize:
            self.data.to_csv(self.file, single_file=True, index=False, header=False, encoding='utf-8', mode='a', quoting=csv.QUOTE_NONE, doublequote=False, na_rep=" ", sep="~", escapechar='')
        else:
            with open(self.file, 'a', encoding='utf-8') as fp:
                for partition in self.data.partitions:
                    fp.writelines(partition.compute())
                    partition = None
        
        # Write the optional header, each line
        if self.footer:
            with open(self.file, 'a', encoding='utf-8') as fp:
                if self.footer and util.contains_jinja(self.footer):
                    footer_template = util.build_jinja_template(self.earthmover.macros + self.footer)
                    rendered_template = self.render_row(first_row, template_bytecode_file=footer_template, template_string=self.footer, dunder_row_data=True)
                    fp.write(rendered_template.strip("\r\n") + "\n")
                elif self.footer: # no jinja
                    fp.write(self.footer.strip("\r\n") + "\n")

        self.logger.debug(f"output `{self.file}` written")
        self.size = os.path.getsize(self.file)

    @staticmethod
    def apply_render_row(template_string, render_row, x):
        template = util.build_jinja_template(template_string)
        dunder_row_data = '__row_data__' in util.get_jinja_template_params(template_string)
        return x.apply(
            render_row,
            template = template,
            template_string = template_string,
            dunder_row_data = dunder_row_data,
            axis=1)
    
    def render_row(self, row: pd.Series, template, template_string, dunder_row_data=False):
        try:
            json_string = util.render_jinja_template(
                row,
                template,
                template_string,
                dunder_row_data)

        except Exception as err:
            print(err)
            self.error_handler.throw(
                f"error rendering Jinja template in `template` file {self.template} ({err})"
            )
            raise
        
        return json_string
