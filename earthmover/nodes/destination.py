import os
import pandas as pd
import polars as pl
import re
import tempfile

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
        # Guard `pd.isna` against array-like values (e.g. nested list/dict columns), which
        # would otherwise raise an ambiguous-truth-value error.
        try:
            if bool(pd.isna(value)):
                return cls.NULL_REPR
        except (ValueError, TypeError):
            pass

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

    # Number of rows materialized at a time when writing output. Bounds peak memory during
    # the write, regardless of total output size.
    WRITE_BATCH_SIZE = 100_000

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.template = self.error_handler.assert_get_key(self.config, 'template', dtype=str, required=False, default=None)
        self.header = self.error_handler.assert_get_key(self.config, 'header', dtype=str, required=False, default=None)
        self.footer = self.error_handler.assert_get_key(self.config, 'footer', dtype=str, required=False, default=None)
        self.linearize = self.error_handler.assert_get_key(self.config, 'linearize', dtype=bool, required=False, default=True)
        self.extension = self.error_handler.assert_get_key(self.config, 'extension', dtype=str, required=False, default='')
        self.jinja_template = None  # Defined in execute()

        # Render Jinja templates from the directory with the config file
        self.config_dir = os.path.dirname(self.config.__file__)

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

            self.jinja_template = util.build_jinja_template(template_string, macros=self.earthmover.macros, base_dir=self.config_dir)

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

        upstream_data = self.upstream_sources[self.source].data
        upstream_data = self.opt_repartition(upstream_data)  # no-op under Polars (warns if set)
        self.data = upstream_data

        # Verify the output directory exists.
        os.makedirs(os.path.dirname(self.file), exist_ok=True)

        # Spill the TRANSFORMED (pre-render) data to a temporary Arrow file using Polars'
        # streaming engine. This step contains no Python UDF, so it streams with bounded peak
        # memory regardless of dataset size. We then read the spilled data back in row-batches
        # and render each batch to output lines in Python, so the per-row Jinja rendering never
        # has to hold more than one batch in memory at a time.
        tmp_dir = self.earthmover.state_configs['tmp_dir']
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".arrow", prefix="earthmover_", dir=tmp_dir)
        os.close(tmp_fd)

        try:
            upstream_data.sink_ipc(tmp_path)
            scan = pl.scan_ipc(tmp_path)
            total_rows = scan.select(pl.len()).collect().item()
            self.num_rows = total_rows

            # Write the optional header, each line, and the optional footer.
            with open(self.file, 'w+', encoding='utf-8') as fp:

                # only load the first row if header/footer contain Jinja that might need it:
                first_row = None
                if (
                    (self.header and util.contains_jinja(self.header))
                    or (self.footer and util.contains_jinja(self.footer))
                ):
                    head_df = scan.head(1).collect()
                    if head_df.height > 0:
                        first_row = head_df.row(0, named=True)
                    else:  # If no rows are present, build a representation of the row with empty values
                        first_row = {col: "" for col in scan.collect_schema().names()}

                if self.header and util.contains_jinja(self.header):
                    jinja_template = util.build_jinja_template(self.header, macros=self.earthmover.macros)
                    rendered_template = self.render_row(first_row, jinja_template=jinja_template)
                    fp.write(rendered_template)
                elif self.header: # no jinja
                    fp.write(self.header)

                offset = 0
                while offset < total_rows:
                    batch = scan.slice(offset, self.WRITE_BATCH_SIZE).collect()
                    fp.writelines(
                        self.render_row(row, jinja_template=self.jinja_template)
                        for row in batch.iter_rows(named=True)
                    )
                    offset += self.WRITE_BATCH_SIZE
                    batch = None  # Release the batch from memory immediately after write.

                if self.footer and util.contains_jinja(self.footer):
                    jinja_template = util.build_jinja_template(self.footer, macros=self.earthmover.macros)
                    rendered_template = self.render_row(first_row, jinja_template)
                    fp.write(rendered_template)
                elif self.footer: # no jinja
                    fp.write(self.footer)

        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

        self.logger.debug(f"output `{self.file}` written")
        self.size = os.path.getsize(self.file)

    def render_row(self, row, jinja_template):
        row_data = row if isinstance(row, dict) else row.to_dict()
        row_data = {
            field: self.cast_output_dtype(value)
            for field, value in row_data.items()
        }
        row_data["__row_data__"] = row_data

        try:
            json_string = jinja_template.render(row_data) + "\n"

        except Exception as err:
            print(err)
            self.error_handler.throw(
                f"error rendering Jinja template in `template` file {self.template} ({err})"
            )
            raise

        return json_string
