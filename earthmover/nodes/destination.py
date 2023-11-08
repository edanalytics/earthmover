import jinja2
import os
import polars as pl
import re
import shutil
import tempfile

from functools import partial

from earthmover.node import Node
from earthmover import util

from typing import Tuple


class Destination(Node):
    """

    """
    type: str = 'destination'
    mode: str = None  # Documents which class was chosen.
    allowed_configs: Tuple[str] = ('debug', 'expect', 'show_progress', 'repartition', 'source',)

    def __new__(cls, *args, **kwargs):
        return object.__new__(FileDestination)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source: str = self.error_handler.assert_get_key(self.config, 'source', dtype=str)
        self.upstream_sources[self.source] = None


class FileDestination(Destination):
    """

    """
    mode: str = 'file'
    allowed_configs: Tuple[str] = (
        'debug', 'expect', 'show_progress', 'repartition', 'source',
        'template', 'extension', 'linearize', 'header', 'footer',
    )

    EXP = re.compile(r"\s+")
    TEMPLATED_COL = "____OUTPUT____"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file: str = None
        self.template: str = None
        self.jinja_template: jinja2.Template = None
        self.header: str = None
        self.footer: str = None

    def compile(self):
        """

        :return:
        """
        super().compile()
        self.template = self.error_handler.assert_get_key(self.config, 'template', dtype=str)

        #config->extension is optional: if not present, we assume the destination name has an extension
        extension = ""
        if "extension" in self.config:
            extension = f".{self.config['extension']}"
            
        self.file = os.path.join(
            self.earthmover.state_configs['output_dir'],
            f"{self.name}{extension}"
        )

        #
        try:
            with open(self.template, 'r', encoding='utf-8') as fp:
                template_string = fp.read()

        except Exception as err:
            self.error_handler.throw(
                f"`template` file {self.template} cannot be opened ({err})"
            )
            raise

        #
        if self.config.get('linearize', True):
            template_string = self.EXP.sub(" ", template_string)  # Replace multiple spaces with a single space.

        if 'header' in self.config:
            self.header = self.config["header"]

        if 'footer' in self.config:
            self.footer = self.config["footer"]

        #
        try:
            self.jinja_template = util.build_jinja_template(template_string, macros=self.earthmover.macros)

        except Exception as err:
            self.earthmover.error_handler.throw(
                f"syntax error in Jinja template in `template` file {self.template} ({err})"
            )
            raise

    def execute(self, **kwargs):
        """
        There is a bug in Dask where where `dd.to_csv(mode='a', single_file=True)` fails.
        This is resolved in 2023.8.1: https://docs.dask.org/en/stable/changelog.html#id7 

        :return:
        """
        super().execute(**kwargs)

        apply_template = partial(self.render_jinja_template,
            template=self.jinja_template,
            template_str=self.template,
            error_handler=self.error_handler,
        )

        self.data = (
            self.upstream_sources[self.source].data
                .fill_nan('').fill_null('')
                .select(
                    pl.struct(pl.all()).map_elements(function=apply_template, return_dtype=str).alias(self.TEMPLATED_COL),
                )
        )

        # Repartition before writing, if specified.
        self.data = self.opt_repartition(self.data)

        # Verify the output directory exists.
        os.makedirs(os.path.dirname(self.file), exist_ok=True)

        # TODO: Make this less jank!
        # One must sink using a Write operation, so headers require silly roundabout logic.
        if not self.header:
            self.data.sink_csv(
                path=self.file,
                has_header=False,
                separator="\x02",
                quote_style='never',
                maintain_order=False,
            )

        else:
            with open(self.file, mode="w") as fp:
                fp.write(self.header)

            temp_sink_file = tempfile.NamedTemporaryFile()

            self.data.sink_csv(
                path=temp_sink_file.name,
                has_header=False,
                separator="\x02",
                quote_style='never',
                maintain_order=False,
            )

            # Rewrite the file contents to its correct location.
            # https://stackoverflow.com/a/14947384
            # https://stackoverflow.com/a/15235559
            with open(temp_sink_file.name, 'r') as in_fp:
                with open(self.file, 'a') as out_fp:
                    shutil.copyfileobj(in_fp, out_fp)

        if self.footer:
            with open(self.file, 'a', encoding='utf-8') as fp:
                fp.write(self.footer)

        self.logger.debug(f"output `{self.file}` written")
        self.size = os.path.getsize(self.file)
