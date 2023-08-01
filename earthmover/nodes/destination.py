import os
import jinja2
import re

from earthmover.node import Node
from earthmover import util

class Destination(Node):
    """

    """
    type: str = 'destination'
    mode: str = None  # Documents which class was chosen.
    allowed_configs: tuple = ('debug', 'expect', 'source',)

    def __new__(cls, *args, **kwargs):
        return object.__new__(FileDestination)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source = self.error_handler.assert_get_key(self.config, 'source', dtype=str)
        self.upstream_sources[self.source] = None


class FileDestination(Destination):
    """

    """
    mode: str = 'file'
    allowed_configs: tuple = (
        'debug', 'expect', 'source',
        'template', 'extension', 'linearize', 'header', 'footer',
    )

    EXP = re.compile(r"\s+")
    TEMPLATED_COL = "____OUTPUT____"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file: str = None
        self.template: str = None
        self.jinja_template: str = None
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

    def execute(self):
        """

        :return:
        """
        super().execute()

        self.data = self.upstream_sources[self.source].data.copy().fillna('')

        os.makedirs(os.path.dirname(self.file), exist_ok=True)
        with open(self.file, 'w', encoding='utf-8') as fp:

            if self.header:
                fp.write(self.header + "\n")

            for row_data in self.data.itertuples(index=False):
                _data_tuple = dict(row_data._asdict().items())
                _data_tuple["__row_data__"] = row_data._asdict()

                try:
                    json_string = self.jinja_template.render(_data_tuple)

                except Exception as err:
                    self.error_handler.throw(
                        f"error rendering Jinja template in `template` file {self.template} ({err})"
                    )
                    raise

                fp.write(json_string + "\n")

            if self.footer:
                fp.write(self.footer)

        self.logger.debug(f"output `{self.file}` written")
        self.size = os.path.getsize(self.file)
