import abc
import os
import json
import jinja2
import re
from dask.diagnostics import ProgressBar

from earthmover.node import Node
from earthmover import util

class Destination(Node):
    """

    """
    def __new__(cls, *args, **kwargs):
        return object.__new__(FileDestination)


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type = 'destination'

        self.allowed_configs.update(['source'])

        self.mode = None  # Documents which class was chosen.

        # Should this be moved to compile?
        self.source = self.error_handler.assert_get_key(self.config, 'source', dtype=str)


    @abc.abstractmethod
    def compile(self):
        """

        :return:
        """
        super().compile()
        pass


    @abc.abstractmethod
    def execute(self):
        """

        :return:
        """
        super().execute()
        self.data = self.get_source_node(self.source).data

        pass



class FileDestination(Destination):
    """

    """
    EXP = re.compile(r"\s+")

    TEMPLATED_COL = "____OUTPUT____"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = 'file'

        self.allowed_configs.update(['template', 'extension', 'linearize', 'header', 'footer'])

        self.file = None
        self.template = None
        self.jinja_template = None
        self.header = None
        self.footer = None


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
            self.jinja_template = jinja2.Environment(
                    loader=jinja2.FileSystemLoader(os.path.dirname('./'))
                ).from_string(self.earthmover.state_configs['macros'] + template_string)

        except Exception as err:
            self.earthmover.error_handler.throw(
                f"syntax error in Jinja template in `template` file {self.template} ({err})"
            )
            raise

        self.jinja_template.globals['md5'] = util.jinja_md5


    def execute(self):
        """

        :return:
        """
        super().execute()

        self.data = self.data.fillna('')

        os.makedirs(os.path.dirname(self.file), exist_ok=True)
        with open(self.file, 'w', encoding='utf-8') as fp:

            if self.header:
                fp.write(self.header + "\n")
            
            def render(row):
                _data_tuple = row.to_dict()
                _data_tuple["__row_data__"] = row

                try:
                    json_string = self.jinja_template.render(_data_tuple)

                except Exception as err:
                    self.error_handler.throw(
                        f"error rendering Jinja template in `template` file {self.template} ({err})"
                    )
                    raise

                fp.write(json_string + "\n")
                # really we don't need this data anymore, but apply() expects _somthing_ to be returned
                # so let's return just the first element of the row
                return row[list(_data_tuple.keys())[0]]

            with ProgressBar():
                self.logger.debug(f"writing output file `{self.file}`...")
                # this renders each row without having to itertupes() (which is much slower)
                # (meta=... below is how we prevent dask warnings that it can't infer the output data type)
                self.data.map_partitions(lambda x: x.apply(render, axis=1), meta=(self.data.columns[0], str)).compute()

            if self.footer:
                fp.write(self.footer)

        self.logger.debug(f"output `{self.file}` written")
        self.size = os.path.getsize(self.file)
