import abc
import os
import jinja2
import re

from earthmover.refactor.nodes.node import Node


class Destination(Node):
    """

    """
    def __new__(cls, *args, **kwargs):
        return object.__new__(FileDestination)


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.type = 'destination'
        self.mode = None  # Documents which class was chosen.

        self.source = None
        self.template = None
        self.jinja_template = None

        self.data = None


    @abc.abstractmethod
    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, "source", str)
        self.source = self.config['source']

        self.error_handler.assert_key_exists_and_type_is(self.config, "template", str)
        self.template = self.config['template']

        pass


    @abc.abstractmethod
    def execute(self):
        """

        :return:
        """
        super().execute()
        self.data = self.earthmover.graph.ref(self.source).data

        pass



class FileDestination(Destination):
    """

    """
    EXP = re.compile(r"\s+")

    TEMPLATED_COL = "____OUTPUT____"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = 'file'

        self.file_path = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, "extension", str)
        self.file_path = os.path.join(
            self.earthmover.state_configs['output_dir'],
            f"{self.name}.{self.config['extension']}"
        )

        #
        try:
            with open(self.template, 'r') as fp:
                template_string = fp.read()

        except Exception as err:
            self.error_handler.throw(
                f"`template` file {self.template} cannot be opened ({err})"
            )
            raise  # Avoids linter reference-before-assignment of `value`

        #
        if 'linearize' in self.config:
            template_string = template_string.replace("\n", "")
            template_string = template_string.replace("\r", "")
            template_string = template_string.strip()
            template_string = self.EXP.sub(" ", template_string).strip()

        #
        try:
            self.jinja_template = jinja2.Environment(
                    loader=jinja2.FileSystemLoader(os.path.dirname('/'))
                ).from_string(self.earthmover.state_configs['macros'] + template_string)

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

        self.data = self.data.fillna('')

        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        with open(self.file_path, 'w') as fp:

            # TODO: Verify this does not load everything into memory.
            for row_data in self.data.itertuples(index=False):
                _data_tuple = row_data._asdict().items()
                try:
                    json_string = self.jinja_template.render(_data_tuple)

                except Exception as err:
                    self.error_handler.throw(
                        f"error rendering Jinja template in `template` file {self.template} ({err})"
                    )
                    raise

                fp.write(json_string + "\n")

        self.is_done = True
        self.logger.debug(f"output `{self.file_path}` written")
