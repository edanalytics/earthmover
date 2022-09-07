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


    @abc.abstractmethod
    def compile(self):
        self.error_handler.ctx.update(
            file=self.earthmover.config_file, line=self.config['__line__'], node=self, operation=None
        )

        self.error_handler.assert_key_exists_and_type_is(self.config, "source", str)
        self.source = self.config['source']

        self.error_handler.assert_key_exists_and_type_is(self.config, "template", str)
        self.template = self.config['template']

        pass


    @abc.abstractmethod
    def execute(self):
        self.error_handler.ctx.update(
            file=self.earthmover.config_file, line=self.config["__line__"], node=self, operation=None
        )
        pass


class FileDestination(Destination):
    """

    """
    EXP = re.compile(r"\s+")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = 'file'

        self.output_dir = self.earthmover.config['output_dir']  # This is guaranteed define via defaults.
        self.extension = None

        self.template = None
        self.jinja_template = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, "extension", str)
        self.extension = self.config['extension']

        self.error_handler.assert_key_exists_and_type_is(self.config, "template", str)
        self.template = self.config['template']

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
                ).from_string(self.earthmover.config['macros'] + template_string)

        except Exception as err:
            self.earthmover.error_handler.throw(
                f"syntax error in Jinja template in `template` file {self.template} ({err})"
            )


    def execute(self):
        """

        :return:
        """
        super().execute()

        target = self.earthmover.graph.ref(self.source)
        target.data.fillna('', inplace=True)

        #
        file_name = os.path.join(
            self.output_dir, f"{self.name}.{self.extension}"
        )

        os.makedirs(os.path.dirname(file_name), exist_ok=True)

        with open(file_name, 'w') as fp:
            for row in target.data.to_records(index=False):

                row_data = list(zip(target.data.columns, row))

                try:
                    json_string = self.jinja_template.render(row_data)

                except Exception as err:
                    self.error_handler.throw(
                        f"error rendering Jinja template in `template` file {self.template} ({err})"
                    )
                    raise

                fp.write(json_string + "\n")

        self.is_done = True
        self.logger.debug(f"output `{file_name}` written")
