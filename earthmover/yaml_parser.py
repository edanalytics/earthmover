import logging
import os
import yaml

from dataclasses import dataclass
from string import Template
from yaml import SafeLoader
from yaml.reader import Reader

from earthmover import util


@dataclass
class YamlMapping(dict):
    __line__: int = None


class EnvironmentJinjaReader(Reader):
    def __init__(self, stream):

        try:
            stream = Template(stream).safe_substitute(self.params)
            stream = util.build_jinja_template(stream, macros=self.macros).render()

        except Exception as err:
            lineno = util.jinja2_template_error_lineno()

            if lineno:
                lineno = ", near line " + str(lineno)

            logging.critical(
                f"Jinja syntax error in YAML configuration template{lineno} ({err})"
            )
            raise

        super().__init__(stream)


class YamlJinjaLoader(EnvironmentJinjaReader, SafeLoader):
    """
    Convert the mapping to a YamlMapping in order to store line number internally
        - Allows us to determine the line number for any element loaded from YAML file
        - Very useful for debugging and giving meaningful error messages
        - See https://stackoverflow.com/a/53647080 and https://stackoverflow.com/a/67254800

    Add environment variable interpolation
        - See https://stackoverflow.com/questions/52412297
    """
    params: dict = {}
    macros: str = ""

    def construct_yaml_map(self, node):
        """
        Add line numbers as attribute of pyyaml.Constructor
        - See https://github.com/yaml/pyyaml

        :param node:
        :return:
        """
        data = YamlMapping()  # Originally `data = {}`
        data.__line__ = node.start_mark.line + 1  # Start line numbering at 1
        yield data

        value = self.construct_mapping(node)
        data.update(value)


    @classmethod
    def load_config_file(cls, filepath: str, params: dict) -> (YamlMapping, YamlMapping):
        """
        TODO: I jerryrig this function by splitting the input into two pseudo-YAML documents before parsing:
              the project configs and the node configs. I'd prefer to use the YAML loader directly to parse these.

        :param filepath:
        :param params:
        :return:
        """
        cls.params = {**os.environ.copy(), **params}

        ### Read the full configs and split into "header" and "node" configs.
        with open(filepath, "r", encoding='utf-8') as stream:
            raw_configs_string = stream.read()

        if not len(raw_configs_string.split('---')) >= 3:
            raise Exception(
                "Earthmover 1.x requires the config-level parameters to be wrapped in triple dashes (---).\n"
                "Please verify your YAML file adheres to this requirement before reattempting run."
            )

        _, header_configs_string, node_configs_string = raw_configs_string.split('---', 2)

        ### Load in header config block first to parse `version`, `macros`, and `parameter_defaults`
        header_configs = yaml.load(header_configs_string, Loader=cls)

        if header_configs.get('version') != 2:
            raise Exception(
                "Earthmover version 1.x requires `version: 2` be defined in your YAML file!\n"
                "Please add this key and reattempt run."
            )

        project_configs = header_configs.get("config", {})
        cls.macros = project_configs.get("macros", "").strip()

        # Add parameter defaults to class params
        for k, v in project_configs.get("parameter_defaults", {}).items():
            if isinstance(v, str):
                cls.params.setdefault(k, v)  # set defaults, if any
            else:
                logging.critical(
                    f"YAML config.parameter_defaults.{k} must be a string"
                )

        ### Load in the rest of the template, applying Jinja templating before streaming to the parser
        try:
            node_configs = yaml.load(node_configs_string, Loader=cls)

        except yaml.YAMLError as err:
            linear_err = " ".join(
                line.replace("^", "").strip()
                for line in str(err).split("\n")
            )
            logging.critical(
                f"YAML could not be parsed: {linear_err}"
            )
            raise

        return project_configs, node_configs


YamlJinjaLoader.add_constructor(
    'tag:yaml.org,2002:map',
    YamlJinjaLoader.construct_yaml_map
)