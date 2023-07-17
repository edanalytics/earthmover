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
    """
    Overload yaml.Reader to substitute environment variables and process Jinja before parsing.
    This class acts as a Mixin and references class attributes `params` and `macros` of YamlJinjaLoader.
    """
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
    # Set class variables that update during parsing
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


    def construct_mapping(self, node, deep=False):
        """
        Immediately after constructing the mapping, search for and update config macros and parameter_defaults.
        TODO: There may be a more efficient way to use this such that *every* mapping need not be searched.

        :param node:
        :param deep:
        :return:
        """
        mapping = super().construct_mapping(node, deep=deep)

        # Dynamically unpack macros and parameter defaults
        if "config" in mapping:
            if "macros" in mapping["config"]:
                self.macros += mapping["config"]["macros"] + "\n"

            if "parameter_defaults" in mapping["config"]:
                for k, v in mapping["config"]["parameter_defaults"].items():
                    if isinstance(v, str):
                        self.params.setdefault(k, v)  # set defaults, if any
                    else:
                        raise TypeError(
                            f"YAML config.parameter_defaults.{k} must be a string"
                        )

        return mapping


    @classmethod
    def load_config_file(cls, filepath: str, params: dict) -> YamlMapping:
        """

        :param filepath:
        :param params:
        :return:
        """
        cls.params = {**os.environ.copy(), **params}

        # Load configs string and parse using custom classes.
        with open(filepath, "r", encoding='utf-8') as stream:
            raw_config_string = stream.read()  # Force to a string for new EnvironmentJinjaReader constructor

        try:
            yaml_configs = yaml.load(raw_config_string, Loader=cls)

        except yaml.YAMLError as err:
            linear_err = " ".join(
                line.replace("^", "").strip()
                for line in str(err).split("\n")
            )
            logging.critical(
                f"YAML could not be parsed: {linear_err}"
            )
            raise

        # Force version 2 check to ensure consistency across Earthmover versions.
        if yaml_configs.get('version') != 2:
            raise Exception(
                "Earthmover version 1.x requires `version: 2` be defined in your YAML file!\n"
                "Please add this key and reattempt run."
            )

        return yaml_configs


YamlJinjaLoader.add_constructor(
    'tag:yaml.org,2002:map',
    YamlJinjaLoader.construct_yaml_map
)