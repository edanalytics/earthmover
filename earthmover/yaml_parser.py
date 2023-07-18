import logging
import os
import yaml

from dataclasses import dataclass
from string import Template

from earthmover import util


@dataclass
class YamlMapping(dict):
    __line__: int = None


class YamlEnvironmentJinjaLoader(yaml.SafeLoader):
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

    @classmethod
    def load_config_file(cls, filepath: str, params: dict) -> YamlMapping:
        """

        :param filepath:
        :param params:
        :return:
        """
        cls.params = {**os.environ.copy(), **params}

        # Load configs string and parse the project configs block if present.
        with open(filepath, "r", encoding='utf-8') as stream:
            raw_config_string = stream.read()  # Force to a string for new EnvironmentJinjaReader constructor

        cls.parse_project_configs(raw_config_string)
        # print(cls.macros)
        # print(cls.params)

        # Complete full parsing with environmental vars and macros added.
        try:
            # print("#1 BEFORE PARSING\n", raw_config_string, "\n")
            raw_config_string = Template(raw_config_string).safe_substitute(cls.params)
            # print("#2 AFTER ENV_VARS\n", raw_config_string, "\n")
            raw_config_string = util.build_jinja_template(raw_config_string, macros=cls.macros).render()
            # print("#3 AFTER_JINJA\n", raw_config_string, "\n")

            yaml_configs = yaml.load(raw_config_string, Loader=cls)

        except yaml.YAMLError as err:
            raise Exception(
                f"YAML could not be parsed: {cls.linearize_error(err)}"
            )

        except Exception as err:
            lineno = util.jinja2_template_error_lineno()
            if lineno:
                lineno = ", near line " + str(lineno)

            raise Exception(
                f"Jinja syntax error in YAML configuration template{lineno} ({err})"
            )

        # Force version 2 check to ensure consistency across Earthmover versions.
        if yaml_configs.get('version') != 2:
            raise Exception(
                "Earthmover version 1.x requires `version: 2` be defined in your YAML file!\n"
                "Please add this key and reattempt run."
            )

        return yaml_configs

    @classmethod
    def parse_project_configs(cls, stream):
        """
        Helper method to retrieve user-provided macros and environment vars to apply before parsing.

        :param stream:
        :return:
        """
        mapping_depth = 0
        in_config_mapping = False
        config_events = []

        # Parse the file into events until a non-config element is reached
        for event in yaml.parse(stream, Loader=yaml.SafeLoader):
            # print(event, mapping_depth)
            if isinstance(event, yaml.events.MappingStartEvent):
                mapping_depth += 1
            elif isinstance(event, yaml.events.MappingEndEvent):
                mapping_depth -= 1

            # Configs are top-level (i.e., in the first nested dictionary of the parse)
            if mapping_depth == 1 and hasattr(event, 'value') and event.value == "config":
                in_config_mapping = True

            if in_config_mapping:
                config_events.append(event)

            # Exit the mapping dict
            if mapping_depth == 1 and hasattr(event, 'value') and event.value == 'sources':
                break

        if not config_events:
            # print("No config mapping found!")
            return  # End early if no configs are defined

        # Convert the events back into a mapping, then parse.
        config_events = list([
            yaml.events.StreamStartEvent, yaml.events.MappingStartEvent,
            *config_events,
            yaml.events.MappingEndEvent, yaml.events.StreamEndEvent
        ])
        print(config_events)
        config_events_mapping = yaml.load(yaml.emit(config_events), Loader=yaml.SafeLoader)

        # Retrieve and set macros and parameter defaults
        if "macros" in config_events_mapping["config"]:
            cls.macros = config_events_mapping["config"]["macros"]

        if "parameter_defaults" in config_events_mapping["config"]:
            for k, v in config_events_mapping["config"]["parameter_defaults"].items():
                if isinstance(v, str):
                    cls.params.setdefault(k, v)  # set defaults, if any
                else:
                    raise TypeError(
                        f"YAML config.parameter_defaults.{k} must be a string"
                    )

    @staticmethod
    def linearize_error(error_message) -> str:
        return " ".join(
            line.replace("^", "").strip()
            for line in str(error_message).split("\n")
        )


YamlEnvironmentJinjaLoader.add_constructor(
    'tag:yaml.org,2002:map',
    YamlEnvironmentJinjaLoader.construct_yaml_map
)