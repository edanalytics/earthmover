import dataclasses
import os
import yaml

from string import Template
from typing import Dict

from earthmover import util


@dataclasses.dataclass
class YamlMapping(dict):
    __line__: int = None
    __file__: str = None

    def update(self, _dict: 'YamlMapping'):
        """
        Inspired by Tom Reitz's Bifrost (https://github.com/edanalytics/bifrost/blob/main/build-swagger.py).
        Includes merging of line and file dunders.

        :param _dict:
        :return:
        """
        for key, val in _dict.items():
            if key in self and isinstance(self[key], type(self)) and isinstance(val, type(self)):
                self[key] = self[key].update(val)
                self[key].__line__ = val.__line__
                self[key].__file__ = val.__file__
            else:
                self[key] = val
        return self

    def to_dict(self):
        """
        Convert a YAML Mapping to a standard dictionary.
        """
        output_dict: dict = {}

        for key, val in self.items():
            if isinstance(val, (list, tuple)):
                output_dict[key] = list(map(self._recurse_to_dict, val))
            else:
                output_dict[key] = self._recurse_to_dict(val)

        return output_dict

    @staticmethod
    def _recurse_to_dict(item):
        return item.to_dict() if isinstance(item, YamlMapping) else item

    def to_disk(self, path: str):
        """ Write the YamlMapping as a YAML file. """
        with open(path, 'w') as outfile:
            yaml.dump(self.to_dict(), outfile, default_flow_style=False, sort_keys=False)


class JinjaEnvironmentYamlLoader(yaml.SafeLoader):
    """
    Convert the mapping to a YamlMapping in order to store line number internally
        - Allows us to determine the line number for any element loaded from YAML file
        - Very useful for debugging and giving meaningful error messages
        - See https://stackoverflow.com/a/53647080 and https://stackoverflow.com/a/67254800

    Add environment variable interpolation
        - See https://stackoverflow.com/questions/52412297
    """
    num_macros_lines: int = 0
    file: str = None

    def construct_yaml_map(self, node):
        """
        Add line numbers as attribute of pyyaml.Constructor
        - See https://github.com/yaml/pyyaml

        :param node:
        :return:
        """
        data = YamlMapping()  # Originally `data = {}`
        data.__line__ = node.start_mark.line + self.num_macros_lines
        data.__file__ = self.file
        yield data

        value = self.construct_mapping(node)
        data.update(value)

    @classmethod
    def load_config_file(cls, filepath: str, params: Dict[str, str], macros: str) -> YamlMapping:
        """

        :param filepath:
        :param params:
        :param macros:
        :return:
        """
        # Load the YAML filepath and apply environment-variable templating.
        raw_yaml = cls.template_open_filepath(filepath, params)

        # Expand Jinja and complete full parsing
        try:
            raw_yaml = util.build_jinja_template(raw_yaml, macros=macros).render()
            yaml_configs = yaml.load(raw_yaml, Loader=cls)

        except yaml.YAMLError as err:
            linear_error_message = " ".join(
                line.replace("^", "").strip()
                for line in str(err).split("\n")
            )

            raise Exception(
                f"YAML could not be parsed: {linear_error_message}"
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
                "Earthmover version 0.2.x requires `version: 2` be defined in your YAML file!\n"
                "Please add this key and reattempt run."
            )

        return yaml_configs

    @classmethod
    def load_project_configs(cls, filepath: str, params: Dict[str, str]) -> Dict[str, object]:
        """
        Helper method to retrieve user-provided macros and environment vars to apply at full parsing.
        Events are returned element-by-element, so we can rely on certain keywords and datatypes.

        For example:
        ```
        while True:
            node = loader.compose_node(None, None)
            value = loader.construct_object(node, True)
        ```

        yields:
        ```
        version
        2
        config
        {...}
        sources
        {...}
        ...
        ```

        :param filepath:
        :param params:
        :return:
        """
        # Load the YAML filepath and apply environment-variable templating.
        raw_yaml = cls.template_open_filepath(filepath, params)
        loader = yaml.SafeLoader(raw_yaml)

        # Assert the file is properly formatted
        for required_event in (yaml.StreamStartEvent, yaml.DocumentStartEvent, yaml.MappingStartEvent):
            assert loader.check_event(required_event)
            loader.get_event()

        # Parse the file until we hit a dictionary that is not headed by "config".
        project_configs = {}  # Return empty dict if no configs are found
        last_value = None  # Keep track of previous nodes

        while True:
            try:
                node = loader.compose_node(None, None)
                value = loader.construct_object(node, True)
            except Exception:
                break  # If we run into parsing errors, assume we've hit Jinja (and passed the configs).

            if isinstance(value, dict):  # This prevents immediate return on `version: 2`.
                if last_value == "config":
                    project_configs = value
                break  # Presume the first dictionary mapping of the file is the config block

            last_value = value

        macros = project_configs.get('macros', "")
        JinjaEnvironmentYamlLoader.num_macros_lines = len(macros.split("\n"))  # Save line count for accurate logging

        return project_configs

    @staticmethod
    def template_open_filepath(filepath: str, params: Dict[str, str]) -> str:
        """

        :param filepath:
        :param params:
        :return:
        """
        full_params = {**params, **os.environ.copy()}
        full_params = {k: str(v) for k, v in full_params.items()}  # Force values to strings before templating.

        JinjaEnvironmentYamlLoader.file = filepath
        with open(filepath, "r", encoding='utf-8') as stream:
            content_string = stream.read()  # Force to a string to apply templating and expand Jinja

        return Template(content_string).safe_substitute(full_params)


JinjaEnvironmentYamlLoader.add_constructor(
    'tag:yaml.org,2002:map',
    JinjaEnvironmentYamlLoader.construct_yaml_map
)