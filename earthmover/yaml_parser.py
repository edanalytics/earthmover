import os

from dataclasses import dataclass
from yaml import SafeLoader


@dataclass
class YamlMapping(dict):
    __line__: int = None


class SafeLineEnvVarLoader(SafeLoader):
    """
    Convert the mapping to a YamlMapping in order to store line number internally
        - Allows us to determine the line number for any element loaded from YAML file
        - Very useful for debugging and giving meaningful error messages
        - See https://stackoverflow.com/a/53647080 and https://stackoverflow.com/a/67254800

    Add environment variable interpolation
        - See https://stackoverflow.com/questions/52412297
    """
    def construct_mapping(self, node, deep=False):
        """
        Add environment variable interpolation to Constructor.construct_mapping()

        :param node:
        :param deep:
        :return:
        """
        mapping = super().construct_mapping(node, deep=deep)

        #
        for k, v in mapping.copy().items():
            del mapping[k]
            if isinstance(v, str):
                mapping[os.path.expandvars(k)] = os.path.expandvars(v)
            else: mapping[os.path.expandvars(k)] = v

        return mapping

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



SafeLineEnvVarLoader.add_constructor(
    'tag:yaml.org,2002:map',
    SafeLineEnvVarLoader.construct_yaml_map
)
