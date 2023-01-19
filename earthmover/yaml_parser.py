import os

from yaml import SafeLoader



class YamlMapping(dict):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__line__ = None


class SafeLineEnvVarLoader(SafeLoader):
    """
    This allows us to determine the YAML file line number for any element loaded from YAML
    (very useful for debugging and giving meaningful error messages)
    (derived from https://stackoverflow.com/a/53647080)

    Also added env var interpolation based on
    https://stackoverflow.com/questions/52412297
    """
    def construct_mapping(self, node, deep=False):
        mapping = super().construct_mapping(node, deep=deep)

        # expand env vars:
        for k, v in mapping.items():
            if isinstance(v, str):
                mapping[k] = os.path.expandvars(v)

        # Convert the mapping to a YamlMapping in order to store line number internally.
        # Add 1 so line numbering starts at 1
        mapping = YamlMapping(mapping)
        mapping.__line__ = node.start_mark.line + 1
        return mapping