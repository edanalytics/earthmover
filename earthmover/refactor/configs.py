import os
import yaml

from yaml.loader import SafeLoader

from earthmover.refactor.error_handler import ErrorHandler


# This allows us to determine the YAML file line number for any element loaded from YAML
# (very useful for debugging and giving meaningful error messages)
# (derived from https://stackoverflow.com/a/53647080)
# Also added env var interpolation based on
# https://stackoverflow.com/questions/52412297/how-to-replace-environment-variable-value-in-yaml-file-to-be-parsed-using-python#answer-55301129
class SafeLineEnvVarLoader(SafeLoader):

    def construct_mapping(self, node, deep=False):
        mapping = super().construct_mapping(node, deep=deep)

        # expand env vars:
        for k, v in mapping.items():
            if isinstance(v, str):
                mapping[k] = os.path.expandvars(v)

        # Add 1 so line numbering starts at 1
        mapping['__line__'] = node.start_mark.line + 1
        return mapping



class UserConfigs:
    """

    """
    config_defaults = {
        "state_file": os.path.join(os.path.expanduser("~"), ".earthmover.csv"),
        "output_dir": "./",
        "macros": "",
        "show_graph": False,
        "log_level": "INFO",
        "show_stacktrace": False
    }

    def __init__(self, config_file: str, params: dict, error_handler: ErrorHandler):
        self.config_file = config_file
        self.params = params
        self.error_handler = error_handler

        self.user_configs = self.load_config_file()


    def load_config_file(self):
        """

        :return:
        """
        _env_backup = os.environ.copy()

        # Load & parse config YAML (using modified environment vars)
        os.environ.update(self.params)

        with open(self.config_file, "r") as stream:
            try:
                configs = yaml.load(stream, Loader=SafeLineEnvVarLoader)
            except yaml.YAMLError as err:
                raise Exception(self.error_handler.ctx + f"YAML could not be parsed: {err}")

        # Return environment to original backup
        os.environ = _env_backup

        return configs


    def get_state_configs(self):
        """

        :return:
        """
        custom_configs = self.user_configs.get('config', {})
        state_configs = {**self.config_defaults, **custom_configs}

        state_configs['state_file'] = os.path.expanduser(state_configs['state_file'])
        state_configs['output_dir'] = os.path.expanduser(state_configs['output_dir'])
        state_configs['macros'    ] = state_configs['macros'].strip()
        state_configs['log_level' ] = state_configs['log_level'].upper()

        return state_configs


    def get_sources(self):
        self.error_handler.assert_key_exists_and_type_is(self.user_configs, 'sources', dict)
        return self.user_configs['sources']


    def get_transformations(self):
        # Transformations are optional.
        if "transformations" in self.user_configs:
            self.error_handler.assert_key_type_is(self.user_configs, 'transformations', dict)
            return self.user_configs['transformations']
        else:
            return {}


    def get_destinations(self):
        self.error_handler.assert_key_exists_and_type_is(self.user_configs, 'destinations', dict)
        return self.user_configs['destinations']
