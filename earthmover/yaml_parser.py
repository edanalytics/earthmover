import jinja2
import hashlib
import logging
import string
import os
import yaml

from dataclasses import dataclass
from string import Template
from yaml import SafeLoader

from earthmover import util


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
        return super().construct_mapping(node, deep=deep)

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
    def load_config_file(cls, filepath: str, params: dict) -> dict:
        """

        :param: params
        :return:
        """

        # pass 1: grab config.macros (if any) so Jinja in the YAML can be rendered with macros
        with open(filepath, "r", encoding='utf-8') as stream:
            # cannot just yaml.load() here, since Jinja in the YAML may make it invalid...
            # instead, pull out just the `config` section, which must not contain Jinja (except for `macros`)
            # then we yaml.load() just the config section to grab any `macros`
            start = None
            end = None

            lines = stream.readlines()
            for idx, line in enumerate(lines):

                # Find the start of the config block.
                if line.startswith("config:"):
                    start = idx
                    continue

                # Find the end of the config block (i.e., the next top-level field)
                if start is not None and not line.startswith(tuple(string.whitespace + "#")):
                    end = idx
                    break

            # Read the configs block and extract the (optional) macros field.
            if start is not None and end is not None:
                configs_pass1 = yaml.safe_load("".join(lines[start:end]))
                macros = configs_pass1.get("config", {}).get("macros", "")
            else:
                configs_pass1 = {}
                macros = ""

            # Figure out lines range of macro definitions, to skip (re)reading/parsing them later
            macros_lines = macros.count("\n")
            macros = macros.strip()

        # pass 2:
        #   (a) load template YAML minus macros (which were already loaded in pass 1)
        #   (b) replace envvars
        #   (c) render Jinja in YAML template
        #   (d) load YAML to config Dict

        # (a)
        config_template_string = "".join(lines)

        # (b)
        _env_backup = os.environ.copy()  # backup envvars
        os.environ.update(params)  # override with CLI params

        for k, v in configs_pass1.get("config", {}).get("parameter_defaults", {}).items():
            if isinstance(v, str):
                os.environ.setdefault(k, v)  # set defaults, if any
            else:
                logging.critical(
                    f"YAML config.parameter_defaults.{k} must be a string"
                )
                raise

        config_template_string = Template(config_template_string).safe_substitute(os.environ)
        os.environ = _env_backup  # restore envvars

        # Uncomment the following to view original template yaml and parsed yaml:
        # with open("./earthmover_template.yml", "w") as f:
        #     f.write(config_template_string)

        # (c)
        try:
            config_yaml = util.build_jinja_template(config_template_string, macros=macros).render()

            # Uncomment the following to view original template yaml and parsed yaml:
            # with open("./earthmover_yaml.yml", "w") as f:
            #     f.write(config_yaml)

        except Exception as err:
            lineno = util.jinja2_template_error_lineno()
            if lineno:
                lineno = ", near line " + str(lineno - macros_lines - 1)
            logging.critical(
                f"Jinja syntax error in YAML configuration template{lineno} ({err})"
            )
            raise

        # (d)
        try:
            configs_pass2 = yaml.load(config_yaml, Loader=cls)
            configs_pass2.get("config", {}).update({"macros": macros})
        except yaml.YAMLError as err:
            linear_err = " ".join([line.replace("^", "").strip() for line in str(err).split("\n")])
            logging.critical(
                f"YAML could not be parsed: {linear_err}"
            )
            raise

        return configs_pass2


SafeLineEnvVarLoader.add_constructor(
    'tag:yaml.org,2002:map',
    SafeLineEnvVarLoader.construct_yaml_map
)