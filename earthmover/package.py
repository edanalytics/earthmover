import git
import os
import shutil
from typing import Optional, TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.earthmover import Earthmover
    from earthmover.yaml_parser import YamlMapping
    from earthmover.error_handler import ErrorHandler
from logging import Logger
from earthmover.yaml_parser import JinjaEnvironmentYamlLoader

class Package:
    """

    """
    mode: str = None

    def __new__(cls, name: str, config: 'YamlMapping', *, earthmover: 'Earthmover', package_path: Optional[str] = None):
        """
        Logic for assigning packages to their respective classes.

        :param name:
        :param config:
        :param earthmover:
        :param package_path:
        """
        if 'local' in config:
            return object.__new__(LocalPackage)

        elif 'git' in config:
            return object.__new__(GitHubPackage)
        
        elif name == 'root':
            return object.__new__(RootPackage)

        else:
            earthmover.error_handler.throw(
                "packages must specify either a `local` folder path or a `git` package URL"
            )
            raise
    
    def __init__(self, name: str, config: 'YamlMapping', *, earthmover: 'Earthmover', package_path: Optional[str] = None):
        self.name: str = name
        self.config: 'YamlMapping' = config
        self.earthmover: 'Earthmover' = earthmover
        self.package_path: str = package_path

        self.logger: 'Logger' = earthmover.logger
        self.error_handler: 'ErrorHandler' = earthmover.error_handler

        self.package_yaml: dict = None


    def install(self, packages_dir):
        """

        """
        self.logger.info(f"installing '{self.name}'...")
        self.package_path = os.path.join(packages_dir, self.name)

        if os.path.lexists(self.package_path):
            if not os.path.islink(self.package_path):
                shutil.rmtree(self.package_path)
            else:
                os.remove(self.package_path)


    def get_installed_config_file(self):
        """
        Find the Earthmover config file for the installed package.
        TODO: allow the config filepath to be specified for the package rather than requiring a default location and name
        """
        if not os.path.isdir(self.package_path):
            self.error_handler.throw(
                f"The package '{self.name}' has not been installed. Run an 'earthmover deps' command to install packages."
            )
        
        for file in ['earthmover.yaml', 'earthmover.yml']:
            test_file = os.path.join(self.package_path, file)
            if os.path.isfile(test_file):
                return test_file

        self.error_handler.throw(
            f"Config file not found for package '{self.name}'. Ensure the package has a file named 'earthmover.yaml' or 'earthmover.yml' in the root directory."
        )
        raise


    def load_package_yaml(self, params: dict, macros: str):
        """
        Loads the config file and replaces all relative filepaths with absolute paths based on the location of the package and stores the resulting mapping in the package_yaml variable.

        TODO: This function is pretty 'brute force' in that it relies on a known list of key names and operations types (map_values) to find all file paths. Can we make it more robust to 
        ensure it doesn't break if additional keys are used for filepaths in future refactors or additions to earthmover?
        """
        config_file = self.get_installed_config_file()
        yaml_mapping = JinjaEnvironmentYamlLoader.load_config_file(config_file, params, macros)

        # Replace relative paths with absolute paths to installed package location for any filepaths in source or destinations
        for node_type in ('sources', 'destinations'):
            for node in self.error_handler.assert_get_key(yaml_mapping, node_type, dtype=dict, required=False, default={}):
                # These are the current key names that denote filepaths
                for file_config in ('file', 'template'):
                    filepath = self.error_handler.assert_get_key(yaml_mapping[node_type][node], file_config, dtype=str, required=False, default=None)
                    if filepath and not os.path.isabs(filepath):
                        yaml_mapping[node_type][node][file_config] = os.path.join(self.package_path, filepath.strip('./'))

        # Replace relative paths for any map files in map_values operations
        for transformation in self.error_handler.assert_get_key(yaml_mapping, 'transformations', dtype=dict, required=False, default={}):
            operation_num = 0
            for operation in self.error_handler.assert_get_key(yaml_mapping['transformations'][transformation], 'operations', dtype=list, required=False, default=[]):
                if operation['operation'] == 'map_values' and 'map_file' in operation and not os.path.isabs(operation['map_file']):
                    yaml_mapping['transformations'][transformation]['operations'][operation_num]['map_file'] = os.path.join(self.package_path, operation['map_file'].strip('./'))
                operation_num += 1

        self.package_yaml = yaml_mapping

        return self.package_yaml


class RootPackage(Package):
    """

    """
    mode: str = 'root'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class LocalPackage(Package):
    """

    """
    mode: str = 'local'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def install(self, packages_dir):
        """
        Makes a copy of a local package directory into <project>/packages.
        :return:
        """
        super().install(packages_dir)

        source_path = self.error_handler.assert_get_key(self.config, 'local', dtype=str, required=False)

        if not os.path.exists(source_path):
            self.error_handler.throw(
                f"Local package '{self.name}' not found: verify that the path is correct"
            )
            raise

        shutil.copytree(source_path, self.package_path)

        return super().get_installed_config_file()


class GitHubPackage(Package):
    """

    """
    mode: str = 'git'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def install(self, packages_dir):
        """
        Clones a GitHub repository into <project>/packages.
        If a subdirectory is specified, clone the repository into a temporary folder and copy the desired subdirectory into <project>/packages.
        :return:
        """
        super().install(packages_dir)

        source_path = self.error_handler.assert_get_key(self.config, 'git', dtype=str, required=False)
        subdirectory = self.error_handler.assert_get_key(self.config, 'subdirectory', dtype=str, required=False, default=None)
        branch = self.error_handler.assert_get_key(self.config, 'branch', dtype=str, required=False, default=None)

        tmp_package_path = os.path.join(packages_dir, 'tmp_git')
        os.mkdir(tmp_package_path)

        if branch:
            repo = git.Repo.clone_from(source_path, tmp_package_path, branch=branch)
        else:  #If branch is not specified, default working branch is used
            repo = git.Repo.clone_from(source_path, tmp_package_path)

        if subdirectory: # Avoids the package being nested in folders
            subdirectory_path = os.path.join(repo.working_tree_dir, subdirectory)
            shutil.copytree(subdirectory_path, self.package_path)
        else:
            shutil.copytree(repo.working_tree_dir, self.package_path)

        git.rmtree(repo.working_tree_dir)

        return super().get_installed_config_file()