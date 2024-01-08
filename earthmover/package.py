import git
import os
import shutil
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.earthmover import Earthmover
    from earthmover.yaml_parser import YamlMapping
    from earthmover.error_handler import ErrorHandler
    from logging import Logger

class Package:
    """

    """
    mode: str = None

    def __new__(cls, name: str, config: 'YamlMapping', *, earthmover: 'Earthmover'):
        """
        Logic for assigning packages to their respective classes.

        :param name:
        :param config:
        :param earthmover:
        """
        if 'local' in config:
            return object.__new__(LocalPackage)

        elif 'git' in config:
            return object.__new__(GitHubPackage)

        else:
            earthmover.error_handler.throw(
                "packages must specify either a `local` folder path or a `git` package URL"
            )
            raise
    
    def __init__(self, name: str, config: 'YamlMapping', *, earthmover: 'Earthmover'):
        self.name: str = name
        self.config: 'YamlMapping' = config
        self.earthmover: 'Earthmover' = earthmover

        self.logger: 'Logger' = earthmover.logger
        self.error_handler: 'ErrorHandler' = earthmover.error_handler

        self.destination_path: str = None
        self.package_config_file: str = None

    def install(self, packages_dir):
        """

        """
        self.logger.info(f"installing '{self.name}'...")
        self.destination_path = os.path.join(packages_dir, self.name)

        if os.path.lexists(self.destination_path):
            if not os.path.islink(self.destination_path):
                shutil.rmtree(self.destination_path)
            else:
                os.remove(self.destination_path)

    def installed_package_config(self):
        """
        Find the Earthmover config file for the installed package.
        TODO: allow the config filepath to be specified for the package rather than requiring a default location and name
        """
        for file in ['earthmover.yaml', 'earthmover.yml']:
            test_file = os.path.join(self.destination_path, file)
            if os.path.isfile(test_file):
                self.package_config_file = test_file
                return

        self.error_handler.throw(
            f"config file not found for package '{self.name}'. Ensure the package has a file named 'earthmover.yaml' or 'earthmover.yml' in the root directory."
        )
        raise


class LocalPackage(Package):
    """

    """
    mode: str = 'local'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def install(self, packages_dir):
        """
        Creates a symlink (if allowed) or makes a copy of a local package directory into <project>/packages.
        :return:
        """
        super().install(packages_dir)

        source_path = self.error_handler.assert_get_key(self.config, 'local', dtype=str, required=False)

        if not os.path.exists(source_path):
            self.error_handler.throw(
                f"Local package '{self.name}' not found: verify that the path is correct"
            )
            raise

        try:
            os.symlink(source_path, self.destination_path, target_is_directory=True)
        except OSError:
            shutil.copytree(source_path, self.destination_path)

        super().installed_package_config()

        return self.package_config_file


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

        if 'subdirectory' in self.config:
            subdirectory = self.error_handler.assert_get_key(self.config, 'subdirectory', dtype=str, required=False)

            tmp_destination_path = os.path.join(packages_dir, 'tmp_git')
            os.mkdir(tmp_destination_path)

            repo = git.Repo.clone_from(source_path, tmp_destination_path)

            subdirectory_path = os.path.join(repo.working_tree_dir, subdirectory)
            shutil.copytree(subdirectory_path, self.destination_path)

            git.rmtree(repo.working_tree_dir)

        else:
            git.Repo.clone_from(source_path, self.destination_path)

        super().installed_package_config()

        return self.package_config_file