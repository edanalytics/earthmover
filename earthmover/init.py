# inspired by dbt init
# ref: https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/task/init.py
import shutil

# These files are not needed for the starter project but exist for finding the resource path
IGNORE_FILES = ["__init__.py", "__pycache__"]

def copy_starter_repo(project_name: str) -> None:
    # Lazy import to avoid ModuleNotFoundError
    from earthmover.include.starter_project import (
        PACKAGE_PATH as starter_project_directory,
    )

    shutil.copytree(
        starter_project_directory, project_name, ignore=shutil.ignore_patterns(*IGNORE_FILES)
    )
