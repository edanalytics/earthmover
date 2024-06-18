# inspired by dbt init
# ref: https://github.com/dbt-labs/dbt-core/blob/main/core/dbt/task/init.py
import os
from pathlib import PurePath
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

def run_init() -> None:
    project_name = input("Enter a name for your project: ")

    # sanitize
    clean_name = "".join(c for c in project_name if (c.isalnum() or c in "_-"))
    full_path = PurePath(os.getcwd(), clean_name)

    try:
        copy_starter_repo(full_path)
    except FileExistsError:
        print(f"ERROR: a file or directory already exists at {full_path}")
        return None

    return full_path
