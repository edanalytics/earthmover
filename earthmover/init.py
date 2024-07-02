import os
from pathlib import Path
import shutil

def populate_project(project_name: str) -> None:
    root_path = Path(os.path.dirname(__file__))

    # initialize starter project
    shutil.copytree(root_path / "include" / "starter_project", project_name)

    # copy example files used by test suite into starter project
    test_path = root_path / "tests"

    project_path = Path(project_name)
    project_sources = project_path / "sources"
    project_templates = project_path / "templates"

    shutil.copy2(test_path / "sources" / "mammals.csv", project_sources)
    shutil.copy2(test_path / "sources" / "fishes.csv", project_sources)
    shutil.copy2(test_path / "templates" / "animal.jsont", project_templates)

def run_init() -> None:
    project_name = input("Enter a name for your project: ")

    # sanitize
    clean_name = "".join(c for c in project_name if (c.isalnum() or c in "_-"))
    if len(clean_name) == 0:
        print(f"ERROR: entered name has no valid characters (alphanumeric, underscore, dash)")
        return None

    full_path = Path(os.getcwd(), clean_name)

    try:
        populate_project(full_path)
    except FileExistsError:
        print(f"ERROR: a file or directory already exists at {full_path}")
        return None

    return full_path
