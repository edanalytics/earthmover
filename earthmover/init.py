import os
from pathlib import Path
import shutil

def populate_project(project_name: str) -> None:
    root_path = Path(os.path.dirname(__file__))
    include_path = root_path / "include" / "starter_project"

    # initialize starter project
    project_path = Path(project_name)
    project_path.mkdir()
    shutil.copy2(include_path / "README.md", project_path)
    shutil.copy2(include_path / "earthmover.yaml", project_path)

    project_sources = project_path / "sources"
    project_templates = project_path / "templates"
    project_sources.mkdir()
    project_templates.mkdir()

    # copy example files used by test suite into starter project
    test_path = root_path / "tests"
    shutil.copy2(test_path / "sources" / "mammals.csv", project_sources)
    shutil.copy2(test_path / "sources" / "fishes.csv", project_sources)
    shutil.copy2(test_path / "templates" / "animal.jsont", project_templates)

def run_init() -> None:
    project_name = input("Enter a name for your project: ")

    # sanitize by removing spaces and most punctuation (just to eliminate possibility of invalid names)
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
