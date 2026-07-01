"""
Standalone file-reading interface for Earthmover.

This module exposes Earthmover's file reader (the same code path used by a normal
`earthmover run`) as a plain function that returns a dataframe, without requiring a
config file, an output directory, or a full DAG execution. It is intended for callers
(e.g. Runway's executor) that need to read a source file into memory *exactly* as
Earthmover would, while reusing all of Earthmover's existing configuration surface
(`header_rows`, `colspec_file`, `encoding`, `type`, `columns`, etc.).

Two entry points:
- `read_file(file, config)` -- read a file given its path and an explicit source config.
- `read_source(config_file, source_name)` -- read a source named in an existing
  earthmover.yaml, pulling its config (and resolving paths/Jinja/macros) from that file
  so you don't have to re-encode `header_rows`/`colspec_file`/etc. anywhere else.

Notes for callers:
- File reads go through `FileSource`, which raises a plain `Exception` on error (via
  the project's `ErrorHandler`). Nothing in this path calls `exit()` or prints a
  stacktrace -- those behaviors live only in the CLI (`__main__.py`) and in
  `Earthmover.generate()`. So errors are catchable normally.
- For `read_file`, pass only recognized Earthmover source-config keys; unrecognized keys
  log a warning (the same warning a normal run would emit).
- For `read_file`, relative `file:` paths are NOT resolved against a project directory
  (there is no config file to chdir into) -- pass an absolute path. A `colspec_file` is
  resolved relative to the origin of the config: the input file's directory for
  `read_file`, or the earthmover.yaml's directory for `read_source`.
- `read_source` parses only the named config file (Jinja, params, and `config.macros`
  are applied, mirroring a real run). It does NOT merge `packages:`, so a source defined
  only inside an installed package will not be found.
"""
import json
import logging
import os

from earthmover.error_handler import ErrorHandler
from earthmover.nodes.source import FileSource
from earthmover.yaml_parser import JinjaEnvironmentYamlLoader, YamlMapping

from typing import Optional, Union
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import pandas as pd


# Module-level logger; callers may pass their own.
# Note: the level must be set explicitly. `Node` enables debug mode (which prints the
# dataframe head to stdout) when `logger.level <= DEBUG`, and a fresh logger defaults to
# NOTSET (0), which would wrongly trigger that. The CLI avoids this by setting INFO too.
_default_logger = logging.getLogger("earthmover.reader")
_default_logger.setLevel(logging.INFO)


class _ReaderContext:
    """
    Minimal stand-in for an `Earthmover` instance, exposing only the attributes a
    `Source` node touches during construction and reading: `logger`, `error_handler`,
    and `state_configs`. This lets us reuse `FileSource` without spinning up a full
    Earthmover project (config file, output dir, package graph, etc.).
    """
    def __init__(self, logger: logging.Logger, error_handler: ErrorHandler):
        self.logger = logger
        self.error_handler = error_handler
        self.state_configs = {"show_progress": False}


def read_file(
    file: str,
    config: Optional[dict] = None,
    *,
    compute: bool = True,
    post_process: bool = True,
    logger: Optional[logging.Logger] = None,
):
    """
    Read a single file into a dataframe using Earthmover's `FileSource` reader.

    :param file: Path to the file to read. Should be absolute (see module docstring).
    :param config: Earthmover source config, e.g. {"type": "csv", "header_rows": 2,
        "encoding": "latin1", "columns": [...]}. The `file` key is set automatically.
        See `FileSource.allowed_configs` for the full set of recognized keys.
    :param compute: If True (default), return an eager pandas DataFrame. If False,
        return whatever the underlying reader produced (a Dask DataFrame for csv/tsv/
        json/parquet/orc, a pandas DataFrame for excel/sas/spss/stata/feather/html/xml).
    :param post_process: If True (default), apply Earthmover's standard post-read
        processing -- this is the *faithful* behavior identical to `earthmover run`,
        and it DROPS fully-empty rows (all-null or all-empty-string) and adds any
        `optional_fields`. Set to False to skip post-processing and preserve 1:1 row
        positions with the source file (needed if you intend to write values back by
        row position).
    :param logger: Optional logger; defaults to the module logger.
    :return: A pandas DataFrame (compute=True) or a Dask/pandas DataFrame (compute=False).
    """
    logger = logger or _default_logger
    file = os.fspath(file)
    abs_file = os.path.abspath(file)

    # Build a YamlMapping config so the node can resolve relative resource paths (e.g. a
    # `colspec_file`) and so error messages have a file reference. If the caller passed a
    # YamlMapping that already carries an origin (i.e. it came from a parsed
    # earthmover.yaml, via read_source), preserve it so `colspec_file` resolves relative
    # to that file; otherwise anchor to the input file's directory.
    origin = getattr(config, "__file__", None) or abs_file
    mapping = YamlMapping(__file__=origin, __line__=(getattr(config, "__line__", 0) or 0))
    if config:
        for key, value in config.items():
            mapping[key] = value
    mapping["file"] = abs_file

    error_handler = ErrorHandler(file=origin)
    context = _ReaderContext(logger=logger, error_handler=error_handler)

    source = FileSource("__read_file__", mapping, earthmover=context)
    source.execute()
    if post_process:
        source.post_execute()

    data = source.data
    if compute and hasattr(data, "compute"):
        data = data.compute()
    return data


def read_source(
    config_file: str,
    source_name: str,
    *,
    params: Optional[Union[dict, str]] = None,
    compute: bool = True,
    post_process: bool = True,
    logger: Optional[logging.Logger] = None,
):
    """
    Read a single named source from an existing earthmover.yaml, reusing both its config
    and Earthmover's reader -- so nothing about the source (header rows, colspecs,
    encoding, column selection, ...) has to be re-encoded outside the bundle.

    The named config file is parsed exactly as a run would parse it (environment-variable
    templating, `params`/`parameter_defaults`, and `config.macros` are all applied), the
    requested source's config is pulled from the `sources:` block, its `file` path is
    resolved relative to the config file, and the result is handed to `read_file`.

    Limitations (kept out of scope for simplicity):
    - `packages:` are NOT merged, so a source defined only inside an installed package
      will not be found.
    - File sources only -- a `connection`/`query` (SQL/FTP) source raises a clear error.

    :param config_file: Path to the earthmover.yaml (or .yml) to read the source from.
    :param source_name: The key under `sources:` to read.
    :param params: Optional run parameters, as a dict or a JSON string (mirrors the CLI
        `-p`/`--params` flag). Overrides environment variables during templating.
    :param compute: See `read_file`.
    :param post_process: See `read_file`.
    :param logger: Optional logger; defaults to the module logger.
    :return: A pandas DataFrame (compute=True) or a Dask/pandas DataFrame (compute=False).
    """
    logger = logger or _default_logger
    config_file = os.path.abspath(os.fspath(config_file))
    config_dir = os.path.dirname(config_file)

    # Normalize params (accept a dict or a JSON string, like the CLI).
    if params is None:
        params = {}
    elif isinstance(params, str):
        params = json.loads(params) if params else {}
    else:
        params = dict(params)

    # Mirror Earthmover's (non-package) config loading: pull parameter defaults and
    # macros from the `config:` block, then fully parse the file with Jinja + macros.
    project_configs = JinjaEnvironmentYamlLoader.load_project_configs(config_file, params=params)
    for key, val in project_configs.get("parameter_defaults", {}).items():
        params.setdefault(key, val)
    macros = project_configs.get("macros", "").strip()

    user_configs = JinjaEnvironmentYamlLoader.load_config_file(config_file, params=params, macros=macros)

    sources = user_configs.get("sources") or {}
    if source_name not in sources:
        available = ", ".join(sorted(sources)) or "(none)"
        raise Exception(
            f"source `{source_name}` not found in {config_file}. Available sources: {available}"
        )

    source_config = sources[source_name]
    if "file" not in source_config:
        raise Exception(
            f"source `{source_name}` in {config_file} is not a file source "
            f"(read_source supports file sources only, not SQL/FTP `connection` sources)"
        )

    # Resolve the source's `file` relative to the config file's directory (a real run
    # does this by chdir-ing to the config file's location before reading).
    src_file = source_config["file"]
    if not os.path.isabs(src_file):
        src_file = os.path.join(config_dir, src_file)

    return read_file(
        src_file, source_config,
        compute=compute, post_process=post_process, logger=logger,
    )
