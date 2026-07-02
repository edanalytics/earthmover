"""
Standalone file-reading interface for Earthmover.

Exposes Earthmover's `FileSource` reader as a plain function returning a dataframe,
without a full DAG execution. Two entry points:

- `read_file(file, config)` -- read a file given its path and a dict of source config.
- `read_compiled_source(source_name, compiled_config)` -- read a source by name from an
  `earthmover_compiled.yaml`.

"""
import logging
import os

import yaml

from earthmover.error_handler import ErrorHandler
from earthmover.nodes.source import FileSource
from earthmover.yaml_parser import YamlMapping

from typing import Optional


# Module-level logger; callers may pass their own.
# Set the level explicitly: `Node` treats a NOTSET logger (level 0) as debug mode and
# prints the dataframe head to stdout. The CLI avoids this by setting INFO too.
_default_logger = logging.getLogger("earthmover.reader")
_default_logger.setLevel(logging.INFO)


class _ReaderContext:
    """
    Minimal stand-in for an `Earthmover` instance, exposing only what a `Source` node
    touches during construction and reading: `logger`, `error_handler`, `state_configs`.
    """
    def __init__(self, logger: logging.Logger, error_handler: ErrorHandler):
        self.logger = logger
        self.error_handler = error_handler
        self.state_configs = {"show_progress": False}


def read_file(
    file: str,
    config: Optional[dict] = None,
    *,
    materialize: bool = True,
    post_process: bool = True,
    logger: Optional[logging.Logger] = None,
):
    """
    Read a single file into a dataframe using Earthmover's `FileSource` reader.

    :param file: path is not resolved relative to earthmover config - pass an absolute path
    :param config: Earthmover source config as a dict, e.g. {"type": "csv", "header_rows": 2, ...}.
    :param materialize: If True, return a materialized pandas dataframe; if False, return the
        underlying reader's output. Default True
    :param post_process: If True, apply Earthmover's standard post-read
        processing - this drops fully-empty rows and adds `optional_fields`. 
        Set False to preserve 1:1 row positions with the source file. Default True
    :param logger: Optional logger.
    """
    logger = logger or _default_logger
    abs_file = os.path.abspath(file)

    # Build a YamlMapping so the node can resolve a relative `colspec_file` and error
    # messages have a file reference. Preserve an origin carried by a passed YamlMapping;
    # otherwise anchor to the input file's directory.
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
    if materialize and hasattr(data, "compute"):
        data = data.compute()
    return data


def read_compiled_source(
    source_name: str,
    compiled_config: str,
    *,
    materialize: bool = True,
    post_process: bool = True,
    logger: Optional[logging.Logger] = None,
):
    """
    Read a named source from an `earthmover_compiled.yaml`.

    The compiled file is a fully-resolved project (packages merged, Jinja/params/macros
    applied, `file:` paths absolute), so this just reads plain YAML, pulls the named
    source, and hands it to `read_file`. Run `earthmover compile` first (and
    `earthmover deps` if the project uses packages).

    :param source_name: The key under `sources:` to read.
    :param compiled_file: Path to the compiled YAML.
    :param materialize: See `read_file`.
    :param post_process: See `read_file`.
    :param logger: Optional logger.
    """
    logger = logger or _default_logger
    config = os.path.abspath(compiled_config)

    if not os.path.isfile(config):
        raise FileNotFoundError(
            f"compiled config not found at {config}"
        )

    with open(config, "r", encoding="utf-8") as fp:
        configs = yaml.safe_load(fp) or {}

    sources = configs.get("sources") or {}
    if source_name not in sources:
        available = ", ".join(sorted(sources)) or "(none)"
        raise KeyError(
            f"source `{source_name}` not found in {config}. Available sources: {available}"
        )

    source_config = sources[source_name]
    if "file" not in source_config:
        raise TypeError(
            f"source `{source_name}` in {config} is not a file source, so is not supported"
        )

    return read_file(
        source_config["file"], source_config,
        materialize=materialize, post_process=post_process, logger=logger,
    )
