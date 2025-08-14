This page tracks releases of `earthmover`, with a summary of what was changed, fixed, added in each new version.


## 2025 releases

### [v0.4.6](https://github.com/edanalytics/earthmover/releases/tag/v0.4.6)
(Released 2025-08-14)

* bugfix: [`rename_columns` operation to an existing column name could result in two columns of the same name; now this results in an error](https://github.com/edanalytics/earthmover/pull/166)
* feature: [Add support for non-UTF8 file encodings for fixed-width inputs](https://github.com/edanalytics/earthmover/pull/165)


### [v0.4.5](https://github.com/edanalytics/earthmover/releases/tag/v0.4.5)
(Released 2025-07-11)

* bugfix: [update MANIFEST.in to fix `earthmover init`](https://github.com/edanalytics/earthmover/pull/161)
* feature: [New year, new docs!](https://github.com/edanalytics/earthmover/pull/148)
* feature: [wildcard matching for columns](https://github.com/edanalytics/earthmover/pull/156)
* feature: [multi-directional sorting](https://github.com/edanalytics/earthmover/pull/159)
* feature: [make `SqlSource`s hashable (work with state-tracking)](https://github.com/edanalytics/earthmover/pull/162)


### [v0.4.4](https://github.com/edanalytics/earthmover/releases/tag/v0.4.4)
(Released 2025-03-06)

* bugfix: [improve exception-handling when loading a `SQLSource` using SQLAlchemy 2.x](https://github.com/edanalytics/earthmover/pull/153)


### [v0.4.3](https://github.com/edanalytics/earthmover/releases/tag/v0.4.3)
(Released 2025-01-23)

* feature: [allow a `colspec_file` config with column info for `fixedwidth` inputs](https://github.com/edanalytics/earthmover/pull/139)
* feature: [error messages for `keep_columns` and `drop_columns` now specify the columns](https://github.com/edanalytics/earthmover/pull/150)


## 2024 releases

### [v0.4.2](https://github.com/edanalytics/earthmover/releases/tag/v0.4.2)
(Released 2024-11-15)

* feature: [interpolate params into destination templates](https://github.com/edanalytics/earthmover/pull/141)
* feature: [lowercase columns](https://github.com/edanalytics/earthmover/pull/143)
* fix: [optional fields recursion](https://github.com/edanalytics/earthmover/pull/142)
* fix: [`earthmover deps` fails when not all params are passed](https://github.com/edanalytics/earthmover/pull/140)
* fix: [make all pandas/dask config conditional on >3.10](https://github.com/edanalytics/earthmover/pull/146)


### [v0.4.1](https://github.com/edanalytics/earthmover/releases/tag/v0.4.1)
(Released 2024-11-15)

* feature: [allow specifying `colspecs` for fixed-width files](https://github.com/edanalytics/earthmover/pull/133)
* feature: [allow `config` params to be passable at the CLI and have a `parameter_default`](https://github.com/edanalytics/earthmover/pull/130)
* feature: [refactor Source columns list logic as a select instead of a rename](https://github.com/edanalytics/earthmover/pull/137)
* bugfix: []`earthmover deps` failed to find nested local packages](https://github.com/edanalytics/earthmover/pull/134)
* bugfix: [relative paths not resolved correct when using project composition](https://github.com/edanalytics/earthmover/pull/134)
* bugfix: `--results-file` required a directory prefix
* bugfix: [some functionality was broken for Python versions < 3.10](https://github.com/edanalytics/earthmover/pull/136)


### [v0.4.0](https://github.com/edanalytics/earthmover/releases/tag/v0.4.0)
(Released 2024-10-16)

* feature: add support for Python 3.12, with corresponding updates to core dataframe dependencies
* feature: add `--set` flag for overriding values within `earthmover.yml` from the command line


### [v0.3.8](https://github.com/edanalytics/earthmover/releases/tag/v0.3.8)
(Released 2024-09-06)

* bugfix: Jinja in destination `header` failed if dataframe is empty


### [v0.3.7](https://github.com/edanalytics/earthmover/releases/tag/v0.3.7)
(Released 2024-09-04)

* feature: implementing a limit_rows operation
* feature: add support for a `require_rows` boolean or non-negative int on any node
* feature: add support for Jinja in a destination node header and footer
* bugfix: union fails with duplicate columns


### [v0.3.6](https://github.com/edanalytics/earthmover/releases/tag/v0.3.6)
(Released 2024-08-07)

* feature: add `json_array_agg` function to `group_by` operation
* feature: select all columns using "*" in `modify_columns` operation
* internal: set working directory to the location of the `earthmover.yaml` file
* documentation: add information on `earthmover init` and `earthmover clean` to the README
* bugfix: fix bug with `earthmover clean` that could have removed earthmover.yaml files


### [v0.3.5](https://github.com/edanalytics/earthmover/releases/tag/v0.3.5)
(Released 2024-07-12)

* feature: add `earthmover init` command to initialize a new sample project in the expected bundle structure
* internal: expand test run to include the new `debug` and `flatten` operations, as well as a nested JSON source file
* internal: improve customization in write behavior in new file destinations
* bugfix: Fix bug when writing null values in `FileDestination`


### [v0.3.4](https://github.com/edanalytics/earthmover/releases/tag/v0.3.4)
(Released 2024-06-26)

* hotfix: Fix bug when writing out JSON in `FileDestination`


### [v0.3.3](https://github.com/edanalytics/earthmover/releases/tag/v0.3.3)
(Released 2024-06-18)

* hotfix: Resolve incompatible package dependencies
* hotfix: Fix type casting of nested JSON for destination templates


### [v0.3.2](https://github.com/edanalytics/earthmover/releases/tag/v0.3.2)
(Released 2024-06-14)

* feature: Add `DebugOperation` for logging data head, tail, columns, or metadata midrun
* feature: Add `FlattenOperation` for splitting and exploding string columns into values
* feature: Add optional 'fill_missing_columns' field to `UnionOperation` to fill disjunct columns with nulls, instead of raising an error (default `False`)
* feature: Add `git_auth_timeout` config when entering Git credentials during package composition
* feature: [Add `earthmover clean` command that removes local project artifacts](https://github.com/edanalytics/earthmover/pull/87)
* feature: only output compiled template during `earthmover compile`
* feature: Render full row into JSON lines when `template` is undefined in `FileDestination`
* internal: Move `FileSource` size-checking and `FtpSource` FTP-connecting from compile to execute
* internal: Move template-file check from compile to execute in `FileDestination`
* internal: Allow filepaths to be passed to an optional `FileSource`, and check for file before creating empty dataframe
* internal: Build an empty dataframe if an empty folder is passed to an optional `FileSource`
* internal: fix some examples in README
* internal: remove GitPython dependency
* bugfix: fix bug in `FileDestination` where `linearize: False` resulted in BOM characters
* bugfix: fix bug where nested JSON would be loaded as a stringified Python dictionary
* bugfix: [Ensure command list in help menu and log output is always consistent](https://github.com/edanalytics/earthmover/pull/87)
* bugfix: fix bug in `ModifyColumnsOperation` where `__row_data__` was not exposed in Jinja templating


### [v0.3.1](https://github.com/edanalytics/earthmover/releases/tag/v0.3.1)
(Released 2024-04-26)

* internal: allow any ordering of Transformations during graph-building in compile
* internal: only create a `/packages` dir when `earthmover deps` succeeds


### [v0.3.0](https://github.com/edanalytics/earthmover/releases/tag/v0.3.0)
(Released 2024-04-17)

* feature: add project composition using `packages` keyword in template file (see README)
* feature: add installation extras for optional libraries, and improve error logging to notify which is missing
* feature: `GroupByWithRankOperation` cumulatively sums record counts by group-by columns
* feature: setting `log_level: DEBUG` in template configs or setting `debug: True` for a node displays the head of the node mid-run 
* feature: add `optional_fields` key to all Sources to add optional empty columns when missing from schema
* feature: add optional `ignore_errors` and `exact_match` boolean flags to `DateFormatOperation`
* internal: force-cast a dataframe to string-type before writing as a Destination
* internal: remove attempted directory-hashing when a source is a directory (i.e., Parquet)
* internal: refactor project to standardize import paths for Node and Operation
* internal: add `Node.full_name` attribute and `Node.set_upstream_source()` method
* internal: unify graph-building into compilation
* internal: refactor compilation and execution code for cleanliness
* internal: unify `Node.compile()` into initialization to ease Node development
* internal: Remove unused `group_by_with_count` and `group_by_with_agg` operations


### [v0.2.1](https://github.com/edanalytics/earthmover/releases/tag/v0.2.1)
(Released 2024-04-08)

* feature: [adding fromjson() function to Jinja](https://github.com/edanalytics/earthmover/pull/75)
* feature: [fix docs typos](https://github.com/edanalytics/earthmover/pull/68)
* feature: [`SortRowsOperation` sorts the dataset by `columns`](https://github.com/edanalytics/earthmover/pull/56)

## 2023 releases

### [v0.2.0](https://github.com/edanalytics/earthmover/releases/tag/v0.2.0)
(Released 2023-09-11)

* breaking change: remove `source` as Operation config and move to Transformation; this simplifies templates and reduces memory usage
* breaking change: `version: 2` required in Earthmover YAML files 
* feature: `SnakeCaseColumnsOperation` converts all columns to snake_case
* feature: `show_progress` can be turned on globally in `config` or locally in any Source, Transformation, or Destination to display a progress bar
* feature: `repartition` can be turned on in any applicable `Node` to alter Dask partition-sizes post-execute
* feature: improve performance when writing Destination files
* feature: improved Earthmover YAML-parsing and config-retrieval
* internal: rename `YamlEnvironmentJinjaLoader` to `JinjaEnvironmentYamlLoader` for better transparency of use
* internal: simplify Earthmover.build_graph()
* internal: unify Jinja rendering into a single util function, instead of redeclaring across project
* internal: unify `Node.verify()` into `Node.execute()` for improved code legibility
* internal: improve attribute declarations across project
* internal: improve type-hinting and doc-strings across project
* bugfix: refactor SqlSource to be compatible with SQLAlchemy 2.x


### [v0.1.6](https://github.com/edanalytics/earthmover/releases/tag/v0.1.6)
(Released 2023-07-11)

* bugfix: [fixing a bug to create the results_file directory if needed](https://github.com/edanalytics/earthmover/pull/40)
* bugfix: [process a copy of each nodes data at each step, to avoid modifying original node data which downstreams nodes may rely on](https://github.com/edanalytics/earthmover/pull/41)


### [v0.1.5](https://github.com/edanalytics/earthmover/releases/tag/v0.1.5)
(Released 2023-06-13)

* bugfix: [fixing a bug to skip hashing missing optional source files](https://github.com/edanalytics/earthmover/pull/34)
* feature: [adding a tmp_dir config so we can tell Dask where to store data it spills to disk](https://github.com/edanalytics/earthmover/pull/37)
* feature: [adding a `--results-file` option to produce structured run metadata](https://github.com/edanalytics/earthmover/pull/35)
* feature: [adding a skip exit code](https://github.com/edanalytics/earthmover/pull/36)


### [v0.1.4](https://github.com/edanalytics/earthmover/releases/tag/v0.1.4)
(Released 2023-05-12)

* bugfix: `config.state`_file was being ignored when specified
* bugfix: further issues with multi-line `config.macros` - the resolution here (hopefully the last one!) is to pre-load macros (so they can be injected into run-time Jinja contexts) and then just allow the Jinja to render and macro definitions down to nothing in the config YAML... you do have to be careful with Jinja linebreak suppression, i.e.
    ```yaml
    config:
    macros: > # this is a macro!
        {%- macro test() -%}
        testing!
        {%- endmacro -%}
    sources:
    ...
    ```
    could render down to
    ```yaml
    config:
    macros: > # this is a macro!sources:
    ...
    ```
    which will fail with an error about no sources defined.

* bugfix: charset issues when reading / writing non-UTF8 files - this should be resolved by enforcing every file read/write to specify UTF8 encoding


### [v0.1.3](https://github.com/edanalytics/earthmover/releases/tag/v0.1.3)
(Released 2023-05-05)

* feature: implement ability to call ` {{ md5(column) }}` in Jinja throughout eathmover, with a framework for other Python functions to be added in the future
* bugfix: fix multi-line macros issue


### [v0.1.2](https://github.com/edanalytics/earthmover/releases/tag/v0.1.2)
(Released 2023-05-02)

* bugfix: fix continued issues with environment variable expansion under Windows by changing from `os.path.expandvars()` to native Python `String.Template` implementation
* bugfix: change how earthmover loads `config.macros` from YAML to prevent issues with multi-line macros definitions


### [v0.1.1](https://github.com/edanalytics/earthmover/releases/tag/v0.1.1)
(Released 2023-03-27)

* bugfix: a single quote in the config YAML could prevent environment variable expansion from working since `os.path.expandvars()` [does not expand variables within single quotes](https://hg.python.org/cpython/file/v2.7.3/Lib/ntpath.py#l330) in Python under Windows


### [v0.1.0](https://github.com/edanalytics/earthmover/releases/tag/v0.1.0)
(Released 2023-03-23)

* feature: added parse-time Jinja templating to YAML configuration

> :warning: **Potentially breaking change:** if your config YAML contains `add_columns` or `modify_columns` operations *with Jinja expressions*, these will now be parsed at YAML load time. To preserve the Jinja for runtime parsing, wrap the expressions with `{%raw%}...{%endraw%}`. See [YAML parsing](usage.md#jinja-in-yaml-configuration) for further information.

* feature: removed dependency on matplotlib, which is only required if your YAML specified `config.show_graph: True`... now if you try to `show_graph` without matplotlib installed, you'll get an error prompting you to install matplotlib


### [v0.0.7](https://github.com/edanalytics/earthmover/releases/tag/v0.0.7)
(Released 2023-02-23)

* feature: added `str_min()` and `str_max()` functions for `group by` operation


### [v0.0.6](https://github.com/edanalytics/earthmover/releases/tag/v0.0.6)
(Released 2023-02-17)

* feature: pass `__row_data__` dict into Jinja templates for easier dynamic column referencing
* bugfix: parameter / env var interpolation into YAML keys, not just values
* refactor error handling key assertion methods
* refactor YAML loader line number context handling


## 2022 releases

### [v0.0.5](https://github.com/edanalytics/earthmover/releases/tag/v0.0.5)
(Released 2022-12-16)

* trim nodes not connected to a destination from DAG
* ensure all source datatypes return a Dask dataframe
* update [optional source functionality](usage.md#optional-sources) to require `columns` list, and pass an empty dataframe through the DAG


### [v0.0.4](https://github.com/edanalytics/earthmover/releases/tag/v0.0.4)
(Released 2022-10-27)

* support running in Google Colab


### [v0.0.3](https://github.com/edanalytics/earthmover/releases/tag/v0.0.3)
(Released 2022-10-27)

* support for Python 3.7


### [v0.0.2](https://github.com/edanalytics/earthmover/releases/tag/0.0.2)
(Released 2022-09-22)

* initial release

