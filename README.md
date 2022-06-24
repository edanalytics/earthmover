<!-- Logo/image -->
![earthmover](images/earthmover.png)

`earthmover` transforms collections of tabular source data (flat files, FTP files, database tables/queries) into text-based (JSONL, XML) data via YAML configuration.
<!-- GIF or screenshot? -->


# Table of Contents  
* [Requirements](#requirements)
* [Installation](#installation)
* [Setup](#setup)
* [Usage](#usage)
* [Features](#features)
* [Tests](#tests)
* [Design](#design)
* [Performance](#performance)
* [Limitations](#limitations)
* [Changelog](#changelog)
* [Contributing](#contributing)<!--
Guides and Resources -->
* [License](#license)
* [Bundles](#bundles)
<!-- References -->


# Requirements
[Python 3](https://www.python.org/) and [pip](https://pypi.org/project/pip/).


# Installation
```
pip install earthmover
```


# Setup
Running the tool requires
1. [source data](#source-data), such as CSV or TSV files or a relational database table
1. Jinja [templates](#templates) defining the desired output format (JSON, XML, etc.)
1. a [YAML configuration](#yaml-configuration) file specifying the source data, doing any necessary transformations (joins, value mappings, etc.), and destinations (Ed-Fi API resources) to write to

Item 1 is usually your own data. Items 2 & 3 together may be shared as a reusable "bundle" (zip file); see [available bundles](docs/bundles.md) for more information and a list of published bundles.

> **If you  already have a bundle, continue to the [usage](#Usage) section below.**

If you develop a bundle for a particular source data system or format, please consider contributing it to the community by publishing it online and emailing the link to treitz@edanalytics.org to get it listed [here](docs/bundles.md).


## Source data
This tool is designed to operate on tabluar data in the form of multiple CSV or TSV files, such as those created by an export from some software system, or from a set of database tables.

There are few limitations on the source data besides its format (CSV or TSV). Generally it is better to avoid using spaces in column names, however this can be managed by renaming columns as described in the [`sources`](#sources) YAML configuration section below.


## Templates
After transforming the source data, this tool converts it to a text-based file like JSON(L) or XML based on a template using the Jinja templating language.

Briefly, Jinja interpolates variables in double curly braces `{{...}}` to actual values, as well as providing other convenience functionality, such as string manipulation functions, logic blocks like if-tests, looping functionality, and much more. See the examples in the `examples/sample_templates/` folder, or check out [the official Jinja documentation](https://jinja.palletsprojects.com/en/3.1.x/).


## YAML configuration
All the instructions for this tool &mdash; where to find the source data, what transformations to apply to it, and how and where to save the output &mdash; are specified in a single YAML configuration file. Example YAML configuration files can be found in `examples/sample_configs/`.

The general structure of the YAML involves four main sections:
1. [`config`](#config), which specifies options like the memory limit and whether to show verbose output
1. [`sources`](#sources), where each source file is listed with details like the number of header rows
1. [`transformations`](#transformations), where source data can be transformed in various ways
1. [`destinations`](#destinations), where transformed data can be mapped to JSON templates and Ed-Fi endpoints and sent to an Ed-Fi API

Section 1 has general options. Sections 2, 3, and 4 define a [DAG](#dag) which enables efficient data processing. Below, we document each section in detail:

### **`config`**
The `config` section of the [YAML configuration](#yaml-configuration) specifies various options for the operation of this tool.

A sample `config` section is shown here; the options are explained below.
```yaml
config:
  output_dir: ./
  state_file: ~/.earthmover.csv
  memory_limit: 500MB
  verbose: True
  show_stacktrace: True
  show_graph: True
  macros: >
    {% macro example_macro(value) -%}
        prefixed-int-{{value|int}}
    {%- endmacro %}
```
* (optional) `output_dir` determines where generated JSONL is stored. The default is `./`.
* (optional) `state_file` determines the file which maintains [tool state](#state). The default is `~/.earthmover.csv` on *nix systems, `C:/Users/USER/.earthmover.csv` on Windows systems.
* (optional) Specify a `memory_limit` for the tool. The default is `1GB`.
* (optional) Turn on `verbose` output. The default is `False`.
* (optional) Specify whether to show a stacktrace for runtime errors. The default is `False`.
* (optional) Specify whether or not `show_graph` (default is `False`), which requires [PyGraphViz](https://pygraphviz.github.io/) to be installed and creates `graph.png` and `graph.svg` which are visual depictions of the dependency graph.
* (optional) Specify Jinja `macros` which will be available within any Jinja template content throughout the project. (This can slow performance.)


### **`sources`**
The `sources` section of the [YAML configuration](#yaml-configuration) specifies source data the tool will work with.

A sample `sources` section is shown here; the options are explained below.
```yaml
sources:
  districts:
    connection: "ftp://user:pass@host:port/path/to/districts.csv"
  tx_schools:
    connection: "postgresql://user:pass@host/database"
    query: >
      select school_id, school_name, school_website
      from schema.schools
      where school_address_state='TX'
  courses:
    file: ./data/Courses.csv
    header_rows: 1
    columns:
      - school_id
      - school_year
      - course_code
      - course_title
      - subject_id
  more_schools:
    file: ./data/Schools.csv
    header_rows: 1
    columns:
      - school_id
      - school_name
      - address
      - phone_number
    expect:
      - low_grade != ''
      - high_grade != ''
      - low_grade|int <= high_grade|int
```
Each source must have a name (which is how it is referenced by transformations and destinations) such as `districts`, `courses`, `tx_schools`, or `more_schools` in this example. Three types of `sources` are currently supported:
* File sources must specify the relative or absolute path to the source `file`, the number of `header_rows`, and (if `header_rows` > 0, optionally) overwrite the `column` names.
* Database sources are supported via [SQLAlchemy](https://www.sqlalchemy.org/). They must specify a database `connection` string and SQL `query` to run. Large database sources support [chunking](#chunking).
* FTP file sources are supported via [ftplib](https://docs.python.org/3/library/ftplib.html). They must specify an FTP `connection` string. FTP sources must fit in memory, chunking not supported at this time. 

For any source, optionally specify conditions you `expect` data to meet which, if not true for any row, will cause the run to fail with an error. (This can be useful for detecing and rejecting NULL or missing values before processing the data.) The format must be a Jinja expression that returns a boolean value. This is enables casting values (which are all treated as strings) to numeric formats like int and float for numeric comparisons.

The examples above show `user:pass` in the `connection` string, but if you are version-controlling your YAML you must avoid publishing such credentials. Typically this is done via [environment variables](#environment-variable-references) or [command line parameters](#command-line-parameters), which are both supported by this tool. Such environment variable references may be used throughout your YAML (not just in the `sources` section), and are parsed at load time.


### **`transformations`**
The `transformations` section of the [YAML configuration](#yaml-configuration) specifies how source data is manipulated by the tool.

A sample `transformations` section is shown here; the options are explained below.
```yaml
transformations:
  courses:
    - operation: map_values
      source: $sources.courses
      column: subject_id
      mapping:
        01: 1 (Mathematics)
        02: 2 (Literature)
        03: 3 (History)
        04: 4 (Language)
        05: 5 (Computer and Information Systems)
    - operations: join
      sources:
        - $transformations.courses
        - $sources.schools
      join_type: inner
      left_key: school_id
      right_key: school_id
    - operation: drop_columns
      source: $transformations.courses
      columns:
        - address
        - phone_number
```
The above example shows a transformation of the `courses` source, which consists of an ordered list of operations. Each operation has one or more sources, which may be an original `$source`, another `$transformation`, or the prior step of the same `$transformation` (operations can be chained together within a transformation). Transformation operations each require further specification depending on their type; the operations are listed and documented below.

#### Frame operations

<details>
<summary><code>union</code></summary>

Concatenates two or more sources sources of the same shape.
```yaml
    - operation: union
      sources:
        - $sources.courses_list_1
        - $sources.courses_list_2
        - $sources.courses_list_3
```
At most one source may be large (chunked).
</details>


<details>
<summary><code>join</code></summary>

Joins two sources.
```yaml
    - operation: join
      sources:
        - $transformations.courses
        - $sources.schools
      join_type: inner | left | right
      left_key: school_id
      right_key: school_id
      # or:
      left_keys:
        - school_id
        - school_year
      right_keys:
        - school_id
        - school_year
      # optionally specify columns to (only) keep from the left and/or right sources:
      left_keep_columns:
        - left_col_1
        - left_col_2
      right_keep_columns:
        - right_col_1
        - right_col_2
      # or columns to discard from the left and/or right sources:
      left_drop_columns:
        - left_col_1
        - left_col_2
      right_drop_columns:
        - right_col_1
        - right_col_2
      # (if neither ..._keep nor ..._drop are specified, all columns are retained)
```
Joining can lead to a wide result; the `..._keep_columns` and `..._drop_columns` options enable narrowing it.

Besides the join column(s), if a column `my_column` with the same name exists in both tables and is not dropped, it will be renamed `my_column_x` and `my_column_y`, from the left and right respectively, in the result.

At most one source may be large (chunked).
</details>


#### Column operations

<details>
<summary><code>add_columns</code></summary>

Adds columns with specified values.
```yaml
    - operation: add_columns
      source: $transformations.courses
      columns:
        - new_column_1: value_1
        - new_column_2: "{% if True %}Jinja works here{% endif %}"
        - new_column_3: "Reference values from {{AnotherColumn}} in this new column"
        - new_column_4: "{% if col1>col2 %}{{col1|float + col2|float}}{% else %}{{col1|float - col2|float}}{% endif %}"
```
</details>


<details>
<summary><code>rename_columns</code></summary>

Renames columns.
```yaml
    - operation: rename_columns
      source: $transformations.courses
      columns:
        old_column_1: new_column_1
        old_column_2: new_column_2
        old_column_3: new_column_3
```
</details>


<details>
<summary><code>duplicate_columns</code></summary>

Duplicates columns (and all their values).
```yaml
    - operation: duplicate_columns
      source: $transformations.courses
      columns:
        existing_column1: new_copy_of_column1
        existing_column2: new_copy_of_column2
```
</details>


<details>
<summary><code>drop_columns</code></summary>

Removes the specified columns.
```yaml
    - operation: drop_columns
      source: $transformations.courses
      columns:
        - column_to_drop_1
        - column_to_drop_2
```
</details>


<details>
<summary><code>keep_columns</code></summary>

Keeps only the specified columns, discards the rest.
```yaml
    - operation: keep_columns
      source: $transformations.courses
      columns:
        - column_to_keep_1
        - column_to_keep_2
```
</details>


<details>
<summary><code>combine_columns</code></summary>

Combines the values of the specified columns, delimited by a separator, into a new column.
```yaml
    - operation: combine_columns
      source: $transformations.courses
      columns:
        - column_1
        - column_2
      new_column: new_column_name
      separator: "_"
```
Default `separator` is none - values are smashed together.
</details>


<details>
<summary><code>modify_columns</code></summary>

Modify the values in the specified columns.
```yaml
    - operation: modify_columns
      source: $transformations.school_directory
      columns:
        state_abbr: "XXX{{value|reverse}}XXX"
        school_year: "20{{value[-2:]}}"
        zipcode: "{{ value|int ** 2 }}"
```
Use Jinja: `{{value}}` refers to this column's value; `{{AnotherColumn}}` refers to another column's value. Any [Jinja filters](https://jinja.palletsprojects.com/en/3.1.x/templates/#builtin-filters) and [math operations](https://jinja.palletsprojects.com/en/3.0.x/templates/#math) should work.
</details>


<details>
<summary><code>map_values</code></summary>

Map the values of a column.
```yaml
    - operation: map_values
      source: $sources.courses
      column: column_name
      mapping:
        old_value_1: new_value_1
        old_value_2: new_value_2
      # or a CSV/TSV with two columns (from, to) and header row:
      map_file: path/to/mapping.csv
```
</details>


<details>
<summary><code>date_format</code></summary>

Change the format of a date column.
```yaml
    - operation: date_format
      source: $transformations.students
      column: date_of_birth
      # or
      columns:
        - date_column_1
        - date_column_2
        - date_column_3
      from_format: "%b %d %Y %H:%M%p"
      to_format: "%Y-%m-%d"
```
The `from_format` and `to_format` must follow [Python's strftime() and strptime() formats](https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior).
</details>


#### Row operations

<details>
<summary><code>distinct_rows</code></summary>

Removes duplicate rows.
```yaml
    - operation: distinct_rows
      source: $transformations.courses
      columns:
        - distinctness_column_1
        - distinctness_column_2
```
Optionally specify the `columns` to use for uniqueness, otherwise all columns are used. If duplicate rows are found, only the first is kept. This operation is only valid for non-chunked sources.
</details>


<details>
<summary><code>filter_rows</code></summary>

Filter (include or exclude) rows matching a query.
```yaml
    - operation: filter_rows
      source: $transformations.courses
      query: school_year < 2020
      behavior: exclude | include
```
The query format is anything supported by [Pandas.DataFrame.query](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.query.html). Specifying `behavior` as `exclude` wraps the Pandas `query()` with `not()`.
</details>


#### Group operations

<details>
<summary><code>group_by</code></summary>

Reduce the number of rows by grouping, and add columns with values calculated over each group.
```yaml
    - operation: group_by
      source: $transformations.assessment_items
      group_by_columns:
        - student_id
      create_columns:
        num_scores: count()
        min_score: min(item_score)
        max_score: max(item_score)
        avg_score: mean(item_score)
        item_scores: agg(item_score,;)
```
Valid aggregation functions are
* `count()` or `size()` - the number of rows in each group
* `min(column)` - the minumum (numeric) value in `column` for each group
* `max(column)` - the maximum (numeric) value in `column` for each group
* `sum(column)` - the sum of (numeric) values in `column` for each group
* `mean(column)` or `avg(column)` - the mean of (numeric) values in `column` for each group
* `std(column)` - the standard deviation of (numeric) values in `column` for each group
* `var(column)` - the variance of (numeric) values in `column` for each group
* `agg(column,separator)` - the values of `column` in each group are concatenated, delimited by `separator` (default `separator` is none)

Numeric aggregation functions will fail with errors if used on non-numeric column values.
</details>

<!--

* <a id='op_group_by_with_count'></a> reduce number of rows via group by, and add a column for group counts:
```yaml
    - operation: group_by_with_count
      source: $transformations.sessions
      group_by_columns:
        - school_number
        - school_year
        - structure_name
        - term_code
      count_column: instructional_days
```
One use-case for this is to calculate instructional days per session by joining sessions with a table of instructional dates, filtering for rows where the instructional date is between the session start and end date, then using this operator to group back down to one row per session but adding a count column.

* <a id='op_group_by_with_agg'></a> reduce number of rows via group by, and add a column with concatenated values of another column:
```yaml
    - operation: group_by_with_agg
      source: $transformations.assessment_items
      group_by_columns:
        - student_id
      agg_column: item_score
      separator: ";"
```
This transformation can be useful for building up nested structures, like arrays of objects in a JSON structure. `separator` is `,` if unspecified.

-->

#### Global options

Any operation may also specify `debug: True` which will output the dataframe shape and columns after the operation. This can be very useful for building and debugging transformations.



### **`destinations`**
The `destinations` section of the [YAML configuration](#yaml-configuration) specifies how transformed data is materialized to files of JSON payloads.

A sample `destinations` section is shown here; the options are explained below.
```yaml
destinations:
  schools:
    source: $transformations.school_list
    template: ./json_templates/school.jsont
    extension: jsonl
    linearize: True
  courses:
    source: $transformations.course_list
    template: ./json_templates/course.jsont
    extension: jsonl
    linearize: True
```
Provide an entry for each Ed-Fi resource you want to POST to, with the `source` and the JSON `template` (a JSON payload will be generated for each row of the source). Files are materialized using the `extension` you specify. If `linearize` is `True`, all line breaks are removed from the template, resulting in one output line per row. (This is useful for creating JSONL and other linear output formats.)



# Usage
Once you have the required [setup](#setup) and your source data, generate the JSONL payloads with
```bash
earthmover path/to/config.yaml
```

See a help message with
```bash
earthmover -h
earthmover --help
```

See the tool version with
```bash
earthmover -v
earthmover --version
```


# Features
This tool includes several special features:

## Selectors
Run only portions of the [DAG](#dag) by using a selector:
```bash
earthmover path/to/config.yaml -s people,people_*
```
This processes all DAG paths (from sources to destinations) through any matched nodes.

## Environment variable references
In your [YAML configuration](#yaml-configuration), you may reference environment variables with `${ENV_VAR}`. This can be useful for making references to source file locations dynamic, such as
```yaml
sources:
  people:
    file: ${BASE_DIR}/people.csv
    ...
```

## Command-line parameters
Similarly, you can specify parameters via the command line with
```bash
earthmover path/to/config.yaml -p '{"BASE_DIR":"path/to/my/base/dir"}'
earthmover path/to/config.yaml --params '{"BASE_DIR":"path/to/my/base/dir"}'
```
Command-line parameters override any environment variables of the same name.

## State
This tool *maintains state about past runs.* Subsequent runs only re-process if something has changed &ndash; the [YAML configuration](#yaml-configuration) itself, data files of `sources`, `value_mapping` CSVs of `transformations`, template files of `destinations`, or CLI parameters. (Changes are tracked by hashing files; hashes and run timestamps are stored in the file specified by [config](#config)/`state_file`.) You may choose to override this behavior and force reprocessing of the whole DAG, regardless of whether files have changed or not, using the `-f` or `--force` command-line flag:
```bash
earthmover path/to/config.yaml -f
earthmover path/to/config.yaml --force-regenerate
```
To further avoid computing input hashes and not log a run to the `state_file`, use the `-k` or `--skip-hashing` flag:
```bash
earthmover path/to/config.yaml -k
earthmover path/to/config.yaml --skip-hashing
```
(This makes a one-time run on large input files faster.)


# Tests
This tool ships with a test suite covering all transformation operations. It can be run with `earthmover -t`, which simply runs the tool on the `config.yaml` and toy data in the `earthmover/tests/` folder. (The DAG is pictured below.) Rendered `earthmover/tests/output/` are then compared against the `earthmover/tests/expected/` output; the test passes only if all files match exactly.

![tests DAG](earthmover/tests/tests-dag.png)

Run tests with
```bash
earthmover -t
```


# Design
Some details of the design of this tool are discussed below.

## DAG
The mapping of sources through transformations to destinations is modeled as a directed acyclic graph ([DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph)). Each [component](https://en.wikipedia.org/wiki/Component_(graph_theory)) of the DAG is run separately.

![dataflow graph components](images/dataflow-graph-components.gif)

Each component is materialized in [topological order](https://en.wikipedia.org/wiki/Topological_sorting). This minimizes memory usage, as only the data from the current and previous layer must be retained in memory.

![dataflow graph layers](images/dataflow-graph-layers.gif)

## Dataframes
All data processing is done using [Pandas Dataframes](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) with values stored as strings (or [Categoricals](https://pandas.pydata.org/docs/user_guide/categorical.html), for memory efficiency in columns with few unique values). This choice of datatypes prevents issues arising from Pandas' datatype inference (like inferring numbers as dates), but does require casting string-representations of numeric values using Jinja when doing comparisons or computations.

## Chunking
Some effort has been made to enable this tool to work with data larger than available memory. This is possible via a process we call *chunking*, where large data sources are processed as a series of smaller chunks that do fit in memory. We call large `sources` that require chunking *chunked sources*.

File and database `sources` support chunking; FTP `sources` do not. Likewise, some `transformation` operations are impossible on chunked sources: `join` may have at most one chunked source (depending on the `join_type`), and `distinct_rows` and `group_by` operations raise an error if invoked on a chunked source.

In the [DAG](#dag), each component may have at most one chunked source. Suppose *C* is the chunked source, and *T* is the tree emanating from *C*. Nodes outside (up to) *T* are processed first as usual, and nodes adjacent to *T* are retained in memory. Then *T* is processed repeatedly on each chunk separately.

![dataflow graph layers](images/dataflow-graph-bigsource.gif)

**Above:** dashed arrows indicate the flow of chunked data from the chunked data source *C* (they form the tree *T*). Green nodes are those retained in memory during chunked processing.



# Performance & Limitations
Tool performance depends on a variety of factors including source file size and/or database performance, the system's storage performance (HDD vs. SSD), and transformation complexity. But some effort has been made to engineer this tool for high throughput and to work in memory- and compute-constrained environments.

Smaller source data (which all fits into memory) processes very quickly. Larger chunked sources are necessarily slower. We have tested with sources files of 3.3GB, 100M rows (synthetic attendance data): creating 100M lines of JSONL (50GB) takes around 45 minutes on a modern laptop.

The [state feature](#state) adds some overhead, as hashes of input data and JSON payloads must be computed and stored, but this can be disabled if desired.


# Change log
[2022-06-??] Version 0.0.1 released


# Contributing
Bugfixes and new features (such as additional transformation operations) are gratefully accepted via pull requests here on GitHub.

## Contributions
* Cover image created with [DALL &bull; E mini](https://huggingface.co/spaces/dalle-mini/dalle-mini)



# License
See [License](LICENSE).


# Bundles
Bundles are pre-built data mappings for converting various data formats to Ed-Fi format using this tool. They consist of a folder with CSV seed data, JSON template files, and a `config.yaml` with sources, transformations, and destinations.

We currently have the following bundles available:

Source System and version | Ed-Fi API version | Author | Last Updated | Link
---|---|---|---|---
NWEA MAP v1 | Ed-Fi API v3.2 | [@tomreitz](https://github.com/tomreitz) | May 2022 | [Link](https://github.com/edanalytics/dsst_edfi_import_tool/tree/main/bundles/NWEA-MAP-v1__EDFI-2.5)
NWEA MAP v2 | Ed-Fi API v3.2 | [@tomreitz](https://github.com/tomreitz) | May 2022 | [Link](https://github.com/edanalytics/dsst_edfi_import_tool/tree/main/bundles/NWEA-MAP-v2__EDFI-2.5)


## Bundle Maker
We also provide a python script for converting Ed-Fi data import tool mappings to bundles for this tool. Given a `mapping.json` file from Ed-Fi DIT, from within the `bundles/` directory, you can run
```bash
python3 bundle-maker.py NewBundleName path/to/mapping.json
```
This will create a folder `bundles/NewBundleName/` with data and configuration derived from `mapping.json`. **Note** that if the Ed-Fi DIT mapping also requires data pre-processing done by PowerShell scripts, you must manually re-create those transformations in the `transformations:` section of the generated `config.yaml` file -- this cannot be done automatically.

If you develop a bundle for a particular SIS or other education data system, please consider contributing it to the community by publishing it online and emailing the link to treitz@edanalytics.org to get it listed above.