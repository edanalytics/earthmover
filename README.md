<!-- Logo/image -->
![earthmover](https://raw.githubusercontent.com/edanalytics/earthmover/main/images/earthmover.png)

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
* [Best Practices](#best-practices)
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
1. a [YAML configuration](#yaml-configuration) file specifying the source data, doing any necessary transformations (joins, value mappings, etc.), and destinations (output files) to write to

Item 1 is usually your own data. Items 2 & 3 together may be shared as a reusable "bundle" (zip file); see [available bundles](#bundles) for more information and a list of published bundles.

> **If you  already have a bundle, continue to the [usage](#Usage) section below.**

If you develop a bundle for a particular source data system or format, please consider contributing it to the community by publishing it online and emailing the link to treitz@edanalytics.org to get it listed [here](#bundles).


## Source data
This tool is designed to operate on tabular data in the form of multiple CSV or TSV files, such as those created by an export from some software system, or from a set of database tables.

There are few limitations on the source data besides its format (CSV or TSV). Generally it is better to avoid using spaces in column names, however this can be managed by renaming columns as described in the [`sources`](#sources) YAML configuration section below.


## Templates
After transforming the source data, this tool converts it to a text-based file like JSON(L) or XML based on a template using the Jinja templating language.

Briefly, Jinja interpolates variables in double curly braces `{{...}}` to actual values, as well as providing other convenience functionality, such as string manipulation functions, logic blocks like if-tests, looping functionality, and much more. See the examples in `example_projects/`, or check out [the official Jinja documentation](https://jinja.palletsprojects.com/en/3.1.x/).

Note that templates may [include](https://jinja.palletsprojects.com/en/3.1.x/templates/#include) other templates, specified relative to the path from which `earthmover` is run - see `example_projects/06_subtemplates/earthmover.yaml` and `example_projects/06_subtemplates/mood.jsont` for an example.


## YAML configuration

<details>
<summary>When updating to 0.2.x</summary>

-----
A breaking change was introduced in version 0.2 of Earthmover.
Before this update, each operation under a transformation required a `source` be defined.
This allowed inconsistent behavior where the results of an upstream operation could be discarded if misdefined.

The `source` key has been moved into transformations as a required field.
In unary operations, the source is the output of the previous operation (or the transformation `source` if the first defined).
In operations with more than one source (i.e., `join` and `union`), the output of the previous operation is treated as the first source;
any additional sources are defined using the `sources` field.

For example:
```yaml
# Before                          # After
transA:                           transA:
                                    source: $sources.A
  operations:                       operations:
    - operation: add_columns          - operation: add_columns
      source: $sources.A
      columns:                          columns:
        A: "a"                            A: "a"
        B: "b"                            B: "b"
    - operation: union                - operation: union
      sources:                          sources:
      - $transformations.transA
      - $sources.B                        - $sources.B
      - $sources.C                        - $sources.C
```

To ensure the user has updated their templates accordingly, the key and value `version: 2` is mandatory at the beginning of Earthmover templates going forward.

-----
</details>

All the instructions for this tool &mdash; where to find the source data, what transformations to apply to it, and how and where to save the output &mdash; are specified in a single YAML configuration file. Example YAML configuration files and projects can be found in `example_projects/`.

The YAML configuration may also [contain Jinja](#jinja-in-yaml-configuration) and [environment variable references](#environment-variable-references).

The general structure of the YAML involves the following sections:
1. `version`, with required value `2` (Earthmover 0.2.x and later)
1. [`config`](#config), which specifies options like the logging level and parameter defaults
1. [`definitions`](#definitions) is an *optional* way to specify reusable values and blocks
1. [`packages`](#packages), an *optional* way to import and build on existing projects
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
  log_level: INFO
  tmp_dir: /tmp
  show_stacktrace: True
  show_graph: True
  macros: >
    {% macro example_macro(value) -%}
        prefixed-int-{{value|int}}
    {%- endmacro %}
  parameter_defaults:
    SOURCE_DIR: ./sources/
  show_progress: True
  git_auth_timeout: 120

```
* (optional) `output_dir` determines where generated JSONL is stored. The default is `./`.
* (optional) `state_file` determines the file which maintains [tool state](#state). The default is `~/.earthmover.csv` on *nix systems, `C:/Users/USER/.earthmover.csv` on Windows systems.
* (optional) Specify a `log_level` for output. Possible values are
  - `ERROR`: only output errors like missing required sources, invalid references, invalid [YAML configuration](#yaml-configuration), etc.
  - `WARNING`: output errors and warnings like when the run log is getting long
  - `INFO`: all errors and warnings plus basic information about what `earthmover` is doing: start and stop, how many rows were removed by a `distinct_rows` or `filter_rows` operation, etc. (This is the default `log_level`.)
  - `DEBUG`: all output above, plus verbose details about each transformation step, timing, memory usage, and more. (This `log_level` is recommended for [debugging](#debugging-practices) transformations.)
* (optional) Specify the `tmp_dir` path to use when dask must spill data to disk. The default is `/tmp`.
* (optional) Specify whether to show a stacktrace for runtime errors. The default is `False`.
* (optional) Specify whether or not `show_graph` (default is `False`), which requires [PyGraphViz](https://pygraphviz.github.io/) to be installed and creates `graph.png` and `graph.svg` which are visual depictions of the dependency graph.
* (optional) Specify Jinja `macros` which will be available within any Jinja template content throughout the project. (This can slow performance.)
* (optional) Specify `parameter_defaults` which will be used if the user fails to specify a particular [parameter](#command-line-parameters) or [environment variable](#environment-variable-references).
* (optional) Specify whether to `show_progress` while processing, via a Dask progress bar.
* (optional) Specify the `git_auth_timeout` (in seconds) to wait for the user to enter Git credentials if needed during package installation; default is 60. See [project composition](#project-composition) for more details on package installation.

### **`definitions`**
The `definitions` section of the [YAML configuration](#yaml-configuration) is an optional section you can use to define configurations which are reused throughout the rest of the configuration. `earthmover` does nothing special with this section, it's just interpreted by the YAML parser. However, this can be a very useful way to keep your YAML configuration [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself) &ndash; rather than redefine the same values, Jinja phrases, etc. throughout your config, define them once in this section and refer to them later using [YAML anchors, aliases, and overrides](https://www.linode.com/docs/guides/yaml-anchors-aliases-overrides-extensions/).

An example `definitions` section is shown below:

```yaml
definitions:
  operations:
    - &student_join_op
      operation: join
      join_type: left
      left_key: student_id
      right_key: student_id
  ...
  date_to_year_jinja: &date_to_year "{%raw%}{{ val[-4:] }}{%endraw%}"
...

transformations:
  roster:
    operations:
      - <<: *student_join_op
        sources:
        - $sources.roster
        - $sources.students
  enrollment:
    operations:
      - <<: *student_join_op
        sources:
        - $sources.enrollment
        - $sources.students
  ...
  academic_terms:
    operations:
      - operation: duplicate_columns
        source: $sources.academic_terms
        columns:
          start_date: school_year
      - operation: modify_columns
        columns:
          school_year: *date_to_year
```


### **`packages`**
The `packages` section of the [YAML configuration](#yaml-configuration) is an optional section you can use to specify packages &ndash; other `earthmover` projects from a local directory or GitHub &ndash; to import and build upon exisiting code. See [Project Composition](#project-composition) for more details and considerations.

A sample `packages` section is shown here; the options are explained below.
```yaml
packages:
  year_end_assessment:
    git: "https://github.com/edanalytics/earthmover_edfi_bundles.git"
    subdirectory: "assessments/assessment_name"
  student_id_macros:
    local: "path/to/student_id_macros"
```
Each package must have a name (which will be used to name the folder where it is installed in `/packages`) such as `year_end_assessment` or `student_id_macros` in this example. Two sources of `packages` are currently supported:
* GitHub packages: Specify the URL of the repository containing the package. If the package YAML configuration is not in the top level of the repository, include the path to the folder with the the optional `subdirectory`.
* Local packages: Specify the relative or absolute path to the folder containing the package YAML configuration.


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
* File sources must specify the relative or absolute path to the source `file`. Supported file types are
  - Row-based formats:
    - `.csv`: Specify the number of `header_rows`, and (if `header_rows` > 0, optionally) overwrite the `column` names. Optionally specify an `encoding` to use when reading the file (the default is UTF8).
    - `.tsv`: Specify the number of `header_rows`, and (if `header_rows` > 0, optionally) overwrite the `column` names. Optionally specify an `encoding` to use when reading the file (the default is UTF8).
    - `.txt`: a fixed-width text file; column widths are inferred from the first 100 lines.
  - Column-based formats: `.parquet`, `.feather`, `.orc` &mdash; these require the [`pyarrow` library](https://arrow.apache.org/docs/python/index.html), which can be installed with `pip install pyarrow` or similar
  - Structured formats:
    - `.json`: Optionally specify a `object_type` (`frame` or `series`) and `orientation` (see [these docs](https://pandas.pydata.org/docs/reference/api/pandas.read_json.html)) to interpret different JSON structures.
    - `.jsonl` or `.ndjson`: reads files with a flat JSON structure per line.
    - `.xml`: Optionally specify an `xpath` to [select a set of nodes](https://pandas.pydata.org/docs/reference/api/pandas.read_xml.html) deeper in the XML.
    - `.html`: Optionally specify a regex to `match` for [selecting one of many tables](https://pandas.pydata.org/docs/reference/api/pandas.read_html.html) in the HTML. This can be used to extract tables from a live web page.
  - Excel formats: `.xls`, `.xlsx`, `.xlsm`, `.xlsb`, `.odf`, `.ods` and `.odt` &mdash; optionally specify the `sheet` name (as a string) or index (as an integer) to load.
  - Other formats:
    - `.pkl` or `.pickle`: a [pickled](https://docs.python.org/3/library/pickle.html) Python object (typically a Pandas dataframe)
    - `.sas7bdat`: a [SAS data file](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/hostwin/n0sk6o15955yoen19n9ghdziqw1u.htm)
    - `.sav`: a [SPSS data file](https://www.ibm.com/docs/en/spss-statistics/saas?topic=files-spss-statistics-data)
    - `.dta`: a [Stata data file](https://www.stata.com/manuals/gsw5.pdf)

  
  File type is inferred from the file extension, however you may manually specify `type:` (`csv`, `tsv`, `fixedwidth`, `parquet`, `feather`, `orc`, `json`, `jsonl`, `xml`, `html`, `excel`, `pickle`, `sas`, `spss`, or `stata`) to force `earthmover` to treat a file with an arbitrary extension as a certain type. Remote file paths (`https://somesite.com/path/to/file.csv`) generally work.
* Database sources are supported via [SQLAlchemy](https://www.sqlalchemy.org/). They must specify a database `connection` string and SQL `query` to run.
* FTP file sources are supported via [ftplib](https://docs.python.org/3/library/ftplib.html). They must specify an FTP `connection` string.

For any source, optionally specify conditions you `expect` data to meet which, if not true for any row, will cause the run to fail with an error. (This can be useful for detecting and rejecting NULL or missing values before processing the data.) The format must be a Jinja expression that returns a boolean value. This is enables casting values (which are all treated as strings) to numeric formats like int and float for numeric comparisons.

The examples above show `user:pass` in the `connection` string, but if you are version-controlling your YAML you must avoid publishing such credentials. Typically this is done via [environment variables](#environment-variable-references) or [command line parameters](#command-line-parameters), which are both supported by this tool. Such environment variable references may be used throughout your YAML (not just in the `sources` section), and are parsed at load time.


### **`transformations`**
The `transformations` section of the [YAML configuration](#yaml-configuration) specifies how source data is manipulated by the tool.

A sample `transformations` section is shown here; the options are explained below.
```yaml
transformations:
  courses:
    source: $sources.courses
    operations:
      - operation: map_values
        column: subject_id
        mapping:
          01: 1 (Mathematics)
          02: 2 (Literature)
          03: 3 (History)
          04: 4 (Language)
          05: 5 (Computer and Information Systems)
      - operation: join
        sources:
          - $sources.schools
        join_type: inner
        left_key: school_id
        right_key: school_id
      - operation: drop_columns
        columns:
          - address
          - phone_number
```
The above example shows a transformation of the `courses` source, which consists of an ordered list of operations. A transformation defines a source to which a series of operations are applied. This source may be an original `$source` or another `$transformation`. Transformation operations each require further specification depending on their type; the operations are listed and documented below.


#### Frame operations

<details>
<summary><code>union</code></summary>

Concatenates the transformation source with one or more sources sources of the same shape.
```yaml
      - operation: union
        sources:
          - $sources.courses_list_1
          - $sources.courses_list_2
          - $sources.courses_list_3
        fill_missing_columns: False
```
By default, unioning sources with different columns raises an error.
Set `fill_missing_columns` to `True` to union all columns into the output dataframe.
</details>


<details>
<summary><code>join</code></summary>

Joins the transformation source with one or more sources.
```yaml
      - operation: join
        sources:
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
</details>


#### Column operations

<details>
<summary><code>add_columns</code></summary>

Adds columns with specified values.
```yaml
      - operation: add_columns
        columns:
          new_column_1: value_1
          new_column_2: "{%raw%}{% if True %}Jinja works here{% endif %}{%endraw%}"
          new_column_3: "{%raw%}Reference values from {{AnotherColumn}} in this new column{%endraw%}"
          new_column_4: "{%raw%}{% if col1>col2 %}{{col1|float + col2|float}}{% else %}{{col1|float - col2|float}}{% endif %}{%endraw%}"
```
Use Jinja: `{{value}}` refers to this column's value; `{{AnotherColumn}}` refers to another column's value. Any [Jinja filters](https://jinja.palletsprojects.com/en/3.1.x/templates/#builtin-filters) and [math operations](https://jinja.palletsprojects.com/en/3.0.x/templates/#math) should work. Reference the current row number with `{{__row_number__}}` or a dictionary containing the row data with `{{__row_data__['column_name']}}`. *You must wrap Jinja expressions* in `{%raw%}...{%endraw%}` to avoid them being parsed at YAML load time.
</details>


<details>
<summary><code>rename_columns</code></summary>

Renames columns.
```yaml
      - operation: rename_columns
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
        columns:
          state_abbr: "{%raw%}XXX{{value|reverse}}XXX{%endraw%}"
          school_year: "{%raw%}20{{value[-2:]}}{%endraw%}"
          zipcode: "{%raw%}{{ value|int ** 2 }}{%endraw%}"
```
Use Jinja: `{{value}}` refers to this column's value; `{{AnotherColumn}}` refers to another column's value. Any [Jinja filters](https://jinja.palletsprojects.com/en/3.1.x/templates/#builtin-filters) and [math operations](https://jinja.palletsprojects.com/en/3.0.x/templates/#math) should work. Reference the current row number with `{{__row_number__}}` or a dictionary containing the row data with `{{__row_data__['column_name']}}`. *You must wrap Jinja expressions* in `{%raw%}...{%endraw%}` to avoid them being parsed at YAML load time.
</details>


<details>
<summary><code>map_values</code></summary>

Map the values of a column.
```yaml
      - operation: map_values
        column: column_name
        # or, to map multiple columns simultaneously
        columns:
          - col_1
          - col_2
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
        column: date_of_birth
        # or
        columns:
          - date_column_1
          - date_column_2
          - date_column_3
        from_format: "%b %d %Y %H:%M%p"
        to_format: "%Y-%m-%d"
        ignore_errors: False  # Default False
        exact_match: False    # Default False
```
The `from_format` and `to_format` must follow [Python's strftime() and strptime() formats](https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior).

When `ignore_errors` is set to True, empty strings will be replaced with Pandas NaT (not-a-time) datatypes.
This ensures column-consistency and prevents a mix of empty strings and timestamps.

When `exact_match` is set to True, the operation will only run successfully if the `from_format` input exactly matches the format of the date column.
When False, the operation allows the format to partially-match the target string.

</details>


<details>
<summary><code>snake_case_columns</code></summary>

Force the names of all columns to [snake_case](https://en.wikipedia.org/wiki/Snake_case).
```yaml
      - operation: snake_case_columns
```
</details>


#### Row operations

<details>
<summary><code>distinct_rows</code></summary>

Removes duplicate rows.
```yaml
      - operation: distinct_rows
        columns:
          - distinctness_column_1
          - distinctness_column_2
```
Optionally specify the `columns` to use for uniqueness, otherwise all columns are used. If duplicate rows are found, only the first is kept.
</details>


<details>
<summary><code>filter_rows</code></summary>

Filter (include or exclude) rows matching a query.
```yaml
      - operation: filter_rows
        query: school_year < 2020
        behavior: exclude | include
```
The query format is anything supported by [Pandas.DataFrame.query](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.query.html). Specifying `behavior` as `exclude` wraps the Pandas `query()` with `not()`.
</details>


<details>
<summary><code>sort_rows</code></summary>

Sort rows by one or more columns.
```yaml
      - operation: sort_rows
        columns:
          - sort_column_1
        descending: False
```
By default, rows are sorted ascendingly. Set `descending: True` to reverse this order.
</details>


#### Group operations

<details>
<summary><code>group_by</code></summary>

Reduce the number of rows by grouping, and add columns with values calculated over each group.
```yaml
      - operation: group_by
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
* `min(column)` - the minimum (numeric) value in `column` for each group
* `str_min(column)` - the minimum (string) value in `column` for each group
* `max(column)` - the maximum (numeric) value in `column` for each group
* `str_max(column)` - the maximum (string) value in `column` for each group
* `sum(column)` - the sum of (numeric) values in `column` for each group
* `mean(column)` or `avg(column)` - the mean of (numeric) values in `column` for each group
* `std(column)` - the standard deviation of (numeric) values in `column` for each group
* `var(column)` - the variance of (numeric) values in `column` for each group
* `agg(column,separator)` - the values of `column` in each group are concatenated, delimited by `separator` (default `separator` is none)

Numeric aggregation functions will fail with errors if used on non-numeric column values.

Note the difference between `min()`/`max()` and `str_min()`/`str_max()`: given a list like `10, 11, 98, 99, 100, 101`, return values are

|    function | return |
| ----------- | ------ |
|     `min()` |     10 |
| `str_min()` |     10 |
|     `max()` |    101 |
| `str_max()` |     99 |

</details>



### **`destinations`**
The `destinations` section of the [YAML configuration](#yaml-configuration) specifies how transformed data is materialized to files.

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
  course_report:
    source: $transformations.course_list
    template: ./json_templates/course.htmlt
    extension: html
    linearize: False
    header: <html><body><h1>Course List:</h1>
    footer: </body></html>
```
For each file you want materialized, provide the `source` and the `template` file &mdash; a text file (JSON, XML, HTML, etc.) containing Jinja with references to the columns of `source`. The materialized file will contain `template` rendered for each row of `source`, with an optional `header` prefix and `footer` postfix. Files are materialized using your specified `extension` (which is required).

If `linearize` is `True`, all line breaks are removed from the template, resulting in one output line per row. (This is useful for creating JSONL and other linear output formats.) If omitted, `linearize` is `True`.


## Global options

Any source, transformation, or destination may also specify `debug: True` which will output the dataframe shape and columns after the node completes processing. This can be very useful while building and debugging.

Additionally, the `show_progress` boolean flag can be specified on any source, transformation, or destination to display a progress bar while processing.

Finally, `repartition` can be passed to any node to repartition the node in memory before continuing to the next node.
Set either the number of bytes, or a text representation (e.g., "100MB") to shuffle data into new partitions of that size.
(Note: this configuration is advanced, and its use may drastically affect performance.)

# Usage
Once you have the required [setup](#setup) and your source data, run the transformations with
```bash
earthmover run -c path/to/config.yaml
```
If you omit the optional `-c` flag, `earthmover` will look for an `earthmover.yaml` in the current directory.

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

## Jinja in YAML configuration
You may use Jinja in the YAML configuration, which will be parsed at load time. Only the `config` section may not contain Jinja, except for `macros` which are made available both at parse-time for Jinja in the YAML configuration and at run-time for Jinja in `add_columns` or `modify_columns` transformation operations.

The following example
1. loads 9 source files
1. adds a column indicating the source file each row came from
1. unions the sources together
1. if an environment variable or parameter `DO_FILTERING=True` is passed, filters out certain rows

```jinja
config:
  show_graph: True
  parameter_defaults:
    DO_FILTERING: "False"

sources:
{% for i in range(1,10) %}
  source{{i}}:
    file: ./sources/source{{i}}.csv
    header_rows: 1
{% endfor %}

transformations:
{% for i in range(1,10) %}
  source{{i}}:
    source: $sources.source{{i}}
    operations:
      - operation: add_columns
        columns:
          source_file: {{i}}
{% endfor %}
  stacked:
    source: $transformations.source1
    operations:
      - operation: union
        sources:
{% for i in range(2,10) %}
          - $transformations.source{{i}}
{% endfor %}
{% if "${DO_FILTERING}"=="True" %}
      - operations: filter_rows
        query: school_year < 2020
        behavior: exclude
{% endif %}

destinations:
  final:
    source: $transformations.stacked
    template: ./json_templates/final.jsont
    extension: jsonl
    linearize: True
```


## Selectors
Run only portions of the [DAG](#dag) by using a selector:
```bash
earthmover run -c path/to/config.yaml -s people,people_*
```
This processes all DAG paths (from sources to destinations) through any matched nodes.

## Optional Sources
If you specify the `columns` list and `optional: True` on a file `source` but leave the `file` blank, `earthmover` will create an empty dataframe with the specified columns and pass it through the rest of the [DAG](#dag). This, combined with the use of [environment variable references](#environment-variable-references) and/or [command-line parameters](#command-line-parameters) to specify a `source`'s `file`, provides flexibility to include data when it's available but still run when it is missing.

## Environment variable references
In your [YAML configuration](#yaml-configuration), you may reference environment variables with `${ENV_VAR}`. This can be useful for making references to source file locations dynamic, such as
```yaml
sources:
  people:
    file: ${BASE_DIR}/people.csv
    ...
```
**Note:** because `os.path.expandvars()` [does not expand variables within single quotes](https://hg.python.org/cpython/file/v2.7.3/Lib/ntpath.py#l330) in Python under Windows, *Windows users should avoid placing environment variable references (or [CLI parameter](#command-line-parameters) references) inside single quote strings* in their YAML.

## Command-line parameters
Similarly, you can specify parameters via the command line with
```bash
earthmover run -c path/to/config.yaml -p '{"BASE_DIR":"path/to/my/base/dir"}'
earthmover run -c path/to/config.yaml --params '{"BASE_DIR":"path/to/my/base/dir"}'
```
Command-line parameters override any environment variables of the same name.

## State
This tool *maintains state about past runs.* Subsequent runs only re-process if something has changed &ndash; the [YAML configuration](#yaml-configuration) itself, data files of `sources`, `value_mapping` CSVs of `transformations`, template files of `destinations`, or CLI parameters. (Changes are tracked by hashing files; hashes and run timestamps are stored in the file specified by [config](#config)/`state_file`.) You may choose to override this behavior and force reprocessing of the whole DAG, regardless of whether files have changed or not, using the `-f` or `--force` command-line flag:
```bash
earthmover run -c path/to/config.yaml -f
earthmover run -c path/to/config.yaml --force-regenerate
```
To further avoid computing input hashes and not log a run to the `state_file`, use the `-k` or `--skip-hashing` flag: 
```bash
earthmover run -c path/to/config.yaml -k
earthmover run -c path/to/config.yaml --skip-hashing
```
(This makes a one-time run on large input files faster.) If earthmover skips running because nothing has changed, it returns bash exit code `99` (this was chosen because [it signals a "skipped" task in Airflow](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/bash.html#skipping)).

## Structured output of run results
To produce a JSON file with metadata about the run, invoke earthmover with
```bash
earthmover run -c path/to/config.yaml --results-file ./results.json
```
For example, for `example_projects/09_edfi/`, a sample results file would be:

```json
{
    "started_at": "2023-06-08T10:21:42.445835",
    "working_dir": "/home/someuser/code/repos/earthmover/example_projects/09_edfi",
    "config_file": "./earthmover.yaml",
    "output_dir": "./output/",
    "row_counts": {
        "$sources.schools": 6,
        "$sources.students_anytown": 1199,
        "$sources.students_someville": 1199,
        "$destinations.schools": 6,
        "$transformations.all_students": 2398,
        "$destinations.students": 2398,
        "$destinations.studentEducationOrganizationAssociations": 2398,
        "$destinations.studentSchoolAssociations": 2398
    },
    "completed_at": "2023-06-08T10:21:43.118854",
    "runtime_sec": 0.673019
}
```

## **Project composition**
An `earthmover` project can import and build upon other `earthmover` projects by importing them as packages, similar to the concept of dbt packages. When a project uses a package, any elements of the package can be overwritten by the project. This allows you to use majority of the code from a package and specify only the necessary changes in the project.

To install the packages specified in your [YAML Configuration](#yaml-configuration), run `earthmover deps`. Packages will be installed in a nested format in a `packages/` directory. Once packages are installed, `earthmover` can be run as usual. If you make any changes to the packages, run `earthmover deps` again to install the latest version of the packages. 

<details>
<summary>Example of a composed project</summary>

```yaml
# projA/earthmover.yml                     # projA/pkgB/earthmover.yml
config:                                    config:
  show_graph: True                           parameter_defaults:
  output_dir: ./output                         DO_FILTERING: "False"

packages:                                  sources:
  pkgB:                                      source1:
    local: pkgB                                file: ./seeds/source1.csv
                                               header_rows: 1
sources:                                     source2:
  source1:                                     file: ./seeds/source2.csv
    file: ./seeds/source1.csv                  header_rows: 1
    header_rows: 1                                           
                                           transformations:
destinations:                                trans1:
  dest1:                                       ...
    source: $transformations.trans1
    template: ./templates/dest1.jsont      destinations:
                                             dest1:
                                               source: $transformations.trans1
                                               template: ./templates/dest1.jsont
                                             dest2:
                                               source: $sources.source2
                                               template: ./templates/dest2.jsont
```
Composed results:
```yaml
config:
  show_graph: True
  output_dir: ./output 
  parameter_defaults:
    DO_FILTERING: "False"

packages:
  pkgB:
    local: pkgB

sources:
  source1:
    file: ./seeds/source1.csv
    header_rows: 1  
  source2: 
    file: ./packages/pkgB/seeds/source2.csv
    header_rows: 1    
    
transformations:
  trans1:
    ...

destinations:
  dest1:
    source: $transformations.trans1
    template: ./templates/dest1.jsont
  dest2:
    source: $sources.source2
    template: ./packages/pkgB/templates/dest2.jsont
```
</details>

### Project Composition Considerations
There is no limit to the number of packages that can be imported and no limit to how deeply they can be nested (i.e. packages can import other packages). However, there are a few things to keep in mind with using multiple packages.
* If multiple packages at the same level (e.g. `projA/packages/pkgB` and `projA/packages/pkgC`, not `projA/packages/pkgB/packages/pkgC`) include same-named nodes, the package specified later in the `packages` list will overwrite. If the node is also specified in the top-level project, its version of the node will overwrite as usual.
* A similar limitation exists for macros &ndash; a single definition of each macro will be applied everywhere in the project and packages using the same overwrite logic used for the nodes. When you are creating projects that are likely to be used as packages, consider including a namespace in the names of macros with more common operations, such as `assessment123_filter()` instead of the more generic `filter()`. 


# Tests
This tool ships with a test suite covering all transformation operations. It can be run with `earthmover -t`, which simply runs the tool on the `config.yaml` and toy data in the `earthmover/tests/` folder. (The DAG is pictured below.) Rendered `earthmover/tests/output/` are then compared against the `earthmover/tests/expected/` output; the test passes only if all files match exactly.

![tests DAG](https://raw.githubusercontent.com/edanalytics/earthmover/main/earthmover/tests/tests-dag.png)

Run tests with
```bash
earthmover -t
```


# Design
Some details of the design of this tool are discussed below.

## YAML parsing
`earthmover` [allows Jinja templating expressions in its YAML configuration files](#jinja-in-yaml-configuration). (This is similar to how [Ansible Playbooks](https://docs.ansible.com/ansible/latest/playbook_guide/playbooks.html) work.) `earthmover` parses the YAML in several steps:
1. Extract only the [`config` section](#config) (if any), in order to make available any `macros` when parsing the rest of the Jinja + YAML. The `config` section *only* **may not contain any Jinja** (besides `macros`).
1. Load the entire Jinja + YAML as a string and hydrate all [environment variable](#environment-variable-references) or [parameter](#command-line-parameters) references.
1. Parse the hydrated Jinja + YAML string with any `macros` to plain YAML.
1. Load the plain YAML string as a nested dictionary and begin building and processing the [DAG](#dag).

Note that due to step (3) above, *runtime* Jinja expressions (such as column definitions for `add_columns` or `modify_columns` operations) should be wrapped with `{%raw%}...{%endraw%}` to avoid being parsed when the YAML is being loaded.

The parsed YAML is written to a file called `earthmover_compiled.yaml` in your working directory during a `compile` command. This file can be used to debug issues related to compile-time Jinja or [project composition](#project-composition).


## DAG
The mapping of sources through transformations to destinations is modeled as a directed acyclic graph ([DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph)). Each [component](https://en.wikipedia.org/wiki/Component_(graph_theory)) of the DAG is run separately.

![dataflow graph components](https://raw.githubusercontent.com/edanalytics/earthmover/main/images/dataflow-graph-components.gif)

Each component is materialized in [topological order](https://en.wikipedia.org/wiki/Topological_sorting). This minimizes memory usage, as only the data from the current and previous layer must be retained in memory.

![dataflow graph layers](https://raw.githubusercontent.com/edanalytics/earthmover/main/images/dataflow-graph-layers.gif)

## Dataframes
All data processing is done using [Pandas Dataframes](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) and [Dask](https://www.dask.org/), with values stored as strings (or [Categoricals](https://pandas.pydata.org/docs/user_guide/categorical.html), for memory efficiency in columns with few unique values). This choice of datatypes prevents issues arising from Pandas' datatype inference (like inferring numbers as dates), but does require casting string-representations of numeric values using Jinja when doing comparisons or computations.


# Performance & Limitations
Tool performance depends on a variety of factors including source file size and/or database performance, the system's storage performance (HDD vs. SSD), memory, and transformation complexity. But some effort has been made to engineer this tool for high throughput and to work in memory- and compute-constrained environments.

Smaller source data (which all fits into memory) processes very quickly. Larger chunked sources are necessarily slower. We have tested with sources files of 3.3GB, 100M rows (synthetic attendance data): creating 100M lines of JSONL (30GB) takes around 50 minutes on a modern laptop.

The [state feature](#state) adds some overhead, as hashes of input data and JSON payloads must be computed and stored, but this can be disabled if desired.



# Best Practices
In this section we outline some suggestions for best practices to follow when using `earthmover`, based on our experience with the tool. Many of these are based on best practices for using [dbt](https://www.getdbt.com/), to which `earthmover` is similar, although `earthmover` operates on dataframes rather than database tables.

## Project Structure Practices
A typical `earthmover` project might have a structure like this:
```
project/
├── README.md
├── sources/
│   └── source_file_1.csv
│   └── source_file_2.csv
│   └── source_file_3.csv
├── earthmover.yaml
├── output/
│   └── output_file_1.jsonl
│   └── output_file_2.xml
├── seeds/
│   └── crosswalk_1.csv
│   └── crosswalk_2.csv
├── templates/
│   └── json_template_1.jsont
│   └── json_template_2.jsont
│   └── xml_template_1.xmlt
│   └── xml_template_2.xmlt
```
Generally you should separate the mappings, transformations, and structure of your data &ndash; which are probably *not* sensitive &ndash; from the actual input and output &ndash; which may be large and/or sensitive, and therefore should not be committed to a version control system. This can be accomplished in two ways:
1. include a `.gitignore` or similar file in your project which excludes the `sources/` and `output/` directories from being committed the repository
1. remove the `sources/` and `output/` directories from your project and update `earthmover.yaml`'s `sources` and `destinations` to reference another location outside the `project/` directory

When dealing with sensitive source data, you may have to comply with security protocols, such as referencing sensitive data from a network storage location rather than copying it to your own computer. In this situation, option 2 above is a good choice.

To facilitate [operationalization]($operationalization-practices), we recommended using [environment variables](#environment-variable-references) or [command-line parameters](#command-line-parameters) to pass input and output directories and filenames to `earthmover`, rather than hard-coding them into `earthmover.yaml`. For example, rather than
```yaml
config:
  output_dir: path/to/outputs/
...
sources:
  source_1:
    file: path/to/inputs/source_file_1.csv
    header_rows: 1
  source_2:
    file: path/to/inputs/source_file_2.csv
    header_rows: 1
...
destinations:
  output_1:
    source: $transformations.transformed_1
    ...
  output_2:
    source: $transformations.transformed_2
    ...
```
instead consider using
```yaml
config:
  output_dir: ${OUTPUT_DIR}
...
sources:
  source_1:
    file: ${INPUT_DIR}${INPUT_FILE_1}
    header_rows: 1
  source_2:
    file: ${INPUT_DIR}${INPUT_FILE_2}
    header_rows: 1
...
destinations:
  output_1:
    source: $transformations.transformed_1
    ...
  output_2:
    source: $transformations.transformed_2
    ...
```
and then run with
```bash
earthmover earthmover.yaml -p '{ "OUTPUT_DIR": "path/to/outputs/", "INPUT_DIR": "path/to/inputs/", "INPUT_FILE_1": "source_file_1.csv", "INPUT_FILE_2": "source_file_2.csv" }'
```
Note that with this pattern you can also use [optional sources](#optional-sources) to only create one of the outputs if needed, for example
```bash
earthmover earthmover.yaml -p '{ "OUTPUT_DIR": "path/to/outputs/", "INPUT_DIR": "path/to/inputs/", "INPUT_FILE_1": "source_file_1.csv" }'
```
would only create `output_1` if `source_1` had `required: False` (since `INPUT_FILE_2` is missing).

## Development practices
While YAML is a data format, it is best to treat the `earthmover` [YAML configuration](#yaml-configuration) as code, meaning you should
* [version](https://en.wikipedia.org/wiki/Version_control) it!
* avoid putting credentials and other sensitive information in the configuration; rather specify such values as [environment variables](#environment-variable-references) or [command-line parameters](#command-line-parameters)
* keep your YAML [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself) by using [Jinja macros](#config) and [YAML anchors and aliases](#definitions)

Remember that [code is poetry](https://medium.com/@launchyard/code-is-poetry-3d13d50a36b3): it should be beautiful! To that end
* Carefully choose concise, good names for your `sources`, `transformations`, and `destinations`.
  - Good names for `sources` could be based on their source file/table (e.g. `students` for `students.csv`)
  - Good names for `transformations` indicate what they do (e.g. `students_with_mailing_addresses`)
  - Good names for `destinations` could be based on the destination file (e.g. `student_mail_merge.xml`)
* Add good, descriptive comments throughout your YAML explaining any assumptions or non-intuitive operations (including complex Jinja).
* Likewise put Jinja comments in your templates, explaining any complex logic and structures.
* Keep YAML concise by composing `transformation` operations where possible. Many operations like `add_columns`, `map_values`, and others can operate on multiple `columns` in a dataframe.
* At the same time, avoid doing too much at once in a single `transformation`; splitting multiple `join` operations into separate transformations can make [debugging](#debugging-practices) easier.

## Debugging practices
When developing your transformations, it can be helpful to
* specify `config` &raquo; `log_level: DEBUG` and `transformation` &raquo; `operation` &raquo; `debug: True` to verify the columns and shape of your data after each `operation`
* turn on `config` &raquo; `show_stacktrace: True` to get more detailed error messages
* avoid name-sharing for a `source`, a `transformation`, and/or a `destination` - this is allowed but can make debugging confusing
* [install pygraphviz](https://pygraphviz.github.io/documentation/stable/install.html) and turn on `config` &raquo; `show_graph: True`, then visually inspect your transformations in `graph.png` for structural errors
* use a linter/validator to validate the formatting of your generated data

You can remove these settings once your `earthmover` project is ready for operationalization.

## Operationalization practices
Typically `earthmover` is used when the same (or similar) data transformations must be done repeatedly. (A one-time data transformation task may be more easily done with [SQLite](https://www.sqlite.org/index.html) or a similar tool.) When deploying/operationalizing `earthmover`, whether with a simple scheduler like [cron](https://en.wikipedia.org/wiki/Cron) or an orchestration tool like [Airflow](https://airflow.apache.org/) or [Dagster](https://dagster.io/), consider
* specifying conditions you `expect` your [sources](#sources) to meet, so `earthmover` will fail on source data errors
* specifying `config` &raquo; `log_level: INFO` and monitoring logs for phrases like
  > `distinct_rows` operation removed NN duplicate rows

  > `filter_rows` operation removed NN rows



# Changelog
See [CHANGELOG](CHANGELOG.md).



# Contributing
Bugfixes and new features (such as additional transformation operations) are gratefully accepted via pull requests here on GitHub.

## Contributions
* Cover image created with [DALL &bull; E mini](https://huggingface.co/spaces/dalle-mini/dalle-mini)



# License
See [License](LICENSE).


# Bundles
Bundles are pre-built data mappings for converting various data formats to Ed-Fi format using this tool. They consist of a folder with CSV seed data, JSON template files, and a `config.yaml` with sources, transformations, and destinations.

Here we maintain a list of bundles for various domain-specific uses:
* Bundles for transforming [assessment data from various vendors to the Ed-Fi data standard](https://github.com/edanalytics/earthmover_edfi_bundles)

If you develop bundles, please consider contributing them to the community by publishing them online and emailing the link to treitz@edanalytics.org to get them listed above.
