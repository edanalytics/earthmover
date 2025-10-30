## Project structure
An `earthmover` project consists of

* source data to be transformed, such as CSV or TSV files, or a relational database table
* an `earthmover.yml` YAML configuration file that defines
    * your project [`config`](#config)
    * (optional) [`definitions`](#definitions) of YAML anchors to be used elsewhere
    * (optional) [`packages`](#packages) to include in your project
    * data [`sources`](#sources) to transform
    * data [`transformations`](#transformations) to execute
    * data [`destinations`](#destinations) to materialize
* (optional) [Jinja templates](#jinja-templates) to be rendered for each row of the final `destination`

???+ tip "Tips"

    * To quickly see `earthmover` in action, run `earthmover init` to [initialize](usage.md#earthmover-init) a simple starter project.
    * Example `earthmover` projects can be found in the [example_projects/](https://github.com/edanalytics/earthmover/tree/main/example_projects) folder.
    * See the [recommended best practices](practices.md#project-structure) for building an `earthmover` project

Within an earthmover project folder, you can simply
```bash
earthmover run
```
to [run](usage.md#earthmover-run) the data transformations.

## YAML configuration

All the instructions for `earthmover` — where to find the source data, what transformations to apply to it, and how and where to save the output — are specified in a YAML configuration file, typically named `earthmover.yml`.

Note that `earthmover.yml` may also contain [Jinja](usage.md#jinja-in-yaml-configuration) and/or [parameters](usage.md#parameters) which are rendered in an [initial compilation step](design.md#yaml-compilation) before execution.

### `version`
A `version` label is required in `earthmover.yml` for compatibility reasons. The current `version` is `2`.

```yaml+jinja title="earthmover.yml"
version: 2
```

### `config`
The `config` section specifies various options for the operation of this tool.

A sample `config` section is shown here; the options are explained below.

```yaml+jinja title="earthmover.yml"
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

| Required? | Key | Type | Description | Default value |
|---|---|---|---|---|
| (optional) | `output_dir` | `string` | The folder where `destinations` will be materialized. | `./` (current directory) |
| (optional) | `state_file` | `string` | The file where [tool state](usage.md#state) is maintained. | `~/.earthmover.csv` on *nix systems, `C:/Users/USER/.earthmover.csv` on Windows systems |
| (optional) | `log_level` | `string` | The console output verbosity. Options include: <ul><li>`ERROR`: only output important errors like invalid YAML configuration, missing required sources, invalid DAG node references, etc.</li><li>`WARNING`: output errors and warnings, like when [tool state](usage.md#state) becomes large</li><li>`INFO`: all errors and warnings plus basic information about what earthmover is doing: start and stop, how many rows were removed by a `distinct_rows` or `filter_rows` operation, etc. (the default `log_level`)</li><li>`DEBUG`: all output above, plus verbose details about each transformation step, timing, memory usage, and more (recommended for [debugging transformations](practices.md#debugging-practices).)</li></ul> | `INFO` |
| (optional) | `tmp_dir` | `string` | The folder to use when dask must spill data to disk. | `/tmp` |
| (optional) | `show_graph` | `boolean` | Whether  or not to create `./graph.png` and `./graph.svg` showing the data dependency graph. (Requires [PyGraphViz](https://pygraphviz.github.io/) to be installed.) | `False` |
| (optional) | `show_stacktrace` | `boolean` | Whether to show a stacktrace for runtime errors. | `False` |
| (optional) | `macros` | `string` | [Jinja macros](https://jinja.palletsprojects.com/en/stable/templates/#macros) which will be available within any [template](#jinja-templates) throughout the project. (This can slow performance.) | (none) |
| (optional) | `parameter_defaults` | `dict` | Default values to be used if the user fails to specify a [parameter](usage.md#parameters). | (none) |
| (optional) | `show_progress` | `boolean` | Whether to show a progress bar for each Dask transformation | `False` |
| (optional) | `git_auth_timeout` | `integer` | Number of seconds to wait for the user to enter Git credentials if needed during package installation (see [project composition](usage.md#earthmover-deps)) | `60` |


### `definitions`

The (optional) `definitions` section can be used to define YAML elements which are reused throughout the rest of the configuration. `earthmover` does nothing special with this section, it's just interpreted by the YAML parser. However, this can be a very useful way to keep your YAML configuration [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself) – rather than redefine the same values, Jinja phrases, etc. throughout your config, define them once in this section and refer to them later using [YAML anchors, aliases, and overrides](https://www.linode.com/docs/guides/yaml-anchors-aliases-overrides-extensions/).

An example `definitions` section, and how it can be used later on, are shown below:

```yaml+jinja title="earthmover.yml"
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

### `packages`

The (optional) `packages` section can be used to specify packages – other earthmover projects from a local directory or GitHub – to import and build upon exisiting code. See [Project Composition](usage.md#project-composition) for details and considerations.

A sample `package`s section is shown here; the options are explained below.

```yaml+jinja title="earthmover.yml"
packages:
  year_end_assessment:
    git: https://github.com/edanalytics/earthmover_edfi_bundles.git
    subdirectory: assessments/assessment_name
  student_id_macros:
    local: path/to/student_id_macros
```
Each package must have a name (which will be used to name the folder where it is installed in `./packages/`) such as `year_end_assessment` or `student_id_macros` in the above example. Two sources of packages are currently supported:

* GitHub packages: Specify the URL of the repository containing the package. If the package YAML configuration is not in the top level of the repository, include the path to the folder with the the optional subdirectory.

    !!! tip
        `earthmover` uses the system's `git` client to clone packages from GitHub. To access non-public packages, the `git` client must have [authentication configured separately](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/about-authentication-to-github#authenticating-with-the-command-line).

* Local packages: Specify the path to the folder containing the package YAML configuration. Paths may be absolute or relative paths to the location of the `earthmover` YAML configuration file.

### `sources`

The `sources` section specifies source data `earthmover` will transform.

A sample `sources` section is shown here; the options are explained below.

```yaml+jinja title="earthmover.yml"
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

Each source must have a name (which is how it is referenced by transformations and destinations) such as `districts`, `courses`, `tx_schools`, or `more_schools` in the above example. Currently-supported source types include:

| source type | format | file type | notes |
|---|---|---|---|
| file {: rowspan=13} | row-based {: rowspan=3} | `.csv` | Specify the number of `header_rows`, and (if `header_rows` > 0, optionally) overwrite the `column` names. Optionally specify an `encoding` to use when reading the file (the default is UTF8). |
| `.tsv` | Specify the number of `header_rows`, and (if `header_rows` > 0, optionally) overwrite the `column` names. Optionally specify an `encoding` to use when reading the file (the default is UTF8). |
| `.txt` | A fixed-width text file; see the [documentation below](#fixed-width-config) for configuration details. |
| column-based | `.parquet`, `.feather`, `.orc` | These require the [`pyarrow` library](https://arrow.apache.org/docs/python/index.html), which can be installed with `pip install pyarrow` or similar |
| structured {: rowspan=4} | `.json` | Optionally specify a `object_type` (`frame` or `series`) and `orientation` (see [these docs](https://pandas.pydata.org/docs/reference/api/pandas.read_json.html)) to interpret different JSON structures. |
| `.jsonl` or `.ndjson` | Files with a flat JSON structure per line. |
| `.xml` | Optionally specify an `xpath` to [select a set of nodes](https://pandas.pydata.org/docs/reference/api/pandas.read_xml.html) deeper in the XML. |
| `.html` | Optionally specify a regex to `match` for [selecting one of many tables](https://pandas.pydata.org/docs/reference/api/pandas.read_html.html) in the HTML. This can be used to extract tables from a live web page. |
| Excel | `.xls`, `.xlsx`, `.xlsm`, `.xlsb`, `.odf`, `.ods` and `.odt` | Optionally specify the `sheet` name (as a string) or index (as an integer) to load. |
| other {: rowspan=4} | `.pkl` or `.pickle` | A [pickled](https://docs.python.org/3/library/pickle.html) Python object (typically a Pandas dataframe) |
| `.sas7bdat` | A [SAS data file](https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/hostwin/n0sk6o15955yoen19n9ghdziqw1u.htm) |
| `.sav` | A [SPSS data file](https://www.ibm.com/docs/en/spss-statistics/saas?topic=files-spss-statistics-data) |
| `.dta` | A [Stata data file](https://www.stata.com/manuals/gsw5.pdf) |
| Database | various | - | Database sources are supported via [SQLAlchemy](https://www.sqlalchemy.org/). They must specify a database `connection` string and SQL `query` to run. |
| FTP | various | - | FTP file sources are supported via [ftplib](https://docs.python.org/3/library/ftplib.html). They must specify an FTP `connection` string with the path to the file. |

File `type` is inferred from the file extension, however you may manually specify `type:` (`csv`, `tsv`, `fixedwidth`, `parquet`, `feather`, `orc`, `json`, `jsonl`, `xml`, `html`, `excel`, `pickle`, `sas`, `spss`, or `stata`) to force `earthmover` to treat a file with an arbitrary extension as a certain type. Remote file paths (`https://somesite.com/path/to/file.csv`) generally work.


#### Fixed-width config
Using a fixed-width file (FWF) as a source requires additional metadata, configuring how `earthmover` should slice each row into its constituent columns. Two ways to provide this metadata are supported:

??? info "Provide a `colspec_file`"

    Example configuration for a `fixedwidth` source with a `colspec_file`:

    ```yaml
    sources:
      input:
        file: ./data/input.txt
        colspec_file: ./seed/colspecs.csv # required
        colspec_headers:
          name: field_name                # required
          start: start_index              # required if `width` is not provided
          end: end_index                  # required if `width` is not provided
          width: field_length             # required if `start` or `end` is not provided
        type: fixedwidth                  # required if `file` does not end with '.txt'
        header_rows: 0
    ```

    Notes:

    - (required) `colspec_file`: path to the CSV containing `colspec` details
    - (required) `colspec_headers`: mapping between the `colspec_file`'s column names and fields required by `earthmover`. (Columns may have any name and position.)
        - Only `name` is always required: `colspec_file` must contain a column that assigns a name to each field in the FWF
        - Either `width` or both `start` and `end` are required
            - If `width` is provided, `colspec_file` should include a column of integer values indicating the number of characters in each field in the FWF
            - If `start` and `end` are provided, `colspec_file` should include two columns of integer values [giving extents of the FWF's fields as half-open intervals (i.e., \[from, to\[ )](https://pandas.pydata.org/docs/reference/api/pandas.read_fwf.html) 
    - (optional) `type`: optional if source file has `.txt` extension; otherwise, specify `type: fixedwidth` (there is no standard file extension for FWFs)
    - (optional) `header_rows`: usually 0 for FWFs; `earthmover` attempts to infer if not specified

    **Formatting a `colspec_file`**

    A `colspec_file` must include a column with field names, and
    
    1. either a column with field widths
    1. or two columns with start and end positions

    Example of (1):

    ```csv
    name,width
    date,8
    id,16
    score_1,2
    score_2,2
    ```
    with `earthmover.yaml` like:

    ```yaml
    colspec_headers:
      name: name
      width: width
    ```

    Example of (2):

    ```csv
    start_idx, end_idx, other_data, full_field_name, other_data_2
    0, 8, abc, date, def
    8, 24, abc, id, def
    24, 26, abc, score_1, def
    26, 28, abc, score_2, def
    ```
    with `earthmover.yaml` like:

    ```yaml
    colspec_headers:
      name: full_field_name
      start: start_idx
      end: end_idx
    ```

??? info "Provide `colspecs` and `columns`"

    Example configuration for a `fixedwidth` source with a `colspecs` and `columns`:

    ```yaml
    sources:
      input:
        file: ./data/input.txt
        type: fixedwidth        # required if `file` does not end with '.txt'
        header_rows: 0
        colspecs:               # required
          - [0, 8]
          - [8, 24]
          - [24, 26]
          - [26, 28]
        columns:                # required
          - date
          - id
          - score_1
          - score_2
    ```

    Notes:

    - (required) `colspecs`: list of start and end indices [giving extents of the FWF's fields as half-open intervals (i.e., \[from, to\[ )](https://pandas.pydata.org/docs/reference/api/pandas.read_fwf.html) 
    - (required) `columns`: list of column names corresponding to the indices in `colspecs`


<hr />

#### `source` examples

=== "`.csv`"

    ```yaml+jinja
    sources:
      mydata:
        file: ./data/mydata.csv
        header_rows: 1
    ```

    (for an input file like...)

    ```csv title="mydata.csv"
    id,year,code
    1234,2005,54321
    1235,2006,54322
    ...
    ```

=== "`.tsv`"

    ```yaml+jinja
    sources:
      mydata:
        file: ./data/mydata.tsv
        header_rows: 1
    ```

    (for an input file like...)

    ```tsv title="mydata.tsv"
    id  year    code
    1234    2005   54321
    1235    2006   54322
    ...
    ```

=== "`.txt` (fixed-width file)"

    ```yaml+jinja
    sources:
      mydata:
        file: ./data/mydata.txt
        colspecs:
          - [0, 4]   # id
          - [6, 9]   # year
          - [10, 20] # code
          - ...
        columns:
          - id
          - year
          - code
          - ...
        # or
        colspec_file: ./mydata_colspec.csv
        # where `mydata_colspec.csv` is a file with columns `col_name`, `start_pos`, and `end_pos`
    ```

    (for an input file like...)

    ```txt title="mydata.txt"
    1234 2005    54321
    1235 2006    54322
    ...
    ```

=== "`.parquet`"

    ```yaml+jinja
    sources:
      mydata:
        file: ./data/mydata.parquet
        columns:
        - school_id
        - school_year
        - course_code
        - ...
    ```

=== "`.json`"

    ```yaml+jinja
    sources:
      mydata:
        file: ./data/mydata.json
        orientation: records
    ```

    (for an input file like...)

    ```json title="mydata.json"
    [
        {
            "column1": "value1",
            "column2": "value2",
            ...
        },
        {
            "column1": "value3",
            "column2": "value4",
            ...
        },
        ...
    ]
    ```

=== "`.jsonl`"

    ```yaml+jinja
    sources:
      mydata:
        file: ./data/mydata.jsonl
    ```

    (for an input file like...)

    ```json title="mydata.jsonl"
    { "column1": "value1", "column2": "value2", ...}
    { "column1": "value3", "column2": "value4", ...}
    ...
    ```

=== "`.xml`"

    ```yaml+jinja
    sources:
      mydata:
        file: ./data/mydata.xml
        xpath: //SomeElement
    ```

    (for an input file like...)

    ```xml title="mydata.xml"
    <SomeElement>
        <Attribute1>Value1</Attribute1>
        <Attribute2>Value2</Attribute2>
        ...
    </SomeElement>
    <SomeElement>
        <Attribute1>Value3</Attribute1>
        <Attribute2>Value4</Attribute2>
        ...
    </SomeElement>
    ...
    ```

=== "Database"

    ```yaml+jinja
    sources:
      mydata:
        connection: "postgresql://user:pass@host/mydatabase"
        query: >
          select id, year, code, ...
          from myschema.mytable
          where some_column='some_value'
    ```

=== "FTP file"

    ```yaml+jinja
    sources:
      mydata:
        connection: "ftp://user:pass@host:port/path/to/mydata.csv"
    ```

<hr />

#### `expect`ations
For any source, optionally specify conditions you `expect` data to meet which, if not true for any row, will cause the run to fail with an error. (This can be useful for detecting and rejecting NULL or missing values before processing the data.) The format must be a Jinja expression that returns a boolean value. This is enables casting values (which are all treated as strings) to numeric formats like int and float for numeric comparisons.

```yaml+jinja
sources:
  school_directory_grades_only:
    file: ./sources/school_directory.csv
    header_rows: 1
    expect:
      - "low_grade != ''"
      - "high_grade != ''"
    # ^ require a `low_grade` and a `high_grade`
```

#### credentials

The Database and FTP examples above show `user:pass` in the connection string, but if you are version-controlling your `earthmover` project, you must avoid publishing such credentials. Typically this is done via [parameters](usage.md#parameters), which may be referenced throughout `earthmover.yml` (not just in the `sources` section), and are parsed at run-time. For example:

```yaml+jinja
sources:
  mydata:
    connection: "postgresql://${DB_USERNAME}:${DB_PASSWORD}@host/mydatabase"
    query: >
      select id, year, code, ...
      from myschema.mytable
      where some_column='some_value'
```
and then
```bash
export DB_USERNAME=myuser
export DB_PASSWORD=mypa$$w0rd
earthmover run
# or
earthmover run -p '{"DB_USERNAME":"myuser", "DB_PASSWORD":"mypa$$w0rd"}'
```


### `transformations`

The `transformations` section specifies how source data is transformed by `earthmover`.

A sample `transformations` section is shown here; the options are explained below.

```yaml title="earthmover.yml"
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

The above example shows a transformation of the courses source, which consists of an ordered list of operations. A transformation defines a source to which a series of operations are applied. This source may be an original `$source` or another `$transformation`. Transformation operations each require further specification depending on their type; the operations are listed and documented below.

#### Frame operations

??? example "union"

    Concatenates or "stacks" the transformation source with one or more sources of the same shape.

    ```yaml
        - operation: union
            sources:
            - $sources.courses_list_1
            - $sources.courses_list_2
            - $sources.courses_list_3
            fill_missing_columns: False
    ```


??? example "join"

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

??? example "melt"

    Unpivots the source from wide to long format, where the names of columns become values in a generic "variable" column, and the values of those columns shift to a generic "values" column

    ```yaml
        - operation: melt
            sources:
            - $sources.score_col_for_each_subject
            # Column(s) from the wide dataset to keep as row identifiers in the long dataset
            id_vars: student_id
            # Column(s) from the wide dataset to translate into values of the "variable" columns. If omitted, melt all columns not in id_vars. 
            value_vars: [math_score, reading_score, science_score, writing_score]
            # Optional name of the column containing value_vars. default to "melt_variable"
            var_name: subject
            # Optional name of the column containing the values of the value_vars columns. Default to "melt_value"
            value_name: score
    ```

??? example "pivot"

    Opposite of `melt` - pivots a source from long to wide format, where the unique values of old columns become the column names of new ones

    ```yaml
        - operation: pivot
            sources:
            - $sources.melted_scores
            # Column in long dataset whose unique values are names of columns in the wide dataset - to pivot by multiple columns, use sequential pivots
            cols_by: subject
            # Column(s) in long dataset that define the rows in the wide dataset
            rows_by: student_id
            # Column(s) in long dataset used to populate the wide dataset's columns
            values: score
    ```

<hr />

#### Column operations

???+ tip "Tip: use Jinja!"

    The `add_columns` and `modify_columns` operations (documented below) support the use of [Jinja templating](https://jinja.palletsprojects.com/en/stable/templates/) to add/modify values dynamically. In the column definitions for such operations:

    * `{{value}}` refers to this column's value
    * `{{AnotherColumn}}` refers to another column's value
    * Any [Jinja filters](https://jinja.palletsprojects.com/en/3.1.x/templates/#builtin-filters) and [math operations](https://jinja.palletsprojects.com/en/3.0.x/templates/#math) should work
    * Reference the current row number with `{{__row_number__}}`
    * Reference a dictionary containing the row data with `{{__row_data__['column_name']}}`
    
    Jinja expressions must be wrapped in `{%raw%}...{%endraw%}` to avoid them being parsed by [the initial parsing step](design.md#yaml-compilation).


???+ tip "Tip: wildcard matching for column names"

    The `modify_columns`, `drop_columns`, `keep_columns`, `combine_columns`, `map_values`, and `date_format` operations (documented below) support the use of wildcard matching (via [fnmatch](https://docs.python.org/3/library/fnmatch.html)) so apply a transformation to multiple columns. Example:

    ```yaml
    - operation: drop_columns
      columns:
        - "*_suffix"             # matches `my_suffix` and `your_suffix`
        - prefix_*             # matches `prefix_1` and `prefix_2`
        - "*_sandwich_*"         # matches `my_sandwich_1` and `your_sandwich_2`
        - single_character_?   # matches `single_character_A`, `single_character_B`, etc.
        - number_[0123456789]  # matches `number_1`, `number_2`, etc.
    ```

    **Note that** this feature means that if your data frame's columns legitimately contain the special characters `*`, `?`, `[`, or `]`, you must first use `rename_columns` to remove those characters before using an operation that supports wildcard matching to avoid unexpected results.

    **Note also that** column specifications cannot begin with `*` or the YAML parser would interpret them as an anchor reference. Hence the examples above are quoted (`"*_..."`), which is the proper way to use wildcard prefixes while avoiding YAML parse errors.


??? example "add_columns"

    Adds columns with specified values.

    ```yaml
        - operation: add_columns
            columns:
            new_column_1: value_1
            new_column_2: "{%raw%}{% if True %}Jinja works here{% endif %}{%endraw%}"
            new_column_3: "{%raw%}Reference values from {{AnotherColumn}} in this new column{%endraw%}"
            new_column_4: "{%raw%}{% if col1>col2 %}{{col1|float + col2|float}}{% else %}{{col1|float - col2|float}}{% endif %}{%endraw%}"
    ```


??? example "rename_columns"

    Renames columns.

    ```yaml
        - operation: rename_columns
            columns:
            old_column_1: new_column_1
            old_column_2: new_column_2
            old_column_3: new_column_3
    ```


??? example "duplicate_columns"

    Duplicates columns (and all their values).

    ```yaml
        - operation: duplicate_columns
            columns:
            existing_column1: new_copy_of_column1
            existing_column2: new_copy_of_column2
    ```


??? example "drop_columns"

    Removes the specified columns.

    ```yaml
        - operation: drop_columns
            columns:
            - column_to_drop_1
            - column_to_drop_2
    ```


??? example "keep_columns"

    Keeps only the specified columns, discards the rest.

    ```yaml
        - operation: keep_columns
            columns:
            - column_to_keep_1
            - column_to_keep_2
    ```


??? example "combine_columns"

    Combines the values of the specified columns, delimited by a separator, into a new column.

    ```yaml
        - operation: combine_columns
            columns:
            - column_1
            - column_2
            new_column: new_column_name
            separator: "_"
    ```


??? example "modify_columns"

    Modify the values in the specified columns.

    ```yaml
        - operation: modify_columns
            columns:
            state_abbr: "{%raw%}XXX{{value|reverse}}XXX{%endraw%}"
            school_year: "{%raw%}20{{value[-2:]}}{%endraw%}"
            zipcode: "{%raw%}{{ value|int ** 2 }}{%endraw%}"
            "*": "{%raw%}{{value|trim}}{%endraw%}" # Edit all values in dataframe
    ```


??? example "map_values"

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
            # or a CSV/TSV with two columns (from, to) and header row
            # paths may be absolute or relative paths to the location of the `earthmover` YAML configuration   file
            map_file: path/to/mapping.csv
    ```



??? example "date_format"

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

    When `ignore_errors` is `True`, empty strings will be replaced with Pandas NaT (not-a-time) datatypes. This ensures column-consistency and prevents a mix of empty strings and timestamps.

    When `exact_match` is `True`, the operation will only run successfully if the `from_format` input exactly matches the format of the date column. When `False`, the operation allows the format to partially-match the target string.


??? example "snake_case_columns"

    Force the names of all columns to [snake_case](https://en.wikipedia.org/wiki/Snake_case).

    ```yaml
        - operation: snake_case_columns
    ```

<hr />

#### Row operations

??? example "distinct_rows"

    Removes duplicate rows.

    ```yaml
        - operation: distinct_rows
            columns:
            - distinctness_column_1
            - distinctness_column_2
    ```

    Optionally specify the `columns` to use for uniqueness, otherwise all columns are used. If duplicate rows are found, only the first is kept.


??? example "filter_rows"

    Filter (include or exclude) rows matching a query.

    ```yaml
        - operation: filter_rows
            query: school_year < 2020
            behavior: exclude # or `include`
    ```

    The query format is anything supported by [Pandas.DataFrame.query](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.query.html). Specifying behavior as `exclude` wraps the Pandas `query()` with `not()`.


??? example "sort_rows"

    Sort rows by one or more columns.

    **Simple sorting**
    ```yaml
        - operation: sort_rows
            columns:
            - sort_column_1
            descending: False
    ```

    By default, rows are sorted ascendingly. Set `descending: True` to reverse this order.

    **Multidirectional sorting (new syntax)**
    Specify the sort direction for each column using + (ascending) or - (descending)
    
    ```yaml
        - operation: sort_rows
            columns:
            - +sort_column_1 # "+" is optional; ascending by default
            - -sort_column_2 # "-" indicates descending;
    ```
    Or use a compact list format:
    
    ```yaml
        - operation: sort_rows
            columns: [ sort_column_1, -sort_column_2 ]
    ```
    
    !!! tip
          `earthmover` cannot distinguish between a leading `+` that is part of the column name vs. a leading `+` that denotes "sort ascendingly;" the first leading `+` will always be removed. Consider prefixing the column explicitly with the sorting direction (e.g. `++sort_column_1`) or renaming the column using `rename_columns`.



??? example "limit_rows"

    Limit the number of rows in the dataframe.

    ```yaml
        - operation: limit_rows
            count: 5 # required, no default
            offset: 10 # optional, default 0
    ```

    (If fewer than count rows in the dataframe, they will all be returned.)


??? example "flatten"

    Split values in a column and create a copy of the row for each value.

    ```yaml
        - operation: flatten
            flatten_column: my_column
            left_wrapper: '["' # characters to trim from the left of values in `flatten_column`
            right_wrapper: '"]' # characters to trim from the right of values in `flatten_column`
            separator: ","  # the string by which to split values in `flatten_column`
            value_column: my_value # name of the new column to create with flattened values
            trim_whitespace: " \t\r\n\"" # characters to trim from `value_column` _after_ flattening
    ```

    The defaults above are designed to allow flattening JSON arrays (in a string) with simply

    ```yaml
        - operation: flatten
            flatten_column: my_column
            value_column: my_value
    ```

    Note that for empty string values or empty arrays, a row will still be preserved. These can be removed in a second step with a `filter_rows` operation. Example:

    ```yaml
    # Given a dataframe like this:
    #   foo     bar    to_flatten
    #   ---     ---    ----------
    #   foo1    bar1   "[\"test1\",\"test2\",\"test3\"]"
    #   foo2    bar2   ""
    #   foo3    bar3   "[]"
    #   foo4    bar4   "[\"test4\",\"test5\",\"test6\"]"
    # 
    # a flatten operation like this:
        - operation: flatten
            flatten_column: to_flatten
            value_column: my_value
    # will produce a dataframe like this:
    #   foo     bar    my_value
    #   ---     ---    --------
    #   foo1    bar1   test1
    #   foo1    bar1   test2
    #   foo1    bar1   test3
    #   foo2    bar2   ""
    #   foo3    bar3   ""
    #   foo4    bar4   test4
    #   foo4    bar4   test5
    #   foo4    bar4   test6
    #
    # and you can remove the blank rows if needed with a further operation:
        - operation: filter_rows
            query: my_value == ''
            behavior: exclude
    ```

<hr />

#### Group operations

??? example "group_by"

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
    * `json_array_agg(column,[str])` - the values of `column` in each group are concatenated into a JSON array (`[1,2,3]`). If the optional `str` argument is provided, the values in the array are quoted (`["1", "2", "3"]`)

    Numeric aggregation functions will fail with errors if used on non-numeric column values.

    Note the difference between `min()`/`max()` and `str_min()`/`str_max()`: given a list like `10, 11, 98, 99, 100, 101`, return values are

    | function | return |
    | --- | --- |
    | `min()` | 10 |
    | `str_min()` | 10 |
    | `max()` | 101 |
    | `str_max()` | 99 |


<hr />

#### Debug operation

??? example "debug"

    Prints out information about the data at the current transformation operation.

    ```yaml
        - operation: debug
            function: head # or `tail`, `describe`, `columns`; default=`head`
            rows: 10 # (optional, default=5; ignored if function=describe|columns)
            transpose: True # (default=False; ignored when function=columns)
            skip_columns: [a, b, c] # to avoid logging PII
            keep_columns: [x, y, z] # to look only at specific columns
    ```


    * `function=head|tail` displays the `rows` first or last rows of the dataframe, respectively. (Note that on large dataframes, these may not truly be the first/last rows, due to Dask's memory optimizations.)
    * `function=describe` shows statistics about the values in the dataframe.
    * `function=columns` shows the column names in the dataframe.
    * `transpose` can be helpful with very wide dataframes.
    * `keep_columns` defaults to all columns, `skip_columns` defaults to no columns.



### `destinations`

The `destinations` section specifies how transformed data is materialized to files.

A sample destinations section is shown here; the options are explained below.

```yaml title="earthmover.yml"
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

For each file you want materialized, provide the `source` and (optionally) the [Jinja `template`](#jinja-templates) file. The materialized file will contain `template` rendered for each row of source, with an optional `header` prefix and `footer` postfix (both of which may contain Jinja, and which may reference `__row_data__` which is the first row of the data frame... a formulation such as `{%raw%}{% for k in __row_data__.pop('__row_data__').keys() %}{{k}}{% if not loop.last %},{% endif %}{% endfor %}{%endraw%}` may be useful). Files are materialized using your specified `extension` (which is required).

If `linearize` is `True`, all line breaks are removed from the template, resulting in one output line per row. (This is useful for creating JSONL and other linear output formats.) If omitted, `linearize` is `True`.

### Global options
Any `source`, `transformation`, or `destination` node may additionally specify

* `debug: True`, which outputs the dataframe shape and columns after the node completes processing (this can be helpful for building and debugging)
* `require_rows: True` or `require_rows: 10` to have earthmover exit with an error if `0` (for `True`) or `<10` (for `10`) rows are present in the dataframe after the node completes processing
* `show_progress: True` to display a progress bar while processing this node
* `repartition: True` to repartition the node in memory before continuing to the next node; set either the number of bytes, or a text representation (e.g., "100MB") to shuffle data into new partitions of that size (Note: this configuration is advanced, and its use may drastically affect performance)

## Jinja templates
`earthmover` destinations can specify a Jinja template to render for each row of the final dataframe — a text file (JSON, XML, HTML, etc.) containing Jinja with references to the columns of the row.

An example Jinja template for JSON could be
```json title="my_template.jsont"
{
  "studentUniqueId": "{{student_id}}",
  "birthDate": "{{birth_date}}",
  "firstName": "{{first_name}}",
  "lastSurname": "{{last_name}}"
}
```

Using a [linter](https://en.wikipedia.org/wiki/Lint_(software)) on the output of `earthmover run` can help catch syntax mistakes when developing Jinja templates.
