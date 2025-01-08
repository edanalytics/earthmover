In this section we outline some suggestions for best practices to follow when using `earthmover`, based on our experience with the tool. Many of these are based on best practices for using [dbt](https://www.getdbt.com/), to which `earthmover` [is similar](design.md#comparison-to-dbt), although `earthmover` operates on dataframes rather than database tables.

## Project structure
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
The mappings, transformations, and structure of your data &ndash; which are probably *not* sensitive &ndash; should generally be separated from the actual input and output &ndash; which may be large and/or sensitive, and therefore should not be committed to a version control system. This can be accomplished in two ways:

1. include a `.gitignore` or similar file in your project which excludes the `sources/` and `output/` directories from being committed the repository
1. remove the `sources/` and `output/` directories from your project and update `earthmover.yaml`'s `sources` and `destinations` to reference another location outside the `project/` directory

When dealing with sensitive source data, you may have to comply with security protocols, such as referencing sensitive data from a network storage location rather than copying it to your own computer. In this situation, option 2 above is a good choice.

To facilitate [operationalization](#operationalization-practices), we recommended using relative paths from the location of the `earthmover.yaml` file and using [parameters](usage.md#parameters) to pass dynamic filenames to `earthmover`, instead of hard-coding them. For example, rather than
```yaml
config:
  output_dir: /path/to/outputs/
...
sources:
  source_1:
    file: /path/to/inputs/source_file_1.csv
    header_rows: 1
  source_2:
    file: /path/to/inputs/source_file_2.csv
    header_rows: 1
  seed_1:
    file: /path/to/seeds/seed_1.csv
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
    file: ${INPUT_FILE_1}
    header_rows: 1
  source_2:
    file: ${INPUT_FILE_2}
    header_rows: 1
  seed_1:
    file: ./seeds/seed_1.csv
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
earthmover -p '{ "OUTPUT_DIR": "/path/to/outputs/", \
"INPUT_FILE_1": "/path/to/source_file_1.csv", \
"INPUT_FILE_2": "/path/to/source_file_2.csv" }'
```
Note that with this pattern you can also use [optional sources](usage.md#optional-sources) to only create one of the outputs if needed, for example
```bash
earthmover -p '{ "OUTPUT_DIR": "/path/to/outputs/", \
"INPUT_FILE_1": "/path/to/source_file_1.csv" }'
```
would only create `output_1` if `source_1` had `required: False` (since `INPUT_FILE_2` is missing).

## Development practices
While YAML is a data format, it is best to treat the `earthmover` [YAML configuration](configuration.md#yaml-configuration) as code, meaning you should

* [version](https://en.wikipedia.org/wiki/Version_control) it!
* avoid putting credentials and other sensitive information in the configuration; rather specify such values as [parameters](usage.md#parameters)
* keep your YAML [DRY](https://en.wikipedia.org/wiki/Don%27t_repeat_yourself) by using [Jinja macros](configuration.md#config) and [YAML anchors and aliases](configuration.md#definitions)

Remember that [code is poetry](https://medium.com/@launchyard/code-is-poetry-3d13d50a36b3): it should be beautiful! To that end

* Carefully choose concise, good names for your `sources`, `transformations`, and `destinations`.
    - Good names for `sources` could be based on their source file/table (e.g. `students` for `students.csv`)
    - Good names for `transformations` indicate what they do (e.g. `students_with_mailing_addresses`)
    - Good names for `destinations` could be based on the destination file (e.g. `student_mail_merge.xml`)
* Add good, descriptive comments throughout your YAML explaining any assumptions or non-intuitive operations (including complex Jinja).
* Likewise put Jinja comments in your templates, explaining any complex logic and structures.
* Keep YAML concise by consolidating `transformation` operations where possible. Many operations like `add_columns`, `map_values`, and others can operate on multiple `columns` in a dataframe.
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

* specifying conditions you [`expect`](configuration.md#expectations) your [sources](configuration.md#sources) to meet, so `earthmover` will fail on source data errors
* specifying [`config`](configuration.md#config) &raquo; `log_level: INFO` and monitoring logs for phrases like

    > `distinct_rows` operation removed NN duplicate rows

    > `filter_rows` operation removed NN rows

* using the [structured run output flag](usage.md#structured-run-output) and shipping the output somewhere it can be queried or drive a monitoring dashboard
