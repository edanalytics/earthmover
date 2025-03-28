This page discusses some aspects of `earthmover`'s design.

## YAML compilation
`earthmover` [allows Jinja templating expressions in its YAML configuration files](usage.md#jinja-in-yaml-configuration). (This is similar to how [Ansible Playbooks](https://docs.ansible.com/ansible/latest/playbook_guide/playbooks.html) work.) `earthmover` parses the YAML in several steps:

1. Extract only the [`config` section](configuration.md#config) (if any), in order to make available any `macros` when parsing the rest of the Jinja + YAML. The `config` section *only* **may not contain any Jinja** (besides `macros`).
1. Load the entire Jinja + YAML as a string and hydrate all [parameter](usage.md#parameters) references.
1. Parse the hydrated Jinja + YAML string with any `macros` to plain YAML.
1. Load the plain YAML string as a nested dictionary and begin building and processing the [DAG](design.md#data-dependency-graph-dag).

Note that due to step (3) above, *runtime* Jinja expressions (such as column definitions for `add_columns` or `modify_columns` operations) should be wrapped with `{%raw%}...{%endraw%}` to avoid being parsed when the YAML is being loaded.

The parsed YAML is written to a file called `earthmover_compiled.yaml` in your working directory during a `compile` command. This file can be used to debug issues related to compile-time Jinja or [project composition](usage.md#project-composition).


## Data dependency graph (DAG)
`earthmover` models the `sources` &rarr; `transformations` &rarr; `destinations` data flow as a directed acyclic graph ([DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph)). `earthmover` will raise an error if your [YAML configuration](configuration.md#yaml-configuration) is not a valid DAG (if, for example, it contains a [cycle](https://en.wikipedia.org/wiki/Cycle_(graph_theory))).

Each [component](https://en.wikipedia.org/wiki/Component_(graph_theory)) of the DAG is run separately.

![dataflow graph components](https://raw.githubusercontent.com/edanalytics/earthmover/main/images/dataflow-graph-components.gif)

Each component is materialized in [topological order](https://en.wikipedia.org/wiki/Topological_sorting). This minimizes memory usage, as only the data from the current and previous layer must be retained in memory.

![dataflow graph layers](https://raw.githubusercontent.com/edanalytics/earthmover/main/images/dataflow-graph-layers.gif)

!!! tip "Tip: visualize an earthmover DAG"

    Setting [`config`](configuration.md#config) &raquo; `show_graph: True` will make `earthmover run` produce a visualization of the DAG for a project, such as

    ![tests DAG](https://raw.githubusercontent.com/edanalytics/earthmover/main/earthmover/tests/tests-dag.png)

    In this diagram:

    * <span style="color:#080;">Green</span> nodes on the left correspond to [`sources`](configuration.md#sources)
    * <span style="color:#36A;">Blue</span> nodes in the middle correspond to [`transformations`](configuration.md#transformations)
    * <span style="color:#B00;">Red</span> nodes on the right correspond to [`destinations`](configuration.md#destinations)
    * `sources` and `destinations` are annotated with the file size (in <u>B</u>ytes)
    * all nodes are annotated with the number of <u>r</u>ows and <u>c</u>olumns of data at that step



## Dataframes
All data processing is done using [Pandas Dataframes](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html) and [Dask](https://www.dask.org/), with values stored as strings (or [Categoricals](https://pandas.pydata.org/docs/user_guide/categorical.html), for memory efficiency in columns with few unique values). This choice of datatypes prevents issues arising from Pandas' datatype inference (like inferring numbers as dates), but does require casting string-representations of numeric values using Jinja when doing comparisons or computations.


## Performance
Tool performance depends on a variety of factors including source file size and/or database performance, the system's storage performance (HDD vs. SSD), memory, and transformation complexity. But some effort has been made to engineer this tool for high throughput and to work in memory- and compute-constrained environments.

Smaller source data (which all fits into memory) processes very quickly. Larger chunked sources are necessarily slower. We have tested with sources files of 3.3GB, 100M rows (synthetic attendance data): creating 100M lines of JSONL (30GB) takes around 50 minutes on a modern laptop.

The [state feature](usage.md#state) adds some overhead, as hashes of input data and JSON payloads must be computed and stored, but this can be disabled if desired.


## Comparison to `dbt`
`earthmover` is similar in a number of ways to [`dbt`](). Some ways in which the tools are similar include...

* both are open-source data transformation tools
* both use YAML project configuration
* both manage data dependencies as a DAG
* both manage data transformation as code
* both support packages (reusable data transformation modules)

But there are some significant differences between the tools too, including...

* `earthmover` runs data transformations locally, while `dbt` issues SQL transformation queries to a database engine for execution. (For database `sources`, `earthmover` downloads the data from the database and processes it locally.)
* `earthmover`'s data transformation instructions are `operations` expressed as YAML, while `dbt`'s transformation instructions are (Jinja-templated) SQL queries.

The team that maintains `earthmover` also uses (and loves!) `dbt`. Our data engineers typically use `dbt` for large datasets (GB+) in a cloud database (like Snowflake) and `earthmover` for smaller datasets (< GB) in files (CSV, etc.).
