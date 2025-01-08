#

<center>
![earthmover](assets/ea-earthmover.png)
</center>

`earthmover` transforms collections of tabular source data (flat files, FTP files, database tables/queries) into text-based (JSONL, XML) data via YAML configuration.

<center>
![earthmover demo](assets/earthmover_demo.gif)
</center>

## Quick-start
1. Install `earthmover` with
    ```bash
    pip install earthmover
    ```

1. Create an `earthmover.yml` [configuration file]() that defines your project [config](), data [sources](), [transformations](), and [destinations]():
    ```yaml+jinja title="earthmover.yml" linenums="1"
    version: 2

    config:
      output_dir: ./output/

    sources:
      csv_source:
        file: ./data/file.csv
        header_rows: 1
      sql_source:
        connection: "postgresql://user:pass@host/database"
        query: >
          select column1, column2, column3
          from myschema.mytable

    transformations:
      stacked:
        source: $sources.csv_source
        operations:
          - operation: union
            sources:
              - $sources.sql_source

    destinations:
      mydata:
        source: $transformations.stacked
        template: ./json_templates/mydata.jsont
        extension: jsonl
        linearize: True
    ```

1. Create the `./json_templates/mydata.jsont` template file (which may use [Jinja](https://jinja.palletsprojects.com/en/stable/templates/)) to use when rendering the data for your `mydata` destination:

    ```js+jinja title="./json_templates/mydata.jsont"
    {
        "column1": "{{column1}}",
        "column2": "{{column2}}",
        "column3": "{{column3}}"
    }
    ```

1. Now run earthmover
    ```bash
    earthmover run
    ```
    and look for the output file `output/mydata.json`.

## How it works
`earthmover` is [similar to `dbt`](design.md#comparison-to-dbt), though it executes data transformations locally using dataframes (rather than in a SQL engine). Like `dbt`, it creates a data dependency DAG from your `earthmover.yml` configuration and materializes output data in dependency-order.

## Read more
Above is a simple quick-start example. Please read the documentation for more information about `earthmover`'s many features, including:

* Resources for [understanding `earthmover`](learn.md)
* How to [install `earthmover`](setup.md)
* How to [configure an `earthmover` project](configuration.md)
* How to use [`earthmover`'s commands](usage.md#commands) and [features](usage.md#features)
* [Best practices](practices.md) around `earthmover` projects
* Details about [the design of `earthmover`](design.md)
* How to [contribute to `earthmover`](contributing.md)
