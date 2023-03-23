### v0.1.0
<details>
<summary>Released 2023-03-23</summary>

* feature: added parse-time Jinja templating to YAML configuration

> :warning: **Potentially breaking change:** if your config YAML contains `add_columns` or `modify_columns` operations *with Jinja expressions*, these will now be parsed at YAML load time. To preserve the Jinja for runtime parsing, wrap the expressions with `{%raw%}...{%endraw%}`.

* feature: removed dependency on matplotlib, which is only required if your YAML specified `config.show_graph: True`... now if you try to `show_graph` without matplotlib installed, you'll get an error prompting you to install matplotlib

</details>

<hr />

### v0.0.7
<details>
<summary>Released 2023-02-23</summary>

* feature: added `str_min()` and `str_max()` functions for `group by` operation
</details>

### v0.0.6
<details>
<summary>Released 2023-02-17</summary>

* feature: pass `__row_data__` dict into Jinja templates for easier dynamic column referencing
* bugfix: parameter / env var interpolation into YAML keys, not just values
* refactor error handling key assertion methods
* refactor YAML loader line number context handling
</details>

### v0.0.5
<details>
<summary>Released 2022-12-16</summary>

* trim nodes not connected to a destination from DAG
* ensure all source datatypes return a Dask dataframe
* update [optional source functionality](#optional-sources) to require `columns` list, and pass an empty dataframe through the DAG
</details>

### v0.0.4
<details>
<summary>Released 2022-10-27</summary>

* support running in Google Colab
</details>

### v0.0.3
<details>
<summary>Released 2022-10-27</summary>

* support for Python 3.7
</details>

### v0.0.2
<details>
<summary>Released 2022-09-22</summary>

* initial release
</details>