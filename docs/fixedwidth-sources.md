# Working with fixed-width source files

One challenge of working with fixed-width files (FWFs) is that they require additional metadata. In particular, any tool that reads a FWF into a tabular structure needs to know how to slice each row into its constituent columns. Earthmover expects this information in the form of a **CSV** called a `colspec_file`

## Specifying a `colspec_file`

In your earthmover.yaml config, a `fixedwidth` source is specified much like any other file source. Here is a complete example:
```yaml
sources:
  input:
    file: ./data/input.txt
    colspec_file: ./seed/colspecs.csv   # always required
    colspec_headers:
      name: field_name                  # always required
      start: start_index                # required if `width` is not provided
      end: end_index                    # required if `width` is not provided
      width: field_length               # required if `start` or `end` is not provided
    type: fixedwidth                    # required if `file` does not end with '.txt'
    header_rows: 0
```

Some notes on the available options
  - (required) `colspec_file`: a path to the CSV containing your colspec metadata
  - (required) `colspec_headers`: a mapping between the `colspec_file`'s column names and the fields Earthmover requires. **Note that the names and positions of these columns do not matter**
    - Of these, only `name` is always required. Your `colspec_file` should contain a column that assigns a name to each field in the FWF
    - You must either provide `width`, or both `start` and `end`
    - If you provide `width` your `colspec_file` should include a column of integer values that specifies the number of characters in each field in the FWF
    - If you provide `start` and `end`, your `colspec_file` should include two columns of integer values [giving the extents of the FWF's fields as half-open intervals (i.e., \[from, to\[ )](https://pandas.pydata.org/docs/reference/api/pandas.read_fwf.html) 
  - (optional) `type`: if the input file has a `.txt` extension, you do not need to specify `type`. However, since there is no standard extension for FWFs, it is a good idea to use `type: fixedwidth`
  - (optional) `header_rows`: this is almost always 0 for FWFs. Earthmover will usually infer this even if you don't specify it, but we recommend doing so

## Formatting a `colspec_file`
In accordance with the above, a `colspec_file` must include a column with field names, as well as either a column with field widths, or two columns with start and end positions.  Both of the following CSVs are valid and equivalent to one another:

```csv
name,width
date,8
id,16
score_1,2
score_2,2
```
For this file, your earthmover.yaml would look like:
```yaml
colspec_headers:
  name: name
  width: width
```

or

```csv
start_idx, end_idx, other_data, full_field_name, other_data_2
0, 8, abc, date, def
8, 24, abc, id, def
24, 26, abc, score_1, def
26, 28, abc, score_2, def
```
For this file, your earthmover.yaml would look like:
```yaml
colspec_headers:
  name: full_field_name
  start: start_idx
  end: end_idx
```
