This folder contains some small CSVs and a configuration file which are used to test the tool's transformations. in the directory above, running `earthmover -t` will
1. load the configuration file `tests/config.yaml` which
1. reads source CSV files from the `tests/sources/` directory, computes transformations on them, and
1. produces output to the `tests/outputs/` directory
1. finally, the contents of `tests/outputs/` are compared to "the right answer" from `tests/expected/`

Tests cover of all transformation operation types.