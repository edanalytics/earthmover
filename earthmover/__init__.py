# only use upgraded dask/pandas config on later versions of python
import sys
if sys.version_info.minor >= 10:

    # September 2024 - for now we need to do this in order to turn off the Dask 
    #    query optimizer - see https://blog.dask.org/2023/08/25/dask-expr-introduction
    #    For reasons unknown, it doesn't yet work with Earthmover. A future Dask 
    #    version may force us to use the query optimizer, but hopefully by then,
    #    the bugs that emerge when we use it with Earthmover will have been fixed.
    import dask
    dask.config.set({'dataframe.query-planning': False})

    # performance enhancements
    dask.config.set({"dataframe.convert-string": True})

    import pandas as pd
    pd.options.mode.copy_on_write = True
    pd.options.mode.string_storage = "pyarrow"