import pandas as pd

# only use upgraded pandas config on later versions of python
import sys
if sys.version_info.minor >= 10:
    pd.options.mode.copy_on_write = True
    pd.options.mode.string_storage = "pyarrow"