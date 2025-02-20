import jinja2
import hashlib
import json
import os
import pandas as pd

from pathlib import Path
from functools import partial
from sys import exc_info

from typing import Optional
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.error_handler import ErrorHandler
    from pandas import Series


def human_time(seconds: int) -> str:
    """
    Turns a raw duration (seconds) integer into a human-readable approximation.
    e.g., "42 minutes"

    :param seconds:
    :return:
    """
    if seconds < 60  : return "less than a minute"
    if seconds < 90  : return "about a minute"
    if seconds < 150 : return "a couple minutes"
    if seconds < 3600: return str(round(seconds/60))+" minutes"
    if round(seconds/60) < 80: return "about an hour"
    if round(seconds/60) < 150: return "a couple hours"
    if seconds < 86400 : return str(round(seconds/3600))+" hours"
    if seconds < 129600: return "about a day"
    if seconds < 216000: return "a couple of days"
    return str(round(seconds/86400)) + " days"


def human_size(bytes_: int, units=('B','KB','MB','GB','TB', 'PB', 'EB')):
    return str(bytes_) + units[0] if bytes_ < 1024 else human_size(bytes_>>10, units[1:])

def get_sep(file: str) -> Optional[str]:
    """
    Determine field separator from file extension
    (this should only be used for local files, aka the map_file for a map_values transformation operation)

    :param file:
    :return:
    """
    ext_mapping = {
        'csv': ',',
        'tsv': '\t',
    }

    ext = file.lower().rsplit('.', 1)[-1]
    return ext_mapping.get(ext)


def flatten_dict(d, parent_key='', sep='.'):
    """Flattens a nested dictionary, using a separator for nested keys."""
    flattened = {}
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            flattened.update(flatten_dict(v, new_key, sep))
        else:
            flattened[new_key] = v
    return flattened


def contains_jinja(string: str) -> bool:
    """

    :param string:
    :return:
    """
    string = str(string)  # Just in case a static int is passed.

    if '{{' in string and '}}' in string:
        return True
    elif '{%' in string and '%}' in string:
        return True
    elif '{#' in string and '#}' in string:
        return True
    else:
        return False


def build_jinja_template(template_string: str):
    """

    """
    template = jinja2.Environment(
        loader=jinja2.FileSystemLoader(os.path.dirname('./'))
    ).from_string(template_string)
    template.globals['md5'] = partial(md5_hash)
    template.globals['fromjson'] = partial(json.loads)
    return template


def get_jinja_template_params(template_string: str):
    """
    This function uses Jinja's meta functions to return a list of variables
    referenced within `template_string`. This may be used in the future to
    trace column-level lineage backward through the data dependency graph,
    and drop unused columns early (to improve earthmover performance).
    """
    from jinja2 import meta
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(os.path.dirname('./'))
    )
    ast = env.parse(template_string)
    return list(meta.find_undeclared_variables(ast))

def render_jinja_template(row: 'Series', template: jinja2.Template, template_string: str, dunder_row_data=False, *, error_handler: Optional['ErrorHandler']=None) -> str:
    """

    :param row:
    :param template:
    :param template_str:
    :param error_handler:
    :return:
    """
    try:
    
        if row.empty and dunder_row_data: row_data = { '__row_data__': { '__row_data__': {}} }
        else:
            row_data = row.to_dict()
            if dunder_row_data: row_data.update({"__row_data__": row.to_dict()})
        
        rendered_row = template.render(row_data)
        return rendered_row

    except Exception as err:
        if error_handler: error_handler.ctx.remove('line')

        if dict(row):
            _joined_keys = "`, `".join(dict(row).keys())
            variables = f"\n(available variables are `{_joined_keys}`)"
        else:
            variables = f"\n(no available variables)"

        if error_handler: error_handler.throw(
            f"Error rendering Jinja template: ({err}):\n===> {template_string}{variables}"
        )
        else: raise Exception(f"Error rendering Jinja template: ({err})")
        raise


def jinja2_template_error_lineno():
    """
    function based on https://stackoverflow.com/questions/26967433/how-to-get-line-number-causing-an-exception-other-than-templatesyntaxerror-in
    :return: int lineno
    """
    type_, value, tb = exc_info()

    # skip non-Jinja errors
    if not issubclass(type_, jinja2.TemplateError):
        return None

    # one particular Exception type has a lineno built in - grab it!
    if hasattr(value, 'lineno'):
        # in case of TemplateSyntaxError
        return value.lineno

    # "tb" is "trace-back"; this walks through the traceback line-by-line looking
    # for the relevant line, then extracts the line number
    while tb:
        if tb.tb_frame.f_code.co_filename == '<template>':
            return tb.tb_lineno
        tb = tb.tb_next


def md5_hash(x):
    return hashlib.md5(x.encode('utf-8')).hexdigest()
