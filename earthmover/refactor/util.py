from typing import Optional


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


def contains_jinja(string):
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


def render_jinja_template(row, template, *, error_handler):
    """

    :param row:
    :param template:
    :param error_handler:
    :return:
    """
    try:
        return template.render(row)

    except Exception as err:
        error_handler.throw(
            f"Error rendering Jinja template: ({err})"
        )
        raise
