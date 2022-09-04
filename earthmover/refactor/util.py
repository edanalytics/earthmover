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


def get_chunksize(memory_limit: int) -> int:
    """

    :param memory_limit:
    :return:
    """
    MB = 1024 * 1024
    BYTES_TO_ROW_MAPPING = {
        5 * 1024 * MB: 2 * 10 ** 7, #   >  5GB -> 20 M
        2 * 1024 * MB: 1 * 10 ** 7, #   >  2GB -> 10 M
            1024 * MB: 5 * 10 ** 6, #   >  1GB -> 5  M
             500 * MB: 2 * 10 ** 6, #   >500MB -> 2  M
             200 * MB: 1 * 10 ** 6, #   >200MB -> 1  M
             100 * MB: 5 * 10 ** 5, #   >100MB -> 500K
              50 * MB: 2 * 10 ** 5, #   > 50MB -> 200K
              20 * MB: 1 * 10 ** 5, #   > 20MB -> 100K
              10 * MB: 5 * 10 ** 4, #   > 10MB -> 50 K
               5 * MB: 2 * 10 ** 4, #   >  5MB -> 20 K
               2 * MB: 1 * 10 ** 4, #   >  2MB -> 10 K
                   MB: 2 * 10 ** 3, #   >  1MB -> 5  K
    }

    #
    for key, val in BYTES_TO_ROW_MAPPING.items():
        if memory_limit > key:
            return val
    else:
        return 5 * 10 ** 2 #  <=1MB -> 500


def get_sep(file: str) -> Optional[str]:
    """
    Determine field separator from file extension
    (this should only be used for local files, aka the map_file for a map_values transformation operation)

    :param file:
    :param return_none:
    :return:
    """
    EXT_MAPPING = {
        'csv': ',',
        'tsv': '\t',
    }

    ext = file.lower().rsplit('.', 1)[-1]
    return EXT_MAPPING.get(ext)


def contains_jinja(string):
    """

    :param string:
    :return:
    """
    if '{{' in string and '}}' in string:
        return True
    elif '{%' in string and '%}' in string:
        return True
    elif '{#' in string and '#}' in string:
        return True
    else:
        return False