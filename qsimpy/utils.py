from __future__ import annotations


def get_all_values(d):
    """
    get_all_values returns all the leaves of a dictionary, whether they are in a list
    or not

    :param d: input dictionary
    :return: list of values
    """
    if isinstance(d, dict):
        for v in d.values():
            yield from get_all_values(v)
    elif isinstance(d, list):
        for v in d:
            yield from get_all_values(v)
    else:
        yield d
