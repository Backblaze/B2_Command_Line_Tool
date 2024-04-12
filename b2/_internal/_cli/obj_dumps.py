######################################################################
#
# File: b2/_internal/_cli/obj_dumps.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import io

from b2sdk.v2 import (
    unprintable_to_hex,
)

_simple_repr_map = {
    False: "false",
    None: "null",
    True: "true",
}
_simple_repr_map_values = set(_simple_repr_map.values()) | {"yes", "no"}


def _yaml_simple_repr(obj):
    """
    Like YAML for simple types, but also escapes control characters for safety.
    """
    if isinstance(obj, (int, float)) and not isinstance(obj, bool):
        return str(obj)
    simple_repr = _simple_repr_map.get(obj)
    if simple_repr:
        return simple_repr
    obj_repr = unprintable_to_hex(str(obj))
    if isinstance(
        obj, str
    ) and (obj == "" or obj_repr.lower() in _simple_repr_map_values or obj_repr.isdigit()):
        obj_repr = repr(obj)  # add quotes to distinguish from numbers and booleans
    return obj_repr


def _id_name_first_key(item):
    try:
        return ("id", "name").index(str(item[0]).lower()), item[0], item[1]
    except ValueError:
        return 2, item[0], item[1]


def _dump(data, indent=0, skip=False, *, output):
    prefix = " " * indent
    if isinstance(data, dict):
        for idx, (key, value) in enumerate(sorted(data.items(), key=_id_name_first_key)):
            output.write(f"{'' if skip and idx == 0 else prefix}{_yaml_simple_repr(key)}: ")
            if isinstance(value, (dict, list)):
                output.write("\n")
                _dump(value, indent + 2, output=output)
            else:
                _dump(value, 0, True, output=output)
    elif isinstance(data, list):
        for idx, item in enumerate(data):
            output.write(f"{'' if skip and idx == 0 else prefix}- ")
            _dump(item, indent + 2, True, output=output)
    else:
        output.write(f"{'' if skip else prefix}{_yaml_simple_repr(data)}\n")


def readable_yaml_dump(data, output: io.TextIOBase) -> None:
    """
    Print YAML-like human-readable representation of the data.

    :param data: The data to be printed. Can be a list, dict, or any basic datatype.
    :param output: An output stream derived from io.TextIOBase where the data is to be printed.
    """
    _dump(data, output=output)
