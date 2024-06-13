######################################################################
#
# File: b2/_internal/_cli/obj_loads.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import argparse
import io
import json
import logging
from typing import TypeVar

from b2sdk.v2 import get_b2sdk_doc_urls

try:
    import pydantic
    from pydantic import TypeAdapter, ValidationError
except ImportError:
    pydantic = None

logger = logging.getLogger(__name__)


def convert_error_to_human_readable(validation_exc: ValidationError) -> str:
    buf = io.StringIO()
    for error in validation_exc.errors():
        loc = '.'.join(str(loc) for loc in error['loc'])
        buf.write(f'  In field {loc!r} input was `{error["input"]!r}`, error: {error["msg"]}\n')
    return buf.getvalue()


def describe_type(type_) -> str:
    urls = get_b2sdk_doc_urls(type_)
    if urls:
        url_links = ', '.join(f'{name} <{url}>' for name, url in urls.items())
        return f'{type_.__name__} ({url_links})'
    return type_.__name__


T = TypeVar('T')

_UNDEF = object()


def validated_loads(data: str, expected_type: type[T] | None = None) -> T:
    val = _UNDEF
    if expected_type is not None and pydantic is not None:
        try:
            ta = TypeAdapter(expected_type)
        except TypeError:
            # TypeError: unsupported operand type(s) for |: 'type' and 'NoneType'
            # This is thrown on python<3.10 even with eval_type_backport
            logger.debug(
                f'Failed to create TypeAdapter for {expected_type!r} using pydantic, falling back to json.loads',
                exc_info=True
            )
            val = _UNDEF
        else:
            try:
                val = ta.validate_json(data)
            except ValidationError as e:
                errors = convert_error_to_human_readable(e)
                raise argparse.ArgumentTypeError(
                    f'Invalid value inputted, expected {describe_type(expected_type)}, got {data!r}, more detail below:\n{errors}'
                ) from e

    if val is _UNDEF:
        try:
            val = json.loads(data)
        except json.JSONDecodeError as e:
            raise argparse.ArgumentTypeError(f'{data!r} is not a valid JSON value') from e
    return val
