######################################################################
#
# File: test/unit/_cli/test_obj_loads.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import argparse

import pytest

try:
    from typing_extensions import TypedDict
except ImportError:
    from typing import TypedDict

from b2._internal._cli.obj_loads import pydantic, validated_loads


@pytest.mark.parametrize(
    'input_, expected_val',
    [
        # json
        ('{"a": 1}', {'a': 1}),
        ('{"a": 1, "b": 2}', {'a': 1, 'b': 2}),
        ('{"a": 1, "b": 2, "c": 3}', {'a': 1, 'b': 2, 'c': 3}),
    ],
)
def test_validated_loads(input_, expected_val):
    assert validated_loads(input_) == expected_val


@pytest.mark.parametrize(
    'input_, error_msg',
    [
        # not valid json nor yaml
        ('{', "'{' is not a valid JSON value"),
    ],
)
def test_validated_loads__invalid_syntax(input_, error_msg):
    with pytest.raises(argparse.ArgumentTypeError, match=error_msg):
        validated_loads(input_)


@pytest.fixture
def typed_dict_cls():
    class MyTypedDict(TypedDict):
        a: int | None
        b: str

    return MyTypedDict


def test_validated_loads__typed_dict(typed_dict_cls):
    input_ = '{"a": 1, "b": "2", "extra": null}'
    expected_val = {'a': 1, 'b': '2', 'extra': None}
    assert validated_loads(input_, typed_dict_cls) == expected_val


@pytest.mark.skipif(pydantic is None, reason='pydantic is not enabled')
def test_validated_loads__typed_dict_types_validation(typed_dict_cls):
    input_ = '{"a": "abc", "b": 2}'
    with pytest.raises(argparse.ArgumentTypeError):
        validated_loads(input_, typed_dict_cls)
