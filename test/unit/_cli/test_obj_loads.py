######################################################################
#
# File: test/unit/_cli/test_obj_loads.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import argparse

import pytest

from b2._internal._cli.obj_loads import validated_loads


@pytest.mark.parametrize(
    "input_, expected_val",
    [
        # json
        ('{"a": 1}', {
            "a": 1
        }),
        ('{"a": 1, "b": 2}', {
            "a": 1,
            "b": 2
        }),
        ('{"a": 1, "b": 2, "c": 3}', {
            "a": 1,
            "b": 2,
            "c": 3
        }),
        # yaml
        ("a: 1", {
            "a": 1
        }),
        ("a: 1\nb: 2", {
            "a": 1,
            "b": 2
        }),
        ("a: 1\nb: 2\nc: 3", {
            "a": 1,
            "b": 2,
            "c": 3
        }),
        # yaml one-liners
        ("a: 1", {
            "a": 1
        }),
        ("{a: 1,b: 2}", {
            "a": 1,
            "b": 2
        }),
        ("{a: test,b: 2,sub:{c: 3}}", {
            "a": "test",
            "b": 2,
            "sub": {
                "c": 3
            }
        }),
    ],
)
def test_validated_loads(input_, expected_val):
    assert validated_loads(input_) == expected_val


@pytest.mark.parametrize(
    "input_, error_msg",
    [
        # not valid json nor yaml
        ("{", "'{' is not a valid JSON/YAML:"),
        # not-valid yaml
        (
            "a: 1, a: 2",
            r"a: 1, a: 2\' is not a valid JSON/YAML: mapping values are not allowed here",
        ),
    ],
)
def test_validated_loads__invalid_syntax(input_, error_msg):
    with pytest.raises(argparse.ArgumentTypeError, match=error_msg):
        validated_loads(input_)
