######################################################################
#
# File: test/unit/test_filter_out_empty_values.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pytest

from b2._internal.console_tool import filter_out_empty_values


@pytest.mark.parametrize(
    'value, expected',
    [
        ({}, None),
        ({'a': None}, None),
        ({'a': 1, 'b': None}, {'a': 1}),
        ({'a': {'b': None}}, None),
        ({'a': {'b': None, 'c': 2}, 'd': {}}, {'a': {'c': 2}}),
        ({'a': {'b': {'c': {}}}, 'd': 4}, {'d': 4}),
        # only None (and dicts that become empty) are dropped; other falsy values are data
        (
            {'a': 0, 'b': '', 'c': False, 'd': [], 'e': ()},
            {'a': 0, 'b': '', 'c': False, 'd': [], 'e': ()},
        ),
        # lists are values, not containers to descend into
        ({'a': [None, {}]}, {'a': [None, {}]}),
        # a non-dict argument is returned unchanged
        (None, None),
        (0, 0),
        ('text', 'text'),
        ([1, None], [1, None]),
    ],
)
def test_default_marker_drops_none_and_emptied_dicts(value, expected):
    assert filter_out_empty_values(value) == expected


def test_input_is_not_mutated():
    value = {'a': {'b': None, 'c': 2}, 'd': None}
    filter_out_empty_values(value)
    assert value == {'a': {'b': None, 'c': 2}, 'd': None}


def test_custom_marker_is_dropped_and_returned_for_empty_result():
    marker = object()
    assert filter_out_empty_values({'a': marker, 'b': 1}, empty_marker=marker) == {'b': 1}
    assert filter_out_empty_values({'a': {'b': marker}}, empty_marker=marker) is marker
    assert filter_out_empty_values({}, empty_marker=marker) is marker


def test_none_is_kept_when_it_is_not_the_marker():
    assert filter_out_empty_values({'a': None, 'b': ''}, empty_marker='') == {'a': None}


def test_webhook_rule_shape():
    # the shape of a notification rule whose optional fields were not given
    rule = {
        'name': 'rule',
        'eventTypes': ['b2:ObjectCreated:*'],
        'isEnabled': True,
        'objectNamePrefix': None,
        'targetConfiguration': {
            'url': 'https://example.com/hook',
            'customHeaders': {},
            'hmacSha256SigningSecret': None,
        },
    }
    assert filter_out_empty_values(rule) == {
        'name': 'rule',
        'eventTypes': ['b2:ObjectCreated:*'],
        'isEnabled': True,
        'targetConfiguration': {'url': 'https://example.com/hook'},
    }
