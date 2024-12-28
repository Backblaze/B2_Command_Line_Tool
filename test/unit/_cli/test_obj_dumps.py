######################################################################
#
# File: test/unit/_cli/test_obj_dumps.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from io import StringIO

import pytest

from b2._internal._cli.obj_dumps import readable_yaml_dump

# Test cases as tuples: (input_data, expected_output)
test_cases = [
    ({'key': 'value'}, 'key: value\n'),
    ([{'a': 1, 'b': 2}], '- a: 1\n  b: 2\n'),
    ([1, 2, 'false'], "- 1\n- 2\n- 'false'\n"),
    ({'true': True, 'null': None}, "'null': null\n'true': true\n"),
    ([1.0, 0.567], '- 1.0\n- 0.567\n'),
    ([''], "- ''\n"),
    (
        # make sure id and name are first, rest should be sorted alphabetically
        [
            {'b': 2, 'a': 1, 'name': 4, 'id': 3},
        ],
        '- id: 3\n  name: 4\n  a: 1\n  b: 2\n',
    ),
    (  # nested data
        [
            {
                'name': 'John Doe',
                'age': 30,
                'addresses': [
                    {
                        'street': '123 Elm St',
                        'city': 'Somewhere',
                    },
                    {
                        'street': '456 Oak St',
                    },
                ],
                'address': {
                    'street': '789 Pine St',
                    'city': 'Anywhere',
                    'zip': '67890',
                },
            }
        ],
        (
            '- name: John Doe\n'
            '  address: \n'
            '    city: Anywhere\n'
            '    street: 789 Pine St\n'
            "    zip: '67890'\n"
            '  addresses: \n'
            '    - city: Somewhere\n'
            '      street: 123 Elm St\n'
            '    - street: 456 Oak St\n'
            '  age: 30\n'
        ),
    ),
]


@pytest.mark.parametrize('input_data,expected', test_cases)
def test_readable_yaml_dump(input_data, expected):
    output = StringIO()
    readable_yaml_dump(input_data, output)
    assert output.getvalue() == expected
