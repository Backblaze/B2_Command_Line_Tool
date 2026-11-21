######################################################################
#
# File: test/unit/_cli/test_b2args.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import pytest

from b2._internal._cli.b2args import parse_bucket_name


@pytest.mark.parametrize('value', ['b2id://4_zabc_def', 'b2id://x'])
@pytest.mark.parametrize('allow_all_buckets', [False, True])
def test_parse_bucket_name_rejects_file_id_uri(value, allow_all_buckets):
    """A `b2id://` URI is not a bucket name and must be rejected locally."""
    with pytest.raises(ValueError):
        parse_bucket_name(value, allow_all_buckets=allow_all_buckets)


@pytest.mark.parametrize(
    'value, expected',
    [('my-bucket', 'my-bucket'), ('b2://my-bucket', 'my-bucket'), ('b2://my-bucket/', 'my-bucket')],
)
def test_parse_bucket_name_accepts_bucket_names(value, expected):
    """Valid forms keep working."""
    assert parse_bucket_name(value) == expected


def test_parse_bucket_name_all_buckets():
    assert parse_bucket_name('b2://', allow_all_buckets=True) == ''
