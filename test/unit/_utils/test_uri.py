######################################################################
#
# File: test/unit/_utils/test_uri.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from pathlib import Path

import pytest

from b2._internal._utils.uri import B2URI, B2FileIdURI, parse_uri


class TestB2URI:
    def test__str__(self):
        uri = B2URI(bucket_name='testbucket', path='path/to/file')
        assert str(uri) == 'b2://testbucket/path/to/file'

    @pytest.mark.parametrize(
        'path, expected',
        [
            ('', True),
            ('path/', True),
            ('path/subpath', None),
        ],
    )
    def test_is_dir(self, path, expected):
        assert B2URI('bucket', path).is_dir() is expected

    def test__bucket_uris_are_normalized(self):
        alternatives = [
            B2URI('bucket'),
            B2URI('bucket', ''),
        ]
        assert len(set(alternatives)) == 1
        assert {str(uri) for uri in alternatives} == {'b2://bucket/'}  # normalized

    @pytest.mark.parametrize(
        'path, expected_uri_str',
        [
            ('', 'b2://bucket/'),
            ('path/', 'b2://bucket/path/'),
            ('path/subpath', 'b2://bucket/path/subpath'),
        ],
    )
    def test__normalization(self, path, expected_uri_str):
        assert str(B2URI('bucket', path)) == expected_uri_str
        assert str(B2URI('bucket', path)) == str(B2URI('bucket', path))  # normalized


def test_b2fileuri_str():
    uri = B2FileIdURI(file_id='file123')
    assert str(uri) == 'b2id://file123'


@pytest.mark.parametrize(
    'uri,expected',
    [
        ('some/local/path', Path('some/local/path')),
        ('./some/local/path', Path('some/local/path')),
        ('.', Path('')),
        ('b2://bucket', B2URI(bucket_name='bucket')),
        (' b2://bucket', B2URI(bucket_name='bucket')),
        ('b2://bucket/', B2URI(bucket_name='bucket')),
        ('b2://bucket/path/to/dir/', B2URI(bucket_name='bucket', path='path/to/dir/')),
        ('b2id://file123', B2FileIdURI(file_id='file123')),
        ('b2://bucket/wild[card]', B2URI(bucket_name='bucket', path='wild[card]')),
        ('b2://bucket/wild?card', B2URI(bucket_name='bucket', path='wild?card')),
        ('b2://bucket/special#char', B2URI(bucket_name='bucket', path='special#char')),
        ('b2://bucket/special#', B2URI(bucket_name='bucket', path='special#')),
        ('b2://bucket/special?', B2URI(bucket_name='bucket', path='special?')),
        ('b2://bucket//special', B2URI(bucket_name='bucket', path='/special')),
    ],
)
def test_parse_uri(uri, expected):
    assert parse_uri(uri) == expected


def test_parse_uri__allow_all_buckets():
    assert parse_uri('b2://', allow_all_buckets=True) == B2URI('')
    with pytest.raises(ValueError) as exc_info:
        parse_uri('b2:///', allow_all_buckets=True)
    assert (
        "Invalid B2 URI: all buckets URI doesn't allow non-empty path, but '/' was provided"
        == str(exc_info.value)
    )


@pytest.mark.parametrize(
    'uri, expected_exception_message',
    [
        ('', 'URI cannot be empty'),
        # Test cases for invalid B2 URIs (missing netloc part)
        ('b2://', "Invalid B2 URI: 'b2://'"),
        ('b2id://', "Invalid B2 URI: 'b2id://'"),
        # Test cases for B2 URIs with credentials
        (
            'b2://user@password:bucket/path',
            "Invalid B2 URI: 'b2://user@password:bucket/path'",
        ),
        (
            'b2id://user@password:file123',
            "Invalid B2 URI: 'b2id://user@password:file123'",
        ),
        # Test cases for unsupported URI schemes
        ('unknown://bucket/path', "Unsupported URI scheme: 'unknown'"),
        (' unknown://bucket/path', "Unsupported URI scheme: 'unknown'"),
    ],
)
def test_parse_uri_exceptions(uri, expected_exception_message):
    with pytest.raises(ValueError) as exc_info:
        parse_uri(uri)
    assert expected_exception_message in str(exc_info.value)
