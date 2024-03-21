######################################################################
#
# File: b2/_internal/_cli/b2args.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Utility functions for adding b2-specific arguments to an argparse parser.
"""
import argparse
import functools
from os import environ
from typing import Optional, Tuple

from b2._internal._cli.arg_parser_types import wrap_with_argument_type_error
from b2._internal._cli.argcompleters import b2uri_file_completer
from b2._internal._cli.const import (
    B2_APPLICATION_KEY_ENV_VAR,
    B2_APPLICATION_KEY_ID_ENV_VAR,
)
from b2._internal._utils.uri import B2URI, B2URIBase, parse_b2_uri


def b2id_or_file_like_b2_uri(value: str) -> B2URIBase:
    b2_uri = parse_b2_uri(value)
    if isinstance(b2_uri, B2URI):
        if b2_uri.is_dir():
            raise ValueError(
                f"B2 URI pointing to a file-like object is required, but {value} was provided"
            )
        return b2_uri

    return b2_uri


B2ID_OR_B2_URI_ARG_TYPE = wrap_with_argument_type_error(parse_b2_uri)
B2ID_OR_B2_URI_OR_ALL_BUCKETS_ARG_TYPE = wrap_with_argument_type_error(
    functools.partial(parse_b2_uri, allow_all_buckets=True)
)
B2ID_OR_FILE_LIKE_B2_URI_ARG_TYPE = wrap_with_argument_type_error(b2id_or_file_like_b2_uri)


def add_b2id_or_b2_uri_argument(
    parser: argparse.ArgumentParser, name="B2_URI", *, allow_all_buckets: bool = False
):
    """
    Add B2 URI (b2:// or b2id://) as an argument to the parser.
    B2 URI can point to a bucket optionally with a object name prefix (directory)
    or a file-like object.

    If allow_all_buckets is True, the argument will accept B2 URI pointing to all buckets.
    """
    if allow_all_buckets:
        argument_spec = parser.add_argument(
            name,
            type=B2ID_OR_B2_URI_OR_ALL_BUCKETS_ARG_TYPE,
            default=None,
            nargs="?",
            help="B2 URI pointing to a bucket, directory, file or all buckets. "
            "e.g. b2://yourBucket, b2://yourBucket/file.txt, b2://yourBucket/folderName/, b2id://fileId, or b2://",
        )
    else:
        argument_spec = parser.add_argument(
            name,
            type=B2ID_OR_B2_URI_ARG_TYPE,
            help="B2 URI pointing to a bucket, directory or a file. "
            "e.g. b2://yourBucket, b2://yourBucket/file.txt, b2://yourBucket/folderName/, or b2id://fileId",
        )

    argument_spec.completer = b2uri_file_completer


def add_b2id_or_file_like_b2_uri_argument(parser: argparse.ArgumentParser, name="B2_URI"):
    """
    Add a B2 URI pointing to a file as an argument to the parser.
    """
    parser.add_argument(
        name,
        type=B2ID_OR_FILE_LIKE_B2_URI_ARG_TYPE,
        help="B2 URI pointing to a file, e.g. b2://yourBucket/file.txt or b2id://fileId",
    ).completer = b2uri_file_completer


def get_keyid_and_key_from_env_vars() -> Tuple[Optional[str], Optional[str]]:
    return environ.get(B2_APPLICATION_KEY_ID_ENV_VAR), environ.get(B2_APPLICATION_KEY_ENV_VAR)
