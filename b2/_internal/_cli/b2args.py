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

from b2._internal._cli.arg_parser_types import wrap_with_argument_type_error
from b2._internal._cli.argcompleters import b2uri_file_completer
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
B2ID_OR_FILE_LIKE_B2_URI_ARG_TYPE = wrap_with_argument_type_error(b2id_or_file_like_b2_uri)


def add_b2id_or_b2_uri_argument(parser: argparse.ArgumentParser, name="B2_URI"):
    """
    Add B2 URI (b2:// or b2id://) as an argument to the parser.
    B2 URI can point to a bucket optionally with a object name prefix (directory)
    or a file-like object.
    """
    parser.add_argument(
        name,
        type=B2ID_OR_B2_URI_ARG_TYPE,
        help="B2 URI pointing to a bucket, directory or a file."
        "e.g. b2://yourBucket, b2://yourBucket/file.txt, b2://yourBucket/folderName/, or b2id://fileId",
    ).completer = b2uri_file_completer


def add_b2id_or_file_like_b2_uri_argument(parser: argparse.ArgumentParser, name="B2_URI"):
    """
    Add a B2 URI pointing to a file as an argument to the parser.
    """
    parser.add_argument(
        name,
        type=B2ID_OR_FILE_LIKE_B2_URI_ARG_TYPE,
        help="B2 URI pointing to a file, e.g. b2://yourBucket/file.txt or b2id://fileId",
    ).completer = b2uri_file_completer
