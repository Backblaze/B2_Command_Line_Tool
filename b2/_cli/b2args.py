######################################################################
#
# File: b2/_cli/b2args.py
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

from b2._cli.arg_parser_types import wrap_with_argument_type_error
from b2._cli.argcompleters import b2uri_file_completer
from b2._utils.uri import B2URI, B2URIBase, parse_b2_uri


def b2_file_uri(value: str) -> B2URIBase:
    b2_uri = parse_b2_uri(value)
    if isinstance(b2_uri, B2URI):
        if b2_uri.is_dir():
            raise ValueError(
                f"B2 URI pointing to a file-like object is required, but {value} was provided"
            )
        return b2_uri

    return b2_uri


B2_URI_ARG_TYPE = wrap_with_argument_type_error(parse_b2_uri)
B2_URI_FILE_ARG_TYPE = wrap_with_argument_type_error(b2_file_uri)


def add_b2_file_argument(parser: argparse.ArgumentParser, name="B2_URI"):
    """
    Add a B2 URI pointing to a file as an argument to the parser.
    """
    parser.add_argument(
        name,
        type=B2_URI_FILE_ARG_TYPE,
        help="B2 URI pointing to a file, e.g. b2://yourBucket/file.txt or b2id://fileId",
    ).completer = b2uri_file_completer
