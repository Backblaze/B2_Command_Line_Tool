######################################################################
#
# File: b2/_internal/_utils/uriparse.py
#
# Copyright 2025 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import re
from collections import namedtuple

_CONTROL_CHARACTERS_AND_SPACE = '\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f '
_B2_URL_RE = re.compile(
    r"""
    (
        (?P<scheme>[a-z0-9]+)
        ://
    )?                       # the delimiter is optional if there is no scheme defined
    (?P<netloc>[-a-z0-9]*)   # scheme and
    (?P<path>\.{0,2}(/.*)?)            # everything else from the first / is part of the path
    """,
    re.VERBOSE | re.IGNORECASE,
)

SplitB2Result = namedtuple("SplitB2Result", "scheme,netloc,path")


def b2_urlsplit(url: str) -> SplitB2Result:
    # clean the url
    url = url.lstrip(_CONTROL_CHARACTERS_AND_SPACE)
    for i in ['\n', '\r', '\t']:
        url.replace(i, '')

    match = _B2_URL_RE.fullmatch(url)
    if not match:
        raise ValueError(f'Invalid B2 URI: {url!r}')

    scheme = (match.group('scheme') or '').lower()
    netloc = match.group('netloc') or ''
    path = match.group('path') or ''

    return SplitB2Result(scheme, netloc, path)
