######################################################################
#
# File: b2/_utils/uri.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import dataclasses
import pathlib
import urllib
from pathlib import Path


class B2URIBase:
    pass


@dataclasses.dataclass
class B2URI(B2URIBase):
    bucket: str
    path: str

    def __str__(self) -> str:
        return f"b2://{self.bucket}{self.path}"


@dataclasses.dataclass
class B2FileIdURI(B2URIBase):
    file_id: str

    def __str__(self) -> str:
        return f"b2id://{self.file_id}"


def parse_uri(uri: str) -> Path | B2URI | B2FileIdURI:
    parsed = urllib.parse.urlparse(uri)
    if parsed.scheme == "":
        return pathlib.Path(uri)
    return _parse_b2_uri(uri, parsed)


def parse_b2_uri(uri: str) -> B2URI | B2FileIdURI:
    parsed = urllib.parse.urlparse(uri)
    return _parse_b2_uri(uri, parsed)


def _parse_b2_uri(uri, parsed: urllib.parse.ParseResult) -> B2URI | B2FileIdURI:
    if parsed.scheme in ("b2", "b2id"):
        if not parsed.netloc:
            raise ValueError(f"Invalid B2 URI: {uri!r}")
        elif parsed.password or parsed.username:
            raise ValueError(
                "Invalid B2 URI: credentials passed using `user@password:` syntax are not supported in URI"
            )

        if parsed.scheme == "b2":
            return B2URI(bucket=parsed.netloc, path=parsed.path[1:])
        elif parsed.scheme == "b2id":
            return B2FileIdURI(file_id=parsed.netloc)
    else:
        raise ValueError(f"Unsupported URI scheme: {parsed.scheme!r}")
