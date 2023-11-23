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

from b2sdk.v2 import (
    B2Api,
    DownloadVersion,
    FileVersion,
)

from b2._utils.python_compat import removeprefix, singledispatchmethod


class B2URIBase:
    pass


@dataclasses.dataclass(frozen=True)
class B2URI(B2URIBase):
    """
    B2 URI designating a particular object by name & bucket or "subdirectory" in a bucket.

    Please note, both files and directories are symbolical concept, not a real one in B2, i.e.
    there is no such thing as "directory" in B2, but it is possible to mimic it by using object names with non-trailing
    slashes.
    To make it possible, it is highly discouraged to use trailing slashes in object names.
    """

    bucket_name: str
    path: str = ""

    def __post_init__(self):
        path = removeprefix(self.path, "/")
        self.__dict__["path"] = path  # hack for a custom init in frozen dataclass

    def __str__(self) -> str:
        return f"b2://{self.bucket_name}/{self.path}"

    def is_dir(self) -> bool | None:
        """
        Return if the path is a directory.

        Please note this is symbolical.
        It is possible for file to have a trailing slash, but it is HIGHLY discouraged, and not supported by B2 CLI.
        At the same time it is possible for a directory to not have a trailing slash,
        which is discouraged, but allowed by B2 CLI.
        This is done to mimic unix-like Path.

        In practice, this means that `.is_dir() == True` will always be interpreted as "this is a directory",
        but reverse is not necessary true, and `not uri.is_dir()` should be merely interpreted as
        "this is a directory or a file".

        :return: True if the path is a directory, None if it is unknown
        """
        return not self.path or self.path.endswith("/") or None


@dataclasses.dataclass(frozen=True)
class B2FileIdURI(B2URIBase):
    """
    B2 URI designating a particular file by its id.
    """

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
            return B2URI(bucket_name=parsed.netloc, path=parsed.path)
        elif parsed.scheme == "b2id":
            file_id = parsed.netloc
            if not file_id:
                raise ValueError(f"File id was not provided in B2 URI: {uri!r}")
            return B2FileIdURI(file_id=file_id)
    else:
        raise ValueError(f"Unsupported URI scheme: {parsed.scheme!r}")


class B2URIAdapter:
    """
    Adapter for using B2URI with B2Api.

    When this matures enough methods from here should be moved to b2sdk B2Api class.
    """

    def __init__(self, api: B2Api):
        self.api = api

    def __getattr__(self, name):
        return getattr(self.api, name)

    @singledispatchmethod
    def download_file_by_uri(self, uri, *args, **kwargs):
        raise NotImplementedError(f"Unsupported URI type: {type(uri)}")

    @download_file_by_uri.register
    def _(self, uri: B2URI, *args, **kwargs):
        bucket = self.get_bucket_by_name(uri.bucket_name)
        return bucket.download_file_by_name(uri.path, *args, **kwargs)

    @download_file_by_uri.register
    def _(self, uri: B2FileIdURI, *args, **kwargs):
        return self.download_file_by_id(uri.file_id, *args, **kwargs)

    @singledispatchmethod
    def get_file_info_by_uri(self, uri, *args, **kwargs):
        raise NotImplementedError(f"Unsupported URI type: {type(uri)}")

    @get_file_info_by_uri.register
    def _(self, uri: B2URI, *args, **kwargs) -> DownloadVersion:
        return self.get_file_info_by_name(uri.bucket_name, uri.path, *args, **kwargs)

    @get_file_info_by_uri.register
    def _(self, uri: B2FileIdURI, *args, **kwargs) -> FileVersion:
        return self.get_file_info(uri.file_id, *args, **kwargs)

    @singledispatchmethod
    def get_download_url_by_uri(self, uri, *args, **kwargs):
        raise NotImplementedError(f"Unsupported URI type: {type(uri)}")

    @get_download_url_by_uri.register
    def _(self, uri: B2URI, *args, **kwargs) -> str:
        return self.get_download_url_for_file_name(uri.bucket_name, uri.path, *args, **kwargs)

    @get_download_url_by_uri.register
    def _(self, uri: B2FileIdURI, *args, **kwargs) -> str:
        return self.get_download_url_for_fileid(uri.file_id, *args, **kwargs)
