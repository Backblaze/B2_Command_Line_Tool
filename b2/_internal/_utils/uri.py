######################################################################
#
# File: b2/_internal/_utils/uri.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import dataclasses
import re
from collections.abc import Sequence
from functools import singledispatchmethod
from pathlib import Path

from b2sdk.v3 import (
    B2Api,
    DownloadVersion,
    FileVersion,
    Filter,
)
from b2sdk.v3.exception import B2Error

_B2ID_PATTERN = re.compile(r'^b2id://(?P<file_id>[a-zA-Z0-9:_-]+)$', re.IGNORECASE)
_B2_PATTERN = re.compile(r'^b2://(?P<bucket>[a-z0-9-]*)(?P<path>/.*)?$', re.IGNORECASE)
_SCHEME_PATTERN = re.compile(r'(?P<scheme>[a-z0-9]*)://.*', re.IGNORECASE)
_CONTROL_CHARACTERS_AND_SPACE = '\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\x0c\r\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f '


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

    Please note `path` attribute should exclude prefixing slash, i.e. `path` should be empty string for the root of the bucket.
    """

    bucket_name: str
    path: str = ''

    def __str__(self) -> str:
        return f'b2://{self.bucket_name}/{self.path}'

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
        return not self.path or self.path.endswith('/') or None


@dataclasses.dataclass(frozen=True)
class B2FileIdURI(B2URIBase):
    """
    B2 URI designating a particular file by its id.
    """

    file_id: str

    def __str__(self) -> str:
        return f'b2id://{self.file_id}'


def parse_uri(uri: str, *, allow_all_buckets: bool = False) -> Path | B2URI | B2FileIdURI:
    """
    Parse URI.

    :param uri: string to parse
    :param allow_all_buckets: if True, allow `b2://` without a bucket name to refer to all buckets
    :return: B2 URI or Path
    :raises ValueError: if the URI is invalid
    """
    if not uri:
        raise ValueError('URI cannot be empty')

    if _SCHEME_PATTERN.fullmatch(_clean_uri(uri)):
        return _parse_b2_uri(uri, allow_all_buckets=allow_all_buckets)
    return Path(uri)


def parse_b2_uri(
    uri: str, *, allow_all_buckets: bool = False, allow_b2id: bool = True
) -> B2URI | B2FileIdURI:
    """
    Parse B2 URI.

    :param uri: string to parse
    :param allow_all_buckets: if True, allow `b2://` without a bucket name to refer to all buckets
    :param allow_b2id: if True, allow `b2id://` to refer to a file by its id
    :return: B2 URI
    :raises ValueError: if the URI is invalid
    """
    return _parse_b2_uri(uri, allow_all_buckets=allow_all_buckets, allow_b2id=allow_b2id)


def _clean_uri(uri: str) -> str:
    # Clean URI
    uri = uri.lstrip(_CONTROL_CHARACTERS_AND_SPACE)
    for i in ('\n', '\r', '\t'):
        uri = uri.replace(i, '')
    return uri


def _parse_b2_uri(
    uri,
    *,
    allow_all_buckets: bool = False,
    allow_b2id: bool = True,
) -> B2URI | B2FileIdURI:
    uri = _clean_uri(uri)
    if uri.lower().startswith('b2://'):
        match = _B2_PATTERN.fullmatch(uri)
        if not match:
            raise ValueError(f'Invalid B2 URI: {uri!r}')

        bucket = match.group('bucket')
        path = match.group('path')
        if not bucket:
            if allow_all_buckets:
                if path:
                    raise ValueError(
                        f"Invalid B2 URI: all buckets URI doesn't allow non-empty path, but {path!r} was provided"
                    )
                return B2URI(bucket_name='')
        else:
            return B2URI(bucket_name=bucket, path=path[1:] if path else '')
    elif allow_b2id and uri.lower().startswith('b2id://'):
        match = _B2ID_PATTERN.fullmatch(uri)
        if match:
            return B2FileIdURI(file_id=match.group('file_id'))
    elif match := _SCHEME_PATTERN.fullmatch(uri):
        raise ValueError(f'Unsupported URI scheme: {match.group("scheme")!r}')

    raise ValueError(f'Invalid B2 URI: {uri!r}')


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
        raise NotImplementedError(f'Unsupported URI type: {type(uri)}')

    @download_file_by_uri.register
    def _(self, uri: B2URI, *args, **kwargs):
        bucket = self.get_bucket_by_name(uri.bucket_name)
        return bucket.download_file_by_name(uri.path, *args, **kwargs)

    @download_file_by_uri.register
    def _(self, uri: B2FileIdURI, *args, **kwargs):
        return self.download_file_by_id(uri.file_id, *args, **kwargs)

    @singledispatchmethod
    def get_file_info_by_uri(self, uri, *args, **kwargs):
        raise NotImplementedError(f'Unsupported URI type: {type(uri)}')

    @get_file_info_by_uri.register
    def _(self, uri: B2URI, *args, **kwargs) -> DownloadVersion:
        return self.get_file_info_by_name(uri.bucket_name, uri.path, *args, **kwargs)

    @get_file_info_by_uri.register
    def _(self, uri: B2FileIdURI, *args, **kwargs) -> FileVersion:
        return self.get_file_info(uri.file_id, *args, **kwargs)

    @singledispatchmethod
    def get_download_url_by_uri(self, uri, *args, **kwargs):
        raise NotImplementedError(f'Unsupported URI type: {type(uri)}')

    @get_download_url_by_uri.register
    def _(self, uri: B2URI, *args, **kwargs) -> str:
        return self.get_download_url_for_file_name(uri.bucket_name, uri.path, *args, **kwargs)

    @get_download_url_by_uri.register
    def _(self, uri: B2FileIdURI, *args, **kwargs) -> str:
        return self.get_download_url_for_fileid(uri.file_id, *args, **kwargs)

    @singledispatchmethod
    def ls(self, uri, *args, **kwargs):
        raise NotImplementedError(f'Unsupported URI type: {type(uri)}')

    @ls.register
    def _(self, uri: B2URI, *args, filters: Sequence[Filter] = (), **kwargs):
        bucket = self.api.get_bucket_by_name(uri.bucket_name)
        try:
            yield from bucket.ls(uri.path, *args, filters=filters, **kwargs)
        except ValueError as error:
            # Wrap these errors into B2Error. At the time of writing there's
            # exactly one – `with_wildcard` being passed without `recursive` option.
            raise B2Error(error.args[0])

    @ls.register
    def _(self, uri: B2FileIdURI, *args, **kwargs):
        yield self.get_file_info_by_uri(uri), None

    @singledispatchmethod
    def copy_by_uri(self, uri, *args, **kwargs):
        raise NotImplementedError(f'Unsupported URI type: {type(uri)}')

    @copy_by_uri.register
    def _(self, source: B2FileIdURI, destination: B2URI, *args, **kwargs):
        destination_bucket = self.get_bucket_by_name(destination.bucket_name)
        return destination_bucket.copy(source.file_id, destination.path, *args, **kwargs)

    @copy_by_uri.register
    def _(self, source: B2URI, destination: B2URI, *args, **kwargs):
        file_info = self.get_file_info_by_uri(source)
        return self.copy_by_uri(B2FileIdURI(file_info.id_), destination, *args, **kwargs)
