#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
######################################################################
#
# File: b2/_internal/console_tool.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
# ruff: noqa: E402
from __future__ import annotations

import copy
import tempfile
import warnings

from b2._internal._cli.autocomplete_cache import AUTOCOMPLETE  # noqa
from b2._internal._utils.python_compat import removeprefix

AUTOCOMPLETE.autocomplete_from_cache()

import argparse
import base64
import contextlib
import csv
import dataclasses
import datetime
import functools
import getpass
import io
import json
import locale
import logging
import logging.config
import os
import pathlib
import platform
import queue
import re
import signal
import subprocess
import sys
import threading
import time
import unicodedata
from abc import ABCMeta, abstractmethod
from concurrent.futures import Executor, Future, ThreadPoolExecutor
from contextlib import suppress
from enum import Enum
from typing import Any, BinaryIO, List

import b2sdk
import requests
import rst2ansi
from b2sdk.v2 import (
    ALL_CAPABILITIES,
    B2_ACCOUNT_INFO_DEFAULT_FILE,
    B2_ACCOUNT_INFO_ENV_VAR,
    B2_ACCOUNT_INFO_PROFILE_FILE,
    DEFAULT_MIN_PART_SIZE,
    DEFAULT_SCAN_MANAGER,
    NO_RETENTION_BUCKET_SETTING,
    REALM_URLS,
    SRC_LAST_MODIFIED_MILLIS,
    SSE_C_KEY_ID_FILE_INFO_KEY_NAME,
    STDOUT_FILEPATH,
    UNKNOWN_KEY_ID,
    XDG_CONFIG_HOME_ENV_VAR,
    AbstractAccountInfo,
    ApplicationKey,
    B2Api,
    BasicSyncEncryptionSettingsProvider,
    Bucket,
    BucketRetentionSetting,
    CompareVersionMode,
    DownloadedFile,
    EncryptionAlgorithm,
    EncryptionKey,
    EncryptionMode,
    EncryptionSetting,
    FileRetentionSetting,
    FileVersion,
    Filter,
    KeepOrDeleteMode,
    LegalHold,
    LifecycleRule,
    NewerFileSyncMode,
    ProgressReport,
    ReplicationConfiguration,
    ReplicationMonitor,
    ReplicationRule,
    ReplicationSetupHelper,
    RetentionMode,
    ScanPoliciesManager,
    Synchronizer,
    SyncReport,
    TqdmProgressListener,
    UploadMode,
    current_time_millis,
    escape_control_chars,
    get_included_sources,
    make_progress_listener,
    notification_rule_response_to_request,
    parse_sync_folder,
    points_to_fifo,
    substitute_control_chars,
    unprintable_to_hex,
)
from b2sdk.v2.exception import (
    B2Error,
    BadFileInfo,
    EmptyDirectory,
    FileNotPresent,
    MissingAccountData,
    NotADirectory,
    UnableToCreateDirectory,
)
from b2sdk.version import VERSION as b2sdk_version
from class_registry import ClassRegistry
from tabulate import tabulate

from b2._internal._cli.arg_parser_types import (
    parse_comma_separated_list,
    parse_default_retention_period,
    parse_millis_from_float_timestamp,
    parse_range,
)
from b2._internal._cli.argcompleters import file_name_completer
from b2._internal._cli.autocomplete_install import (
    SUPPORTED_SHELLS,
    AutocompleteInstallError,
    autocomplete_install,
)
from b2._internal._cli.b2api import _get_b2api_for_profile, _get_inmemory_b2api
from b2._internal._cli.b2args import (
    add_b2_bucket_uri_argument,
    add_b2_uri_argument,
    add_b2id_or_b2_bucket_uri_argument,
    add_b2id_or_b2_uri_argument,
    add_b2id_or_file_like_b2_uri_argument,
    add_b2id_or_file_like_b2_uri_or_bucket_name_argument,
    add_b2id_uri_argument,
    add_bucket_name_argument,
    get_keyid_and_key_from_env_vars,
)
from b2._internal._cli.const import (
    B2_APPLICATION_KEY_ENV_VAR,
    B2_APPLICATION_KEY_ID_ENV_VAR,
    B2_CLI_DOCKER_ENV_VAR,
    B2_DESTINATION_SSE_C_KEY_B64_ENV_VAR,
    B2_DESTINATION_SSE_C_KEY_ID_ENV_VAR,
    B2_ENVIRONMENT_ENV_VAR,
    B2_ESCAPE_CONTROL_CHARACTERS,
    B2_SOURCE_SSE_C_KEY_B64_ENV_VAR,
    B2_USER_AGENT_APPEND_ENV_VAR,
    CREATE_BUCKET_TYPES,
    DEFAULT_THREADS,
)
from b2._internal._cli.obj_dumps import readable_yaml_dump
from b2._internal._cli.obj_loads import validated_loads
from b2._internal._cli.shell import detect_shell, resolve_short_call_name
from b2._internal._utils.uri import B2URI, B2FileIdURI, B2URIAdapter, B2URIBase
from b2._internal.arg_parser import B2ArgumentParser, add_normalized_argument
from b2._internal.json_encoder import B2CliJsonEncoder
from b2._internal.version import VERSION

piplicenses = None
prettytable = None
with suppress(ImportError):
    import piplicenses
    import prettytable

logger = logging.getLogger(__name__)

SEPARATOR = '=' * 40

# Enable to get 0.* behavior in the command-line tool.
# Disable for 1.* behavior.
VERSION_0_COMPATIBILITY = False


def filter_out_empty_values(v, empty_marker=None):
    if isinstance(v, dict):
        d = {}
        for k, v in v.items():
            new_v = filter_out_empty_values(v, empty_marker=empty_marker)
            if new_v is not empty_marker:
                d[k] = new_v
        return d or empty_marker
    return v


def override_dict(base_dict, override):
    result = copy.deepcopy(base_dict)
    for k, v in override.items():
        if isinstance(v, dict):
            result[k] = override_dict(result.get(k, {}), v)
        else:
            result[k] = v
    return result


class NoControlCharactersStdout:
    def __init__(self, stdout):
        self.stdout = stdout

    def __getattr__(self, attr):
        return getattr(self.stdout, attr)

    def write(self, s):
        if s:
            s, cc_present = substitute_control_chars(s)
            if cc_present:
                logger.warning('WARNING: Control Characters were detected in the output')
        self.stdout.write(s)


def resolve_b2_bin_call_name(argv: list[str] | None = None) -> str:
    call_name = resolve_short_call_name((argv or sys.argv)[0])
    if call_name.endswith('.py'):
        version_name = re.search(r'[\\/]b2[\\/]_internal[\\/](_?b2v\d+)[\\/]__main__.py', call_name)
        call_name = version_name.group(1) if version_name else 'b2'
    if 'b2' not in call_name:  # prevent silliness when calling b2 from under different process
        return 'b2'
    return call_name


FILE_RETENTION_COMPATIBILITY_WARNING = """
    .. warning::
       Setting file retention mode to '{}' is irreversible - such files can only be ever deleted after their retention
       period passes, regardless of keys (master or not) used. This is especially dangerous when setting bucket default
       retention, as it may lead to high storage costs.
""".format(RetentionMode.COMPLIANCE.value)

# Strings available to use when formatting doc strings.
DOC_STRING_DATA = dict(
    B2_ACCOUNT_INFO_ENV_VAR=B2_ACCOUNT_INFO_ENV_VAR,
    B2_ACCOUNT_INFO_DEFAULT_FILE=B2_ACCOUNT_INFO_DEFAULT_FILE,
    B2_ACCOUNT_INFO_PROFILE_FILE=B2_ACCOUNT_INFO_PROFILE_FILE,
    XDG_CONFIG_HOME_ENV_VAR=XDG_CONFIG_HOME_ENV_VAR,
    B2_APPLICATION_KEY_ID_ENV_VAR=B2_APPLICATION_KEY_ID_ENV_VAR,
    B2_APPLICATION_KEY_ENV_VAR=B2_APPLICATION_KEY_ENV_VAR,
    B2_USER_AGENT_APPEND_ENV_VAR=B2_USER_AGENT_APPEND_ENV_VAR,
    B2_ENVIRONMENT_ENV_VAR=B2_ENVIRONMENT_ENV_VAR,
    B2_DESTINATION_SSE_C_KEY_B64_ENV_VAR=B2_DESTINATION_SSE_C_KEY_B64_ENV_VAR,
    B2_DESTINATION_SSE_C_KEY_ID_ENV_VAR=B2_DESTINATION_SSE_C_KEY_ID_ENV_VAR,
    B2_SOURCE_SSE_C_KEY_B64_ENV_VAR=B2_SOURCE_SSE_C_KEY_B64_ENV_VAR,
    SSE_C_KEY_ID_FILE_INFO_KEY_NAME=SSE_C_KEY_ID_FILE_INFO_KEY_NAME,
    FILE_RETENTION_COMPATIBILITY_WARNING=FILE_RETENTION_COMPATIBILITY_WARNING,
)


class CommandError(B2Error):
    """
    b2 command error (user caused).  Accepts exactly one argument: message.

    We expect users of shell scripts will parse our ``__str__`` output.
    """

    def __init__(self, message):
        super().__init__()
        self.message = message

    def __str__(self):
        return self.message


def local_path_to_b2_path(path):
    """
    Ensures that the separator in the path is '/', not '\'.

    :param path: A path from the local file system
    :return: A path that uses '/' as the separator.
    """
    return path.replace(os.path.sep, '/')


def keyboard_interrupt_handler(signum, frame):
    raise KeyboardInterrupt()


def mixed_case_to_hyphens(s):
    return s[0].lower() + ''.join(
        c if c.islower() or c.isdigit() else '-' + c.lower() for c in s[1:]
    )


def apply_or_none(fcn, value):
    """
    If the value is None, return None, otherwise return the result of applying the function to it.
    """
    if value is None:
        return None
    else:
        return fcn(value)


def format_account_info(account_info: AbstractAccountInfo) -> dict:
    allowed = account_info.get_allowed()
    allowed['capabilities'] = sorted(allowed['capabilities'])
    return dict(
        accountId=account_info.get_account_id(),
        accountFilePath=getattr(
            account_info,
            'filename',
            None,
        ),  # missing in StubAccountInfo in tests
        allowed=allowed,
        applicationKeyId=account_info.get_application_key_id(),
        applicationKey=account_info.get_application_key(),
        isMasterKey=account_info.is_master_key(),
        accountAuthToken=account_info.get_account_auth_token(),
        apiUrl=account_info.get_api_url(),
        downloadUrl=account_info.get_download_url(),
        s3endpoint=account_info.get_s3_api_url(),
    )


class DescriptionGetter:
    def __init__(self, described_cls, **kwargs):
        self.described_cls = described_cls
        self.kwargs = kwargs

    def __str__(self):
        return self.described_cls._get_description(**self.kwargs)


class Described:
    """
    Base class for Commands, providing them with tools for evaluating docstrings to CLI help texts.
    Allows for including superclasses' evaluated docstrings.
    """

    @classmethod
    def _get_description(cls, **kwargs):
        mro_docs = {
            klass.__name__: klass.lazy_get_description(**kwargs)
            for klass in cls.mro()
            if klass is not cls and klass.__doc__ and issubclass(klass, Described)
        }
        return cls.__doc__.format(**kwargs, **DOC_STRING_DATA, **mro_docs)

    @classmethod
    def lazy_get_description(cls, **kwargs):
        return DescriptionGetter(cls, **kwargs)


class JSONOptionMixin(Described):
    """
    Use ``--json`` to get machine-readable output.
    Unless ``--json`` is used, the output is human-readable, and may change from one minor version to the next.
    Therefore, for scripting, it is strongly encouraged to use ``--json``.
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument(
            '--json', action='store_true', help='output in JSON format to use in scripts'
        )
        super()._setup_parser(parser)  # noqa


class DefaultSseMixin(Described):
    """
    If you want server-side encryption for all of the files that are uploaded to a bucket,
    you can enable SSE-B2 encryption as a default setting for the bucket.
    In order to do that pass ``--default-server-side-encryption=SSE-B2``.
    The default algorithm is set to AES256 which can by changed
    with ``--default-server-side-encryption-algorithm`` parameter.
    All uploads to that bucket, from the time default encryption is enabled onward,
    will then be encrypted with SSE-B2 by default.

    To disable default bucket encryption, use ``--default-server-side-encryption=none``.

    If ``--default-server-side-encryption`` is not provided,
    default server side encryption is determined by the server.

    .. note::

        Note that existing files in the bucket are not affected by default bucket encryption settings.
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(
            parser, '--default-server-side-encryption', default=None, choices=('SSE-B2', 'none')
        )
        add_normalized_argument(
            parser,
            '--default-server-side-encryption-algorithm',
            default='AES256',
            choices=('AES256',)
        )

        super()._setup_parser(parser)  # noqa

    @classmethod
    def _get_default_sse_setting(cls, args):
        mode = apply_or_none(EncryptionMode, args.default_server_side_encryption)
        if mode is not None:
            if mode == EncryptionMode.NONE:
                args.default_server_side_encryption_algorithm = None

            algorithm = apply_or_none(
                EncryptionAlgorithm, args.default_server_side_encryption_algorithm
            )
            return EncryptionSetting(mode=mode, algorithm=algorithm)

        return None


class DestinationSseMixin(Described):
    """
    To request SSE-B2 or SSE-C encryption for destination files,
    please set ``--destination-server-side-encryption=SSE-B2/SSE-C``.
    The default algorithm is set to AES256 which can be changed
    with ``--destination-server-side-encryption-algorithm`` parameter.
    Using SSE-C requires providing ``{B2_DESTINATION_SSE_C_KEY_B64_ENV_VAR}`` environment variable,
    containing the base64 encoded encryption key.
    If ``{B2_DESTINATION_SSE_C_KEY_ID_ENV_VAR}`` environment variable is provided,
    it's value will be saved as ``{SSE_C_KEY_ID_FILE_INFO_KEY_NAME}`` in the
    uploaded file's fileInfo.
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(
            parser,
            '--destination-server-side-encryption',
            default=None,
            choices=('SSE-B2', 'SSE-C')
        )
        add_normalized_argument(
            parser,
            '--destination-server-side-encryption-algorithm',
            default='AES256',
            choices=('AES256',)
        )

        super()._setup_parser(parser)  # noqa

    def _get_destination_sse_setting(self, args):
        mode = apply_or_none(EncryptionMode, args.destination_server_side_encryption)
        if mode is not None:
            algorithm = apply_or_none(
                EncryptionAlgorithm, args.destination_server_side_encryption_algorithm
            )
            if mode == EncryptionMode.SSE_B2:
                key = None
            elif mode == EncryptionMode.SSE_C:
                encryption_key_b64 = os.environ.get(B2_DESTINATION_SSE_C_KEY_B64_ENV_VAR)
                if not encryption_key_b64:
                    raise ValueError(
                        'Using SSE-C requires providing an encryption key via %s env var' %
                        B2_DESTINATION_SSE_C_KEY_B64_ENV_VAR
                    )
                key_id = os.environ.get(B2_DESTINATION_SSE_C_KEY_ID_ENV_VAR)
                if key_id is None:
                    self._print_stderr(
                        f'Encrypting file(s) with SSE-C without providing key id. '
                        f'Set {B2_DESTINATION_SSE_C_KEY_ID_ENV_VAR} to allow key identification.'
                    )
                key = EncryptionKey(secret=base64.b64decode(encryption_key_b64), key_id=key_id)
            else:
                raise NotImplementedError(f'Unsupported encryption mode for writes: {mode.value}')
            return EncryptionSetting(mode=mode, algorithm=algorithm, key=key)

        return None


class FileRetentionSettingMixin(Described):
    """
    Setting file retention settings requires the **writeFileRetentions** capability, and only works in bucket
    with fileLockEnabled=true. Providing ``--file-retention-mode`` requires providing ``--retain-until`` which has to
    be a future timestamp, in the form of an integer representing milliseconds
    since epoch. Leaving out these options results in a file retained according to bucket defaults.
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(
            parser,
            '--file-retention-mode',
            default=None,
            choices=(RetentionMode.COMPLIANCE.value, RetentionMode.GOVERNANCE.value)
        )

        add_normalized_argument(
            parser,
            '--retain-until',
            type=parse_millis_from_float_timestamp,
            default=None,
            metavar='TIMESTAMP'
        )
        super()._setup_parser(parser)  # noqa

    @classmethod
    def _get_file_retention_setting(cls, args):
        if (args.file_retention_mode is None) != (args.retain_until is None):
            raise ValueError(
                'provide either both --retain-until and --file-retention-mode or none of them'
            )

        file_retention_mode = apply_or_none(RetentionMode, args.file_retention_mode)
        if file_retention_mode is None:
            return None

        return FileRetentionSetting(file_retention_mode, args.retain_until)


class HeaderFlagsMixin(Described):
    @classmethod
    def _setup_parser(cls, parser: argparse.ArgumentParser) -> None:
        add_normalized_argument(
            parser,
            '--cache-control',
            help=
            "optional Cache-Control header, value based on RFC 2616 section 14.9, example: 'public, max-age=86400')"
        )
        add_normalized_argument(
            parser,
            '--content-disposition',
            help=
            "optional Content-Disposition header, value based on RFC 2616 section 19.5.1, example: 'attachment; filename=\"fname.ext\"'"
        )
        add_normalized_argument(
            parser,
            '--content-encoding',
            help=
            "optional Content-Encoding header, value based on RFC 2616 section 14.11, example: 'gzip'"
        )
        add_normalized_argument(
            parser,
            '--content-language',
            help=
            "optional Content-Language header, value based on RFC 2616 section 14.12, example: 'mi, en'"
        )
        add_normalized_argument(
            parser,
            '--expires',
            help=
            "optional Expires header, value based on RFC 2616 section 14.21, example: 'Thu, 01 Dec 2050 16:00:00 GMT'"
        )
        super()._setup_parser(parser)

    def _file_info_with_header_args(self, args,
                                    file_info: dict[str, str] | None) -> dict[str, str] | None:
        """Construct an updated file_info dictionary.
        Print a warning if any of file_info items will be overwritten by explicit header arguments.
        """
        add_file_info = {}
        overwritten = []
        if args.cache_control is not None:
            add_file_info['b2-cache-control'] = args.cache_control
        if args.content_disposition is not None:
            add_file_info['b2-content-disposition'] = args.content_disposition
        if args.content_encoding is not None:
            add_file_info['b2-content-encoding'] = args.content_encoding
        if args.content_language is not None:
            add_file_info['b2-content-language'] = args.content_language
        if args.expires is not None:
            add_file_info['b2-expires'] = args.expires

        for key, value in add_file_info.items():
            if file_info is not None and key in file_info and file_info[key] != value:
                overwritten.append(key)

        if overwritten:
            self._print_stderr(
                'The following file info items will be overwritten by explicit arguments:\n    ' +
                '\n    '.join(f'{key} = {add_file_info[key]}' for key in overwritten)
            )

        if add_file_info:
            return {**(file_info or {}), **add_file_info}
        return file_info


class LegalHoldMixin(Described):
    """
    Setting legal holds requires the **writeFileLegalHolds** capability, and only works in bucket
    with fileLockEnabled=true.
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(
            parser, '--legal-hold', default=None, choices=(LegalHold.ON.value, LegalHold.OFF.value)
        )
        super()._setup_parser(parser)  # noqa

    @classmethod
    def _get_legal_hold_setting(cls, args) -> LegalHold:
        return apply_or_none(LegalHold.from_string_or_none, args.legal_hold)


class SourceSseMixin(Described):
    """
    To access SSE-C encrypted files,
    please set ``--source-server-side-encryption=SSE-C``.
    The default algorithm is set to AES256 which can by changed
    with ``--source-server-side-encryption-algorithm`` parameter.
    Using SSE-C requires providing ``{B2_SOURCE_SSE_C_KEY_B64_ENV_VAR}`` environment variable,
    containing the base64 encoded encryption key.
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(
            parser, '--source-server-side-encryption', default=None, choices=('SSE-C',)
        )
        add_normalized_argument(
            parser,
            '--source-server-side-encryption-algorithm',
            default='AES256',
            choices=('AES256',)
        )

        super()._setup_parser(parser)  # noqa

    @classmethod
    def _get_source_sse_setting(cls, args):
        mode = apply_or_none(EncryptionMode, args.source_server_side_encryption)
        if mode is not None:
            algorithm = apply_or_none(
                EncryptionAlgorithm, args.source_server_side_encryption_algorithm
            )
            key = None
            if mode == EncryptionMode.SSE_C:
                encryption_key_b64 = os.environ.get(B2_SOURCE_SSE_C_KEY_B64_ENV_VAR)
                if not encryption_key_b64:
                    raise ValueError(
                        'Using SSE-C requires providing an encryption key via %s env var' %
                        B2_SOURCE_SSE_C_KEY_B64_ENV_VAR
                    )
                key = EncryptionKey(
                    secret=base64.b64decode(encryption_key_b64), key_id=UNKNOWN_KEY_ID
                )
            else:
                raise NotImplementedError(
                    f'Encryption modes other than {EncryptionMode.SSE_C.value} are not supported in reads'
                )
            return EncryptionSetting(mode=mode, algorithm=algorithm, key=key)

        return None


class WriteBufferSizeMixin(Described):
    """
    Use --write-buffer-size to set the size (in bytes) of the buffer used to write files.
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(parser, '--write-buffer-size', type=int, metavar='BYTES')
        super()._setup_parser(parser)  # noqa


class SkipHashVerificationMixin(Described):
    """
    Use --skip-hash-verification to disable hash check on downloaded files.
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(
            parser, '--skip-hash-verification', action='store_true', default=False
        )
        super()._setup_parser(parser)  # noqa


class MaxDownloadStreamsMixin(Described):
    """
    Use --max-download-streams-per-file to set max num of streams for parallel downloader.
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(parser, '--max-download-streams-per-file', type=int)
        super()._setup_parser(parser)  # noqa


class FileIdAndOptionalFileNameMixin(Described):
    """
    Specifying the ``fileName`` is more efficient than leaving it out.
    If you omit the ``fileName``, it requires an initial query to B2
    to get the file name, before making the call to delete the
    file.  This extra query requires the ``readFiles`` capability.
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('fileName', nargs='?')
        parser.add_argument('fileId')
        super()._setup_parser(parser)  # noqa

    def _get_file_name_from_args(self, args):
        if args.fileName is not None:
            return args.fileName
        file_info = self.api.get_file_info(args.fileId)
        return file_info.file_name


class B2URIFileArgMixin:
    @classmethod
    def _setup_parser(cls, parser):
        add_b2id_or_file_like_b2_uri_argument(parser)
        super()._setup_parser(parser)

    def get_b2_uri_from_arg(self, args: argparse.Namespace) -> B2URIBase:
        return args.B2_URI


class B2URIFileOrBucketNameFileNameArgMixin:
    @classmethod
    def _setup_parser(cls, parser):
        add_b2id_or_file_like_b2_uri_or_bucket_name_argument(parser)
        parser.add_argument('fileName', nargs='?', help=argparse.SUPPRESS)
        super()._setup_parser(parser)

    def get_b2_uri_from_arg(self, args: argparse.Namespace) -> B2URIBase | str:
        if isinstance(args.B2_URI, B2URI):
            return args.B2_URI

        bucket_name = args.B2_URI
        return bucket_name


class B2URIFileIDArgMixin:
    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('fileId')
        super()._setup_parser(parser)

    def get_b2_uri_from_arg(self, args: argparse.Namespace) -> B2URIBase:
        return B2FileIdURI(args.fileId)


class B2URIBucketArgMixin:
    @classmethod
    def _setup_parser(cls, parser):
        add_bucket_name_argument(parser)
        super()._setup_parser(parser)

    def get_b2_uri_from_arg(self, args: argparse.Namespace) -> B2URIBase:
        return B2URI(args.bucketName)


class B2URIBucketNFilenameArgMixin:
    @classmethod
    def _setup_parser(cls, parser):
        add_bucket_name_argument(parser)
        parser.add_argument('fileName').completion = file_name_completer
        super()._setup_parser(parser)

    def get_b2_uri_from_arg(self, args: argparse.Namespace) -> B2URIBase:
        return B2URI(args.bucketName, args.fileName)


class B2URIBucketNFolderNameArgMixin:
    ALLOW_ALL_BUCKETS: bool = False

    @classmethod
    def _setup_parser(cls, parser):
        add_bucket_name_argument(parser, nargs='?' if cls.ALLOW_ALL_BUCKETS else None)
        parser.add_argument('folderName', nargs='?').completer = file_name_completer
        super()._setup_parser(parser)

    def get_b2_uri_from_arg(self, args: argparse.Namespace) -> B2URI:
        return B2URI(removeprefix(args.bucketName or '', "b2://"), args.folderName or '')


class B2IDOrB2URIMixin:
    ALLOW_ALL_BUCKETS: bool = False

    @classmethod
    def _setup_parser(cls, parser):
        add_b2id_or_b2_uri_argument(parser, allow_all_buckets=cls.ALLOW_ALL_BUCKETS)
        super()._setup_parser(parser)

    def get_b2_uri_from_arg(self, args: argparse.Namespace) -> B2URI | B2FileIdURI:
        return args.B2_URI


class B2IDOrB2BucketURIMixin:
    @classmethod
    def _setup_parser(cls, parser):
        add_b2id_or_b2_bucket_uri_argument(parser)
        super()._setup_parser(parser)

    def get_b2_uri_from_arg(self, args: argparse.Namespace) -> B2URI | B2FileIdURI:
        return args.B2_URI


class B2BucketURIMixin:
    @classmethod
    def _setup_parser(cls, parser):
        add_b2_bucket_uri_argument(parser)
        super()._setup_parser(parser)

    def get_b2_uri_from_arg(self, args: argparse.Namespace) -> B2URI:
        return args.B2_URI


class B2IDURIMixin:
    @classmethod
    def _setup_parser(cls, parser):
        add_b2id_uri_argument(parser)
        super()._setup_parser(parser)

    def get_b2_uri_from_arg(self, args: argparse.Namespace) -> B2FileIdURI:
        return args.B2_URI


class UploadModeMixin(Described):
    """
    Use --incremental-mode to allow for incremental file uploads to safe bandwidth.  This will only affect files, which
    have been appended to since last upload.
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(parser, '--incremental-mode', action='store_true')
        super()._setup_parser(parser)  # noqa

    @staticmethod
    def _get_upload_mode_from_args(args):
        if args.incremental_mode:
            return UploadMode.INCREMENTAL
        return UploadMode.FULL


class ProgressMixin(Described):
    """
    If the ``tqdm`` library is installed, progress bar is displayed
    on stderr.  Without it, simple text progress is printed.
    Use ``--no-progress`` to disable progress reporting (marginally improves performance in some cases).
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(
            parser, '--no-progress', action='store_true', help="progress will not be reported"
        )
        super()._setup_parser(parser)  # noqa


class LifecycleRulesMixin(Described):
    """
    Use `--lifecycle-rule` to set lifecycle rule for the bucket.
    Multiple rules can be specified by repeating the option.
    All bucket lifecycle rules are set at once, so if you want to add a new rule,
    you need to provide all existing rules.
    Example: :code:`--lifecycle-rule '{{"daysFromHidingToDeleting": 1, "daysFromUploadingToHiding": null, "fileNamePrefix": "documents/"}}' --lifecycle-rule '{{"daysFromHidingToDeleting": 1, "daysFromUploadingToHiding": 7, "fileNamePrefix": "temporary/"}}'`
    """

    @classmethod
    def _setup_parser(cls, parser):
        lifecycle_group = parser.add_mutually_exclusive_group()
        add_normalized_argument(
            lifecycle_group,
            '--lifecycle-rule',
            action='append',
            default=None,
            type=functools.partial(validated_loads, expected_type=LifecycleRule),
            dest='lifecycle_rules',
            help="Lifecycle rule in JSON format. Can be supplied multiple times.",
        )
        add_normalized_argument(
            lifecycle_group,
            '--lifecycle-rules',
            type=functools.partial(validated_loads, expected_type=List[LifecycleRule]),
            help=
            "(deprecated; use --lifecycle-rule instead) List of lifecycle rules in JSON format.",
        )

        super()._setup_parser(parser)  # noqa


class ThreadsMixin(Described):
    """
    Use --threads to manually adjust the number of threads used in the operation.
    Otherwise, the number of threads will be automatically chosen.
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--threads', type=int, default=None)
        super()._setup_parser(parser)  # noqa

    def _get_threads_from_args(self, args) -> int:
        return args.threads or DEFAULT_THREADS

    def _set_threads_from_args(self, args):
        threads = self._get_threads_from_args(args)
        self.api.services.download_manager.set_thread_pool_size(threads)
        self.api.services.upload_manager.set_thread_pool_size(threads)


class _TqdmCloser:
    """
    On OSX using Tqdm with b2sdk causes semaphore leaks. This fix is located here and not in b2sdk, because after this
    cleanup Tqdm might not work properly, therefore it's best to do it when exiting a python process.
    """

    def __init__(self, progress_listener):
        self.progress_listener = progress_listener

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if sys.platform != "darwin" or os.environ.get('B2_TEST_DISABLE_TQDM_CLOSER'):
            return
        try:
            from multiprocessing.synchronize import SemLock
            tqdm_lock = self.progress_listener.tqdm.get_lock()
            if tqdm_lock.mp_lock._semlock.name is not None:
                SemLock._cleanup(tqdm_lock.mp_lock._semlock.name)
        except Exception as ex:
            logger.debug('Error encountered during Tqdm cleanup', exc_info=ex)


class Command(Described, metaclass=ABCMeta):
    COMMAND_NAME: str | None = None
    # Set to True for commands that receive sensitive information in arguments
    FORBID_LOGGING_ARGUMENTS = False

    deprecated = False

    # The registry for the subcommands, should be reinitialized  in subclass
    subcommands_registry = None

    # set to False for commands not requiring b2 authentication
    REQUIRES_AUTH = True

    def __init__(self, console_tool):
        self.console_tool = console_tool
        self.api = B2URIAdapter(console_tool.api)
        self.stdout = console_tool.stdout
        self.stderr = console_tool.stderr
        self.quiet = False
        self.escape_control_characters = True
        self.exit_stack = contextlib.ExitStack()

    def make_progress_listener(self, file_name: str, quiet: bool):
        progress_listener = make_progress_listener(file_name, quiet)
        self.exit_stack.enter_context(progress_listener)
        if isinstance(progress_listener, TqdmProgressListener):
            self.exit_stack.enter_context(_TqdmCloser(progress_listener))
        return progress_listener

    @classmethod
    def name_and_alias(cls):
        name = cls.COMMAND_NAME or cls.__name__
        if '-' not in name:
            name = mixed_case_to_hyphens(name)
        alias = None
        if '-' in name:
            alias = name.replace('-', '_')
        return name, alias

    @classmethod
    def register_subcommand(cls, command_class):
        assert cls.subcommands_registry is not None, 'Initialize the registry class'
        name, _ = command_class.name_and_alias()
        decorator = cls.subcommands_registry.register(key=name)(command_class)
        return decorator

    @classmethod
    def create_parser(
        cls,
        subparsers: argparse._SubParsersAction | None = None,
        parents=None,
        for_docs=False,
        name: str | None = None,
        b2_binary_name: str | None = None,
    ) -> argparse.ArgumentParser:
        """
        Creates a parser for the command.

        :param subparsers: subparsers object to which add new parser
        :param parents: created ArgumentParser `parents`, see `argparse.ArgumentParser`
        :param for_docs: if parser is to be used for documentation generation
        :param name: action name
        :param b2_binary_name: B2 binary call name
        :return: created parser
        """
        if parents is None:
            parents = []

        b2_binary_name = b2_binary_name or resolve_b2_bin_call_name()
        description = cls._get_description(NAME=b2_binary_name)

        if name:
            alias = None
        else:
            name, alias = cls.name_and_alias()
        parser_kwargs = dict(
            prog=name,
            description=description,
            parents=parents,
            for_docs=for_docs,
            deprecated=cls.deprecated,
        )

        if subparsers is None:
            parser = B2ArgumentParser(**parser_kwargs)
        else:
            parser = subparsers.add_parser(
                parser_kwargs.pop('prog'),
                **parser_kwargs,
                aliases=[alias] if alias is not None and not for_docs else (),
                add_help_all=False,
            )
            # Register class that will handle this particular command, for both name and alias.
            parser.set_defaults(command_class=cls)

        cls._setup_parser(parser)

        if cls.subcommands_registry:
            if not parents:
                common_parser = B2ArgumentParser(add_help=False, add_help_all=False)
                add_normalized_argument(
                    common_parser, '--debug-logs', action='store_true', help=argparse.SUPPRESS
                )
                common_parser.add_argument('--verbose', action='store_true', help=argparse.SUPPRESS)
                add_normalized_argument(common_parser, '--log-config', help=argparse.SUPPRESS)
                common_parser.add_argument('--profile', default=None, help=argparse.SUPPRESS)
                common_parser.add_argument(
                    '-q', '--quiet', action='store_true', default=False, help=argparse.SUPPRESS
                )

                common_parser.add_argument(
                    '--escape-control-characters', action='store_true', help=argparse.SUPPRESS
                )
                common_parser.add_argument(
                    '--no-escape-control-characters',
                    dest='escape_control_characters',
                    action='store_false',
                    help=argparse.SUPPRESS
                )

                common_parser.set_defaults(escape_control_characters=None)
                parents = [common_parser]

            subparsers = parser.add_subparsers(
                prog=parser.prog,
                title='usages',
                dest='command',
                parser_class=B2ArgumentParser,
            )
            subparsers.required = True
            for subcommand in cls.subcommands_registry.values():
                subcommand.create_parser(
                    subparsers=subparsers,
                    parents=parents,
                    for_docs=for_docs,
                    b2_binary_name=b2_binary_name
                )

        return parser

    def run(self, args):
        self.quiet = args.quiet
        self.escape_control_characters = args.escape_control_characters
        with self.exit_stack:
            return self._run(args)

    @abstractmethod
    def _run(self, args) -> int:
        ...

    @classmethod
    def _setup_parser(cls, parser):
        pass

    @classmethod
    def _parse_file_infos(cls, args_info):
        file_infos = {}
        for info in args_info:
            parts = info.split('=', 1)
            if len(parts) == 1:
                raise BadFileInfo(info)
            file_infos[parts[0]] = parts[1]
        return file_infos

    def _print_json(self, data) -> None:
        return self._print(
            json.dumps(data, indent=4, sort_keys=True, ensure_ascii=True, cls=B2CliJsonEncoder),
            enforce_output=True
        )

    def _print_human_readable_structure(self, data) -> None:
        output = io.StringIO()
        readable_yaml_dump(data, output)
        return self._print(output.getvalue().rstrip())

    def _print(
        self,
        *args,
        enforce_output: bool = False,
        end: str | None = None,
    ) -> None:
        return self._print_standard_descriptor(
            self.stdout,
            "stdout",
            *args,
            enforce_output=enforce_output,
            end=end,
        )

    def _print_stderr(self, *args, end: str | None = None) -> None:
        return self._print_standard_descriptor(
            self.stderr, "stderr", *args, enforce_output=True, end=end
        )

    def _print_standard_descriptor(
        self,
        descriptor,
        descriptor_name: str,
        *args,
        enforce_output: bool = False,
        end: str | None = None,
    ) -> None:
        """
        Prints to fd, unless quiet is set.

        :param descriptor: file descriptor to print to
        :param descriptor_name: name of the descriptor, used for error reporting
        :param args: object to be printed
        :param enforce_output: overrides quiet setting; Should not be used for anything other than data
        :param end: end of the line characters; None for default newline
        """
        if not self.quiet or enforce_output:
            self._print_helper(
                descriptor,
                descriptor.encoding,
                descriptor_name,
                *args,
                end=end,
                sanitize=self.escape_control_characters
            )

    @classmethod
    def _print_helper(
        cls,
        descriptor,
        descriptor_encoding: str,
        descriptor_name: str,
        *args,
        sanitize: bool = True,
        end: str | None = None
    ):
        if sanitize:
            args = tuple(unprintable_to_hex(arg) or '' for arg in args)
        try:
            descriptor.write(' '.join(args))
        except UnicodeEncodeError:
            sys.stderr.write(
                "\nWARNING: Unable to print unicode.  Encoding for {} is: '{}'\n".format(
                    descriptor_name,
                    descriptor_encoding,
                )
            )
            args = [arg.encode('ascii', 'backslashreplace').decode() for arg in args]
            sys.stderr.write("Trying to print: %s\n" % args)
            descriptor.write(' '.join(args))
        descriptor.write("\n" if end is None else end)

    def __str__(self):
        return f'{self.__class__.__module__}.{self.__class__.__name__}'


class CmdReplacedByMixin:
    deprecated = True
    replaced_by_cmd: type[Command] | tuple[type[Command], ...]

    def run(self, args):
        self._print_stderr(
            f'WARNING: `{self.__class__.name_and_alias()[0]}` command is deprecated. '
            f'Use `{self.get_replaced_command_name()}` instead.'
        )
        return super().run(args)

    @classmethod
    def _get_description(cls, **kwargs):
        return (
            f'{super()._get_description(**kwargs)}\n\n'
            f'.. warning::\n'
            f'   This command is deprecated. Use ``{cls.get_replaced_command_name()}`` instead.\n'
        )

    @classmethod
    def get_replaced_command_name(cls) -> str:
        if isinstance(cls.replaced_by_cmd, tuple):
            return ' '.join(cmd.name_and_alias()[0] for cmd in cls.replaced_by_cmd)
        return cls.replaced_by_cmd.name_and_alias()[0]


class B2(Command):
    """
    This program provides command-line access to the B2 service.

    There are two flows of authorization:

    * call ``{NAME} account authorize`` and have the credentials cached in sqlite
    * set ``{B2_APPLICATION_KEY_ID_ENV_VAR}`` and ``{B2_APPLICATION_KEY_ENV_VAR}`` environment
      variables when running this program

    This program caches authentication-related and other data in a local SQLite database.
    The location of this database is determined in the following way:

    If ``--profile`` arg is provided:

    * ``{XDG_CONFIG_HOME_ENV_VAR}/b2/db-<profile>.sqlite``, if ``{XDG_CONFIG_HOME_ENV_VAR}`` env var is set
    * ``{B2_ACCOUNT_INFO_PROFILE_FILE}``

    Otherwise:

    * ``{B2_ACCOUNT_INFO_ENV_VAR}`` env var's value, if set
    * ``{B2_ACCOUNT_INFO_DEFAULT_FILE}``, if it exists
    * ``{XDG_CONFIG_HOME_ENV_VAR}/b2/account_info``, if ``{XDG_CONFIG_HOME_ENV_VAR}`` env var is set
    * ``{B2_ACCOUNT_INFO_DEFAULT_FILE}``, as default

    If the directory ``{XDG_CONFIG_HOME_ENV_VAR}/b2`` does not exist (and is needed), it is created.
    Please note that the above rules may be changed in next versions of b2sdk, and in order to get
    reliable authentication file location you should use ``b2 account get``.

    Control characters escaping is turned on if running under terminal.
    You can override it by explicitly using `--escape-control-chars`/`--no-escape-control-chars`` option,
    or by setting `B2_ESCAPE_CONTROL_CHARACTERS` environment variable to either `1` or `0`.

    You can suppress command stdout & stderr output by using ``--quiet`` option.
    To supress only progress bar, use ``--no-progress`` option.

    For more details on one command:

    .. code-block::

        {NAME} <command> --help

    When authorizing with application keys, this tool requires that the key
    have the ``listBuckets`` capability so that it can take the bucket names
    you provide on the command line and translate them into bucket IDs for the
    B2 Storage service.  Each different command may required additional
    capabilities.  You can find the details for each command in the help for
    that command.

    A string provided via an optional environment variable ``{B2_USER_AGENT_APPEND_ENV_VAR}``
    will be appended to the User-Agent.
    """

    REQUIRES_AUTH = False

    subcommands_registry = ClassRegistry()

    @classmethod
    def name_and_alias(cls):
        return resolve_b2_bin_call_name(), None

    def _run(self, args):
        # Commands could be named via name or alias, so we fetch
        # the command from args assigned during parser preparation.
        return args.command_class


class AccountAuthorizeBase(Command):
    """
    Authorize an account with credentials.

    Prompts for Backblaze ``applicationKeyId`` and ``applicationKey`` (unless they are given
    on the command line).

    You can authorize with either the master application key or
    a normal application key.

    To use the master application key, provide the application key ID and
    application key from the ``B2 Cloud Storage Buckets`` page on
    the web site: https://secure.backblaze.com/b2_buckets.htm

    To use a normal application key, created with the ``key create``
    command or on the web site, provide the application key ID
    and the application key itself.

    You can also optionally provide application key ID and application key
    using environment variables ``{B2_APPLICATION_KEY_ID_ENV_VAR}`` and
    ``{B2_APPLICATION_KEY_ENV_VAR}`` respectively.

    Stores an account auth token in a local cache, see


    .. code-block::

        {NAME} --help

    for details on how the location of this cache is determined.


    Requires capability:

    - **listBuckets**
    """

    FORBID_LOGGING_ARGUMENTS = True
    REQUIRES_AUTH = False

    @classmethod
    def _setup_parser(cls, parser):
        realm_group = parser.add_mutually_exclusive_group()
        realm_group.add_argument('--dev', action='store_true', help=argparse.SUPPRESS)
        realm_group.add_argument('--staging', action='store_true', help=argparse.SUPPRESS)
        realm_group.add_argument('--environment', help=argparse.SUPPRESS)

        parser.add_argument('applicationKeyId', nargs='?')
        parser.add_argument('applicationKey', nargs='?')
        super()._setup_parser(parser)

    def _run(self, args):
        # Handle internal options for testing inside Backblaze.
        # These are not documented in the usage string.
        realm = self._get_user_requested_realm(args)

        if args.applicationKeyId is None:
            args.applicationKeyId = (
                os.environ.get(B2_APPLICATION_KEY_ID_ENV_VAR) or
                input('Backblaze application key ID: ')
            )

        if args.applicationKey is None:
            args.applicationKey = (
                os.environ.get(B2_APPLICATION_KEY_ENV_VAR) or
                getpass.getpass('Backblaze application key: ')
            )

        status = self.authorize(args.applicationKeyId, args.applicationKey, realm)
        if status == 0:
            data = format_account_info(self.api.account_info)
            self._print_json(data)
        return status

    def authorize(self, application_key_id, application_key, realm: str | None):
        """
        Perform the authorization and capability checks, report errors.

        :param application_key_id: application key ID used to authenticate
        :param application_key: application key
        :param realm: authorization realm; if None, production is used
        :return: exit status
        """
        verbose_realm = bool(realm)
        realm = realm or 'production'
        url = REALM_URLS.get(realm, realm)
        logger.info(f"Using {url}")
        if verbose_realm:
            self._print_stderr(f'Using {url}')
        try:
            self.api.authorize_account(realm, application_key_id, application_key)

            allowed = self.api.account_info.get_allowed()
            if 'listBuckets' not in allowed['capabilities']:
                logger.error(
                    'ConsoleTool cannot work with a bucket-restricted key and no listBuckets capability'
                )
                self._print_stderr(
                    'ERROR: application key has no listBuckets capability, which is required for the b2 command-line tool'
                )
                self.api.account_info.clear()
                return 1
            if allowed['bucketId'] is not None and allowed['bucketName'] is None:
                logger.error('ConsoleTool has bucket-restricted key and the bucket does not exist')
                self._print_stderr(
                    "ERROR: application key is restricted to bucket id '{}', which no longer exists"
                    .format(allowed['bucketId'])
                )
                self.api.account_info.clear()
                return 1
            return 0
        except B2Error as e:
            logger.exception('ConsoleTool account authorization error')
            self._print_stderr('ERROR: unable to authorize account: ' + str(e))
            return 1

    @classmethod
    def _get_user_requested_realm(cls, args) -> str | None:
        """
        Determine the realm to use for authorization.
        """
        if args.dev:
            return 'dev'
        if args.staging:
            return 'staging'
        if args.environment:
            return args.environment

        return os.environ.get(B2_ENVIRONMENT_ENV_VAR)


class FileLargeUnfinishedCancelBase(Command):
    """
    When used with a b2id://fileId, cancels a large file upload.
    Cannot be used once the file is finished.  After finishing,
    use ``rm`` to delete the large file.

    When used with a b2://bucketName, lists all large files that
    have been started but not finished and cancels them.  Any parts
    that have been uploaded will be deleted.

    Requires capability:

    - **listFiles** (if canceling unfinished large files in a bucket)
    - **writeFiles**
    """

    def _run(self, args):
        b2_uri = self.get_b2_uri_from_arg(args)
        if isinstance(b2_uri, B2FileIdURI):
            self.api.cancel_large_file(b2_uri.file_id)
            self._print(b2_uri.file_id, 'canceled')
        elif isinstance(b2_uri, B2URI):
            bucket = self.api.get_bucket_by_name(b2_uri.bucket_name)
            for file_version in bucket.list_unfinished_large_files():
                bucket.cancel_large_file(file_version.file_id)
                self._print(file_version.file_id, 'canceled')
        else:
            self._print_stderr(f'ERROR: unsupported URI "{b2_uri}"')
            return 1
        return 0


class AccountClearBase(Command):
    """
    Erase everything in local cache.

    See

    .. code-block::

        {NAME} --help

    for details on how the location of this cache is determined.
    """

    REQUIRES_AUTH = False

    def _run(self, args):
        self.api.account_info.clear()
        return 0


class FileCopyByIdBase(
    HeaderFlagsMixin, DestinationSseMixin, SourceSseMixin, FileRetentionSettingMixin,
    LegalHoldMixin, Command
):
    """
    Copy a file version to the given bucket (server-side, **not** via download+upload).

    Copies the contents of the source B2 file to destination bucket
    and assigns the given name to the new B2 file,
    possibly setting options like server-side encryption and retention.

    {FILE_RETENTION_COMPATIBILITY_WARNING}

    By default, it copies the file info and content type, therefore ``--content-type`` and ``--info`` are optional.
    If one of them is set, the other has to be set as well.

    To force the destination file to have empty fileInfo, use ``--no-info``.

    By default, the whole file gets copied, but you can copy an (inclusive!) range of bytes
    from the source file to the new file using ``--range`` option.

    Each ``--info`` entry is in the form ``a=b``, you can specify many.

    The maximum file size is 5GB or 10TB, depending on capability of installed ``b2sdk`` version.

    {DestinationSseMixin}
    {SourceSseMixin}
    {FileRetentionSettingMixin}
    {LegalHoldMixin}

    If either the source or the destination uses SSE-C and ``--content-type`` and ``--info`` are not provided, then
    to perform the copy the source file's metadata has to be fetched first - an additional request to B2 cloud has
    to be made. To achieve that, provide ``--fetch-metadata``. Without that flag, the command will fail.

    Requires capability:

    - **readFiles** (if ``sourceFileId`` bucket is private)
    - **writeFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(parser, '--fetch-metadata', action='store_true', default=False)
        add_normalized_argument(
            parser, '--metadata-directive', default=None, help=argparse.SUPPRESS
        )
        add_normalized_argument(parser, '--content-type')
        parser.add_argument('--range', type=parse_range)

        info_group = parser.add_mutually_exclusive_group()

        add_normalized_argument(info_group, '--info', action='append')
        add_normalized_argument(info_group, '--no-info', action='store_true', default=False)

        parser.add_argument('sourceFileId')
        parser.add_argument('destinationBucketName')
        parser.add_argument('b2FileName')

        super()._setup_parser(parser)  # add parameters from the mixins

    def _run(self, args):
        file_infos = None
        if args.info:
            file_infos = self._parse_file_infos(args.info)
        elif args.no_info:
            file_infos = {}
        file_infos = self._file_info_with_header_args(args, file_infos)

        if args.metadata_directive is not None:
            self._print_stderr(
                '--metadata-directive is deprecated, the value of this argument is determined based on the existence of '
                '--content-type and --info.'
            )

        bucket = self.api.get_bucket_by_name(args.destinationBucketName)
        destination_encryption_setting = self._get_destination_sse_setting(args)
        source_encryption_setting = self._get_source_sse_setting(args)
        legal_hold = self._get_legal_hold_setting(args)
        file_retention = self._get_file_retention_setting(args)
        if args.range is not None:
            range_args = {
                'offset': args.range[0],
                'length': args.range[1] - args.range[0] + 1,
            }
        else:
            range_args = {}
        source_file_info, source_content_type = self._determine_source_metadata(
            source_file_id=args.sourceFileId,
            source_encryption=source_encryption_setting,
            destination_encryption=destination_encryption_setting,
            target_content_type=args.content_type,
            target_file_info=file_infos,
            fetch_if_necessary=args.fetch_metadata,
        )
        file_version = bucket.copy(
            args.sourceFileId,
            args.b2FileName,
            **range_args,
            content_type=args.content_type,
            file_info=file_infos,
            destination_encryption=destination_encryption_setting,
            source_encryption=source_encryption_setting,
            legal_hold=legal_hold,
            file_retention=file_retention,
            source_file_info=source_file_info,
            source_content_type=source_content_type,
        )
        self._print_json(file_version)
        return 0

    def _is_ssec(self, encryption: EncryptionSetting | None):
        if encryption is not None and encryption.mode == EncryptionMode.SSE_C:
            return True
        return False

    def _determine_source_metadata(
        self,
        source_file_id: str,
        destination_encryption: EncryptionSetting | None,
        source_encryption: EncryptionSetting | None,
        target_file_info: dict | None,
        target_content_type: str | None,
        fetch_if_necessary: bool,
    ) -> tuple[dict | None, str | None]:
        """Determine if source file metadata is necessary to perform the copy - due to sse_c_key_id"""
        if not self._is_ssec(source_encryption) and not self._is_ssec(
            destination_encryption
        ):  # no sse-c, no problem
            return None, None
        if target_file_info is not None or target_content_type is not None:  # metadataDirective=REPLACE, no problem
            return None, None
        if not fetch_if_necessary:
            raise ValueError(
                'Attempting to copy file with metadata while either source or destination uses '
                'SSE-C. Use --fetch-metadata to fetch source file metadata before copying.'
            )
        source_file_version = self.api.get_file_info(source_file_id)
        return source_file_version.file_info, source_file_version.content_type


class BucketCreateBase(DefaultSseMixin, LifecycleRulesMixin, Command):
    """
    Create a new bucket.

    Prints the ID of the bucket created.
    Optionally stores bucket info, CORS rules and lifecycle rules with the bucket.
    These can be given as JSON on the command line.

    {DefaultSseMixin}
    {LifecycleRulesMixin}

    Requires capability:

    - **writeBuckets**
    - **readBucketEncryption**
    - **writeBucketEncryption**
    - **writeBucketRetentions**
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(parser, '--bucket-info', type=validated_loads)
        add_normalized_argument(
            parser,
            '--cors-rules',
            type=validated_loads,
            help=
            "If given, the bucket will have a 'custom' CORS configuration. Accepts a JSON string."
        )
        add_normalized_argument(
            parser,
            '--file-lock-enabled',
            action='store_true',
            help=
            "If given, the bucket will have the file lock mechanism enabled. This parameter cannot be changed after bucket creation."
        )
        parser.add_argument('--replication', type=validated_loads)
        add_bucket_name_argument(parser)
        parser.add_argument('bucketType', choices=CREATE_BUCKET_TYPES)

        super()._setup_parser(parser)  # add parameters from the mixins

    def _run(self, args):
        encryption_setting = self._get_default_sse_setting(args)
        bucket = self.api.create_bucket(
            args.bucketName,
            args.bucketType,
            bucket_info=args.bucket_info,
            cors_rules=args.cors_rules,
            lifecycle_rules=args.lifecycle_rules,
            default_server_side_encryption=encryption_setting,
            is_file_lock_enabled=args.file_lock_enabled,
            replication=args.replication and ReplicationConfiguration.from_dict(args.replication),
        )
        self._print(bucket.id_)
        return 0


class KeyCreateBase(Command):
    """
    Create a new application key.

    Prints the application key information.  This is the only
    time the application key itself will be returned.  Listing application keys will show
    their IDs, but not the secret keys.

    The capabilities are passed in as a comma-separated list, like ``readFiles,writeFiles``.
    Optionally, you can pass all capabilities known to this client with ``--all-capabilities``.

    The ``duration`` is the length of time (in seconds) the new application key will exist.
    When the time expires the key will disappear and will no longer be usable.  If not
    specified, the key will not expire.

    The ``bucket`` is the name of a bucket in the account.  When specified, the key
    will only allow access to that bucket.

    The ``namePrefix`` restricts file access to files whose names start with the prefix.

    The output is the new application key ID, followed by the application key itself.
    The two values returned are the two that you pass to ``account authorize`` to use the key.

    Requires capability:

    - **writeKeys**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--bucket')
        add_normalized_argument(parser, '--name-prefix')
        parser.add_argument('--duration', type=int)
        parser.add_argument('keyName')

        capabilities = parser.add_mutually_exclusive_group(required=True)
        capabilities.add_argument('capabilities', type=parse_comma_separated_list, nargs='?')
        add_normalized_argument(capabilities, '--all-capabilities', action='store_true')
        super()._setup_parser(parser)

    def _run(self, args):
        # Translate the bucket name into a bucketId
        if args.bucket is None:
            bucket_id_or_none = None
        else:
            bucket_id_or_none = self.api.get_bucket_by_name(args.bucket).id_

        if args.all_capabilities:
            current_key_caps = set(self.api.account_info.get_allowed()['capabilities'])
            preview_feature_caps = {
                'readBucketNotifications',
                'writeBucketNotifications',
            }
            args.capabilities = sorted(
                set(ALL_CAPABILITIES) - preview_feature_caps | current_key_caps
            )

        application_key = self.api.create_key(
            capabilities=args.capabilities,
            key_name=args.keyName,
            valid_duration_seconds=args.duration,
            bucket_id=bucket_id_or_none,
            name_prefix=args.name_prefix
        )

        self._print(f'{application_key.id_} {application_key.application_key}')
        return 0


class BucketDeleteBase(Command):
    """
    Delete the bucket with the given name.

    Requires capability:

    - **deleteBuckets**
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_bucket_name_argument(parser)
        super()._setup_parser(parser)

    def _run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        self.api.delete_bucket(bucket)
        return 0


class DeleteFileVersionBase(FileIdAndOptionalFileNameMixin, Command):
    """
    Permanently and irrevocably delete one version of a file.

    {FileIdAndOptionalFileNameMixin}

    If a file is in governance retention mode, and the retention period has not expired, adding ``--bypass-governance``
    is required.

    Requires capability:

    - **deleteFiles**
    - **readFiles** (if file name not provided)

    and optionally:

    - **bypassGovernance**
    """

    @classmethod
    def _setup_parser(cls, parser):
        super()._setup_parser(parser)
        add_normalized_argument(parser, '--bypass-governance', action='store_true', default=False)

    def _run(self, args):
        file_name = self._get_file_name_from_args(args)
        file_info = self.api.delete_file_version(args.fileId, file_name, args.bypass_governance)
        self._print_json(file_info)
        return 0


class KeyDeleteBase(Command):
    """
    Delete the specified application key by ID.

    Requires capability:

    - **deleteKeys**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('applicationKeyId')
        super()._setup_parser(parser)

    def _run(self, args):
        application_key = self.api.delete_key_by_id(application_key_id=args.applicationKeyId)
        self._print(application_key.id_)
        return 0


class DownloadCommand(
    ProgressMixin,
    SourceSseMixin,
    WriteBufferSizeMixin,
    SkipHashVerificationMixin,
    Command,
    metaclass=ABCMeta
):
    """ helper methods for returning results from download commands """

    def _print_download_info(
        self, downloaded_file: DownloadedFile, output_filepath: pathlib.Path
    ) -> None:
        download_version = downloaded_file.download_version
        output_filepath_string = 'stdout' if output_filepath == STDOUT_FILEPATH else str(
            output_filepath.resolve()
        )
        self._print_file_attribute('File name', download_version.file_name)
        self._print_file_attribute('File id', download_version.id_)
        self._print_file_attribute('Output file path', output_filepath_string)
        self._print_file_attribute('File size', str(download_version.content_length))
        self._print_file_attribute('Content type', download_version.content_type)
        self._print_file_attribute('Content sha1', download_version.content_sha1)
        self._print_file_attribute(
            'Encryption', self._represent_encryption(download_version.server_side_encryption)
        )
        self._print_file_attribute(
            'Retention', self._represent_retention(download_version.file_retention)
        )
        self._print_file_attribute(
            'Legal hold', self._represent_legal_hold(download_version.legal_hold)
        )
        for label, attr_name in [
            ('CacheControl', 'cache_control'),
            ('Expires', 'expires'),
            ('ContentDisposition', 'content_disposition'),
            ('ContentLanguage', 'content_language'),
            ('ContentEncoding', 'content_encoding'),
        ]:
            attr_value = getattr(download_version, attr_name)
            if attr_value is not None:
                self._print_file_attribute(label, attr_value)
        for name in sorted(download_version.file_info):
            self._print_file_attribute(f'INFO {name}', download_version.file_info[name])
        if download_version.content_sha1 != 'none':
            self._print('Checksum matches')
        return 0

    @classmethod
    def _represent_encryption(cls, encryption: EncryptionSetting):
        # TODO: refactor to use "match" syntax after dropping python 3.9 support
        if encryption.mode is EncryptionMode.NONE:
            return 'none'
        result = f'mode={encryption.mode.value}, algorithm={encryption.algorithm.value}'
        if encryption.mode is EncryptionMode.SSE_B2:
            pass
        elif encryption.mode is EncryptionMode.SSE_C:
            if encryption.key.key_id is not None:
                result += f', key_id={encryption.key.key_id}'
        else:
            raise ValueError(f'Unsupported encryption mode: {encryption.mode}')

        return result

    @classmethod
    def _represent_retention(cls, retention: FileRetentionSetting):
        if retention.mode is RetentionMode.NONE:
            return 'none'
        if retention.mode is RetentionMode.UNKNOWN:
            return '<unauthorized to read>'
        if retention.mode in (RetentionMode.COMPLIANCE, RetentionMode.GOVERNANCE):
            return 'mode={}, retainUntil={}'.format(
                retention.mode.value,
                datetime.datetime.fromtimestamp(
                    retention.retain_until / 1000, datetime.timezone.utc
                )
            )
        raise ValueError(f'Unsupported retention mode: {retention.mode}')

    @classmethod
    def _represent_legal_hold(cls, legal_hold: LegalHold):
        if legal_hold in (LegalHold.ON, LegalHold.OFF):
            return legal_hold.value
        if legal_hold is LegalHold.UNKNOWN:
            return '<unauthorized to read>'
        if legal_hold is LegalHold.UNSET:
            return '<unset>'
        raise ValueError(f'Unsupported legal hold: {legal_hold}')

    def _print_file_attribute(self, label, value):
        self._print((label + ':').ljust(20) + ' ' + value)

    def get_local_output_filepath(
        self, filename: str, file_request: DownloadedFile
    ) -> pathlib.Path:
        if filename == '-':
            return STDOUT_FILEPATH

        output_filepath = pathlib.Path(filename)

        # As longs as it's not a directory, we're overwriting everything.
        if not output_filepath.is_dir():
            return output_filepath

        # If the output is directory, we're expected to download the file right there.
        # Normally, we overwrite the target without asking any questions, but in this case
        # user might be oblivious of the actual mistake he's about to commit.
        # If he, e.g.: downloads file by ID, he might not know the name of the file
        # and actually overwrite something unintended.
        output_directory = output_filepath
        output_filepath = output_directory / file_request.download_version.file_name
        # If it doesn't exist, we stop worrying.
        if not output_filepath.exists():
            return output_filepath

        # If it does exist, we make a unique file prefixed with the actual file name.
        file_name_as_path = pathlib.Path(file_request.download_version.file_name)
        file_name = file_name_as_path.stem
        file_extension = file_name_as_path.suffix

        # Default permissions are: readable and writable by this user only, executable by noone.
        # This "temporary" file is not automatically removed, but still created in the safest way possible.
        fd_handle, output_filepath_str = tempfile.mkstemp(
            prefix=file_name,
            suffix=file_extension,
            dir=output_directory,
        )
        # Close the handle, so the file is not locked.
        # This file is no longer 100% "safe", but that's acceptable.
        os.close(fd_handle)

        # "Normal" file created by Python has readable for everyone, writable for user only.
        # We change the permissions, to match the default ones.
        os.chmod(output_filepath_str, 0o644)

        return pathlib.Path(output_filepath_str)


class FileDownloadBase(
    ThreadsMixin,
    MaxDownloadStreamsMixin,
    DownloadCommand,
):
    """
    Download the given file-like object, and store it in the given local file.

    {ProgressMixin}
    {ThreadsMixin}
    {SourceSseMixin}
    {WriteBufferSizeMixin}
    {SkipHashVerificationMixin}
    {MaxDownloadStreamsMixin}

    Requires capability:

    - **readFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        super()._setup_parser(parser)
        parser.add_argument('localFileName')

    def _run(self, args):
        progress_listener = self.make_progress_listener(
            args.localFileName, args.no_progress or args.quiet
        )
        encryption_setting = self._get_source_sse_setting(args)
        self._set_threads_from_args(args)

        b2_uri = self.get_b2_uri_from_arg(args)
        downloaded_file = self.api.download_file_by_uri(
            b2_uri, progress_listener, encryption=encryption_setting
        )

        output_filepath = self.get_local_output_filepath(args.localFileName, downloaded_file)
        self._print_download_info(downloaded_file, output_filepath)
        progress_listener.change_description(output_filepath.name)

        downloaded_file.save_to(output_filepath)
        self._print('Download finished')

        return 0


class FileCatBase(B2URIFileArgMixin, DownloadCommand):
    """
    Download content of a file-like object identified by B2 URI directly to stdout.

    {ProgressMixin}
    {SourceSseMixin}
    {WriteBufferSizeMixin}
    {SkipHashVerificationMixin}

    Requires capability:

    - **readFiles**
    """

    def _run(self, args):
        target_filename = '-'
        progress_listener = self.make_progress_listener(
            target_filename, args.no_progress or args.quiet
        )
        encryption_setting = self._get_source_sse_setting(args)
        file_request = self.api.download_file_by_uri(
            args.B2_URI, progress_listener=progress_listener, encryption=encryption_setting
        )
        output_filepath = self.get_local_output_filepath(target_filename, file_request)
        file_request.save_to(output_filepath)
        return 0


class AccountGetBase(Command):
    """
    Show current account info

    Prints account ID, key, auth token, URLs, and what capabilities
    the current application keys has.
    """

    def _run(self, args):
        data = format_account_info(self.api.account_info)
        self._print_json(data)
        return 0


class BucketGetBase(Command):
    """
    Display bucket info

    Prints all of the information about the bucket, including
    bucket info, CORS rules and lifecycle rules.

    If ``--show-size`` is specified, then display the number of files
    (``fileCount``) in the bucket and the aggregate size of all files
    (``totalSize``). Hidden files and hide markers are accounted for
    in the reported number of files, and hidden files also
    contribute toward the reported aggregate size, whereas hide
    markers do not. Each version of a file counts as an individual
    file, and its size contributes toward the aggregate size.
    Analysis is recursive.

    .. note::

        Note that ``--show-size`` requires multiple
        API calls, and will therefore incur additional latency,
        computation, and Class C transactions.

    Requires capability:

    - **listBuckets**
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(parser, '--show-size', action='store_true')
        add_bucket_name_argument(parser)
        super()._setup_parser(parser)

    def _run(self, args):
        # This always wants up-to-date info, so it does not use
        # the bucket cache.
        for b in self.api.list_buckets(args.bucketName):
            if not args.show_size:
                self._print_json(b)
                return 0
            else:
                result = b.as_dict()
                # `files` is a generator. We don't want to collect all of the values from the
                # generator, as there many be billions of files in a large bucket.
                files = b.ls("", latest_only=False, recursive=True)
                # `files` yields tuples of (file_version, folder_name). We don't care about
                # `folder_name`, so just access the first slot of the tuple directly in the
                # reducer. We can't ask a generator for its size, as the elements are yielded
                # lazily, so we need to accumulate the count as we go. By using a tuple of
                # (file count, total size), we can obtain the desired information very compactly
                # and efficiently.
                count_size_tuple = functools.reduce(
                    (lambda partial, f: (partial[0] + 1, partial[1] + f[0].size)), files, (0, 0)
                )
                result['fileCount'] = count_size_tuple[0]
                result['totalSize'] = count_size_tuple[1]
                self._print_json(result)
                return 0
        self._print_stderr('bucket not found: ' + args.bucketName)
        return 1


class FileInfoBase(Command):
    """
    Print file info

    Prints all of the information about the object, but not its contents.

    Requires capability:

    - **readFiles**
    """

    def _run(self, args):
        b2_uri = self.get_b2_uri_from_arg(args)
        file_version = self.api.get_file_info_by_uri(b2_uri)
        self._print_json(file_version)
        return 0


class BucketGetDownloadAuthBase(Command):
    """
    Display authorization token for downloading files

    Prints an authorization token that is valid only for downloading
    files from the given bucket.

    The token is valid for the duration specified, which defaults
    to 86400 seconds (one day).

    Only files that match that given prefix can be downloaded with
    the token.  The prefix defaults to "", which matches all files
    in the bucket.

    Requires capability:

    - **shareFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--prefix', default='')
        parser.add_argument('--duration', type=int, default=86400)
        add_bucket_name_argument(parser)
        super()._setup_parser(parser)

    def _run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        auth_token = bucket.get_download_authorization(
            file_name_prefix=args.prefix, valid_duration_in_seconds=args.duration
        )
        self._print(auth_token)
        return 0


class GetDownloadUrlWithAuthBase(Command):
    """
    Print a URL to download the given file.

    The URL includes an authorization
    token that allows downloads from the given bucket for files whose names
    start with the given file name.

    The URL will work for the given file, but is not specific to that file.  Files
    with longer names that start with the give file name can also be downloaded
    with the same auth token.

    The token is valid for the duration specified, which defaults
    to 86400 seconds (one day).

    Requires capability:

    - **shareFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--duration', type=int, default=86400)
        add_bucket_name_argument(parser)
        parser.add_argument('fileName').completer = file_name_completer
        super()._setup_parser(parser)

    def _run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        auth_token = bucket.get_download_authorization(
            file_name_prefix=args.fileName, valid_duration_in_seconds=args.duration
        )
        base_url = self.api.get_download_url_for_file_name(args.bucketName, args.fileName)
        url = base_url + '?Authorization=' + auth_token
        self._print(url)
        return 0


class FileHideBase(Command):
    """
    Upload a new, hidden, version of the given file.

    Requires capability:

    - **writeFiles**
    """

    def _run(self, args):
        b2_uri = self.get_b2_uri_from_arg(args)
        if isinstance(b2_uri, B2URI):
            bucket_name = b2_uri.bucket_name
            file_name = b2_uri.path
        else:
            bucket_name = b2_uri
            file_name = args.fileName

        bucket = self.api.get_bucket_by_name(bucket_name)
        file_info = bucket.hide_file(file_name)
        self._print_json(file_info)
        return 0


class HideFileBase(Command):
    @classmethod
    def _setup_parser(cls, parser):
        add_bucket_name_argument(parser)
        parser.add_argument('fileName').completer = file_name_completer
        super()._setup_parser(parser)

    def _run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        file_info = bucket.hide_file(args.fileName)
        self._print_json(file_info)
        return 0


class FileUnhideBase(Command):
    """
    Delete the "hide marker" for a given file.

    Requires capability:

    - **listFiles**
    - **deleteFiles**

    and optionally:

    - **bypassGovernance**
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(parser, '--bypass-governance', action='store_true', default=False)
        super()._setup_parser(parser)

    def _run(self, args):
        b2_uri = self.get_b2_uri_from_arg(args)
        bucket = self.api.get_bucket_by_name(b2_uri.bucket_name)
        file_id_and_name = bucket.unhide_file(b2_uri.path, args.bypass_governance)
        self._print_json(file_id_and_name)
        return 0


class BucketListBase(Command):
    """
    List all of the buckets in the current account.

    Output lines list the bucket ID, bucket type, and bucket name,
    and look like this:

    .. code-block::

        98c960fd1cb4390c5e0f0519  allPublic   my-bucket

    Alternatively, the ``--json`` option produces machine-readable output
    similar (but not identical) to the server api response format.

    Requires capability:

    - **listBuckets**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--json', action='store_true')
        super()._setup_parser(parser)

    def _run(self, args):
        return self.__class__.run_list_buckets(self, json_=args.json)

    @classmethod
    def run_list_buckets(cls, command: Command, *, json_: bool) -> int:
        buckets = command.api.list_buckets()
        if json_:
            command._print_json(list(buckets))
            return 0

        for b in buckets:
            command._print(f'{b.id_}  {b.type_:<10}  {b.name}')
        return 0


class KeyListBase(Command):
    """
    List the application keys for the current account.

    The columns in the output are:

    - ID of the application key
    - Name of the application key
    - Name of the bucket the key is restricted to, or ``-`` for no restriction
    - Date of expiration, or ``-``
    - Time of expiration, or ``-``
    - File name prefix, in single quotes
    - Command-separated list of capabilities

    None of the values contain whitespace.

    For keys restricted to buckets that do not exist any more, the bucket name is
    replaced with ``id=<bucketId>``, because deleted buckets do not have names any
    more.

    Requires capability:

    - **listKeys**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--long', action='store_true')
        super()._setup_parser(parser)

    def __init__(self, console_tool):
        super().__init__(console_tool)
        self.bucket_id_to_bucket_name = None

    def _run(self, args):
        for key in self.api.list_keys():
            self.print_key(key, args.long)

        return 0

    def print_key(self, key: ApplicationKey, is_long_format: bool):
        if is_long_format:
            format_str = "{keyId}   {keyName:20s}   {bucketName:20s}   {dateStr:10s}   {timeStr:8s}   '{namePrefix}'   {capabilities}"
        else:
            format_str = '{keyId}   {keyName:20s}'
        timestamp_or_none = apply_or_none(int, key.expiration_timestamp_millis)
        (date_str, time_str) = self.timestamp_display(timestamp_or_none)
        key_str = format_str.format(
            keyId=key.id_,
            keyName=key.key_name,
            bucketName=self.bucket_display_name(key.bucket_id),
            namePrefix=(key.name_prefix or ''),
            capabilities=','.join(key.capabilities),
            dateStr=date_str,
            timeStr=time_str,
        )
        self._print(key_str)

    def bucket_display_name(self, bucket_id):
        # Special case for no bucket ID
        if bucket_id is None:
            return '-'

        # Make sure we have the map
        if self.bucket_id_to_bucket_name is None:
            self.bucket_id_to_bucket_name = dict((b.id_, b.name) for b in self.api.list_buckets())

        return self.bucket_id_to_bucket_name.get(bucket_id, 'id=' + bucket_id)

    def timestamp_display(self, timestamp_or_none):
        """
        Returns a pair (date_str, time_str) for the given timestamp
        """
        if timestamp_or_none is None:
            return '-', '-'
        else:
            timestamp = timestamp_or_none
            dt = datetime.datetime.fromtimestamp(timestamp / 1000, datetime.timezone.utc)
            return dt.strftime('%Y-%m-%d'), dt.strftime('%H:%M:%S')


class FileLargePartsBase(Command):
    """
    Lists all of the parts that have been uploaded for the given
    large file, which must be a file that was started but not
    finished or canceled.

    Requires capability:

    - **writeFiles**
    """

    def _run(self, args):
        b2_uri = self.get_b2_uri_from_arg(args)
        for part in self.api.list_parts(b2_uri.file_id):
            self._print('%5d  %9d  %s' % (part.part_number, part.content_length, part.content_sha1))
        return 0


class FileLargeUnfinishedListBase(Command):
    """
    Lists all of the large files in the bucket that were started,
    but not finished or canceled.

    Requires capability:

    - **listFiles**
    """

    def _run(self, args):
        b2_uri = self.get_b2_uri_from_arg(args)
        bucket = self.api.get_bucket_by_name(b2_uri.bucket_name)
        for unfinished in bucket.list_unfinished_large_files():
            file_info_text = ' '.join(
                f'{k}={unfinished.file_info[k]}' for k in sorted(unfinished.file_info)
            )
            self._print(
                f'{unfinished.file_id} {unfinished.file_name} {unfinished.content_type} {file_info_text}'
            )
        return 0


class AbstractLsCommand(Command, metaclass=ABCMeta):
    """
    The ``--versions`` option selects all versions of each file, not
    just the most recent.

    The ``--recursive`` option will descend into folders, and will select
    only files, not folders.

    The ``--with-wildcard`` option will allow using ``*``, ``?`` and ```[]```
    characters in ``folderName`` as a greedy wildcard, single character
    wildcard and range of characters. It requires the ``--recursive`` option.
    Remember to quote ``folderName`` to avoid shell expansion.

    The --include and --exclude flags can be used to filter the files returned
    from the server using wildcards. You can specify multiple --include and --exclude filters.
    The order of filters matters. The *last* matching filter decides whether a file
    is included or excluded. If the given list of filters contains only INCLUDE filters,
    then it is assumed that all files are excluded by default.
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--versions', action='store_true')
        parser.add_argument('-r', '--recursive', action='store_true')
        add_normalized_argument(parser, '--with-wildcard', action='store_true')
        parser.add_argument(
            '--include', dest='filters', action='append', type=Filter.include, default=[]
        )
        parser.add_argument(
            '--exclude', dest='filters', action='append', type=Filter.exclude, default=[]
        )
        super()._setup_parser(parser)

    def _print_files(self, args, b2_uri: B2URI | None = None):
        generator = self._get_ls_generator(args, b2_uri=b2_uri)

        for file_version, folder_name in generator:
            self._print_file_version(args, file_version, folder_name)

    def _print_file_version(
        self,
        args,
        file_version: FileVersion,
        folder_name: str | None,
    ) -> None:
        name = folder_name or file_version.file_name
        if args.escape_control_characters:
            name = escape_control_chars(name)
        self._print(name)

    def _get_ls_generator(self, args, b2_uri: B2URI | None = None):
        b2_uri = b2_uri or self.get_b2_uri_from_arg(args)
        try:
            yield from self.api.ls(
                b2_uri,
                latest_only=not args.versions,
                recursive=args.recursive,
                with_wildcard=args.with_wildcard,
                filters=args.filters,
                folder_to_list_can_be_a_file=True,
            )
        except Exception as err:
            raise CommandError(unprintable_to_hex(str(err))) from err

    def get_b2_uri_from_arg(self, args: argparse.Namespace) -> B2URI:
        raise NotImplementedError


class BaseLs(AbstractLsCommand, metaclass=ABCMeta):
    """
    List files in a given folder.

    Using the file naming convention that ``/`` separates folder
    names from their contents, returns a list of the files
    and folders in a given folder.  If no folder name is given,
    lists all files at the top level.

    The ``--long`` option produces very wide multi-column output
    showing the upload date/time, file size, file id, whether it
    is an uploaded file or the hiding of a file, and the file
    name.  Folders don't really exist in B2, so folders are
    shown with ``-`` in each of the fields other than the name.

    The ``--json`` option produces machine-readable output similar to
    the server api response format.

    The ``--replication`` option adds replication status

    {AbstractLsCommand}
    """

    # order is file_id, action, date, time, size(, replication), name
    LS_ENTRY_TEMPLATE = '%83s  %6s  %10s  %8s  %9d  %s'
    LS_ENTRY_TEMPLATE_REPLICATION = LS_ENTRY_TEMPLATE + '  %s'

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--long', action='store_true')
        parser.add_argument('--json', action='store_true')
        parser.add_argument('--replication', action='store_true')
        super()._setup_parser(parser)

    def _run(self, args):
        b2_uri = self.get_b2_uri_from_arg(args)
        if args.long and args.json:
            raise CommandError('Cannot use --long and --json options together')

        if not b2_uri or b2_uri == B2URI(""):
            for option_name in ('long', 'recursive', 'replication'):
                if getattr(args, option_name, False):
                    raise CommandError(
                        f'Cannot use --{option_name} option without specifying a bucket name'
                    )
            return ListBuckets.run_list_buckets(self, json_=args.json)

        if args.json:
            i = -1
            for i, (file_version, _) in enumerate(self._get_ls_generator(args, b2_uri=b2_uri)):
                if i:
                    self._print(',', end='')
                else:
                    self._print('[')
                self._print_json(file_version)
            self._print(']' if i >= 0 else '[]')
        else:
            self._print_files(args)
        return 0

    def _print_file_version(
        self,
        args,
        file_version: FileVersion,
        folder_name: str | None,
    ) -> None:
        if not args.long:
            super()._print_file_version(args, file_version, folder_name)
        elif folder_name is not None:
            self._print(self.format_folder_ls_entry(args, folder_name, args.replication))
        else:
            self._print(self.format_ls_entry(args, file_version, args.replication))

    def format_folder_ls_entry(self, args, name, replication: bool):
        if args.escape_control_characters:
            name = escape_control_chars(name)
        if replication:
            return self.LS_ENTRY_TEMPLATE_REPLICATION % ('-', '-', '-', '-', 0, '-', name)
        return self.LS_ENTRY_TEMPLATE % ('-', '-', '-', '-', 0, name)

    def format_ls_entry(self, args, file_version: FileVersion, replication: bool):
        dt = datetime.datetime.fromtimestamp(
            file_version.upload_timestamp / 1000, datetime.timezone.utc
        )
        date_str = dt.strftime('%Y-%m-%d')
        time_str = dt.strftime('%H:%M:%S')
        size = file_version.size or 0  # required if self.action == 'hide'
        template = replication and self.LS_ENTRY_TEMPLATE_REPLICATION or self.LS_ENTRY_TEMPLATE
        parameters = [
            file_version.id_,
            file_version.action,
            date_str,
            time_str,
            size,
        ]
        if replication:
            replication_status = file_version.replication_status
            parameters.append(replication_status.value if replication_status else '-')
        name = file_version.file_name
        if args.escape_control_characters:
            name = escape_control_chars(name)
        parameters.append(name)
        return template % tuple(parameters)


class Ls(B2IDOrB2URIMixin, BaseLs):
    """
    {BaseLs}

    Examples

    .. note::

        Note the use of quotes, to ensure that special
        characters are not expanded by the shell.


    List csv and tsv files (in any directory, in the whole bucket):

    .. code-block::

        {NAME} ls --recursive --with-wildcard "b2://bucketName/*.[ct]sv"


    List all info.txt files from directories named `b?`, where `?` is any character:

    .. code-block::

        {NAME} ls --recursive --with-wildcard "b2://bucketName/b?/info.txt"


    List all pdf files from directories b0 to b9 (including sub-directories):

    .. code-block::

        {NAME} ls --recursive --with-wildcard "b2://bucketName/b[0-9]/*.pdf"


    List all buckets:

    .. code-block::

        {NAME} ls


    Requires capability:

    - **listFiles**
    - **listBuckets** (if bucket name is not provided)
    """
    ALLOW_ALL_BUCKETS = True


class BaseRm(ThreadsMixin, AbstractLsCommand, metaclass=ABCMeta):
    """
    Remove a "folder" or a set of files matching a pattern.

    Use with caution!

    .. note::

        ``rm`` is a high-level command that under the hood utilizes multiple calls to the server,
        which means the server cannot guarantee consistency between multiple operations. For
        example if a file matching a pattern is uploaded during a run of ``rm`` command, it MIGHT
        be deleted (as "latest") instead of the one present when the ``rm`` run has started.

    If a file is in governance retention mode, and the retention period has not expired,
    adding --bypass-governance is required.

    To list (but not remove) files to be deleted, use ``--dry-run``.  You can also
    list files via ``ls`` command - the listing behaviour is exactly the same.

    Progress is displayed on the console unless ``--no-progress`` is specified.
    {ThreadsMixin}
    {AbstractLsCommand}

    The ``--dry-run`` option prints all the files that would be affected by
    the command, but removes nothing.

    Normally, when an error happens during file removal, log is printed and the command
    goes further. If any error should be immediately breaking the command,
    ``--fail-fast`` can be passed to ensure that first error will stop the execution.
    This could be useful to e.g. check whether provided credentials have **deleteFiles**
    capabilities.

    .. note::

        Using ``--fail-fast`` doesn't prevent the command from trying to remove further files.
        It just stops the progress. Since multiple files are removed in parallel, it's possible
        that just some of them were not reported.

    Command returns 0 if all files were removed successfully and
    a value different from 0 if any file was left.
    """

    PROGRESS_REPORT_CLASS = ProgressReport

    class SubmitThread(threading.Thread):
        END_MARKER = object()
        ERROR_TAG = 'error'
        EXCEPTION_TAG = 'general_exception'

        def __init__(
            self,
            runner: BaseRm,
            args: argparse.Namespace,
            messages_queue: queue.Queue,
            reporter: ProgressReport,
            threads: int,
        ):
            self.runner = runner
            self.args = args
            self.messages_queue = messages_queue
            self.reporter = reporter
            self.threads = threads
            removal_queue_size = self.args.queue_size or (2 * self.threads)
            self.semaphore = threading.BoundedSemaphore(value=removal_queue_size)
            self.fail_fast_event = threading.Event()
            self.mapping_lock = threading.Lock()
            self.futures_mapping = {}
            super().__init__(daemon=True)

        def run(self) -> None:
            try:
                with ThreadPoolExecutor(max_workers=self.threads) as executor:
                    self._run_removal(executor)
            except Exception as error:
                self.messages_queue.put((self.EXCEPTION_TAG, error))
            finally:
                self.messages_queue.put(self.END_MARKER)

        def _run_removal(self, executor: Executor):
            for file_version, subdirectory in self.runner._get_ls_generator(self.args):
                if subdirectory is not None:
                    # This file_version is not for listing/deleting.
                    # It is only here to list the subdirectory, so skip deleting it.
                    continue
                # Obtaining semaphore limits number of elements that we fetch from LS.
                self.semaphore.acquire(blocking=True)
                # This event is updated before the semaphore is released. This way,
                # in a single threaded scenario, we get synchronous responses.
                if self.fail_fast_event.is_set():
                    break

                self.reporter.update_total(1)
                future = executor.submit(
                    self.runner.api.delete_file_version,
                    file_version.id_,
                    file_version.file_name,
                    self.args.bypass_governance,
                )
                with self.mapping_lock:
                    self.futures_mapping[future] = file_version
                # Done callback is added after, so it's "sure" that mapping is updated earlier.
                future.add_done_callback(self._removal_done)

            self.reporter.end_total()

        def _removal_done(self, future: Future) -> None:
            with self.mapping_lock:
                file_version = self.futures_mapping.pop(future)

            try:
                future.result()
                self.reporter.update_count(1)
            except FileNotPresent:
                # We wanted to remove this file anyway.
                self.reporter.update_count(1)
            except B2Error as error:
                if self.args.fail_fast:
                    # This is set before releasing the semaphore.
                    # It means that when the semaphore is released,
                    # we'll already have information about requirement to fail.
                    self.fail_fast_event.set()
                self.messages_queue.put((self.ERROR_TAG, file_version, error))
            except Exception as error:
                self.messages_queue.put((self.EXCEPTION_TAG, error))
            finally:
                self.semaphore.release()

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(parser, '--bypass-governance', action='store_true', default=False)
        add_normalized_argument(parser, '--dry-run', action='store_true')
        add_normalized_argument(parser,
            '--queue-size',
            type=int,
            default=None,
            help='max elements fetched at once for removal, ' \
                 'if left unset defaults to twice the number of threads.',
        )
        add_normalized_argument(parser, '--no-progress', action='store_true')
        add_normalized_argument(parser, '--fail-fast', action='store_true')
        super()._setup_parser(parser)

    def _run(self, args):
        if args.dry_run:
            self._print_files(args)
            return 0
        failed_on_any_file = False
        messages_queue = queue.Queue()

        threads = self._get_threads_from_args(args)
        with self.PROGRESS_REPORT_CLASS(self.stdout, args.no_progress or args.quiet) as reporter:
            submit_thread = self.SubmitThread(self, args, messages_queue, reporter, threads=threads)
            # This thread is started in daemon mode, no joining needed.
            submit_thread.start()

            while True:
                queue_entry = messages_queue.get(block=True)
                if queue_entry is submit_thread.END_MARKER:
                    break

                event_type, *data = queue_entry
                if event_type == submit_thread.ERROR_TAG:
                    file_version, error = data
                    message = f'Deletion of file "{file_version.file_name}" ' \
                              f'({file_version.id_}) failed: {str(error)}'
                    reporter.print_completion(message)

                    failed_on_any_file = True
                    if args.fail_fast:
                        break

                elif event_type == submit_thread.EXCEPTION_TAG:
                    raise data[0]

        return 1 if failed_on_any_file else 0


class Rm(B2IDOrB2URIMixin, BaseRm):
    """
    {BaseRm}

    Examples.

        .. note::

            Note the use of quotes, to ensure that special
            characters are not expanded by the shell.


        .. note::

            Use with caution. Running examples presented below can cause data-loss.


        Remove all csv and tsv files (in any directory, in the whole bucket):

        .. code-block::

            {NAME} rm --recursive --with-wildcard "b2://bucketName/*.[ct]sv"


        Remove all info.txt files from buckets bX, where X is any character:

        .. code-block::

            {NAME} rm --recursive --with-wildcard "b2://bucketName/b?/info.txt"


        Remove all pdf files from buckets b0 to b9 (including sub-directories):

        .. code-block::

            {NAME} rm --recursive --with-wildcard "b2://bucketName/b[0-9]/*.pdf"


    Requires capability:

    - **listFiles**
    - **deleteFiles**
    - **bypassGovernance** (if --bypass-governance is used)
    """


class FileUrlBase(Command):
    """
    Display download URL for a file

    Prints an URL that can be used to download the given file, if
    it is public.

    If it is private, you can use --with-auth to include an authorization
    token in the URL that allows downloads from the given bucket for files
    whose names start with the given file name.

    The URL will work for the given file, but is not specific to that file.  Files
    with longer names that start with the give file name can also be downloaded
    with the same auth token.

    The token is valid for the duration specified, which defaults
    to 86400 seconds (one day).


    Requires capability:

    - **shareFiles** (if using --with-auth)
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(parser, '--with-auth', action='store_true')
        parser.add_argument('--duration', type=int, default=86400)
        super()._setup_parser(parser)

    def _run(self, args):
        b2_uri = self.get_b2_uri_from_arg(args)
        url = self.api.get_download_url_by_uri(b2_uri)
        if args.with_auth:
            bucket = self.api.get_bucket_by_name(b2_uri.bucket_name)
            auth_token = bucket.get_download_authorization(
                file_name_prefix=b2_uri.path, valid_duration_in_seconds=args.duration
            )
            url += '?Authorization=' + auth_token
        self._print(url)
        return 0


class Sync(
    ThreadsMixin,
    DestinationSseMixin,
    SourceSseMixin,
    WriteBufferSizeMixin,
    SkipHashVerificationMixin,
    MaxDownloadStreamsMixin,
    UploadModeMixin,
    Command,
):
    """
    Copy multiple files from source to destination.

    Optionally deletes or hides destination files that the source does not have.

    The synchronizer can copy files:

    - From a B2 bucket to a local destination.
    - From a local source to a B2 bucket.
    - From one B2 bucket to another.
    - Between different folders in the same B2 bucket.

    Use ``b2://<bucketName>/<prefix>`` for B2 paths, e.g. ``b2://my-bucket-name/a/path/prefix/``.

    Progress is displayed on the console unless ``--no-progress`` is
    specified.  A list of actions taken is always printed.

    Specify ``--dry-run`` to simulate the actions that would be taken.

    To allow sync to run when the source directory is empty, potentially
    deleting all files in a bucket, specify ``--allow-empty-source``.
    The default is to fail when the specified source directory doesn't exist
    or is empty.  (This check only applies to version 1.0 and later.)

    {ThreadsMixin}

    You can alternatively control number of threads per each operation.
    The number of files processed in parallel is set by ``--sync-threads``,
    the number of files/file parts downloaded in parallel is set by``--download-threads``,
    and the number of files/file parts uploaded in parallel is set by `--upload-threads``.
    All the three parameters can be set to the same value by ``--threads``.
    Experiment with parameters if the defaults are not working well.

    Users with low-performance networks may benefit from reducing the
    number of threads.  Using just one thread will minimize the impact
    on other users of the network.

    .. note::

        Note that using multiple threads could be detrimental to
        the other users on your network.

    You can specify ``--exclude-regex`` to selectively ignore files that
    match the given pattern. Ignored files will not copy during
    the sync operation. The pattern is a regular expression
    that is tested against the full path of each file.

    You can specify ``--include-regex`` to selectively override ignoring
    files that match the given ``--exclude-regex`` pattern by an
    ``--include-regex`` pattern. Similarly to ``--exclude-regex``, the pattern
    is a regular expression that is tested against the full path
    of each file.

    .. note::

        Note that ``--include-regex`` cannot be used without ``--exclude-regex``.

    You can specify ``--exclude-all-symlinks`` to skip symlinks when
    syncing from a local source.

    When a directory is excluded by using ``--exclude-dir-regex``, all of
    the files within it are excluded, even if they match an ``--include-regex``
    pattern.   This means that there is no need to look inside excluded
    directories, and you can exclude directories containing files for which
    you don't have read permission and avoid getting errors.

    The ``--exclude-dir-regex`` is a regular expression that is tested against
    the full path of each directory.  The path being matched does not have
    a trailing ``/``, so don't include on in your regular expression.

    Multiple regex rules can be applied by supplying them as pipe
    delimited instructions. Note that the regex for this command
    is Python regex.
    Reference: `<https://docs.python.org/3/library/re.html>`_

    Regular expressions are considered a match if they match a substring
    starting at the first character.  ``.*e`` will match ``hello``.  This is
    not ideal, but we will maintain this behavior for compatibility.
    If you want to match the entire path, put a ``$`` at the end of the
    regex, such as ``.*llo$``.

    You can specify ``--exclude-if-modified-after`` to selectively ignore file versions
    (including hide markers) which were synced after given time (for local source)
    or ignore only specific file versions (for b2 source).
    Ignored files or file versions will not be taken for consideration during sync.
    The time should be given as a seconds timestamp (e.g. "1367900664")
    If you need milliseconds precision, put it after the comma (e.g. "1367900664.152")

    Files are considered to be the same if they have the same name
    and modification time.  This behaviour can be changed using the
    ``--compare-versions`` option. Possible values are:

    - ``none``:    Comparison using the file name only
    - ``modTime``: Comparison using the modification time (default)
    - ``size``:    Comparison using the file size

    A future enhancement may add the ability to compare the SHA1 checksum
    of the files.

    Fuzzy comparison of files based on modTime or size can be enabled by
    specifying the ``--compare-threshold`` option.  This will treat modTimes
    (in milliseconds) or sizes (in bytes) as the same if they are within
    the comparison threshold.  Files that match, within the threshold, will
    not be synced. Specifying ``--verbose`` and ``--dry-run`` can be useful to
    determine comparison value differences.

    When a destination file is present that is not in the source, the
    default is to leave it there.  Specifying ``--delete`` means to delete
    destination files that are not in the source.

    When the destination is B2, you have the option of leaving older
    versions in place.  Specifying ``--keep-days`` will delete any older
    versions more than the given number of days old, based on the
    modification time of the file.  This option is not available when
    the destination is a local folder.

    Files at the source that have a newer modification time are always
    copied to the destination.  If the destination file is newer, the
    default is to report an error and stop.  But with ``--skip-newer`` set,
    those files will just be skipped.  With ``--replace-newer`` set, the
    old file from the source will replace the newer one in the destination.

    To make the destination exactly match the source, use:

    .. code-block::

        {NAME} sync --delete --replace-newer ... ...

    .. warning::

        Using ``--delete`` deletes files!  We recommend not using it.
        If you use ``--keep-days`` instead, you will have some time to recover your
        files if you discover they are missing on the source end.

    To make the destination match the source, but retain previous versions
    for 30 days:

    .. code-block::

        {NAME} sync --keep-days 30 --replace-newer ... b2://...

    Example of sync being used with ``--exclude-regex``. This will ignore ``.DS_Store`` files
    and ``.Spotlight-V100`` folders:

    .. code-block::

        {NAME} sync --exclude-regex '(.*\\.DS_Store)|(.*\\.Spotlight-V100)' ... b2://...

    {DestinationSseMixin}
    {SourceSseMixin}

    {WriteBufferSizeMixin}
    {SkipHashVerificationMixin}
    {MaxDownloadStreamsMixin}
    {UploadModeMixin}

    Requires capabilities:

    - **listFiles**
    - **readFiles** (for downloading)
    - **writeFiles** (for uploading)
    """

    DEFAULT_SYNC_THREADS = 10
    DEFAULT_DOWNLOAD_THREADS = 10
    DEFAULT_UPLOAD_THREADS = 10

    FAIL_ON_REPORTER_ERRORS_OR_WARNINGS = True

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(parser, '--no-progress', action='store_true')
        add_normalized_argument(parser, '--dry-run', action='store_true')
        add_normalized_argument(parser, '--allow-empty-source', action='store_true')
        add_normalized_argument(parser, '--exclude-all-symlinks', action='store_true')
        add_normalized_argument(
            parser, '--sync-threads', type=int, default=cls.DEFAULT_SYNC_THREADS
        )
        add_normalized_argument(
            parser, '--download-threads', type=int, default=cls.DEFAULT_DOWNLOAD_THREADS
        )
        add_normalized_argument(
            parser, '--upload-threads', type=int, default=cls.DEFAULT_UPLOAD_THREADS
        )
        add_normalized_argument(
            parser, '--compare-versions', default='modTime', choices=('none', 'modTime', 'size')
        )
        add_normalized_argument(parser, '--compare-threshold', type=int, metavar='MILLIS')
        add_normalized_argument(
            parser, '--exclude-regex', action='append', default=[], metavar='REGEX'
        )
        add_normalized_argument(
            parser, '--include-regex', action='append', default=[], metavar='REGEX'
        )
        add_normalized_argument(
            parser, '--exclude-dir-regex', action='append', default=[], metavar='REGEX'
        )
        add_normalized_argument(
            parser,
            '--exclude-if-modified-after',
            type=parse_millis_from_float_timestamp,
            default=None,
            metavar='TIMESTAMP'
        )
        super()._setup_parser(parser)  # add parameters from the mixins, and the parent class
        parser.add_argument('source')
        parser.add_argument('destination')

        skip_group = parser.add_mutually_exclusive_group()
        add_normalized_argument(skip_group, '--skip-newer', action='store_true')
        add_normalized_argument(skip_group, '--replace-newer', action='store_true')

        del_keep_group = parser.add_mutually_exclusive_group()
        add_normalized_argument(del_keep_group, '--delete', action='store_true')
        add_normalized_argument(del_keep_group, '--keep-days', type=float, metavar='DAYS')

    def _run(self, args):
        policies_manager = self.get_policies_manager_from_args(args)

        if args.threads is not None:
            if args.sync_threads != self.DEFAULT_SYNC_THREADS \
                    or args.upload_threads != self.DEFAULT_UPLOAD_THREADS \
                    or args.download_threads != self.DEFAULT_DOWNLOAD_THREADS:
                raise ValueError("--threads cannot be used with other thread options")
            sync_threads = upload_threads = download_threads = args.threads
        else:
            sync_threads = args.sync_threads
            upload_threads = args.upload_threads
            download_threads = args.download_threads

        self.api.services.upload_manager.set_thread_pool_size(upload_threads)
        self.api.services.download_manager.set_thread_pool_size(download_threads)

        source = parse_sync_folder(args.source, self.console_tool.api)
        destination = parse_sync_folder(args.destination, self.console_tool.api)
        allow_empty_source = args.allow_empty_source or VERSION_0_COMPATIBILITY

        synchronizer = self.get_synchronizer_from_args(
            args,
            sync_threads,
            policies_manager,
            allow_empty_source,
            self.api.session.account_info.get_absolute_minimum_part_size(),
        )

        kwargs = {}
        read_encryption_settings = {}
        write_encryption_settings = {}
        source_bucket = destination_bucket = None
        destination_sse = self._get_destination_sse_setting(args)
        if destination.folder_type() == 'b2':
            destination_bucket = destination.bucket_name
            write_encryption_settings[destination_bucket] = destination_sse
        elif destination_sse is not None:
            raise ValueError('server-side encryption cannot be set for a non-b2 sync destination')

        source_sse = self._get_source_sse_setting(args)
        if source.folder_type() == 'b2':
            source_bucket = source.bucket_name
            read_encryption_settings[source_bucket] = source_sse
        elif source_sse is not None:
            raise ValueError('server-side encryption cannot be set for a non-b2 sync source')

        if read_encryption_settings or write_encryption_settings:
            kwargs['encryption_settings_provider'] = BasicSyncEncryptionSettingsProvider(
                read_bucket_settings=read_encryption_settings,
                write_bucket_settings=write_encryption_settings,
            )

        with SyncReport(self.stdout, args.no_progress or args.quiet) as reporter:
            try:
                synchronizer.sync_folders(
                    source_folder=source,
                    dest_folder=destination,
                    now_millis=current_time_millis(),
                    reporter=reporter,
                    **kwargs
                )
            except EmptyDirectory as ex:
                raise CommandError(
                    f'Directory {ex.path} is empty.  Use --allow-empty-source to sync anyway.'
                )
            except NotADirectory as ex:
                raise CommandError(f'{ex.path} is not a directory')
            except UnableToCreateDirectory as ex:
                raise CommandError(f'unable to create directory {ex.path}')
            if self.FAIL_ON_REPORTER_ERRORS_OR_WARNINGS and reporter.has_errors_or_warnings():
                return 1
        return 0

    def get_policies_manager_from_args(self, args):
        return ScanPoliciesManager(
            exclude_dir_regexes=args.exclude_dir_regex,
            exclude_file_regexes=args.exclude_regex,
            include_file_regexes=args.include_regex,
            exclude_all_symlinks=args.exclude_all_symlinks,
            exclude_modified_after=args.exclude_if_modified_after,
        )

    def get_synchronizer_from_args(
        self,
        args,
        max_workers,
        policies_manager=DEFAULT_SCAN_MANAGER,
        allow_empty_source=False,
        absolute_minimum_part_size=None,
    ):
        if args.replace_newer:
            newer_file_mode = NewerFileSyncMode.REPLACE
        elif args.skip_newer:
            newer_file_mode = NewerFileSyncMode.SKIP
        else:
            newer_file_mode = NewerFileSyncMode.RAISE_ERROR

        if args.compare_versions == 'none':
            compare_version_mode = CompareVersionMode.NONE
        elif args.compare_versions == 'modTime':
            compare_version_mode = CompareVersionMode.MODTIME
        elif args.compare_versions == 'size':
            compare_version_mode = CompareVersionMode.SIZE
        else:
            compare_version_mode = CompareVersionMode.MODTIME
        compare_threshold = args.compare_threshold

        keep_days = None

        if args.delete:
            keep_days_or_delete = KeepOrDeleteMode.DELETE
        elif args.keep_days:
            keep_days_or_delete = KeepOrDeleteMode.KEEP_BEFORE_DELETE
            keep_days = args.keep_days
        else:
            keep_days_or_delete = KeepOrDeleteMode.NO_DELETE

        upload_mode = self._get_upload_mode_from_args(args)

        return Synchronizer(
            max_workers,
            policies_manager=policies_manager,
            dry_run=args.dry_run,
            allow_empty_source=allow_empty_source,
            newer_file_mode=newer_file_mode,
            keep_days_or_delete=keep_days_or_delete,
            compare_version_mode=compare_version_mode,
            compare_threshold=compare_threshold,
            keep_days=keep_days,
            upload_mode=upload_mode,
            absolute_minimum_part_size=absolute_minimum_part_size,
        )


class BucketUpdateBase(DefaultSseMixin, LifecycleRulesMixin, Command):
    """
    Updates the ``bucketType`` of an existing bucket.

    Prints the ID of the bucket updated.
    Optionally stores bucket info, CORS rules and lifecycle rules with the bucket.
    These can be given as JSON on the command line.

    {DefaultSseMixin}
    {LifecycleRulesMixin}

    To set a default retention for files in the bucket ``--default-retention-mode`` and
    ``--default-retention-period`` have to be specified. The latter one is of the form "X days|years".

    {FILE_RETENTION_COMPATIBILITY_WARNING}

    This command can be used to set the bucket's ``fileLockEnabled`` flag to ``true`` using the ``--file-lock-enabled``
    option.  This can only be done if the bucket is not set up as a replication source.

    .. warning::

        Once ``fileLockEnabled`` is set, it can NOT be reverted back to ``false``

    Please note that replication from file-lock-enabled bucket to file-lock-disabled bucket is not allowed, therefore
    if file lock is enabled on a bucket, it can never again be the replication source bucket for a file-lock-disabled destination.

    Additionally in a file-lock-enabled bucket the file metadata limit will be decreased from 7000 bytes to 2048 bytes for new file versions
    Please consult ``b2_update_bucket`` official documentation for further guidance.


    Requires capability:

    - **writeBuckets**
    - **readBucketEncryption**

    and for some operations:

    - **writeBucketRetentions**
    - **writeBucketEncryption**
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(parser, '--bucket-info', type=validated_loads)
        add_normalized_argument(
            parser,
            '--cors-rules',
            type=validated_loads,
            help=
            "If given, the bucket will have a 'custom' CORS configuration. Accepts a JSON string."
        )
        add_normalized_argument(
            parser,
            '--default-retention-mode',
            choices=(
                RetentionMode.COMPLIANCE.value,
                RetentionMode.GOVERNANCE.value,
                'none',
            ),
            default=None,
        )
        add_normalized_argument(
            parser,
            '--default-retention-period',
            type=parse_default_retention_period,
            metavar='period',
        )
        parser.add_argument('--replication', type=validated_loads)
        add_normalized_argument(
            parser,
            '--file-lock-enabled',
            action='store_true',
            default=None,
            help=
            "If given, the bucket will have the file lock mechanism enabled. This parameter cannot be changed back."
        )
        add_bucket_name_argument(parser)
        parser.add_argument('bucketType', nargs='?', choices=CREATE_BUCKET_TYPES)

        super()._setup_parser(parser)  # add parameters from the mixins and the parent class

    def _run(self, args):
        if args.default_retention_mode is not None:
            if args.default_retention_mode == 'none':
                default_retention = NO_RETENTION_BUCKET_SETTING
            else:
                default_retention = BucketRetentionSetting(
                    RetentionMode(args.default_retention_mode), args.default_retention_period
                )
        else:
            default_retention = None
        encryption_setting = self._get_default_sse_setting(args)
        if args.replication is None:
            replication = None
        else:
            replication = ReplicationConfiguration.from_dict(args.replication)
        bucket = self.api.get_bucket_by_name(args.bucketName)
        bucket = bucket.update(
            bucket_type=args.bucketType,
            bucket_info=args.bucket_info,
            cors_rules=args.cors_rules,
            lifecycle_rules=args.lifecycle_rules,
            default_server_side_encryption=encryption_setting,
            default_retention=default_retention,
            replication=replication,
            is_file_lock_enabled=args.file_lock_enabled,
        )
        self._print_json(bucket)
        return 0


class MinPartSizeMixin(Described):
    """
    By default, the file is broken into many parts to maximize upload parallelism and increase speed.
    Setting ``--min-part-size`` controls the minimal upload file part size.
    Part size must be in 5MB to 5GB range.
    Reference: `<https://www.backblaze.com/docs/cloud-storage-create-large-files-with-the-native-api>`_
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(
            parser,
            '--min-part-size',
            type=int,
            help="minimum part size in bytes",
            default=None,
        )
        super()._setup_parser(parser)  # noqa


class UploadFileMixin(
    HeaderFlagsMixin,
    MinPartSizeMixin,
    ThreadsMixin,
    ProgressMixin,
    DestinationSseMixin,
    LegalHoldMixin,
    FileRetentionSettingMixin,
    metaclass=ABCMeta
):
    """
    Content type is optional.
    If not set, it will be guessed.

    The maximum number of upload threads to use to upload parts of a large file is specified by ``--threads``.
    It has no effect on "small" files (under 200MB as of writing this).

    Each fileInfo is of the form ``a=b``.
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(
            parser,
            '--content-type',
            help="MIME type of the file being uploaded. If not set it will be guessed."
        )
        parser.add_argument(
            '--sha1', help="SHA-1 of the data being uploaded for verifying file integrity"
        )
        parser.add_argument(
            '--info',
            action='append',
            default=[],
            help=
            "additional file info to be stored with the file. Can be used multiple times for different information."
        )
        add_normalized_argument(
            parser,
            '--custom-upload-timestamp',
            type=int,
            help="overrides object creation date. Expressed as a number of milliseconds since epoch."
        )
        add_bucket_name_argument(parser, help="name of the bucket where the file will be stored")
        parser.add_argument('localFilePath', help="path of the local file or stream to be uploaded")
        parser.add_argument('b2FileName', help="name file will be given when stored in B2")

        super()._setup_parser(parser)  # add parameters from the mixins

    def _run(self, args):
        self._set_threads_from_args(args)
        upload_kwargs = self.get_execute_kwargs(args)
        file_info = self.execute_operation(**upload_kwargs)
        bucket = upload_kwargs["bucket"]
        self._print("URL by file name: " + bucket.get_download_url(file_info.file_name))
        self._print("URL by fileId: " + self.api.get_download_url_for_fileid(file_info.id_))
        self._print_json(file_info)
        return 0

    def get_execute_kwargs(self, args) -> dict:
        file_infos = self._parse_file_infos(args.info)

        if SRC_LAST_MODIFIED_MILLIS not in file_infos and os.path.exists(args.localFilePath):
            try:
                mtime = os.path.getmtime(args.localFilePath)
            except OSError:
                if not points_to_fifo(pathlib.Path(args.localFilePath)):
                    self._print_stderr(
                        "WARNING: Unable to determine file modification timestamp. "
                        f"{SRC_LAST_MODIFIED_MILLIS!r} file info won't be set."
                    )
            else:
                file_infos[SRC_LAST_MODIFIED_MILLIS] = str(int(mtime * 1000))

        file_infos = self._file_info_with_header_args(args, file_infos)

        return {
            "bucket":
                self.api.get_bucket_by_name(args.bucketName),
            "content_type":
                args.content_type,
            "custom_upload_timestamp":
                args.custom_upload_timestamp,
            "encryption":
                self._get_destination_sse_setting(args),
            "file_info":
                file_infos,
            "file_name":
                args.b2FileName,
            "file_retention":
                self._get_file_retention_setting(args),
            "legal_hold":
                self._get_legal_hold_setting(args),
            "local_file":
                args.localFilePath,
            "min_part_size":
                args.min_part_size,
            "progress_listener":
                self.make_progress_listener(args.localFilePath, args.no_progress or args.quiet),
            "sha1_sum":
                args.sha1,
            "threads":
                self._get_threads_from_args(args),
        }

    @abstractmethod
    def execute_operation(self, **kwargs) -> b2sdk.file_version.FileVersion:
        raise NotImplementedError

    def upload_file_kwargs_to_unbound_upload(self, **kwargs):
        """
        Translate `file upload` kwargs to unbound_upload equivalents
        """
        kwargs["large_file_sha1"] = kwargs.pop("sha1_sum", None)
        kwargs["buffers_count"] = kwargs["threads"] + 1
        kwargs["read_size"] = kwargs["min_part_size"] or DEFAULT_MIN_PART_SIZE
        return kwargs

    def get_input_stream(self, filename: str) -> str | int | io.BinaryIO:
        """Get input stream IF filename points to a FIFO or stdin."""
        if filename == "-":
            return sys.stdin.buffer if platform.system() == "Windows" else sys.stdin.fileno()
        elif points_to_fifo(pathlib.Path(filename)):
            return filename

        raise self.NotAnInputStream()

    def file_identifier_to_read_stream(self, file_id: str | int | BinaryIO, buffering) -> BinaryIO:
        if isinstance(file_id, (str, int)):
            return open(
                file_id,
                mode="rb",
                closefd=not isinstance(file_id, int),
                buffering=buffering,
            )
        return file_id

    class NotAnInputStream(Exception):
        pass


class FileUploadBase(UploadFileMixin, UploadModeMixin, Command):
    """
    Upload single file to the given bucket.

    Uploads the contents of the local file, and assigns the given name to the B2 file,
    possibly setting options like server-side encryption and retention.

    A FIFO file (such as named pipe) can be given instead of regular file.

    By default, `file upload` will compute the sha1 checksum of the file
    to be uploaded.  But, if you already have it, you can provide it
    on the command line to save a little time.

    {FILE_RETENTION_COMPATIBILITY_WARNING}
    {UploadFileMixin}
    {MinPartSizeMixin}
    {ProgressMixin}
    {ThreadsMixin}
    {DestinationSseMixin}
    {FileRetentionSettingMixin}
    {LegalHoldMixin}
    {UploadModeMixin}

    The ``--custom-upload-timestamp``, in milliseconds-since-epoch, can be used
    to artificially change the upload timestamp of the file for the purpose
    of preserving retention policies after migration of data from other storage.
    The access to this feature is restricted - if you really need it, you'll
    need to contact customer support to enable it temporarily for your account.

    Requires capability:

    - **writeFiles**
    """

    def get_execute_kwargs(self, args) -> dict:
        kwargs = super().get_execute_kwargs(args)
        kwargs["upload_mode"] = self._get_upload_mode_from_args(args)
        return kwargs

    def execute_operation(self, local_file, bucket, threads, **kwargs):
        try:
            input_stream = self.get_input_stream(local_file)
        except self.NotAnInputStream:  # it is a regular file
            file_version = bucket.upload_local_file(local_file=local_file, **kwargs)
        else:
            if kwargs.pop("upload_mode", None) != UploadMode.FULL:
                self._print_stderr(
                    "WARNING: Ignoring upload mode setting as we are uploading a stream."
                )
            kwargs = self.upload_file_kwargs_to_unbound_upload(threads=threads, **kwargs)
            del kwargs["threads"]
            input_stream = self.file_identifier_to_read_stream(
                input_stream, kwargs["min_part_size"] or DEFAULT_MIN_PART_SIZE
            )
            with input_stream:
                file_version = bucket.upload_unbound_stream(read_only_object=input_stream, **kwargs)
        return file_version


class UploadUnboundStreamBase(UploadFileMixin, Command):
    """
    Uploads an unbound stream to the given bucket.

    Uploads the contents of the unbound stream such as stdin or named pipe,
    and assigns the given name to the resulting B2 file.

    {FILE_RETENTION_COMPATIBILITY_WARNING}
    {UploadFileMixin}
    {MinPartSizeMixin}

    As opposed to ``b2 file upload``, ``b2 upload-unbound-stream`` cannot choose optimal `partSize` on its own.
    So on memory constrained system it is best to use ``--part-size`` option to set it manually.
    During upload of unbound stream ``--part-size`` as well as ``--threads`` determine the amount of memory used.
    The maximum memory use for the upload buffers can be estimated at ``partSize * threads``, that is ~1GB by default.
    What is more, B2 Large File may consist of at most 10,000 parts, so ``minPartSize`` should be adjusted accordingly,
    if you expect the stream to be larger than 50GB.

    {ProgressMixin}
    {ThreadsMixin}
    {DestinationSseMixin}
    {FileRetentionSettingMixin}
    {LegalHoldMixin}

    The ``--custom-upload-timestamp``, in milliseconds-since-epoch, can be used
    to artificially change the upload timestamp of the file for the purpose
    of preserving retention policies after migration of data from other storage.
    The access to this feature is restricted - if you really need it, you'll
    need to contact customer support to enable it temporarily for your account.

    Requires capability:

    - **writeFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(
            parser,
            '--part-size',
            type=int,
            default=None,
            help=("part size in bytes. Must be in range of <minPartSize, 5GB>"),
        )
        add_normalized_argument(
            parser,
            '--unused-buffer-timeout-seconds',
            type=float,
            default=3600.0,
            help=(
                "maximum time in seconds that not a single part may sit in the queue,"
                " waiting to be uploaded, before an error is returned"
            ),
        )
        super()._setup_parser(parser)

    def get_execute_kwargs(self, args) -> dict:
        kwargs = super().get_execute_kwargs(args)
        kwargs = self.upload_file_kwargs_to_unbound_upload(**kwargs)
        kwargs["recommended_upload_part_size"] = args.part_size
        kwargs["unused_buffer_timeout_seconds"] = args.unused_buffer_timeout_seconds
        return kwargs

    def execute_operation(self, local_file, bucket, threads, **kwargs):
        try:
            input_stream = self.get_input_stream(local_file)
        except self.NotAnInputStream:  # it is a regular file
            self._print_stderr(
                "WARNING: You are using a stream upload command to upload a regular file. "
                "While it will work, it is inefficient. "
                "Use of `file upload` command is recommended."
            )
            input_stream = local_file

        input_stream = self.file_identifier_to_read_stream(
            input_stream, kwargs["min_part_size"] or DEFAULT_MIN_PART_SIZE
        )
        with input_stream:
            file_version = bucket.upload_unbound_stream(read_only_object=input_stream, **kwargs)
        return file_version


class FileUpdateBase(B2URIFileArgMixin, LegalHoldMixin, Command):
    """
    Update file settings.

    Setting legal holds only works in bucket with fileLockEnabled=true.

    Retention:

      Only works in bucket with fileLockEnabled=true. Providing a ``retention-mode`` other than ``none`` requires
      providing ``retainUntil``, which has to be a future timestamp in the form of an integer representing milliseconds
      since epoch.

      If a file already is in governance mode, disabling retention or shortening it's period requires providing
      ``--bypass-governance``.

      If a file already is in compliance mode, disabling retention or shortening it's period is impossible.

      In both cases prolonging the retention period is possible. Changing from governance to compliance is also supported.

      {FILE_RETENTION_COMPATIBILITY_WARNING}

    Requires capability:

    - **readFiles**
    - **writeFileLegalHolds** (if updating legal holds)
    - **writeFileRetentions** (if updating retention)
    - **bypassGovernance** (if --bypass-governance is used)
    """

    @classmethod
    def _setup_parser(cls, parser):
        super()._setup_parser(parser)

        add_normalized_argument(
            parser,
            '--file-retention-mode',
            default=None,
            choices=(RetentionMode.COMPLIANCE.value, RetentionMode.GOVERNANCE.value, 'none')
        )
        add_normalized_argument(
            parser,
            '--retain-until',
            type=parse_millis_from_float_timestamp,
            metavar='TIMESTAMP',
            default=None
        )
        add_normalized_argument(parser, '--bypass-governance', action='store_true', default=False)

    def _run(self, args):
        b2_uri = self.get_b2_uri_from_arg(args)
        file_version = self.api.get_file_info_by_uri(b2_uri)

        if args.legal_hold is not None:
            self.api.update_file_legal_hold(
                file_version.id_, file_version.file_name, LegalHold(args.legal_hold)
            )

        if args.file_retention_mode is not None:
            if args.file_retention_mode == 'none':
                file_retention = FileRetentionSetting(RetentionMode.NONE)
            else:
                file_retention = FileRetentionSetting(
                    RetentionMode(args.file_retention_mode), args.retain_until
                )

            self.api.update_file_retention(
                file_version.id_, file_version.file_name, file_retention, args.bypass_governance
            )

        return 0


class UpdateFileLegalHoldBase(FileIdAndOptionalFileNameMixin, Command):
    """
    Only works in buckets with fileLockEnabled=true.

    {FileIdAndOptionalFileNameMixin}

    Requires capability:

    - **writeFileLegalHolds**
    - **readFiles** (if file name not provided)

    """

    @classmethod
    def _setup_parser(cls, parser):
        super()._setup_parser(parser)
        parser.add_argument('legalHold', choices=(LegalHold.ON.value, LegalHold.OFF.value))

    def _run(self, args):
        file_name = self._get_file_name_from_args(args)
        legal_hold = LegalHold(args.legalHold)
        self.api.update_file_legal_hold(args.fileId, file_name, legal_hold)
        return 0


class UpdateFileRetentionBase(FileIdAndOptionalFileNameMixin, Command):
    """
    Only works in buckets with fileLockEnabled=true. Providing a ``retention-mode`` other than ``none`` requires
    providing ``retainUntil``, which has to be a future timestamp in the form of an integer representing milliseconds
    since epoch.

    If a file already is in governance mode, disabling retention or shortening it's period requires providing
    ``--bypass-governance``.

    If a file already is in compliance mode, disabling retention or shortening it's period is impossible.

    {FILE_RETENTION_COMPATIBILITY_WARNING}

    In both cases prolonging the retention period is possible. Changing from governance to compliance is also supported.

    {FileIdAndOptionalFileNameMixin}

    Requires capability:

    - **writeFileRetentions**
    - **readFiles** (if file name not provided)

    and optionally:

    - **bypassGovernance**
    """

    @classmethod
    def _setup_parser(cls, parser):
        super()._setup_parser(parser)
        parser.add_argument(
            'retentionMode',
            choices=(RetentionMode.GOVERNANCE.value, RetentionMode.COMPLIANCE.value, 'none')
        )
        add_normalized_argument(
            parser,
            '--retain-until',
            type=parse_millis_from_float_timestamp,
            metavar='TIMESTAMP',
            default=None
        )
        add_normalized_argument(parser, '--bypass-governance', action='store_true', default=False)

    def _run(self, args):
        file_name = self._get_file_name_from_args(args)

        if args.retentionMode == 'none':
            file_retention = FileRetentionSetting(RetentionMode.NONE)
        else:
            file_retention = FileRetentionSetting(
                RetentionMode(args.retentionMode), args.retain_until
            )

        self.api.update_file_retention(
            args.fileId, file_name, file_retention, args.bypass_governance
        )
        return 0


class ReplicationSetupBase(Command):
    """
    Set up replication between two buckets.

    Sets up replication between two buckets (potentially from different accounts), creating and replacing keys if necessary.

    Requires capabilities on both profiles:

    - **listKeys**
    - **createKeys**
    - **readReplications**
    - **writeReplications**
    """

    @classmethod
    def _setup_parser(cls, parser):
        super()._setup_parser(parser)
        add_normalized_argument(parser, '--destination-profile', default=None)
        parser.add_argument('source', metavar='SOURCE_BUCKET_NAME')
        parser.add_argument('destination', metavar='DESTINATION_BUCKET_NAME')
        add_normalized_argument(
            parser, '--name', help='name for the new replication rule on the source side'
        )
        add_normalized_argument(
            parser,
            '--priority',
            help=
            'priority for the new replication rule on the source side [%d-%d]. Will be set automatically when not specified.'
            % (
                ReplicationRule.MIN_PRIORITY,
                ReplicationRule.MAX_PRIORITY,
            ),
            type=int,
            default=None,
        )
        add_normalized_argument(
            parser,
            '--file-name-prefix',
            metavar='PREFIX',
            help='only replicate files starting with PREFIX'
        )
        add_normalized_argument(
            parser,
            '--include-existing-files',
            action='store_true',
            help='if given, also replicates files uploaded prior to creation of the replication rule'
        )

    def _run(self, args):
        if args.destination_profile is None:
            destination_api = self.api
        else:
            destination_api = _get_b2api_for_profile(args.destination_profile)

        helper = ReplicationSetupHelper()
        helper.setup_both(
            source_bucket=self.api.get_bucket_by_name(args.source).get_fresh_state(),
            destination_bucket=destination_api.get_bucket_by_name(args.destination
                                                                 ).get_fresh_state(),
            name=args.name,
            priority=args.priority,
            prefix=args.file_name_prefix,
            include_existing_files=args.include_existing_files,
        )
        return 0


class ReplicationRuleChanger(Command, metaclass=ABCMeta):
    @classmethod
    def _setup_parser(cls, parser):
        super()._setup_parser(parser)
        parser.add_argument('source', metavar='SOURCE_BUCKET_NAME')
        parser.add_argument('rule_name', metavar='REPLICATION_RULE_NAME')

    def _run(self, args):
        bucket = self.api.get_bucket_by_name(args.source).get_fresh_state()
        found, altered = self.alter_rule_by_name(bucket, args.rule_name)
        if not found:
            print('ERROR: replication rule could not be found!')
            return 1
        elif not altered:
            print('ERROR: replication rule was found, but could not be changed!')
            return 1
        return 0

    @classmethod
    def alter_rule_by_name(cls, bucket: Bucket, name: str) -> tuple[bool, bool]:
        """ returns False if rule could not be found """
        if not bucket.replication or not bucket.replication.rules:
            return False, False

        found = False
        altered = False

        new_rules = []
        for rule in bucket.replication.rules:
            if rule.name == name:
                found = True
                old_dict_form = rule.as_dict()
                new = cls.alter_one_rule(rule)
                if new is None:
                    altered = True
                    continue
                if old_dict_form != new.as_dict():
                    altered = True
            new_rules.append(rule)

        if altered:
            new_replication_configuration = ReplicationConfiguration(
                **{
                    'rules': new_rules,
                    'source_key_id': bucket.replication.source_key_id,
                },
                **bucket.replication.get_destination_configuration_as_dict(),
            )
            bucket.update(
                if_revision_is=bucket.revision,
                replication=new_replication_configuration,
            )
        return found, altered

    @classmethod
    @abstractmethod
    def alter_one_rule(cls, rule: ReplicationRule) -> ReplicationRule | None:
        """ return None to delete a rule """
        pass


class ReplicationDeleteBase(ReplicationRuleChanger):
    """
    Delete a replication rule

    Requires capabilities:

    - **readReplications**
    - **writeReplications**
    """

    @classmethod
    def alter_one_rule(cls, rule: ReplicationRule) -> ReplicationRule | None:
        """ return None to delete rule """
        return None


class ReplicationPauseBase(ReplicationRuleChanger):
    """
    Pause a replication rule

    Requires capabilities:

    - **readReplications**
    - **writeReplications**
    """

    @classmethod
    def alter_one_rule(cls, rule: ReplicationRule) -> ReplicationRule | None:
        """ return None to delete rule """
        rule.is_enabled = False
        return rule


class ReplicationUnpauseBase(ReplicationRuleChanger):
    """
    Unpause a replication rule

    Requires capabilities:

    - **readReplications**
    - **writeReplications**
    """

    @classmethod
    def alter_one_rule(cls, rule: ReplicationRule) -> ReplicationRule | None:
        """ return None to delete rule """
        rule.is_enabled = True
        return rule


class ReplicationStatusBase(Command):
    """
    Display detailed replication statistics

    Inspects files in only source or both source and destination buckets
    (potentially from different accounts) and provides detailed replication statistics.

    Please be aware that only latest file versions are inspected, so any previous
    file versions are not represented in these statistics.

    --output-format
    "Console" output format is meant to be human-readable and is subject to change
    in any further release. One should use "json" for reliable "no-breaking-changes"
    output format. When piping "csv" format to some .csv file, it's handy to use
    --no-progress flag which will disable interactive reporting output, otherwise it will
    also go to target csv file's first line.

    --columns
    Comma-separated list of columns to be shown. The rows are still grouped by _all_
    columns, no matter which of them are shown / hidden when using --columns flag.
    """

    @classmethod
    def _setup_parser(cls, parser):
        super()._setup_parser(parser)
        parser.add_argument('source', metavar='SOURCE_BUCKET_NAME')
        add_normalized_argument(parser, '--rule', metavar='REPLICATION_RULE_NAME', default=None)
        add_normalized_argument(parser, '--destination-profile')
        add_normalized_argument(parser, '--dont-scan-destination', action='store_true')
        add_normalized_argument(
            parser, '--output-format', default='console', choices=('console', 'json', 'csv')
        )
        add_normalized_argument(parser, '--no-progress', action='store_true')
        add_normalized_argument(
            parser,
            '--columns',
            default=['all'],
            type=lambda value: re.split(r', ?', value),
            metavar='COLUMN ONE,COLUMN TWO'
        )

    def _run(self, args):
        destination_api = args.destination_profile and _get_b2api_for_profile(
            args.destination_profile
        )

        try:
            bucket = self.api.list_buckets(args.source)[0]
        except IndexError:
            self._print_stderr(f'ERROR: bucket "{args.source}" not found')
            return 1

        rules = bucket.replication.rules
        if args.rule:
            rules = [rule for rule in rules if rule.name == args.rule]
            if not rules:
                self._print_stderr(
                    f'ERROR: no replication rule "{args.rule}" set up for bucket "{args.source}"'
                )
                return 1

        results = {
            rule.name: self.get_results_for_rule(
                bucket=bucket,
                rule=rule,
                destination_api=destination_api,
                scan_destination=not args.dont_scan_destination,
                quiet=args.no_progress or args.quiet,
            )
            for rule in rules
        }

        if args.columns[0] != 'all':
            results = {
                rule_name: self.filter_results_columns(
                    rule_results,
                    [column.replace(' ', '_') for column in args.columns
                    ],  # allow users to use spaces instead of underscores
                )
                for rule_name, rule_results in results.items()
            }

        if args.output_format == 'json':
            self.output_json(results)
        elif args.output_format == 'console':
            self.output_console(results)
        elif args.output_format == 'csv':
            self.output_csv(results)
        else:
            self._print_stderr(f'ERROR: format "{args.output_format}" is not supported')

        return 0

    @classmethod
    def get_results_for_rule(
        cls, bucket: Bucket, rule: ReplicationRule, destination_api: B2Api | None,
        scan_destination: bool, quiet: bool
    ) -> list[dict]:
        monitor = ReplicationMonitor(
            bucket=bucket,
            rule=rule,
            destination_api=destination_api,
            report=ProgressReport(sys.stdout, quiet),
        )
        report = monitor.scan(scan_destination=scan_destination)

        return [
            {
                **dataclasses.asdict(result),
                'count': count,
            } for result, count in report.counter_by_status.items()
        ]

    @classmethod
    def filter_results_columns(cls, results: list[dict], columns: list[str]) -> list[dict]:
        return [{key: result[key] for key in columns} for result in results]

    @classmethod
    def to_human_readable(cls, value: Any) -> str:
        if isinstance(value, Enum):
            return value.name

        if isinstance(value, bool):
            return 'Yes' if value else 'No'

        if value is None:
            return ''

        return str(value)

    def output_json(self, results: dict[str, list[dict]]) -> None:
        self._print_json(results)

    def output_console(self, results: dict[str, list[dict]]) -> None:
        for rule_name, rule_results in results.items():
            self._print(f'Replication "{rule_name}":')
            rule_results = [
                {
                    key.replace('_', '\n'):  # split key to minimize column size
                    self.to_human_readable(value)
                    for key, value in result.items()
                } for result in rule_results
            ]
            self._print(tabulate(rule_results, headers='keys', tablefmt='grid'))

    def output_csv(self, results: dict[str, list[dict]]) -> None:

        rows = []

        for rule_name, rule_results in results.items():
            rows += [
                {
                    'rule name': rule_name,
                    **{
                        key.replace('_', '\n'):  # split key to minimize column size
                        self.to_human_readable(value)
                        for key, value in result.items()
                    },
                } for result in rule_results
            ]

        if not rows:
            return

        writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


class Version(Command):
    """
    Print the version number of this tool.
    """

    REQUIRES_AUTH = False

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(parser, '--short', action='store_true')
        super()._setup_parser(parser)

    def _run(self, args):
        if args.short:
            self._print(VERSION)
        else:
            self._print('b2 command line tool, version', VERSION)
        return 0


class License(Command):  # pragma: no cover
    """
    Print the license information for this tool.

    Displays the license of B2 Command line tool and all libraries shipped with it.
    """
    LICENSE_OUTPUT_FILE = pathlib.Path(__file__).parent.parent / 'licenses_output.txt'

    REQUIRES_AUTH = False
    IGNORE_MODULES = {'b2', 'distlib', 'patchelf-wrapper', 'platformdirs'}
    REQUEST_TIMEOUT_S = 5

    # In case of some modules, we provide manual
    # overrides to the license text extracted by piplicenses.
    # Thanks to this set, we make sure the module is still used
    # PTable is used on versions below Python 3.11
    MODULES_TO_OVERRIDE_LICENSE_TEXT = {'rst2ansi', 'b2sdk'}

    LICENSES = {
        'argcomplete':
            'https://raw.githubusercontent.com/kislyuk/argcomplete/develop/LICENSE.rst',
        'atomicwrites':
            'https://raw.githubusercontent.com/untitaker/python-atomicwrites/master/LICENSE',
        'platformdirs':
            'https://raw.githubusercontent.com/platformdirs/platformdirs/main/LICENSE.txt',
        'PTable':
            'https://raw.githubusercontent.com/jazzband/prettytable/main/LICENSE',
        'pipx':
            'https://raw.githubusercontent.com/pypa/pipx/main/LICENSE',
        'userpath':
            'https://raw.githubusercontent.com/ofek/userpath/master/LICENSE.txt',
        'future':
            'https://raw.githubusercontent.com/PythonCharmers/python-future/master/LICENSE.txt',
        'pefile':
            'https://raw.githubusercontent.com/erocarrera/pefile/master/LICENSE',
        'https://github.com/python/typeshed':
            'https://raw.githubusercontent.com/python/typeshed/main/LICENSE',
    }

    class NormalizingStringIO(io.StringIO):
        def write(self, text, *args, **kwargs):
            super().write(unicodedata.normalize('NFKD', text), *args, **kwargs)

    def __init__(self, console_tool):
        super().__init__(console_tool)
        self.request_session = requests.session()

    @classmethod
    def _setup_parser(cls, parser):
        # these are for building, users should not call it:
        add_normalized_argument(
            parser, '--dump', action='store_true', default=False, help=argparse.SUPPRESS
        )
        add_normalized_argument(
            parser, '--with-packages', action='store_true', default=False, help=argparse.SUPPRESS
        )
        super()._setup_parser(parser)

    def _run(self, args):
        if self.LICENSE_OUTPUT_FILE.exists() and not args.dump:
            self._print(self.LICENSE_OUTPUT_FILE.read_text(encoding='utf8'))
            return 0

        if args.dump:
            with self.LICENSE_OUTPUT_FILE.open('w', encoding='utf8') as file:
                self._put_license_text(file, with_packages=args.with_packages)
        else:
            stream = self.NormalizingStringIO()
            self._put_license_text(stream, with_packages=args.with_packages)
            stream.seek(0)
            self._print(stream.read())

        return 0

    def _put_license_text(self, stream: io.StringIO, with_packages: bool = False):
        if with_packages:
            self._put_license_text_for_packages(stream)

        b2_call_name = self.console_tool.b2_binary_name
        included_sources = get_included_sources()
        if included_sources:
            stream.write(
                f'\n\nThird party libraries modified and included in {b2_call_name} or {b2sdk.__name__}:\n'
            )
        for src in included_sources:
            stream.write('\n')
            stream.write(src.name)
            stream.write('\n')
            stream.write(src.comment)
            stream.write('\n')
            stream.write('Files included for legal compliance reasons:\n')
            files_table = prettytable.PrettyTable(['File name', 'Content'], hrules=prettytable.ALL)
            for file_name, file_content in src.files.items():
                files_table.add_row([file_name, file_content])
            stream.write(str(files_table))
        stream.write(f'\n\n{b2_call_name} license:\n')
        b2_license_file_text = (pathlib.Path(__file__).parent.parent /
                                'LICENSE').read_text(encoding='utf8')
        stream.write(b2_license_file_text)

    def _put_license_text_for_packages(self, stream: io.StringIO):
        license_table = prettytable.PrettyTable(
            ['Module name', 'License text'], hrules=prettytable.ALL
        )
        summary_table = prettytable.PrettyTable(
            ['Module name', 'Version', 'License', 'Author', 'URL'], hrules=prettytable.ALL
        )

        licenses = self._get_licenses_dicts()
        modules_added = set()
        for module_info in licenses:
            if module_info['Name'] in self.IGNORE_MODULES:
                continue
            summary_table.add_row(
                [
                    module_info['Name'],
                    module_info['Version'],
                    module_info['License'].replace(';', '\n'),
                    module_info['Author'],
                    module_info['URL'],
                ]
            )
            license_table.add_row([module_info['Name'], self._get_single_license(module_info)])
            modules_added.add(module_info['Name'])

        assert not (self.MODULES_TO_OVERRIDE_LICENSE_TEXT - modules_added)
        b2_call_name = self.console_tool.b2_binary_name
        stream.write(
            f'Licenses of all modules used by {b2_call_name}, shipped with it in binary form:\n'
        )
        stream.write(str(license_table))
        stream.write(
            f'\n\nSummary of all modules used by {b2_call_name}, shipped with it in binary form:\n'
        )
        stream.write(str(summary_table))

    @classmethod
    def _get_licenses_dicts(cls) -> list[dict]:
        assert piplicenses, 'In order to run this command, you need to install the `license` extra: pip install b2[license]'
        pipdeptree_run = subprocess.run(
            ["pipdeptree", "--json", "-p", "b2"],
            capture_output=True,
            text=True,
            check=True,
        )
        pipdeptree = json.loads(pipdeptree_run.stdout)
        used_packages = [dep["package"]['package_name'] for dep in pipdeptree]

        parser = piplicenses.create_parser()
        args = parser.parse_args(
            [
                '--format',
                'j',
                '--with-system',
                '--with-authors',
                '--with-urls',
                '--with-license-file',
                '--packages',
                *used_packages,
            ]
        )
        licenses_output = piplicenses.create_output_string(args)
        licenses = validated_loads(licenses_output)
        return licenses

    def _fetch_license_from_url(self, url: str) -> str:
        response = self.request_session.get(url, timeout=self.REQUEST_TIMEOUT_S)
        response.raise_for_status()
        return response.text

    def _get_single_license(self, module_dict: dict):
        license_ = module_dict['LicenseText']
        module_name = module_dict['Name']
        if module_name == 'rst2ansi':
            # this one module is problematic, we need to extract the license text from its docstring
            assert license_ == piplicenses.LICENSE_UNKNOWN  # let's make sure they didn't fix it
            license_ = rst2ansi.__doc__
            assert 'MIT License' in license_  # let's make sure the license is still there
        elif module_name == 'b2sdk':
            license_ = (pathlib.Path(b2sdk.__file__).parent / 'LICENSE').read_text()
        else:
            license_url = self.LICENSES.get(module_name) or self.LICENSES.get(
                module_dict.get('URL')
            )
            if license_url:
                license_ = self._fetch_license_from_url(license_url)

        assert license_ != piplicenses.LICENSE_UNKNOWN, module_name

        return license_


class InstallAutocomplete(Command):
    """
    Install autocomplete for supported shells.

    Autocomplete is installed for the current user only and will become available after shell reload.
    Any existing autocomplete configuration for same executable name will be overwritten.

    --shell SHELL
    Shell to install autocomplete for. Autodetected if not specified.
    Manually specify "bash" to force bash autocomplete installation when running under different shell.

    .. note::

        Please note this command WILL modify your shell configuration file (e.g. ~/.bashrc).
    """

    REQUIRES_AUTH = False

    @classmethod
    def _setup_parser(cls, parser):
        add_normalized_argument(parser, '--shell', choices=SUPPORTED_SHELLS, default=None)
        super()._setup_parser(parser)

    def _run(self, args):
        shell = args.shell or detect_shell()
        if shell not in SUPPORTED_SHELLS:
            self._print_stderr(
                f'ERROR: unsupported shell: {shell}. Supported shells: {SUPPORTED_SHELLS}. Use --shell to specify a target shell manually.'
            )
            return 1

        try:
            autocomplete_install(self.console_tool.b2_binary_name, shell=shell)
        except AutocompleteInstallError as e:
            raise CommandError(str(e)) from e
        self._print(f'Autocomplete successfully installed for {shell}.')
        self._print(
            f'Spawn a new shell instance to use it (log in again or just type `{shell}` in your current shell to start a new session inside of the existing session).'
        )
        return 0


class BucketNotificationRuleWarningMixin(Described):
    """
    .. warning::

        Event Notifications feature is in \"Private Preview\" state and may change without notice.
        See https://www.backblaze.com/blog/announcing-event-notifications/ for details.
    """


class BucketNotificationRuleBase(BucketNotificationRuleWarningMixin, Command):
    """
    Bucket notification rules management subcommands.

    {BucketNotificationRuleWarningMixin}

    For more information on each subcommand, use ``{NAME} bucket notification-rule SUBCOMMAND --help``.

    Examples:

    .. code-block::

        {NAME} bucket notification-rule create b2://bucketName/optionalSubPath/ ruleName --event-type "b2:ObjectCreated:*" --webhook-url https://example.com/webhook
        {NAME} bucket notification-rule list b2://bucketName
        {NAME} bucket notification-rule update b2://bucketName/newPath/ ruleName --disable --event-type "b2:ObjectCreated:*" --event-type "b2:ObjectHidden:*"
        {NAME} bucket notification-rule delete b2://bucketName ruleName
    """
    subcommands_registry = ClassRegistry(attr_name='COMMAND_NAME')


@BucketNotificationRuleBase.subcommands_registry.register
class BucketNotificationRuleList(JSONOptionMixin, BucketNotificationRuleWarningMixin, Command):
    """
    Allows listing bucket notification rules of the given bucket.

    {BucketNotificationRuleWarningMixin}

    {JSONOptionMixin}

    Examples:

    .. code-block::

        {NAME} notification-rule list b2://bucketName


    Requires capability:

    - **readBucketNotifications**
    """
    COMMAND_NAME = 'list'

    @classmethod
    def _setup_parser(cls, parser):
        add_b2_uri_argument(
            parser,
            help=
            "B2 URI of the bucket with optional path prefix, e.g. b2://bucketName or b2://bucketName/optionalSubPath/"
        )
        super()._setup_parser(parser)

    def _run(self, args):
        bucket = self.api.get_bucket_by_name(args.B2_URI.bucket_name)
        rules = sorted(
            (
                rule for rule in bucket.get_notification_rules()
                if rule["objectNamePrefix"].startswith(args.B2_URI.path)
            ),
            key=lambda rule: rule["name"]
        )
        if args.json:
            self._print_json(rules)
        else:
            if rules:
                self._print(f'Notification rules for {args.B2_URI} :')
                self._print_human_readable_structure(rules)
            else:
                self._print(f'No notification rules for {args.B2_URI}')
        return 0


class BucketNotificationRuleCreateBase(
    JSONOptionMixin, BucketNotificationRuleWarningMixin, Command
):
    @classmethod
    def _validate_secret(cls, value: str) -> str:
        if not re.match(r'^[a-zA-Z0-9]{32}$', value):
            raise argparse.ArgumentTypeError(
                f'the secret has to be exactly 32 alphanumeric characters, got: {value!r}'
            )
        return value

    @classmethod
    def setup_rule_fields_parser(cls, parser, creation: bool):
        add_b2_uri_argument(
            parser,
            help=
            "B2 URI of the bucket with optional path prefix, e.g. b2://bucketName or b2://bucketName/optionalSubPath/"
        )
        parser.add_argument('ruleName', help="Name of the rule")
        parser.add_argument(
            '--event-type',
            action='append',
            help=
            "Events scope, e.g., 'b2:ObjectCreated:*'. Can be used multiple times to set multiple scopes.",
            required=creation
        )
        parser.add_argument(
            '--webhook-url', help="URL to send the notification to", required=creation
        )
        parser.add_argument(
            '--sign-secret',
            help="optional signature key consisting of 32 alphanumeric characters ",
            type=cls._validate_secret,
            default=None,
        )
        parser.add_argument(
            '--custom-header',
            action='append',
            help=
            "Custom header to be sent with the notification. Can be used multiple times to set multiple headers. Format: HEADER_NAME=VALUE"
        )
        parser.add_argument(
            '--enable',
            action='store_true',
            help="Flag to enable the notification rule",
            default=None
        )
        parser.add_argument(
            '--disable',
            action='store_false',
            help="Flag to disable the notification rule",
            dest='enable'
        )

    def get_rule_from_args(self, args):
        custom_headers = None
        if args.custom_header is not None:
            custom_headers = {}
            for header in args.custom_header:
                try:
                    name, value = header.split('=', 1)
                except ValueError:
                    name, value = header, ''
                custom_headers[name] = value

        rule = {
            'name': args.ruleName,
            'eventTypes': args.event_type,
            'isEnabled': args.enable,
            'objectNamePrefix': args.B2_URI.path,
            'targetConfiguration':
                {
                    'url': args.webhook_url,
                    'customHeaders': custom_headers,
                    'hmacSha256SigningSecret': args.sign_secret,
                },
        }
        return filter_out_empty_values(rule)

    def print_rule(self, args, rule):
        if args.json:
            self._print_json(rule)
        else:
            self._print_human_readable_structure(rule)


class BucketNotificationRuleUpdateBase(BucketNotificationRuleCreateBase):
    def _run(self, args):
        bucket = self.api.get_bucket_by_name(args.B2_URI.bucket_name)
        rules_by_name = {rule["name"]: rule for rule in bucket.get_notification_rules()}
        rule = rules_by_name.get(args.ruleName)
        if not rule:
            raise CommandError(
                f'rule with name {args.ruleName!r} does not exist on bucket {bucket.name!r}, '
                f'available rules: {sorted(rules_by_name)}'
            )

        rules_by_name[args.ruleName] = override_dict(
            rule,
            self.get_rule_from_args(args),
        )

        rules = bucket.set_notification_rules(
            [notification_rule_response_to_request(rule) for rule in rules_by_name.values()]
        )
        rule = next(rule for rule in rules if rule["name"] == args.ruleName)
        self.print_rule(args=args, rule=rule)
        return 0


@BucketNotificationRuleBase.subcommands_registry.register
class BucketNotificationRuleCreate(BucketNotificationRuleCreateBase):
    """
    Allows creating bucket notification rules for the given bucket.

    {BucketNotificationRuleWarningMixin}

    Examples:

    .. code-block::

        {NAME} notification-rule create b2://bucketName/optionalSubPath/ ruleName --event-type "b2:ObjectCreated:*" --webhook-url https://example.com/webhook


    Requires capability:

    - **readBucketNotifications**
    - **writeBucketNotifications**
    """
    COMMAND_NAME = 'create'

    NEW_RULE_DEFAULTS = {
        'isEnabled': True,
        'objectNamePrefix': '',
        'targetConfiguration': {
            'targetType': 'webhook',
        },
    }

    @classmethod
    def _setup_parser(cls, parser):
        cls.setup_rule_fields_parser(parser, creation=True)
        super()._setup_parser(parser)

    def _run(self, args):
        bucket = self.api.get_bucket_by_name(args.B2_URI.bucket_name)
        rules_by_name = {rule["name"]: rule for rule in bucket.get_notification_rules()}
        if args.ruleName in rules_by_name:
            raise CommandError(
                f'rule with name {args.ruleName!r} already exists on bucket {bucket.name!r}'
            )

        rule = override_dict(
            self.NEW_RULE_DEFAULTS,
            self.get_rule_from_args(args),
        )
        rules_by_name[args.ruleName] = rule

        rules = bucket.set_notification_rules(
            [
                notification_rule_response_to_request(rule)
                for rule in sorted(rules_by_name.values(), key=lambda r: r["name"])
            ]
        )
        rule = next(rule for rule in rules if rule["name"] == args.ruleName)
        self.print_rule(args=args, rule=rule)
        return 0


@BucketNotificationRuleBase.subcommands_registry.register
class BucketNotificationRuleUpdate(BucketNotificationRuleUpdateBase):
    """
    Allows updating notification rule of the given bucket.

    {BucketNotificationRuleWarningMixin}

    Examples:

    .. code-block::

        {NAME} notification-rule update b2://bucketName/newPath/ ruleName --disable --event-type "b2:ObjectCreated:*" --event-type "b2:ObjectHidden:*"
        {NAME} notification-rule update b2://bucketName/newPath/ ruleName --enable


    Requires capability:

    - **readBucketNotifications**
    - **writeBucketNotifications**
    """

    COMMAND_NAME = 'update'

    @classmethod
    def _setup_parser(cls, parser):
        cls.setup_rule_fields_parser(parser, creation=False)
        super()._setup_parser(parser)


@BucketNotificationRuleBase.subcommands_registry.register
class BucketNotificationRuleEnable(BucketNotificationRuleUpdateBase):
    """
    Allows enabling notification rule of the given bucket.

    {BucketNotificationRuleWarningMixin}

    Examples:

    .. code-block::

        {NAME} notification-rule enable b2://bucketName/ ruleName


    Requires capability:

    - **readBucketNotifications**
    - **writeBucketNotifications**
    """

    COMMAND_NAME = 'enable'

    @classmethod
    def _setup_parser(cls, parser):
        add_b2_uri_argument(
            parser, help="B2 URI of the bucket to enable the rule for, e.g. b2://bucketName"
        )
        parser.add_argument('ruleName', help="Name of the rule to enable")
        super()._setup_parser(parser)

    def get_rule_from_args(self, args):
        logger.warning("WARNING: ignoring path from %r", args.B2_URI)
        return {'name': args.ruleName, 'isEnabled': True}


@BucketNotificationRuleBase.subcommands_registry.register
class BucketNotificationRuleDisable(BucketNotificationRuleUpdateBase):
    """
    Allows disabling notification rule of the given bucket.

    {BucketNotificationRuleWarningMixin}

    Examples:

    .. code-block::

        {NAME} notification-rule disable b2://bucketName/ ruleName


    Requires capability:

    - **readBucketNotifications**
    - **writeBucketNotifications**
    """

    COMMAND_NAME = 'disable'

    @classmethod
    def _setup_parser(cls, parser):
        add_b2_uri_argument(
            parser, help="B2 URI of the bucket to enable the rule for, e.g. b2://bucketName"
        )
        parser.add_argument('ruleName', help="Name of the rule to enable")
        super()._setup_parser(parser)

    def get_rule_from_args(self, args):
        logger.warning("WARNING: ignoring path from %r", args.B2_URI)
        return {'name': args.ruleName, 'isEnabled': False}


@BucketNotificationRuleBase.subcommands_registry.register
class BucketNotificationRuleDelete(Command):
    """
    Allows deleting bucket notification rule of the given bucket.

    Requires capability:

    - **readBucketNotifications**
    - **writeBucketNotifications**
    """

    COMMAND_NAME = 'delete'

    @classmethod
    def _setup_parser(cls, parser):
        add_b2_uri_argument(
            parser, help="B2 URI of the bucket to delete the rule from, e.g. b2://bucketName"
        )
        parser.add_argument('ruleName', help="Name of the rule to delete")
        super()._setup_parser(parser)

    def _run(self, args):
        bucket = self.api.get_bucket_by_name(args.B2_URI.bucket_name)
        rules_by_name = {rule["name"]: rule for rule in bucket.get_notification_rules()}

        try:
            del rules_by_name[args.ruleName]
        except KeyError:
            raise CommandError(
                f'no such rule to delete: {args.ruleName!r}, '
                f'available rules: {sorted(rules_by_name.keys())!r}; No rules have been deleted.'
            )
        bucket.set_notification_rules(
            [notification_rule_response_to_request(rule) for rule in rules_by_name.values()]
        )
        self._print(f'Rule {args.ruleName!r} has been deleted from {args.B2_URI}')
        return 0


class Key(Command):
    """
    Application keys management subcommands.

    For more information on each subcommand, use ``{NAME} key SUBCOMMAND --help``.

    Examples:

    .. code-block::

        {NAME} key list
        {NAME} key create my-key listFiles,deleteFiles
        {NAME} key delete 005c398ac3212400000000010
    """
    subcommands_registry = ClassRegistry(attr_name='COMMAND_NAME')


@Key.subcommands_registry.register
class KeyListSubcommand(KeyListBase):
    __doc__ = KeyListBase.__doc__
    COMMAND_NAME = 'list'


@Key.subcommands_registry.register
class KeyCreateSubcommand(KeyCreateBase):
    __doc__ = KeyCreateBase.__doc__
    COMMAND_NAME = 'create'


@Key.subcommands_registry.register
class KeyDeleteSubcommand(KeyDeleteBase):
    __doc__ = KeyDeleteBase.__doc__
    COMMAND_NAME = 'delete'


class ListKeys(CmdReplacedByMixin, KeyListBase):
    __doc__ = KeyListBase.__doc__
    replaced_by_cmd = (Key, KeyListSubcommand)


class CreateKey(CmdReplacedByMixin, KeyCreateBase):
    __doc__ = KeyCreateBase.__doc__
    replaced_by_cmd = (Key, KeyCreateSubcommand)


class DeleteKey(CmdReplacedByMixin, KeyDeleteBase):
    __doc__ = KeyDeleteBase.__doc__
    replaced_by_cmd = (Key, KeyDeleteSubcommand)


class Replication(Command):
    """
    Replication rule management subcommands.

    For more information on each subcommand, use ``{NAME} replication SUBCOMMAND --help``.

    Examples:

    .. code-block::

        {NAME} replication setup --name=my-repl-rule src-bucket dest-bucket
        {NAME} replication status --rule=my-repl-rule src-bucket
        {NAME} replication pause src-bucket my-repl-rule
        {NAME} replication unpause src-bucket my-repl-rule
        {NAME} replication delete src-bucket my-repl-rule
    """
    subcommands_registry = ClassRegistry(attr_name='COMMAND_NAME')


@Replication.subcommands_registry.register
class ReplicationSetupSubcommand(ReplicationSetupBase):
    __doc__ = ReplicationSetupBase.__doc__
    COMMAND_NAME = 'setup'


@Replication.subcommands_registry.register
class ReplicationStatusSubcommand(ReplicationStatusBase):
    __doc__ = ReplicationStatusBase.__doc__
    COMMAND_NAME = 'status'


@Replication.subcommands_registry.register
class ReplicationPauseSubcommand(ReplicationPauseBase):
    __doc__ = ReplicationPauseBase.__doc__
    COMMAND_NAME = 'pause'


@Replication.subcommands_registry.register
class ReplicationUnpauseSubcommand(ReplicationUnpauseBase):
    __doc__ = ReplicationUnpauseBase.__doc__
    COMMAND_NAME = 'unpause'


@Replication.subcommands_registry.register
class ReplicationDeleteSubcommand(ReplicationDeleteBase):
    __doc__ = ReplicationDeleteBase.__doc__
    COMMAND_NAME = 'delete'


class ReplicationSetup(CmdReplacedByMixin, ReplicationSetupBase):
    __doc__ = ReplicationSetupBase.__doc__
    replaced_by_cmd = (Replication, ReplicationSetupSubcommand)


class ReplicationStatus(CmdReplacedByMixin, ReplicationStatusBase):
    __doc__ = ReplicationStatusBase.__doc__
    replaced_by_cmd = (Replication, ReplicationStatusSubcommand)


class ReplicationPause(CmdReplacedByMixin, ReplicationPauseBase):
    __doc__ = ReplicationPauseBase.__doc__
    replaced_by_cmd = (Replication, ReplicationPauseSubcommand)


class ReplicationUnpause(CmdReplacedByMixin, ReplicationUnpauseBase):
    __doc__ = ReplicationUnpauseBase.__doc__
    replaced_by_cmd = (Replication, ReplicationUnpauseSubcommand)


class ReplicationDelete(CmdReplacedByMixin, ReplicationDeleteBase):
    __doc__ = ReplicationDeleteBase.__doc__
    replaced_by_cmd = (Replication, ReplicationDeleteSubcommand)


class Account(Command):
    """
    Account management subcommands.

    For more information on each subcommand, use ``{NAME} account SUBCOMMAND --help``.

    Examples:

    .. code-block::

        {NAME} account authorize [applicationKeyId] [applicationKey]
        {NAME} account get
        {NAME} account clear
    """
    subcommands_registry = ClassRegistry(attr_name='COMMAND_NAME')


@Account.subcommands_registry.register
class AccountAuthorize(AccountAuthorizeBase):
    __doc__ = AccountAuthorizeBase.__doc__
    COMMAND_NAME = 'authorize'


@Account.subcommands_registry.register
class AccountGet(AccountGetBase):
    __doc__ = AccountGetBase.__doc__
    COMMAND_NAME = 'get'


@Account.subcommands_registry.register
class AccountClear(AccountClearBase):
    __doc__ = AccountClearBase.__doc__
    COMMAND_NAME = 'clear'


class AuthorizeAccount(CmdReplacedByMixin, AccountAuthorizeBase):
    __doc__ = AccountAuthorizeBase.__doc__
    replaced_by_cmd = (Account, AccountAuthorize)


class GetAccountInfo(CmdReplacedByMixin, AccountGetBase):
    __doc__ = AccountGetBase.__doc__
    replaced_by_cmd = (Account, AccountGet)


class ClearAccount(CmdReplacedByMixin, AccountClearBase):
    __doc__ = AccountClearBase.__doc__
    replaced_by_cmd = (Account, AccountClear)


class BucketCmd(Command):
    """
    Bucket management subcommands.

    For more information on each subcommand, use ``{NAME} bucket SUBCOMMAND --help``.

    Examples:

    .. code-block::

        {NAME} bucket list
        {NAME} bucket get
        {NAME} bucket create
        {NAME} bucket update
        {NAME} bucket delete
        {NAME} bucket get-download-auth
    """
    # to avoid conflicts with the Bucket class this class is named BucketCmd
    COMMAND_NAME = "bucket"

    subcommands_registry = ClassRegistry(attr_name='COMMAND_NAME')


@BucketCmd.subcommands_registry.register
class BucketList(BucketListBase):
    __doc__ = BucketListBase.__doc__
    COMMAND_NAME = 'list'


@BucketCmd.subcommands_registry.register
class BucketGet(BucketGetBase):
    __doc__ = BucketGetBase.__doc__
    COMMAND_NAME = 'get'


@BucketCmd.subcommands_registry.register
class BucketCreate(BucketCreateBase):
    __doc__ = BucketCreateBase.__doc__
    COMMAND_NAME = 'create'


@BucketCmd.subcommands_registry.register
class BucketUpdate(BucketUpdateBase):
    __doc__ = BucketUpdateBase.__doc__
    COMMAND_NAME = 'update'


@BucketCmd.subcommands_registry.register
class BucketDelete(BucketDeleteBase):
    __doc__ = BucketDeleteBase.__doc__
    COMMAND_NAME = 'delete'


@BucketCmd.subcommands_registry.register
class BucketGetDownloadAuth(BucketGetDownloadAuthBase):
    __doc__ = BucketGetDownloadAuthBase.__doc__
    COMMAND_NAME = 'get-download-auth'


@BucketCmd.subcommands_registry.register
class BucketNotificationRule(BucketNotificationRuleBase):
    __doc__ = BucketNotificationRuleBase.__doc__
    COMMAND_NAME = 'notification-rule'


class ListBuckets(CmdReplacedByMixin, BucketListBase):
    __doc__ = BucketListBase.__doc__
    replaced_by_cmd = (BucketCmd, BucketList)


class GetBucket(CmdReplacedByMixin, BucketGetBase):
    __doc__ = BucketGetBase.__doc__
    replaced_by_cmd = (BucketCmd, BucketGet)


class CreateBucket(CmdReplacedByMixin, BucketCreateBase):
    __doc__ = BucketCreateBase.__doc__
    replaced_by_cmd = (BucketCmd, BucketCreate)


class UpdateBucket(CmdReplacedByMixin, BucketUpdateBase):
    __doc__ = BucketUpdateBase.__doc__
    replaced_by_cmd = (BucketCmd, BucketUpdate)


class DeleteBucket(CmdReplacedByMixin, BucketDeleteBase):
    __doc__ = BucketDeleteBase.__doc__
    replaced_by_cmd = (BucketCmd, BucketDelete)


class GetDownloadAuth(CmdReplacedByMixin, BucketGetDownloadAuthBase):
    __doc__ = BucketGetDownloadAuthBase.__doc__
    replaced_by_cmd = (BucketCmd, BucketGetDownloadAuth)


class NotificationRules(CmdReplacedByMixin, BucketNotificationRuleBase):
    __doc__ = BucketNotificationRuleBase.__doc__
    replaced_by_cmd = (BucketCmd, BucketNotificationRule)


class File(Command):
    """
    File management subcommands.

    For more information on each subcommand, use ``{NAME} file SUBCOMMAND --help``.

    Examples:

    .. code-block::

        {NAME} file cat b2://yourBucket/file.txt
        {NAME} file copy-by-id sourceFileId yourBucket file.txt
        {NAME} file download b2://yourBucket/file.txt localFile.txt
        {NAME} file hide yourBucket file.txt
        {NAME} file info b2://yourBucket/file.txt
        {NAME} file update --legal-hold off b2://yourBucket/file.txt
        {NAME} file upload yourBucket localFile.txt file.txt
        {NAME} file url b2://yourBucket/file.txt
    """
    subcommands_registry = ClassRegistry(attr_name='COMMAND_NAME')


@File.subcommands_registry.register
class FileInfo(B2URIFileArgMixin, FileInfoBase):
    __doc__ = FileInfoBase.__doc__
    COMMAND_NAME = 'info'


@File.subcommands_registry.register
class FileUrl(B2URIFileArgMixin, FileUrlBase):
    __doc__ = FileUrlBase.__doc__
    COMMAND_NAME = 'url'


@File.subcommands_registry.register
class FileCat(FileCatBase):
    __doc__ = FileCatBase.__doc__
    COMMAND_NAME = 'cat'


@File.subcommands_registry.register
class FileUpload(FileUploadBase):
    __doc__ = FileUploadBase.__doc__
    COMMAND_NAME = 'upload'


@File.subcommands_registry.register
class FileDownload(B2URIFileArgMixin, FileDownloadBase):
    __doc__ = FileDownloadBase.__doc__
    COMMAND_NAME = 'download'


@File.subcommands_registry.register
class FileCopyById(FileCopyByIdBase):
    __doc__ = FileCopyByIdBase.__doc__
    COMMAND_NAME = 'copy-by-id'


@File.subcommands_registry.register
class FileHide(B2URIFileOrBucketNameFileNameArgMixin, FileHideBase):
    __doc__ = FileHideBase.__doc__
    COMMAND_NAME = 'hide'


@File.subcommands_registry.register
class FileUnhide(B2URIFileArgMixin, FileUnhideBase):
    __doc__ = FileUnhideBase.__doc__
    COMMAND_NAME = 'unhide'


@File.subcommands_registry.register
class FileUpdate(FileUpdateBase):
    __doc__ = FileUpdateBase.__doc__
    COMMAND_NAME = 'update'


class FileInfo2(CmdReplacedByMixin, B2URIFileArgMixin, FileInfoBase):
    __doc__ = FileInfoBase.__doc__
    replaced_by_cmd = (File, FileInfo)
    COMMAND_NAME = 'file-info'


class GetFileInfo(CmdReplacedByMixin, B2URIFileIDArgMixin, FileInfoBase):
    __doc__ = FileInfoBase.__doc__
    replaced_by_cmd = (File, FileInfo)


class GetUrl(CmdReplacedByMixin, B2URIFileArgMixin, FileUrlBase):
    __doc__ = FileUrlBase.__doc__
    replaced_by_cmd = (File, FileUrl)


class MakeUrl(CmdReplacedByMixin, B2URIFileIDArgMixin, FileUrlBase):
    __doc__ = FileUrlBase.__doc__
    replaced_by_cmd = (File, FileUrl)


class MakeFriendlyUrl(CmdReplacedByMixin, B2URIBucketNFilenameArgMixin, FileUrlBase):
    __doc__ = FileUrlBase.__doc__
    replaced_by_cmd = (File, FileUrl)


class Cat(CmdReplacedByMixin, FileCatBase):
    __doc__ = FileCatBase.__doc__
    replaced_by_cmd = (File, FileCat)


class UploadFile(CmdReplacedByMixin, FileUploadBase):
    __doc__ = FileUploadBase.__doc__
    replaced_by_cmd = (File, FileUpload)


class UploadUnboundStream(CmdReplacedByMixin, UploadUnboundStreamBase):
    __doc__ = UploadUnboundStreamBase.__doc__
    replaced_by_cmd = (File, FileUpload)


class DownloadFile(CmdReplacedByMixin, B2URIFileArgMixin, FileDownloadBase):
    __doc__ = FileDownloadBase.__doc__
    replaced_by_cmd = (File, FileDownload)


class DownloadFileById(CmdReplacedByMixin, B2URIFileIDArgMixin, FileDownloadBase):
    __doc__ = FileDownloadBase.__doc__
    replaced_by_cmd = (File, FileDownload)


class DownloadFileByName(CmdReplacedByMixin, B2URIBucketNFilenameArgMixin, FileDownloadBase):
    __doc__ = FileDownloadBase.__doc__
    replaced_by_cmd = (File, FileDownload)


class CopyFileById(CmdReplacedByMixin, FileCopyByIdBase):
    __doc__ = FileCopyByIdBase.__doc__
    replaced_by_cmd = (File, FileCopyById)


class HideFile(CmdReplacedByMixin, HideFileBase):
    __doc__ = FileHideBase.__doc__
    replaced_by_cmd = (File, FileHide)


class UpdateFileLegalHold(CmdReplacedByMixin, UpdateFileLegalHoldBase):
    __doc__ = UpdateFileLegalHoldBase.__doc__
    replaced_by_cmd = (File, FileUpdate)


class UpdateFileRetention(CmdReplacedByMixin, UpdateFileRetentionBase):
    __doc__ = UpdateFileRetentionBase.__doc__
    replaced_by_cmd = (File, FileUpdate)


class GetDownloadUrlWithAuth(CmdReplacedByMixin, GetDownloadUrlWithAuthBase):
    __doc__ = GetDownloadUrlWithAuthBase.__doc__
    replaced_by_cmd = (File, FileUrl)


class DeleteFileVersion(CmdReplacedByMixin, DeleteFileVersionBase):
    __doc__ = DeleteFileVersionBase.__doc__
    replaced_by_cmd = Rm


@File.subcommands_registry.register
class FileLarge(Command):
    """
    Large file uploads management subcommands.

    For more information on each subcommand, use ``{NAME} file large SUBCOMMAND --help``.

    Examples:

    .. code-block::

        {NAME} file large parts b2id://yourFileId
        {NAME} file large unfinished list b2://yourBucket
        {NAME} file large unfinished cancel b2://yourBucket
        {NAME} file large unfinished cancel b2id://yourFileId
    """
    COMMAND_NAME = 'large'
    subcommands_registry = ClassRegistry(attr_name='COMMAND_NAME')


@FileLarge.subcommands_registry.register
class FileLargeParts(B2IDURIMixin, FileLargePartsBase):
    __doc__ = FileLargePartsBase.__doc__
    COMMAND_NAME = 'parts'


@FileLarge.subcommands_registry.register
class FileLargeUnfinished(Command):
    """
    Large file unfinished uploads management subcommands.

    For more information on each subcommand, use ``{NAME} file large unfinished SUBCOMMAND --help``.

    Examples:

    .. code-block::

        {NAME} file large unfinished list b2://yourBucket
        {NAME} file large unfinished cancel b2://yourBucket
        {NAME} file large unfinished cancel b2id://yourFileId
    """
    COMMAND_NAME = 'unfinished'
    subcommands_registry = ClassRegistry(attr_name='COMMAND_NAME')


@FileLargeUnfinished.subcommands_registry.register
class FileLargeUnfinishedList(B2BucketURIMixin, FileLargeUnfinishedListBase):
    __doc__ = FileLargePartsBase.__doc__
    COMMAND_NAME = 'list'


@FileLargeUnfinished.subcommands_registry.register
class FileLargeUnfinishedCancel(B2IDOrB2BucketURIMixin, FileLargeUnfinishedCancelBase):
    __doc__ = FileLargeUnfinishedCancelBase.__doc__
    COMMAND_NAME = 'cancel'


class ListParts(CmdReplacedByMixin, B2URIFileIDArgMixin, FileLargePartsBase):
    __doc__ = FileLargePartsBase.__doc__
    replaced_by_cmd = (File, FileLarge, FileLargeParts)


class ListUnfinishedLargeFiles(
    CmdReplacedByMixin, B2URIBucketArgMixin, FileLargeUnfinishedListBase
):
    __doc__ = FileLargeUnfinishedListBase.__doc__
    replaced_by_cmd = (File, FileLarge, FileLargeUnfinished, FileLargeUnfinishedList)


class CancelAllUnfinishedLargeFiles(
    CmdReplacedByMixin, B2URIBucketArgMixin, FileLargeUnfinishedCancelBase
):
    """
    Lists all large files that have been started but not
    finished and cancels them.  Any parts that have been
    uploaded will be deleted.

    Requires capability:

    - **listFiles**
    - **writeFiles**
    """
    replaced_by_cmd = (File, FileLarge, FileLargeUnfinished, FileLargeUnfinishedCancel)


class CancelLargeFile(CmdReplacedByMixin, B2URIFileIDArgMixin, FileLargeUnfinishedCancelBase):
    """
    Cancels a large file upload.  Used to undo a ``start-large-file``.

    Cannot be used once the file is finished.  After finishing,
    using ``delete-file-version`` to delete the large file.

    Requires capability:

    - **writeFiles**
    """
    replaced_by_cmd = (File, FileLarge, FileLargeUnfinished, FileLargeUnfinishedCancel)


class ConsoleTool:
    """
    Implements the commands available in the B2 command-line tool
    using the B2Api library.

    Uses a ``b2sdk.SqlitedAccountInfo`` object to keep account data between runs
    (unless authorization is performed via environment variables).
    """

    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr
        self.b2_binary_name = 'b2'

    def _get_default_escape_cc_setting(self):
        escape_cc_env_var = os.environ.get(B2_ESCAPE_CONTROL_CHARACTERS, None)
        if escape_cc_env_var is not None:
            if int(escape_cc_env_var) in (0, 1):
                return int(escape_cc_env_var) == 1
            else:
                logger.warning(
                    "WARNING: invalid value for {B2_ESCAPE_CONTROL_CHARACTERS} environment variable, available options are 0 or 1 - will assume variable is not set"
                )
        return self.stdout.isatty()

    def run_command(self, argv):
        signal.signal(signal.SIGINT, keyboard_interrupt_handler)
        self.b2_binary_name = resolve_b2_bin_call_name(argv)
        parser = B2.create_parser(name=self.b2_binary_name, b2_binary_name=self.b2_binary_name)
        AUTOCOMPLETE.cache_and_autocomplete(parser)
        args = parser.parse_args(argv[1:])
        self._setup_logging(args, argv)

        if args.escape_control_characters is None:
            args.escape_control_characters = self._get_default_escape_cc_setting()

        if args.escape_control_characters:
            # in case any control characters slip through escaping, just delete them
            self.stdout = NoControlCharactersStdout(self.stdout)
            self.stderr = NoControlCharactersStdout(self.stderr)

        kwargs = {}
        with suppress(AttributeError):
            kwargs['save_to_buffer_size'] = args.write_buffer_size
        with suppress(AttributeError):
            kwargs['check_download_hash'] = not args.skip_hash_verification
        with suppress(AttributeError):
            kwargs['max_download_streams_per_file'] = args.max_download_streams_per_file

        self.api = self._initialize_b2_api(args=args, kwargs=kwargs)

        b2_command = B2(self)
        command_class = b2_command.run(args)
        command = command_class(self)

        if command.FORBID_LOGGING_ARGUMENTS:
            logger.info('starting command [%s] (arguments hidden)', command)
        else:
            logger.info('starting command [%s] with arguments: %s', command, argv)

        try:
            if command_class.REQUIRES_AUTH:
                auth_ret = self.authorize_from_env()
                if auth_ret:
                    return auth_ret
            return command.run(args)
        except MissingAccountData as e:
            logger.exception('ConsoleTool missing account data error')
            self._print_stderr(
                f'ERROR: {e}  Use: \'{self.b2_binary_name} account authorize\' or provide auth data with '
                f'{B2_APPLICATION_KEY_ID_ENV_VAR!r} and {B2_APPLICATION_KEY_ENV_VAR!r} environment variables'
            )
            return 1
        except B2Error as e:
            logger.exception('ConsoleTool command error')
            self._print_stderr(f'ERROR: {e}')
            return 1
        except KeyboardInterrupt:
            logger.exception('ConsoleTool command interrupt')
            self._print_stderr('\nInterrupted.  Shutting down...\n')
            return 1
        except Exception:
            logger.exception('ConsoleTool unexpected exception')
            raise

    @classmethod
    def _initialize_b2_api(cls, args: argparse.Namespace, kwargs: dict) -> B2Api:
        b2_api = None
        key_id, key = get_keyid_and_key_from_env_vars()
        if key_id and key:
            try:
                # here we initialize regular b2 api on disk and check whether it matches
                # the keys from env vars; if they indeed match then there's no need to
                # initialize in-memory account info cause it's already stored on disk
                b2_api = _get_b2api_for_profile(
                    profile=args.profile, raise_if_does_not_exist=True, **kwargs
                )
                realm = os.environ.get(B2_ENVIRONMENT_ENV_VAR) or 'production'
                is_same_key_on_disk = b2_api.account_info.is_same_key(key_id, realm)
            except MissingAccountData:
                is_same_key_on_disk = False

            if not is_same_key_on_disk and not issubclass(
                args.command_class, (AccountAuthorizeBase, AccountClearBase)
            ):
                # when user specifies keys via env variables, we switch to in-memory account info
                return _get_inmemory_b2api(**kwargs)

        return b2_api or _get_b2api_for_profile(profile=args.profile, **kwargs)

    def authorize_from_env(self) -> int:

        key_id, key = get_keyid_and_key_from_env_vars()

        if key_id is None and key is None:
            return 0

        if (key_id is None) or (key is None):
            self._print_stderr(
                f'Please provide both "{B2_APPLICATION_KEY_ENV_VAR}" and "{B2_APPLICATION_KEY_ID_ENV_VAR}" environment variables or none of them'
            )
            return 1
        realm = os.environ.get(B2_ENVIRONMENT_ENV_VAR)

        if self.api.account_info.is_same_key(key_id, realm or 'production'):
            return 0

        logger.info('`account authorize` is being run from env variables')
        return AccountAuthorizeBase(self).authorize(key_id, key, realm)

    def _print(self, *args, **kwargs):
        print(*args, file=self.stdout, **kwargs)

    def _print_stderr(self, *args, **kwargs):
        print(*args, file=self.stderr, **kwargs)

    @classmethod
    def _setup_logging(cls, args, argv):
        if args.log_config and (args.verbose or args.debug_logs):
            raise ValueError('Please provide either --log-config or --verbose/--debug-logs')
        errors_kwarg = {'errors': 'backslashreplace'} if sys.version_info >= (3, 9) else {}
        if args.log_config:
            logging.config.fileConfig(args.log_config)
        elif args.verbose or args.debug_logs:
            # set log level to DEBUG for ALL loggers (even those not belonging to B2), but without any handlers,
            # those will added as needed (file and/or stderr)
            logging.basicConfig(level=logging.DEBUG, handlers=[], **errors_kwarg)
        else:
            logger.setLevel(logging.CRITICAL + 1)  # No logs!
        if args.verbose:
            formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)

            # logs from ALL loggers sent to stderr should be formatted this way
            logging.root.addHandler(handler)
        if args.debug_logs:
            formatter = logging.Formatter(
                '%(asctime)s\t%(process)d\t%(thread)d\t%(name)s\t%(levelname)s\t%(message)s'
            )
            formatter.converter = time.gmtime
            handler = logging.FileHandler('b2_cli.log', **errors_kwarg)
            handler.setFormatter(formatter)

            # logs from ALL loggers sent to the log file should be formatted this way
            logging.root.addHandler(handler)
        if not args.debug_logs and not args.verbose:
            warnings.showwarning = lambda message, category, *arg_, **_: print(
                f'{category.__name__}: {message}', file=sys.stderr
            )

        logger.info(r'// %s %s %s \\', SEPARATOR, VERSION.center(8), SEPARATOR)
        logger.debug('platform is %s', platform.platform())
        if os.environ.get(B2_CLI_DOCKER_ENV_VAR) == "1":
            logger.debug('running as a Docker container')
        logger.debug(
            'Python version is %s %s', platform.python_implementation(),
            sys.version.replace('\n', ' ')
        )
        logger.debug('b2sdk version is %s', b2sdk_version)
        logger.debug('locale is %s', locale.getlocale())
        logger.debug('filesystem encoding is %s', sys.getfilesystemencoding())
        logger.debug('default encoding is %s', sys.getdefaultencoding())
        logger.debug('flags.utf8_mode is %s', sys.flags.utf8_mode)


# used by Sphinx
get_parser = functools.partial(B2.create_parser, for_docs=True)


def main():
    ct = ConsoleTool(stdout=sys.stdout, stderr=sys.stderr)
    exit_status = ct.run_command(sys.argv)
    logger.info('\\\\ %s %s %s //', SEPARATOR, ('exit=%s' % exit_status).center(8), SEPARATOR)

    # I haven't tracked down the root cause yet, but in Python 2.7, the futures
    # packages is hanging on exit sometimes, waiting for a thread to finish.
    # This happens when using sync to upload files.
    sys.stdout.flush()
    sys.stderr.flush()

    logging.shutdown()

    os._exit(exit_status)


if __name__ == '__main__':
    main()
