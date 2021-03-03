######################################################################
#
# File: b2/console_tool.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import argparse
import datetime
import functools
import getpass
import json
import locale
import logging
import logging.config
import os
import platform
import signal
import sys
import time

from class_registry import ClassRegistry

from b2sdk.account_info.sqlite_account_info import (
    B2_ACCOUNT_INFO_ENV_VAR, B2_ACCOUNT_INFO_DEFAULT_FILE
)
from b2sdk.progress import make_progress_listener
from b2sdk.raw_api import MetadataDirectiveMode, SRC_LAST_MODIFIED_MILLIS
from b2sdk.version import VERSION as b2sdk_version
from b2sdk.v1 import (
    parse_sync_folder,
    AuthInfoCache,
    B2Api,
    B2Http,
    B2RawApi,
    Synchronizer,
    SyncReport,
    NewerFileSyncMode,
    CompareVersionMode,
    KeepOrDeleteMode,
    DownloadDestLocalFile,
    FileVersionInfo,
    SqliteAccountInfo,
    ScanPoliciesManager,
    DEFAULT_SCAN_MANAGER,
)
from b2sdk.v1.exception import B2Error, BadFileInfo, MissingAccountData
from b2.arg_parser import ArgumentParser, parse_comma_separated_list, \
    parse_millis_from_float_timestamp, parse_range
from b2.json_encoder import B2CliJsonEncoder
from b2.version import VERSION

logger = logging.getLogger(__name__)

SEPARATOR = '=' * 40

# Optional Env variable to use for getting account info while authorizing
B2_APPLICATION_KEY_ID_ENV_VAR = 'B2_APPLICATION_KEY_ID'
B2_APPLICATION_KEY_ENV_VAR = 'B2_APPLICATION_KEY'
# Optional Env variable to use for adding custom string to the User Agent
B2_USER_AGENT_APPEND_ENV_VAR = 'B2_USER_AGENT_APPEND'
B2_ENVIRONMENT_ENV_VAR = 'B2_ENVIRONMENT'

# Enable to get 0.* behavior in the command-line tool.
# Disable for 1.* behavior.
VERSION_0_COMPATIBILITY = False

# The name of an executable entry point
NAME = os.path.basename(sys.argv[0])
if NAME.endswith('.py'):
    NAME = 'b2'

# Strings available to use when formatting doc strings.
DOC_STRING_DATA = dict(
    NAME=NAME,
    B2_ACCOUNT_INFO_ENV_VAR=B2_ACCOUNT_INFO_ENV_VAR,
    B2_ACCOUNT_INFO_DEFAULT_FILE=B2_ACCOUNT_INFO_DEFAULT_FILE,
    B2_APPLICATION_KEY_ID_ENV_VAR=B2_APPLICATION_KEY_ID_ENV_VAR,
    B2_APPLICATION_KEY_ENV_VAR=B2_APPLICATION_KEY_ENV_VAR,
    B2_USER_AGENT_APPEND_ENV_VAR=B2_USER_AGENT_APPEND_ENV_VAR,
    B2_ENVIRONMENT_ENV_VAR=B2_ENVIRONMENT_ENV_VAR,
)


def current_time_millis():
    """
    File times are in integer milliseconds, to avoid roundoff errors.
    """
    return int(round(time.time() * 1000))


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


class Command(object):
    # Set to True for commands that receive sensitive information in arguments
    FORBID_LOGGING_ARGUMENTS = False

    # The registry for the subcommands, should be reinitialized  in subclass
    subcommands_registry = None

    # set to False for commands not requiring b2 authentication
    REQUIRES_AUTH = True

    def __init__(self, console_tool):
        self.console_tool = console_tool
        self.api = console_tool.api
        self.stdout = console_tool.stdout
        self.stderr = console_tool.stderr

    @classmethod
    def name_and_alias(cls):
        name = mixed_case_to_hyphens(cls.__name__)
        alias = None
        if '-' in name:
            alias = name.replace('-', '_')
        return name, alias

    @classmethod
    def register_subcommand(cls, command_class):
        assert cls.subcommands_registry is not None, 'Initialize the registry class'
        name, alias = command_class.name_and_alias()
        decorator = cls.subcommands_registry.register(key=name)(command_class)
        # Register alias if present
        if alias is not None:
            cls.subcommands_registry[alias] = command_class
        return decorator

    @classmethod
    def get_parser(cls, subparsers=None, parents=None, for_docs=False):
        if parents is None:
            parents = []

        description = cls.__doc__.format(**DOC_STRING_DATA)

        if subparsers is None:
            name, _ = cls.name_and_alias()
            parser = ArgumentParser(
                prog=name,
                description=description,
                parents=parents,
                for_docs=for_docs,
            )
        else:
            name, alias = cls.name_and_alias()
            parser = subparsers.add_parser(
                name,
                description=description,
                parents=parents,
                aliases=[alias] if alias is not None and not for_docs else (),
                for_docs=for_docs,
            )

        cls._setup_parser(parser)

        if cls.subcommands_registry:
            if not parents:
                common_parser = ArgumentParser(add_help=False)
                common_parser.add_argument(
                    '--debugLogs', action='store_true', help=argparse.SUPPRESS
                )
                common_parser.add_argument('--verbose', action='store_true', help=argparse.SUPPRESS)
                common_parser.add_argument('--logConfig', help=argparse.SUPPRESS)
                parents = [common_parser]

            subparsers = parser.add_subparsers(prog=parser.prog, title='usages', dest='command')
            subparsers.required = True
            for subcommand in cls.subcommands_registry.values():
                subcommand.get_parser(subparsers=subparsers, parents=parents, for_docs=for_docs)

        return parser

    def run(self, args):
        pass

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

    def _print(self, *args):
        self._print_helper(self.stdout, self.stdout.encoding, 'stdout', *args)

    def _print_json(self, data):
        self._print(json.dumps(data, indent=4, sort_keys=True, cls=B2CliJsonEncoder))

    def _print_stderr(self, *args, **kwargs):
        self._print_helper(self.stderr, self.stderr.encoding, 'stderr', *args)

    def _print_helper(self, descriptor, descriptor_encoding, descriptor_name, *args):
        try:
            descriptor.write(' '.join(args))
        except UnicodeEncodeError:
            sys.stderr.write(
                "\nWARNING: Unable to print unicode.  Encoding for %s is: '%s'\n" % (
                    descriptor_name,
                    descriptor_encoding,
                )
            )
            sys.stderr.write("Trying to print: %s\n" % (repr(args),))
            args = [arg.encode('ascii', 'backslashreplace').decode() for arg in args]
            descriptor.write(' '.join(args))
        descriptor.write('\n')

    def __str__(self):
        return '%s.%s' % (self.__class__.__module__, self.__class__.__name__)


class B2(Command):
    """
    This program provides command-line access to the B2 service.

    There are two flows of authorization:

    * call ``{NAME}`` authorize-account and have the credentials cached in sqlite
    * set ``{B2_APPLICATION_KEY_ID_ENV_VAR}`` and ``{B2_APPLICATION_KEY_ENV_VAR}`` environment
      variables when running this program

    The environment variable ``{B2_ACCOUNT_INFO_ENV_VAR}`` specifies the sqlite
    file to use for caching authentication information.
    The default file to use is: ``{B2_ACCOUNT_INFO_DEFAULT_FILE}``

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
        return NAME, None

    def run(self, args):
        return self.subcommands_registry.get_class(args.command)


@B2.register_subcommand
class AuthorizeAccount(Command):
    """
    Prompts for Backblaze ``applicationKeyId`` and ``applicationKey`` (unless they are given
    on the command line).

    You can authorize with either the master application key or
    a normal application key.

    To use the master application key, provide the application key ID and
    application key from the ``B2 Cloud Storage Buckets`` page on
    the web site: https://secure.backblaze.com/b2_buckets.htm

    To use a normal application key, created with the ``create-key``
    command or on the web site, provide the application key ID
    and the application key itself.

    You can also optionally provide application key ID and application key
    using environment variables ``{B2_APPLICATION_KEY_ID_ENV_VAR}`` and
    ``{B2_APPLICATION_KEY_ENV_VAR}`` respectively.

    Stores an account auth token in ``{B2_ACCOUNT_INFO_DEFAULT_FILE}`` by default,
    or the file specified by the ``{B2_ACCOUNT_INFO_ENV_VAR}`` environment variable.

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

    def run(self, args):
        # Handle internal options for testing inside Backblaze.
        # These are not documented in the usage string.
        realm = self._get_realm(args)

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

        return self.authorize(args.applicationKeyId, args.applicationKey, realm)

    def authorize(self, application_key_id, application_key, realm):
        """
        Perform the authorization and capability checks, report errors.

        :param application_key_id: application key ID used to authenticate
        :param application_key: application key
        :param realm: authorization realm
        :return: exit status
        """
        url = self.api.account_info.REALM_URLS.get(realm, realm)
        self._print('Using %s' % url)
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
                    "ERROR: application key is restricted to bucket id '%s', which no longer exists"
                    % (allowed['bucketId'],)
                )
                self.api.account_info.clear()
                return 1
            return 0
        except B2Error as e:
            logger.exception('ConsoleTool account authorization error')
            self._print_stderr('ERROR: unable to authorize account: ' + str(e))
            return 1

    @classmethod
    def _get_realm(cls, args):
        if args.dev:
            return 'dev'
        if args.staging:
            return 'staging'
        if args.environment:
            return args.environment

        return os.environ.get(B2_ENVIRONMENT_ENV_VAR, 'production')


@B2.register_subcommand
class CancelAllUnfinishedLargeFiles(Command):
    """
    Lists all large files that have been started but not
    finished and cancels them.  Any parts that have been
    uploaded will be deleted.

    Requires capability:

    - **listFiles**
    - **writeFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('bucketName')

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        for file_version in bucket.list_unfinished_large_files():
            bucket.cancel_large_file(file_version.file_id)
            self._print(file_version.file_id, 'canceled')
        return 0


@B2.register_subcommand
class CancelLargeFile(Command):
    """
    Cancels a large file upload.  Used to undo a ``start-large-file``.

    Cannot be used once the file is finished.  After finishing,
    using ``delete-file-version`` to delete the large file.

    Requires capability:

    - **writeFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('fileId')

    def run(self, args):
        self.api.cancel_large_file(args.fileId)
        self._print(args.fileId, 'canceled')
        return 0


@B2.register_subcommand
class ClearAccount(Command):
    """
    Erases everything in ``{B2_ACCOUNT_INFO_DEFAULT_FILE}``.  Location
    of file can be overridden by setting ``{B2_ACCOUNT_INFO_ENV_VAR}``.
    """

    REQUIRES_AUTH = False

    def run(self, args):
        self.api.account_info.clear()
        return 0


@B2.register_subcommand
class CopyFileById(Command):
    """
    Copy a file version to the given bucket (server-side, **not** via download+upload).
    Copies the contents of the source B2 file to destination bucket
    and assigns the given name to the new B2 file.

    By default, it copies the file info and content type. You can replace those
    by setting the ``metadataDirective`` to ``replace``.

    ``--contentType`` and ``--info`` should only be provided when ``--metadataDirective``
    is set to ``replace`` and should not be provided when ``--metadataDirective``
    is set to ``copy``.

    ``--contentType`` and ``--info`` are optional.  If not set, they will be set based on the
    source file.

    By default, the whole file gets copied, but you can copy an (inclusive!) range of bytes
    from the source file to the new file using ``--range`` option.

    Each ``--info`` entry is in the form ``a=b``, you can specify many.

    The maximum file size is 5GB or 10TB, depending on capability of installed ``b2sdk`` version.

    Requires capability:

    - **readFiles** (if ``sourceFileId`` bucket is private)
    - **writeFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--metadataDirective', choices=('copy', 'replace'))
        parser.add_argument('--contentType')
        parser.add_argument('--range', type=parse_range)
        parser.add_argument('--info', action='append', default=[])
        parser.add_argument('sourceFileId')
        parser.add_argument('destinationBucketName')
        parser.add_argument('b2FileName')

    def run(self, args):
        file_infos = None
        if args.info is not None:
            file_infos = self._parse_file_infos(args.info)

        if args.metadataDirective == 'copy':
            metadata_directive = MetadataDirectiveMode.COPY
        elif args.metadataDirective == 'replace':
            metadata_directive = MetadataDirectiveMode.REPLACE
        else:
            metadata_directive = None

        bucket = self.api.get_bucket_by_name(args.destinationBucketName)

        response = bucket.copy_file(
            args.sourceFileId,
            args.b2FileName,
            bytes_range=args.range,
            metadata_directive=metadata_directive,
            content_type=args.contentType,
            file_info=file_infos,
        )
        self._print_json(response)
        return 0


@B2.register_subcommand
class CreateBucket(Command):
    """
    Creates a new bucket.  Prints the ID of the bucket created.

    Optionally stores bucket info, CORS rules and lifecycle rules with the bucket.
    These can be given as JSON on the command line.

    Requires capability:

    - **writeBuckets**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--bucketInfo', type=json.loads)
        parser.add_argument('--corsRules', type=json.loads)
        parser.add_argument('--lifecycleRules', type=json.loads)
        parser.add_argument('bucketName')
        parser.add_argument('bucketType')

    def run(self, args):
        bucket = self.api.create_bucket(
            args.bucketName,
            args.bucketType,
            bucket_info=args.bucketInfo,
            cors_rules=args.corsRules,
            lifecycle_rules=args.lifecycleRules
        )
        self._print(bucket.id_)
        return 0


@B2.register_subcommand
class CreateKey(Command):
    """
    Creates a new application key.  Prints the application key information.  This is the only
    time the application key itself will be returned.  Listing application keys will show
    their IDs, but not the secret keys.

    The capabilities are passed in as a comma-separated list, like ``readFiles,writeFiles``.

    The ``duration`` is the length of time the new application key will exist.
    When the time expires the key will disappear and will no longer be usable.  If not
    specified, the key will not expire.

    The ``bucket`` is the name of a bucket in the account.  When specified, the key
    will only allow access to that bucket.

    The ``namePrefix`` restricts file access to files whose names start with the prefix.

    The output is the new application key ID, followed by the application key itself.
    The two values returned are the two that you pass to ``authorize-account`` to use the key.

    Requires capability:

    - **writeKeys**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--bucket')
        parser.add_argument('--namePrefix')
        parser.add_argument('--duration', type=int)
        parser.add_argument('keyName')
        parser.add_argument('capabilities', type=parse_comma_separated_list)

    def run(self, args):
        # Translate the bucket name into a bucketId
        if args.bucket is None:
            bucket_id_or_none = None
        else:
            bucket_id_or_none = self.api.get_bucket_by_name(args.bucket).id_

        response = self.api.create_key(
            capabilities=args.capabilities,
            key_name=args.keyName,
            valid_duration_seconds=args.duration,
            bucket_id=bucket_id_or_none,
            name_prefix=args.namePrefix
        )

        application_key_id = response['applicationKeyId']
        application_key = response['applicationKey']
        self._print(application_key_id + " " + application_key)
        return 0


@B2.register_subcommand
class DeleteBucket(Command):
    """
    Deletes the bucket with the given name.

    Requires capability:

    - **deleteBuckets**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('bucketName')

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        self.api.delete_bucket(bucket)
        return 0


@B2.register_subcommand
class DeleteFileVersion(Command):
    """
    Permanently and irrevocably deletes one version of a file.

    Specifying the ``fileName`` is more efficient than leaving it out.
    If you omit the ``fileName``, it requires an initial query to B2
    to get the file name, before making the call to delete the
    file.  This extra query requires the ``readFiles`` capability.

    Requires capability:

    - **deleteFiles**
    - **readFiles** (if file name not provided)
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('fileName', nargs='?')
        parser.add_argument('fileId')

    def run(self, args):
        if args.fileName is not None:
            file_name = args.fileName
        else:
            file_name = self._get_file_name_from_file_id(args.fileId)

        file_info = self.api.delete_file_version(args.fileId, file_name)
        self._print_json(file_info)
        return 0

    def _get_file_name_from_file_id(self, file_id):
        file_info = self.api.get_file_info(file_id)
        return file_info['fileName']


@B2.register_subcommand
class DeleteKey(Command):
    """
    Deletes the specified application key by its ID.

    Requires capability:

    - **deleteKeys**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('applicationKeyId')

    def run(self, args):
        response = self.api.delete_key(application_key_id=args.applicationKeyId)
        self._print(response['applicationKeyId'])
        return 0


@B2.register_subcommand
class DownloadFileById(Command):
    """
    Downloads the given file, and stores it in the given local file.

    If the ``tqdm`` library is installed, progress bar is displayed
    on stderr.  Without it, simple text progress is printed.
    Use ``--noProgress`` to disable progress reporting.

    Requires capability:

    - **readFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--noProgress', action='store_true')
        parser.add_argument('fileId')
        parser.add_argument('localFileName')

    def run(self, args):
        progress_listener = make_progress_listener(args.localFileName, args.noProgress)
        download_dest = DownloadDestLocalFile(args.localFileName)
        self.api.download_file_by_id(args.fileId, download_dest, progress_listener)
        self.console_tool._print_download_info(download_dest)
        return 0


@B2.register_subcommand
class DownloadFileByName(Command):
    """
    Downloads the given file, and stores it in the given local file.

    If the ``tqdm`` library is installed, progress bar is displayed
    on stderr.  Without it, simple text progress is printed.
    Use ``--noProgress`` to disable progress reporting.

    Requires capability:

    - **readFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--noProgress', action='store_true')
        parser.add_argument('bucketName')
        parser.add_argument('b2FileName')
        parser.add_argument('localFileName')

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        progress_listener = make_progress_listener(args.localFileName, args.noProgress)
        download_dest = DownloadDestLocalFile(args.localFileName)
        bucket.download_file_by_name(args.b2FileName, download_dest, progress_listener)
        self.console_tool._print_download_info(download_dest)
        return 0


@B2.register_subcommand
class GetAccountInfo(Command):
    """
    Shows the account ID, key, auth token, URLs, and what capabilities
    the current application keys has.
    """

    REQUIRES_AUTH = False

    def run(self, args):
        account_info = self.api.account_info
        data = dict(
            accountId=account_info.get_account_id(),
            allowed=account_info.get_allowed(),
            applicationKey=account_info.get_application_key(),
            accountAuthToken=account_info.get_account_auth_token(),
            apiUrl=account_info.get_api_url(),
            downloadUrl=account_info.get_download_url()
        )
        self._print_json(data)
        return 0


@B2.register_subcommand
class GetBucket(Command):
    """
    Prints all of the information about the bucket, including
    bucket info, CORS rules and lifecycle rules.

    If ``--showSize`` is specified, then display the number of files
    (``fileCount``) in the bucket and the aggregate size of all files
    (``totalSize``). Hidden files and hide markers are accounted for
    in the reported number of files, and hidden files also
    contribute toward the reported aggregate size, whereas hide
    markers do not. Each version of a file counts as an individual
    file, and its size contributes toward the aggregate size.
    Analysis is recursive.

    .. note::

        Note that ``--showSize`` requires multiple
        API calls, and will therefore incur additional latency,
        computation, and Class C transactions.

    Requires capability:

    - **listBuckets**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--showSize', action='store_true')
        parser.add_argument('bucketName')

    def run(self, args):
        # This always wants up-to-date info, so it does not use
        # the bucket cache.
        for b in self.api.list_buckets(args.bucketName):
            if not args.showSize:
                self._print_json(b)
                return 0
            else:
                result = b.as_dict()
                # `files` is a generator. We don't want to collect all of the values from the
                # generator, as there many be billions of files in a large bucket.
                files = b.ls("", show_versions=True, recursive=True)
                # `files` yields tuples of (file_version_info, folder_name). We don't care about
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


@B2.register_subcommand
class GetFileInfo(Command):
    """
    Prints all of the information about the file, but not its contents.

    Requires capability:

    - **readFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('fileId')

    def run(self, args):
        response = self.api.get_file_info(args.fileId)
        self._print_json(response)
        return 0


@B2.register_subcommand
class GetDownloadAuth(Command):
    """
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
        parser.add_argument('bucketName')

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        auth_token = bucket.get_download_authorization(
            file_name_prefix=args.prefix, valid_duration_in_seconds=args.duration
        )
        self._print(auth_token)
        return 0


@B2.register_subcommand
class GetDownloadUrlWithAuth(Command):
    """
    Prints a URL to download the given file.  The URL includes an authorization
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
        parser.add_argument('bucketName')
        parser.add_argument('fileName')

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        auth_token = bucket.get_download_authorization(
            file_name_prefix=args.fileName, valid_duration_in_seconds=args.duration
        )
        base_url = self.api.get_download_url_for_file_name(args.bucketName, args.fileName)
        url = base_url + '?Authorization=' + auth_token
        self._print(url)
        return 0


@B2.register_subcommand
class HideFile(Command):
    """
    Uploads a new, hidden, version of the given file.

    Requires capability:

    - **writeFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('bucketName')
        parser.add_argument('fileName')

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        file_info = bucket.hide_file(args.fileName)
        self._print_json(file_info)
        return 0


@B2.register_subcommand
class ListBuckets(Command):
    """
    Lists all of the buckets in the current account.

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

    def run(self, args):
        buckets = self.api.list_buckets()
        if args.json:
            self._print_json(list(buckets))
            return 0

        for b in buckets:
            self._print('%s  %-10s  %s' % (b.id_, b.type_, b.name))
        return 0


@B2.register_subcommand
class ListKeys(Command):
    """
    Lists the application keys for the current account.

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

    def __init__(self, console_tool):
        super(ListKeys, self).__init__(console_tool)
        self.bucket_id_to_bucket_name = None

    def run(self, args):
        # The first query doesn't pass in a starting key id
        start_id = None

        # Keep querying until there are no more.
        while True:
            # Get some keys and print them
            response = self.api.list_keys(start_id)
            self.print_keys(response['keys'], args.long)

            # Are there more?  If so, we'll set the start_id for the next time around.
            next_id = response.get('nextApplicationKeyId')
            if next_id is None:
                break
            else:
                start_id = next_id

        return 0

    def print_keys(self, keys_from_response, is_long_format):
        if is_long_format:
            format_str = "{keyId}   {keyName:20s}   {bucketName:20s}   {dateStr:10s}   {timeStr:8s}   '{namePrefix}'   {capabilities}"
        else:
            format_str = '{keyId}   {keyName:20s}'
        for key in keys_from_response:
            timestamp_or_none = apply_or_none(int, key.get('expirationTimestamp'))
            (date_str, time_str) = self.timestamp_display(timestamp_or_none)
            key_str = format_str.format(
                keyId=key['applicationKeyId'],
                keyName=key['keyName'],
                bucketName=self.bucket_display_name(key.get('bucketId')),
                namePrefix=(key.get('namePrefix') or ''),
                capabilities=','.join(key['capabilities']),
                dateStr=date_str,
                timeStr=time_str
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
            dt = datetime.datetime.utcfromtimestamp(timestamp / 1000)
            return dt.strftime('%Y-%m-%d'), dt.strftime('%H:%M:%S')


@B2.register_subcommand
class ListParts(Command):
    """
    Lists all of the parts that have been uploaded for the given
    large file, which must be a file that was started but not
    finished or canceled.

    Requires capability:

    - **writeFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('largeFileId')

    def run(self, args):
        for part in self.api.list_parts(args.largeFileId):
            self._print('%5d  %9d  %s' % (part.part_number, part.content_length, part.content_sha1))
        return 0


@B2.register_subcommand
class ListUnfinishedLargeFiles(Command):
    """
    Lists all of the large files in the bucket that were started,
    but not finished or canceled.

    Requires capability:

    - **listFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('bucketName')

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        for unfinished in bucket.list_unfinished_large_files():
            file_info_text = ' '.join(
                '%s=%s' % (k, unfinished.file_info[k]) for k in sorted(unfinished.file_info)
            )
            self._print(
                '%s %s %s %s' %
                (unfinished.file_id, unfinished.file_name, unfinished.content_type, file_info_text)
            )
        return 0


@B2.register_subcommand
class Ls(Command):
    """
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

    The ``--versions`` option shows all versions of each file, not
    just the most recent.

    The ``--recursive`` option will descend into folders, and will show
    only files, not folders.

    Requires capability:

    - **listFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--long', action='store_true')
        parser.add_argument('--json', action='store_true')
        parser.add_argument('--versions', action='store_true')
        parser.add_argument('--recursive', action='store_true')
        parser.add_argument('bucketName')
        parser.add_argument('folderName', nargs='?')

    def run(self, args):
        if args.folderName is None:
            start_file_name = ""
        else:
            start_file_name = args.folderName
            if not start_file_name.endswith('/'):
                start_file_name += '/'

        bucket = self.api.get_bucket_by_name(args.bucketName)
        generator = bucket.ls(
            start_file_name,
            show_versions=args.versions,
            recursive=args.recursive,
        )

        if args.json:
            self._print_json([file_version_info for file_version_info, _ in generator])
            return 0

        for file_version_info, folder_name in generator:
            if not args.long:
                self._print(folder_name or file_version_info.file_name)
            elif folder_name is not None:
                self._print(FileVersionInfo.format_folder_ls_entry(folder_name))
            else:
                self._print(file_version_info.format_ls_entry())

        return 0


@B2.register_subcommand
class MakeUrl(Command):
    """
    Prints an URL that can be used to download the given file, if
    it is public.
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('fileId')

    def run(self, args):
        self._print(self.api.get_download_url_for_fileid(args.fileId))
        return 0


@B2.register_subcommand
class MakeFriendlyUrl(Command):
    """
    Prints a short URL that can be used to download the given file, if
    it is public.
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('bucketName')
        parser.add_argument('fileName')

    def run(self, args):
        self._print(self.api.get_download_url_for_file_name(args.bucketName, args.fileName))
        return 0


@B2.register_subcommand
class Sync(Command):
    """
    Copies multiple files from source to destination.  Optionally
    deletes or hides destination files that the source does not have.

    The synchronizer can copy files:

    - From a B2 bucket to a local destination.
    - From a local source to a B2 bucket.
    - From one B2 bucket to another.
    - Between different folders in the same B2 bucket.

    Use ``b2://<bucketName>/<prefix>`` for B2 paths, e.g. ``b2://my-bucket-name/a/path/prefix/``.

    Progress is displayed on the console unless ``--noProgress`` is
    specified.  A list of actions taken is always printed.

    Specify ``--dryRun`` to simulate the actions that would be taken.

    To allow sync to run when the source directory is empty, potentially
    deleting all files in a bucket, specify ``--allowEmptySource``.
    The default is to fail when the specified source directory doesn't exist
    or is empty.  (This check only applies to version 1.0 and later.)

    Users with high-performance networks, or file sets with very small
    files, will benefit from multi-threaded uploads.  The default number
    of threads is 10.  Experiment with the ``--threads`` parameter if the
    default is not working well.

    Users with low-performance networks may benefit from reducing the
    number of threads.  Using just one thread will minimize the impact
    on other users of the network.

    .. note::

        Note that using multiple threads will usually be detrimental to
        the other users on your network.

    You can specify ``--excludeRegex`` to selectively ignore files that
    match the given pattern. Ignored files will not copy during
    the sync operation. The pattern is a regular expression
    that is tested against the full path of each file.

    You can specify ``--includeRegex`` to selectively override ignoring
    files that match the given ``--excludeRegex`` pattern by an
    ``--includeRegex`` pattern. Similarly to ``--excludeRegex``, the pattern
    is a regular expression that is tested against the full path
    of each file.

    .. note::

        Note that ``--includeRegex`` cannot be used without ``--excludeRegex``.

    You can specify ``--excludeAllSymlinks`` to skip symlinks when
    syncing from a local source.

    When a directory is excluded by using ``--excludeDirRegex``, all of
    the files within it are excluded, even if they match an ``--includeRegex``
    pattern.   This means that there is no need to look inside excluded
    directories, and you can exclude directories containing files for which
    you don't have read permission and avoid getting errors.

    The ``--excludeDirRegex`` is a regular expression that is tested against
    the full path of each directory.  The path being matched does not have
    a trailing ``/``, so don't include on in your regular expression.

    Multiple regex rules can be applied by supplying them as pipe
    delimited instructions. Note that the regex for this command
    is Python regex.
    Reference: `<https://docs.python.org/2/library/re.html>`_

    Regular expressions are considered a match if they match a substring
    starting at the first character.  ``.*e`` will match ``hello``.  This is
    not ideal, but we will maintain this behavior for compatibility.
    If you want to match the entire path, put a ``$`` at the end of the
    regex, such as ``.*llo$``.

    You can specify ``--excludeIfModifiedAfter`` to selectively ignore file versions
    (including hide markers) which were synced after given time (for local source)
    or ignore only specific file versions (for b2 source).
    Ignored files or file versions will not be taken for consideration during sync.
    The time should be given as a seconds timestamp (e.g. "1367900664")
    If you need milliseconds precision, put it after the comma (e.g. "1367900664.152")

    Files are considered to be the same if they have the same name
    and modification time.  This behaviour can be changed using the
    ``--compareVersions`` option. Possible values are:

    - ``none``:    Comparison using the file name only
    - ``modTime``: Comparison using the modification time (default)
    - ``size``:    Comparison using the file size

    A future enhancement may add the ability to compare the SHA1 checksum
    of the files.

    Fuzzy comparison of files based on modTime or size can be enabled by
    specifying the ``--compareThreshold`` option.  This will treat modTimes
    (in milliseconds) or sizes (in bytes) as the same if they are within
    the comparison threshold.  Files that match, within the threshold, will
    not be synced. Specifying ``--verbose`` and ``--dryRun`` can be useful to
    determine comparison value differences.

    When a destination file is present that is not in the source, the
    default is to leave it there.  Specifying ``--delete`` means to delete
    destination files that are not in the source.

    When the destination is B2, you have the option of leaving older
    versions in place.  Specifying ``--keepDays`` will delete any older
    versions more than the given number of days old, based on the
    modification time of the file.  This option is not available when
    the destination is a local folder.

    Files at the source that have a newer modification time are always
    copied to the destination.  If the destination file is newer, the
    default is to report an error and stop.  But with ``--skipNewer`` set,
    those files will just be skipped.  With ``--replaceNewer`` set, the
    old file from the source will replace the newer one in the destination.

    To make the destination exactly match the source, use:

    .. code-block::

        {NAME} sync --delete --replaceNewer ... ...

    .. warning::

        Using ``--delete`` deletes files!  We recommend not using it.
        If you use ``--keepDays`` instead, you will have some time to recover your
        files if you discover they are missing on the source end.

    To make the destination match the source, but retain previous versions
    for 30 days:

    .. code-block::

        {NAME} sync --keepDays 30 --replaceNewer ... b2://...

    Example of sync being used with ``--excludeRegex``. This will ignore ``.DS_Store`` files
    and ``.Spotlight-V100`` folders:

    .. code-block::

        {NAME} sync --excludeRegex '(.*\.DS_Store)|(.*\.Spotlight-V100)' ... b2://...

    Requires capabilities:

    - **listFiles**
    - **readFiles** (for downloading)
    - **writeFiles** (for uploading)
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--noProgress', action='store_true')
        parser.add_argument('--dryRun', action='store_true')
        parser.add_argument('--allowEmptySource', action='store_true')
        parser.add_argument('--excludeAllSymlinks', action='store_true')
        parser.add_argument('--threads', type=int, default=10)
        parser.add_argument(
            '--compareVersions', default='modTime', choices=('none', 'modTime', 'size')
        )
        parser.add_argument('--compareThreshold', type=int, metavar='MILLIS')
        parser.add_argument('--excludeRegex', action='append', default=[], metavar='REGEX')
        parser.add_argument('--includeRegex', action='append', default=[], metavar='REGEX')
        parser.add_argument('--excludeDirRegex', action='append', default=[], metavar='REGEX')
        parser.add_argument(
            '--excludeIfModifiedAfter',
            type=parse_millis_from_float_timestamp,
            default=None,
            metavar='TIMESTAMP'
        )
        parser.add_argument('source')
        parser.add_argument('destination')

        skip_group = parser.add_mutually_exclusive_group()
        skip_group.add_argument('--skipNewer', action='store_true')
        skip_group.add_argument('--replaceNewer', action='store_true')

        del_keep_group = parser.add_mutually_exclusive_group()
        del_keep_group.add_argument('--delete', action='store_true')
        del_keep_group.add_argument('--keepDays', type=float, metavar='DAYS')

    def run(self, args):
        policies_manager = self.get_policies_manager_from_args(args)

        self.api.services.upload_manager.set_thread_pool_size(args.threads)

        source = parse_sync_folder(args.source, self.console_tool.api)
        destination = parse_sync_folder(args.destination, self.console_tool.api)
        allow_empty_source = args.allowEmptySource or VERSION_0_COMPATIBILITY

        synchronizer = self.get_synchronizer_from_args(
            args,
            args.threads,
            policies_manager,
            allow_empty_source,
        )
        with SyncReport(self.stdout, args.noProgress) as reporter:
            synchronizer.sync_folders(
                source_folder=source,
                dest_folder=destination,
                now_millis=current_time_millis(),
                reporter=reporter,
            )
        return 0

    def get_policies_manager_from_args(self, args):
        return ScanPoliciesManager(
            exclude_dir_regexes=args.excludeDirRegex,
            exclude_file_regexes=args.excludeRegex,
            include_file_regexes=args.includeRegex,
            exclude_all_symlinks=args.excludeAllSymlinks,
            exclude_modified_after=args.excludeIfModifiedAfter,
        )

    def get_synchronizer_from_args(
        self,
        args,
        max_workers,
        policies_manager=DEFAULT_SCAN_MANAGER,
        allow_empty_source=False,
    ):
        if args.replaceNewer:
            newer_file_mode = NewerFileSyncMode.REPLACE
        elif args.skipNewer:
            newer_file_mode = NewerFileSyncMode.SKIP
        else:
            newer_file_mode = NewerFileSyncMode.RAISE_ERROR

        if args.compareVersions == 'none':
            compare_version_mode = CompareVersionMode.NONE
        elif args.compareVersions == 'modTime':
            compare_version_mode = CompareVersionMode.MODTIME
        elif args.compareVersions == 'size':
            compare_version_mode = CompareVersionMode.SIZE
        else:
            compare_version_mode = CompareVersionMode.MODTIME
        compare_threshold = args.compareThreshold

        keep_days = None

        if args.delete:
            keep_days_or_delete = KeepOrDeleteMode.DELETE
        elif args.keepDays:
            keep_days_or_delete = KeepOrDeleteMode.KEEP_BEFORE_DELETE
            keep_days = args.keepDays
        else:
            keep_days_or_delete = KeepOrDeleteMode.NO_DELETE

        return Synchronizer(
            max_workers,
            policies_manager=policies_manager,
            dry_run=args.dryRun,
            allow_empty_source=allow_empty_source,
            newer_file_mode=newer_file_mode,
            keep_days_or_delete=keep_days_or_delete,
            compare_version_mode=compare_version_mode,
            compare_threshold=compare_threshold,
            keep_days=keep_days,
        )


@B2.register_subcommand
class UpdateBucket(Command):
    """
    Updates the ``bucketType`` of an existing bucket.  Prints the ID
    of the bucket updated.

    Optionally stores bucket info, CORS rules and lifecycle rules with the bucket.
    These can be given as JSON on the command line.

    Requires capability:

    - **writeBuckets**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--bucketInfo', type=json.loads)
        parser.add_argument('--corsRules', type=json.loads)
        parser.add_argument('--lifecycleRules', type=json.loads)
        parser.add_argument('bucketName')
        parser.add_argument('bucketType')

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        response = bucket.update(
            bucket_type=args.bucketType,
            bucket_info=args.bucketInfo,
            cors_rules=args.corsRules,
            lifecycle_rules=args.lifecycleRules
        )
        self._print_json(response)
        return 0


@B2.register_subcommand
class UploadFile(Command):
    """
    Uploads one file to the given bucket.  Uploads the contents
    of the local file, and assigns the given name to the B2 file.

    By default, upload_file will compute the sha1 checksum of the file
    to be uploaded.  But, if you already have it, you can provide it
    on the command line to save a little time.

    Content type is optional.  If not set, it will be set based on the
    file extension.

    By default, the file is broken into as many parts as possible to
    maximize upload parallelism and increase speed.  The minimum that
    B2 allows is 100MB.  Setting ``--minPartSize`` to a larger value will
    reduce the number of parts uploaded when uploading a large file.

    The maximum number of upload threads to use to upload parts of a large file
    is specified by ``--threads``.  It has no effect on small files (under 200MB).
    Default is 10.

    If the ``tqdm`` library is installed, progress bar is displayed
    on stderr.  Without it, simple text progress is printed.
    Use ``--noProgress`` to disable progress reporting.

    Each fileInfo is of the form ``a=b``.

    Requires capability:

    - **writeFiles**
    """

    @classmethod
    def _setup_parser(cls, parser):
        parser.add_argument('--noProgress', action='store_true')
        parser.add_argument('--quiet', action='store_true')
        parser.add_argument('--contentType')
        parser.add_argument('--minPartSize', type=int)
        parser.add_argument('--sha1')
        parser.add_argument('--threads', type=int, default=10)
        parser.add_argument('--info', action='append', default=[])
        parser.add_argument('bucketName')
        parser.add_argument('localFilePath')
        parser.add_argument('b2FileName')

    def run(self, args):
        file_infos = self._parse_file_infos(args.info)

        if SRC_LAST_MODIFIED_MILLIS not in file_infos:
            file_infos[SRC_LAST_MODIFIED_MILLIS] = str(
                int(os.path.getmtime(args.localFilePath) * 1000)
            )

        self.api.services.upload_manager.set_thread_pool_size(args.threads)

        bucket = self.api.get_bucket_by_name(args.bucketName)
        file_info = bucket.upload_local_file(
            local_file=args.localFilePath,
            file_name=args.b2FileName,
            content_type=args.contentType,
            file_infos=file_infos,
            sha1_sum=args.sha1,
            min_part_size=args.minPartSize,
            progress_listener=make_progress_listener(args.localFilePath, args.noProgress),
        )
        if not args.quiet:
            self._print("URL by file name: " + bucket.get_download_url(args.b2FileName))
            self._print("URL by fileId: " + self.api.get_download_url_for_fileid(file_info.id_))
        self._print_json(file_info)
        return 0


@B2.register_subcommand
class Version(Command):
    """
    Prints the version number of this tool.
    """

    REQUIRES_AUTH = False

    def run(self, args):
        self._print('b2 command line tool, version', VERSION)
        return 0


class ConsoleTool(object):
    """
    Implements the commands available in the B2 command-line tool
    using the B2Api library.

    Uses the StoredAccountInfo object to keep account data in
    ``{B2_ACCOUNT_INFO_DEFAULT_FILE}`` between runs.
    """

    def __init__(self, b2_api, stdout, stderr):
        self.api = b2_api
        self.stdout = stdout
        self.stderr = stderr

    def run_command(self, argv):
        signal.signal(signal.SIGINT, keyboard_interrupt_handler)
        b2_command = B2(self)
        args = b2_command.get_parser().parse_args(argv[1:])

        command_class = b2_command.run(args)
        command = command_class(self)

        self._setup_logging(args, command, argv)

        try:
            auth_ret = self.authorize_from_env(command_class)
            if auth_ret:
                return auth_ret
            return command.run(args)
        except MissingAccountData as e:
            logger.exception('ConsoleTool missing account data error')
            self._print_stderr(
                'ERROR: %s  Use: %s authorize-account or provide auth data with "%s" and "%s"'
                ' environment variables' %
                (str(e), NAME, B2_APPLICATION_KEY_ID_ENV_VAR, B2_APPLICATION_KEY_ENV_VAR)
            )
            return 1
        except B2Error as e:
            logger.exception('ConsoleTool command error')
            self._print_stderr('ERROR: %s' % (str(e),))
            return 1
        except KeyboardInterrupt:
            logger.exception('ConsoleTool command interrupt')
            self._print('\nInterrupted.  Shutting down...\n')
            return 1
        except Exception:
            logger.exception('ConsoleTool unexpected exception')
            raise

    def authorize_from_env(self, command_class):
        if not command_class.REQUIRES_AUTH:
            return 0

        key_id = os.environ.get(B2_APPLICATION_KEY_ID_ENV_VAR)
        key = os.environ.get(B2_APPLICATION_KEY_ENV_VAR)

        if key_id is None and key is None:
            return 0

        if (key_id is None) or (key is None):
            self._print_stderr(
                'Please provide both "%s" and "%s" environment variables or none of them' %
                (B2_APPLICATION_KEY_ENV_VAR, B2_APPLICATION_KEY_ID_ENV_VAR)
            )
            return 1
        realm = os.environ.get(B2_ENVIRONMENT_ENV_VAR, 'production')

        if self.api.account_info.is_same_key(key_id, realm):
            return 0

        return AuthorizeAccount(self).authorize(key_id, key, realm)

    def _print(self, *args, **kwargs):
        print(*args, file=self.stdout, **kwargs)

    def _print_stderr(self, *args, **kwargs):
        print(*args, file=self.stderr, **kwargs)

    def _print_download_info(self, download_dest):
        self._print('File name:   ', download_dest.file_name)
        self._print('File id:     ', download_dest.file_id)
        self._print('File size:   ', download_dest.content_length)
        self._print('Content type:', download_dest.content_type)
        self._print('Content sha1:', download_dest.content_sha1)
        for name in sorted(download_dest.file_info):
            self._print('INFO', name + ':', download_dest.file_info[name])
        if download_dest.content_sha1 != 'none':
            self._print('checksum matches')
        return 0

    @classmethod
    def _setup_logging(cls, args, command, argv):
        if args.logConfig:
            logging.config.fileConfig(args.logConfig)
        elif args.verbose:
            logging.basicConfig(level=logging.DEBUG)
        elif args.debugLogs:
            formatter = logging.Formatter(
                '%(asctime)s\t%(process)d\t%(thread)d\t%(name)s\t%(levelname)s\t%(message)s'
            )
            formatter.converter = time.gmtime
            handler = logging.FileHandler('b2_cli.log')
            handler.setLevel(logging.DEBUG)
            handler.setFormatter(formatter)

            b2_logger = logging.getLogger('b2')
            b2_logger.setLevel(logging.DEBUG)
            b2_logger.addHandler(handler)
            b2_logger.propagate = False

        logger.info('// %s %s %s \\\\', SEPARATOR, VERSION.center(8), SEPARATOR)
        logger.debug('platform is %s', platform.platform())
        logger.debug(
            'Python version is %s %s', platform.python_implementation(),
            sys.version.replace('\n', ' ')
        )
        logger.debug('b2sdk version is %s', b2sdk_version)
        logger.debug('locale is %s', locale.getdefaultlocale())
        logger.debug('filesystem encoding is %s', sys.getfilesystemencoding())

        if command.FORBID_LOGGING_ARGUMENTS:
            logger.info('starting command [%s] (arguments hidden)', command)
        else:
            logger.info('starting command [%s] with arguments: %s', command, argv)


# used by Sphinx
get_parser = functools.partial(B2.get_parser, for_docs=True)


# TODO: import from b2sdk as soon as we rely on 1.0.0
class InvalidArgument(B2Error):
    """
    Raised when one or more arguments are invalid
    """

    def __init__(self, parameter_name, message):
        """
        :param parameter_name: name of the function argument
        :param message: brief explanation of misconfiguration
        """
        super(InvalidArgument, self).__init__()
        self.parameter_name = parameter_name
        self.message = message

    def __str__(self):
        return "%s %s" % (self.parameter_name, self.message)


def main():
    info = SqliteAccountInfo()
    cache = AuthInfoCache(info)
    raw_api = B2RawApi(B2Http(user_agent_append=os.environ.get(B2_USER_AGENT_APPEND_ENV_VAR)))
    b2_api = B2Api(info, cache=cache, raw_api=raw_api)
    ct = ConsoleTool(b2_api=b2_api, stdout=sys.stdout, stderr=sys.stderr)
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
