######################################################################
#
# File: b2/console_tool.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import absolute_import, print_function

import copy
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
import textwrap
import time

import six

from .account_info.sqlite_account_info import (
    B2_ACCOUNT_INFO_ENV_VAR, B2_ACCOUNT_INFO_DEFAULT_FILE, SqliteAccountInfo
)
from .account_info.test_upload_url_concurrency import test_upload_url_concurrency
from .account_info.exception import (MissingAccountData)
from .api import (B2Api)
from .b2http import (test_http)
from .cache import (AuthInfoCache)
from .download_dest import (DownloadDestLocalFile)
from .exception import (B2Error, BadFileInfo)
from .sync.scan_policies import ScanPoliciesManager
from .file_version import (FileVersionInfo)
from .parse_args import parse_arg_list
from .progress import (make_progress_listener)
from .raw_api import (SRC_LAST_MODIFIED_MILLIS, test_raw_api)
from .sync import parse_sync_folder, sync_folders
from .utils import (current_time_millis, set_shutting_down)
from .version import (VERSION)

logger = logging.getLogger(__name__)

SEPARATOR = '=' * 40

# Strings available to use when formatting doc strings.
DOC_STRING_DATA = dict(
    B2_ACCOUNT_INFO_ENV_VAR=B2_ACCOUNT_INFO_ENV_VAR,
    B2_ACCOUNT_INFO_DEFAULT_FILE=B2_ACCOUNT_INFO_DEFAULT_FILE
)

# Enable to get 0.* behavior in the command-line tool.
# Disable for 1.* behavior.
VERSION_0_COMPATIBILITY = False


def local_path_to_b2_path(path):
    """
    Ensures that the separator in the path is '/', not '\'.

    :param path: A path from the local file system
    :return: A path that uses '/' as the separator.
    """
    return path.replace(os.path.sep, '/')


def keyboard_interrupt_handler(signum, frame):
    set_shutting_down()
    raise KeyboardInterrupt()


def mixed_case_to_hyphens(s):
    return s[0].lower() + ''.join(c if c.islower() else '-' + c.lower() for c in s[1:])


def parse_comma_separated_list(s):
    return [word.strip() for word in s.split(',')]


def apply_or_none(fcn, value):
    """
    If the value is None, return None, otherwise return the result of applying the function to it.
    """
    if value is None:
        return None
    else:
        return fcn(value)


class Command(object):
    """
    Base class for commands.  Has basic argument parsing and printing.
    """

    # Option flags.  A name here that looks like "fast" can be set to
    # True with a command line option "--fast".  All option flags
    # default to False.
    OPTION_FLAGS = []

    # Global option flags.  Not shown in help.
    GLOBAL_OPTION_FLAGS = ['debugLogs', 'verbose']

    # Explicit arguments.  These always come before the positional arguments.
    # Putting "color" here means you can put something like "--color blue" on
    # the command line, and args.color will be set to "blue".  These all
    # default to None.
    OPTION_ARGS = []

    # Global explicit arguments.  Not shown in help.
    GLOBAL_OPTION_ARGS = ['logConfig']

    # Optional arguments that you can specify zero or more times and the
    # values are collected into a list.  Default is []
    LIST_ARGS = []

    # Optional, positional, parameters that come before the required
    # arguments.
    OPTIONAL_BEFORE = []

    # Required positional arguments.  Never None.
    REQUIRED = []

    # Optional positional arguments.  Default to None if not present.
    OPTIONAL = []

    # Set to True for commands that should not be listed in the summary.
    PRIVATE = False

    # Set to True for commands that receive sensitive information in arguments
    FORBID_LOGGING_ARGUMENTS = False

    # Parsers for each argument.  Each should be a function that
    # takes a string and returns the value.
    ARG_PARSER = {}

    def __init__(self, console_tool):
        self.console_tool = console_tool
        self.api = console_tool.api
        self.stdout = console_tool.stdout
        self.stderr = console_tool.stderr

    @classmethod
    def summary_line(cls):
        """
        Returns the one-line summary of how to call the command.
        """
        lines = cls.command_usage().split('\n')
        while lines[0].strip() == '':
            lines = lines[1:]
        result = []
        for line in lines:
            result.append(line)
            if not line.endswith('\\'):
                break
        return six.u('\n').join(result)

    @classmethod
    def command_usage(cls):
        """
        Returns the doc string for this class, with templated fields
        filled in, and leading whitespace removed.
        """
        return textwrap.dedent(cls.__doc__).format(**DOC_STRING_DATA)

    def parse_arg_list(self, arg_list):
        return parse_arg_list(
            arg_list,
            option_flags=self.OPTION_FLAGS + self.GLOBAL_OPTION_FLAGS,
            option_args=self.OPTION_ARGS + self.GLOBAL_OPTION_ARGS,
            list_args=self.LIST_ARGS,
            optional_before=self.OPTIONAL_BEFORE,
            required=self.REQUIRED,
            optional=self.OPTIONAL,
            arg_parser=self.ARG_PARSER
        )

    def _print(self, *args):
        self._print_helper(self.stdout, self.stdout.encoding, 'stdout', *args)

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


class AuthorizeAccount(Command):
    """
    b2 authorize-account [<accountIdOrKeyId>] [<applicationKey>]

        Prompts for Backblaze accountID and applicationKey (unless they
        are given on the command line).

        You can authorize with either the master application key or
        a normal application key.

        To use the master application key, provide the account ID and
        application key from the "B2 Cloud Storage Buckets" page on
        the web site: https://secure.backblaze.com/b2_buckets.htm

        To use a normal application key, created with the create-key
        command or on the web site, provide the application key ID
        and the application key itself.

        Stores an account auth token in {B2_ACCOUNT_INFO_DEFAULT_FILE} by default,
        or the file specified by the {B2_ACCOUNT_INFO_ENV_VAR} environment variable.

        Requires capability: listBuckets
    """

    OPTION_FLAGS = ['dev', 'staging']  # undocumented

    OPTIONAL = ['accountId', 'applicationKey']

    FORBID_LOGGING_ARGUMENTS = True

    def run(self, args):
        # Handle internal options for testing inside Backblaze.  These
        # are not documented in the usage string.
        realm = 'production'
        if args.staging:
            realm = 'staging'
        if args.dev:
            realm = 'dev'

        url = self.api.account_info.REALM_URLS[realm]
        self._print('Using %s' % url)

        if args.accountId is None:
            args.accountId = six.moves.input('Backblaze account ID: ')

        if args.applicationKey is None:
            args.applicationKey = getpass.getpass('Backblaze application key: ')

        try:
            self.api.authorize_account(realm, args.accountId, args.applicationKey)

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


class CancelAllUnfinishedLargeFiles(Command):
    """
    b2 cancel-all-unfinished-large-files <bucketName>

        Lists all large files that have been started but not
        finished and cancels them.  Any parts that have been
        uploaded will be deleted.

        Requires capability: listFiles, writeFiles
    """

    REQUIRED = ['bucketName']

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        for file_version in bucket.list_unfinished_large_files():
            bucket.cancel_large_file(file_version.file_id)
            self._print(file_version.file_id, 'canceled')
        return 0


class CancelLargeFile(Command):
    """
    b2 cancel-large-file <fileId>

        Cancels a large file upload.  Used to undo a start-large-file.

        Cannot be used once the file is finished.  After finishing,
        using delete-file-version to delete the large file.

        Requires capability: writeFiles
    """

    REQUIRED = ['fileId']

    def run(self, args):
        self.api.cancel_large_file(args.fileId)
        self._print(args.fileId, 'canceled')
        return 0


class ClearAccount(Command):
    """
    b2 clear-account

        Erases everything in {B2_ACCOUNT_INFO_DEFAULT_FILE}.  Location
        of file can be overridden by setting {B2_ACCOUNT_INFO_ENV_VAR}.
    """

    def run(self, args):
        self.api.account_info.clear()
        return 0


class CreateBucket(Command):
    """
    b2 create-bucket [--bucketInfo <json>] [--corsRules <json>] [--lifecycleRules <json>] <bucketName> [allPublic | allPrivate]

        Creates a new bucket.  Prints the ID of the bucket created.

        Optionally stores bucket info, CORS rules and lifecycle rules with the bucket.
        These can be given as JSON on the command line.

        Requires capability: writeBuckets
    """

    REQUIRED = ['bucketName', 'bucketType']

    OPTION_ARGS = ['bucketInfo', 'corsRules', 'lifecycleRules']

    ARG_PARSER = {'bucketInfo': json.loads, 'corsRules': json.loads, 'lifecycleRules': json.loads}

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


class CreateKey(Command):
    """
    b2 create-key [--duration <validDurationSeconds>] [--bucket <bucketName>] [--namePrefix <namePrefix>] <keyName> <capabilities>

        Creates a new application key.  Prints the application key information.  This is the only
        time the application key itself will be returned.  Listing application keys will show
        their IDs, but not the secret keys.

        The capabilities are passed in as a comma-separated list, like "readFiles,writeFiles".

        The 'validDurationSeconds' is the length of time the new application key will exist.
        When the time expires the key will disappear and will no longer be usable.  If not
        specified, the key will not expire.

        The 'bucketName' is the name of a bucket in the account.  When specified, the key
        will only allow access to that bucket.

        The 'namePrefix' restricts file access to files whose names start with the prefix.

        The output is the new application key ID, followed by the application key itself.
        The two values returned are the two that you pass to authorize-account to use the key.

        Requires capability: writeKeys
    """

    REQUIRED = ['keyName', 'capabilities']

    OPTION_ARGS = ['bucket', 'namePrefix', 'duration']

    ARG_PARSER = {'capabilities': parse_comma_separated_list, 'duration': int}

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


class DeleteBucket(Command):
    """
    b2 delete-bucket <bucketName>

        Deletes the bucket with the given name.

        Requires capability: deleteBuckets
    """

    REQUIRED = ['bucketName']

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        response = self.api.delete_bucket(bucket)
        self._print(json.dumps(response, indent=4, sort_keys=True))
        return 0


class DeleteFileVersion(Command):
    """
    b2 delete-file-version [<fileName>] <fileId>

        Permanently and irrevocably deletes one version of a file.

        Specifying the fileName is more efficient than leaving it out.
        If you omit the fileName, it requires an initial query to B2
        to get the file name, before making the call to delete the
        file.  This extra query requires the readFiles capability.

        Requires capability: deleteFiles, readFiles (if file name not provided)
    """

    OPTIONAL_BEFORE = ['fileName']
    REQUIRED = ['fileId']

    def run(self, args):
        if args.fileName is not None:
            file_name = args.fileName
        else:
            file_name = self._get_file_name_from_file_id(args.fileId)

        file_info = self.api.delete_file_version(args.fileId, file_name)
        self._print(json.dumps(file_info.as_dict(), indent=2, sort_keys=True))
        return 0

    def _get_file_name_from_file_id(self, file_id):
        file_info = self.api.get_file_info(file_id)
        return file_info['fileName']


class DeleteKey(Command):
    """
    b2 delete-key <applicationKeyId>

        Deletes the specified application key by its 'ID'.

        Requires capability: deleteKeys
    """

    REQUIRED = ['applicationKeyId']

    def run(self, args):
        response = self.api.delete_key(application_key_id=args.applicationKeyId)
        self._print(response['applicationKeyId'])
        return 0


class DownloadFileById(Command):
    """
    b2 download-file-by-id [--noProgress] <fileId> <localFileName>

        Downloads the given file, and stores it in the given local file.

        If the 'tqdm' library is installed, progress bar is displayed
        on stderr.  Without it, simple text progress is printed.
        Use '--noProgress' to disable progress reporting.

        Requires capability: readFiles
    """

    OPTION_FLAGS = ['noProgress']
    REQUIRED = ['fileId', 'localFileName']

    def run(self, args):
        progress_listener = make_progress_listener(args.localFileName, args.noProgress)
        download_dest = DownloadDestLocalFile(args.localFileName)
        self.api.download_file_by_id(args.fileId, download_dest, progress_listener)
        self.console_tool._print_download_info(download_dest)
        return 0


class DownloadFileByName(Command):
    """
    b2 download-file-by-name [--noProgress] <bucketName> <fileName> <localFileName>

        Downloads the given file, and stores it in the given local file.

        If the 'tqdm' library is installed, progress bar is displayed
        on stderr.  Without it, simple text progress is printed.
        Use '--noProgress' to disable progress reporting.

        Requires capability: readFiles
    """

    OPTION_FLAGS = ['noProgress']
    REQUIRED = ['bucketName', 'b2FileName', 'localFileName']

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        progress_listener = make_progress_listener(args.localFileName, args.noProgress)
        download_dest = DownloadDestLocalFile(args.localFileName)
        bucket.download_file_by_name(args.b2FileName, download_dest, progress_listener)
        self.console_tool._print_download_info(download_dest)
        return 0


class GetAccountInfo(Command):
    """
    b2 get-account-info

        Shows the account ID, key, auth token, URLs, and what capabilities
        the current application keys has.
    """

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
        self._print(json.dumps(data, indent=4, sort_keys=True))
        return 0


class GetBucket(Command):
    """
    b2 get-bucket [--showSize] <bucketName>

        Prints all of the information about the bucket, including
        bucket info, CORS rules and lifecycle rules.

        If --showSize is specified, then display the number of files
        (fileCount) in the bucket and the aggregate size of all files
        (totalSize). Hidden files and hide markers are accounted for
        in the reported number of files, and hidden files also
        contribute toward the reported aggregate size, whereas hide
        markers do not. Each version of a file counts as an individual
        file, and its size contributes toward the aggregate size.
        Analysis is recursive. Note that --showSize requires multiple
        API calls, and will therefore incur additional latency,
        computation, and Class C transactions.

        Requires capability: listBuckets
    """

    OPTION_FLAGS = ['showSize']

    REQUIRED = ['bucketName']

    def run(self, args):
        # This always wants up-to-date info, so it does not use
        # the bucket cache.
        for b in self.api.list_buckets(args.bucketName):
            if not args.showSize:
                self._print(json.dumps(b.bucket_dict, indent=4, sort_keys=True))
                return 0
            else:
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
                result = copy.copy(b.bucket_dict)
                result['fileCount'] = count_size_tuple[0]
                result['totalSize'] = count_size_tuple[1]
                self._print(json.dumps(result, indent=4, sort_keys=True))
                return 0
        self._print_stderr('bucket not found: ' + args.bucketName)
        return 1


class GetFileInfo(Command):
    """
    b2 get-file-info <fileId>

        Prints all of the information about the file, but not its contents.

        Requires capability: readFiles
    """

    REQUIRED = ['fileId']

    def run(self, args):
        response = self.api.get_file_info(args.fileId)
        self._print(json.dumps(response, indent=2, sort_keys=True))
        return 0


class GetDownloadAuth(Command):
    """
    b2 get-download-auth [--prefix <fileNamePrefix>] [--duration <durationInSeconds>] <bucketName>

        Prints an authorization token that is valid only for downloading
        files from the given bucket.

        The token is valid for the duration specified, which defaults
        to 86400 seconds (one day).

        Only files that match that given prefix can be downloaded with
        the token.  The prefix defaults to "", which matches all files
        in the bucket.

        Requires capability: shareFiles
    """

    OPTION_ARGS = ['prefix', 'duration']

    REQUIRED = ['bucketName']

    ARG_PARSER = {'duration': int}

    def run(self, args):
        prefix = args.prefix or ""
        duration = args.duration or 86400
        bucket = self.api.get_bucket_by_name(args.bucketName)
        auth_token = bucket.get_download_authorization(
            file_name_prefix=prefix, valid_duration_in_seconds=duration
        )
        self._print(auth_token)
        return 0


class GetDownloadUrlWithAuth(Command):
    """
    b2 get-download-url-with-auth [--duration <durationInSeconds>] <bucketName> <fileName>

        Prints a URL to download the given file.  The URL includes an authorization
        token that allows downloads from the given bucket for files whose names
        start with the given file name.

        The URL will work for the given file, but is not specific to that file.  Files
        with longer names that start with the give file name can also be downloaded
        with the same auth token.

        The token is valid for the duration specified, which defaults
        to 86400 seconds (one day).

        Requires capability: shareFiles
    """

    OPTION_ARGS = ['duration']

    REQUIRED = ['bucketName', 'fileName']

    ARG_PARSER = {'duration': int}

    def run(self, args):
        prefix = args.fileName
        duration = args.duration or 86400
        bucket = self.api.get_bucket_by_name(args.bucketName)
        auth_token = bucket.get_download_authorization(
            file_name_prefix=prefix, valid_duration_in_seconds=duration
        )
        base_url = self.api.get_download_url_for_file_name(args.bucketName, args.fileName)
        url = base_url + '?Authorization=' + auth_token
        self._print(url)
        return 0


class Help(Command):
    """
    b2 help [commandName]

        When no command is specified, prints general help.
        With a valid command name, prints details about that command.
    """

    OPTIONAL = ['commandName']

    def run(self, args):
        if args.commandName is None:
            return self.console_tool._usage_and_fail()
        command_cls = self.console_tool.command_name_to_class.get(args.commandName)
        if command_cls is None:
            return self.console_tool._usage_and_fail()
        self._print_stderr(command_cls.command_usage())
        return 1


class HideFile(Command):
    """
    b2 hide-file <bucketName> <fileName>

        Uploads a new, hidden, version of the given file.

        Requires capability: writeFiles
    """

    REQUIRED = ['bucketName', 'fileName']

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        file_info = bucket.hide_file(args.fileName)
        response = file_info.as_dict()
        self._print(json.dumps(response, indent=2, sort_keys=True))
        return 0


class ListBuckets(Command):
    """
    b2 list-buckets

        Lists all of the buckets in the current account.

        Output lines list the bucket ID, bucket type, and bucket name,
        and look like this:

            98c960fd1cb4390c5e0f0519  allPublic   my-bucket

        Requires capability: listBuckets
    """

    def run(self, args):
        for b in self.api.list_buckets():
            self._print('%s  %-10s  %s' % (b.id_, b.type_, b.name))
        return 0


class ListFileVersions(Command):
    """
    b2 list-file-versions <bucketName> [<startFileName>] [<startFileId>] [<maxToShow>]

        Lists the names of the files in a bucket, starting at the
        given point.  This is a low-level operation that reports the
        raw JSON returned from the service.  'b2 ls' provides a higher-
        level view.

        Requires capability: listFiles
    """

    REQUIRED = ['bucketName']

    OPTIONAL = ['startFileName', 'startFileId', 'maxToShow']

    ARG_PARSER = {'maxToShow': int}

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        response = bucket.list_file_versions(args.startFileName, args.startFileId, args.maxToShow)
        self._print(json.dumps(response, indent=2, sort_keys=True))
        return 0


class ListFileNames(Command):
    """
    b2 list-file-names <bucketName> [<startFileName>] [<maxToShow>]

        Lists the names of the files in a bucket, starting at the
        given point.

        Requires capability: listFiles
    """

    REQUIRED = ['bucketName']

    OPTIONAL = ['startFileName', 'maxToShow']

    ARG_PARSER = {'maxToShow': int}

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        response = bucket.list_file_names(args.startFileName, args.maxToShow)
        self._print(json.dumps(response, indent=2, sort_keys=True))
        return 0


class ListKeys(Command):
    """
    b2 list-keys

       Lists the application keys for the current account.

       The columns in the output are:
           - ID of the application key
           - Name of the application key
           - Name of the bucket the key is restricted to, or '-' for no restriction
           - Date of expiration, or '-'
           - Time of expiration, or '-'
           - File name prefix, in single quotes
           - Command-separated list of capabilities

        None of the values contain whitespace.

        For keys restricted to buckets that do not exist any more, the bucket name is
        replaced with 'id=<bucketId>', because deleted buckets do not have names any
        more.

        Requires capability: listKeys
    """

    OPTION_FLAGS = ['long']

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


class ListParts(Command):
    """
    b2 list-parts <largeFileId>

        Lists all of the parts that have been uploaded for the given
        large file, which must be a file that was started but not
        finished or canceled.

        Requires capability: writeFiles
    """

    REQUIRED = ['largeFileId']

    def run(self, args):
        for part in self.api.list_parts(args.largeFileId):
            self._print('%5d  %9d  %s' % (part.part_number, part.content_length, part.content_sha1))
        return 0


class ListUnfinishedLargeFiles(Command):
    """
    b2 list-unfinished-large-files <bucketName>

        Lists all of the large files in the bucket that were started,
        but not finished or canceled.

        Requires capability: listFiles

    """

    REQUIRED = ['bucketName']

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        for unfinished in bucket.list_unfinished_large_files():
            file_info_text = six.u(' ').join(
                '%s=%s' % (k, unfinished.file_info[k])
                for k in sorted(six.iterkeys(unfinished.file_info))
            )
            self._print(
                '%s %s %s %s' %
                (unfinished.file_id, unfinished.file_name, unfinished.content_type, file_info_text)
            )
        return 0


class Ls(Command):
    """
    b2 ls [--long] [--versions] [--recursive] <bucketName> [<folderName>]

        Using the file naming convention that "/" separates folder
        names from their contents, returns a list of the files
        and folders in a given folder.  If no folder name is given,
        lists all files at the top level.

        The --long option produces very wide multi-column output
        showing the upload date/time, file size, file id, whether it
        is an uploaded file or the hiding of a file, and the file
        name.  Folders don't really exist in B2, so folders are
        shown with "-" in each of the fields other than the name.

        The --version option shows all of versions of each file, not
        just the most recent.

        The --recursive option will descend into folders, and will show
        only files, not folders.

        Requires capability: listFiles
    """

    OPTION_FLAGS = ['long', 'versions', 'recursive']

    REQUIRED = ['bucketName']

    OPTIONAL = ['folderName']

    def run(self, args):
        if args.folderName is None:
            prefix = ""
        else:
            prefix = args.folderName
            if not prefix.endswith('/'):
                prefix += '/'

        bucket = self.api.get_bucket_by_name(args.bucketName)
        for file_version_info, folder_name in bucket.ls(
            prefix, show_versions=args.versions, recursive=args.recursive
        ):
            if not args.long:
                self._print(folder_name or file_version_info.file_name)
            elif folder_name is not None:
                self._print(FileVersionInfo.format_folder_ls_entry(folder_name))
            else:
                self._print(file_version_info.format_ls_entry())

        return 0


class MakeUrl(Command):
    """
    b2 make-url <fileId>

        Prints an URL that can be used to download the given file, if
        it is public.
    """

    REQUIRED = ['fileId']

    def run(self, args):
        self._print(self.api.get_download_url_for_fileid(args.fileId))
        return 0


class Sync(Command):
    """
    b2 sync [--delete] [--keepDays N] [--skipNewer] [--replaceNewer] \\
            [--compareVersions <option>] [--compareThreshold N] \\
            [--threads N] [--noProgress] [--dryRun ] [--allowEmptySource ] \\
            [--excludeRegex <regex> [--includeRegex <regex>]] \\
            [--excludeDirRegex <regex>] \\
            [--excludeAllSymlinks ] \\
            <source> <destination>

        Copies multiple files from source to destination.  Optionally
        deletes or hides destination files that the source does not have.

        Progress is displayed on the console unless '--noProgress' is
        specified.  A list of actions taken is always printed.

        Specify '--dryRun' to simulate the actions that would be taken.

        To allow sync to run when the source directory is empty, potentially
        deleting all files in a bucket, specify '--allowEmptySource'.
        The default is to fail when the specified source directory doesn't exist
        or is empty.  (This check only applies to version 1.0 and later.)

        Users with high-performance networks, or file sets with very small
        files, will benefit from multi-threaded uploads.  The default number
        of threads is 10.  Experiment with the --threads parameter if the
        default is not working well.

        Users with low-performance networks may benefit from reducing the
        number of threads.  Using just one thread will minimize the impact
        on other users of the network.

        Note that using multiple threads will usually be detrimental to
        the other users on your network.

        You can specify --excludeRegex to selectively ignore files that
        match the given pattern. Ignored files will not copy during
        the sync operation. The pattern is a regular expression
        that is tested against the full path of each file.

        You can specify --includeRegex to selectively override ignoring
        files that match the given --excludeRegex pattern by an
        --includeRegex pattern. Similarly to --excludeRegex, the pattern
        is a regular expression that is tested against the full path
        of each file.

        Note that --includeRegex cannot be used without --excludeRegex.

        You can specify --excludeAllSymlinks to skip symlinks when
        syncing from a local source.

        When a directory is excluded by using --excludeDirRegex, all of
        the files within it are excluded, even if they match an --includeRegex
        pattern.   This means that there is no need to look inside excluded
        directories, and you can exclude directories containing files for which
        you don't have read permission and avoid getting errors.

        The --excludeDirRegex is a regular expression that is tested against
        the full path of each directory.  The path being matched does not have
        a trailing '/', so don't include on in your regular expression.

        Multiple regex rules can be applied by supplying them as pipe
        delimited instructions. Note that the regex for this command
        is Python regex. Reference: https://docs.python.org/2/library/re.html.

        Regular expressions are considered a match if they match a substring
        starting at the first character.  ".*e" will match "hello".  This is
        not ideal, but we will maintain this behavior for compatibility.
        If you want to match the entire path, put a "$" at the end of the
        regex, such as ".*llo$".

        Files are considered to be the same if they have the same name
        and modification time.  This behaviour can be changed using the
        --compareVersions option.  Possible values are:
          'none':    Comparison using the file name only
          'modTime': Comparison using the modification time (default)
          'size':    Comparison using the file size
        A future enhancement may add the ability to compare the SHA1 checksum
        of the files.

        Fuzzy comparison of files based on modTime or size can be enabled by
        specifying the --compareThreshold option.  This will treat modTimes
        (in milliseconds) or sizes (in bytes) as the same if they are within
        the comparison threshold.  Files that match, within the threshold, will
        not be synced. Specifying --verbose and --dryRun can be useful to
        determine comparison value differences.

        One of the paths must be a local file path, and the other must be
        a B2 bucket path. Use "b2://<bucketName>/<prefix>" for B2 paths, e.g.
        "b2://my-bucket-name/a/path/prefix/".

        When a destination file is present that is not in the source, the
        default is to leave it there.  Specifying --delete means to delete
        destination files that are not in the source.

        When the destination is B2, you have the option of leaving older
        versions in place.  Specifying --keepDays will delete any older
        versions more than the given number of days old, based on the
        modification time of the file.  This option is not available when
        the destination is a local folder.

        Files at the source that have a newer modification time are always
        copied to the destination.  If the destination file is newer, the
        default is to report an error and stop.  But with --skipNewer set,
        those files will just be skipped.  With --replaceNewer set, the
        old file from the source will replace the newer one in the destination.

        To make the destination exactly match the source, use:
            b2 sync --delete --replaceNewer ... ...

        WARNING: Using '--delete' deletes files!  We recommend not using it.
        If you use --keepDays instead, you will have some time to recover your
        files if you discover they are missing on the source end.

        To make the destination match the source, but retain previous versions
        for 30 days:
            b2 sync --keepDays 30 --replaceNewer ... b2://...

        Example of sync being used with excludeRegex. This will ignore .DS_Store files
        and .Spotlight-V100 folders
            b2 sync -excludeRegex '(.*\.DS_Store)|(.*\.Spotlight-V100)' ... b2://...

        Requires capabilities: listFiles, readFiles (for downloading), writeFiles (for uploading)

    """

    OPTION_FLAGS = [
        'delete',
        'noProgress',
        'skipNewer',
        'replaceNewer',
        'dryRun',
        'allowEmptySource',
        'excludeAllSymlinks',
    ]
    OPTION_ARGS = ['keepDays', 'threads', 'compareVersions', 'compareThreshold']
    REQUIRED = ['source', 'destination']
    LIST_ARGS = ['excludeRegex', 'includeRegex', 'excludeDirRegex']
    ARG_PARSER = {'keepDays': float, 'threads': int, 'compareThreshold': int}

    def run(self, args):
        if args.includeRegex and not args.excludeRegex:
            logger.error('ConsoleTool \'includeRegex\' specified without \'excludeRegex\'')
            self._print_stderr(
                'ERROR: --includeRegex cannot be used without --excludeRegex at the same time'
            )
            return 1

        max_workers = args.threads or 10
        self.console_tool.api.set_thread_pool_size(max_workers)
        source = parse_sync_folder(args.source, self.console_tool.api)
        destination = parse_sync_folder(args.destination, self.console_tool.api)
        allow_empty_source = args.allowEmptySource or VERSION_0_COMPATIBILITY
        policies_manager = ScanPoliciesManager(
            exclude_dir_regexes=args.excludeDirRegex,
            exclude_file_regexes=args.excludeRegex,
            include_file_regexes=args.includeRegex,
            exclude_all_symlinks=args.excludeAllSymlinks,
        )
        sync_folders(
            source_folder=source,
            dest_folder=destination,
            args=args,
            now_millis=current_time_millis(),
            stdout=self.stdout,
            no_progress=args.noProgress,
            max_workers=max_workers,
            policies_manager=policies_manager,
            dry_run=args.dryRun,
            allow_empty_source=allow_empty_source
        )
        return 0


class TestHttp(Command):
    """
    b2 test-http

        PRIVATE.  Exercises the HTTP layer.
    """

    PRIVATE = True

    def run(self, args):
        test_http()
        return 0


class TestRawApi(Command):
    """
    b2 test-raw-api

        PRIVATE.  Exercises the B2RawApi class.
    """

    PRIVATE = True

    def run(self, args):
        return test_raw_api()


class TestUploadUrlConcurrency(Command):
    """
    b2 test-upload-url-concurrency

        PRIVATE.  Exercises the HTTP layer.
    """

    PRIVATE = True

    def run(self, args):
        test_upload_url_concurrency()
        return 0


class UpdateBucket(Command):
    """
    b2 update-bucket [--bucketInfo <json>] [--corsRules <json>] [--lifecycleRules <json>] <bucketName> [allPublic | allPrivate]

        Updates the bucketType of an existing bucket.  Prints the ID
        of the bucket updated.

        Optionally stores bucket info, CORS rules and lifecycle rules with the bucket.
        These can be given as JSON on the command line.

        Requires capability: writeBuckets
    """

    REQUIRED = ['bucketName', 'bucketType']

    OPTION_ARGS = ['bucketInfo', 'corsRules', 'lifecycleRules']

    ARG_PARSER = {'bucketInfo': json.loads, 'corsRules': json.loads, 'lifecycleRules': json.loads}

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        response = bucket.update(
            bucket_type=args.bucketType,
            bucket_info=args.bucketInfo,
            cors_rules=args.corsRules,
            lifecycle_rules=args.lifecycleRules
        )
        self._print(json.dumps(response, indent=4, sort_keys=True))
        return 0


class UploadFile(Command):
    """
    b2 upload-file [--sha1 <sha1sum>] [--contentType <contentType>] \\
            [--info <key>=<value>]* [--minPartSize N] \\
            [--noProgress] [--threads N] <bucketName> <localFilePath> <b2FileName>

        Uploads one file to the given bucket.  Uploads the contents
        of the local file, and assigns the given name to the B2 file.

        By default, upload_file will compute the sha1 checksum of the file
        to be uploaded.  But, if you already have it, you can provide it
        on the command line to save a little time.

        Content type is optional.  If not set, it will be set based on the
        file extension.

        By default, the file is broken into as many parts as possible to
        maximize upload parallelism and increase speed.  The minimum that
        B2 allows is 100MB.  Setting --minPartSize to a larger value will
        reduce the number of parts uploaded when uploading a large file.

        The maximum number of threads to use to upload parts of a large file
        is specified by '--threads'.  It has no effect on small files (under 200MB).
        Default is 10.

        If the 'tqdm' library is installed, progress bar is displayed
        on stderr.  Without it, simple text progress is printed.
        Use '--noProgress' to disable progress reporting.

        Each fileInfo is of the form "a=b".

        Requires capability: writeFiles
    """

    OPTION_FLAGS = ['noProgress', 'quiet']
    OPTION_ARGS = ['contentType', 'minPartSize', 'sha1', 'threads']
    LIST_ARGS = ['info']
    REQUIRED = ['bucketName', 'localFilePath', 'b2FileName']
    ARG_PARSER = {'minPartSize': int, 'threads': int}

    def run(self, args):
        file_infos = {}
        for info in args.info:
            parts = info.split('=', 1)
            if len(parts) == 1:
                raise BadFileInfo(info)
            file_infos[parts[0]] = parts[1]

        if SRC_LAST_MODIFIED_MILLIS not in file_infos:
            file_infos[SRC_LAST_MODIFIED_MILLIS] = str(
                int(os.path.getmtime(args.localFilePath) * 1000)
            )

        max_workers = args.threads or 10
        self.api.set_thread_pool_size(max_workers)

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
        response = file_info.as_dict()
        if not args.quiet:
            self._print("URL by file name: " + bucket.get_download_url(args.b2FileName))
            self._print(
                "URL by fileId: " + self.api.get_download_url_for_fileid(response['fileId'])
            )
        self._print(json.dumps(response, indent=2, sort_keys=True))
        return 0


class Version(Command):
    """
    b2 version

        Prints the version number of this tool.
    """

    def run(self, args):
        self._print('b2 command line tool, version', VERSION)
        return 0


class ConsoleTool(object):
    """
    Implements the commands available in the B2 command-line tool
    using the B2Api library.

    Uses the StoredAccountInfo object to keep account data in
    {B2_ACCOUNT_INFO_DEFAULT_FILE} between runs.
    """

    def __init__(self, b2_api, stdout, stderr):
        self.api = b2_api
        self.stdout = stdout
        self.stderr = stderr

        # a *magic* registry of commands
        self.command_name_to_class = dict(
            (mixed_case_to_hyphens(cls.__name__), cls) for cls in Command.__subclasses__()
        )

    def run_command(self, argv):
        signal.signal(signal.SIGINT, keyboard_interrupt_handler)

        if len(argv) < 2:
            logger.info('ConsoleTool error - insufficient arguments')
            return self._usage_and_fail()

        action = argv[1].replace('_', '-')
        arg_list = argv[2:]

        if action not in self.command_name_to_class:
            logger.info('ConsoleTool error - unknown command')
            return self._usage_and_fail()
        else:
            logger.info('Action: %s, arguments: %s', action, arg_list)

        command = self.command_name_to_class[action](self)
        args = command.parse_arg_list(arg_list)
        if args is None:
            logger.info('ConsoleTool \'args is None\' - printing usage')
            self._print_stderr(command.command_usage())
            return 1
        elif [args.logConfig, args.verbose, args.debugLogs].count(True) > 1:
            logger.info(
                'ConsoleTool More than one of \'args.logConfig\', \'args.verbose\', or \'args.debugLogs\' was specified'
            )
            self._print_stderr(
                'ERROR: Only one of --logConfig, --verbose, or --debugLogs can be used'
            )
            return 1

        self._setup_logging(args, command, argv)

        try:
            return command.run(args)
        except MissingAccountData as e:
            logger.exception('ConsoleTool missing account data error')
            self._print_stderr('ERROR: %s  Use: b2 authorize-account' % (str(e),))
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

    def _print(self, *args, **kwargs):
        print(*args, file=self.stdout, **kwargs)

    def _print_stderr(self, *args, **kwargs):
        print(*args, file=self.stderr, **kwargs)

    def _message_and_fail(self, message):
        """Prints a message, and exits with error status.
        """
        self._print_stderr(message)
        return 1

    def _usage_and_fail(self):
        """Prints a usage message, and exits with an error status.
        """
        self._print_stderr('This program provides command-line access to the B2 service.')
        self._print_stderr('')
        self._print_stderr('Usages:')
        self._print_stderr('')

        for name in sorted(six.iterkeys(self.command_name_to_class)):
            cls = self.command_name_to_class[name]
            if not cls.PRIVATE:
                line = '    ' + cls.summary_line()
                self._print_stderr(line)

        epilog = '''
        The environment variable {B2_ACCOUNT_INFO_ENV_VAR} specifies the sqlite
        file to use for caching authentication information.
        The default file to use is: {B2_ACCOUNT_INFO_DEFAULT_FILE}

        For more details on one command: b2 help <command>
        
        When authorizing with application keys, this tool requires that the key
        have the 'listBuckets' capability so that it can take the bucket names 
        you provide on the command line and translate them into bucket IDs for the 
        B2 Storage service.  Each different command may required additional 
        capabilities.  You can find the details for each command in the help for 
        that command.
        '''
        self._print_stderr(textwrap.dedent(epilog).format(**DOC_STRING_DATA))
        return 1

    def _print_download_info(self, download_dest):
        self._print('File name:   ', download_dest.file_name)
        self._print('File id:     ', download_dest.file_id)
        self._print('File size:   ', download_dest.content_length)
        self._print('Content type:', download_dest.content_type)
        self._print('Content sha1:', download_dest.content_sha1)
        for name in sorted(six.iterkeys(download_dest.file_info)):
            self._print('INFO', name + ':', download_dest.file_info[name])
        if download_dest.content_sha1 != 'none':
            self._print('checksum matches')
        return 0

    def _setup_logging(self, args, command, argv):
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
        logger.debug('locale is %s', locale.getdefaultlocale())
        logger.debug('filesystem encoding is %s', sys.getfilesystemencoding())

        if command.FORBID_LOGGING_ARGUMENTS:
            logger.info('starting command [%s] (arguments hidden)', command)
        else:
            logger.info('starting command [%s] with arguments: %s', command, argv)


def decode_sys_argv():
    """
    Returns the command-line arguments as unicode strings, decoding
    whatever format they are in.

    https://stackoverflow.com/questions/846850/read-unicode-characters-from-command-line-arguments-in-python-2-x-on-windows
    """
    if six.PY2:
        encoding = sys.getfilesystemencoding()
        return [arg.decode(encoding) for arg in sys.argv]
    return sys.argv


def main():
    info = SqliteAccountInfo()
    b2_api = B2Api(info, AuthInfoCache(info))
    ct = ConsoleTool(b2_api=b2_api, stdout=sys.stdout, stderr=sys.stderr)
    decoded_argv = decode_sys_argv()
    exit_status = ct.run_command(decoded_argv)
    logger.info('\\\\ %s %s %s //', SEPARATOR, ('exit=%s' % exit_status).center(8), SEPARATOR)

    # I haven't tracked down the root cause yet, but in Python 2.7, the futures
    # packages is hanging on exit sometimes, waiting for a thread to finish.
    # This happens when using sync to upload files.
    sys.stdout.flush()
    sys.stderr.flush()

    logging.shutdown()

    os._exit(exit_status)
    # sys.exit(exit_status)
