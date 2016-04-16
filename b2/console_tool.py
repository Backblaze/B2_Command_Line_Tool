######################################################################
#
# File: b2/console_tool.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import absolute_import, print_function

import getpass
import json
import os
import signal
import sys
import textwrap

import six

from .account_info import (StoredAccountInfo)
from .api import (B2Api)
from .b2http import (test_http)
from .cache import (AuthInfoCache)
from .download_dest import (DownloadDestLocalFile)
from .exception import (B2Error, BadFileInfo, MissingAccountData)
from .file_version import (FileVersionInfo)
from .parse_args import parse_arg_list
from .progress import (make_progress_listener, DoNothingProgressListener)
from .raw_api import (test_raw_api)
from .sync import parse_sync_folder, sync_folders
from .utils import (current_time_millis, set_shutting_down)
from .version import (VERSION)


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


def mixed_case_to_underscores(s):
    return s[0].lower() + ''.join(c if c.islower() else '_' + c.lower() for c in s[1:])


class Command(object):
    """
    Base class for commands.  Has basic argument parsing and printing.
    """

    # Option flags.  A name here that looks like "fast" can be set to
    # True with a command line option "--fast".  All option flags
    # default to False.
    OPTION_FLAGS = []

    # Explicit arguments.  These always come before the positional arguments.
    # Putting "color" here means you can put something like "--color blue" on
    # the command line, and args.color will be set to "blue".  These all
    # default to None.
    OPTION_ARGS = []

    # Optional arguments that you can specify zero or more times and the
    # values are collected into a list.  Default is []
    LIST_ARGS = []

    # Required positional arguments.  Never None.
    REQUIRED = []

    # Optional positional arguments.  Default to None if not present.
    OPTIONAL = []

    # Set to True for commands that should not be listed in the summary.
    PRIVATE = False

    # Parsers for each argument.  Each should be a function that
    # takes a string and returns the vaule.
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
        return textwrap.dedent(cls.__doc__).split('\n')[1]

    @classmethod
    def command_usage(cls):
        """
        Returns the doc string for this class.
        """
        return textwrap.dedent(cls.__doc__)

    def parse_arg_list(self, arg_list):
        return parse_arg_list(
            arg_list,
            option_flags=self.OPTION_FLAGS,
            option_args=self.OPTION_ARGS,
            list_args=self.LIST_ARGS,
            required=self.REQUIRED,
            optional=self.OPTIONAL,
            arg_parser=self.ARG_PARSER
        )

    def _print(self, *args, **kwargs):
        print(*args, file=self.stdout, **kwargs)

    def _print_stderr(self, *args, **kwargs):
        print(*args, file=self.stderr, **kwargs)


class AuthorizeAccount(Command):
    """
    b2 authorize_account [<accountId>] [<applicationKey>]

        Prompts for Backblaze accountID and applicationKey (unless they are given
        on the command line).

        The account ID is a 12-digit hex number that you can get from
        your account page on backblaze.com.

        The application key is a 40-digit hex number that you can get from
        your account page on backblaze.com.

        Stores an account auth token in ~/.b2_account_info
    """

    OPTION_FLAGS = ['dev', 'staging']  # undocumented

    OPTIONAL = ['accountId', 'applicationKey']

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
            return 0
        except B2Error as e:
            self._print_stderr('ERROR: unable to authorize account: ' + str(e))
            return 1


class CancelAllUnfinishedLargeFiles(Command):
    """
    b2 cancel_all_unfinished_large_files <bucketName>

        Lists all large files that have been started but not
        finsished and cancels them.  Any parts that have been
        uploaded will be deleted.
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
    b2 cancel_large_file <fileId>
    """

    REQUIRED = ['fileId']

    def run(self, args):
        self.api.cancel_large_file(args.fileId)
        self._print(args.fileId, 'canceled')
        return 0


class ClearAccount(Command):
    """
    b2 clear_account

        Erases everything in ~/.b2_account_info
    """

    def run(self, args):
        self.api.account_info.clear()
        return 0


class CreateBucket(Command):
    """
    b2 create_bucket <bucketName> [allPublic | allPrivate]

        Creates a new bucket.  Prints the ID of the bucket created.
    """

    REQUIRED = ['bucketName', 'bucketType']

    def run(self, args):
        self._print(self.api.create_bucket(args.bucketName, args.bucketType).id_)
        return 0


class DeleteBucket(Command):
    """
    b2 delete_bucket <bucketName>

        Deletes the bucket with the given name.
    """

    REQUIRED = ['bucketName']

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        response = self.api.delete_bucket(bucket)
        self._print(json.dumps(response, indent=4, sort_keys=True))
        return 0


class DeleteFileVersion(Command):
    """
    b2 delete_file_version <fileName> <fileId>

        Permanently and irrevocably deletes one version of a file.
    """

    REQUIRED = ['fileName', 'fileId']

    def run(self, args):
        file_info = self.api.delete_file_version(args.fileId, args.fileName)
        response = file_info.as_dict()
        self._print(json.dumps(response, indent=2, sort_keys=True))
        return 0


class DownloadFileById(Command):
    """
    b2 download_file_by_id [--noProgress] <fileId> <localFileName>

        Downloads the given file, and stores it in the given local file.

        If the 'tqdm' library is installed, progress bar is displayed
        on stderr.  Without it, simple text progress is printed.
        Use '--no-progress' to disable progress reporting.
    """

    OPTION_FLAGS = ['noProgress']
    REQUIRED = ['fileId', 'localFileName']

    def run(self, args):
        progress_listener = make_progress_listener(args.localFileName, args.noProgress)
        download_dest = DownloadDestLocalFile(args.localFileName, progress_listener)
        self.api.download_file_by_id(args.fileId, download_dest)
        self.console_tool._print_download_info(download_dest)
        return 0


class DownloadFileByName(Command):
    """
    b2 download_file_by_name [--noProgress] <bucketName> <fileName> <localFileName>

        Downloads the given file, and stores it in the given local file.
    """

    OPTION_FLAGS = ['noProgress']
    REQUIRED = ['bucketName', 'b2FileName', 'localFileName']

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        progress_listener = make_progress_listener(args.localFileName, args.noProgress)
        download_dest = DownloadDestLocalFile(args.localFileName, progress_listener)
        bucket.download_file_by_name(args.b2FileName, download_dest)
        self.console_tool._print_download_info(download_dest)
        return 0


class GetFileInfo(Command):
    """
    b2 get_file_info <fileId>

        Prints all of the information about the file, but not its contents.
    """

    REQUIRED = ['fileId']

    def run(self, args):
        response = self.api.get_file_info(args.fileId)
        self._print(json.dumps(response, indent=2, sort_keys=True))
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
        self._print(textwrap.dedent(command_cls.__doc__))
        return 1


class HideFile(Command):
    """
    b2 hide_file <bucketName> <fileName>

        Uploads a new, hidden, version of the given file.
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
    b2 list_buckets

        Lists all of the buckets in the current account.

        Output lines list the bucket ID, bucket type, and bucket name,
        and look like this:

            98c960fd1cb4390c5e0f0519  allPublic   my-bucket
    """

    def run(self, args):
        for b in self.api.list_buckets():
            self._print('%s  %-10s  %s' % (b.id_, b.type_, b.name))
        return 0


class ListFileVersions(Command):
    """
    b2 list_file_versions <bucketName> [<startFileName>] [<startFileId>] [<maxToShow>]

        Lists the names of the files in a bucket, starting at the
        given point.  This is a low-level operation that reports the
        raw JSON returned from the service.  'b2 ls' provides a higher-
        level view.
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

    b2 list_file_names <bucketName> [<startFileName>] [<maxToShow>]

        Lists the names of the files in a bucket, starting at the
        given point.
    """

    REQUIRED = ['bucketName']

    OPTIONAL = ['startFileName', 'maxToShow']

    ARG_PARSER = {'maxToShow': int}

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        response = bucket.list_file_names(args.startFileName, args.maxToShow)
        self._print(json.dumps(response, indent=2, sort_keys=True))
        return 0


class ListParts(Command):
    """
    b2 list_parts <largeFileId>

        Lists all of the parts that have been uploaded for the given
        large file, which must be a file that was started but not
        finished or canceled.
    """

    REQUIRED = ['largeFileId']

    def run(self, args):
        for part in self.api.list_parts(args.largeFileId):
            self._print('%5d  %9d  %s' % (part.part_number, part.content_length, part.content_sha1))
        return 0


class ListUnfinishedLargeFiles(Command):
    """
    b2 list_unfinished_large_files <bucketName>

        Lists all of the large files in the bucket that were started,
        but not finished or canceled.

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
    b2 ls [--long] [--versions] <bucketName> [<folderName>]

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
    """

    OPTION_FLAGS = ['long', 'versions']

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
        for file_version_info, folder_name in bucket.ls(prefix, args.versions):
            if not args.long:
                self._print(folder_name or file_version_info.file_name)
            elif folder_name is not None:
                self._print(FileVersionInfo.format_folder_ls_entry(folder_name))
            else:
                self._print(file_version_info.format_ls_entry())

        return 0


class MakeUrl(Command):
    """
    b2 make_url <fileId>

        Prints an URL that can be used to download the given file, if
        it is public.
    """

    REQUIRED = ['fileId']

    def run(self, args):
        self._print(self.api.get_download_url_for_fileid(args.fileId))
        return 0


class NewSync(Command):
    """
    b2 sync [--delete] [--keepDays NNN] [--skipNewer] [--replaceNewer] <source> <destination>

        Copies multiple files from source to destination.  Optionally
        deletes or hides destination files that the source does not have.

        Files are considered to be the same if they have the same name
        and modification time.  A future enhancement may add the ability
        to compare the SHA1 checksum of the files.

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

        To make the destination match the source, but retain previous versions
        for 30 days:
            b2 sync --keepDays 30 --replaceNewer ... b2://...

    """

    PRIVATE = True
    OPTION_FLAGS = ['delete', 'skipNewer', 'replaceNewer']
    OPTION_ARGS = ['keepDays']
    REQUIRED = ['source', 'destination']
    ARG_PARSER = {
        'keepDays': int,
        'source': parse_sync_folder,
        'destination': parse_sync_folder
    }  # yapf: disable

    def run(self, args):
        sync_folders(args.source, args.destination, args, current_time_millis())



class Sync(Command):
    """
    b2 sync [--delete] [--hide] <source> <destination>

        UNDER DEVELOPMENT -- there may be changes coming to this command

        Uploads or downloads multiple files from source to destination.
        One of the paths must be a local file path, and the other must be
        a B2 bucket path. Use "b2:<bucketName>/<prefix>" for B2 paths, e.g.
        "b2:my-bucket-name/a/path/prefix/".

        If the --delete or --hide flags are specified, destination files
        are deleted or hidden if not present in the source path. Note that
        files are matched only by name and size.
    """

    OPTION_FLAGS = ['delete', 'hide']

    REQUIRED = ['source', 'destination']

    def run(self, args):
        # TODO: break up this method.  it's too long
        # maybe move into its own class?
        dst_is_b2 = args.destination.startswith('b2:')
        local_path = args.source if dst_is_b2 else args.destination
        b2_path = args.destination if dst_is_b2 else args.source
        is_b2_src = b2_path == args.source
        if local_path.startswith('b2:') or not b2_path.startswith('b2:'):
            return self.console_tool._message_and_fail(
                'ERROR: one of the paths must be a "b2:<bucket>" URI'
            )
        elif not os.path.exists(local_path):
            return self.console_tool._message_and_fail(
                'ERROR: local path doesn\'t exist: ' + local_path
            )
        bucket_name = b2_path[3:].split('/')[0]
        bucket_prefix = '/'.join(b2_path[3:].split('/')[1:])
        if bucket_prefix and not bucket_prefix.endswith('/'):
            bucket_prefix += '/'

        bucket = self.api.get_bucket_by_name(bucket_name)

        # Find all matching files in B2
        remote_files = {}
        for file_info, __ in bucket.ls(
            folder_to_list=bucket_prefix,
            max_entries=1000,
            recursive=True
        ):
            name = file_info.file_name
            after_prefix = name[len(bucket_prefix):]
            remote_files[after_prefix] = {
                'fileName': after_prefix,
                'fileId': file_info.id_,
                'size': file_info.size,
            }

        # Find all matching local files
        local_files = {}
        for dirpath, dirnames, filenames in os.walk(local_path):
            for filename in filenames:
                abspath = os.path.join(dirpath, filename)
                relpath = os.path.relpath(abspath, local_path)
                local_files[relpath] = {'fileName': relpath, 'size': os.path.getsize(abspath)}

        # Process differences
        local_fileset = set(local_files.keys())
        remote_fileset = set(remote_files.keys())
        for filename in local_fileset | remote_fileset:
            filepath = os.path.join(local_path, filename)
            dirpath = os.path.dirname(filepath)
            b2_path = local_path_to_b2_path(os.path.join(bucket_prefix, filename))
            local_file = local_files.get(filename)
            remote_file = remote_files.get(filename)
            is_match = local_file and remote_file and local_file['size'] == remote_file['size']
            if is_b2_src and remote_file and not is_match:
                self._print("+ %s" % filename)
                if not os.path.exists(dirpath):
                    os.makedirs(dirpath)
                download_dest = DownloadDestLocalFile(filepath, DoNothingProgressListener())
                self.api.download_file_by_id(remote_file['fileId'], download_dest)
            elif is_b2_src and not remote_file and args.delete:
                self._print("- %s" % filename)
                os.remove(filepath)
            elif not is_b2_src and local_file and not is_match:
                self._print("+ %s" % filename)
                file_infos = {
                    'src_last_modified_millis': str(int(os.path.getmtime(filepath) * 1000))
                }
                bucket.upload_local_file(filepath, b2_path, file_infos=file_infos)
            elif not is_b2_src and not local_file and args.delete:
                self._print("- %s" % filename)
                self.api.delete_file_version(remote_file['fileId'], b2_path)
            elif not is_b2_src and not local_file and args.hide:
                self._print(". %s" % filename)
                bucket.hide_file(b2_path)

        # Remove empty local directories
        if is_b2_src and args.delete:
            for dirpath, dirnames, filenames in os.walk(local_path, topdown=False):
                for name in dirnames:
                    try:
                        os.rmdir(os.path.join(dirpath, name))
                    except Exception:
                        pass

        return 0


class TestHttp(Command):
    """
    b2 test_http

        PRIVATE.  Exercises the HTTP layer.
    """

    PRIVATE = True

    def run(self, args):
        test_http()
        return 0


class TestRawApi(Command):
    """
    b2 test_raw_api

        PRIVATE.  Exercises the B2RawApi class.
    """

    PRIVATE = True

    def run(self, args):
        test_raw_api()
        return 0


class UpdateBucket(Command):
    """
    b2 update_bucket <bucketName> [allPublic | allPrivate]

        Updates the bucketType of an existing bucket.  Prints the ID
        of the bucket updated.
    """

    REQUIRED = ['bucketName', 'bucketType']

    def run(self, args):
        bucket = self.api.get_bucket_by_name(args.bucketName)
        response = bucket.set_type(args.bucketType)
        self._print(json.dumps(response, indent=4, sort_keys=True))
        return 0


class UploadFile(Command):
    """
    b2 upload_file [--sha1 <sha1sum>] [--contentType <contentType>] [--info <key>=<value>]* \
            [--noProgress] <bucketName> <localFilePath> <b2FileName>

        Uploads one file to the given bucket.  Uploads the contents
        of the local file, and assigns the given name to the B2 file.

        By default, upload_file will compute the sha1 checksum of the file
        to be uploaded.  But, if you already have it, you can provide it
        on the command line to save a little time.

        Content type is optional.  If not set, it will be set based on the
        file extension.

        If the 'tqdm' library is installed, progress bar is displayed
        on stderr.  Without it, simple text progress is printed.
        Use '--no-progress' to disable progress reporting.

        Each fileInfo is of the form "a=b".
    """

    OPTION_FLAGS = ['noProgress', 'quiet']

    OPTION_ARGS = ['sha1', 'contentType']

    LIST_ARGS = ['info']

    REQUIRED = ['bucketName', 'localFilePath', 'b2FileName']

    def run(self, args):

        file_infos = {}
        for info in args.info:
            parts = info.split('=', 1)
            if len(parts) == 1:
                raise BadFileInfo(info)
            file_infos[parts[0]] = parts[1]

        bucket = self.api.get_bucket_by_name(args.bucketName)
        with make_progress_listener(args.localFilePath, args.noProgress) as progress_listener:
            file_info = bucket.upload_local_file(
                local_file=args.localFilePath,
                file_name=args.b2FileName,
                content_type=args.contentType,
                file_infos=file_infos,
                sha1_sum=args.sha1,
                progress_listener=progress_listener,
            )
        response = file_info.as_dict()
        if not args.quiet:
            self._print("URL by file name: " + bucket.get_download_url(args.b2FileName))
            self._print(
                "URL by fileId: " + self.api.get_download_url_for_fileid(
                    response[
                        'fileId'
                    ]
                )
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
    ~/.b2_account_info between runs.
    """

    def __init__(self, b2_api, stdout, stderr):
        self.api = b2_api
        self.stdout = stdout
        self.stderr = stderr
        self.command_name_to_class = dict(
            (mixed_case_to_underscores(cls.__name__), cls) for cls in Command.__subclasses__()
        )

    def run_command(self, argv):
        signal.signal(signal.SIGINT, keyboard_interrupt_handler)

        if len(argv) < 2:
            return self._usage_and_fail()

        action = argv[1]
        arg_list = argv[2:]

        if action not in self.command_name_to_class:
            return self._usage_and_fail()

        command = self.command_name_to_class[action](self)
        args = command.parse_arg_list(arg_list)
        if args is None:
            self._print_stderr(command.command_usage())
            return 1

        try:
            return command.run(args)
        except MissingAccountData:
            self._print_stderr('ERROR: Missing account.  Use: b2 authorize_account')
            return 1
        except B2Error as e:
            self._print_stderr('ERROR: %s' % (str(e),))
            return 1
        except KeyboardInterrupt:
            self._print('\ninterrupted')

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

        self._print_stderr('')
        self._print_stderr('For more details on one command: b2 help <command>')
        self._print_stderr('')
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
    info = StoredAccountInfo()
    b2_api = B2Api(info, AuthInfoCache(info))
    ct = ConsoleTool(b2_api=b2_api, stdout=sys.stdout, stderr=sys.stderr)
    decoded_argv = decode_sys_argv()
    exit_status = ct.run_command(decoded_argv)
    sys.exit(exit_status)
