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
import sys

import six

from .account_info import (StoredAccountInfo)
from .api import (B2Api)
from .b2http import (test_http)
from .cache import (AuthInfoCache)
from .download_dest import (DownloadDestLocalFile)
from .exception import (B2Error, BadFileInfo, MissingAccountData)
from .file_version import (FileVersionInfo)
from .progress import (make_progress_listener, DoNothingProgressListener)
from .raw_api import (test_raw_api)
from .version import (VERSION)

USAGE = """This program provides command-line access to the B2 service.

Usages:

    b2 authorize_account [accountId] [applicationKey]

        Prompts for Backblaze accountID and applicationKey (unless they are given
        on the command line).

        The account ID is a 12-digit hex number that you can get from
        your account page on backblaze.com.

        The application key is a 40-digit hex number that you can get from
        your account page on backblaze.com.

        Stores an account auth token in ~/.b2_account_info.  This can be overridden using the
        B2_ACCOUNT_INFO environment variable.

    b2 cancel_all_unfinished_large_files [bucketName]

        Cancels ALL unfinished large files in the bucket.

    b2 cancel_large_file [fileId]

        Deletes all of the parts that have been uploaded for the
        file, as well as the file itself.

    b2 clear_account

        Erases everything in ~/.b2_account_info

    b2 create_bucket <bucketName> <bucketType>

        Creates a new bucket.  Prints the ID of the bucket created.

    b2 delete_bucket <bucketName>

        Deletes the bucket with the given name.

    b2 delete_file_version <fileName> <fileId>

        Permanently and irrevocably deletes one version of a file.

    b2 download_file_by_id <fileId> <localFileName>

        Downloads the given file, and stores it in the given local file.

    b2 download_file_by_name <bucketName> <fileName> <localFileName>

        Downloads the given file, and stores it in the given local file.

    b2 get_file_info <fileId>

        Prints all of the information about the file, but not its contents.

    b2 hide_file <bucketName> <fileName>

        Uploads a new, hidden, version of the given file.

    b2 list_buckets

        Lists all of the buckets in the current account.

    b2 list_file_names <bucketName> [<startingName>] [<numberToShow>]

        Lists the names of the files in a bucket, starting at the
        given point.

    b2 list_file_versions <bucketName> [<startingName>] [<startingFileId>] [<numberToShow>]

        Lists the names of the files in a bucket, starting at the
        given point.

    b2 list_unfinished_large_files <bucketName>

        Lists all of the large files in the bucket that were started,
        but not finished or canceled.

    b2 list_parts <largeFileId>

        Lists all of the parts that have been uploaded for the given
        large file, which must be a file that was started but not
        finished or canceled.

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

    b2 make_url <fileId>

        Prints an URL that can be used to download the given file, if
        it is public.

    b2 sync [--delete] [--hide] <source> <destination>

        UNDER DEVELOPMENT -- there may be changes coming to this command

        Uploads or downloads multiple files from source to destination.
        One of the paths must be a local file path, and the other must be
        a B2 bucket path. Use "b2:<bucketName>/<prefix>" for B2 paths, e.g.
        "b2:my-bucket-name/a/path/prefix/".

        If the --delete or --hide flags are specified, destination files
        are deleted or hidden if not present in the source path. Note that
        files are matched only by name and size.

    b2 update_bucket <bucketName> <bucketType>

        Updates the bucketType of an existing bucket.  Prints the ID
        of the bucket updated.

    b2 upload_file [--sha1 <sha1sum>] [--contentType <contentType>] [--info <key>=<value>]* <bucketName> <localFilePath> <b2FileName>

        Uploads one file to the given bucket.  Uploads the contents
        of the local file, and assigns the given name to the B2 file.

        By default, upload_file will compute the sha1 checksum of the file
        to be uploaded.  But, if you already have it, you can provide it
        on the command line to save a little time.

        Content type is optional.  If not set, it will be set based on the
        file extension.

        If `tqdm` library is installed, progress bar is displayed on stderr.
        (use pip install tqdm to install it)

        Each fileInfo is of the form "a=b".

    b2 version

        Echos the version number of this program.
"""


def local_path_to_b2_path(path):
    """
    Ensures that the separator in the path is '/', not '\'.

    :param path: A path from the local file system
    :return: A path that uses '/' as the separator.
    """
    return path.replace(os.path.sep, '/')


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

    def run_command(self, argv):
        if len(argv) < 2:
            return self._usage_and_fail()

        action = argv[1]
        args = argv[2:]

        try:
            if action == 'authorize_account':
                return self.authorize_account(args)
            elif action == 'cancel_all_unfinished_large_files':
                return self.cancel_all_unfinished_large_files(args)
            elif action == 'cancel_large_file':
                return self.cancel_large_file(args)
            elif action == 'clear_account':
                return self.clear_account(args)
            elif action == 'create_bucket':
                return self.create_bucket(args)
            elif action == 'delete_bucket':
                return self.delete_bucket(args)
            elif action == 'delete_file_version':
                return self.delete_file_version(args)
            elif action == 'download_file_by_id':
                return self.download_file_by_id(args)
            elif action == 'download_file_by_name':
                return self.download_file_by_name(args)
            elif action == 'get_file_info':
                return self.get_file_info(args)
            elif action == 'hide_file':
                return self.hide_file(args)
            elif action == 'list_buckets':
                return self.list_buckets(args)
            elif action == 'list_file_names':
                return self.list_file_names(args)
            elif action == 'list_file_versions':
                return self.list_file_versions(args)
            elif action == 'list_parts':
                return self.list_parts(args)
            elif action == 'list_unfinished_large_files':
                return self.list_unfinished_large_files(args)
            elif action == 'ls':
                return self.ls(args)
            elif action == 'make_url':
                return self.make_url(args)
            elif action == 'six_version':  # temporary for testing mac install
                print(six.__version__)
                return 0
            elif action == 'sync':
                return self.sync(args)
            elif action == 'test_http':
                return self.test_http(args)
            elif action == 'test_raw_api':
                return self.test_raw_api(args)
            elif action == 'update_bucket':
                return self.update_bucket(args)
            elif action == 'upload_file':
                return self.upload_file(args)
            elif action == 'version':
                return self.version()
            else:
                return self._usage_and_fail()
        except MissingAccountData:
            self._print_stderr('ERROR: Missing account.  Use: b2 authorize_account')
            return 1
        except B2Error as e:
            self._print_stderr('ERROR: %s' % (str(e),))
            return 1

    def _message_and_fail(self, message):
        """Prints a message, and exits with error status.
        """
        self._print_stderr(message)
        return 1

    def _usage_and_fail(self):
        """Prints a usage message, and exits with an error status.
        """
        return self._message_and_fail(USAGE)

    def _print(self, *args):
        print(*args, file=self.stdout)

    def _print_stderr(self, *args):
        print(*args, file=self.stderr)

    # testing

    def test_raw_api(self, args):
        if len(args) != 0:
            return self._usage_and_fail()
        test_raw_api()

    def test_http(self, args):
        if len(args) != 0:
            return self._usage_and_fail()
        test_http()

    # bucket

    def cancel_all_unfinished_large_files(self, args):
        if len(args) != 1:
            return self._usage_and_fail()
        bucket_name = args[0]
        bucket = self.api.get_bucket_by_name(bucket_name)
        for file_version in bucket.list_unfinished_large_files():
            bucket.cancel_large_file(file_version.file_id)
            self._print(file_version.file_id, 'canceled')
        return 0

    def cancel_large_file(self, args):
        if len(args) != 1:
            return self._usage_and_fail()
        file_id = args[0]
        self.api.cancel_large_file(file_id)
        self._print(file_id, 'canceled')
        return 0

    def create_bucket(self, args):
        if len(args) != 2:
            return self._usage_and_fail()
        bucket_name = args[0]
        bucket_type = args[1]
        self._print(self.api.create_bucket(bucket_name, bucket_type).id_)
        return 0

    def delete_bucket(self, args):
        if len(args) != 1:
            return self._usage_and_fail()
        bucket_name = args[0]

        bucket = self.api.get_bucket_by_name(bucket_name)
        response = self.api.delete_bucket(bucket)

        self._print(json.dumps(response, indent=4, sort_keys=True))
        return 0

    def update_bucket(self, args):
        if len(args) != 2:
            return self._usage_and_fail()
        bucket_name = args[0]
        bucket_type = args[1]

        bucket = self.api.get_bucket_by_name(bucket_name)
        response = bucket.set_type(bucket_type)

        self._print(json.dumps(response, indent=4, sort_keys=True))
        return 0

    def list_buckets(self, args):
        if len(args) != 0:
            return self._usage_and_fail()

        for b in self.api.list_buckets():
            self._print('%s  %-10s  %s' % (b.id_, b.type_, b.name))
        return 0

    # file

    def delete_file_version(self, args):
        if len(args) != 2:
            return self._usage_and_fail()
        file_name = args[0]
        file_id = args[1]

        file_info = self.api.delete_file_version(file_id, file_name)

        response = file_info.as_dict()

        self._print(json.dumps(response, indent=2, sort_keys=True))
        return 0

    def download_file_by_id(self, args):
        if len(args) != 2:
            return self._usage_and_fail()
        file_id = args[0]
        local_file_name = args[1]

        progress_listener = make_progress_listener(local_file_name, False)
        download_dest = DownloadDestLocalFile(local_file_name, progress_listener)
        self.api.download_file_by_id(file_id, download_dest)
        self._print_download_info(download_dest)
        return 0

    def download_file_by_name(self, args):
        if len(args) != 3:
            return self._usage_and_fail()
        bucket_name = args[0]
        file_name = args[1]
        local_file_name = args[2]

        bucket = self.api.get_bucket_by_name(bucket_name)
        progress_listener = make_progress_listener(local_file_name, False)
        download_dest = DownloadDestLocalFile(local_file_name, progress_listener)
        bucket.download_file_by_name(file_name, download_dest)
        self._print_download_info(download_dest)
        return 0

    def _print_download_info(self, download_dest):
        self._print('File name:   ', download_dest.file_name)
        self._print('File id:     ', download_dest.file_id)
        self._print('File size:   ', download_dest.content_length)
        self._print('Content type:', download_dest.content_type)
        self._print('Content sha1:', download_dest.content_sha1)
        for name in sorted(six.iterkeys(download_dest.file_info)):
            self._print('INFO', name + ':', download_dest.file_info[name])
        self._print('checksum matches')
        return 0

    def get_file_info(self, args):
        if len(args) != 1:
            return self._usage_and_fail()
        file_id = args[0]

        response = self.api.get_file_info(file_id)

        self._print(json.dumps(response, indent=2, sort_keys=True))
        return 0

    def hide_file(self, args):
        if len(args) != 2:
            return self._usage_and_fail()
        bucket_name = args[0]
        file_name = args[1]

        bucket = self.api.get_bucket_by_name(bucket_name)
        file_info = bucket.hide_file(file_name)

        response = file_info.as_dict()

        self._print(json.dumps(response, indent=2, sort_keys=True))
        return 0

    def list_parts(self, args):
        if len(args) != 1:
            return self._usage_and_fail()
        file_id = args[0]
        for part in self.api.list_parts(file_id):
            self._print('%5d  %9d  %s' % (part.part_number, part.content_length, part.content_sha1))
        return 0

    def list_unfinished_large_files(self, args):
        if len(args) != 1:
            return self._usage_and_fail()
        bucket_name = args[0]
        bucket = self.api.get_bucket_by_name(bucket_name)
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

    def upload_file(self, args):
        content_type = None
        file_infos = {}
        sha1_sum = None
        quiet = False

        while 0 < len(args) and args[0][0] == '-':
            option = args[0]
            if option == '--sha1':
                if len(args) < 2:
                    return self._usage_and_fail()
                sha1_sum = args[1]
                args = args[2:]
            elif option == '--contentType':
                if len(args) < 2:
                    return self._usage_and_fail()
                content_type = args[1]
                args = args[2:]
            elif option == '--info':
                if len(args) < 2:
                    return self._usage_and_fail()
                parts = args[1].split('=', 1)
                if len(parts) == 1:
                    raise BadFileInfo(args[1])
                file_infos[parts[0]] = parts[1]
                args = args[2:]
            elif option == '--quiet':
                quiet = True
                args = args[1:]
            else:
                return self._usage_and_fail()

        if len(args) != 3:
            return self._usage_and_fail()
        bucket_name = args[0]
        local_file = args[1]
        remote_file = local_path_to_b2_path(args[2])

        bucket = self.api.get_bucket_by_name(bucket_name)
        with make_progress_listener(local_file, quiet) as progress_listener:
            file_info = bucket.upload_local_file(
                local_file=local_file,
                file_name=remote_file,
                content_type=content_type,
                file_infos=file_infos,
                sha1_sum=sha1_sum,
                progress_listener=progress_listener,
            )
        response = file_info.as_dict()
        if not quiet:
            self._print("URL by file name: " + bucket.get_download_url(remote_file))
            self._print(
                "URL by fileId: " + self.api.get_download_url_for_fileid(
                    response[
                        'fileId'
                    ]
                )
            )
        self._print(json.dumps(response, indent=2, sort_keys=True))
        return 0

    # account

    def authorize_account(self, args):
        realm = 'production'
        while 0 < len(args) and args[0][0] == '-':
            realm = args[0][2:]
            args = args[1:]
            if realm in self.api.account_info.REALM_URLS:
                break
            else:
                self._print('ERROR: unknown option', realm)
                return self._usage_and_fail()

        url = self.api.account_info.REALM_URLS[realm]
        self._print('Using %s' % url)

        if 2 < len(args):
            return self._usage_and_fail()
        if 0 < len(args):
            account_id = args[0]
        else:
            account_id = six.moves.input('Backblaze account ID: ')

        if 1 < len(args):
            application_key = args[1]
        else:
            application_key = getpass.getpass('Backblaze application key: ')

        try:
            self.api.authorize_account(realm, account_id, application_key)
            return 0
        except B2Error as e:
            self._print_stderr('ERROR: unable to authorize account: ' + str(e))
            return 1

    def clear_account(self, args):
        if len(args) != 0:
            return self._usage_and_fail()
        self.api.account_info.clear()
        return 0

    # listing

    def list_file_names(self, args):
        if len(args) < 1 or 3 < len(args):
            return self._usage_and_fail()

        bucket_name = args[0]
        if 2 <= len(args):
            first_file_name = args[1]
        else:
            first_file_name = None
        if 3 <= len(args):
            count = int(args[2])
        else:
            count = 100

        bucket = self.api.get_bucket_by_name(bucket_name)

        response = bucket.list_file_names(first_file_name, count)
        self._print(json.dumps(response, indent=2, sort_keys=True))
        return 0

    def list_file_versions(self, args):
        if len(args) < 1 or 4 < len(args):
            return self._usage_and_fail()

        bucket_name = args[0]
        if 2 <= len(args):
            first_file_name = args[1]
        else:
            first_file_name = None
        if 3 <= len(args):
            first_file_id = args[2]
        else:
            first_file_id = None
        if 4 <= len(args):
            count = int(args[3])
        else:
            count = 100

        bucket = self.api.get_bucket_by_name(bucket_name)

        response = bucket.list_file_versions(first_file_name, first_file_id, count)
        self._print(json.dumps(response, indent=2, sort_keys=True))
        return 0

    def ls(self, args):
        long_format = False
        show_versions = False
        while len(args) != 0 and args[0][0] == '-':
            option = args[0]
            args = args[1:]
            if option == '--long':
                long_format = True
            elif option == '--versions':
                show_versions = True
            else:
                self._print('Unknown option:', option)
                return self._usage_and_fail()
        if len(args) < 1 or len(args) > 2:
            return self._usage_and_fail()
        bucket_name = args[0]
        if len(args) == 1:
            prefix = ""
        else:
            prefix = args[1]
            if not prefix.endswith('/'):
                prefix += '/'

        bucket = self.api.get_bucket_by_name(bucket_name)
        for file_version_info, folder_name in bucket.ls(prefix, show_versions):
            if not long_format:
                self._print(folder_name or file_version_info.file_name)
            elif folder_name is not None:
                self._print(FileVersionInfo.format_folder_ls_entry(folder_name))
            else:
                self._print(file_version_info.format_ls_entry())

        return 0

    # other

    def make_url(self, args):
        if len(args) != 1:
            return self._usage_and_fail()

        file_id = args[0]

        self._print(self.api.get_download_url_for_fileid(file_id))
        return 0

    def sync(self, args):
        # TODO: break up this method.  it's too long
        # maybe move into its own class?
        options = {'delete': False, 'hide': False}
        while args and args[0][0] == '-':
            option = args[0]
            args = args[1:]
            if option == '--delete':
                options['delete'] = True
            elif option == '--hide':
                options['hide'] = True
            else:
                return self._message_and_fail('ERROR: unknown option: ' + option)
        if len(args) != 2:
            return self._usage_and_fail()
        src = args[0]
        dst = args[1]
        local_path = src if dst.startswith('b2:') else dst
        b2_path = dst if dst.startswith('b2:') else src
        is_b2_src = b2_path == src
        if local_path.startswith('b2:') or not b2_path.startswith('b2:'):
            return self._message_and_fail('ERROR: one of the paths must be a "b2:<bucket>" URI')
        elif not os.path.exists(local_path):
            return self._message_and_fail('ERROR: local path doesn\'t exist: ' + local_path)
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
            elif is_b2_src and not remote_file and options['delete']:
                self._print("- %s" % filename)
                os.remove(filepath)
            elif not is_b2_src and local_file and not is_match:
                self._print("+ %s" % filename)
                file_infos = {
                    'src_last_modified_millis': str(int(os.path.getmtime(filepath) * 1000))
                }
                bucket.upload_local_file(filepath, b2_path, file_infos=file_infos)
            elif not is_b2_src and not local_file and options['delete']:
                self._print("- %s" % filename)
                self.api.delete_file_version(remote_file['fileId'], b2_path)
            elif not is_b2_src and not local_file and options['hide']:
                self._print(". %s" % filename)
                bucket.hide_file(b2_path)

        # Remove empty local directories
        if is_b2_src and options['delete']:
            for dirpath, dirnames, filenames in os.walk(local_path, topdown=False):
                for name in dirnames:
                    try:
                        os.rmdir(os.path.join(dirpath, name))
                    except Exception:
                        pass

        return 0

    def version(self):
        self._print('b2 command line tool, version', VERSION)
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
