######################################################################
#
# File: b2/bucket.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import six
import threading

from .download_dest import DownloadDestProgressWrapper
from .exception import (
    AlreadyFailed, B2Error, MaxFileSizeExceeded, MaxRetriesExceeded, UnrecognizedBucketType
)
from .file_version import FileVersionInfoFactory
from .progress import DoNothingProgressListener, AbstractProgressListener, RangeOfInputStream, StreamWithProgress
from .unfinished_large_file import UnfinishedLargeFile
from .upload_source import UploadSourceBytes, UploadSourceLocalFile
from .utils import b2_url_encode, choose_part_ranges, hex_sha1_of_stream, interruptible_get_result, validate_b2_file_name
from .utils import B2TraceMeta, disable_trace, limit_trace_arguments


class LargeFileUploadState(object):
    """
    Tracks the status of uploading a large file, accepting updates
    from the tasks that upload each of the parts.

    The aggregated progress is passed on to a ProgressListener that
    reports the progress for the file as a whole.

    This class is THREAD SAFE.
    """

    def __init__(self, file_progress_listener):
        self.lock = threading.RLock()
        self.error_message = None
        self.file_progress_listener = file_progress_listener
        self.part_number_to_part_state = {}
        self.bytes_completed = 0

    def set_error(self, message):
        with self.lock:
            self.error_message = message

    def has_error(self):
        with self.lock:
            return self.error_message is not None

    def get_error_message(self):
        with self.lock:
            assert self.has_error()
            return self.error_message

    def update_part_bytes(self, bytes_delta):
        with self.lock:
            self.bytes_completed += bytes_delta
            self.file_progress_listener.bytes_completed(self.bytes_completed)


class PartProgressReporter(AbstractProgressListener):
    """
    An adapter that listens to the progress of upload a part and
    gives the information to a LargeFileUploadState.

    Accepts absolute bytes_completed from the uploader, and reports
    deltas to the LargeFileUploadState.  The bytes_completed for the
    part will drop back to 0 on a retry, which will result in a
    negative delta.
    """

    def __init__(self, large_file_upload_state):
        self.large_file_upload_state = large_file_upload_state
        self.prev_byte_count = 0

    def bytes_completed(self, byte_count):
        self.large_file_upload_state.update_part_bytes(byte_count - self.prev_byte_count)
        self.prev_byte_count = byte_count

    def close(self):
        pass

    def set_total_bytes(self, total_byte_count):
        pass


@six.add_metaclass(B2TraceMeta)
class Bucket(object):
    """
    Provides access to a bucket in B2: listing files, uploading and downloading.
    """

    DEFAULT_CONTENT_TYPE = 'b2/x-auto'
    MAX_UPLOAD_ATTEMPTS = 5
    MAX_LARGE_FILE_SIZE = 10 * 1000 * 1000 * 1000 * 1000  # 10 TB

    def __init__(self, api, id_, name=None, type_=None):
        self.api = api
        self.id_ = id_
        self.name = name
        self.type_ = type_

    def get_id(self):
        return self.id_

    def set_type(self, type_):
        account_id = self.api.account_info.get_account_id()
        return self.api.session.update_bucket(account_id, self.id_, type_)

    def cancel_large_file(self, file_id):
        return self.api.cancel_large_file(file_id)

    def download_file_by_id(self, file_id, download_dest, progress_listener=None):
        self.api.download_file_by_id(file_id, download_dest, progress_listener)

    def download_file_by_name(self, file_name, download_dest, progress_listener=None):
        progress_listener = progress_listener or DoNothingProgressListener()
        self.api.session.download_file_by_name(
            self.name,
            file_name,
            DownloadDestProgressWrapper(download_dest, progress_listener),
            url_factory=self.api.account_info.get_download_url
        )
        progress_listener.close()

    def list_parts(self, file_id, start_part_number=None, batch_size=None):
        return self.api.list_parts(file_id, start_part_number, batch_size)

    def ls(
        self,
        folder_to_list='',
        show_versions=False,
        max_entries=None,
        recursive=False,
        fetch_count=100
    ):
        """Pretends that folders exist, and yields the information about the files in a folder.

        B2 has a flat namespace for the files in a bucket, but there is a convention
        of using "/" as if there were folders.  This method searches through the
        flat namespace to find the files and "folders" that live within a given
        folder.

        When the `recursive` flag is set, lists all of the files in the given
        folder, and all of its sub-folders.

        :param folder: The name of the folder to list.  Must not start with "/".
                       Empty string means top-level folder.
        :param show_versions: When true returns info about all versions of a file,
                              when false, just returns info about the most recent
                              versions.
        :param max_entries: How many entries to return.  1 - 1000
        :param recursive:
        :return:
        """
        # Every file returned must have a name that starts with the
        # folder name and a "/".
        prefix = folder_to_list
        if prefix != '' and not prefix.endswith('/'):
            prefix += '/'

        # Loop until all files in the named directory have been listed.
        # The starting point of the first list_file_names request is the
        # prefix we're looking for.  The prefix ends with '/', which is
        # now allowed for file names, so no file name will match exactly,
        # but the first one after that point is the first file in that
        # "folder".   If the first search doesn't produce enough results,
        # then we keep calling list_file_names until we get all of the
        # names in this "folder".
        current_dir = None
        start_file_name = prefix
        start_file_id = None
        session = self.api.session
        while True:
            if show_versions:
                response = session.list_file_versions(
                    self.id_, start_file_name, start_file_id, fetch_count
                )
            else:
                response = session.list_file_names(self.id_, start_file_name, fetch_count)
            for entry in response['files']:
                file_version_info = FileVersionInfoFactory.from_api_response(entry)
                if not file_version_info.file_name.startswith(prefix):
                    # We're past the files we care about
                    return
                after_prefix = file_version_info.file_name[len(prefix):]
                if '/' not in after_prefix or recursive:
                    # This is not a folder, so we'll print it out and
                    # continue on.
                    yield file_version_info, None
                    current_dir = None
                else:
                    # This is a folder.  If it's different than the folder
                    # we're already in, then we can print it.  This check
                    # is needed, because all of the files in the folder
                    # will be in the list.
                    folder_with_slash = after_prefix.split('/')[0] + '/'
                    if folder_with_slash != current_dir:
                        folder_name = prefix + folder_with_slash
                        yield file_version_info, folder_name
                        current_dir = folder_with_slash
            if response['nextFileName'] is None:
                # The response says there are no more files in the bucket,
                # so we can stop.
                return

            # Now we need to set up the next search.  The response from
            # B2 has the starting point to continue with the next file,
            # but if we're in the middle of a "folder", we can skip ahead
            # to the end of the folder.  The character after '/' is '0',
            # so we'll replace the '/' with a '0' and start there.
            #
            # When recursive is True, current_dir is always None.
            if current_dir is None:
                start_file_name = response.get('nextFileName')
                start_file_id = response.get('nextFileId')
            else:
                start_file_name = max(response['nextFileName'],
                                      prefix + current_dir[:-1] + '0',)

    def list_file_names(self, start_filename=None, max_entries=None):
        """ legacy interface which just returns whatever remote API returns """
        return self.api.session.list_file_names(self.id_, start_filename, max_entries)

    def list_file_versions(self, start_filename=None, start_file_id=None, max_entries=None):
        """ legacy interface which just returns whatever remote API returns """
        return self.api.session.list_file_versions(
            self.id_, start_filename, start_file_id, max_entries
        )

    def list_unfinished_large_files(self, start_file_id=None, batch_size=None):
        """
        A generator that yields an UnfinishedLargeFile for each
        unfinished large file in the bucket, starting at the
        given file.
        """
        batch_size = batch_size or 100
        while True:
            batch = self.api.session.list_unfinished_large_files(
                self.id_, start_file_id, batch_size
            )
            for file_dict in batch['files']:
                yield UnfinishedLargeFile(file_dict)
            start_file_id = batch.get('nextFileId')
            if start_file_id is None:
                break

    def start_large_file(self, file_name, content_type=None, file_info=None):
        return UnfinishedLargeFile(
            self.api.session.start_large_file(self.id_, file_name, content_type, file_info)
        )

    @limit_trace_arguments(skip=('data_bytes',))
    def upload_bytes(
        self, data_bytes, file_name, content_type=None, file_infos=None, progress_listener=None
    ):
        """
        Upload bytes in memory to a B2 file
        """
        upload_source = UploadSourceBytes(data_bytes)
        return self.upload(
            upload_source,
            file_name,
            content_type=content_type,
            file_info=file_infos,
            progress_listener=progress_listener
        )

    def upload_local_file(
        self,
        local_file,
        file_name,
        content_type=None,
        file_infos=None,
        sha1_sum=None,
        min_part_size=None,
        progress_listener=None
    ):
        """
        Uploads a file on local disk to a B2 file.
        """
        upload_source = UploadSourceLocalFile(local_path=local_file, content_sha1=sha1_sum)
        return self.upload(
            upload_source,
            file_name,
            content_type=content_type,
            file_info=file_infos,
            min_part_size=min_part_size,
            progress_listener=progress_listener
        )

    def upload(
        self,
        upload_source,
        file_name,
        content_type=None,
        file_info=None,
        min_part_size=None,
        progress_listener=None
    ):
        """
        Uploads a file to B2, retrying as needed.

        The source of the upload is an UploadSource object that can be used to
        open (and re-open) the file.  The result of opening should be a binary
        file whose read() method returns bytes.

        :param upload_source: an UploadSource object that opens the source of the upload
        :param file_name: the file name of the new B2 file
        :param content_type: the MIME type, or None to accept the default based on file extension of the B2 file name
        :param file_infos: custom file info to be stored with the file
        :param min_part_size: the smallest part size to use
        :param progress_listener: object to notify as data is transferred
        :return:

        The function `opener` should return a file-like object, and it
        must be possible to call it more than once in case the upload
        is retried.
        """

        validate_b2_file_name(file_name)
        file_info = file_info or {}
        content_type = content_type or self.DEFAULT_CONTENT_TYPE
        progress_listener = progress_listener or DoNothingProgressListener()

        # We don't upload any large files unless all of the parts can be at least
        # the minimum part size.
        min_part_size = max(min_part_size or 0, self.api.account_info.get_minimum_part_size())
        min_large_file_size = min_part_size * 2
        if upload_source.get_content_length() < min_large_file_size:
            # Run small uploads in the same thread pool as large file uploads,
            # so that they share resources during a sync.
            f = self.api.get_thread_pool().submit(
                self._upload_small_file, upload_source, file_name, content_type, file_info,
                progress_listener
            )
            return f.result()
        else:
            return self._upload_large_file(
                upload_source, file_name, content_type, file_info, progress_listener
            )

    def _upload_small_file(
        self, upload_source, file_name, content_type, file_info, progress_listener
    ):
        content_length = upload_source.get_content_length()
        sha1_sum = upload_source.get_content_sha1()
        exception_info_list = []
        for _ in six.moves.xrange(self.MAX_UPLOAD_ATTEMPTS):
            # refresh upload data in every attempt to work around a "busy storage pod"
            upload_url, upload_auth_token = self._get_upload_data()

            try:
                with upload_source.open() as file:
                    progress_listener.set_total_bytes(content_length)
                    input_stream = StreamWithProgress(file, progress_listener)
                    upload_response = self.api.raw_api.upload_file(
                        upload_url, upload_auth_token, file_name, content_length, content_type,
                        sha1_sum, file_info, input_stream
                    )
                    self.api.account_info.put_bucket_upload_url(
                        self.id_, upload_url, upload_auth_token
                    )
                    progress_listener.close()
                    return FileVersionInfoFactory.from_api_response(upload_response)

            except B2Error as e:
                if not e.should_retry_upload():
                    raise
                exception_info_list.append(e)
                self.api.account_info.clear_bucket_upload_data(self.id_)

        raise MaxRetriesExceeded(self.MAX_UPLOAD_ATTEMPTS, exception_info_list)

    def _upload_large_file(
        self, upload_source, file_name, content_type, file_info, progress_listener
    ):
        content_length = upload_source.get_content_length()
        if self.MAX_LARGE_FILE_SIZE < content_length:
            raise MaxFileSizeExceeded(content_length, self.MAX_LARGE_FILE_SIZE)
        minimum_part_size = self.api.account_info.get_minimum_part_size()

        # Set up the progress reporting for the parts
        progress_listener.set_total_bytes(content_length)
        large_file_upload_state = LargeFileUploadState(progress_listener)

        # Select the part boundaries
        part_ranges = choose_part_ranges(content_length, minimum_part_size)

        # Check for unfinished files with same name
        unfinished_file, finished_parts = self._find_unfinished_file(
            upload_source, file_name, file_info, part_ranges
        )

        # Tell B2 we're going to upload a file if necessary
        if unfinished_file is None:
            unfinished_file = self.start_large_file(file_name, content_type, file_info)
        file_id = unfinished_file.file_id

        # Tell the executor to upload each of the parts
        part_futures = [
            self.api.get_thread_pool().submit(
                self._upload_part,
                file_id,
                part_index + 1,  # part number
                part_range,
                upload_source,
                large_file_upload_state,
                finished_parts
            ) for (part_index, part_range) in enumerate(part_ranges)
        ]

        # Collect the sha1 checksums of the parts as the uploads finish.
        # If any of them raised an exception, that same exception will
        # be raised here by result()
        part_sha1_array = [interruptible_get_result(f)['contentSha1'] for f in part_futures]

        # Finish the large file
        response = self.api.session.finish_large_file(file_id, part_sha1_array)
        progress_listener.close()
        return FileVersionInfoFactory.from_api_response(response)

    def _find_unfinished_file(self, upload_source, file_name, file_info, part_ranges):
        """
        Find an unfinished file which may be used to resume a large file upload. The
        file is found using the filename and comparing the uploaded parts against
        the local file.
        """
        for file_ in self.list_unfinished_large_files():
            if file_.file_name == file_name and file_.file_info == file_info:
                files_match = True
                finished_parts = {}
                for part in self.list_parts(file_.file_id):
                    # Compare part sizes
                    offset, part_length = part_ranges[part.part_number - 1]
                    if part_length != part.content_length:
                        files_match = False
                        break

                    # Compare hash
                    with upload_source.open() as f:
                        f.seek(offset)
                        sha1_sum = hex_sha1_of_stream(f, part_length)
                    if sha1_sum != part.content_sha1:
                        files_match = False
                        break

                    # Save part
                    finished_parts[part.part_number] = part

                # Skip not matching files or unfinished files with no uploaded parts
                if not files_match or not finished_parts:
                    continue

                # Return first matched file
                return file_, finished_parts
        return None, {}

    def _upload_part(
        self,
        file_id,
        part_number,
        part_range,
        upload_source,
        large_file_upload_state,
        finished_parts=None
    ):
        # Check if this part was uploaded before
        if finished_parts is not None and part_number in finished_parts:
            # Report this part finished
            part = finished_parts[part_number]
            large_file_upload_state.update_part_bytes(part.content_length)

            # Return SHA1 hash
            return {'contentSha1': part.content_sha1}

        # Compute the SHA1 of the part
        offset, content_length = part_range
        with upload_source.open() as f:
            f.seek(offset)
            sha1_sum = hex_sha1_of_stream(f, content_length)

        # Set up a progress listener
        part_progress_listener = PartProgressReporter(large_file_upload_state)

        # Retry the upload as needed
        exception_list = []
        for _ in six.moves.xrange(self.MAX_UPLOAD_ATTEMPTS):
            # refresh upload data in every attempt to work around a "busy storage pod"
            upload_url, upload_auth_token = self._get_upload_part_data(file_id)

            # if another part has already had an error there's no point in
            # uploading this part
            if large_file_upload_state.has_error():
                raise AlreadyFailed(large_file_upload_state.get_error_message())

            try:
                with upload_source.open() as file:
                    file.seek(offset)
                    range_stream = RangeOfInputStream(file, offset, content_length)
                    input_stream = StreamWithProgress(range_stream, part_progress_listener)
                    response = self.api.raw_api.upload_part(
                        upload_url, upload_auth_token, part_number, content_length, sha1_sum,
                        input_stream
                    )
                    assert sha1_sum == response['contentSha1']
                    self.api.account_info.put_large_file_upload_url(
                        file_id, upload_url, upload_auth_token
                    )
                    return response

            except B2Error as e:
                if not e.should_retry_upload():
                    raise
                exception_list.append(e)
                self.api.account_info.clear_bucket_upload_data(self.id_)

        large_file_upload_state.set_error(str(exception_list[-1]))
        raise MaxRetriesExceeded(self.MAX_UPLOAD_ATTEMPTS, exception_list)

    def _get_upload_data(self):
        """
        Takes ownership of an upload URL / auth token for the bucket and
        returns it.
        """
        account_info = self.api.account_info
        upload_url, upload_auth_token = account_info.take_bucket_upload_url(self.id_)
        if None not in (upload_url, upload_auth_token):
            return upload_url, upload_auth_token

        response = self.api.session.get_upload_url(self.id_)
        return response['uploadUrl'], response['authorizationToken']

    def _get_upload_part_data(self, file_id):
        """
        Makes sure that we have an upload URL and auth token for the given bucket and
        returns it.
        """
        account_info = self.api.account_info
        upload_url, upload_auth_token = account_info.take_large_file_upload_url(file_id)
        if None not in (upload_url, upload_auth_token):
            return upload_url, upload_auth_token

        response = self.api.session.get_upload_part_url(file_id)
        return (response['uploadUrl'], response['authorizationToken'])

    def get_download_url(self, filename):
        return "%s/file/%s/%s" % (
            self.api.account_info.get_download_url(),
            b2_url_encode(self.name),
            b2_url_encode(filename),
        )

    def hide_file(self, file_name):
        response = self.api.session.hide_file(self.id_, file_name)
        return FileVersionInfoFactory.from_api_response(response)

    def delete_file_version(self, file_id, file_name):
        # filename argument is not first, because one day it may become optional
        return self.api.delete_file_version(file_id, file_name)

    @disable_trace
    def as_dict(self):  # TODO: refactor with other as_dict()
        result = {'accountId': self.api.account_info.get_account_id(),
                  'bucketId': self.id_,}
        if self.name is not None:
            result['bucketName'] = self.name
        if self.type_ is not None:
            result['bucketType'] = self.type_
        return result

    def __repr__(self):
        return 'Bucket<%s,%s,%s>' % (self.id_, self.name, self.type_)


class BucketFactory(object):
    @classmethod
    def from_api_response(cls, api, response):
        return [cls.from_api_bucket_dict(api, bucket_dict) for bucket_dict in response['buckets']]

    @classmethod
    def from_api_bucket_dict(cls, api, bucket_dict):
        """
            turns this:
            {
                "bucketType": "allPrivate",
                "bucketId": "a4ba6a39d8b6b5fd561f0010",
                "bucketName": "zsdfrtsazsdfafr",
                "accountId": "4aa9865d6f00"
            }
            into a Bucket object
        """
        bucket_name = bucket_dict['bucketName']
        bucket_id = bucket_dict['bucketId']
        type_ = bucket_dict['bucketType']
        if type_ is None:
            raise UnrecognizedBucketType(bucket_dict['bucketType'])
        return Bucket(api, bucket_id, bucket_name, type_)
