######################################################################
#
# File: b2/raw_api.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import print_function

import base64
import hashlib
import os
import random
import re
import sys
import time
from abc import (ABCMeta, abstractmethod)

import six

from .b2http import (B2Http)
from .download_dest import DownloadDestBytes
from .exception import ChecksumMismatch, TruncatedOutput, UnexpectedCloudBehaviour
from .utils import b2_url_encode, hex_sha1_of_stream

# Standard names for file info entries
SRC_LAST_MODIFIED_MILLIS = 'src_last_modified_millis'


@six.add_metaclass(ABCMeta)
class AbstractRawApi(object):
    """
    Direct access to the B2 web apis.
    """

    @abstractmethod
    def cancel_large_file(self, api_url, account_auth_token, file_id):
        pass

    @abstractmethod
    def delete_bucket(self, api_url, account_auth_token, account_id, bucket_id):
        pass

    @abstractmethod
    def delete_file_version(self, api_url, account_auth_token, file_id, file_name):
        pass

    @abstractmethod
    def finish_large_file(self, api_url, account_auth_token, file_id, part_sha1_array):
        pass

    @abstractmethod
    def get_upload_part_url(self, api_url, account_auth_token, file_id):
        pass

    @abstractmethod
    def hide_file(self, api_url, account_auth_token, bucket_id, file_name):
        pass

    @abstractmethod
    def list_parts(self, api_url, account_auth_token, file_id, start_part_number, max_part_count):
        pass

    @abstractmethod
    def list_unfinished_large_files(
        self, api_url, account_auth_token, bucket_id, start_file_id=None, max_file_count=None
    ):
        pass

    @abstractmethod
    def start_large_file(
        self, api_url, account_auth_token, bucket_id, file_name, content_type, file_info
    ):
        pass

    @abstractmethod
    def update_bucket(
        self,
        api_url,
        account_auth_token,
        account_id,
        bucket_id,
        bucket_type=None,
        bucket_info=None,
        lifecycle_rules=None,
        if_revision_is=None
    ):
        pass

    @abstractmethod
    def upload_part(
        self, upload_url, upload_auth_token, part_number, content_length, sha1_sum, input_stream
    ):
        pass


class B2RawApi(AbstractRawApi):
    """
    Provides access to the B2 web APIs, exactly as they are provided by B2.

    Requires that you provide all necessary URLs and auth tokens for each call.

    Each API call decodes the returned JSON and returns a dict.

    For details on what each method does, see the B2 docs:
        https://www.backblaze.com/b2/docs/

    This class is intended to be a super-simple, very thin layer on top
    of the HTTP calls.  It can be mocked-out for testing higher layers.
    And this class can be tested by exercising each call just once,
    which is relatively quick.

    All public methods of this class except authorize_account shall accept
    api_url and account_info as first two positional arguments. This is needed
    for B2Session magic.
    """

    def __init__(self, b2_http):
        self.b2_http = b2_http

    def _post_json(self, base_url, api_name, auth, **params):
        """
        Helper method for calling an API with the given auth and params.
        :param base_url: Something like "https://api001.backblazeb2.com/"
        :param auth: Passed in Authorization header.
        :param api_name: Example: "b2_create_bucket"
        :param args: The rest of the parameters are passed to B2.
        :return:
        """
        url = base_url + '/b2api/v1/' + api_name
        headers = {'Authorization': auth}
        return self.b2_http.post_json_return_json(url, headers, params)

    def authorize_account(self, realm_url, account_id, application_key):
        auth = b'Basic ' + base64.b64encode(six.b('%s:%s' % (account_id, application_key)))
        return self._post_json(realm_url, 'b2_authorize_account', auth)

    def cancel_large_file(self, api_url, account_auth_token, file_id):
        return self._post_json(api_url, 'b2_cancel_large_file', account_auth_token, fileId=file_id)

    def create_bucket(
        self,
        api_url,
        account_auth_token,
        account_id,
        bucket_name,
        bucket_type,
        bucket_info=None,
        lifecycle_rules=None
    ):
        return self._post_json(
            api_url,
            'b2_create_bucket',
            account_auth_token,
            accountId=account_id,
            bucketName=bucket_name,
            bucketType=bucket_type,
            bucketInfo=bucket_info,
            lifecycleRules=lifecycle_rules
        )

    def delete_bucket(self, api_url, account_auth_token, account_id, bucket_id):
        return self._post_json(
            api_url,
            'b2_delete_bucket',
            account_auth_token,
            accountId=account_id,
            bucketId=bucket_id
        )

    def delete_file_version(self, api_url, account_auth_token, file_id, file_name):
        return self._post_json(
            api_url,
            'b2_delete_file_version',
            account_auth_token,
            fileId=file_id,
            fileName=file_name
        )

    def download_file_by_id(
        self, download_url, account_auth_token_or_none, file_id, download_dest, range_=None
    ):
        url = download_url + '/b2api/v1/b2_download_file_by_id?fileId=' + file_id
        return self._download_file_from_url(
            url, account_auth_token_or_none, download_dest, range_=range_
        )

    def download_file_by_name(
        self,
        download_url,
        account_auth_token_or_none,
        bucket_name,
        file_name,
        download_dest,
        range_=None
    ):
        url = download_url + '/file/' + bucket_name + '/' + b2_url_encode(file_name)
        return self._download_file_from_url(
            url, account_auth_token_or_none, download_dest, range_=range_
        )

    def _download_file_from_url(self, url, account_auth_token_or_none, download_dest, range_=None):
        """
        Downloads a file from given url and stores it in the given download_destination.

        Returns a dict containing all of the file info from the headers in the reply.

        :param url: The full URL to download from
        :param account_auth_token_or_none: an optional account auth token to pass in
        :param download_dest: where to put the file when it is downloaded
        :param progress_listener: where to notify about progress downloading
        :return:
        """
        request_headers = {}
        if range_ is not None:
            assert len(range_) == 2, range_
            assert (range_[0] + 0) <= (range_[1] + 0), range_  # not strings
            assert range_[0] >= 0, range_
            assert range_[1] >= 1, range_
            request_headers['Range'] = "bytes=%d-%d" % range_

        if account_auth_token_or_none is not None:
            request_headers['Authorization'] = account_auth_token_or_none

        with self.b2_http.get_content(url, request_headers) as response:

            info = response.headers

            file_id = info['x-bz-file-id']
            file_name = info['x-bz-file-name']
            content_type = info['content-type']
            content_length = int(info['content-length'])
            content_sha1 = info['x-bz-content-sha1']
            if range_ is not None:
                if 'Content-Range' not in info:
                    raise UnexpectedCloudBehaviour('Content-Range header was expected')
            file_info = dict((k[10:], info[k]) for k in info if k.startswith('x-bz-info-'))

            if SRC_LAST_MODIFIED_MILLIS in file_info:
                mod_time_millis = int(file_info[SRC_LAST_MODIFIED_MILLIS])
            else:
                mod_time_millis = int(info['x-bz-upload-timestamp'])

            block_size = 4096
            digest = hashlib.sha1()
            bytes_read = 0

            with download_dest.open(
                file_id,
                file_name,
                content_length,
                content_type,
                content_sha1,
                file_info,
                mod_time_millis,
                range_=range_
            ) as file:
                for data in response.iter_content(chunk_size=block_size):
                    file.write(data)
                    digest.update(data)
                    bytes_read += len(data)

                if range_ is None:
                    if bytes_read != int(info['content-length']):
                        raise TruncatedOutput(bytes_read, content_length)

                    if content_sha1 != 'none' and digest.hexdigest() != content_sha1:
                        raise ChecksumMismatch(
                            checksum_type='sha1',
                            expected=content_length,
                            actual=digest.hexdigest()
                        )
                else:
                    desired_length = range_[1] - range_[0]
                    if bytes_read != desired_length:
                        raise TruncatedOutput(bytes_read, desired_length)

            return dict(
                fileId=file_id,
                fileName=file_name,
                contentType=content_type,
                contentLength=content_length,
                contentSha1=content_sha1,
                fileInfo=file_info
            )

    def finish_large_file(self, api_url, account_auth_token, file_id, part_sha1_array):
        return self._post_json(
            api_url,
            'b2_finish_large_file',
            account_auth_token,
            fileId=file_id,
            partSha1Array=part_sha1_array
        )

    def get_download_authorization(
        self, api_url, account_auth_token, bucket_id, file_name_prefix, valid_duration_in_seconds
    ):
        return self._post_json(
            api_url,
            'b2_get_download_authorization',
            account_auth_token,
            bucketId=bucket_id,
            fileNamePrefix=file_name_prefix,
            validDurationInSeconds=valid_duration_in_seconds
        )

    def get_file_info(self, api_url, account_auth_token, file_id):
        return self._post_json(api_url, 'b2_get_file_info', account_auth_token, fileId=file_id)

    def get_upload_url(self, api_url, account_auth_token, bucket_id):
        return self._post_json(api_url, 'b2_get_upload_url', account_auth_token, bucketId=bucket_id)

    def get_upload_part_url(self, api_url, account_auth_token, file_id):
        return self._post_json(
            api_url, 'b2_get_upload_part_url', account_auth_token, fileId=file_id
        )

    def hide_file(self, api_url, account_auth_token, bucket_id, file_name):
        return self._post_json(
            api_url, 'b2_hide_file', account_auth_token, bucketId=bucket_id, fileName=file_name
        )

    def list_buckets(self, api_url, account_auth_token, account_id):
        return self._post_json(api_url, 'b2_list_buckets', account_auth_token, accountId=account_id)

    def list_file_names(
        self, api_url, account_auth_token, bucket_id, start_file_name=None, max_file_count=None
    ):
        return self._post_json(
            api_url,
            'b2_list_file_names',
            account_auth_token,
            bucketId=bucket_id,
            startFileName=start_file_name,
            maxFileCount=max_file_count
        )

    def list_file_versions(
        self,
        api_url,
        account_auth_token,
        bucket_id,
        start_file_name=None,
        start_file_id=None,
        max_file_count=None
    ):
        return self._post_json(
            api_url,
            'b2_list_file_versions',
            account_auth_token,
            bucketId=bucket_id,
            startFileName=start_file_name,
            startFileId=start_file_id,
            maxFileCount=max_file_count
        )

    def list_parts(self, api_url, account_auth_token, file_id, start_part_number, max_part_count):
        return self._post_json(
            api_url,
            'b2_list_parts',
            account_auth_token,
            fileId=file_id,
            startPartNumber=start_part_number,
            maxPartCount=max_part_count
        )

    def list_unfinished_large_files(
        self, api_url, account_auth_token, bucket_id, start_file_id=None, max_file_count=None
    ):
        return self._post_json(
            api_url,
            'b2_list_unfinished_large_files',
            account_auth_token,
            bucketId=bucket_id,
            startFileId=start_file_id,
            maxFileCount=max_file_count
        )

    def start_large_file(
        self, api_url, account_auth_token, bucket_id, file_name, content_type, file_info
    ):
        return self._post_json(
            api_url,
            'b2_start_large_file',
            account_auth_token,
            bucketId=bucket_id,
            fileName=file_name,
            fileInfo=file_info,
            contentType=content_type
        )

    def update_bucket(
        self,
        api_url,
        account_auth_token,
        account_id,
        bucket_id,
        bucket_type=None,
        bucket_info=None,
        lifecycle_rules=None,
        if_revision_is=None
    ):
        assert bucket_info or bucket_type

        kwargs = {}
        if if_revision_is is not None:
            kwargs['ifRevisionIs'] = if_revision_is
        if bucket_info is not None:
            kwargs['bucketInfo'] = bucket_info
        if bucket_type is not None:
            kwargs['bucketType'] = bucket_type
        if lifecycle_rules is not None:
            kwargs['lifecycleRules'] = lifecycle_rules

        return self._post_json(
            api_url,
            'b2_update_bucket',
            account_auth_token,
            accountId=account_id,
            bucketId=bucket_id,
            **kwargs
        )

    def upload_file(
        self, upload_url, upload_auth_token, file_name, content_length, content_type, content_sha1,
        file_infos, data_stream
    ):
        """
        Uploads one small file to B2.

        :param upload_url: The upload_url from b2_authorize_account
        :param upload_auth_token: The auth token from b2_authorize_account
        :param file_name: The name of the B2 file
        :param content_length: Number of bytes in the file.
        :param content_type: MIME type.
        :param content_sha1: Hex SHA1 of the contents of the file
        :param file_infos: Extra file info to upload
        :param data_stream: A file like object from which the contents of the file can be read.
        :return:
        """
        headers = {
            'Authorization': upload_auth_token,
            'Content-Length': str(content_length),
            'X-Bz-File-Name': b2_url_encode(file_name),
            'Content-Type': content_type,
            'X-Bz-Content-Sha1': content_sha1
        }
        for k, v in six.iteritems(file_infos):
            headers['X-Bz-Info-' + k] = b2_url_encode(v)

        return self.b2_http.post_content_return_json(upload_url, headers, data_stream)

    def upload_part(
        self, upload_url, upload_auth_token, part_number, content_length, content_sha1, data_stream
    ):
        headers = {
            'Authorization': upload_auth_token,
            'Content-Length': str(content_length),
            'X-Bz-Part-Number': str(part_number),
            'X-Bz-Content-Sha1': content_sha1
        }

        return self.b2_http.post_content_return_json(upload_url, headers, data_stream)


def test_raw_api():
    """
    Exercises the code in B2RawApi by making each call once, just
    to make sure the parameters are passed in, and the result is
    passed back.

    The goal is to be a complete test of B2RawApi, so the tests for
    the rest of the code can use the simulator.

    Prints to stdout if things go wrong.

    :return: 0 on success, non-zero on failure.
    """
    try:
        raw_api = B2RawApi(B2Http())
        test_raw_api_helper(raw_api)
        return 0
    except Exception as e:
        print('Caught exception: %s' % (repr(e),), file=sys.stdout)
        return 1


def test_raw_api_helper(raw_api):
    """
    Tries each of the calls to the raw api.  Raises an
    except if anything goes wrong.

    This uses a Backblaze account that is just for this test.
    The account uses the free level of service, which should
    be enough to run this test a reasonable number of times
    each day.  If somebody abuses the account for other things,
    this test will break and we'll have to do something about
    it.
    """

    account_id = os.environ.get('TEST_ACCOUNT_ID')
    if account_id is None:
        print('TEST_ACCOUNT_ID is not set.', file=sys.stderr)
        sys.exit(1)
    application_key = os.environ.get('TEST_APPLICATION_KEY')
    if application_key is None:
        print('TEST_APPLICATION_KEY is not set.', file=sys.stderr)
        sys.exit(1)
    realm_url = 'https://api.backblazeb2.com'

    # b2_authorize_account
    print('b2_authorize_account')
    auth_dict = raw_api.authorize_account(realm_url, account_id, application_key)
    account_auth_token = auth_dict['authorizationToken']
    api_url = auth_dict['apiUrl']
    download_url = auth_dict['downloadUrl']

    # b2_create_bucket, with a unique bucket name
    # Include the account ID in the bucket name to be
    # sure it doesn't collide with bucket names from
    # other accounts.
    print('b2_create_bucket')
    bucket_name = '%s-%d-%d' % (account_id, int(time.time()), random.randint(1000, 9999))
    bucket_dict = raw_api.create_bucket(
        api_url, account_auth_token, account_id, bucket_name, 'allPublic'
    )
    bucket_id = bucket_dict['bucketId']

    # b2_list_buckets
    print('b2_list_buckets')
    bucket_list_dict = raw_api.list_buckets(api_url, account_auth_token, account_id)

    # b2_get_upload_url
    print('b2_get_upload_url')
    upload_url_dict = raw_api.get_upload_url(api_url, account_auth_token, bucket_id)
    upload_url = upload_url_dict['uploadUrl']
    upload_auth_token = upload_url_dict['authorizationToken']

    # b2_upload_file
    print('b2_upload_file')
    file_name = 'test.txt'
    file_contents = six.b('hello world')
    file_sha1 = hex_sha1_of_stream(six.BytesIO(file_contents), len(file_contents))
    file_dict = raw_api.upload_file(
        upload_url, upload_auth_token, file_name,
        len(file_contents), 'text/plain', file_sha1, {'color': 'blue'}, six.BytesIO(file_contents)
    )
    file_id = file_dict['fileId']

    # b2_download_file_by_id with auth
    print('b2_download_file_by_id (auth)')
    download_dest = DownloadDestBytes()
    raw_api.download_file_by_id(download_url, account_auth_token, file_id, download_dest)
    assert file_contents == download_dest.bytes_io.getvalue()

    # b2_download_file_by_id no auth
    print('b2_download_file_by_id (no auth)')
    download_dest = DownloadDestBytes()
    raw_api.download_file_by_id(download_url, None, file_id, download_dest)
    assert file_contents == download_dest.bytes_io.getvalue()

    # b2_download_file_by_name with auth
    print('b2_download_file_by_name (auth)')
    download_dest = DownloadDestBytes()
    raw_api.download_file_by_name(
        download_url, account_auth_token, bucket_name, file_name, download_dest
    )
    assert file_contents == download_dest.bytes_io.getvalue()

    # b2_download_file_by_name no auth
    print('b2_download_file_by_name (no auth)')
    download_dest = DownloadDestBytes()
    raw_api.download_file_by_name(download_url, None, bucket_name, file_name, download_dest)
    assert file_contents == download_dest.bytes_io.getvalue()

    # b2_get_download_authorization
    print('b2_get_download_authorization')
    download_auth = raw_api.get_download_authorization(
        api_url, account_auth_token, bucket_id, "prefix", 12345
    )
    download_auth_token = download_auth['authorizationToken']

    # b2_download_file_by_name with download auth
    print('b2_download_file_by_name (download auth)')
    download_dest = DownloadDestBytes()
    raw_api.download_file_by_name(
        download_url, download_auth_token, bucket_name, file_name, download_dest
    )
    assert file_contents == download_dest.bytes_io.getvalue()

    # b2_list_file_names
    print('b2_list_file_names')
    list_names_dict = raw_api.list_file_names(api_url, account_auth_token, bucket_id)
    assert [file_name] == [f_dict['fileName'] for f_dict in list_names_dict['files']]

    # b2_list_file_names (start, count)
    print('b2_list_file_names (start, count)')
    list_names_dict = raw_api.list_file_names(
        api_url, account_auth_token, bucket_id, start_file_name=file_name, max_file_count=5
    )
    assert [file_name] == [f_dict['fileName'] for f_dict in list_names_dict['files']]

    # b2_hide_file
    print('b2_hide_file')
    raw_api.hide_file(api_url, account_auth_token, bucket_id, file_name)

    # b2_get_file_info
    print('b2_get_file_info')
    file_info_dict = raw_api.get_file_info(api_url, account_auth_token, file_id)
    assert file_info_dict['fileName'] == file_name

    # b2_start_large_file
    print('b2_start_large_file')
    file_info = {'color': 'red'}
    large_info = raw_api.start_large_file(
        api_url, account_auth_token, bucket_id, file_name, 'text/plain', file_info
    )
    large_file_id = large_info['fileId']

    # b2_get_upload_part_url
    print('b2_get_upload_part_url')
    upload_part_dict = raw_api.get_upload_part_url(api_url, account_auth_token, large_file_id)
    upload_part_url = upload_part_dict['uploadUrl']
    upload_path_auth = upload_part_dict['authorizationToken']

    # b2_upload_part
    print('b2_upload_part')
    part_contents = six.b('hello part')
    part_sha1 = hex_sha1_of_stream(six.BytesIO(part_contents), len(part_contents))
    raw_api.upload_part(
        upload_part_url, upload_path_auth, 1,
        len(part_contents), part_sha1, six.BytesIO(part_contents)
    )

    # b2_list_parts
    print('b2_list_parts')
    parts_response = raw_api.list_parts(api_url, account_auth_token, large_file_id, 1, 100)
    assert [1] == [part['partNumber'] for part in parts_response['parts']]

    # b2_list_unfinished_large_files
    unfinished_list = raw_api.list_unfinished_large_files(api_url, account_auth_token, bucket_id)
    assert [file_name] == [f_dict['fileName'] for f_dict in unfinished_list['files']]
    assert file_info == unfinished_list['files'][0]['fileInfo']

    # b2_finish_large_file
    # We don't upload enough data to actually finish on, so we'll just
    # check that the right error is returned.
    print('b2_finish_large_file')
    try:
        raw_api.finish_large_file(api_url, account_auth_token, large_file_id, [part_sha1])
        raise Exception('finish should have failed')
    except Exception as e:
        assert 'large files must have at least 2 parts' in str(e)

    # b2_update_bucket
    print('b2_update_bucket')
    updated_bucket = raw_api.update_bucket(
        api_url,
        account_auth_token,
        account_id,
        bucket_id,
        'allPrivate',
        bucket_info={'color': 'blue'}
    )
    assert updated_bucket['revision'] == 2

    # clean up this test
    _clean_and_delete_bucket(raw_api, api_url, account_auth_token, account_id, bucket_id)

    # Clean up from old tests.  Empty and delete any buckets more than an hour old.
    for bucket_dict in bucket_list_dict['buckets']:
        bucket_id = bucket_dict['bucketId']
        bucket_name = bucket_dict['bucketName']
        if _should_delete_bucket(bucket_name):
            print('cleaning up old bucket: ' + bucket_name)
            _clean_and_delete_bucket(raw_api, api_url, account_auth_token, account_id, bucket_id)


def _clean_and_delete_bucket(raw_api, api_url, account_auth_token, account_id, bucket_id):
    # Delete the files.  This test never creates more than a few files,
    # so one call to list_file_versions should get them all.
    versions_dict = raw_api.list_file_versions(api_url, account_auth_token, bucket_id)
    for version_dict in versions_dict['files']:
        file_id = version_dict['fileId']
        file_name = version_dict['fileName']
        action = version_dict['action']
        if action in ['hide', 'upload']:
            print('b2_delete_file', file_name, action)
            raw_api.delete_file_version(api_url, account_auth_token, file_id, file_name)
        else:
            print('b2_cancel_large_file', file_name)
            raw_api.cancel_large_file(api_url, account_auth_token, file_id)

    # Delete the bucket
    print('b2_delete_bucket', bucket_id)
    raw_api.delete_bucket(api_url, account_auth_token, account_id, bucket_id)


def _should_delete_bucket(bucket_name):
    # Bucket names for this test look like: c7b22d0b0ad7-1460060364-5670
    # Other buckets should not be deleted.
    match = re.match(r'^[a-f0-9]+-([0-9]+)-([0-9]+)', bucket_name)
    if match is None:
        return False

    # Is it more than an hour old?
    bucket_time = int(match.group(1))
    now = time.time()
    return bucket_time + 3600 <= now
