######################################################################
#
# File: b2/raw_api.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import base64
import hashlib
import json
import socket
import sys
from abc import (ABCMeta, abstractmethod)

import six
from six.moves import urllib

from .exception import (
    map_error_dict_to_exception, ChecksumMismatch, FatalError, TruncatedOutput, WrappedHttpError,
    WrappedHttplibError, WrappedSocketError, WrappedUrlError
)
from .utils import (b2_url_encode)
from .version import (USER_AGENT)


class OpenUrl(object):
    """
    Context manager that handles an open urllib2.Request, and provides
    the file-like object that is the response.
    """

    def __init__(self, url, data, headers, params=None):
        self.url = url
        self.data = data
        self.headers = self._add_user_agent(headers)
        self.file = None
        self.params = None  # for debugging

    def _add_user_agent(self, headers):
        """
        Adds a User-Agent header if there isn't one already.

        Reminder: HTTP header names are case-insensitive.
        """
        for k in headers:
            if k.lower() == 'user-agent':
                return headers
        else:
            result = dict(headers)
            result['User-Agent'] = USER_AGENT
            return result

    def __enter__(self):
        try:
            request = urllib.request.Request(self.url, self.data, self.headers)
            self.file = urllib.request.urlopen(request)
            return self.file
        except urllib.error.HTTPError as e:
            data = e.read()
            raise WrappedHttpError(data, self.url, self.params, self.headers, sys.exc_info())
        except urllib.error.URLError as e:
            raise WrappedUrlError(e.reason, self.url, self.params, self.headers, sys.exc_info())
        except socket.error as e:  # includes socket.timeout
            # reportedly socket errors are not wrapped in urllib2.URLError since Python 2.7
            raise WrappedSocketError(
                'errno=%s' % (e.errno,),
                self.url,
                self.params,
                self.headers,
                sys.exc_info(),
            )
        except six.moves.http_client.HTTPException as e:  # includes httplib.BadStatusLine
            raise WrappedHttplibError(str(e), self.url, self.params, self.headers, sys.exc_info())

    def __exit__(self, exception_type, exception, traceback):
        if self.file is not None:
            self.file.close()


def read_str_from_http_response(response):
    """
    This is an ugly hack.  I probably don't understand Python 2/3
    compatibility well enough.

    The read() method on urllib responses returns a str in Python 2,
    and bytes in Python 3, so json.load() won't work in both. This
    function converts the result into a str that json.loads() will
    take.
    """
    if six.PY2:
        return response.read()
    else:
        return response.read().decode('utf-8')


def post_json(url, params, auth_token=None):
    """Coverts params to JSON and posts them to the given URL.

    Returns the resulting JSON, decoded into a dict or an
    exception, custom if possible, WrappedHttpError otherwise
    """
    data = six.b(json.dumps(params))
    headers = {}
    if auth_token is not None:
        headers['Authorization'] = auth_token
    try:
        with OpenUrl(url, data, headers, params) as f:
            json_text = read_str_from_http_response(f)
            return json.loads(json_text)
    except WrappedHttpError as e:
        e_backup = sys.exc_info()
        try:
            error_dict = json.loads(e.data.decode('utf-8'))
            raise map_error_dict_to_exception(e, error_dict, params)
        except ValueError:
            v = sys.exc_info()
            error_dict = {'error_decoding_json': v}
            raise FatalError('error decoding JSON when handling an exception', [e_backup, v],)


@six.add_metaclass(ABCMeta)
class RawApi(object):
    """
    Direct access to the B2 web apis.
    """

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
    def list_unfinished_large_files(
        self,
        api_url,
        account_auth_token,
        bucket_id,
        start_file_id=None,
        max_file_count=None
    ):
        pass

    @abstractmethod
    def start_large_file(
        self, api_url, account_auth_token, bucket_id, file_name, content_type, file_info
    ):
        pass

    @abstractmethod
    def update_bucket(self, api_url, account_auth_token, account_id, bucket_id, bucket_type):
        pass

    @abstractmethod
    def upload_part(
        self, upload_url, upload_auth_token, part_number, content_length, sha1_sum, input_stream
    ):
        pass


class B2RawApi(RawApi):
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

    def _post_json(self, base_url, api_name, auth, **params):
        """
        Helper method for calling an API with the given auth and params.
        :param base_url: Something like "https://api001.backblaze.com/"
        :param auth: Passed in Authorization header.
        :param api_name: Example: "b2_create_bucket"
        :param args: The rest of the parameters are passed to B2.
        :return:
        """
        url = base_url + '/b2api/v1/' + api_name
        return post_json(url, params, auth)

    def authorize_account(self, realm_url, account_id, application_key):
        auth = b'Basic ' + base64.b64encode(six.b('%s:%s' % (account_id, application_key)))
        return self._post_json(realm_url, 'b2_authorize_account', auth)

    def create_bucket(self, api_url, account_auth_token, account_id, bucket_name, bucket_type):
        return self._post_json(
            api_url,
            'b2_create_bucket',
            account_auth_token,
            accountId=account_id,
            bucketName=bucket_name,
            bucketType=bucket_type
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

    def download_file_by_id(self, download_url, account_auth_token_or_none, file_id, download_dest):
        url = download_url + '/b2api/v1/b2_download_file_by_id?fileId=' + file_id
        return self._download_file_from_url(url, account_auth_token_or_none, download_dest)

    def download_file_by_name(
        self, download_url, account_auth_token_or_none, bucket_id, file_name, download_dest
    ):
        url = download_url + '/file/' + bucket_id + '/' + b2_url_encode(file_name)
        return self._download_file_from_url(url, account_auth_token_or_none, download_dest)

    def _download_file_from_url(self, url, account_auth_token_or_none, download_dest):
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
        if account_auth_token_or_none is not None:
            request_headers['Authorization'] = account_auth_token_or_none

        with OpenUrl(url, None, request_headers) as response:

            info = response.info()

            file_id = info['x-bz-file-id']
            file_name = info['x-bz-file-name']
            content_type = info['content-type']
            content_length = int(info['content-length'])
            content_sha1 = info['x-bz-content-sha1']
            file_info = dict((k[10:], info[k]) for k in info if k.startswith('x-bz-info-'))

            block_size = 4096
            digest = hashlib.sha1()
            bytes_read = 0

            with download_dest.open(
                file_id, file_name, content_length, content_type, content_sha1, file_info
            ) as file:
                while True:
                    data = response.read(block_size)
                    if len(data) == 0:
                        break
                    file.write(data)
                    digest.update(data)
                    bytes_read += len(data)

                if bytes_read != int(info['content-length']):
                    raise TruncatedOutput(bytes_read, content_length)

                if digest.hexdigest() != content_sha1:
                    raise ChecksumMismatch(
                        checksum_type='sha1',
                        expected=content_length,
                        actual=digest.hexdigest()
                    )

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
            api_url, 'b2_finish_large_file', account_auth_token, file_id, part_sha1_array
        )

    def get_file_info(self, api_url, account_auth_token, file_id):
        return self._post_json(api_url, 'b2_get_file_info', account_auth_token, fileId=file_id)

    def get_upload_url(self, api_url, account_auth_token, bucket_id):
        return self._post_json(api_url, 'b2_get_upload_url', account_auth_token, bucketId=bucket_id)

    def get_upload_part_url(self, api_url, account_auth_token, file_id):
        return self._post_json(api_url, 'b2_get_upload_url', account_auth_token, fileId=file_id)

    def hide_file(self, api_url, account_auth_token, bucket_id, file_name):
        return self._post_json(
            api_url,
            'b2_hide_file',
            account_auth_token,
            bucketId=bucket_id,
            fileName=file_name
        )

    def list_buckets(self, api_url, account_auth_token, account_id):
        return self._post_json(api_url, 'b2_list_buckets', account_auth_token, accountId=account_id)

    def list_file_names(
        self,
        api_url,
        account_auth_token,
        bucket_id,
        start_file_name=None,
        max_file_count=None
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

    def list_unfinished_large_files(
        self,
        api_url,
        account_auth_token,
        bucket_id,
        start_file_id=None,
        max_file_count=None
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
            contentType=content_type
        )

    def update_bucket(self, api_url, account_auth_token, account_id, bucket_id, bucket_type):
        return self._post_json(
            api_url,
            'b2_update_bucket',
            account_auth_token,
            accountId=account_id,
            bucketId=bucket_id,
            bucketType=bucket_type
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

        with OpenUrl(upload_url, data_stream, headers) as response_file:
            json_text = read_str_from_http_response(response_file)
            return json.loads(json_text)

    def upload_part(
        self, upload_url, upload_auth_token, part_number, content_length, content_sha1, data_stream
    ):
        headers = {
            'Authorization': upload_auth_token,
            'Content-Length': str(content_length),
            'X-Bz-Part-Number': str(part_number),
            'X-Bz-Content-Sha1': content_sha1
        }

        with OpenUrl(upload_url, data_stream, headers) as response_file:
            json_text = read_str_from_http_response(response_file)
            return json.loads(json_text)
