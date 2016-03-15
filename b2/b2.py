######################################################################
#
# File: b2
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
This is a B2 command-line tool.  See the USAGE message for details.
"""

from __future__ import print_function

import json
import os
import stat
from abc import ABCMeta, abstractmethod

import six

from .bucket import (Bucket, BucketFactory)
from .exception import (MissingAccountData, NonExistentBucket)
from .file_version import (FileVersionInfoFactory)
from .raw_api import (B2RawApi)
from .session import (B2Session)

## Cache


@six.add_metaclass(ABCMeta)
class AbstractCache(object):
    def clear(self):
        self.set_bucket_name_cache(tuple())

    @abstractmethod
    def get_bucket_id_or_none_from_bucket_name(self, name):
        pass

    @abstractmethod
    def save_bucket(self, bucket):
        pass

    @abstractmethod
    def set_bucket_name_cache(self, buckets):
        pass

    def _name_id_iterator(self, buckets):
        return ((bucket.name, bucket.id_) for bucket in buckets)


class DummyCache(AbstractCache):
    """ Cache that does nothing """

    def get_bucket_id_or_none_from_bucket_name(self, name):
        return None

    def save_bucket(self, bucket):
        pass

    def set_bucket_name_cache(self, buckets):
        pass


class InMemoryCache(AbstractCache):
    """ Cache that stores the information in memory """

    def __init__(self):
        self.name_id_map = {}

    def get_bucket_id_or_none_from_bucket_name(self, name):
        return self.name_id_map.get(name)

    def save_bucket(self, bucket):
        self.name_id_map[bucket.name] = bucket.id_

    def set_bucket_name_cache(self, buckets):
        self.name_id_map = dict(self._name_id_iterator(buckets))


class AuthInfoCache(AbstractCache):
    """ Cache that stores data persistently in StoredAccountInfo """

    def __init__(self, info):
        self.info = info

    def get_bucket_id_or_none_from_bucket_name(self, name):
        return self.info.get_bucket_id_or_none_from_bucket_name(name)

    def save_bucket(self, bucket):
        self.info.save_bucket(bucket)

    def set_bucket_name_cache(self, buckets):
        self.info.refresh_entire_bucket_name_cache(self._name_id_iterator(buckets))

## B2Api


class B2Api(object):
    """
    Provides file-level access to B2 services.

    While B2RawApi provides direct access to the B2 web APIs, this
    class handles several things that simplify the task of uploading
    and downloading files:
      - re-acquires authorization tokens when they expire
      - retrying uploads when an upload URL is busy
      - breaking large files into parts
      - emulating a directory structure (B2 buckets are flat)

    Adds an object-oriented layer on top of the raw API, so that
    buckets and files returned are Python objects with accessor
    methods.

    Also,  keeps a cache of information needed to access the service,
    such as auth tokens and upload URLs.
    """

    # TODO: move HTTP code out to B2RawApi
    # TODO: ConsoleTool passes the account info cache into the constructor
    # TODO: provide method to get the account info cache (so ConsoleTool can save it)

    def __init__(self, account_info=None, cache=None, raw_api=None):
        """
        Initializes the API using the given account info.
        :param account_info:
        :param cache:
        :param raw_api:
        :return:
        """
        self.raw_api = raw_api or B2RawApi()
        if account_info is None:
            account_info = StoredAccountInfo()
            if cache is None:
                cache = AuthInfoCache(account_info)
        self.session = B2Session(account_info, self.raw_api)
        self.account_info = account_info
        if cache is None:
            cache = DummyCache()
        self.cache = cache

    def authorize_automatically(self):
        try:
            self.authorize(
                self.account_info.get_realm(),
                self.account_info.get_account_id(),
                self.account_info.get_application_key(),
            )
        except MissingAccountData:
            return False
        return True

    def authorize_account(self, realm, account_id, application_key):
        try:
            old_account_id = self.account_info.get_account_id()
            old_realm = self.account_info.get_realm()
            if account_id != old_account_id or realm != old_realm:
                self.cache.clear()
        except MissingAccountData:
            self.cache.clear()

        realm_url = self.account_info.REALM_URLS[realm]
        response = self.raw_api.authorize_account(realm_url, account_id, application_key)

        self.account_info.set_auth_data(
            response['accountId'],
            response['authorizationToken'],
            response['apiUrl'],
            response['downloadUrl'],
            response['minimumPartSize'],
            application_key,
            realm,
        )

    def get_account_id(self):
        return self.account_info.get_account_id()

    # buckets

    def create_bucket(self, name, type_):
        account_id = self.account_info.get_account_id()

        response = self.session.create_bucket(account_id, name, type_)
        bucket = BucketFactory.from_api_bucket_dict(self, response)
        assert name == bucket.name, 'API created a bucket with different name\
                                     than requested: %s != %s' % (name, bucket.name)
        assert type_ == bucket.type_, 'API created a bucket with different type\
                                     than requested: %s != %s' % (type_, bucket.type_)
        self.cache.save_bucket(bucket)
        return bucket

    def download_file_by_id(self, file_id, download_dest):
        self.raw_api.download_file_by_id(
            self.account_info.get_download_url(), self.account_info.get_account_auth_token(),
            file_id, download_dest
        )

    def get_bucket_by_id(self, bucket_id):
        return Bucket(self, bucket_id)

    def get_bucket_by_name(self, bucket_name):
        """
        Returns the bucket_id for the given bucket_name.

        If we don't already know it from the cache, try fetching it from
        the B2 service.
        """
        # If we can get it from the stored info, do that.
        id_ = self.cache.get_bucket_id_or_none_from_bucket_name(bucket_name)
        if id_ is not None:
            return Bucket(self, id_, name=bucket_name)

        for bucket in self.list_buckets():
            if bucket.name == bucket_name:
                return bucket
        raise NonExistentBucket(bucket_name)

    def delete_bucket(self, bucket):
        """
        Deletes the bucket remotely.
        For legacy reasons it returns whatever server sends in response,
        but API user should not rely on the response: if it doesn't raise
        an exception, it means that the operation was a success
        """
        account_id = self.account_info.get_account_id()
        return self.session.delete_bucket(account_id, bucket.id_)

    def list_buckets(self):
        """
        Calls b2_list_buckets and returns the JSON for *all* buckets.
        """
        account_id = self.account_info.get_account_id()

        response = self.session.list_buckets(account_id)

        buckets = BucketFactory.from_api_response(self, response)

        self.cache.set_bucket_name_cache(buckets)
        return buckets

    # delete
    def delete_file_version(self, file_id, file_name):
        # filename argument is not first, because one day it may become optional
        response = self.session.delete_file_version(file_id, file_name)
        file_info = FileVersionInfoFactory.from_api_response(response, force_action='delete',)
        assert file_info.id_ == file_id
        assert file_info.file_name == file_name
        assert file_info.action == 'delete'
        return file_info

    # download
    def get_download_url_for_fileid(self, file_id):
        url = url_for_api(self.account_info, 'b2_download_file_by_id')
        return '%s?fileId=%s' % (url, file_id)

    # other
    def get_file_info(self, file_id):
        """ legacy interface which just returns whatever remote API returns """
        return self.session.get_file_info(file_id)

## v0.3.x functions


@six.add_metaclass(ABCMeta)
class AbstractAccountInfo(object):
    """
    Holder for auth token, API URL, and download URL.
    """
    REALM_URLS = {
        'production': 'https://api.backblaze.com',
        'dev': 'http://api.test.blaze:8180',
        'staging': 'https://api.backblaze.net',
    }

    @abstractmethod
    def clear(self):
        """
        Removes all stored information
        """

    @abstractmethod
    def clear_bucket_upload_data(self, bucket_id):
        """
        Removes all upload URLs for the given bucket.
        """

    @abstractmethod
    def get_account_id(self):
        """ returns account_id or raises MissingAccountData exception """
        pass

    @abstractmethod
    def get_account_auth_token(self):
        """ returns account_auth_token or raises MissingAccountData exception """
        pass

    @abstractmethod
    def get_api_url(self):
        """ returns api_url or raises MissingAccountData exception """
        pass

    @abstractmethod
    def get_application_key(self):
        """ returns application_key or raises MissingAccountData exception """
        pass

    @abstractmethod
    def get_download_url(self):
        """ returns download_url or raises MissingAccountData exception """
        pass

    @abstractmethod
    def get_realm(self):
        """ returns realm or raises MissingAccountData exception """
        pass

    @abstractmethod
    def set_auth_data(self, account_id, auth_token, api_url, download_url, application_key, realm):
        pass

    @abstractmethod
    def set_bucket_upload_data(self, bucket_id, upload_url, upload_auth_token):
        pass

    @abstractmethod
    def set_large_file_upload_data(self, file_id, upload_url, upload_auth_token):
        pass

    @abstractmethod
    def get_large_file_upload_data(self, file_id):
        pass

    @abstractmethod
    def clear_large_file_upload_data(self, file_id):
        pass


class StoredAccountInfo(AbstractAccountInfo):
    """Manages the file that holds the account ID and stored auth tokens.

    When an instance of this class is created, it reads the account
    info file in the home directory of the user, and remembers the info.

    When any changes are made, they are written out to the file.
    """

    ACCOUNT_AUTH_TOKEN = 'account_auth_token'
    ACCOUNT_ID = 'account_id'
    APPLICATION_KEY = 'application_key'
    API_URL = 'api_url'
    BUCKET_NAMES_TO_IDS = 'bucket_names_to_ids'
    BUCKET_UPLOAD_DATA = 'bucket_upload_data'
    BUCKET_UPLOAD_URL = 'bucket_upload_url'
    BUCKET_UPLOAD_AUTH_TOKEN = 'bucket_upload_auth_token'
    DOWNLOAD_URL = 'download_url'
    MINIMUM_PART_SIZE = 'minimum_part_size'
    REALM = 'realm'

    def __init__(self):
        user_account_info_path = os.environ.get('B2_ACCOUNT_INFO', '~/.b2_account_info')
        self.filename = os.path.expanduser(user_account_info_path)
        self.data = self._try_to_read_file()
        self._set_defaults()

    def _set_defaults(self):
        if self.BUCKET_UPLOAD_DATA not in self.data:
            self.data[self.BUCKET_UPLOAD_DATA] = {}
        if self.BUCKET_NAMES_TO_IDS not in self.data:
            self.data[self.BUCKET_NAMES_TO_IDS] = {}
        # We don't keep large file upload URLs across a reload
        self.large_file_uploads = {}

    def _try_to_read_file(self):
        try:
            with open(self.filename, 'rb') as f:
                # is there a cleaner way to do this that works in both Python 2 and 3?
                json_str = f.read().decode('utf-8')
                data = json.loads(json_str)
                # newer version of this tool require minimumPartSize.
                # if it's not there, we need to refresh
                if self.MINIMUM_PART_SIZE not in data:
                    data = {}
                return data
        except Exception:
            return {}

    def clear(self):
        self.data = {}
        self._write_file()
        self._set_defaults()

    def get_account_id(self):
        return self._get_account_info_or_exit(self.ACCOUNT_ID)

    def get_account_auth_token(self):
        return self._get_account_info_or_exit(self.ACCOUNT_AUTH_TOKEN)

    def get_api_url(self):
        return self._get_account_info_or_exit(self.API_URL)

    def get_application_key(self):
        return self._get_account_info_or_exit(self.APPLICATION_KEY)

    def get_download_url(self):
        return self._get_account_info_or_exit(self.DOWNLOAD_URL)

    def get_minimum_part_size(self):
        return self._get_account_info_or_exit(self.MINIMUM_PART_SIZE)

    def get_realm(self):
        return self._get_account_info_or_exit(self.REALM)

    def _get_account_info_or_exit(self, key):
        result = self.data.get(key)
        if result is None:
            raise MissingAccountData(key)
        return result

    def set_auth_data(
        self, account_id, auth_token, api_url, download_url, minimum_part_size, application_key,
        realm
    ):
        self.data[self.ACCOUNT_ID] = account_id
        self.data[self.ACCOUNT_AUTH_TOKEN] = auth_token
        self.data[self.API_URL] = api_url
        self.data[self.APPLICATION_KEY] = application_key
        self.data[self.REALM] = realm
        self.data[self.DOWNLOAD_URL] = download_url
        self.data[self.MINIMUM_PART_SIZE] = minimum_part_size
        self._write_file()

    def set_bucket_upload_data(self, bucket_id, upload_url, upload_auth_token):
        self.data[self.BUCKET_UPLOAD_DATA][bucket_id] = {
            self.BUCKET_UPLOAD_URL: upload_url,
            self.BUCKET_UPLOAD_AUTH_TOKEN: upload_auth_token,
        }
        self._write_file()

    def get_bucket_upload_data(self, bucket_id):
        bucket_upload_data = self.data[self.BUCKET_UPLOAD_DATA].get(bucket_id)
        if bucket_upload_data is None:
            return None, None
        url = bucket_upload_data[self.BUCKET_UPLOAD_URL]
        upload_auth_token = bucket_upload_data[self.BUCKET_UPLOAD_AUTH_TOKEN]
        return url, upload_auth_token

    def clear_bucket_upload_data(self, bucket_id):
        self.data[self.BUCKET_UPLOAD_DATA].pop(bucket_id, None)

    def set_large_file_upload_data(self, file_id, upload_url, upload_auth_token):
        self.large_file_uploads[file_id] = (upload_url, upload_auth_token)

    def get_large_file_upload_data(self, file_id):
        return self.large_file_uploads.get(file_id, (None, None))

    def clear_large_file_upload_data(self, file_id):
        if file_id in self.large_file_uploads:
            del self.large_file_uploads[file_id]

    def save_bucket(self, bucket):
        names_to_ids = self.data[self.BUCKET_NAMES_TO_IDS]
        if names_to_ids.get(bucket.name) != bucket.id_:
            names_to_ids[bucket.name] = bucket.id_
            self._write_file()

    def refresh_entire_bucket_name_cache(self, name_id_iterable):
        names_to_ids = self.data[self.BUCKET_NAMES_TO_IDS]
        new_cache = dict(name_id_iterable)
        if names_to_ids != new_cache:
            self.data[self.BUCKET_NAMES_TO_IDS] = new_cache
            self._write_file()

    def remove_bucket_name(self, bucket_name):
        names_to_ids = self.data[self.BUCKET_NAMES_TO_IDS]
        if bucket_name in names_to_ids:
            del names_to_ids[bucket_name]
        self._write_file()

    def get_bucket_id_or_none_from_bucket_name(self, bucket_name):
        names_to_ids = self.data[self.BUCKET_NAMES_TO_IDS]
        return names_to_ids.get(bucket_name)

    def _write_file(self):
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        if os.name == 'nt':
            flags |= os.O_BINARY
        with os.fdopen(os.open(self.filename, flags, stat.S_IRUSR | stat.S_IWUSR), 'wb') as f:
            # is there a cleaner way to do this that works in both Python 2 and 3?
            json_bytes = json.dumps(self.data, indent=4, sort_keys=True).encode('utf-8')
            f.write(json_bytes)


def url_for_api(info, api_name):
    if api_name in ['b2_download_file_by_id']:
        base = info.get_download_url()
    else:
        base = info.get_api_url()
    return base + '/b2api/v1/' + api_name
