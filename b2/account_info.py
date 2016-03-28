######################################################################
#
# File: b2/account_info.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import json
import os
import portalocker
import stat
from abc import (ABCMeta, abstractmethod)

import six

from .exception import (MissingAccountData)


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

    It assumes many processes are accessing the same account info file,
    so not everything can be cached in memory.
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

    def __init__(self, internal_lock_timeout=120):
        user_account_info_path = os.environ.get('B2_ACCOUNT_INFO', '~/.b2_account_info')
        self.filename = os.path.expanduser(user_account_info_path)
        self._lock_filename = self.filename + '.lock'
        self._lock_timeout = internal_lock_timeout
        self._large_file_uploads = {}  # We don't keep large file upload URLs across a reload
        self._bucket_names_to_ids = {}  # for in-memory cache

    def _get_data(self):
        data = self._try_to_read_file()
        # newer version of this tool require minimumPartSize.
        # if it's not there, we need to refresh
        if self.MINIMUM_PART_SIZE not in data:
            data = {}

        if self.BUCKET_UPLOAD_DATA not in data:
            data[self.BUCKET_UPLOAD_DATA] = {}
        if self.BUCKET_NAMES_TO_IDS not in data:
            data[self.BUCKET_NAMES_TO_IDS] = {}
        self._bucket_names_to_ids = data[self.BUCKET_NAMES_TO_IDS]
        return data

    def _try_to_read_file(self):
        with self._shared_lock():
            try:
                with open(self.filename, 'rb') as f:
                    # is there a cleaner way to do this that works in both Python 2 and 3?
                    json_str = f.read().decode('utf-8')
                    data = json.loads(json_str)
                    return data
            except Exception:
                return {}

    def _exclusive_lock(self):
        # TODO: repackage timeout?
        return portalocker.Lock(
            self._lock_filename,
            timeout=self._lock_timeout,
            flags=portalocker.LOCK_EX,
        )

    def _shared_lock(self):
        # TODO: repackage timeout?
        return portalocker.Lock(
            self._lock_filename,
            timeout=self._lock_timeout,
            flags=portalocker.LOCK_SH,
        )

    def clear(self):
        self._write_file({})
        self._bucket_names_to_ids = {}

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
        result = self._get_data().get(key)
        if result is None:
            raise MissingAccountData(key)
        return result

    def set_auth_data(
        self, account_id, auth_token, api_url, download_url, minimum_part_size, application_key,
        realm
    ):
        data = self._get_data()
        data[self.ACCOUNT_ID] = account_id
        data[self.ACCOUNT_AUTH_TOKEN] = auth_token
        data[self.API_URL] = api_url
        data[self.APPLICATION_KEY] = application_key
        data[self.REALM] = realm
        data[self.DOWNLOAD_URL] = download_url
        data[self.MINIMUM_PART_SIZE] = minimum_part_size
        self._write_file(data)

    def set_bucket_upload_data(self, bucket_id, upload_url, upload_auth_token):
        data = self._get_data()
        data[self.BUCKET_UPLOAD_DATA][bucket_id] = {
            self.BUCKET_UPLOAD_URL: upload_url,
            self.BUCKET_UPLOAD_AUTH_TOKEN: upload_auth_token,
        }
        self._write_file(data)

    def get_bucket_upload_data(self, bucket_id):
        data = self._get_data()
        bucket_upload_data = data[self.BUCKET_UPLOAD_DATA].get(bucket_id)
        if bucket_upload_data is None:
            return None, None
        url = bucket_upload_data[self.BUCKET_UPLOAD_URL]
        upload_auth_token = bucket_upload_data[self.BUCKET_UPLOAD_AUTH_TOKEN]
        return url, upload_auth_token

    def clear_bucket_upload_data(self, bucket_id):
        data = self._get_data()
        bucket_upload_data = data[self.BUCKET_UPLOAD_DATA].pop(bucket_id, None)
        if bucket_upload_data is not None:
            self._write_file(data)

    def set_large_file_upload_data(self, file_id, upload_url, upload_auth_token):
        self._large_file_uploads[file_id] = (upload_url, upload_auth_token)

    def get_large_file_upload_data(self, file_id):
        return self._large_file_uploads.get(file_id, (None, None))

    def clear_large_file_upload_data(self, file_id):
        if file_id in self._large_file_uploads:
            del self._large_file_uploads[file_id]

    def save_bucket(self, bucket):
        self._bucket_names_to_ids[bucket.name] = bucket.id_
        data = self._get_data()
        names_to_ids = data[self.BUCKET_NAMES_TO_IDS]
        if names_to_ids.get(bucket.name) != bucket.id_:
            names_to_ids[bucket.name] = bucket.id_
            self._write_file(data)

    def refresh_entire_bucket_name_cache(self, name_id_iterable):
        new_cache = dict(name_id_iterable)
        self._bucket_names_to_ids = new_cache
        data = self._get_data()
        old_cache = data[self.BUCKET_NAMES_TO_IDS]
        if old_cache != new_cache:
            data[self.BUCKET_NAMES_TO_IDS] = new_cache
            self._write_file(data)

    def remove_bucket_name(self, bucket_name):
        del self._bucket_names_to_ids[bucket_name]
        data = self._get_data()
        names_to_ids = data[self.BUCKET_NAMES_TO_IDS]
        if bucket_name in names_to_ids:
            del names_to_ids[bucket_name]
            self._write_file(data)

    def get_bucket_id_or_none_from_bucket_name(self, bucket_name):
        bucket_id = self._bucket_names_to_ids.get(bucket_name)
        if bucket_id is not None:
            return bucket_id
        data = self._get_data()
        names_to_ids = data[self.BUCKET_NAMES_TO_IDS]
        return names_to_ids.get(bucket_name)

    def _write_file(self, data):
        """
        makes sure the file is consistent for read and not corrupted by multiple writers
        by using an inteprocess lock
        """
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        if os.name == 'nt':
            flags |= os.O_BINARY
        with self._exclusive_lock():
            with os.fdopen(os.open(self.filename, flags, stat.S_IRUSR | stat.S_IWUSR), 'wb') as f:
                # is there a cleaner way to do this that works in both Python 2 and 3?
                json_bytes = json.dumps(data, indent=4, sort_keys=True).encode('utf-8')
                f.write(json_bytes)


class StubAccountInfo(AbstractAccountInfo):
    REALM_URLS = {'production': 'http://production.example.com'}

    def __init__(self):
        self.clear()

    def clear(self):
        self.account_id = None
        self.auth_token = None
        self.api_url = None
        self.download_url = None
        self.minimum_part_size = None
        self.application_key = None
        self.realm = None
        self.buckets = {}
        self._large_file_uploads = {}

    def clear_bucket_upload_data(self, bucket_id):
        if bucket_id in self.buckets:
            del self.buckets[bucket_id]

    def set_auth_data(
        self, account_id, auth_token, api_url, download_url, minimum_part_size, application_key,
        realm
    ):
        self.account_id = account_id
        self.auth_token = auth_token
        self.api_url = api_url
        self.download_url = download_url
        self.minimum_part_size = minimum_part_size
        self.application_key = application_key
        self.realm = realm

    def set_bucket_upload_data(self, bucket_id, upload_url, upload_auth_token):
        self.buckets[bucket_id] = (upload_url, upload_auth_token)

    def get_account_id(self):
        return self.account_id

    def get_account_auth_token(self):
        return self.auth_token

    def get_api_url(self):
        return self.api_url

    def get_application_key(self):
        return self.application_key

    def get_download_url(self):
        return self.download_url

    def get_minimum_part_size(self):
        return self.minimum_part_size

    def get_realm(self):
        return self.realm

    def get_bucket_upload_data(self, bucket_id):
        return self.buckets.get(bucket_id, (None, None))

    def set_large_file_upload_data(self, file_id, upload_url, upload_auth_token):
        self._large_file_uploads[file_id] = (upload_url, upload_auth_token)

    def get_large_file_upload_data(self, file_id):
        return self._large_file_uploads.get(file_id, (None, None))

    def clear_large_file_upload_data(self, file_id):
        if file_id in self._large_file_uploads:
            del self._large_file_uploads[file_id]
