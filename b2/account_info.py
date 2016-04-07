######################################################################
#
# File: b2/account_info.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import collections
import json
import os
import portalocker
import stat
import threading
from abc import (ABCMeta, abstractmethod)

import six

from .exception import (MissingAccountData)


@six.add_metaclass(ABCMeta)
class AbstractAccountInfo(object):
    """
    Holder for all account-related information that needs to be kept
    between API calls, and between invocations of the command-line
    tool.  This includes: account id, application key, auth tokens,
    API URL, download URL, and uploads URLs.

    This class must be THREAD SAFE because it may be used by multiple
    threads running in the same Python process.  It also needs to be
    safe against multiple processes running at the same time.
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
    def take_bucket_upload_url(self, bucket_id):
        """
        Returns a pair (upload_url, upload_auth_token) that has been removed
        from the pool for this bucket, or (None, None) if there are no more
        left.
        """

    @abstractmethod
    def put_bucket_upload_url(self, bucket_id, upload_url, upload_auth_token):
        """
        Add an (upload_url, upload_auth_token) pair to the pool available for
        the bucket.
        """

    @abstractmethod
    def put_large_file_upload_url(self, file_id, upload_url, upload_auth_token):
        pass

    @abstractmethod
    def take_large_file_upload_url(self, file_id):
        pass

    @abstractmethod
    def clear_large_file_upload_urls(self, file_id):
        pass


class StoredAccountInfo(AbstractAccountInfo):
    """Manages the file that holds the account ID and stored auth tokens.

    Bucket upload URLs are treated as a pool, from which threads can
    borrow an URL, use it, and then put it back.

    Large file upload URLs are also a pool, but are not stored in the file.
    They are kept in memory, and lost on process exit.  Typically, a
    large file upload is done as a single task in one process.
    """

    # Keys in top-level data dict:
    ACCOUNT_AUTH_TOKEN = 'account_auth_token'
    ACCOUNT_ID = 'account_id'
    APPLICATION_KEY = 'application_key'
    API_URL = 'api_url'
    BUCKET_NAMES_TO_IDS = 'bucket_names_to_ids'
    BUCKET_UPLOAD_URLS = 'bucket_upload_urls'
    DOWNLOAD_URL = 'download_url'
    MINIMUM_PART_SIZE = 'minimum_part_size'
    REALM = 'realm'
    VERSION = 'version'

    # Keys in each entry in BUCKET_UPLOAD_URLS
    URL = 'url'
    AUTH_TOKEN = 'auth_token'

    # Value of the VERSION field.
    CURRENT_VERSION = 2

    def __init__(self, file_name=None, internal_lock_timeout=120):
        if file_name is None:
            user_account_info_path = os.environ.get('B2_ACCOUNT_INFO', '~/.b2_account_info')
            self.filename = os.path.expanduser(user_account_info_path)
        else:
            self.filename = file_name
        self._lock_filename = self.filename + '.lock'
        self._lock_timeout = internal_lock_timeout
        self._large_file_uploads = collections.defaultdict(
            list
        )  # We don't keep large file upload URLs across a reload
        self._bucket_names_to_ids = {}  # for in-memory cache

        # Lock to manage in-memory structures
        self._lock = threading.Lock()

    def clear(self):
        self._update_data(lambda d: self._scrub_data({}))

    def get_account_id(self):
        return self._get_account_info_or_raise(self.ACCOUNT_ID)

    def get_account_auth_token(self):
        return self._get_account_info_or_raise(self.ACCOUNT_AUTH_TOKEN)

    def get_api_url(self):
        return self._get_account_info_or_raise(self.API_URL)

    def get_application_key(self):
        return self._get_account_info_or_raise(self.APPLICATION_KEY)

    def get_download_url(self):
        return self._get_account_info_or_raise(self.DOWNLOAD_URL)

    def get_minimum_part_size(self):
        return self._get_account_info_or_raise(self.MINIMUM_PART_SIZE)

    def get_realm(self):
        return self._get_account_info_or_raise(self.REALM)

    def _get_account_info_or_raise(self, key):
        result = self._read_data().get(key)
        if result is None:
            raise MissingAccountData(key)
        return result

    def set_auth_data(
        self, account_id, auth_token, api_url, download_url, minimum_part_size, application_key,
        realm
    ):
        def update_fcn(data):
            data[self.ACCOUNT_ID] = account_id
            data[self.ACCOUNT_AUTH_TOKEN] = auth_token
            data[self.API_URL] = api_url
            data[self.APPLICATION_KEY] = application_key
            data[self.REALM] = realm
            data[self.DOWNLOAD_URL] = download_url
            data[self.MINIMUM_PART_SIZE] = minimum_part_size

        self._update_data(update_fcn)

    def put_bucket_upload_url(self, bucket_id, upload_url, upload_auth_token):
        def update_fcn(data):
            upload_urls = data[self.BUCKET_UPLOAD_URLS].get(bucket_id, [])
            upload_urls.append({self.URL: upload_url, self.AUTH_TOKEN: upload_auth_token})
            data[self.BUCKET_UPLOAD_URLS][bucket_id] = upload_urls

        self._update_data(update_fcn)

    def take_bucket_upload_url(self, bucket_id):
        result_holder = [(None, None)]

        def update_fcn(data):
            upload_urls = data[self.BUCKET_UPLOAD_URLS].get(bucket_id, [])
            if len(upload_urls) != 0:
                first = upload_urls[0]
                result_holder[0] = (first[self.URL], first[self.AUTH_TOKEN])
                data[self.BUCKET_UPLOAD_URLS][bucket_id] = upload_urls[1:]

        self._update_data(update_fcn)
        return result_holder[0]

    def clear_bucket_upload_data(self, bucket_id):
        def update_fcn(data):
            data[self.BUCKET_UPLOAD_URLS].pop(bucket_id, None)

        self._update_data(update_fcn)

    def put_large_file_upload_url(self, file_id, upload_url, upload_auth_token):
        with self._lock:
            self._large_file_uploads[file_id].append((upload_url, upload_auth_token))

    def take_large_file_upload_url(self, file_id):
        with self._lock:
            url_list = self._large_file_uploads.get(file_id, [])
            if len(url_list) == 0:
                return (None, None)
            else:
                return url_list.pop()

    def clear_large_file_upload_urls(self, file_id):
        with self._lock:
            if file_id in self._large_file_uploads:
                del self._large_file_uploads[file_id]

    def save_bucket(self, bucket):
        def update_fcn(data):
            names_to_ids = data[self.BUCKET_NAMES_TO_IDS]
            if names_to_ids.get(bucket.name) != bucket.id_:
                names_to_ids[bucket.name] = bucket.id_

        self._update_data(update_fcn)

    def refresh_entire_bucket_name_cache(self, name_id_iterable):
        def update_fcn(data):
            data[self.BUCKET_NAMES_TO_IDS] = dict(name_id_iterable)

        self._update_data(update_fcn)

    def remove_bucket_name(self, bucket_name):
        def update_fcn(data):
            names_to_ids = data[self.BUCKET_NAMES_TO_IDS]
            if bucket_name in names_to_ids:
                del names_to_ids[bucket_name]

        self._update_data(update_fcn)

    def get_bucket_id_or_none_from_bucket_name(self, bucket_name):
        bucket_id = self._bucket_names_to_ids.get(bucket_name)
        if bucket_id is not None:
            return bucket_id
        data = self._read_data()
        names_to_ids = data[self.BUCKET_NAMES_TO_IDS]
        return names_to_ids.get(bucket_name)

    def _read_data(self):
        """
        Returns the data currently in the file, then releases the lock.

        There is no guarantee that the data in the file won't change.
        """
        with _shared_lock(self._lock_filename, self._lock_timeout):
            self._data = self._scrub_data(_read_file_while_locked(self.filename))
            return self._data

    def _update_data(self, update_fcn):
        """
        Locks the file, applies the update_fcn to the data in the file,
        and then writes the results back to disk.

        self._data is updated to contain the modified data.

        The update function takes the existing data as a parameter, and
        returns the updated data to store.  The function is allowed to
        modify the existing dictionary in place if it wants; returning
        None says to use the existing dictionary that was modified in
        place.
        """
        with _exclusive_lock(self._lock_filename, self._lock_timeout):
            old_data = self._scrub_data(_read_file_while_locked(self.filename))
            new_data = update_fcn(old_data)
            if new_data is None:
                new_data = old_data  # data was modified in place
            _write_file_while_locked(new_data, self.filename)
            self._data = new_data
            self._bucket_names_to_ids = new_data[self.BUCKET_NAMES_TO_IDS]

    def _scrub_data(self, data):
        """
        Takes the data read from disk, and cleans it for use, making sure it
        matches the structure we are expecting.
        """
        # newer version of this tool require minimumPartSize.
        # if it's not there, we need to refresh
        if data.get(self.VERSION, 0) != self.CURRENT_VERSION:
            data = {self.VERSION: self.CURRENT_VERSION}

        if self.BUCKET_UPLOAD_URLS not in data:
            data[self.BUCKET_UPLOAD_URLS] = {}

        if self.BUCKET_NAMES_TO_IDS not in data:
            data[self.BUCKET_NAMES_TO_IDS] = {}

        # This is an ugly thing with the side-effect of updating the
        # state of this object.  Takes advantage of the knowledge that
        # this _scrub_data method is called every time data is loaded
        # from disk.
        return data


def _exclusive_lock(lock_file, timeout):
    return portalocker.Lock(
        lock_file,
        timeout=timeout,
        fail_when_locked=False,
        flags=portalocker.LOCK_EX
    )


def _shared_lock(lock_file, timeout):
    return portalocker.Lock(
        lock_file,
        timeout=timeout,
        fail_when_locked=False,
        flags=portalocker.LOCK_SH
    )


def _read_file_while_locked(file_name):
    """
    Returns the contents of the file, if present, after converting
    it to JSON and running the result through the data_scrubber.

    Assumes that the caller has done all necessary locking to make
    sure that nobody else is writing the file while we are reading.
    """
    # Read the contents of the file.
    try:
        with open(file_name, 'rb') as f:
            json_str = f.read().decode('utf-8')
            return json.loads(json_str)
    except Exception:
        return {}


def _write_file_while_locked(data, file_name):
    """
    Converts a dict to JSON and writes it to the given file.

    Assumes that the caller has done all necessary locking to make
    sure that nobody else is reading or writing the file at the
    same time.
    """
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    if os.name == 'nt':
        flags |= os.O_BINARY
    with os.fdopen(os.open(file_name, flags, stat.S_IRUSR | stat.S_IWUSR), 'wb') as f:
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
        self._large_file_uploads = collections.defaultdict(list)

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

    def take_bucket_upload_url(self, bucket_id):
        return (None, None)

    def put_bucket_upload_url(self, bucket_id, upload_url, upload_auth_token):
        pass

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

    def put_large_file_upload_url(self, file_id, upload_url, upload_auth_token):
        self._large_file_uploads[file_id].append((upload_url, upload_auth_token))

    def take_large_file_upload_url(self, file_id):
        upload_urls = self._large_file_uploads.get(file_id, [])
        if len(upload_urls) == 0:
            return (None, None)
        else:
            return upload_urls.pop()

    def clear_large_file_upload_urls(self, file_id):
        if file_id in self._large_file_uploads:
            del self._large_file_uploads[file_id]
