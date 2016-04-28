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
import sqlite3
import stat
import threading
from abc import (ABCMeta, abstractmethod)

import six

from .exception import (CorruptAccountInfo, MissingAccountData)


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
    def refresh_entire_bucket_name_cache(self, name_id_iterable):
        """
        Removes all previous name-to-id mappings and stores new ones.
        """

    @abstractmethod
    def remove_bucket_name(self, bucket_name):
        """
        Removes one entry from the bucket name cache.
        """

    @abstractmethod
    def save_bucket(self, bucket):
        """
        Remembers the ID for a bucket name.
        """

    @abstractmethod
    def get_bucket_id_or_none_from_bucket_name(self, bucket_name):
        """
        Looks up the bucket ID for a given bucket name.
        """

    @abstractmethod
    def clear_bucket_upload_data(self, bucket_id):
        """
        Removes all upload URLs for the given bucket.
        """

    @abstractmethod
    def get_account_id(self):
        """ returns account_id or raises MissingAccountData exception """

    @abstractmethod
    def get_account_auth_token(self):
        """ returns account_auth_token or raises MissingAccountData exception """

    @abstractmethod
    def get_api_url(self):
        """ returns api_url or raises MissingAccountData exception """

    @abstractmethod
    def get_application_key(self):
        """ returns application_key or raises MissingAccountData exception """

    @abstractmethod
    def get_download_url(self):
        """ returns download_url or raises MissingAccountData exception """

    @abstractmethod
    def get_realm(self):
        """ returns realm or raises MissingAccountData exception """

    @abstractmethod
    def get_minimum_part_size(self):
        """
        :return: returns the minimum number of bytes in a part of a large file
        """

    @abstractmethod
    def set_auth_data(
        self, account_id, auth_token, api_url, download_url, minimum_part_size, application_key,
        realm
    ):
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


class SqliteAccountInfo(AbstractAccountInfo):
    """
    Stores account information in an sqlite database, which is
    used to manage concurrent access to the data.
    """

    def __init__(self, file_name=None):
        self.thread_local = threading.local()
        user_account_info_path = file_name or os.environ.get(
            'B2_ACCOUNT_INFO', '~/.b2_account_info'
        )
        self.filename = file_name or os.path.expanduser(user_account_info_path)
        self._validate_database()
        with self._get_connection() as conn:
            self._create_tables(conn)

        self._large_file_uploads = collections.defaultdict(
            list
        )  # We don't keep large file upload URLs across a reload

        # this lock controls access to self._large_file_uploads
        self._lock = threading.Lock()

    def _validate_database(self):
        """
        Makes sure that the database is openable.  Removes the file if it's not.
        """
        # If there is no file there, that's fine.  It will get created when
        # we connect.
        if not os.path.exists(self.filename):
            self._create_database()
            return

        # If we can connect to the database, and do anything, then all is good.
        try:
            with self._connect() as conn:
                self._create_tables(conn)
                return
        except sqlite3.DatabaseError:
            pass  # fall through to next case

        # If the file contains JSON with the right stuff in it, convert from
        # the old representation.
        try:
            with open(self.filename, 'rb') as f:
                data = json.loads(f.read().decode('utf-8'))
                keys = [
                    'account_id', 'application_key', 'account_auth_token', 'api_url',
                    'download_url', 'minimum_part_size', 'realm'
                ]
                if all(k in data for k in keys):
                    # remove the json file
                    os.unlink(self.filename)
                    # create a database
                    self._create_database()
                    # add the data from the JSON file
                    with self._connect() as conn:
                        self._create_tables(conn)
                        insert_statement = """
                            INSERT INTO account
                            (account_id, application_key, account_auth_token, api_url, download_url, minimum_part_size, realm)
                            values (?, ?, ?, ?, ?, ?, ?);
                        """

                        conn.execute(insert_statement, tuple(data[k] for k in keys))
                    # all is happy now
                    return
        except ValueError:  # includes json.decoder.JSONDecodeError
            pass

        # Remove the corrupted file and create a new database
        raise CorruptAccountInfo(self.filename)

    def _get_connection(self):
        """
        Connections to sqlite cannot be shared across threads.
        """
        try:
            return self.thread_local.connection
        except:
            self.thread_local.connection = self._connect()
            return self.thread_local.connection

    def _connect(self):
        return sqlite3.connect(self.filename, isolation_level='EXCLUSIVE')

    def _create_database(self):
        """
        Makes sure that the database is created and sets the file permissions.
        This should be done before storing any sensitive data in it.
        """
        # Create the tables in the database
        conn = self._connect()
        try:
            with conn:
                self._create_tables(conn)
        finally:
            conn.close()

        # Set the file permissions
        os.chmod(self.filename, stat.S_IRUSR | stat.S_IWUSR)

    def _create_tables(self, conn):
        conn.execute(
            """
           CREATE TABLE IF NOT EXISTS
           account (
               account_id TEXT NOT NULL,
               application_key TEXT NOT NULL,
               account_auth_token TEXT NOT NULL,
               api_url TEXT NOT NULL,
               download_url TEXT NOT NULL,
               minimum_part_size INT NOT NULL,
               realm TEXT NOT NULL
           );
        """
        )
        conn.execute(
            """
           CREATE TABLE IF NOT EXISTS
           bucket (
               bucket_name TEXT NOT NULL,
               bucket_id TEXT NOT NULL
           );
        """
        )
        conn.execute(
            """
           CREATE TABLE IF NOT EXISTS
           bucket_upload_url (
               bucket_id TEXT NOT NULL,
               upload_url TEXT NOT NULL,
               upload_auth_token TEXT NOT NULL
           );
        """
        )

    def clear(self):
        with self._get_connection() as conn:
            conn.execute('DELETE FROM account;')
            conn.execute('DELETE FROM bucket;')
            conn.execute('DELETE FROM bucket_upload_url;')

    def set_auth_data(
        self, account_id, account_auth_token, api_url, download_url, minimum_part_size,
        application_key, realm
    ):
        with self._get_connection() as conn:
            conn.execute('DELETE FROM account;')
            conn.execute('DELETE FROM bucket;')
            conn.execute('DELETE FROM bucket_upload_url;')
            insert_statement = """
                INSERT INTO account
                (account_id, application_key, account_auth_token, api_url, download_url, minimum_part_size, realm)
                values (?, ?, ?, ?, ?, ?, ?);
            """

            conn.execute(
                insert_statement, (
                    account_id, application_key, account_auth_token, api_url, download_url,
                    minimum_part_size, realm
                )
            )

    def get_application_key(self):
        return self._get_account_info_or_raise('application_key')

    def get_account_id(self):
        return self._get_account_info_or_raise('account_id')

    def get_api_url(self):
        return self._get_account_info_or_raise('api_url')

    def get_account_auth_token(self):
        return self._get_account_info_or_raise('account_auth_token')

    def get_download_url(self):
        return self._get_account_info_or_raise('download_url')

    def get_realm(self):
        return self._get_account_info_or_raise('realm')

    def get_minimum_part_size(self):
        return self._get_account_info_or_raise('minimum_part_size')

    def _get_account_info_or_raise(self, column_name):
        try:
            with self._get_connection() as conn:
                cursor = conn.execute('SELECT %s FROM account;' % (column_name,))
                value = cursor.fetchone()[0]
                return value
        except Exception as e:
            raise MissingAccountData(str(e))

    def refresh_entire_bucket_name_cache(self, name_id_iterable):
        with self._get_connection() as conn:
            conn.execute('DELETE FROM bucket;')
            for (bucket_name, bucket_id) in name_id_iterable:
                conn.execute(
                    'INSERT INTO bucket (bucket_name, bucket_id) VALUES (?, ?);',
                    (bucket_name, bucket_id)
                )

    def save_bucket(self, bucket):
        with self._get_connection() as conn:
            conn.execute('DELETE FROM bucket WHERE bucket_id = ?;', (bucket.id_,))
            conn.execute(
                'INSERT INTO bucket (bucket_id, bucket_name) VALUES (?, ?);',
                (bucket.id_, bucket.name)
            )

    def remove_bucket_name(self, bucket_name):
        with self._get_connection() as conn:
            conn.execute('DELETE FROM bucket WHERE bucket_name = ?;', (bucket_name,))

    def get_bucket_id_or_none_from_bucket_name(self, bucket_name):
        try:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    'SELECT bucket_id FROM bucket WHERE bucket_name = ?;', (bucket_name,)
                )
                return cursor.fetchone()[0]
        except:
            return None

    def put_bucket_upload_url(self, bucket_id, upload_url, upload_auth_token):
        with self._get_connection() as conn:
            conn.execute(
                'INSERT INTO bucket_upload_url (bucket_id, upload_url, upload_auth_token) values (?, ?, ?);',
                (bucket_id, upload_url, upload_auth_token)
            )

    def clear_bucket_upload_data(self, bucket_id):
        with self._get_connection() as conn:
            conn.execute('DELETE FROM bucket_upload_url WHERE bucket_id = ?;', (bucket_id,))

    def take_bucket_upload_url(self, bucket_id):
        try:
            with self._get_connection() as conn:
                cursor = conn.execute(
                    'SELECT upload_url, upload_auth_token FROM bucket_upload_url WHERE bucket_id = ?;',
                    (bucket_id,)
                )
                (upload_url, upload_auth_token) = cursor.fetchone()
                conn.execute(
                    'DELETE FROM bucket_upload_url WHERE upload_auth_token = ?;',
                    (upload_auth_token,)
                )
                return (upload_url, upload_auth_token)
        except:
            return (None, None)

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

    def refresh_entire_bucket_name_cache(self, name_id_iterable):
        self.buckets = {}

    def get_bucket_id_or_none_from_bucket_name(self, bucket_name):
        return None

    def save_bucket(self, bucket):
        pass

    def remove_bucket_name(self, bucket_name):
        pass

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


def test_upload_url_concurrency():
    # Clean up from previous tests
    file_name = '/tmp/test_upload_conncurrency.db'
    try:
        os.unlink(file_name)
    except OSError:
        pass

    # Make an account info with a bunch of upload URLs in it.
    account_info = SqliteAccountInfo(file_name)
    available_urls = set()
    for i in six.moves.range(3000):
        url = 'url_%d' % i
        account_info.put_bucket_upload_url('bucket-id', url, 'auth-token-%d' % i)
        available_urls.add(url)

    # Pull them all from the account info, from multiple threads
    lock = threading.Lock()

    def run_thread():
        while True:
            (url, _) = account_info.take_bucket_upload_url('bucket-id')
            if url is None:
                break
            with lock:
                if url in available_urls:
                    available_urls.remove(url)
                else:
                    print('DOUBLE:', url)

    threads = []
    for i in six.moves.range(5):
        thread = threading.Thread(target=run_thread)
        thread.start()
        threads.append(thread)
    for t in threads:
        t.join()

    # Check
    if len(available_urls) != 0:
        print('LEAK:', available_urls)

    # Clean up
    os.unlink(file_name)
