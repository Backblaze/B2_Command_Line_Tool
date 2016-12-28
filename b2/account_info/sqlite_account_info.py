######################################################################
#
# File: b2/account_info/sqlite_account_info.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import json
import logging
import os
import platform
import stat
import threading

from .exception import (CorruptAccountInfo, MissingAccountData)
from .upload_url_pool import UrlPoolAccountInfo

if not platform.system().lower().startswith('java'):
    # in Jython 2.7.1b3 there is no sqlite3
    import sqlite3

logger = logging.getLogger(__name__)


class SqliteAccountInfo(UrlPoolAccountInfo):
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
        super(SqliteAccountInfo, self).__init__()

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
        except AttributeError:
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
        # This table is not used any more.  We may use it again
        # someday if we save upload URLs across invocations of
        # the command-line tool.
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
            logger.exception(
                '_get_account_info_or_raise encountered a problem while trying to retrieve "%s"',
                column_name
            )
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
        except TypeError:  # TypeError: 'NoneType' object is unsubscriptable
            return None
        except sqlite3.Error:
            return None
