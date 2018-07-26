######################################################################
#
# File: b2/account_info/abstract.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import abstractmethod

import six

from b2.raw_api import ALL_CAPABILITIES
from b2.utils import B2TraceMetaAbstract, limit_trace_arguments


@six.add_metaclass(B2TraceMetaAbstract)
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
        'production': 'https://api.backblazeb2.com',
        'dev': 'http://api.backblazeb2.xyz:8180',
        'staging': 'https://api.backblaze.net',
    }

    # The 'allowed' structure to use for old account info that was saved without 'allowed'.
    DEFAULT_ALLOWED = dict(
        bucketId=None,
        bucketName=None,
        capabilities=ALL_CAPABILITIES,
        namePrefix=None,
    )

    @classmethod
    def all_capabilities(cls):
        return cls.ALL_CAPABILITIES

    @abstractmethod
    def clear(self):
        """
        Removes all stored information
        """

    @abstractmethod
    @limit_trace_arguments(only=['self'])
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
    def get_allowed(self):
        """
        An 'allowed' dict, as returned by b2_authorize_account.
        Never None; for account info that was saved before 'allowed' existed,
        returns DEFAULT_ALLOWED.
        """

    @limit_trace_arguments(only=['self', 'api_url', 'download_url', 'minimum_part_size', 'realm'])
    def set_auth_data(
        self,
        account_id,
        auth_token,
        api_url,
        download_url,
        minimum_part_size,
        application_key,
        realm,
        allowed=None,
    ):
        """
        Stores the results of b2_authorize_account.

        All of the information returned by b2_authorize_account is saved, because all of it is
        needed by some command.

        The allowed structure is the one returned b2_authorize_account, with the addition of
        a bucketName field.  For keys with bucket restrictions, the name of the bucket is looked
        up and stored, too.  The console_tool does everything by bucket name, so it's convenient
        to have the restricted bucket name handy.
        """
        if allowed is None:
            allowed = self.DEFAULT_ALLOWED
        assert self.allowed_is_valid(allowed)
        self._set_auth_data(
            account_id,
            auth_token,
            api_url,
            download_url,
            minimum_part_size,
            application_key,
            realm,
            allowed,
        )

    @classmethod
    def allowed_is_valid(cls, allowed):
        """
        Makes sure that all of the required fields are present, and that
        bucketId is set if bucketName is.

        If the bucketId is for a bucket that no longer exists, or the
        capabilities do not allow listBuckets, then we won't have a bucketName.
        """
        return (
            ('bucketId' in allowed) and ('bucketName' in allowed) and
            ((allowed['bucketId'] is not None) or (allowed['bucketName'] is None)) and
            ('capabilities' in allowed) and ('namePrefix' in allowed)
        )

    @abstractmethod
    def _set_auth_data(
        self, account_id, auth_token, api_url, download_url, minimum_part_size, application_key,
        realm, allowed
    ):
        """
        Stores the auth data.  Can assume that 'allowed' is present and valid.
        """

    @abstractmethod
    def take_bucket_upload_url(self, bucket_id):
        """
        Returns a pair (upload_url, upload_auth_token) that has been removed
        from the pool for this bucket, or (None, None) if there are no more
        left.
        """

    @abstractmethod
    @limit_trace_arguments(only=['self', 'bucket_id'])
    def put_bucket_upload_url(self, bucket_id, upload_url, upload_auth_token):
        """
        Add an (upload_url, upload_auth_token) pair to the pool available for
        the bucket.
        """

    @abstractmethod
    @limit_trace_arguments(only=['self'])
    def put_large_file_upload_url(self, file_id, upload_url, upload_auth_token):
        pass

    @abstractmethod
    def take_large_file_upload_url(self, file_id):
        pass

    @abstractmethod
    def clear_large_file_upload_urls(self, file_id):
        pass
