######################################################################
#
# File: raw_simulator.py
#
# Copyright 2015 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from .b2 import DuplicateBucketName


class BucketSimulator(object):
    def __init__(self, account_id, bucket_name, bucket_type):
        assert bucket_type in ['allPrivate', 'allPublic']
        self.account_id = account_id
        self.bucket_name = bucket_name
        self.bucket_id = 'BUCKET:' + bucket_name
        self.bucket_type = bucket_type

    def bucket_json(self):
        return dict(
            accountId=self.account_id,
            bucketName=self.bucket_name,
            bucketId=self.bucket_id,
            bucketType=self.bucket_type
        )


class RawSimulator(object):
    """
    Implements the same interface as B2RawApi by simulating all of the
    calls and keeping state in memory.

    The intended use for this class is for unit tests that test things
    built on top of B2RawApi.
    """

    API_URL = 'http://api.example.com'
    DOWNLOAD_URL = 'http://download.example.com'

    MIN_PART_SIZE = 10000

    def __init__(self):
        self.authorized_accounts = set()
        self.bucket_name_to_bucket = dict()

    def authorize_account(self, _realm_url, account_id, _application_key):
        self.authorized_accounts.add(account_id)
        return dict(
            accountId=account_id,
            authorizationToken='AUTH:' + account_id,
            apiUrl=self.API_URL,
            downloadUrl=self.DOWNLOAD_URL,
            minimumPartSize=self.MIN_PART_SIZE
        )

    def create_bucket(self, api_url, account_auth_token, account_id, bucket_name, bucket_type):
        self._assert_account_auth(api_url, account_auth_token, account_id)
        if bucket_name in self.bucket_name_to_bucket:
            raise DuplicateBucketName(bucket_name)
        bucket_sim = BucketSimulator(account_id, bucket_name, bucket_type)
        self.bucket_name_to_bucket[bucket_name] = bucket_sim
        return bucket_sim.bucket_json()

    def list_file_names(
        self,
        api_url,
        account_auth,
        bucket_id,
        start_file_name=None,
        max_file_count=None
    ):
        return dict(files=[], nextFileName=None)

    def _assert_account_auth(self, api_url, account_auth_token, account_id):
        assert api_url == self.API_URL
        assert account_auth_token == 'AUTH:' + account_id
        assert account_id in self.authorized_accounts
