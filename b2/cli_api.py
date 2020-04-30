######################################################################
#
# File: b2/cli_api.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk import v1
from .cli_bucket import CliBucket, CliBucketFactory


class CliB2Api(v1.B2Api):
    BUCKET_FACTORY_CLASS = staticmethod(CliBucketFactory)
    BUCKET_CLASS = staticmethod(CliBucket)

    def delete_bucket(self, bucket):
        """
        Delete the chosen bucket.

        For legacy reasons it returns whatever server sends in response,
        but API user should not rely on the response: if it doesn't raise
        an exception, it means that the operation was a success.

        :param b2sdk.v1.Bucket bucket: a :term:`bucket` to delete
        """
        account_id = self.account_info.get_account_id()
        return self.session.delete_bucket(account_id, bucket.id_)

