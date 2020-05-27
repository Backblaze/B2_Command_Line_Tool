######################################################################
#
# File: b2/cli_bucket.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2sdk import v1


class CliBucket(v1.Bucket):
    def list_file_names(self, start_filename=None, max_entries=None, prefix=None):
        """
        Legacy interface which just returns whatever remote API returns.
        According to b2sdk version policy, usage of B2Session requires more strict pinning
        """
        return self.api.session.list_file_names(self.id_, start_filename, max_entries, prefix)

    def list_file_versions(
        self, start_filename=None, start_file_id=None, max_entries=None, prefix=None
    ):
        """
        Legacy interface which just returns whatever remote API returns.
        According to b2sdk version policy, usage of B2Session requires more strict pinning
        """
        return self.api.session.list_file_versions(
            self.id_, start_filename, start_file_id, max_entries, prefix
        )


class CliBucketFactory(v1.BucketFactory):
    """
    This is a factory for creating bucket objects from different kind of objects.
    """
    BUCKET_CLASS = staticmethod(CliBucket)
