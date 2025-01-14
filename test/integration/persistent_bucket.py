######################################################################
#
# File: test/integration/persistent_bucket.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import hashlib
import os
from dataclasses import dataclass
from functools import cached_property
from typing import List

import backoff
from b2sdk.v2 import Bucket
from b2sdk.v2.exception import DuplicateBucketName, NonExistentBucket

from test.integration.helpers import BUCKET_NAME_LENGTH, Api

PERSISTENT_BUCKET_NAME_PREFIX = 'constst'


@dataclass
class PersistentBucketAggregate:
    bucket_name: str
    subfolder: str

    @cached_property
    def virtual_bucket_name(self):
        return f'{self.bucket_name}/{self.subfolder}'


def get_persistent_bucket_name(b2_api: Api) -> str:
    bucket_base = os.environ.get('GITHUB_REPOSITORY_ID', b2_api.api.get_account_id())
    bucket_hash = hashlib.sha256(bucket_base.encode()).hexdigest()
    return f'{PERSISTENT_BUCKET_NAME_PREFIX}-{bucket_hash}'[:BUCKET_NAME_LENGTH]


@backoff.on_exception(
    backoff.expo,
    DuplicateBucketName,
    max_tries=3,
    jitter=backoff.full_jitter,
)
def get_or_create_persistent_bucket(b2_api: Api) -> Bucket:
    bucket_name = get_persistent_bucket_name(b2_api)
    try:
        bucket = b2_api.api.get_bucket_by_name(bucket_name)
    except NonExistentBucket:
        bucket = b2_api.api.create_bucket(
            bucket_name,
            bucket_type='allPublic',
            lifecycle_rules=[
                {
                    'daysFromHidingToDeleting': 1,
                    'daysFromUploadingToHiding': 1,
                    'fileNamePrefix': '',
                }
            ],
        )
    # add the new bucket name to the list of bucket names
    b2_api.bucket_name_log.append(bucket_name)
    return bucket

def prune_used_files(b2_api: Api, bucket: Bucket, folders: List[str]):
    b2_api.clean_bucket(bucket_object=bucket, only_files=True, only_folders=folders,ignore_retentions=True)
