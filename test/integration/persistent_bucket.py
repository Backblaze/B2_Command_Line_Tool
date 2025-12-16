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

import tenacity
from b2sdk.v3 import Bucket
from b2sdk.v3.exception import DuplicateBucketName, NonExistentBucket
from b2sdk.v3.testing import BUCKET_NAME_LENGTH, BucketManager

PERSISTENT_BUCKET_NAME_PREFIX = 'constst'


@dataclass
class PersistentBucketAggregate:
    bucket_name: str
    subfolder: str

    @cached_property
    def virtual_bucket_name(self):
        return f'{self.bucket_name}/{self.subfolder}'


def get_persistent_bucket_name(bucket_manager: BucketManager) -> str:
    bucket_base = os.environ.get('GITHUB_REPOSITORY_ID', bucket_manager.b2_api.get_account_id())
    bucket_hash = hashlib.sha256(bucket_base.encode()).hexdigest()
    return f'{PERSISTENT_BUCKET_NAME_PREFIX}-{bucket_hash}'[:BUCKET_NAME_LENGTH]


@tenacity.retry(
    retry=tenacity.retry_if_exception_type(DuplicateBucketName),
    wait=tenacity.wait_exponential_jitter(),
    stop=tenacity.stop_after_attempt(3),
)
def get_or_create_persistent_bucket(bucket_manager: BucketManager) -> Bucket:
    bucket_name = get_persistent_bucket_name(bucket_manager)
    try:
        bucket = bucket_manager.b2_api.get_bucket_by_name(bucket_name)
    except NonExistentBucket:
        bucket = bucket_manager.b2_api.create_bucket(
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
    bucket_manager.bucket_name_log.append(bucket_name)
    return bucket


def prune_used_files(bucket_manager: BucketManager, bucket: Bucket, folders: list[str]):
    bucket_manager.clean_bucket(
        bucket=bucket, only_files=True, only_folders=folders, ignore_retentions=True
    )
