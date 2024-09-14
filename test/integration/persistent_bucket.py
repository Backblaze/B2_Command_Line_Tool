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
import sys
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from test.integration.helpers import BUCKET_NAME_LENGTH, Api

import backoff
from b2sdk.v2 import Bucket, SqliteAccountInfo
from b2sdk.v2.exception import NonExistentBucket

PERSISTENT_BUCKET_NAME_PREFIX = "constst"


@dataclass
class PersistentBucketAggregate:
    bucket_name: str
    subfolder: str

    @cached_property
    def virtual_bucket_name(self):
        return f"{self.bucket_name}/{self.subfolder}"


@backoff.on_exception(backoff.expo, Exception, max_tries=3, max_time=10)
def delete_all_files(bucket: Bucket):
    all_items = list(bucket.ls(recursive=True))
    for item, _ in all_items:
        bucket.delete_file_version(item.id_, item.file_name)


@backoff.on_exception(backoff.expo, Exception, max_tries=3, max_time=10)
def delete_files(bucket: Bucket, subfolder: str):
    for file_version, _ in bucket.ls(recursive=True, folder_to_list=subfolder):
        bucket.delete_file_version(file_version.id_, file_version.file_name)


def cleanup_persistent_bucket(b2_api: Api):
    all_buckets = b2_api.api.list_buckets()
    for bucket in all_buckets:
        if bucket.name.startswith(PERSISTENT_BUCKET_NAME_PREFIX):
            print(f"Deleting all files in bucket {bucket.name}", flush=True, file=sys.stderr)
            delete_all_files(bucket)


def get_persistent_bucket_name(b2_api: Api, account_info_file: Path) -> str:
    if "CI" in os.environ:
        # CI environment
        repo_id = os.environ.get("GITHUB_REPOSITORY_ID")
        if not repo_id:
            raise ValueError("GITHUB_REPOSITORY_ID is not set")
        bucket_hash = hashlib.sha256(repo_id.encode()).hexdigest()
    else:
        # Local development
        account_info = SqliteAccountInfo(file_name=account_info_file)
        bucket_hash = hashlib.sha256(account_info.get_account_id().encode()).hexdigest()

    return f"{PERSISTENT_BUCKET_NAME_PREFIX}-{bucket_hash}" [:BUCKET_NAME_LENGTH]


def get_or_create_persistent_bucket(b2_api: Api, account_info_file: Path) -> Bucket:
    bucket_name = get_persistent_bucket_name(b2_api, account_info_file)
    try:
        bucket = b2_api.api.get_bucket_by_name(bucket_name)
    except NonExistentBucket:
        bucket = b2_api.api.create_bucket(
            bucket_name,
            bucket_type="allPublic",
            lifecycle_rules=[
                {
                    "daysFromHidingToDeleting": 1,
                    "daysFromUploadingToHiding": 14,
                    "fileNamePrefix": "",
                }
            ],
        )
    # add the new bucket name to the list of bucket names
    b2_api.bucket_name_log.append(bucket_name)
    return bucket
