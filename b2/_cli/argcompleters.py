######################################################################
#
# File: b2/_cli/argcompleters.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from functools import wraps
from itertools import islice

from b2sdk.v2 import LIST_FILE_NAMES_MAX_LIMIT
from b2sdk.v2.api import B2Api

from b2._cli.b2api import _get_b2api_for_profile


def _with_api(func):
    """Decorator to inject B2Api instance into argcompleter function."""

    @wraps(func)
    def wrapper(prefix, parsed_args, **kwargs):
        api = _get_b2api_for_profile(parsed_args.profile)
        return func(prefix=prefix, parsed_args=parsed_args, api=api, **kwargs)

    return wrapper


@_with_api
def bucket_name_completer(api: B2Api, **kwargs):
    return [bucket.name for bucket in api.list_buckets(use_cache=True)]


@_with_api
def file_name_completer(api: B2Api, parsed_args, **kwargs):
    """
    Completes file names in a bucket.

    To limit delay & cost only lists files returned from by single call to b2_list_file_names
    """
    bucket = api.get_bucket_by_name(parsed_args.bucketName)
    file_versions = bucket.ls(
        getattr(parsed_args, 'folderName', None) or '',
        latest_only=True,
        recursive=False,
        fetch_count=LIST_FILE_NAMES_MAX_LIMIT,
    )
    return [
        folder_name or file_version.file_name
        for file_version, folder_name in islice(file_versions, LIST_FILE_NAMES_MAX_LIMIT)
    ]
