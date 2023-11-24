######################################################################
#
# File: b2/_cli/argcompleters.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################


def bucket_name_completer(prefix, parsed_args, **kwargs):
    from b2._cli.b2api import _get_b2api_for_profile
    api = _get_b2api_for_profile(getattr(parsed_args, 'profile', None))
    res = [bucket.name for bucket in api.list_buckets(use_cache=True)]
    return res


def file_name_completer(prefix, parsed_args, **kwargs):
    """
    Completes file names in a bucket.

    To limit delay & cost only lists files returned from by single call to b2_list_file_names
    """
    from itertools import islice

    from b2._cli.b2api import _get_b2api_for_profile
    from b2._cli.const import LIST_FILE_NAMES_MAX_LIMIT

    api = _get_b2api_for_profile(parsed_args.profile)
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
