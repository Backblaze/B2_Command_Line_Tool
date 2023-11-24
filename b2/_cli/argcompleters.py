######################################################################
#
# File: b2/_cli/argcompleters.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

# We import all the necessary modules lazily in completers in order
# to avoid upfront cost of the imports when argcompleter is used for
# autocompletions.

from itertools import islice


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
    from b2sdk.v2 import LIST_FILE_NAMES_MAX_LIMIT

    from b2._cli.b2api import _get_b2api_for_profile

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


def b2uri_file_completer(prefix: str, parsed_args, **kwargs):
    """
    Complete B2 URI pointing to a file-like object in a bucket.
    """
    from b2sdk.v2 import LIST_FILE_NAMES_MAX_LIMIT

    from b2._cli.b2api import _get_b2api_for_profile
    from b2._utils.python_compat import removeprefix
    from b2._utils.uri import parse_b2_uri

    api = _get_b2api_for_profile(getattr(parsed_args, 'profile', None))
    if prefix.startswith('b2://'):
        prefix_without_scheme = removeprefix(prefix, 'b2://')
        if '/' not in prefix_without_scheme:
            return [f"b2://{bucket.name}/" for bucket in api.list_buckets(use_cache=True)]

        b2_uri = parse_b2_uri(prefix)
        bucket = api.get_bucket_by_name(b2_uri.bucket_name)
        file_versions = bucket.ls(
            f"{b2_uri.path}*",
            latest_only=True,
            recursive=True,
            fetch_count=LIST_FILE_NAMES_MAX_LIMIT,
            with_wildcard=True,
        )
        return [
            f"b2://{bucket.name}/{file_version.file_name}"
            for file_version, folder_name in islice(file_versions, LIST_FILE_NAMES_MAX_LIMIT)
            if file_version
        ]
    elif prefix.startswith('b2id://'):
        # listing all files from all buckets is unreasonably expensive
        return ["b2id://"]
    else:
        return [
            "b2://",
            "b2id://",
        ]
