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

from b2._cli.b2api import _get_b2api_for_profile


def _with_api(func):
    @wraps(func)
    def wrapper(prefix, parsed_args, **kwargs):
        api = _get_b2api_for_profile(parsed_args.profile)
        return func(prefix=prefix, parsed_args=parsed_args, api=api, **kwargs)

    return wrapper


@_with_api
def bucket_name_completer(api, **kwargs):
    return [bucket.name for bucket in api.list_buckets()]
