######################################################################
#
# File: b2/sync/folder_parser.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from ..exception import CommandError
from .folder import B2Folder, LocalFolder


def parse_sync_folder(folder_name, api):
    """
    Takes either a local path, or a B2 path, and returns a Folder
    object for it.

    B2 paths look like: b2://bucketName/path/name.  The '//' is optional,
    because the previous sync command didn't use it.

    Anything else is treated like a local folder.
    """
    if folder_name.startswith('b2://'):
        return _parse_bucket_and_folder(folder_name[5:], api)
    elif folder_name.startswith('b2:') and folder_name[3].isalnum():
        return _parse_bucket_and_folder(folder_name[3:], api)
    else:
        return LocalFolder(folder_name)


def _parse_bucket_and_folder(bucket_and_path, api):
    """
    Turns 'my-bucket/foo' into B2Folder(my-bucket, foo)
    """
    if '//' in bucket_and_path:
        raise CommandError("'//' not allowed in path names")
    if '/' not in bucket_and_path:
        bucket_name = bucket_and_path
        folder_name = ''
    else:
        (bucket_name, folder_name) = bucket_and_path.split('/', 1)
    if folder_name.endswith('/'):
        folder_name = folder_name[:-1]
    return B2Folder(bucket_name, folder_name, api)
