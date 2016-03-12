######################################################################
#
# File: __main__.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import absolute_import

import sys

import six

from .b2 import B2Error, MissingAccountData
from .console_tool import ConsoleTool


def decode_sys_argv():
    """
    Returns the command-line arguments as unicode strings, decoding
    whatever format they are in.

    https://stackoverflow.com/questions/846850/read-unicode-characters-from-command-line-arguments-in-python-2-x-on-windows
    """
    encoding = sys.getfilesystemencoding()
    if six.PY2:
        return [arg.decode(encoding) for arg in sys.argv]
    return sys.argv


def main():
    ct = ConsoleTool(stdout=sys.stdout, stderr=sys.stderr)

    if len(sys.argv) < 2:
        ct._usage_and_exit()

    decoded_argv = decode_sys_argv()

    action = decoded_argv[1]
    args = decoded_argv[2:]

    try:
        if action == 'authorize_account':
            ct.authorize_account(args)
        elif action == 'clear_account':
            ct.clear_account(args)
        elif action == 'create_bucket':
            ct.create_bucket(args)
        elif action == 'delete_bucket':
            ct.delete_bucket(args)
        elif action == 'delete_file_version':
            ct.delete_file_version(args)
        elif action == 'download_file_by_id':
            ct.download_file_by_id(args)
        elif action == 'download_file_by_name':
            ct.download_file_by_name(args)
        elif action == 'get_file_info':
            ct.get_file_info(args)
        elif action == 'hide_file':
            ct.hide_file(args)
        elif action == 'list_buckets':
            ct.list_buckets(args)
        elif action == 'list_file_names':
            ct.list_file_names(args)
        elif action == 'list_file_versions':
            ct.list_file_versions(args)
        elif action == 'ls':
            ct.ls(args)
        elif action == 'make_url':
            ct.make_url(args)
        elif action == 'sync':
            ct.sync(args)
        elif action == 'update_bucket':
            ct.update_bucket(args)
        elif action == 'upload_file':
            ct.upload_file(args)
        elif action == 'version':
            ct.version()
        else:
            ct._usage_and_exit()
    except MissingAccountData:
        print('ERROR: Missing account.  Use: b2 authorize_account')
        sys.exit(1)
    except B2Error as e:
        print('ERROR: %s' % (e,))
        sys.exit(1)
