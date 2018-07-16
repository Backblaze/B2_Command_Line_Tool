######################################################################
#
# File: b2/account_info/allowed_policy.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from b2.exception import BucketNotAllowed, CapabilityNotAllowed, FileNameNotAllowed


def check_command_allowed(capability, named_bucket, named_file, account_info):
    """
    Checks whether the 'allowed' information for the account allows the requested
    operation.

    If not allowed, throws a B2Error with a helpful message about what's not
    allowed.  The error messages from the service just say 'unauthorized', and
    do not help diagnose the problem.

    :param capability: The capability required for the requested command.
    :param named_bucket: The bucket mentioned on the command line, or None.
    :param named_file: The file name (or prefix) mentioned on the command line,
                       or none.  Listing files without a prefix should use ''
                       to indicate it wants to see all files.
    :param account_info: The stored account info.
    """

    # Special case for account info stored before capabilities were implemented.
    # If that happened, it must have been authorized with an account master key,
    # and everything will be allowed.
    allowed = account_info.get_allowed()
    if allowed is None:
        return

    # Check that the requested capability is in the allowed list.
    # TODO: remove the check for 'all' after the service bug is fixed
    if capability not in allowed['capabilities']:
        if 'all' not in allowed['capabilities']:
            raise CapabilityNotAllowed("application key does not allow '%s'" % capability)

    # If there is a bucket restriction, then all requests must name the bucket.
    restricted_bucket = allowed.get('bucketName')
    if restricted_bucket is not None:
        if named_bucket is None or named_bucket != restricted_bucket:
            raise BucketNotAllowed(
                "application key does not allow access to buckets other than '%s'" %
                restricted_bucket
            )

    # If there is a name restriction, then all requests that deal with files
    # must stay within the restriction.
    restricted_prefix = allowed.get('namePrefix')
    if restricted_prefix is not None:
        if named_file is None or not named_file.startswith(restricted_prefix):
            raise FileNameNotAllowed(
                "application key does not allow access to files whose name does not start with '%s'"
                % restricted_prefix
            )
