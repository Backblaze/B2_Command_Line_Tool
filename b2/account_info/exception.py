######################################################################
#
# File: b2/account_info/exception.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import (ABCMeta)

import six

from ..exception import B2Error


@six.add_metaclass(ABCMeta)
class AccountInfoError(B2Error):
    pass


class CorruptAccountInfo(AccountInfoError):
    def __init__(self, file_name):
        super(CorruptAccountInfo, self).__init__()
        self.file_name = file_name

    def __str__(self):
        return 'Account info file (%s) appears corrupted.  Try removing and then re-authorizing the account.' % (
            self.file_name,
        )


class MissingAccountData(AccountInfoError):
    def __init__(self, key):
        super(MissingAccountData, self).__init__()
        self.key = key

    def __str__(self):
        return 'Missing account data: %s' % (self.key,)
