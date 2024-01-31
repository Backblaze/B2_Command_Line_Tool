######################################################################
#
# File: b2/_internal/b2v3/rm.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2._internal._b2v4.registry import B2URIBucketNFolderNameArgMixin, BaseRm


# NOTE: We need to keep v3 Rm in separate file, because we need to import it in
# unit tests without registering any commands.
class Rm(B2URIBucketNFolderNameArgMixin, BaseRm):
    __doc__ = BaseRm.__doc__
    # TODO: fix doc
