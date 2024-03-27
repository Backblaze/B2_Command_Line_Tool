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
    """
        {BaseRm}

        Examples.

    .. note::

        Note the use of quotes, to ensure that special
        characters are not expanded by the shell.


    .. note::

        Use with caution. Running examples presented below can cause data-loss.


    Remove all csv and tsv files (in any directory, in the whole bucket):

    .. code-block::

        {NAME} rm --recursive --withWildcard bucketName "*.[ct]sv"


    Remove all info.txt files from buckets bX, where X is any character:

    .. code-block::

        {NAME} rm --recursive --withWildcard bucketName "b?/info.txt"


    Remove all pdf files from buckets b0 to b9 (including sub-directories):

    .. code-block::

        {NAME} rm --recursive --withWildcard bucketName "b[0-9]/*.pdf"


        Requires capability:

        - **listFiles**
        - **deleteFiles**
        """
