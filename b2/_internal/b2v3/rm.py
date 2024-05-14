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

import dataclasses
import typing

from b2._internal.b2v4.registry import B2URIBucketNFolderNameArgMixin, BaseRm

if typing.TYPE_CHECKING:
    import argparse

    from b2._internal._utils.uri import B2URI


class B2URIMustPointToFolderMixin:
    """
    Extension to B2URI*Mixins to ensure that the b2:// URIs point to a folder.

    This is directly related to how b2sdk.v3.Bucket.ls() treats paths ending with a slash as folders, where as
    paths not ending with a slash are treated as potential files.

    For b2v3 we need to support old behavior which never attempted to treat the path as a file.
    """

    def get_b2_uri_from_arg(self, args: argparse.Namespace) -> B2URI:
        b2_uri = super().get_b2_uri_from_arg(args)
        if b2_uri.path and not args.with_wildcard and not b2_uri.path.endswith("/"):
            b2_uri = dataclasses.replace(b2_uri, path=b2_uri.path + "/")
        return b2_uri


# NOTE: We need to keep v3 Rm in separate file, because we need to import it in
# unit tests without registering any commands.
class Rm(B2URIMustPointToFolderMixin, B2URIBucketNFolderNameArgMixin, BaseRm):
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
        - **bypassGovernance** (if --bypass-governance is used)
    """
