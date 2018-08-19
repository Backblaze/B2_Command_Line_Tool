######################################################################
#
# File: b2/transferer/abstract.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from abc import abstractmethod

import six

from ..utils import B2TraceMetaAbstract


@six.add_metaclass(B2TraceMetaAbstract)
class AbstractDownloader(object):
    @abstractmethod
    def is_suitable(self, metadata, progress_listener):
        """
        analyzes metadata (possibly against options passed earlier to constructor
        to find out whether the given download request should be handled by this downloader
        """
        pass

    @abstractmethod
    def download(self, file, response, metadata):
        """
        @returns (bytes_read, actual_sha1)
        """
        pass
