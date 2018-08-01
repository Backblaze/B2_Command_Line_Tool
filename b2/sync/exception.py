######################################################################
#
# File: b2/sync/exception.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from ..exception import B2Error


class EnvironmentEncodingError(B2Error):
    def __init__(self, filename, encoding):
        super(EnvironmentEncodingError, self).__init__()
        self.filename = filename
        self.encoding = encoding

    def __str__(self):
        return """file name %s cannot be decoded with system encoding (%s).
We think this is an environment error which you should workaround by
setting your system encoding properly, for example like this:
export LANG=en_US.UTF-8""" % (
            self.filename,
            self.encoding,
        )
