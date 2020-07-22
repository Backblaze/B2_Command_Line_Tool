######################################################################
#
# File: b2/__init__.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

# Set default logging handler to avoid "No handler found" warnings.
import logging  # noqa

logging.getLogger(__name__).addHandler(logging.NullHandler())

import b2.version
__version__ = b2.version.VERSION
assert __version__  # PEP-0396
