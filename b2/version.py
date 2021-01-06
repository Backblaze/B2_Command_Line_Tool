######################################################################
#
# File: b2/version.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

try:
    from importlib.metadata import version
except ImportError:  # ModuleNotFoundError is not available in Python 3.5
    from importlib_metadata import version

VERSION = version('b2')
