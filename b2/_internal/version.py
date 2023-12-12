######################################################################
#
# File: b2/_internal/version.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

try:
    from importlib.metadata import version
except ModuleNotFoundError:
    from importlib_metadata import version

VERSION = version('b2')
