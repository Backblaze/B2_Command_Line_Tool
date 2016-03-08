######################################################################
#
# File: __init__.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from .b2 import main

assert main  # silence pyflakes

# These are for tests.
# TODO: find a way to make them avaible to tests, but not be public
from .b2 import LocalFolder

