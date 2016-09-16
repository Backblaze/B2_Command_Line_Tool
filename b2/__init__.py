######################################################################
#
# File: __init__.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

# This is a workaround for a problem with OS X El Capitan.  It has
# six==1.4.1 installed in the system Python, and it cannot be removed.
# Installing with 'pip install b2 --ignore-installed six' will install
# the more recent version, but it's put in /Library, which is *after*
# /System/Library in the path, so it's not found unless the path is
# reordered.
#
# https://github.com/pypa/pip/issues/3165
# http://apple.stackexchange.com/questions/209572/how-to-use-pip-after-the-el-capitan-max-os-x-upgrade/
#
# __init__ is loaded before __main__, so this is the first opportunity
# we have to adjust the path.

import sys
if '/Library/Python/2.7/site-packages' in sys.path:
    sys.path = ['/Library/Python/2.7/site-packages'] + sys.path

# Set default logging handler to avoid "No handler found" warnings.
import logging
try:
    from logging import NullHandler
except ImportError:  # Python 2.6

    class NullHandler(logging.Handler):
        def emit(self, record):
            pass


logging.getLogger(__name__).addHandler(NullHandler())
