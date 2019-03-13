######################################################################
#
# File: __init__.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
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
import warnings
from .import_hooks import ProxyImporter
importer = ProxyImporter(__name__, 'b2sdk')


@importer.exclude_predicate
def exclude_modules(source_name, fullname):
    """
    Determine which modules to exclude from being handled by an import hook
    """
    names = ['console_tool', 'utils', 'version', 'time', '__main__', 'b2sdk', 'parse_args']
    excl_names = {'{0}.{1}'.format(source_name, n) for n in names}
    return fullname in excl_names


@importer.callback
def show_warning(orig_name, target_name):
    """
    Show deprecation warnig if some modules was imported from b2 package,
    but should be imported from b2sdk.
    """
    message = '{0} is deprecated, use {1} instead'.format(orig_name, target_name)
    warnings.warn(message, DeprecationWarning)


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
sys.meta_path.insert(0, importer)
