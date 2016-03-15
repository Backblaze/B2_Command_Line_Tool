######################################################################
#
# File: __init__.py
#
# Copyright 2016 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

# These are for tests.
# TODO: find a way to make them available to tests, but not be public
from .b2 import File, Folder, LocalFolder, FileVersion, make_folder_sync_actions, zip_folders

assert File or FileVersion or Folder or LocalFolder or make_folder_sync_actions or zip_folders  # silence pyflakes
