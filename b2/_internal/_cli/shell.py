######################################################################
#
# File: b2/_internal/_cli/shell.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import os
import os.path
import shutil
from typing import Optional


def detect_shell() -> Optional[str]:
    """Detect the shell we are running in."""
    shell_var = os.environ.get('SHELL')
    if shell_var:
        return os.path.basename(shell_var)
    return None


def resolve_short_call_name(binary_path: str) -> str:
    """
    Resolve the short name of the binary.

    If binary is in PATH, return only basename, otherwise return a full path.
    This method is to be used with sys.argv[0] to resolve handy name for the user instead of full path.
    """
    if shutil.which(binary_path) == binary_path:
        return os.path.basename(binary_path)
    return binary_path
