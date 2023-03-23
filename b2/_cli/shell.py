######################################################################
#
# File: b2/_cli/shell.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import os
import os.path
from typing import Optional


def detect_shell() -> Optional[str]:
    """Detect the shell we are running in."""
    shell_var = os.environ.get('SHELL')
    if shell_var:
        return os.path.basename(shell_var)
    return None
