######################################################################
#
# File: test/unit/_cli/test_shell.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import os
from unittest import mock

from b2._cli import shell


@mock.patch.dict(os.environ, {"SHELL": "/bin/bash"})
def test_detect_shell():
    assert shell.detect_shell() == 'bash'
