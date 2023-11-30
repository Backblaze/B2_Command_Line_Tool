######################################################################
#
# File: test/unit/console_tool/conftest.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os
import sys

import pytest

import b2.console_tool


@pytest.fixture
def cwd_path(tmp_path):
    """Set up a test directory and return its path."""
    prev_cwd = os.getcwd()
    os.chdir(tmp_path)
    yield tmp_path
    os.chdir(prev_cwd)


@pytest.fixture
def b2_cli_log_fix(caplog):
    caplog.set_level(0)  # prevent pytest from blocking logs
    b2.console_tool.logger.setLevel(0)  # reset logger level to default


@pytest.fixture
def mock_stdin(monkeypatch):
    out_, in_ = os.pipe()
    monkeypatch.setattr(sys, 'stdin', os.fdopen(out_))
    in_f = open(in_, 'w')
    yield in_f
    in_f.close()