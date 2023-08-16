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
from test.unit.test_console_tool import BaseConsoleToolTest

import pytest


class ConsoleToolTester(BaseConsoleToolTest):
    def authorize(self):
        self._authorize_account()

    def run(self, *args, **kwargs):
        return self._run_command(*args, **kwargs)


@pytest.fixture
def b2_cli():
    cli_tester = ConsoleToolTester()
    cli_tester.setUp()
    yield cli_tester
    cli_tester.tearDown()


@pytest.fixture
def authorized_b2_cli(b2_cli):
    b2_cli.authorize()
    yield b2_cli


@pytest.fixture
def bucket(b2_cli, authorized_b2_cli):
    bucket_name = "my-bucket"
    b2_cli.run(['create-bucket', bucket_name, 'allPublic'], expected_stdout='bucket_0\n')
    yield bucket_name


@pytest.fixture
def mock_stdin(monkeypatch):
    out_, in_ = os.pipe()
    monkeypatch.setattr(sys, 'stdin', os.fdopen(out_))
    in_f = open(in_, 'w')
    yield in_f
    in_f.close()
