######################################################################
#
# File: test/unit/test_apiver.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import unittest

import pytest


@pytest.fixture
def inject_cli_int_version(request, cli_int_version):
    request.cls.cli_int_version = cli_int_version


@pytest.mark.usefixtures('inject_cli_int_version')
class UnitTestClass(unittest.TestCase):
    cli_int_version: int

    @pytest.mark.cli_version(to_version=3)
    def test_passes_below_and_on_v3(self):
        assert self.cli_int_version <= 3

    @pytest.mark.cli_version(from_version=4)
    def test_passes_above_and_on_v4(self):
        assert self.cli_int_version >= 4

    @pytest.mark.cli_version(3)
    def test_passes_only_on_v3(self):
        assert self.cli_int_version == 3

    @pytest.mark.cli_version(4)
    def test_passes_only_on_v4(self):
        assert self.cli_int_version == 4

    @pytest.mark.cli_version(3, 4)
    def test_passes_on_both_v3_and_v4(self):
        assert self.cli_int_version in {3, 4}


@pytest.mark.cli_version(to_version=3)
def test_passes_below_and_on_v3(cli_int_version):
    assert cli_int_version <= 3


@pytest.mark.cli_version(from_version=4)
def test_passes_above_and_on_v4(cli_int_version):
    assert cli_int_version >= 4


@pytest.mark.cli_version(3)
def test_passes_only_on_v3(cli_int_version):
    assert cli_int_version == 3


@pytest.mark.cli_version(4)
def test_passes_only_on_v4(cli_int_version):
    assert cli_int_version == 4


@pytest.mark.cli_version(3, 4)
def test_passes_on_both_v3_and_v4(cli_int_version):
    assert cli_int_version in {3, 4}
