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
def inject_apiver_int(request, apiver_int):
    request.cls.apiver_int = apiver_int


@pytest.mark.usefixtures('inject_apiver_int')
class UnitTestClass(unittest.TestCase):
    apiver_int: int

    @pytest.mark.apiver(to_ver=3)
    def test_passes_below_and_on_v3(self):
        assert self.apiver_int <= 3

    @pytest.mark.apiver(from_ver=4)
    def test_passes_above_and_on_v4(self):
        assert self.apiver_int >= 4

    @pytest.mark.apiver(3)
    def test_passes_only_on_v3(self):
        assert self.apiver_int == 3

    @pytest.mark.apiver(4)
    def test_passes_only_on_v4(self):
        assert self.apiver_int == 4

    @pytest.mark.apiver(3, 4)
    def test_passes_on_both_v3_and_v4(self):
        assert self.apiver_int in {3, 4}


@pytest.mark.apiver(to_ver=3)
def test_passes_below_and_on_v3(apiver_int):
    assert apiver_int <= 3


@pytest.mark.apiver(from_ver=4)
def test_passes_above_and_on_v4(apiver_int):
    assert apiver_int >= 4


@pytest.mark.apiver(3)
def test_passes_only_on_v3(apiver_int):
    assert apiver_int == 3


@pytest.mark.apiver(4)
def test_passes_only_on_v4(apiver_int):
    assert apiver_int == 4


@pytest.mark.apiver(3, 4)
def test_passes_on_both_v3_and_v4(apiver_int):
    assert apiver_int in {3, 4}
