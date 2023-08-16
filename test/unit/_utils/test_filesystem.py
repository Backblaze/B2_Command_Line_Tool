######################################################################
#
# File: test/unit/_utils/test_filesystem.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os
from test.helpers import skip_on_windows

from b2._utils.filesystem import points_to_fifo


def test_points_to_fifo__doesnt_exist(tmp_path):
    non_existent = tmp_path / 'non-existent'
    assert not non_existent.exists()
    assert not points_to_fifo(non_existent)


@skip_on_windows
def test_points_to_fifo__named_pipe(tmp_path):
    named_pipe = tmp_path / 'fifo'
    os.mkfifo(str(named_pipe))
    assert points_to_fifo(named_pipe)


def test_points_to_fifo__regular_file(tmp_path):
    regular_file = tmp_path / 'regular'
    regular_file.write_text('hello')
    assert not points_to_fifo(regular_file)
