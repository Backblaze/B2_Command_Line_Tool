######################################################################
#
# File: test/unit/test_filesystem.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os

from b2._filesystem import points_to_fifo


def test_points_to_fifo__doesnt_exist(tmpdir):
    non_existent = tmpdir.join('non-existent')
    assert not non_existent.exists()
    assert not points_to_fifo(str(non_existent))


def test_points_to_fifo__named_pipe(tmpdir):
    named_pipe = tmpdir.join('fifo')
    os.mkfifo(str(named_pipe))
    assert points_to_fifo(str(named_pipe))


def test_points_to_fifo__regular_file(tmpdir):
    regular_file = tmpdir.join('regular')
    regular_file.write('hello')
    assert not points_to_fifo(str(regular_file))
