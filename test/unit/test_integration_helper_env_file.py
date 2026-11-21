######################################################################
#
# File: test/unit/test_integration_helper_env_file.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import json
import os
import sys
from pathlib import Path

import pytest

from test.integration.helpers import CommandLine


def make_tool(command):
    return CommandLine(command, None, None, None, None, 'ENVFILE', None, None)


@pytest.mark.skipif(os.name == 'nt', reason='POSIX file permissions and umask')
def test_env_file_is_private_and_removed_after_child_reads_it(tmp_path):
    child = tmp_path / 'read_env.py'
    child.write_text(
        'import json, pathlib, stat, sys\n'
        'p = pathlib.Path(sys.argv[1])\n'
        "print(json.dumps([str(p), stat.S_IMODE(p.stat().st_mode), 'C32_MARKER=present' in p.read_text()]))\n"
    )
    tool = make_tool(f'{sys.executable} {child} ENVFILE')
    previous_umask = os.umask(0)
    path = None
    try:
        status, stdout, _ = tool.execute(additional_env={'C32_MARKER': 'present'})
        assert status == 0
        path_text, mode, child_read_marker = json.loads(stdout)
        path = Path(path_text)
        assert child_read_marker
        assert mode == 0o600
        assert not path.exists()
    finally:
        os.umask(previous_umask)
        if path is not None:
            path.unlink(missing_ok=True)


def test_env_file_is_removed_if_process_start_fails(monkeypatch):
    tool = make_tool('missing-executable ENVFILE')
    created_path = None

    def fail_start(command, **kwargs):
        nonlocal created_path
        created_path = Path(command[1])
        assert created_path.is_file()
        raise OSError('cannot start')

    monkeypatch.setattr('test.integration.helpers.subprocess.Popen', fail_start)
    try:
        with pytest.raises(OSError, match='cannot start'):
            tool.execute(additional_env={'C32_MARKER': 'present'})
        assert not created_path.exists()
    finally:
        if created_path is not None:
            created_path.unlink(missing_ok=True)


def test_env_file_lives_for_scoped_command_and_is_removed_on_error():
    tool = make_tool('dummy ENVFILE')
    path = None
    with pytest.raises(RuntimeError, match='pipeline failed'):
        with tool.parse_command({'C32_MARKER': 'present'}) as command:
            path = Path(command[1])
            assert path.read_text() == 'C32_MARKER=present'
            raise RuntimeError('pipeline failed')
    assert path is not None
    assert not path.exists()


def test_env_file_rejects_multiline_values():
    tool = make_tool('dummy ENVFILE')
    with pytest.raises(ValueError, match='new line'):
        with tool.parse_command({'C32_MARKER': 'one\ntwo'}):
            pass
