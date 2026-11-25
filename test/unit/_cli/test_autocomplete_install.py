######################################################################
#
# File: test/unit/_cli/test_autocomplete_install.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os
import pathlib
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from unittest import mock

import pytest

from b2._internal._cli.autocomplete_install import (
    SHELL_REGISTRY,
    AutocompleteInstallError,
    _silent_success_run,
    _silent_success_run_with_files,
    _silent_success_run_with_pipes,
    add_or_update_shell_section,
)
from test.helpers import skip_on_windows

section = 'test_section'
managed_by = 'pytest'
content = 'test content'


@pytest.mark.skipif(os.name != 'posix', reason='POSIX process groups')
@pytest.mark.parametrize('parent_exits', [False, True])
def test_completion_timeout_cleans_descendants_holding_pipes(tmp_path, monkeypatch, parent_exits):
    import fcntl

    ready = tmp_path / 'ready'
    processes = []
    real_popen = subprocess.Popen

    def start(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        processes.append(process)
        deadline = time.monotonic() + 10
        while not ready.exists() or ready.read_text() != 'ready':
            assert time.monotonic() < deadline, 'child did not signal readiness'
            time.sleep(0.01)
        return process

    monkeypatch.setattr(subprocess, 'Popen', start)
    child_code = (
        'import fcntl, time; '
        f'lock = open({str(ready)!r}, "w"); '
        'fcntl.flock(lock, fcntl.LOCK_EX); lock.write("ready"); lock.flush(); time.sleep(30)'
    )
    code = (
        'import pathlib, subprocess, sys, time; '
        f'subprocess.Popen([sys.executable, "-c", {child_code!r}]); '
        + ('' if parent_exits else 'time.sleep(30)')
    )
    verified_cleanup = False
    try:
        assert _silent_success_run([sys.executable, '-c', code], timeout=0.1) is False
        assert processes[0].returncode is not None
        assert processes[0].stdout.closed and processes[0].stderr.closed
        with ready.open() as lock:
            # Closing our pipes alone would leave the descendant holding this lock.
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        verified_cleanup = True
    finally:
        for process in processes:
            # Only clean up after a failed check. A successful run has already
            # terminated this group; signaling it again can return EPERM on macOS.
            if not verified_cleanup:
                with suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=5)


def test_completion_timeout_cleanup_remains_bounded(monkeypatch):
    process = mock.Mock()
    process.communicate.side_effect = [
        subprocess.TimeoutExpired('shell', 0.1),
        subprocess.TimeoutExpired('shell', 1, output=b'partial', stderr=b'error'),
    ]
    monkeypatch.setattr(subprocess, 'Popen', mock.Mock(return_value=process))
    if hasattr(os, 'killpg'):
        monkeypatch.setattr(os, 'killpg', mock.Mock())
    assert _silent_success_run_with_pipes(['shell'], timeout=0.1) is False
    process.stdout.close.assert_called_once()
    process.stderr.close.assert_called_once()


@pytest.mark.parametrize('status', [0, 1])
def test_completion_file_capture_preserves_output_and_exit_status(caplog, status):
    caplog.set_level('DEBUG')
    code = f'import sys; print("captured-out"); print("captured-err", file=sys.stderr); sys.exit({status})'
    assert _silent_success_run_with_files([sys.executable, '-c', code], timeout=10) is (status == 0)
    assert 'captured-out' in caplog.text and 'captured-err' in caplog.text


@pytest.mark.skipif(os.name != 'posix', reason='POSIX process groups')
@pytest.mark.parametrize('error', [ProcessLookupError, PermissionError])
def test_completion_timeout_handles_already_terminated_group(monkeypatch, error):
    process = mock.Mock(returncode=0)
    process.communicate.side_effect = [subprocess.TimeoutExpired('shell', 0.1), (b'', b'')]
    monkeypatch.setattr(subprocess, 'Popen', mock.Mock(return_value=process))
    # macOS can return EPERM for a group with no remaining signalable processes.
    monkeypatch.setattr(os, 'killpg', mock.Mock(side_effect=error))
    assert _silent_success_run(['shell'], timeout=0.1) is False


@pytest.mark.skipif(os.name != 'nt', reason='Windows pipe reader threads')
def test_windows_completion_timeout_with_descendant_returns_within_bound(tmp_path, monkeypatch):
    ready = tmp_path / 'child-pid'
    started = threading.Event()
    real_popen = subprocess.Popen
    processes = []

    def start(*args, **kwargs):
        process = real_popen(*args, **kwargs)
        processes.append(process)
        deadline = time.monotonic() + 15
        while not ready.exists() or not ready.read_text():
            assert time.monotonic() < deadline, 'descendant did not start'
            time.sleep(0.01)
        started.set()
        return process

    monkeypatch.setattr(subprocess, 'Popen', start)
    code = (
        'import pathlib, subprocess, sys, time; '
        'child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"], '
        'stdout=sys.stdout, stderr=sys.stderr); '
        f'pathlib.Path({str(ready)!r}).write_text(str(child.pid)); time.sleep(60)'
    )
    with ThreadPoolExecutor(max_workers=1) as executor:
        result = executor.submit(_silent_success_run, [sys.executable, '-c', code], timeout=0.1)
        try:
            assert started.wait(timeout=20), 'shell and descendant did not become ready'
            assert result.result(timeout=5) is False
        finally:
            # Releasing the real child's handles also unblocks a regressed reader,
            # so even a failing bounded-return assertion can finish its teardown.
            if ready.exists() and ready.read_text():
                real_popen(
                    ['taskkill', '/PID', ready.read_text(), '/T', '/F'],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                ).wait(timeout=10)
            for process in processes:
                process.kill()
                process.wait(timeout=10)


@pytest.fixture
def test_file(tmp_path):
    yield tmp_path / 'test_file.sh'


def test_add_or_update_shell_section_new_section(test_file):
    test_file.write_text('# preexisting content\n\n')

    add_or_update_shell_section(test_file, section, managed_by, content)

    assert (
        test_file.read_text()
        == f"""# preexisting content


# >>> {section} >>>
# This section is managed by {managed_by} . Manual edit may break automated updates.
{content}
# <<< {section} <<<
"""
    )


def test_add_or_update_shell_section_existing_section(test_file):
    old_content = 'old content'
    new_content = 'new content'

    # Write the initial file with an existing section
    test_file.write_text(
        f"""# preexisting content

# >>> {section} >>>
# This section is managed by {managed_by} . Manual edit may break automated updates.
{old_content}
# <<< {section} <<<
"""
    )

    # Add the new content to the section
    add_or_update_shell_section(test_file, section, managed_by, new_content)

    assert (
        test_file.read_text()
        == f"""# preexisting content

# >>> {section} >>>
# This section is managed by {managed_by} . Manual edit may break automated updates.
{new_content}
# <<< {section} <<<
"""
    )


@pytest.mark.parametrize('new_content', [r'echo \n', r'echo \g<0>', r'echo \\server\path'])
def test_add_or_update_shell_section_preserves_backslashes(test_file, new_content):
    original = f'# before\n# >>> {section} >>>\nold content\n# <<< {section} <<<\n# after\n'
    test_file.write_text(original)

    add_or_update_shell_section(test_file, section, managed_by, new_content)

    assert test_file.read_text() == (
        f'# before\n# >>> {section} >>>\n'
        f'# This section is managed by {managed_by} . Manual edit may break automated updates.\n'
        f'{new_content}\n# <<< {section} <<<\n# after\n'
    )


@skip_on_windows
@pytest.mark.parametrize('mode', [None, 0o600, 0o640])
@pytest.mark.parametrize('umask', [0o022, 0o077])
def test_add_or_update_shell_section_permissions(test_file, mode, umask):
    if mode is not None:
        test_file.write_text('existing content\n')
        test_file.chmod(mode)

    previous_umask = os.umask(umask)
    try:
        add_or_update_shell_section(test_file, section, managed_by, content)
    finally:
        os.umask(previous_umask)

    expected_mode = 0o644 & ~umask if mode is None else mode
    assert stat.S_IMODE(test_file.stat().st_mode) == expected_mode


@skip_on_windows
def test_add_or_update_shell_section_preserves_inode_and_ownership(test_file):
    test_file.write_text('existing content\n')
    original = test_file.stat()
    for group in os.getgroups():
        if group != original.st_gid:
            try:
                os.chown(test_file, -1, group)
            except PermissionError:
                continue
            break
    original = test_file.stat()
    add_or_update_shell_section(test_file, section, managed_by, content)
    updated = test_file.stat()
    assert (updated.st_dev, updated.st_ino, updated.st_uid, updated.st_gid) == (
        original.st_dev,
        original.st_ino,
        original.st_uid,
        original.st_gid,
    )


@pytest.mark.skipif(
    sys.platform != 'darwin' and not hasattr(os, 'setxattr'),
    reason='extended attributes are unavailable',
)
def test_add_or_update_shell_section_preserves_extended_attributes(test_file):
    test_file.write_text('existing content\n')
    name = 'user.b2-completion-test'
    if sys.platform == 'darwin':
        subprocess.run(['xattr', '-w', name, 'preserved-value', str(test_file)], check=True)
    else:
        os.setxattr(test_file, name, b'preserved-value')
    add_or_update_shell_section(test_file, section, managed_by, content)
    if sys.platform == 'darwin':
        names = subprocess.check_output(['xattr', str(test_file)], text=True).splitlines()
        assert name in names
        value = subprocess.check_output(['xattr', '-p', name, str(test_file)]).rstrip(b'\n')
    else:
        value = os.getxattr(test_file, name)
    assert value == b'preserved-value'


@pytest.mark.skipif(sys.platform != 'darwin', reason='macOS ACL syntax')
def test_add_or_update_shell_section_preserves_acl(test_file):
    test_file.write_text('existing content\n')
    subprocess.run(['chmod', '+a', 'everyone deny execute', str(test_file)], check=True)
    before = subprocess.check_output(['ls', '-lde', str(test_file)], text=True)
    assert 'everyone deny execute' in before
    add_or_update_shell_section(test_file, section, managed_by, content)
    after = subprocess.check_output(['ls', '-lde', str(test_file)], text=True)
    assert 'everyone deny execute' in after


def test_add_or_update_shell_section_write_failure(test_file, monkeypatch):
    original = 'original content\n'
    test_file.write_text(original)

    def fail_write(*args, **kwargs):
        raise OSError('write failed')

    monkeypatch.setattr(pathlib.Path, 'write_text', fail_write)
    with pytest.raises(OSError, match='write failed'):
        add_or_update_shell_section(test_file, section, managed_by, content)

    assert test_file.read_text() == original


@skip_on_windows
@pytest.mark.parametrize('umask', [0o022, 0o077])
def test_zsh_is_enabled_creates_non_executable_rc_file(test_file, monkeypatch, umask):
    installer = SHELL_REGISTRY.get('zsh', prog='b2')
    monkeypatch.setattr(installer, 'get_rc_path', lambda: test_file)
    monkeypatch.setattr(
        'b2._internal._cli.autocomplete_install._silent_success_run_with_tty',
        lambda *args, **kwargs: False,
    )

    previous_umask = os.umask(umask)
    try:
        assert installer.is_enabled() is False
    finally:
        os.umask(previous_umask)
    assert stat.S_IMODE(test_file.stat().st_mode) == 0o644 & ~umask


def test_add_or_update_shell_section_no_file(test_file):
    # Add the new content to the section, which should create the file
    add_or_update_shell_section(test_file, section, managed_by, content)

    assert (
        test_file.read_text()
        == f"""
# >>> {section} >>>
# This section is managed by {managed_by} . Manual edit may break automated updates.
{content}
# <<< {section} <<<
"""
    )


@pytest.fixture
def dummy_command(homedir, monkeypatch, env):
    name = 'dummy_command'
    bin_path = homedir / 'bin' / name
    bin_path.parent.mkdir(parents=True, exist_ok=True)
    bin_path.symlink_to(pathlib.Path(__file__).parent / 'fixtures' / f'{name}.py')
    monkeypatch.setenv('PATH', f"{homedir}/bin:{env['PATH']}")
    yield name


@pytest.mark.parametrize('shell', ['bash', 'zsh', 'fish'])
@skip_on_windows
def test_autocomplete_installer(homedir, env, shell, caplog, dummy_command):
    caplog.set_level(10)
    shell_installer = SHELL_REGISTRY.get(shell, prog=dummy_command)

    shell_bin = shutil.which(shell)
    if shell_bin is None:
        pytest.skip(f'{shell} is not installed')

    assert shell_installer.is_enabled() is False
    shell_installer.install()
    assert shell_installer.is_enabled() is True


@pytest.mark.parametrize(
    'new_content',
    [
        r'eval "$(register-python-argcomplete b2 | sed -e \1)"',  # group reference in the content
        r'printf "a\nb"',  # escape sequence in the content
        r'echo C:\Users\dir',  # unknown escape (\U) in the content
    ],
)
def test_add_or_update_shell_section_existing_section_content_with_backslashes(
    test_file, new_content
):
    """Replacement text containing backslashes must be written verbatim."""
    test_file.write_text(
        f"""# preexisting content

# >>> {section} >>>
# This section is managed by {managed_by} . Manual edit may break automated updates.
old content
# <<< {section} <<<
"""
    )

    add_or_update_shell_section(test_file, section, managed_by, new_content)

    assert (
        test_file.read_text()
        == f"""# preexisting content

# >>> {section} >>>
# This section is managed by {managed_by} . Manual edit may break automated updates.
{new_content}
# <<< {section} <<<
"""
    ), 'shell section content was rewritten'


@skip_on_windows
@pytest.mark.parametrize('umask', [0o022, 0o077])
@pytest.mark.parametrize('mode', [0o600, 0o640])
def test_force_enable_backup_is_owner_only(tmp_path, monkeypatch, mode, umask):
    """The rc backup must be owner-only (0600), never the umask default and never more
    accessible than intended — even when the source is group-readable, since the backup
    may be owned by a different group than the source."""
    rc_path = tmp_path / 'rc'
    rc_path.write_text('# existing content\n')
    rc_path.chmod(mode)

    installer = SHELL_REGISTRY.get('bash', prog='b2')
    monkeypatch.setattr(installer, 'get_rc_path', lambda: rc_path)

    previous_umask = os.umask(umask)
    try:
        installer.force_enable(tmp_path / 'completion.sh')
    finally:
        os.umask(previous_umask)

    backups = list(tmp_path.glob('rc.*.bak'))
    assert len(backups) == 1, f'expected exactly one backup, found {backups}'
    assert stat.S_IMODE(backups[0].stat().st_mode) == 0o600


@skip_on_windows
def test_force_enable_backup_not_left_empty_on_read_failure(tmp_path, monkeypatch):
    """A failure reading the rc file must not leave an empty backup behind."""
    rc_path = tmp_path / 'rc'
    rc_path.write_text('# existing content\n')

    installer = SHELL_REGISTRY.get('bash', prog='b2')
    monkeypatch.setattr(installer, 'get_rc_path', lambda: rc_path)

    real_read_bytes = pathlib.Path.read_bytes

    def failing_read_bytes(self):
        if self == rc_path:
            raise OSError('read failed')
        return real_read_bytes(self)

    monkeypatch.setattr(pathlib.Path, 'read_bytes', failing_read_bytes)

    with pytest.raises(AutocompleteInstallError):
        installer.force_enable(tmp_path / 'completion.sh')
    assert list(tmp_path.glob('rc.*.bak')) == []


@skip_on_windows
def test_force_enable_two_backups_do_not_collide(tmp_path, monkeypatch):
    """Two consecutive fallback installs must each keep their own backup, not clash."""
    rc_path = tmp_path / 'rc'
    rc_path.write_text('# existing content\n')

    installer = SHELL_REGISTRY.get('bash', prog='b2')
    monkeypatch.setattr(installer, 'get_rc_path', lambda: rc_path)

    installer.force_enable(tmp_path / 'completion.sh')
    installer.force_enable(tmp_path / 'completion.sh')

    assert len(list(tmp_path.glob('rc.*.bak'))) == 2
