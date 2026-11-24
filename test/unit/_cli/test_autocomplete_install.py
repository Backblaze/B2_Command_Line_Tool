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
import stat
import subprocess
import sys

import pytest

from b2._internal._cli.autocomplete_install import (
    SHELL_REGISTRY,
    add_or_update_shell_section,
)
from test.helpers import skip_on_windows

section = 'test_section'
managed_by = 'pytest'
content = 'test content'


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
