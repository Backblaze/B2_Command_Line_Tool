######################################################################
#
# File: test/unit/_cli/test_autocomplete_install.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pathlib
import shutil
import stat

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
def test_add_or_update_shell_section_permissions(test_file, mode):
    if mode is not None:
        test_file.write_text('existing content\n')
        test_file.chmod(mode)

    add_or_update_shell_section(test_file, section, managed_by, content)

    assert stat.S_IMODE(test_file.stat().st_mode) == (0o644 if mode is None else mode)


def test_add_or_update_shell_section_replace_failure(test_file, monkeypatch):
    original = 'original content\n'
    test_file.write_text(original)

    def fail_replace(*args, **kwargs):
        raise OSError('replacement failed')

    monkeypatch.setattr('os.replace', fail_replace)
    with pytest.raises(OSError, match='replacement failed'):
        add_or_update_shell_section(test_file, section, managed_by, content)

    assert test_file.read_text() == original
    assert list(test_file.parent.iterdir()) == [test_file]


@skip_on_windows
def test_zsh_is_enabled_creates_non_executable_rc_file(test_file, monkeypatch):
    installer = SHELL_REGISTRY.get('zsh', prog='b2')
    monkeypatch.setattr(installer, 'get_rc_path', lambda: test_file)
    monkeypatch.setattr(
        'b2._internal._cli.autocomplete_install._silent_success_run_with_tty',
        lambda *args, **kwargs: False,
    )

    assert installer.is_enabled() is False
    assert stat.S_IMODE(test_file.stat().st_mode) == 0o644


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
