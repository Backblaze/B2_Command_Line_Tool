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
from test.helpers import skip_on_windows

import pytest

from b2._internal._cli.autocomplete_install import (
    SHELL_REGISTRY,
    add_or_update_shell_section,
)

section = "test_section"
managed_by = "pytest"
content = "test content"


@pytest.fixture
def test_file(tmp_path):
    yield tmp_path / "test_file.sh"


def test_add_or_update_shell_section_new_section(test_file):
    test_file.write_text("# preexisting content\n\n")

    add_or_update_shell_section(test_file, section, managed_by, content)

    assert test_file.read_text() == f"""# preexisting content


# >>> {section} >>>
# This section is managed by {managed_by} . Manual edit may break automated updates.
{content}
# <<< {section} <<<
"""


def test_add_or_update_shell_section_existing_section(test_file):
    old_content = "old content"
    new_content = "new content"

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

    assert test_file.read_text() == f"""# preexisting content

# >>> {section} >>>
# This section is managed by {managed_by} . Manual edit may break automated updates.
{new_content}
# <<< {section} <<<
"""


def test_add_or_update_shell_section_no_file(test_file):
    # Add the new content to the section, which should create the file
    add_or_update_shell_section(test_file, section, managed_by, content)

    assert test_file.read_text() == f"""
# >>> {section} >>>
# This section is managed by {managed_by} . Manual edit may break automated updates.
{content}
# <<< {section} <<<
"""


@pytest.fixture
def dummy_command(homedir, monkeypatch, env):
    name = "dummy_command"
    bin_path = homedir / "bin" / name
    bin_path.parent.mkdir(parents=True, exist_ok=True)
    bin_path.symlink_to(pathlib.Path(__file__).parent / "fixtures" / f"{name}.py")
    monkeypatch.setenv("PATH", f"{homedir}/bin:{env['PATH']}")
    yield name


@pytest.mark.parametrize("shell", ["bash", "zsh", "fish"])
@skip_on_windows
def test_autocomplete_installer(homedir, env, shell, caplog, dummy_command):
    caplog.set_level(10)
    shell_installer = SHELL_REGISTRY.get(shell, prog=dummy_command)

    shell_bin = shutil.which(shell)
    if shell_bin is None:
        pytest.skip(f"{shell} is not installed")

    assert shell_installer.is_enabled() is False
    shell_installer.install()
    assert shell_installer.is_enabled() is True
