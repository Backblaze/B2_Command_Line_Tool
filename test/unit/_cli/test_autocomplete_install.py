######################################################################
#
# File: test/unit/_cli/test_autocomplete_install.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import pytest

from b2._cli.autocomplete_install import add_or_update_shell_section

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
