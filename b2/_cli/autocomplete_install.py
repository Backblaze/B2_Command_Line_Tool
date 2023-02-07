######################################################################
#
# File: b2/_cli/autocomplete_install.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import logging
import re
import subprocess
from pathlib import Path
from shlex import quote
from typing import List

import argcomplete

logger = logging.getLogger(__name__)

SUPPORTED_SHELLS = ('bash',)


def autocomplete_install(prog: str, shell: str = 'bash') -> None:
    """Install autocomplete for the given program."""
    shellcode = argcomplete.shellcode([prog], shell=shell)
    assert shell in SUPPORTED_SHELLS

    if not _silent_success_run([shell, '-c', quote(prog)]):
        logger.warning(
            "%s is not in PATH of new %s shell. This will prevent autocomplete from working properly.",
            prog, shell
        )

    _autocomplete_install_bash(prog, shellcode)
    logger.info(
        "Autocomplete for %s has been enabled. Restart your %s shell to use it.", prog, shell
    )


def _autocomplete_install_bash(prog: str, shellcode: str) -> None:
    bash_completion_path = Path(f"~/.bash_completion.d/").expanduser() / prog
    logger.info("Installing bash completion script under %s", bash_completion_path)
    bash_completion_path.parent.mkdir(exist_ok=True)
    bash_completion_path.write_text(shellcode)

    if not _bash_complete_enabled(prog):
        logger.info(
            "Bash completion doesn't seem to be autoloaded from %s. Possible reason: missing bash_completion.",
            bash_completion_path.parent
        )
        logger.warning("Explicitly adding %s to ~/.bashrc", bash_completion_path)
        bashrc_path = Path("~/.bashrc").expanduser()
        add_or_update_shell_section(
            bashrc_path, f"{prog} autocomplete", prog, f"source {bash_completion_path}"
        )

    if not _bash_complete_enabled(prog):
        logger.error("Bash completion is still not enabled.")
        raise AutocompleteInstallError(f"Bash completion for {prog} install failed.")


def _bash_complete_enabled(prog: str) -> bool:
    """Check if bash completion is enabled."""
    return _silent_success_run(['bash', '-i', '-c', f'complete -p {quote(prog)}'])


def _silent_success_run(cmd: List[str]) -> bool:
    return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0


def add_or_update_shell_section(
    path: Path, section: str, managed_by: str, content: str, comment_sign="#"
) -> None:
    """Add or update a section in a file."""
    section_start = f"{comment_sign} >>> {section} >>>"
    section_end = f"{comment_sign} <<< {section} <<<"
    assert section_end not in content
    try:
        file_content = path.read_text()
    except FileNotFoundError:
        file_content = ""

    full_content = f"""
{section_start}
{comment_sign} This section is managed by {managed_by} . Manual edit may break automated updates.
{content}
{section_end}
    """.strip()

    pattern = re.compile(
        rf'^{re.escape(section_start)}.*?^{re.escape(section_end)}', flags=re.MULTILINE | re.DOTALL
    )
    if pattern.search(file_content):
        file_content = pattern.sub(full_content, file_content)
    else:
        file_content += f"\n{full_content}\n"
    path.write_text(file_content)


class AutocompleteInstallError(Exception):
    """Exception raised when autocomplete installation fails."""