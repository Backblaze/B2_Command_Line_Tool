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
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from shlex import quote
from typing import List

import argcomplete
from class_registry import ClassRegistry

logger = logging.getLogger(__name__)

SHELL_REGISTRY = ClassRegistry()


def autocomplete_install(prog: str, shell: str = 'bash') -> None:
    """Install autocomplete for the given program."""
    try:
        shell_cls = SHELL_REGISTRY[shell]
    except KeyError:
        raise AutocompleteInstallError(f"Unsupported shell: {shell}")
    shell_cls.autocomplete_install(prog)
    logger.info("Autocomplete for %s has been enabled.", prog)


class Shell:
    shell_exec: str

    @classmethod
    def autocomplete_install(cls, prog: str) -> None:
        """Install autocomplete for the given program."""
        script_path = cls.create_autocomplete_for_program(prog)
        if not cls.autocomplete_enabled(prog):
            cls.enable_autocomplete_for_program(prog, script_path)
        if not cls.autocomplete_enabled(prog):
            logger.error("Autocomplete is still not enabled.")
            raise AutocompleteInstallError(f"Autocomplete for {prog} install failed.")

    @classmethod
    def create_autocomplete_for_program(cls, prog: str) -> Path:
        """Create autocomplete for the given program."""
        shellcode = cls.get_shellcode(prog)

        bash_completion_path = Path("~/.bash_completion.d/").expanduser() / prog
        logger.info("Creating bash completion script under %s", bash_completion_path)
        bash_completion_path.parent.mkdir(exist_ok=True)
        bash_completion_path.write_text(shellcode)
        return bash_completion_path

    @classmethod
    def enable_autocomplete_for_program(cls, prog: str, completion_script: Path) -> None:
        """Enable autocomplete for the given program."""
        logger.info(
            "Bash completion doesn't seem to be autoloaded from %s. Most likely `bash-completion` is not installed.",
            completion_script.parent
        )
        bashrc_path = Path("~/.bashrc").expanduser()
        bck_path = bashrc_path.with_suffix(f".{datetime.now():%Y-%m-%dT%H-%M-%S}.bck")
        logger.warning("Backing up %s to %s", bashrc_path, bck_path)
        try:
            shutil.copyfile(bashrc_path, bck_path)
        except OSError as e:
            raise AutocompleteInstallError(
                f"Failed to backup {bashrc_path} under {bck_path}"
            ) from e
        logger.warning("Explicitly adding %s to %s", completion_script, bashrc_path)
        add_or_update_shell_section(
            bashrc_path, f"{prog} autocomplete", prog, f"source {completion_script}"
        )

    @classmethod
    def get_shellcode(cls, prog: str) -> str:
        """Get autocomplete shellcode for the given program."""
        return argcomplete.shellcode([prog], shell=cls.shell_exec)

    @classmethod
    def program_in_path(cls, prog: str) -> bool:
        """Check if the given program is in PATH."""
        return _silent_success_run([cls.shell_exec, '-c', quote(prog)])

    @classmethod
    def autocomplete_enabled(cls, prog: str) -> bool:
        """Check if bash completion is enabled."""
        return _silent_success_run([cls.shell_exec, '-i', '-c', f'complete -p {quote(prog)}'])


@SHELL_REGISTRY.register('bash')
class Bash(Shell):
    shell_exec = 'bash'


def _silent_success_run(cmd: List[str]) -> bool:
    return subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0


def _path_with_suffix(path: Path, suffix: str) -> Path:
    return path.parent / (path.name + suffix)


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


SUPPORTED_SHELLS = sorted(SHELL_REGISTRY.keys())
