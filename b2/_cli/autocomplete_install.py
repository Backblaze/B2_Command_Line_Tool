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
from class_registry import ClassRegistry, RegistryKeyError

logger = logging.getLogger(__name__)

SHELL_REGISTRY = ClassRegistry()


def autocomplete_install(prog: str, shell: str = 'bash') -> None:
    """Install autocomplete for the given program."""
    try:
        autocomplete_installer = SHELL_REGISTRY.get(shell, prog=prog)
    except RegistryKeyError:
        raise AutocompleteInstallError(f"Unsupported shell: {shell}")
    autocomplete_installer.install()
    logger.info("Autocomplete for %s has been enabled.", prog)


class ShellAutocompleteInstaller:
    shell_exec: str

    def __init__(self, prog: str):
        self.prog = prog

    def install(self) -> None:
        """Install autocomplete for the given program."""
        script_path = self.create_script()
        if not self.is_enabled():
            try:
                self.force_enable(script_path)
            except NotImplementedError as e:
                logging.warning(
                    "Autocomplete wasn't automatically picked up and cannot force enable it: %s", e
                )

            if not self.is_enabled():
                logger.error("Autocomplete is still not enabled.")
                raise AutocompleteInstallError(f"Autocomplete for {self.prog} install failed.")

    def create_script(self) -> Path:
        """Create autocomplete for the given program."""
        shellcode = self.get_shellcode()

        script_path = self.get_script_path()
        logger.info("Creating autocompletion script under %s", script_path)
        script_path.parent.mkdir(exist_ok=True)
        script_path.write_text(shellcode)
        return script_path

    def force_enable(self, completion_script: Path) -> None:
        """
        Enable autocomplete for the given program.

        Used as fallback if shell doesn't automatically enable autocomplete.
        """
        raise NotImplementedError

    def get_shellcode(self) -> str:
        """Get autocomplete shellcode for the given program."""
        return argcomplete.shellcode([self.prog], shell=self.shell_exec)

    def get_script_path(self) -> Path:
        """Get autocomplete script path for the given program."""
        raise NotImplementedError

    def program_in_path(self) -> bool:
        """Check if the given program is in PATH."""
        return _silent_success_run([self.shell_exec, '-c', quote(self.prog)])

    def is_enabled(self) -> bool:
        """Check if autocompletion is enabled."""
        return _silent_success_run([self.shell_exec, '-i', '-c', f'complete -p {quote(self.prog)}'])


@SHELL_REGISTRY.register('bash')
class BashAutocompleteInstaller(ShellAutocompleteInstaller):
    shell_exec = 'bash'

    def force_enable(self, completion_script: Path) -> None:
        """Enable autocomplete for the given program."""
        logger.info(
            "Bash completion doesn't seem to be autoloaded from %s. Most likely `bash-completion` is not installed.",
            completion_script.parent
        )
        bashrc_path = Path("~/.bashrc").expanduser()
        if bashrc_path.exists():
            bck_path = bashrc_path.with_suffix(f".{datetime.now():%Y-%m-%dT%H-%M-%S}.bak")
            logger.warning("Backing up %s to %s", bashrc_path, bck_path)
            try:
                shutil.copyfile(bashrc_path, bck_path)
            except OSError as e:
                raise AutocompleteInstallError(
                    f"Failed to backup {bashrc_path} under {bck_path}"
                ) from e
        logger.warning("Explicitly adding %s to %s", completion_script, bashrc_path)
        add_or_update_shell_section(
            bashrc_path, f"{self.prog} autocomplete", self.prog, f"source {completion_script}"
        )

    def get_script_path(self) -> Path:
        """Get autocomplete script path for the given program."""
        return Path("~/.bash_completion.d/").expanduser() / self.prog


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


SUPPORTED_SHELLS = sorted(SHELL_REGISTRY.keys())