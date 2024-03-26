######################################################################
#
# File: b2/_internal/_cli/autocomplete_install.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import abc
import io
import logging
import os
import re
import shlex
import shutil
import signal
import subprocess
import textwrap
from datetime import datetime
from importlib.util import find_spec
from pathlib import Path
from shlex import quote

import argcomplete
from class_registry import ClassRegistry, RegistryKeyError

from b2._internal._utils.python_compat import shlex_join

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


class ShellAutocompleteInstaller(abc.ABC):
    shell_exec: str

    def __init__(self, prog: str):
        self.prog = prog

    def install(self) -> None:
        """Install autocomplete for the given program."""
        script_path = self.create_script()
        if not self.is_enabled():
            logger.info(
                "%s completion doesn't seem to be autoloaded from %s.", self.shell_exec,
                script_path.parent
            )
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
        script_path.parent.mkdir(exist_ok=True, parents=True, mode=0o755)
        script_path.write_text(shellcode)
        return script_path

    @abc.abstractmethod
    def force_enable(self, completion_script: Path) -> None:
        """
        Enable autocomplete for the given program.

        Used as fallback if shell doesn't automatically enable autocomplete.
        """
        raise NotImplementedError

    def get_shellcode(self) -> str:
        """Get autocomplete shellcode for the given program."""
        return argcomplete.shellcode([self.prog], shell=self.shell_exec)

    @abc.abstractmethod
    def get_script_path(self) -> Path:
        """Get autocomplete script path for the given program."""
        raise NotImplementedError

    def program_in_path(self) -> bool:
        """Check if the given program is in PATH."""
        return _silent_success_run([self.shell_exec, '-c', self.prog])

    @abc.abstractmethod
    def is_enabled(self) -> bool:
        """Check if autocompletion is enabled."""
        raise NotImplementedError


class BashLikeAutocompleteInstaller(ShellAutocompleteInstaller):
    shell_exec: str
    rc_file_path: str

    def get_rc_path(self) -> Path:
        return Path(self.rc_file_path).expanduser()

    def force_enable(self, completion_script: Path) -> None:
        """Enable autocomplete for the given program, common logic."""
        rc_path = self.get_rc_path()
        if rc_path.exists() and rc_path.read_text().strip():
            bck_path = rc_path.with_suffix(f".{datetime.now():%Y-%m-%dT%H-%M-%S}.bak")
            logger.warning("Backing up %s to %s", rc_path, bck_path)
            try:
                shutil.copyfile(rc_path, bck_path)
            except OSError as e:
                raise AutocompleteInstallError(
                    f"Failed to backup {rc_path} under {bck_path}"
                ) from e
        logger.warning("Explicitly adding %s to %s", completion_script, rc_path)
        add_or_update_shell_section(
            rc_path, f"{self.prog} autocomplete", self.prog, self.get_rc_section(completion_script)
        )

    def get_rc_section(self, completion_script: Path) -> str:
        return f"source {quote(str(completion_script))}"

    def get_script_path(self) -> Path:
        """Get autocomplete script path for the given program, common logic."""
        script_dir = Path(f"~/.{self.shell_exec}_completion.d/").expanduser()
        return script_dir / self.prog

    def is_enabled(self) -> bool:
        """Check if autocompletion is enabled."""
        return _silent_success_run([self.shell_exec, '-i', '-c', f'complete -p {quote(self.prog)}'])


@SHELL_REGISTRY.register('bash')
class BashAutocompleteInstaller(BashLikeAutocompleteInstaller):
    shell_exec = 'bash'
    rc_file_path = "~/.bashrc"


@SHELL_REGISTRY.register('zsh')
class ZshAutocompleteInstaller(BashLikeAutocompleteInstaller):
    shell_exec = 'zsh'
    rc_file_path = "~/.zshrc"

    def get_rc_section(self, completion_script: Path) -> str:
        return textwrap.dedent(
            f"""\
            if [[ -z "$_comps" ]] && [[ -t 0 ]]; then autoload -Uz compinit && compinit -i -D; fi
            source {quote(str(completion_script))}
            """
        )

    def get_script_path(self) -> Path:
        """Custom get_script_path for Zsh, if the structure differs from the base implementation."""
        return Path("~/.zsh/completion/").expanduser() / f"_{self.prog}"

    def is_enabled(self) -> bool:
        rc_path = self.get_rc_path()
        if not rc_path.exists():
            # if zshrc is missing `zshrc -i` may hang on creation wizard when emulating tty
            rc_path.touch(mode=0o750)
        _silent_success_run_with_pty(
            [self.shell_exec, '-c', 'autoload -Uz compaudit; echo AUDIT; compaudit']
        )

        cmd = [self.shell_exec, '-i', '-c', f'[[ -v _comps[{quote(self.prog)}] ]]']
        return _silent_success_run_with_tty(cmd)


@SHELL_REGISTRY.register('fish')
class FishAutocompleteInstaller(ShellAutocompleteInstaller):
    shell_exec = 'fish'
    rc_file_path = "~/.config/fish/config.fish"

    def force_enable(self, completion_script: Path) -> None:
        raise NotImplementedError("Fish shell doesn't support manual completion enabling.")

    def get_script_path(self) -> Path:
        """Get autocomplete script path for the given program, common logic."""
        complete_paths = [
            Path(p) for p in shlex.split(
                subprocess.run(
                    [self.shell_exec, '-c', 'echo $fish_complete_path'],
                    timeout=30,
                    text=True,
                    check=True,
                    capture_output=True
                ).stdout
            )
        ]
        user_path = Path("~/.config/fish/completions").expanduser()
        if complete_paths:
            target_path = user_path if user_path in complete_paths else complete_paths[0]
        else:
            logger.warning("$fish_complete_path is empty, falling back to %r", user_path)
            target_path = user_path
        return target_path / f"{self.prog}.fish"

    def is_enabled(self) -> bool:
        """
        Check if autocompletion is enabled.

        Fish seems to lazy-load completions, hence first we trigger completion.
        That alone cannot be used, since fish tends to always propose completions (e.g. suggesting similarly
        named filenames).
        """
        environ = os.environ.copy()
        environ.setdefault("TERM", "xterm")  # TERM has to be set for fish to load completions
        return _silent_success_run_with_tty(
            [
                self.shell_exec, '-i', '-c',
                f'string length -q -- (complete -C{quote(f"{self.prog} ")} >/dev/null && complete -c {quote(self.prog)})'
            ],
            env=environ,
        )


def _silent_success_run_with_tty(
    cmd: list[str], timeout: int = 30, env: dict | None = None
) -> bool:
    emulate_tty = not os.isatty(0)  # is True under GHA or pytest-xdist
    if emulate_tty and not find_spec('pexpect'):
        emulate_tty = False
        logger.warning(
            "pexpect is needed to check autocomplete installation correctness without tty. "
            "You can install it via `pip install pexpect`."
        )
    run_func = _silent_success_run_with_pty if emulate_tty else _silent_success_run
    return run_func(cmd, timeout=timeout, env=env)


def _silent_success_run(cmd: list[str], timeout: int = 30, env: dict | None = None) -> bool:
    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.DEVNULL,
        start_new_session=True,  # prevents `zsh -i` messing with parent tty under pytest-xdist
        env=env,
    )

    try:
        stdout, stderr = p.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        p.kill()
        stdout, stderr = p.communicate(timeout=1)
        logger.warning("Command %r timed out, stdout: %r, stderr: %r", cmd, stdout, stderr)
    else:
        logger.log(
            logging.DEBUG if p.returncode == 0 else logging.WARNING,
            "Command %r exited with code %r, stdout: %r, stderr: %r", cmd, p.returncode, stdout,
            stderr
        )
    return p.returncode == 0


def _silent_success_run_with_pty(
    cmd: list[str], timeout: int = 30, env: dict | None = None
) -> bool:
    """
    Run a command with emulated terminal and return whether it succeeded.
    """
    import pexpect

    command_str = shlex_join(cmd)

    child = pexpect.spawn(command_str, timeout=timeout, env=env)
    output = io.BytesIO()
    try:
        child.logfile_read = output
        child.expect(pexpect.EOF)
    except pexpect.TIMEOUT:
        logger.warning("Command %r timed out, output: %r", cmd, output.getvalue())
        child.kill(signal.SIGKILL)
        return False
    finally:
        child.close()

    logger.log(
        logging.DEBUG if child.exitstatus == 0 else logging.WARNING,
        "Command %r exited with code %r, output: %r", cmd, child.exitstatus, output.getvalue()
    )
    return child.exitstatus == 0


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
