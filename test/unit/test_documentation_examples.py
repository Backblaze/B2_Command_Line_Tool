######################################################################
#
# File: test/unit/test_documentation_examples.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Validate the `b2` command lines published in the README and the Sphinx docs.

The examples need a live B2 account, so they cannot be executed here. What can drift silently
is the command syntax: a renamed subcommand or option leaves the documentation showing a command
line the tool rejects. This module extracts every shell example that invokes `b2` / `b2v5` (also
when wrapped in `docker run ... backblazeit/b2:latest b2v5 ...`) and checks that the real argument
parser accepts it. Nothing is executed and no network is used.

Limitation: argparse accepts unambiguous option prefixes, so a pass proves that the parser accepts
the documented text, not that the exact option spelled in the documentation exists.
"""

from __future__ import annotations

import contextlib
import io
import pathlib
import re
import shlex

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DOC_SOURCE = REPO_ROOT / 'doc' / 'source'

_RST_SH_BLOCK_RE = re.compile(r'^(?P<indent>[ ]*)\.\. code-block:: (?:sh|bash|shell)[ ]*$')
_MARKDOWN_SH_FENCE_RE = re.compile(r'^[ ]*```(?:sh|bash|shell)[ ]*$')
_DOCKER_IMAGE = 'backblazeit/b2:latest'
_B2_BINARIES = ('b2', 'b2v3', 'b2v4', 'b2v5')


def extract_shell_blocks(text: str) -> list[list[str]]:
    """Return the lines of each shell code block (reST `code-block` or Markdown fence)."""
    lines = text.splitlines()
    blocks: list[list[str]] = []
    i = 0
    while i < len(lines):
        rst = _RST_SH_BLOCK_RE.match(lines[i])
        if rst:
            base_indent = len(rst.group('indent'))
            i += 1
            body: list[str] = []
            while i < len(lines):
                line = lines[i]
                if line.strip() and len(line) - len(line.lstrip()) <= base_indent:
                    break
                body.append(line.strip())
                i += 1
            blocks.append(body)
        elif _MARKDOWN_SH_FENCE_RE.match(lines[i]):
            i += 1
            body = []
            while i < len(lines) and not lines[i].lstrip().startswith('```'):
                body.append(lines[i].strip())
                i += 1
            blocks.append(body)
            i += 1
        else:
            i += 1
    return blocks


def _logical_commands(block: list[str]) -> list[str]:
    """Join physical lines into commands: a command continues while a quote is still open."""
    commands: list[str] = []
    pending = ''
    for line in block:
        candidate = f'{pending}\n{line}' if pending else line
        try:
            shlex.split(candidate, comments=True)
        except ValueError:  # unbalanced quote: the command continues on the next line
            pending = candidate
            continue
        pending = ''
        if line:
            commands.append(candidate)
    return commands


def b2_argument_lists(block: list[str]) -> list[list[str]]:
    """
    Return the argument vector (everything after the `b2` binary name) of each `b2` invocation.

    Lines that do not invoke `b2` (installers, `export`, sample output, ...) are ignored.
    """
    result = []
    for command in _logical_commands(block):
        command = command.removeprefix('$ ')
        tokens = shlex.split(command, comments=True)
        if _DOCKER_IMAGE in tokens:
            tokens = tokens[tokens.index(_DOCKER_IMAGE) + 1 :]
        if tokens and tokens[0] in _B2_BINARIES:
            result.append(tokens[1:])
    return result


def documentation_files() -> list[pathlib.Path]:
    return [REPO_ROOT / 'README.md', *sorted(DOC_SOURCE.glob('*.rst'))]


def documented_commands() -> list:
    params = []
    for path in documentation_files():
        text = path.read_text(encoding='utf-8')
        for block in extract_shell_blocks(text):
            for args in b2_argument_lists(block):
                label = ' '.join(shlex.join(args).split())[:80]
                params.append(
                    pytest.param(args, id=f'{path.relative_to(REPO_ROOT).as_posix()}::{label}')
                )
    return params


def parse_with_real_parser(args: list[str]) -> tuple[bool, str]:
    """Return (accepted, stderr). Help requests (exit status 0) count as accepted."""
    from b2._internal.console_tool import B2

    parser = B2.create_parser(name='b2', b2_binary_name='b2')
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr), contextlib.redirect_stdout(io.StringIO()):
        try:
            parser.parse_args(args)
        except SystemExit as exc:
            return exc.code in (0, None), stderr.getvalue()
    return True, ''


class TestExtraction:
    def test_rst_block_with_prompt_and_output(self):
        text = (
            'Title\n\n.. code-block:: sh\n\n    $ b2 bucket list\n'
            '    346501784642eb3e60980d10  allPublic   my-bucket\n\nAfter.\n'
        )
        [block] = extract_shell_blocks(text)
        assert b2_argument_lists(block) == [['bucket', 'list']]

    def test_markdown_fence_ignores_non_b2_lines_and_comments(self):
        text = '```bash\npip install b2\nb2 file upload bkt local.txt remote.txt  # comment\n```\n'
        [block] = extract_shell_blocks(text)
        assert b2_argument_lists(block) == [['file', 'upload', 'bkt', 'local.txt', 'remote.txt']]

    def test_multiline_quoted_argument_is_one_command(self):
        text = (
            ".. code-block:: sh\n\n    $ b2 bucket update --replication '{\n"
            '        "a": 1\n    }\' my-bucket\n'
        )
        [block] = extract_shell_blocks(text)
        [args] = b2_argument_lists(block)
        assert args[:3] == ['bucket', 'update', '--replication']
        assert args[-1] == 'my-bucket'

    def test_docker_wrapper_is_unwrapped(self):
        block = ['docker run --rm -v b2:/root backblazeit/b2:latest b2v5 bucket list']
        assert b2_argument_lists(block) == [['bucket', 'list']]

    def test_docker_placeholder_and_unlabelled_fences_are_not_commands(self):
        assert b2_argument_lists(['docker run backblazeit/b2:latest  ...']) == []
        assert extract_shell_blocks('```\nb2 account   Account management.\n```\n') == []


class TestRealParser:
    @pytest.fixture(autouse=True)
    def _latest_cli_only(self, console_tool_class, cli_version):
        # The README and docs describe the latest interface (`b2` / `b2v5`); the registry of
        # older apivers registers a different set of commands.
        if cli_version != 'b2v5':
            pytest.skip('documentation describes the b2v5 interface')

    def test_accepts_valid_command(self):
        assert parse_with_real_parser(['bucket', 'list'])[0]

    def test_rejects_unknown_subcommand(self):
        accepted, stderr = parse_with_real_parser(['bucket', 'no-such-subcommand'])
        assert not accepted
        assert 'invalid choice' in stderr

    def test_rejects_unknown_option(self):
        accepted, stderr = parse_with_real_parser(['bucket', 'list', '--no-such-option'])
        assert not accepted
        assert 'unrecognized arguments' in stderr


def test_documentation_contains_commands_to_validate():
    # Guard against the extraction silently matching nothing (e.g. after a docs reformat).
    # 16 commands are documented today; the minimum is kept close to that because turning one
    # `code-block:: sh` into `text` made 5 commands vanish and a looser bound (10) did not notice.
    assert len(documented_commands()) >= 15


@pytest.mark.usefixtures('console_tool_class')
@pytest.mark.parametrize('args', documented_commands())
def test_documented_command_is_accepted_by_parser(args, cli_version):
    if cli_version != 'b2v5':
        pytest.skip('documentation describes the b2v5 interface')
    accepted, stderr = parse_with_real_parser(args)
    assert accepted, f'`b2 {shlex.join(args)}` is rejected by the argument parser:\n{stderr}'
