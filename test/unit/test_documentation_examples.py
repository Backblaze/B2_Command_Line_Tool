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
Validate the `b2` command lines published in the README, Sphinx docs, and CLI skill.

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
import sys

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DOC_SOURCE = REPO_ROOT / 'doc' / 'source'
SKILL_PATH = REPO_ROOT / '.claude' / 'skills' / 'b2-cli' / 'SKILL.md'

_RST_SH_BLOCK_RE = re.compile(r'^(?P<indent>[ ]*)\.\. code-block:: (?:sh|bash|shell)[ ]*$')
_MARKDOWN_SH_FENCE_RE = re.compile(r'^[ ]*```(?:sh|bash|shell)[ ]*$')
_DOCKER_IMAGE = 'backblazeit/b2:latest'
_B2_BINARIES = ('b2', 'b2v3', 'b2v4', 'b2v5')


def _numbered_shell_blocks(text: str) -> list[list[tuple[int, str]]]:
    """Return the (1-based line number, stripped line) pairs of each shell code block."""
    lines = text.splitlines()
    blocks: list[list[tuple[int, str]]] = []
    i = 0
    while i < len(lines):
        rst = _RST_SH_BLOCK_RE.match(lines[i])
        if rst:
            base_indent = len(rst.group('indent'))
            i += 1
            body: list[tuple[int, str]] = []
            while i < len(lines):
                line = lines[i]
                if line.strip() and len(line) - len(line.lstrip()) <= base_indent:
                    break
                body.append((i + 1, line.strip()))
                i += 1
            blocks.append(body)
        elif _MARKDOWN_SH_FENCE_RE.match(lines[i]):
            i += 1
            body = []
            while i < len(lines) and not lines[i].lstrip().startswith('```'):
                body.append((i + 1, lines[i].strip()))
                i += 1
            blocks.append(body)
            i += 1
        else:
            i += 1
    return blocks


def extract_shell_blocks(text: str) -> list[list[str]]:
    """Return the lines of each shell code block (reST `code-block` or Markdown fence)."""
    return [[line for _, line in block] for block in _numbered_shell_blocks(text)]


def _starts_b2_command(line: str) -> bool:
    """Whether a physical line starts a `b2` invocation (directly or through the docker image)."""
    words = line.removeprefix('$ ').split()
    return bool(words) and (words[0] in _B2_BINARIES or _DOCKER_IMAGE in words)


def _logical_commands(block: list[tuple[int, str]]) -> list[tuple[int, str, bool]]:
    """
    Join physical lines into commands: a command continues while a quote is still open.

    Only a line that starts a `b2` command opens a command; other lines (installers, sample
    output, ...) are never tokenized, so an apostrophe in output cannot swallow later commands.
    Returns (first line number, command text, whether its quotes are closed) for each command;
    a command still open at the end of the block is returned as not closed instead of dropped.
    """
    commands: list[tuple[int, str, bool]] = []
    pending = ''
    start = 0
    for lineno, line in block:
        if not pending:
            if not _starts_b2_command(line):
                continue
            start = lineno
        candidate = f'{pending}\n{line}' if pending else line
        try:
            shlex.split(candidate, comments=True)
        except ValueError:  # unbalanced quote: the command continues on the next line
            pending = candidate
            continue
        pending = ''
        commands.append((start, candidate, True))
    if pending:
        commands.append((start, pending, False))
    return commands


def _b2_arguments(command: str) -> list[str] | None:
    """Return the arguments after the `b2` binary name, or None if the command is not `b2`."""
    tokens = shlex.split(command.removeprefix('$ '), comments=True)
    if _DOCKER_IMAGE in tokens:
        tokens = tokens[tokens.index(_DOCKER_IMAGE) + 1 :]
    if tokens and tokens[0] in _B2_BINARIES:
        # Shell redirection belongs to the shell, not the CLI argument parser.
        if len(tokens) >= 3 and tokens[-2] == '>':
            tokens = tokens[:-2]
        return tokens[1:]
    return None


def b2_argument_lists(block: list[str]) -> list[list[str]]:
    """
    Return the argument vector (everything after the `b2` binary name) of each `b2` invocation.

    Lines that do not invoke `b2` (installers, `export`, sample output, ...) are ignored.
    Raises ValueError for a `b2` command whose quote is never closed.
    """
    result = []
    for _, command, closed in _logical_commands(list(enumerate(block, start=1))):
        if not closed:
            raise ValueError(f'unclosed quote in documented shell block: {command!r}')
        args = _b2_arguments(command)
        if args is not None:
            result.append(args)
    return result


def documentation_files() -> list[pathlib.Path]:
    return [REPO_ROOT / 'README.md', *sorted(DOC_SOURCE.glob('*.rst')), SKILL_PATH]


def documented_commands() -> list:
    """
    One pytest param (args, problem) per documented `b2` command, with id `file:line::command`.

    A command that cannot be extracted (an unclosed quote) becomes a param with a `problem`
    message, so it fails as one named test case instead of interrupting collection.
    """
    params = []
    for path in documentation_files():
        source = path.relative_to(REPO_ROOT).as_posix()
        for block in _numbered_shell_blocks(path.read_text(encoding='utf-8')):
            for lineno, command, closed in _logical_commands(block):
                if not closed:
                    problem = (
                        f'{source}:{lineno}: unclosed quote in documented command: {command!r}'
                    )
                    params.append(
                        pytest.param(None, problem, id=f'{source}:{lineno}::unclosed quote')
                    )
                    continue
                args = _b2_arguments(command)
                if args is None:
                    continue
                label = ' '.join(shlex.join(args).split())[:80]
                params.append(pytest.param(args, None, id=f'{source}:{lineno}::{label}'))
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

    def test_unclosed_quote_at_end_of_block_fails_instead_of_vanishing(self):
        # A documented command that lost its closing quote must not silently drop out of the
        # checked set (and must not hide later commands in the same block).
        block = ["$ b2 bucket delete 'example-mybucket-b2-1", '$ b2 bucket list']
        with pytest.raises(ValueError, match='unclosed quote'):
            b2_argument_lists(block)

    def test_apostrophes_in_sample_output_do_not_swallow_a_command(self):
        # Two apostrophes in output lines must not pair up across the `b2` command between them.
        block = ['$ b2 bucket list', "it's sample output", '$ b2 bucket get no-such', "don't"]
        assert b2_argument_lists(block) == [['bucket', 'list'], ['bucket', 'get', 'no-such']]

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


def test_broken_example_is_one_failing_case_with_file_and_line(monkeypatch, tmp_path):
    # A broken example must become a single failing test case naming its file and line, not a
    # collection error that interrupts the whole unit run; the other commands are still checked.
    readme = tmp_path / 'README.md'
    readme.write_text(
        "```sh\nb2 bucket list\nb2 bucket delete 'oops\n```\n\n```sh\nb2 key list\n```\n",
        encoding='utf-8',
    )
    module = sys.modules[__name__]
    monkeypatch.setattr(module, 'REPO_ROOT', tmp_path)
    monkeypatch.setattr(module, 'documentation_files', lambda: [readme])

    params = documented_commands()

    assert [p.id for p in params] == [
        'README.md:2::bucket list',
        'README.md:3::unclosed quote',
        'README.md:7::key list',
    ]
    assert [p.values for p in params if p.id != 'README.md:3::unclosed quote'] == [
        (['bucket', 'list'], None),
        (['key', 'list'], None),
    ]
    [(args, problem)] = [p.values for p in params if p.id == 'README.md:3::unclosed quote']
    assert args is None
    assert problem.startswith('README.md:3: unclosed quote')


def test_documentation_contains_commands_to_validate():
    # Guard against the extraction silently matching nothing (e.g. after a docs reformat).
    # 16 commands are documented today; the minimum is kept close to that because turning one
    # `code-block:: sh` into `text` made 5 commands vanish and a looser bound (10) did not notice.
    assert len(documented_commands()) >= 15


@pytest.mark.usefixtures('console_tool_class')
@pytest.mark.parametrize('args, problem', documented_commands())
def test_documented_command_is_accepted_by_parser(args, problem, cli_version):
    if cli_version != 'b2v5':
        pytest.skip('documentation describes the b2v5 interface')
    if problem:
        pytest.fail(problem, pytrace=False)
    accepted, stderr = parse_with_real_parser(args)
    assert accepted, f'`b2 {shlex.join(args)}` is rejected by the argument parser:\n{stderr}'


def test_skill_examples_are_collected():
    commands = [
        args
        for block in extract_shell_blocks(SKILL_PATH.read_text(encoding='utf-8'))
        for args in b2_argument_lists(block)
    ]
    assert len(commands) >= 10
    assert SKILL_PATH in documentation_files()
    assert ['version'] in commands


def test_skill_contains_no_credential_values():
    text = SKILL_PATH.read_text(encoding='utf-8')
    assert not re.search(r'(?:K00|005)[A-Za-z0-9]{20,}', text), 'credential-shaped value in skill'
    for match in re.finditer(r'B2_APPLICATION_KEY\s*=\s*([^\s`]+)', text):
        value = match.group(1).strip('"\'')
        assert re.fullmatch(
            r'<[A-Za-z_][A-Za-z_0-9-]*>', value
        ), 'non-placeholder key assignment in skill'


def test_shell_redirection_is_not_a_cli_argument():
    assert b2_argument_lists(['b2v5 key create scoped readFiles > /secure/output']) == [
        ['key', 'create', 'scoped', 'readFiles']
    ]
