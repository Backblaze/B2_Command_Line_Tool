######################################################################
#
# File: test/unit/test_llms_txt.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY_PREFIX = 'https://github.com/Backblaze/B2_Command_Line_Tool/blob/master/'
LINK = re.compile(r'^- \[([^\]]+)\]\((https://[^\s)]+)\): (\S.*[.])$')
REQUIRED_URLS = {
    REPOSITORY_PREFIX + path
    for path in (
        'README.md',
        'README.md#installation',
        'README.md#usage',
        'README.md#apiver-cli-versions-b2-vs-b2v3-b2v4-b2v5-etc',
        'doc/source/quick_start.rst',
        'doc/source/scripting.rst',
        'doc/authorization_realm.md',
        'doc/testing.md',
        'doc/writing-style.md',
        'CONTRIBUTING.md',
        'RELEASE.md',
        'SECURITY.md',
        'CHANGELOG.md',
        'b2/_internal/console_tool.py',
    )
} | {'https://b2-command-line-tool.readthedocs.io/en/master/'}


def validate_index(text):
    lines = text.splitlines()
    assert lines and re.fullmatch(r'# [^#\s].*', lines[0]), 'first line must be an H1'
    assert (
        len(lines) > 2 and lines[1] == '' and re.fullmatch(r'> \S.*', lines[2])
    ), 'H1 must be followed by a one-line blockquote summary'
    section = False
    section_links = 0
    total_links = 0
    urls = set()
    for line in lines[3:]:
        if not line:
            continue
        if re.fullmatch(r'## [^#\s].*', line):
            assert not section or section_links, 'each section must contain links'
            section, section_links = True, 0
            continue
        assert section, 'links must appear in a section'
        match = LINK.fullmatch(line)
        assert match, f'invalid link line: {line}'
        url = match.group(2)
        urls.add(url)
        parts = urlsplit(url)
        repo_path = unquote(parts.path).lower()
        if parts.hostname in ('github.com', 'raw.githubusercontent.com') and (
            repo_path == '/backblaze/b2_command_line_tool'
            or repo_path.startswith('/backblaze/b2_command_line_tool/')
        ):
            assert url.startswith(REPOSITORY_PREFIX), f'invalid repository ref: {url}'
            assert not parts.query, f'invalid repository query: {url}'
            path = unquote(parts.path.removeprefix('/Backblaze/B2_Command_Line_Tool/blob/master/'))
            assert parts.path == '/Backblaze/B2_Command_Line_Tool/blob/master/' + path
            target = (ROOT / path).resolve()
            assert target.is_relative_to(ROOT) and target.is_file(), f'missing file: {path}'
            if parts.fragment:
                assert path == 'README.md', f'anchor validation unsupported: {path}'
                headings = re.findall(r'^#{1,6} +(.+?)\s*#*\s*$', target.read_text(), re.M)
                anchors = [re.sub(r'[^\w\- ]', '', h.lower()).replace(' ', '-') for h in headings]
                assert (
                    unquote(parts.fragment) in anchors
                ), f'missing README anchor: {parts.fragment}'
        total_links += 1
        section_links += 1
    assert section and section_links and total_links, 'at least one populated section is required'
    assert REQUIRED_URLS <= urls, f'missing required URLs: {REQUIRED_URLS - urls}'


def test_llms_txt_structure_and_local_links():
    validate_index((ROOT / 'llms.txt').read_text(encoding='utf-8'))


def test_missing_required_apiver_link_fails():
    text = (ROOT / 'llms.txt').read_text(encoding='utf-8')
    text = '\n'.join(line for line in text.splitlines() if not line.startswith('- [ApiVer]'))
    with pytest.raises(AssertionError):
        validate_index(text)


@pytest.mark.parametrize(
    'url',
    [
        'https://github.com/Backblaze/B2_Command_Line_Tool/tree/master/missing',
        'https://github.com/backblaze/b2_command_line_tool/blob/master/README.md',
        'https://raw.githubusercontent.com/Backblaze/B2_Command_Line_Tool/master/README.md',
    ],
)
def test_repository_url_variants_fail_closed(url):
    text = (ROOT / 'llms.txt').read_text(encoding='utf-8')
    with pytest.raises(AssertionError, match='invalid repository ref'):
        validate_index(text + f'\n- [Extra]({url}): Refer to the file.\n')
