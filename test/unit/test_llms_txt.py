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

ROOT = Path(__file__).resolve().parents[2]
REPOSITORY_PREFIX = 'https://github.com/Backblaze/B2_Command_Line_Tool/blob/master/'
LINK = re.compile(r'^- \[([^\]]+)\]\((https://[^\s)]+)\): (\S.*[.])$')


def validate_index(text):
    lines = text.splitlines()
    assert lines and re.fullmatch(r'# [^#\s].*', lines[0]), 'first line must be an H1'
    assert (
        len(lines) > 2 and lines[1] == '' and re.fullmatch(r'> \S.*', lines[2])
    ), 'H1 must be followed by a one-line blockquote summary'
    section = False
    section_links = 0
    total_links = 0
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
        if url.startswith('https://github.com/Backblaze/B2_Command_Line_Tool/blob/'):
            assert url.startswith(REPOSITORY_PREFIX), f'invalid repository ref: {url}'
            parts = urlsplit(url)
            path = unquote(parts.path.removeprefix('/Backblaze/B2_Command_Line_Tool/blob/master/'))
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


def test_llms_txt_structure_and_local_links():
    validate_index((ROOT / 'llms.txt').read_text(encoding='utf-8'))
