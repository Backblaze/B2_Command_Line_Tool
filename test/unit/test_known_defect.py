######################################################################
#
# File: test/unit/test_known_defect.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Exercise known-defect reporting through pytest in a separate process."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    ('body', 'exception', 'stage', 'parameterized', 'status', 'outcome'),
    [
        (
            'raise AssertionError("intended defect")',
            'AssertionError',
            'call',
            False,
            0,
            '1 xfailed',
        ),
        (
            'raise AssertionError("unrelated failure")',
            'AssertionError',
            'call',
            False,
            1,
            '1 failed',
        ),
        ('raise ValueError("intended defect")', 'AssertionError', 'call', False, 1, '1 failed'),
        ('raise ValueError("intended defect")', 'ValueError', 'call', False, 0, '1 xfailed'),
        ('pass', 'AssertionError', 'setup', False, 1, '1 error'),
        ('unrelated()', 'AssertionError', 'helper', False, 1, '1 failed'),
        ('intended()', 'AssertionError', 'named_helper', False, 0, '1 xfailed'),
        ('unrelated()', 'AssertionError', 'wrong_helper', False, 1, '1 failed'),
        (
            'raise AssertionError("intended defect")',
            'AssertionError',
            'teardown',
            False,
            1,
            '1 error',
        ),
        ('pass', 'AssertionError', 'call', False, 1, 'XPASS(strict)'),
        ('raise AssertionError("intended defect")', 'AssertionError', 'call', True, 0, '1 xfailed'),
        (
            'raise AssertionError("unrelated failure")',
            'AssertionError',
            'call',
            True,
            1,
            '1 failed',
        ),
    ],
)
def test_known_defect_accepts_only_its_expected_call_failure(
    tmp_path, body, exception, stage, parameterized, status, outcome
):
    source = 'import pytest\nfrom test.known_defect import known_defect, known_defect_marks\n'
    if stage in ('helper', 'named_helper', 'wrong_helper'):
        source += 'def unrelated():\n    raise AssertionError("intended defect")\n'
    elif stage == 'setup':
        source += '@pytest.fixture\ndef prepared():\n    raise AssertionError("intended defect")\n'
    elif stage == 'teardown':
        source += '@pytest.fixture\ndef prepared():\n    yield\n    raise AssertionError("intended defect")\n'
    if stage in ('named_helper', 'wrong_helper'):
        source += 'def intended():\n    raise AssertionError("intended defect")\n'
    origin = ', assertion_origin="intended"' if stage in ('named_helper', 'wrong_helper') else ''
    if parameterized:
        source += (
            '@pytest.mark.parametrize("value", [pytest.param(0, marks=known_defect_marks('
            f'"example", raises={exception}, match="^intended defect$", test_name="test_case"{origin}))])\n'
        )
        arguments = 'value'
    else:
        source += (
            f'@known_defect("example", raises={exception}, match="^intended defect$"{origin})\n'
        )
        arguments = 'prepared' if stage in ('setup', 'teardown') else ''
    source += f'def test_case({arguments}):\n    {body}\n'
    module = tmp_path / 'test_case.py'
    module.write_text(source)
    env = {key: value for key, value in os.environ.items() if not key.startswith('B2_')}
    env.update(PYTHONPATH=str(ROOT), PYTEST_DISABLE_PLUGIN_AUTOLOAD='1', PYTEST_ADDOPTS='')
    result = subprocess.run(
        [
            sys.executable,
            '-m',
            'pytest',
            str(module),
            '-q',
            '--tb=short',
            '-c',
            str(ROOT / 'setup.cfg'),
            '--confcutdir',
            str(tmp_path),
            '-o',
            'addopts=',
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == status, result.stdout + result.stderr
    assert outcome in result.stdout, result.stdout + result.stderr
