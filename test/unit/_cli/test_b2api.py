######################################################################
#
# File: test/unit/_cli/test_b2api.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2._internal._cli.b2api import _get_b2api_for_profile
from test.known_defect import known_defect

HTTP_SETTINGS = (
    'CONNECTION_TIMEOUT',
    'TIMEOUT',
    'TIMEOUT_FOR_COPY',
    'TIMEOUT_FOR_UPLOAD',
    'TRY_COUNT_DATA',
    'TRY_COUNT_DOWNLOAD',
    'TRY_COUNT_HEAD',
    'TRY_COUNT_OTHER',
)


def _http_settings(monkeypatch, tmp_path, env: dict[str, str]) -> dict[str, object]:
    monkeypatch.setenv('B2_ACCOUNT_INFO', str(tmp_path / 'account_info'))
    for name in ('CI', 'GITHUB_REPOSITORY'):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    b2http = _get_b2api_for_profile().session.raw_api.b2_http
    return {name: getattr(b2http, name) for name in HTTP_SETTINGS}


@known_defect(
    'the released CLI changes its HTTP timeouts and retry limits when run from a CI environment'
)
def test_shipped_cli_does_not_change_http_limits_for_a_fork_in_ci(monkeypatch, tmp_path):
    """A user running the released tool in their own (fork) CI must keep the default timeouts and retries."""
    default = _http_settings(monkeypatch, tmp_path, {})
    in_ci = _http_settings(
        monkeypatch, tmp_path, {'CI': 'true', 'GITHUB_REPOSITORY': 'someone/B2_Command_Line_Tool'}
    )
    assert in_ci == default
