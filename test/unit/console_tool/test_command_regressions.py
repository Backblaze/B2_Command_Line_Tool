######################################################################
#
# File: test/unit/console_tool/test_command_regressions.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Regression tests for console-tool command behaviour.

Every test asserts the CORRECT behaviour. Tests decorated with `known_defect` describe a defect that is
still present: they are strict expected failures (see `test/known_defect.py`) and the decorator has to be
removed in the change that fixes the defect.
"""

from __future__ import annotations

import json
import os
from io import StringIO
from unittest import mock

import pytest
from b2sdk.v3 import Bucket, DownloadedFile, LegalHold
from b2sdk.v3.exception import B2SimpleError

from b2._internal._cli.const import (
    B2_ESCAPE_CONTROL_CHARACTERS,
)
from b2._internal.console_tool import _TqdmCloser
from test.known_defect import known_defect

# an invalid character inserted into an otherwise valid key; non-strict decoding drops it silently


def _propagating_exit(self, exc_type, exc_val, exc_tb):
    return None


def run(b2_cli, argv):
    """
    Run one command, return (status, stdout, stderr). Never swallows unexpected exceptions.

    The autouse fixture `disable_tqdm_closer_cleanup` mocks `_TqdmCloser.__exit__` with a MagicMock whose
    truthy return value swallows every exception raised inside a command, which would make
    failing commands look like `status=None`. Replace it with a pass-through for the duration of the call.
    """
    stdout, stderr = StringIO(), StringIO()
    try:
        with mock.patch.object(_TqdmCloser, '__exit__', _propagating_exit):
            status = b2_cli.console_tool_class(stdout, stderr).run_command(['b2', *argv])
    except SystemExit as e:
        status = e.code
    return status, stdout.getvalue(), stderr.getvalue()


def run_no_traceback(b2_cli, argv):
    """Run a command that must report a user error instead of letting an exception escape."""
    try:
        return run(b2_cli, argv)
    except Exception as e:  # noqa: BLE001
        # an AssertionError, so that a known_defect on such a test accepts exactly this outcome
        raise AssertionError(f'command raised {e!r} instead of reporting an error') from e


# ----------------------------------------------------------------------------------------
# High
# ----------------------------------------------------------------------------------------


def test_notification_rule_update_without_path_keeps_stored_prefix(b2_cli, bucket):
    """Updating a rule via a URI with no path must not widen the stored prefix."""
    b2_cli.run(
        [
            'bucket',
            'notification-rule',
            'create',
            f'b2://{bucket}/logs/',
            'scoped-rule',
            '--webhook-url',
            'https://example.com/hook',
            '--event-type',
            'b2:ObjectCreated:*',
        ],
        expected_part_of_stdout='scoped-rule',
    )

    b2_cli.run(
        ['bucket', 'notification-rule', 'update', f'b2://{bucket}', 'scoped-rule', '--disable'],
        expected_part_of_stdout='scoped-rule',
    )

    _, stdout, _ = b2_cli.run(['bucket', 'notification-rule', 'list', '--json', f'b2://{bucket}'])
    (rule,) = json.loads(stdout)
    assert rule['objectNamePrefix'] == 'logs/', 'prefix of the stored rule was silently widened'


@pytest.mark.parametrize('quiet', [False, True])
def test_replication_pause_error_goes_to_stderr(b2_cli, bucket_info, quiet, capsys):
    """The "rule could not be found" error must use stderr (not bare print to stdout)."""
    option = ['--quiet'] if quiet else []
    capsys.readouterr()
    status, stdout, stderr = run(
        b2_cli, ['replication', 'pause', *option, bucket_info['bucketName'], 'nope']
    )
    assert status == 1
    assert (
        'nope' in stderr or 'could not be found' in stderr
    ), f'error message not on the tool stderr (stdout={stdout!r}, stderr={stderr!r})'
    assert stdout == ''
    assert capsys.readouterr().out == ''


@pytest.mark.parametrize('command', ['pause', 'unpause'])
def test_replication_pause_twice_is_not_an_error(b2_cli, bucket_info, command):
    """Applying an existing pause/unpause state must succeed without another update."""
    b2_cli.run(['bucket', 'create', 'dest-bucket', 'allPrivate'], expected_stdout='bucket_1\n')
    b2_cli.run(
        ['replication', 'setup', '--name', 'rule1', bucket_info['bucketName'], 'dest-bucket'],
        expected_part_of_stdout='',
    )
    argv = ['replication', command, bucket_info['bucketName'], 'rule1']
    assert run(b2_cli, argv)[0] == 0
    before = b2_cli.b2_api.get_bucket_by_name(bucket_info['bucketName']).get_fresh_state()
    status, stdout, stderr = run(b2_cli, argv)
    assert status == 0, f'second {command} of the same rule failed: {stderr!r}'
    assert stdout == stderr == ''
    after = b2_cli.b2_api.get_bucket_by_name(bucket_info['bucketName']).get_fresh_state()
    assert after.revision == before.revision
    assert after.replication.rules[0].is_enabled is (command == 'unpause')


def test_ls_json_quiet_is_valid_json(b2_cli, uploaded_file):
    """`ls --json -q` must print a valid JSON document (brackets are not quieted)."""
    status, stdout, stderr = run(b2_cli, ['ls', '--json', '-q', f'b2://{uploaded_file["bucket"]}'])
    assert status == 0, stderr
    parsed = json.loads(stdout)
    assert isinstance(parsed, list), 'quiet JSON listing is not a list'
    assert [item['fileName'] for item in parsed] == [uploaded_file['fileName']]


def test_download_to_stdout_keeps_metadata_out_of_data_stream(b2_cli, uploaded_file, capfdbinary):
    """With output `-` the tool's stdout carries only the file content."""
    expected_content = uploaded_file['content'].encode()
    assert expected_content, 'the uploaded test file must not be empty'
    capfdbinary.readouterr()  # drop whatever the fixtures wrote to the real stdout
    status, tool_stdout, _ = run(
        b2_cli,
        ['file', 'download', f'b2://{uploaded_file["bucket"]}/{uploaded_file["fileName"]}', '-'],
    )
    assert status == 0
    # the real stdout (file descriptor 1) must carry exactly the file's bytes, no prefix and no suffix
    assert capfdbinary.readouterr().out == expected_content
    # and no file metadata may be written into the data stream
    for label in (
        'File name:',
        'File id:',
        'Output file path:',
        'File size:',
        'Content type:',
        'Content sha1:',
        'Encryption:',
        'Retention:',
        'Legal hold:',
    ):
        assert label not in tool_stdout, 'download stdout carries file metadata: ' + str(
            tool_stdout
        )


# ----------------------------------------------------------------------------------------
# Medium
# ----------------------------------------------------------------------------------------


@known_defect(
    'a non-numeric B2_ESCAPE_CONTROL_CHARACTERS raises ValueError',
    raises=ValueError,
    match="invalid literal for int\\(\\) with base 10: 'abc'",
)
def test_invalid_escape_control_characters_env_does_not_crash(b2_cli, monkeypatch):
    """A non-numeric B2_ESCAPE_CONTROL_CHARACTERS must be handled, not raise ValueError."""
    monkeypatch.setenv(B2_ESCAPE_CONTROL_CHARACTERS, 'abc')
    b2_cli.authorize()
    status, _, _ = run_no_traceback(b2_cli, ['bucket', 'list'])
    assert status == 0


@pytest.fixture
def lock_bucket_file(b2_cli, tmp_path):
    b2_cli.authorize()
    b2_cli.run(
        ['bucket', 'create', '--file-lock-enabled', 'lock-bucket', 'allPrivate'],
        expected_stdout='bucket_0\n',
    )
    local = tmp_path / 'f.txt'
    local.write_text('data')
    b2_cli.run(['file', 'upload', '--quiet', 'lock-bucket', str(local), 'f.txt'])
    return 'b2://lock-bucket/f.txt'


@known_defect(
    'file update leaves the legal-hold change applied when the retention step fails',
    match='^legal\\ hold\\ was\\ applied\\ although\\ the\\ command\\ failed',
)
def test_file_update_failing_retention_step_leaves_no_partial_update(b2_cli, lock_bucket_file):
    """If the retention step fails, the legal hold step must not stay applied."""
    # `file info` goes through download headers, which the simulator renders as "on" for every legal-hold
    # value, so read the stored value through the SDK instead
    _, stdout, _ = b2_cli.run(['file', 'info', lock_bucket_file])
    file_id = json.loads(stdout)['fileId']

    def stored_legal_hold():
        return b2_cli.b2_api.get_file_info(file_id).legal_hold

    b2_cli.run(['file', 'update', lock_bucket_file, '--legal-hold', 'off'])
    legal_hold_before = stored_legal_hold()
    assert legal_hold_before == LegalHold.OFF, 'the test needs a file whose legal hold is off'

    with mock.patch.object(
        b2_cli.b2_api, 'update_file_retention', side_effect=B2SimpleError('injected failure')
    ) as update_file_retention:
        status, _, stderr = run(
            b2_cli,
            [
                'file',
                'update',
                lock_bucket_file,
                '--legal-hold',
                'on',
                '--file-retention-mode',
                'governance',
                '--retain-until',
                '4102444800',
            ],
        )
    # the injected failure must really have been reached and reported ...
    update_file_retention.assert_called_once()
    assert status != 0
    assert 'injected failure' in stderr, stderr
    # ... and the earlier legal-hold step must not stay applied
    assert (
        stored_legal_hold() == legal_hold_before
    ), 'legal hold was applied although the command failed'


def test_retention_period_without_mode_is_not_silently_ignored(b2_cli):
    """--default-retention-period alone must be rejected before accessing a bucket."""
    b2_cli.authorize()
    b2_cli.run(['bucket', 'create', 'b8', 'allPrivate'], expected_stdout='bucket_0\n')
    with mock.patch.object(
        b2_cli.b2_api, 'get_bucket_by_name', wraps=b2_cli.b2_api.get_bucket_by_name
    ) as get_bucket:
        status, stdout, stderr = run_no_traceback(
            b2_cli,
            ['bucket', 'update', 'b8', 'allPublic', '--default-retention-period', '1 days'],
        )
    assert status == 1
    assert stdout == ''
    assert stderr == 'ERROR: --default-retention-period requires --default-retention-mode\n'
    get_bucket.assert_not_called()


@pytest.mark.parametrize('mode', ['compliance', 'governance'])
def test_retention_mode_without_period_is_rejected_cleanly(b2_cli, mode):
    """A retention mode that needs a period must be rejected with a clear error."""
    b2_cli.authorize()
    b2_cli.run(['bucket', 'create', 'b8', 'allPrivate'], expected_stdout='bucket_0\n')
    with mock.patch.object(
        b2_cli.b2_api, 'get_bucket_by_name', wraps=b2_cli.b2_api.get_bucket_by_name
    ) as get_bucket:
        status, stdout, stderr = run_no_traceback(
            b2_cli,
            ['bucket', 'update', 'b8', 'allPublic', '--default-retention-mode', mode],
        )
    assert status == 1
    assert stdout == ''
    assert stderr == (
        'ERROR: --default-retention-mode requires --default-retention-period '
        'unless the mode is none\n'
    )
    get_bucket.assert_not_called()


def test_retention_period_with_none_mode_clears_retention(b2_cli):
    """Mode none keeps its existing behavior even when a period is supplied."""
    b2_cli.authorize()
    b2_cli.run(
        ['bucket', 'create', 'b8-none', 'allPrivate', '--file-lock-enabled'],
        expected_stdout='bucket_0\n',
    )
    b2_cli.run(
        [
            'bucket',
            'update',
            'b8-none',
            'allPrivate',
            '--default-retention-mode',
            'governance',
            '--default-retention-period',
            '1 days',
        ]
    )
    status, stdout, stderr = run_no_traceback(
        b2_cli,
        [
            'bucket',
            'update',
            'b8-none',
            'allPublic',
            '--default-retention-mode',
            'none',
            '--default-retention-period',
            '1 days',
        ],
    )
    assert status == 0
    assert stderr == ''
    assert json.loads(stdout)['defaultRetention'] == {'mode': None}
    bucket = b2_cli.b2_api.get_bucket_by_name('b8-none')
    assert bucket.default_retention.mode.value is None
    assert bucket.default_retention.period is None
    assert bucket.type_ == 'allPublic'


@pytest.mark.parametrize('mode', [None, 'none', 'compliance', 'governance'])
def test_valid_bucket_default_retention_options(b2_cli, mode):
    b2_cli.authorize()
    b2_cli.run(
        ['bucket', 'create', 'b8-valid', 'allPrivate', '--file-lock-enabled'],
        expected_stdout='bucket_0\n',
    )
    options = [] if mode is None else ['--default-retention-mode', mode]
    if mode in ('compliance', 'governance'):
        options += ['--default-retention-period', '1 days']
    status, stdout, stderr = run_no_traceback(
        b2_cli, ['bucket', 'update', 'b8-valid', 'allPublic', *options]
    )
    assert status == 0
    assert stderr == ''
    retention = json.loads(stdout)['defaultRetention']
    expected = {'mode': None}
    if mode in ('compliance', 'governance'):
        expected = {'mode': mode, 'period': {'duration': 1, 'unit': 'days'}}
    assert retention == expected


def _run_sync_with_keep_days(b2_cli, tmp_path, keep_days):
    src = tmp_path / 'src'
    dst = tmp_path / 'dst'
    src.mkdir()
    dst.mkdir()
    (src / 'a.txt').write_text('a')
    with mock.patch('b2._internal.console_tool.Synchronizer') as synchronizer:
        status, _, stderr = run_no_traceback(
            b2_cli, ['sync', f'--keep-days={keep_days}', str(src), str(dst)]
        )
    return status, stderr, synchronizer


def _assert_keep_days_rejected(keep_days, status, stderr, synchronizer):
    """The contract is rejection: nonzero status, an error naming --keep-days, nothing synchronized."""
    assert status != 0, (
        f'--keep-days={keep_days} was accepted: '
        f'{synchronizer.call_args and synchronizer.call_args.kwargs}'
    )
    assert '--keep-days' in stderr, f'the error does not identify --keep-days: {stderr!r}'
    synchronizer.assert_not_called()


@known_defect(
    'sync accepts a negative or zero --keep-days',
    match='--keep-days=-1 was accepted:',
    assertion_origin='_assert_keep_days_rejected',
)
def test_sync_negative_keep_days_is_rejected(b2_cli, tmp_path):
    """A negative --keep-days must be rejected, not accepted as a retention period."""
    status, stderr, synchronizer = _run_sync_with_keep_days(b2_cli, tmp_path, '-1')
    _assert_keep_days_rejected('-1', status, stderr, synchronizer)


@known_defect(
    'sync accepts a negative or zero --keep-days',
    match='--keep-days=0 was accepted:',
    assertion_origin='_assert_keep_days_rejected',
)
def test_sync_keep_days_zero_is_rejected(b2_cli, tmp_path):
    """--keep-days 0 must be rejected, not silently turned into NO_DELETE (nothing is ever deleted)."""
    status, stderr, synchronizer = _run_sync_with_keep_days(b2_cli, tmp_path, '0')
    _assert_keep_days_rejected('0', status, stderr, synchronizer)


@known_defect(
    'a rejected authorize attempt wipes the previously stored authorization',
    match='^previous\\ authorization\\ was\\ wiped\\ by\\ the\\ rejected\\ key',
)
def test_failed_authorize_keeps_previous_authorization(b2_cli):
    """A rejected key (no listBuckets) must not wipe the previously stored authorization."""
    b2_cli.authorize()
    b2_cli.run(['key', 'create', 'key1', 'listKeys'], expected_stdout='appKeyId0 appKey0\n')
    status, _, _ = run(b2_cli, ['account', 'authorize', 'appKeyId0', 'appKey0'])
    assert status == 1
    allowed = b2_cli.account_info.get_allowed()
    assert allowed is not None, 'previous authorization was wiped by the rejected key'
    assert 'listBuckets' in allowed['capabilities']


@known_defect(
    'bucket update prints more than its help text promises',
    match='^help\\ promises\\ the\\ ID,\\ command\\ prints\\ more',
)
def test_bucket_update_help_matches_printed_output(b2_cli, bucket_info):
    """Help says the bucket ID is printed; the printed output must agree with the help."""
    from b2._internal.console_tool import BucketUpdateBase

    help_text = BucketUpdateBase.__doc__
    assert 'Prints the ID of the bucket updated' in help_text  # documents the contract under test
    _, stdout, _ = b2_cli.run(['bucket', 'update', bucket_info['bucketName'], 'allPrivate'])
    assert stdout.strip() == bucket_info['bucketId'], 'help promises the ID, command prints more'


@known_defect(
    'ls --json leaves unterminated JSON when the listing fails midway',
    match='^stdout is not a complete JSON document:',
)
def test_ls_json_failure_midway_does_not_leave_unterminated_json(b2_cli, uploaded_file):
    """A failure in the middle of `ls --json` must not leave unterminated JSON on stdout."""
    real_ls = Bucket.ls

    def failing_ls(self, *args, **kwargs):
        yield next(iter(real_ls(self, *args, **kwargs)))
        raise B2SimpleError('injected failure')

    with mock.patch.object(Bucket, 'ls', failing_ls):
        status, stdout, _ = run(b2_cli, ['ls', '--json', f'b2://{uploaded_file["bucket"]}'])
    assert status != 0
    try:
        json.loads(stdout)
    except ValueError as e:
        raise AssertionError(f'stdout is not a complete JSON document: {stdout!r}') from e


@pytest.mark.parametrize(
    'argv',
    [
        ['file', 'upload', '--file-retention-mode', 'governance', 'my-bucket', 'x', 'y'],
        ['sync', '--threads', '2', '--sync-threads', '3', 'x', 'y'],
    ],
)
@known_defect(
    'bad option combinations raise ValueError instead of reporting an error',
    match="^command raised ValueError\\('(?:provide either both --retain-until and --file-retention-mode or none of them|--threads cannot be used with other thread options)'\\)",
    assertion_origin='run_no_traceback',
)
def test_bad_option_combinations_report_error_not_traceback(b2_cli, bucket, argv):
    """User input errors raised as bare ValueError must become `ERROR:` output."""
    status, _, stderr = run_no_traceback(b2_cli, argv)
    assert status != 0
    assert 'ERROR' in stderr or 'usage' in stderr.lower()


@pytest.mark.parametrize(
    'argv',
    [
        ['file', 'hide', 'b2://my-bucket/file1.txt', 'file1.txt'],
        ['file', 'hide', 'my-bucket'],
    ],
)
@known_defect(
    'conflicting file arguments raise ArgumentError instead of reporting a usage error',
    match='^command raised ArgumentError\\(.*(?:Both B2 URI and file name were provided|Incorrect B2 URI was provided)',
    assertion_origin='run_no_traceback',
)
def test_conflicting_file_arguments_report_usage_error(b2_cli, uploaded_file, argv):
    """ArgumentError raised in _run must be shown as a usage error, not a traceback."""
    status, _, stderr = run_no_traceback(b2_cli, argv)
    assert status != 0
    assert stderr


@known_defect(
    'a failed download leaves an empty temporary file behind',
    match='^failed\\ download\\ left\\ temporary\\ artifacts',
)
def test_failed_download_leaves_no_temporary_file(b2_cli, uploaded_file, tmp_path):
    """A failed download into a directory must not leave an empty mkstemp file behind."""
    out_dir = tmp_path / 'out'
    out_dir.mkdir()
    (out_dir / uploaded_file['fileName']).write_text('existing')  # forces the mkstemp branch
    before = sorted(p.name for p in out_dir.iterdir())
    with mock.patch.object(DownloadedFile, 'save_to', side_effect=B2SimpleError('injected')):
        status, _, _ = run(
            b2_cli,
            [
                'file',
                'download',
                f'b2://{uploaded_file["bucket"]}/{uploaded_file["fileName"]}',
                str(out_dir),
            ],
        )
    assert status != 0
    assert (
        sorted(p.name for p in out_dir.iterdir()) == before
    ), 'failed download left temporary artifacts'


@known_defect(
    'a custom header not given as NAME=VALUE is accepted',
    match='^malformed\\ header\\ accepted:\\ ',
)
def test_malformed_custom_header_is_rejected(b2_cli, bucket):
    """A header not given as NAME=VALUE must be rejected, not accepted with an empty value."""
    status, stdout, stderr = run(
        b2_cli,
        [
            'bucket',
            'notification-rule',
            'create',
            f'b2://{bucket}',
            'hdr-rule',
            '--webhook-url',
            'https://example.com/hook',
            '--event-type',
            'b2:ObjectCreated:*',
            '--custom-header',
            'X-Custom-Header: value',
        ],
    )
    assert status != 0, f'malformed header accepted: {stdout!r}'


# ----------------------------------------------------------------------------------------
# Low
# ----------------------------------------------------------------------------------------


@known_defect(
    'download with --skip-hash-verification still prints "Checksum matches"',
    match='^unchecked\\ download\\ claimed\\ checksum\\ matches',
)
def test_skip_hash_verification_does_not_claim_checksum_matches(b2_cli, uploaded_file, tmp_path):
    """ "Checksum matches" must not be printed when verification is skipped."""
    out = tmp_path / 'out.txt'
    status, stdout, _ = run(
        b2_cli,
        [
            'file',
            'download',
            '--skip-hash-verification',
            f'b2://{uploaded_file["bucket"]}/{uploaded_file["fileName"]}',
            str(out),
        ],
    )
    assert status == 0
    assert 'Checksum matches' not in stdout, 'unchecked download claimed checksum matches'


@pytest.fixture
def clean_root_logging():
    import logging

    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield root
    for handler in list(root.handlers):
        if handler not in handlers:
            root.removeHandler(handler)
            handler.close()
    root.setLevel(level)


def _logging_args(**overrides):
    import argparse

    values = {'log_config': None, 'verbose': False, 'debug_logs': False}
    values.update(overrides)
    return argparse.Namespace(**values)


@known_defect(
    'logging set-up does not match the documented --verbose / --debug-logs behaviour',
    match='^verbose\\ output\\ stream\\ differs\\ from\\ documented\\ stdout',
)
def test_verbose_logs_go_to_stdout_as_documented(b2_cli, clean_root_logging, monkeypatch, tmp_path):
    """README says "Verbose logs to stdout can be enabled with the --verbose flag"."""
    import sys

    monkeypatch.chdir(tmp_path)
    before = list(clean_root_logging.handlers)
    b2_cli.console_tool_class._setup_logging(_logging_args(verbose=True), ['b2'])
    (handler,) = [h for h in clean_root_logging.handlers if h not in before]
    assert handler.stream is sys.stdout, 'verbose output stream differs from documented stdout'


@pytest.mark.skipif(os.name != 'posix', reason='debug log file is secured only on POSIX')
@known_defect(
    'logging set-up does not match the documented --verbose / --debug-logs behaviour',
    match='^the\\ log\\ was\\ not\\ rotated',
)
def test_debug_log_file_rotates_at_midnight_as_documented(
    b2_cli, clean_root_logging, monkeypatch, tmp_path
):
    """README says --debug-logs writes b2_cli.log "with log rotation at midnight"."""
    import calendar
    import logging
    import time

    monkeypatch.chdir(tmp_path)
    old_tz = os.environ.get('TZ')
    os.environ['TZ'] = 'UTC'  # the clock below is read as UTC whatever the zone of the machine
    time.tzset()
    clock = [calendar.timegm((2030, 1, 1, 23, 59, 0))]  # one minute before midnight
    try:
        with mock.patch('time.time', lambda: clock[0]):
            with mock.patch('time.time_ns', lambda: int(clock[0] * 1_000_000_000)):
                b2_cli.console_tool_class._setup_logging(_logging_args(debug_logs=True), ['b2'])
                test_logger = logging.getLogger('b2-midnight-rotation-test')
                test_logger.error('record before midnight')
                clock[0] += 120  # two minutes later it is past midnight
                test_logger.error('record after midnight')
    finally:
        if old_tz is None:
            os.environ.pop('TZ', None)
        else:
            os.environ['TZ'] = old_tz
        time.tzset()

    contents = {p.name: p.read_text() for p in tmp_path.glob('b2_cli.log*')}
    assert 'b2_cli.log' in contents, contents.keys()
    assert 'record after midnight' in contents['b2_cli.log']
    assert 'record before midnight' not in contents['b2_cli.log'], 'the log was not rotated'
    rotated = {name: text for name, text in contents.items() if name != 'b2_cli.log'}
    assert any('record before midnight' in text for text in rotated.values()), rotated
    assert not any('record after midnight' in text for text in rotated.values()), rotated
