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
from b2._internal.console_tool import DownloadCommand, _TqdmCloser
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


@pytest.mark.parametrize('control', ['\r', '\n', '\t'])
def test_hide_with_uri_controls_does_not_hide_another_object(b2_cli, api_bucket, control, capsys):
    uploaded = api_bucket.upload_bytes(b'original', 'ab')
    status, _, _ = run(b2_cli, ['file', 'hide', f'b2://{api_bucket.name}/a{control}b'])
    assert status == 2
    assert 'control characters' in capsys.readouterr().err
    assert api_bucket.get_file_info_by_name('ab').id_ == uploaded.id_


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


def test_replication_status_rejects_unknown_column(b2_cli, bucket_info):
    b2_cli.run(['bucket', 'create', 'dest-bucket', 'allPrivate'], expected_stdout='bucket_1\n')
    b2_cli.run(
        ['replication', 'setup', '--name', 'rule1', bucket_info['bucketName'], 'dest-bucket'],
        expected_part_of_stdout='',
    )
    columns = [
        'source_replication_status',
        'source_has_hide_marker',
        'source_encryption_mode',
        'source_has_large_metadata',
        'source_has_file_retention',
        'source_has_legal_hold',
        'destination_replication_status',
        'metadata_differs',
        'hash_differs',
        'count',
    ]
    with mock.patch(
        'b2._internal.console_tool.ReplicationStatusBase.get_results_for_rule',
        return_value=[dict.fromkeys(columns, 1)],
    ):
        status, stdout, stderr = run_no_traceback(
            b2_cli,
            [
                'replication',
                'status',
                '--no-progress',
                '--columns',
                'foo',
                bucket_info['bucketName'],
            ],
        )
        assert status == 1
        assert 'foo' in stderr and all(column in stderr for column in columns)
        assert 'Traceback' not in stderr
        status, stdout, stderr = run(
            b2_cli,
            [
                'replication',
                'status',
                '--no-progress',
                '--output-format',
                'json',
                '--columns',
                'count',
                bucket_info['bucketName'],
            ],
        )
        assert status == 0
        assert json.loads(stdout) == {'rule1': [{'count': 1}]}


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


@pytest.mark.parametrize('invalid_value', ['abc', '2'])
def test_invalid_escape_control_characters_env_does_not_crash(b2_cli, monkeypatch, invalid_value):
    """Invalid values warn and let commands use the terminal-based default."""
    b2_cli.authorize()
    monkeypatch.setenv(B2_ESCAPE_CONTROL_CHARACTERS, invalid_value)
    with mock.patch('b2._internal.console_tool.logger.warning') as warning:
        status, _, _ = run(b2_cli, ['bucket', 'list'])
    assert status == 0
    warning.assert_called_once()
    assert (
        'invalid value for B2_ESCAPE_CONTROL_CHARACTERS environment variable'
        in warning.call_args.args[0]
    )

    for is_tty in (False, True):
        stdout = StringIO()
        stdout.isatty = lambda: is_tty
        tool = b2_cli.console_tool_class(stdout, StringIO())
        assert tool._get_default_escape_cc_setting() is is_tty


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
    # ... and legal hold must remain unchanged
    assert (
        stored_legal_hold() == legal_hold_before
    ), 'legal hold was applied although the command failed'


def test_file_update_failing_legal_hold_reports_applied_retention(b2_cli, lock_bucket_file):
    """A failed second update must identify the durable retention change."""
    with (
        mock.patch.object(
            b2_cli.b2_api,
            'update_file_retention',
            wraps=b2_cli.b2_api.update_file_retention,
        ) as update_retention,
        mock.patch.object(
            b2_cli.b2_api, 'update_file_legal_hold', side_effect=B2SimpleError('injected failure')
        ) as update_legal_hold,
    ):
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
    update_retention.assert_called_once()
    update_legal_hold.assert_called_once()
    assert status != 0
    assert 'File retention was applied, but legal hold update failed:' in stderr
    assert 'injected failure' in stderr
    file_version = b2_cli.b2_api.get_file_info(update_retention.call_args.args[0])
    assert file_version.file_retention.mode.value == 'governance'
    assert file_version.file_retention.retain_until == 4102444800000


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
    assert status == 1, (
        f'--keep-days={keep_days} was accepted: '
        f'{synchronizer.call_args and synchronizer.call_args.kwargs}'
    )
    assert stderr == 'ERROR: --keep-days must be greater than 0\n'
    synchronizer.assert_not_called()


def test_sync_negative_keep_days_is_rejected(b2_cli, tmp_path):
    """A negative --keep-days must be rejected, not accepted as a retention period."""
    status, stderr, synchronizer = _run_sync_with_keep_days(b2_cli, tmp_path, '-1')
    _assert_keep_days_rejected('-1', status, stderr, synchronizer)


def test_sync_keep_days_zero_is_rejected(b2_cli, tmp_path):
    """--keep-days 0 must be rejected, not silently turned into NO_DELETE (nothing is ever deleted)."""
    status, stderr, synchronizer = _run_sync_with_keep_days(b2_cli, tmp_path, '0')
    _assert_keep_days_rejected('0', status, stderr, synchronizer)


@known_defect(
    'a rejected authorize attempt wipes the previously stored authorization',
    match='^previous authorization was (?:wiped|replaced) by the rejected key',
)
def test_failed_authorize_keeps_previous_authorization(b2_cli):
    """A rejected key (no listBuckets) must not wipe the previously stored authorization."""
    b2_cli.authorize()
    b2_cli.run(['key', 'create', 'key1', 'listKeys'], expected_stdout='appKeyId0 appKey0\n')
    status, _, _ = run(b2_cli, ['account', 'authorize', 'appKeyId0', 'appKey0'])
    assert status == 1
    allowed = b2_cli.account_info.get_allowed()
    assert allowed is not None, 'previous authorization was wiped by the rejected key'
    assert (
        'listBuckets' in allowed['capabilities']
    ), 'previous authorization was replaced by the rejected key'


def test_bucket_update_help_matches_printed_output(b2_cli, bucket_info):
    """The bucket update help and output both describe the updated bucket as JSON."""
    from b2._internal.console_tool import BucketUpdateBase

    assert 'Prints the updated bucket as JSON' in BucketUpdateBase.__doc__
    _, stdout, _ = b2_cli.run(['bucket', 'update', bucket_info['bucketName'], 'allPrivate'])
    bucket = json.loads(stdout)
    assert bucket['bucketId'] == bucket_info['bucketId']
    assert bucket['bucketName'] == bucket_info['bucketName']


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
def test_conflicting_file_arguments_report_usage_error(b2_cli, uploaded_file, argv):
    """ArgumentError raised in _run must be shown as a usage error, not a traceback."""
    status, _, stderr = run_no_traceback(b2_cli, argv)
    assert status != 0
    assert stderr


def test_failed_download_leaves_no_temporary_file(b2_cli, uploaded_file, tmp_path):
    """A failed download into a directory must not leave an empty mkstemp file behind."""
    out_dir = tmp_path / 'out'
    out_dir.mkdir()
    existing = out_dir / uploaded_file['fileName']
    existing.write_text('existing')  # forces the mkstemp branch
    before = sorted(p.name for p in out_dir.iterdir())

    def fail_after_partial_write(path):
        path.write_text('partial')
        raise B2SimpleError('injected')

    with mock.patch.object(DownloadedFile, 'save_to', side_effect=fail_after_partial_write):
        status, _, stderr = run(
            b2_cli,
            [
                'file',
                'download',
                f'b2://{uploaded_file["bucket"]}/{uploaded_file["fileName"]}',
                str(out_dir),
            ],
        )
    assert status != 0
    assert 'injected' in stderr
    assert existing.read_text() == 'existing'
    assert (
        sorted(p.name for p in out_dir.iterdir()) == before
    ), 'failed download left temporary artifacts'


def test_failed_download_does_not_remove_explicit_destination(b2_cli, uploaded_file, tmp_path):
    destination = tmp_path / 'destination.txt'
    destination.write_text('existing')
    with mock.patch.object(DownloadedFile, 'save_to', side_effect=B2SimpleError('injected')):
        status, _, stderr = run(
            b2_cli,
            [
                'file',
                'download',
                f'b2://{uploaded_file["bucket"]}/{uploaded_file["fileName"]}',
                str(destination),
            ],
        )
    assert status != 0
    assert 'injected' in stderr
    assert destination.read_text() == 'existing'


def test_download_setup_failure_removes_temporary_file(b2_cli, uploaded_file, tmp_path):
    destination = tmp_path / uploaded_file['fileName']
    destination.write_text('existing')
    with mock.patch.object(
        DownloadCommand, '_print_download_info', side_effect=B2SimpleError('setup failed')
    ):
        status, _, stderr = run(
            b2_cli,
            [
                'file',
                'download',
                f'b2://{uploaded_file["bucket"]}/{uploaded_file["fileName"]}',
                str(tmp_path),
            ],
        )
    assert status != 0
    assert 'setup failed' in stderr
    assert destination.read_text() == 'existing'
    assert list(tmp_path.iterdir()) == [destination]


def test_cleanup_failure_preserves_download_error(b2_cli, uploaded_file, tmp_path):
    original = tmp_path / uploaded_file['fileName']
    original.write_text('existing')
    with (
        mock.patch.object(DownloadedFile, 'save_to', side_effect=B2SimpleError('download failed')),
        mock.patch('pathlib.Path.unlink', side_effect=PermissionError('cleanup failed')) as unlink,
    ):
        status, _, stderr = run(
            b2_cli,
            [
                'file',
                'download',
                f'b2://{uploaded_file["bucket"]}/{uploaded_file["fileName"]}',
                str(tmp_path),
            ],
        )
    assert status != 0
    assert 'download failed' in stderr
    unlink.assert_called_once()
    (remaining,) = [path for path in tmp_path.iterdir() if path != original]
    assert f'Unable to remove temporary download file: {remaining}' in stderr


def test_successful_download_keeps_unique_file(b2_cli, uploaded_file, tmp_path):
    destination = tmp_path / uploaded_file['fileName']
    destination.write_text('existing')
    status, _, _ = run(
        b2_cli,
        [
            'file',
            'download',
            f'b2://{uploaded_file["bucket"]}/{uploaded_file["fileName"]}',
            str(tmp_path),
        ],
    )
    assert status == 0
    assert destination.read_text() == 'existing'
    (downloaded,) = [path for path in tmp_path.iterdir() if path != destination]
    assert downloaded.read_text() == uploaded_file['content']


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
