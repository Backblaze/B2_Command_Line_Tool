######################################################################
#
# File: test/unit/console_tool/test_low_audit_hardening.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pytest

from test.unit.console_tool.test_command_regressions import run_no_traceback


@pytest.mark.parametrize(
    'env_name,command',
    [
        ('B2_DESTINATION_SSE_C_KEY_B64', 'upload'),
        ('B2_SOURCE_SSE_C_KEY_B64', 'download'),
    ],
)
def test_invalid_sse_key_is_rejected(
    b2_cli, bucket, local_file, tmp_path, monkeypatch, env_name, command
):
    monkeypatch.setenv(env_name, 'YWJj$ZA==')
    if command == 'upload':
        argv = [
            'file',
            'upload',
            '--destination-server-side-encryption',
            'SSE-C',
            bucket,
            str(local_file),
            'file',
        ]
    else:
        argv = [
            'file',
            'download',
            '--source-server-side-encryption',
            'SSE-C',
            f'b2://{bucket}/file',
            str(tmp_path / 'out'),
        ]
    status, _, stderr = run_no_traceback(b2_cli, argv)
    assert status == 1
    assert 'ERROR:' in stderr and env_name in stderr
    assert 'YWJj$ZA==' not in stderr


@pytest.mark.parametrize(
    'case', ['missing-destination-key', 'missing-source-key', 'retention', 'logging']
)
def test_invalid_options_report_command_error(
    b2_cli, bucket, local_file, tmp_path, monkeypatch, case
):
    monkeypatch.delenv('B2_DESTINATION_SSE_C_KEY_B64', raising=False)
    monkeypatch.delenv('B2_SOURCE_SSE_C_KEY_B64', raising=False)
    if case == 'missing-destination-key':
        argv = [
            'file',
            'upload',
            '--destination-server-side-encryption',
            'SSE-C',
            bucket,
            str(local_file),
            'file',
        ]
    elif case == 'missing-source-key':
        argv = [
            'file',
            'download',
            '--source-server-side-encryption',
            'SSE-C',
            f'b2://{bucket}/file',
            str(tmp_path / 'out'),
        ]
    elif case == 'retention':
        argv = [
            'file',
            'upload',
            '--file-retention-mode',
            'governance',
            bucket,
            str(local_file),
            'file',
        ]
    else:
        argv = ['version', '--log-config', 'unused.ini', '--verbose']
    status, _, stderr = run_no_traceback(b2_cli, argv)
    assert status == 1
    assert 'ERROR:' in stderr
    assert 'Traceback' not in stderr


def test_restricted_key_authorization_is_not_cleared(b2_cli):
    from test.unit.console_tool.test_command_regressions import run

    b2_cli.authorize()
    b2_cli.run(['key', 'create', 'key1', 'listKeys'], expected_stdout='appKeyId0 appKey0\n')
    status, _, stderr = run(b2_cli, ['account', 'authorize', 'appKeyId0', 'appKey0'])
    assert status == 1
    assert 'listBuckets' in stderr
    assert b2_cli.account_info.get_application_key_id() == 'appKeyId0'
    assert b2_cli.account_info.get_allowed()['capabilities'] == ['listKeys']


def test_failed_download_does_not_claim_checksum_matches(b2_cli, uploaded_file, tmp_path):
    from unittest import mock

    from b2._internal.console_tool import B2URIAdapter, CommandError
    from test.unit.console_tool.test_command_regressions import run

    download = B2URIAdapter.__dict__['download_file_by_uri']

    def fail_during_save(adapter, *args, **kwargs):
        result = download.__get__(adapter, type(adapter))(*args, **kwargs)
        result.save_to = mock.Mock(side_effect=CommandError('download failed'))
        return result

    with mock.patch.object(B2URIAdapter, 'download_file_by_uri', fail_during_save):
        status, stdout, stderr = run(
            b2_cli,
            [
                'file',
                'download',
                f'b2://{uploaded_file["bucket"]}/{uploaded_file["fileName"]}',
                str(tmp_path / 'out'),
            ],
        )
    assert status == 1
    assert 'ERROR:' in stderr
    assert 'Checksum matches' not in stdout


def test_listing_failure_before_first_item_does_not_emit_json(b2_cli, bucket):
    from unittest import mock

    from b2._internal.console_tool import CommandError
    from test.unit.console_tool.test_command_regressions import run

    def fail(*args, **kwargs):
        raise CommandError('listing failed')

    bucket_obj = b2_cli.b2_api.get_bucket_by_name(bucket)
    with mock.patch.object(type(bucket_obj), 'ls', side_effect=fail):
        status, stdout, stderr = run(b2_cli, ['ls', '--json', f'b2://{bucket}'])
    assert status == 1
    assert stdout == ''
    assert 'listing failed' in stderr
