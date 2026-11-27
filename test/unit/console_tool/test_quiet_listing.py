######################################################################
#
# File: test/unit/console_tool/test_quiet_listing.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from io import StringIO
from unittest import mock

import pytest
from b2sdk.v2 import UploadSourceBytes


def _run(b2_cli, argv):
    stdout, stderr = StringIO(), StringIO()
    status = b2_cli.console_tool_class(stdout, stderr).run_command(['b2', *argv])
    return status, stdout.getvalue(), stderr.getvalue()


def _assert_quiet_matches_normal_at(b2_cli, argv, quiet_index):
    normal = _run(b2_cli, argv)
    quiet = _run(b2_cli, [*argv[:quiet_index], '-q', *argv[quiet_index:]])
    assert normal[0] == 0
    assert normal[1] != ''
    assert quiet == normal


@pytest.fixture
def two_files(b2_cli, bucket, local_file):
    for name in ('dir/a.txt', 'b.txt'):
        b2_cli.run(['file', 'upload', '--quiet', bucket, str(local_file), name])


@pytest.fixture
def large_file_with_parts(b2_cli, bucket_info):
    bucket = b2_cli.b2_api.get_bucket_by_name(bucket_info['bucketName'])
    file = b2_cli.b2_api.services.large_file.start_large_file(bucket.id_, 'large', 'text/plain', {})
    state = mock.MagicMock()
    state.has_error.return_value = False
    for part_number in (1, 2):
        bucket.api.services.upload_manager._upload_part(
            bucket.id_,
            file.file_id,
            UploadSourceBytes(b'hello world'),
            part_number,
            state,
            None,
            None,
        )
    return file


@pytest.mark.parametrize(
    'options',
    [[], ['--long'], ['--long', '--replication'], ['--recursive'], ['--versions']],
)
def test_quiet_ls_files(b2_cli, bucket, two_files, options):
    _assert_quiet_matches_normal_at(b2_cli, ['ls', *options, f'b2://{bucket}'], 1)


def test_quiet_ls_buckets(b2_cli, bucket_info):
    _assert_quiet_matches_normal_at(b2_cli, ['ls'], 1)


def test_quiet_bucket_list(b2_cli, bucket_info):
    _assert_quiet_matches_normal_at(b2_cli, ['bucket', 'list'], 2)


def test_quiet_deprecated_list_buckets(b2_cli, bucket_info):
    _assert_quiet_matches_normal_at(b2_cli, ['list-buckets'], 1)


@pytest.mark.parametrize('options', [[], ['--long']])
def test_quiet_key_list(b2_cli, bucket_info, options):
    b2_cli.run(['key', 'create', 'k1', 'listFiles'])
    _assert_quiet_matches_normal_at(b2_cli, ['key', 'list', *options], 2)


def test_quiet_deprecated_list_keys(b2_cli, bucket_info):
    b2_cli.run(['key', 'create', 'k1', 'listFiles'])
    _assert_quiet_matches_normal_at(b2_cli, ['list-keys'], 1)


def test_quiet_large_unfinished_list(b2_cli, bucket, large_file_with_parts):
    _assert_quiet_matches_normal_at(
        b2_cli, ['file', 'large', 'unfinished', 'list', f'b2://{bucket}'], 4
    )


def test_quiet_deprecated_list_unfinished_large_files(b2_cli, bucket, large_file_with_parts):
    _assert_quiet_matches_normal_at(b2_cli, ['list-unfinished-large-files', bucket], 1)


def test_quiet_large_parts(b2_cli, large_file_with_parts):
    _assert_quiet_matches_normal_at(
        b2_cli, ['file', 'large', 'parts', f'b2id://{large_file_with_parts.file_id}'], 3
    )


def test_quiet_deprecated_list_parts(b2_cli, large_file_with_parts):
    _assert_quiet_matches_normal_at(b2_cli, ['list-parts', large_file_with_parts.file_id], 1)


def test_quiet_bucket_get_download_auth(b2_cli, bucket):
    _assert_quiet_matches_normal_at(b2_cli, ['bucket', 'get-download-auth', bucket], 2)


def test_quiet_deprecated_get_download_auth(b2_cli, bucket):
    _assert_quiet_matches_normal_at(b2_cli, ['get-download-auth', bucket], 1)


def test_quiet_rm_dry_run_stays_quiet(b2_cli, bucket, two_files):
    # The dry-run preview is not part of this change and keeps honoring --quiet.
    assert _run(b2_cli, ['rm', '-q', '--dry-run', '-r', f'b2://{bucket}']) == (0, '', '')
