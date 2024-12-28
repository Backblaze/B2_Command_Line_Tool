######################################################################
#
# File: test/unit/console_tool/test_upload_file.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os

import pytest

from test.helpers import skip_on_windows


def test_upload_file__file_info_src_last_modified_millis_and_headers(b2_cli, bucket, tmpdir):
    """Test `file upload` supports manually specifying file info src_last_modified_millis"""
    filename = 'file1.txt'
    content = 'hello world'
    local_file1 = tmpdir.join('file1.txt')
    local_file1.write(content)

    expected_json = {
        'action': 'upload',
        'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
        'fileInfo': {
            'b2-cache-control': 'max-age=3600',
            'b2-expires': 'Thu, 01 Dec 2050 16:00:00 GMT',
            'b2-content-language': 'en',
            'b2-content-disposition': 'attachment',
            'b2-content-encoding': 'gzip',
            'src_last_modified_millis': '1',
        },
        'fileName': filename,
        'size': len(content),
    }
    b2_cli.run(
        [
            'file',
            'upload',
            '--no-progress',
            '--info=src_last_modified_millis=1',
            'my-bucket',
            '--cache-control',
            'max-age=3600',
            '--expires',
            'Thu, 01 Dec 2050 16:00:00 GMT',
            '--content-language',
            'en',
            '--content-disposition',
            'attachment',
            '--content-encoding',
            'gzip',
            str(local_file1),
            'file1.txt',
        ],
        expected_json_in_stdout=expected_json,
        remove_version=True,
    )


@skip_on_windows
def test_upload_file__named_pipe(b2_cli, bucket, tmpdir, bg_executor):
    """Test `file upload` supports named pipes"""
    filename = 'named_pipe.txt'
    content = 'hello world'
    local_file1 = tmpdir.join('file1.txt')
    os.mkfifo(str(local_file1))
    writer = bg_executor.submit(
        local_file1.write, content
    )  # writer will block until content is read

    expected_stdout = f'URL by file name: http://download.example.com/file/my-bucket/{filename}'
    expected_json = {
        'action': 'upload',
        'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
        'contentType': 'b2/x-auto',
        'fileName': filename,
        'size': len(content),
    }
    b2_cli.run(
        ['file', 'upload', '--no-progress', 'my-bucket', str(local_file1), filename],
        expected_json_in_stdout=expected_json,
        remove_version=True,
        expected_part_of_stdout=expected_stdout,
    )
    writer.result(timeout=1)


@pytest.mark.apiver(to_ver=3)
def test_upload_file__hyphen_file_instead_of_stdin(b2_cli, bucket, tmpdir, monkeypatch):
    """Test `file upload` will upload file named `-` instead of stdin by default"""
    filename = 'stdin.txt'
    content = "I'm very rare creature, a file named '-'"
    monkeypatch.chdir(str(tmpdir))
    source_file = tmpdir.join('-')
    source_file.write(content)

    expected_stdout = f'URL by file name: http://download.example.com/file/my-bucket/{filename}'
    expected_json = {
        'action': 'upload',
        'contentSha1': 'ab467567b98216a255f77aef08aa2c418073d974',
        'fileName': filename,
        'size': len(content),
    }
    b2_cli.run(
        ['upload-file', '--no-progress', 'my-bucket', '-', filename],
        expected_json_in_stdout=expected_json,
        remove_version=True,
        expected_part_of_stdout=expected_stdout,
        expected_stderr='WARNING: `upload-file` command is deprecated. Use `file upload` instead.\n'
        "WARNING: Filename `-` won't be supported in the future and will always be treated as stdin alias.\n",
    )


@pytest.mark.apiver(from_ver=4)
def test_upload_file__ignore_hyphen_file(b2_cli, bucket, tmpdir, monkeypatch, mock_stdin):
    """Test `file upload` will upload stdin even when file named `-` is explicitly specified"""
    content = "I'm very rare creature, a file named '-'"
    monkeypatch.chdir(str(tmpdir))
    source_file = tmpdir.join('-')
    source_file.write(content)

    content = 'stdin input'
    filename = 'stdin.txt'

    expected_stdout = f'URL by file name: http://download.example.com/file/my-bucket/{filename}'
    expected_json = {
        'action': 'upload',
        'contentSha1': '2ce72aa159d1f190fddf295cc883f20c4787a751',
        'fileName': filename,
        'size': len(content),
    }
    mock_stdin.write(content)
    mock_stdin.close()

    b2_cli.run(
        ['file', 'upload', '--no-progress', 'my-bucket', '-', filename],
        expected_json_in_stdout=expected_json,
        remove_version=True,
        expected_part_of_stdout=expected_stdout,
    )


def test_upload_file__stdin(b2_cli, bucket, tmpdir, mock_stdin):
    """Test `file upload` stdin alias support"""
    content = 'stdin input'
    filename = 'stdin.txt'

    expected_stdout = f'URL by file name: http://download.example.com/file/my-bucket/{filename}'
    expected_json = {
        'action': 'upload',
        'contentSha1': '2ce72aa159d1f190fddf295cc883f20c4787a751',
        'fileName': filename,
        'size': len(content),
    }
    mock_stdin.write(content)
    mock_stdin.close()

    b2_cli.run(
        ['file', 'upload', '--no-progress', 'my-bucket', '-', filename],
        expected_json_in_stdout=expected_json,
        remove_version=True,
        expected_part_of_stdout=expected_stdout,
    )


def test_upload_file_deprecated__stdin(b2_cli, bucket, tmpdir, mock_stdin):
    """Test `upload-file` stdin alias support"""
    content = 'stdin input deprecated'
    filename = 'stdin-deprecated.txt'

    expected_stdout = f'URL by file name: http://download.example.com/file/my-bucket/{filename}'
    expected_json = {
        'action': 'upload',
        'contentSha1': 'fcaa935e050efe0b5d7b26e65162b32b5e40aa81',
        'fileName': filename,
        'size': len(content),
    }
    mock_stdin.write(content)
    mock_stdin.close()

    b2_cli.run(
        ['upload-file', '--no-progress', 'my-bucket', '-', filename],
        expected_stderr='WARNING: `upload-file` command is deprecated. Use `file upload` instead.\n',
        expected_json_in_stdout=expected_json,
        remove_version=True,
        expected_part_of_stdout=expected_stdout,
    )


def test_upload_file__threads_setting(b2_cli, bucket, tmp_path):
    """Test `file upload` supports setting number of threads"""
    num_threads = 66
    filename = 'file1.txt'
    content = 'hello world'
    local_file1 = tmp_path / 'file1.txt'
    local_file1.write_text(content)

    expected_json = {
        'action': 'upload',
        'contentSha1': '2aae6c35c94fcfb415dbe95f408b9ce91ee846ed',
        'fileInfo': {'src_last_modified_millis': f'{local_file1.stat().st_mtime_ns // 1000000}'},
        'fileName': filename,
        'size': len(content),
    }

    b2_cli.run(
        [
            'file',
            'upload',
            '--no-progress',
            'my-bucket',
            '--threads',
            str(num_threads),
            str(local_file1),
            'file1.txt',
        ],
        expected_json_in_stdout=expected_json,
        remove_version=True,
    )

    assert b2_cli.console_tool.api.services.upload_manager.get_thread_pool_size() == num_threads
