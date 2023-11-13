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
from test.helpers import skip_on_windows

import b2


def test_upload_file__file_info_src_last_modified_millis(b2_cli, bucket, tmpdir):
    """Test upload_file supports manually specifying file info src_last_modified_millis"""
    filename = 'file1.txt'
    content = 'hello world'
    local_file1 = tmpdir.join('file1.txt')
    local_file1.write(content)

    expected_json = {
        "action": "upload",
        "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
        "fileInfo": {
            "src_last_modified_millis": "1"
        },
        "fileName": filename,
        "size": len(content),
    }
    b2_cli.run(
        [
            'upload-file', '--noProgress', '--info=src_last_modified_millis=1', 'my-bucket',
            str(local_file1), 'file1.txt'
        ],
        expected_json_in_stdout=expected_json,
        remove_version=True,
    )


@skip_on_windows
def test_upload_file__named_pipe(b2_cli, bucket, tmpdir, bg_executor):
    """Test upload_file supports named pipes"""
    filename = 'named_pipe.txt'
    content = 'hello world'
    local_file1 = tmpdir.join('file1.txt')
    os.mkfifo(str(local_file1))
    writer = bg_executor.submit(
        local_file1.write, content
    )  # writer will block until content is read

    expected_stdout = f'URL by file name: http://download.example.com/file/my-bucket/{filename}'
    expected_json = {
        "action": "upload",
        "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
        "contentType": "b2/x-auto",
        "fileName": filename,
        "size": len(content),
    }
    b2_cli.run(
        ['upload-file', '--noProgress', 'my-bucket',
         str(local_file1), filename],
        expected_json_in_stdout=expected_json,
        remove_version=True,
        expected_part_of_stdout=expected_stdout,
    )
    writer.result(timeout=1)


def test_upload_file__hyphen_file_instead_of_stdin(b2_cli, bucket, tmpdir, monkeypatch):
    """Test upload_file will upload file named `-` instead of stdin by default"""
    # TODO remove this in v4
    assert b2.__version__ < '4', "`-` filename should not be supported in next major version of CLI"
    filename = 'stdin.txt'
    content = "I'm very rare creature, a file named '-'"
    monkeypatch.chdir(str(tmpdir))
    source_file = tmpdir.join('-')
    source_file.write(content)

    expected_stdout = f'URL by file name: http://download.example.com/file/my-bucket/{filename}'
    expected_json = {
        "action": "upload",
        "contentSha1": "ab467567b98216a255f77aef08aa2c418073d974",
        "fileName": filename,
        "size": len(content),
    }
    b2_cli.run(
        ['upload-file', '--noProgress', 'my-bucket', '-', filename],
        expected_json_in_stdout=expected_json,
        remove_version=True,
        expected_part_of_stdout=expected_stdout,
        expected_stderr=
        "WARNING: Filename `-` won't be supported in the future and will always be treated as stdin alias.\n",
    )


def test_upload_file__stdin(b2_cli, bucket, tmpdir, mock_stdin):
    """Test upload_file stdin alias support"""
    content = "stdin input"
    filename = 'stdin.txt'

    expected_stdout = f'URL by file name: http://download.example.com/file/my-bucket/{filename}'
    expected_json = {
        "action": "upload",
        "contentSha1": "2ce72aa159d1f190fddf295cc883f20c4787a751",
        "fileName": filename,
        "size": len(content),
    }
    mock_stdin.write(content)
    mock_stdin.close()

    b2_cli.run(
        ['upload-file', '--noProgress', 'my-bucket', '-', filename],
        expected_json_in_stdout=expected_json,
        remove_version=True,
        expected_part_of_stdout=expected_stdout,
    )
