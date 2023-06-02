######################################################################
#
# File: test/unit/console_tool/test_upload_unbound_stream.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os

from test.helpers import skip_on_windows
from test.unit.helpers import run_in_background


@skip_on_windows
def test_upload_unbound_stream__named_pipe(b2_cli, bucket, tmpdir):
    """Test upload_unbound_stream supports named pipes"""
    filename = 'named_pipe.txt'
    content = 'hello world'
    local_file1 = tmpdir.join('file1.txt')
    os.mkfifo(str(local_file1))
    writer = run_in_background(
        local_file1.write, content
    )  # writer will block until content is read

    expected_stdout = f'URL by file name: http://download.example.com/file/my-bucket/{filename}'
    expected_json = {
        "action": "upload",
        "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
        "fileName": filename,
        "size": len(content),
    }
    b2_cli.run(
        ['upload-unbound-stream', '--noProgress', 'my-bucket',
         str(local_file1), filename],
        expected_json_in_stdout=expected_json,
        remove_version=True,
        expected_part_of_stdout=expected_stdout,
    )
    writer.join()


def test_upload_unbound_stream__stdin(b2_cli, bucket, tmpdir, mock_stdin):
    """Test upload_unbound_stream stdin alias support"""
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
        ['upload-unbound-stream', '--noProgress', 'my-bucket', '-', filename],
        expected_json_in_stdout=expected_json,
        remove_version=True,
        expected_part_of_stdout=expected_stdout,
    )
