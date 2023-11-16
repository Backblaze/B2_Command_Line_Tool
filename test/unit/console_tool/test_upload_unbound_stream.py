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

from b2sdk.v2 import DEFAULT_MIN_PART_SIZE


@skip_on_windows
def test_upload_unbound_stream__named_pipe(b2_cli, bucket, tmpdir, bg_executor):
    """Test upload_unbound_stream supports named pipes"""
    filename = 'named_pipe.txt'
    content = 'hello world'
    fifo_file = tmpdir.join('fifo_file.txt')
    os.mkfifo(str(fifo_file))
    writer = bg_executor.submit(fifo_file.write, content)  # writer will block until content is read

    expected_stdout = f'URL by file name: http://download.example.com/file/my-bucket/{filename}'
    expected_json = {
        "action": "upload",
        "contentSha1": "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed",
        "fileName": filename,
        "size": len(content),
    }
    b2_cli.run(
        ['upload-unbound-stream', '--noProgress', 'my-bucket',
         str(fifo_file), filename],
        expected_json_in_stdout=expected_json,
        remove_version=True,
        expected_part_of_stdout=expected_stdout,
    )
    writer.result(timeout=1)


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


@skip_on_windows
def test_upload_unbound_stream__with_part_size_options(
    b2_cli, bucket, tmpdir, mock_stdin, bg_executor
):
    """Test upload_unbound_stream with part size options"""
    part_size = DEFAULT_MIN_PART_SIZE
    expected_size = part_size + 500  # has to be bigger to force multipart upload

    filename = 'named_pipe.txt'
    fifo_file = tmpdir.join('fifo_file.txt')
    os.mkfifo(str(fifo_file))
    writer = bg_executor.submit(
        lambda: fifo_file.write("x" * expected_size)
    )  # writer will block until content is read

    expected_stdout = f'URL by file name: http://download.example.com/file/my-bucket/{filename}'
    expected_json = {
        "action": "upload",
        "fileName": filename,
        "size": expected_size,
    }

    b2_cli.run(
        [
            'upload-unbound-stream',
            '--minPartSize',
            str(DEFAULT_MIN_PART_SIZE),
            '--partSize',
            str(part_size),
            '--noProgress',
            'my-bucket',
            str(fifo_file),
            filename,
        ],
        expected_json_in_stdout=expected_json,
        remove_version=True,
        expected_part_of_stdout=expected_stdout,
    )
    writer.result(timeout=1)


def test_upload_unbound_stream__regular_file(b2_cli, bucket, tmpdir):
    """Test upload_unbound_stream regular file support"""
    content = "stdin input"
    filename = 'file.txt'
    filepath = tmpdir.join(filename)
    filepath.write(content)

    expected_stdout = f'URL by file name: http://download.example.com/file/my-bucket/{filename}'
    expected_json = {
        "action": "upload",
        "contentSha1": "2ce72aa159d1f190fddf295cc883f20c4787a751",
        "fileName": filename,
        "size": len(content),
    }

    b2_cli.run(
        ['upload-unbound-stream', '--noProgress', 'my-bucket',
         str(filepath), filename],
        expected_json_in_stdout=expected_json,
        remove_version=True,
        expected_part_of_stdout=expected_stdout,
        expected_stderr=
        "WARNING: You are using a stream upload command to upload a regular file. While it will work, it is inefficient. Use of upload-file command is recommended.\n",
    )
