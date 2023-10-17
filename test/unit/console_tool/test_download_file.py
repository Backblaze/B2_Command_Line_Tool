######################################################################
#
# File: test/unit/console_tool/test_download_file.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import os
import pathlib
from test.helpers import skip_on_windows
from test.unit.helpers import RunOrDieExecutor

import pytest


@pytest.fixture
def local_file(tmp_path):
    """Set up a test file and return its path."""
    filename = 'file1.txt'
    content = 'hello world'
    local_file = tmp_path / filename
    local_file.write_text(content)

    mod_time = 1500111222
    os.utime(local_file, (mod_time, mod_time))

    return local_file


EXPECTED_STDOUT_DOWNLOAD = '''
File name:           file1.txt
File id:             9999
File size:           11
Content type:        b2/x-auto
Content sha1:        2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
Encryption:          none
Retention:           none
Legal hold:          <unset>
INFO src_last_modified_millis: 1500111222000
Checksum matches
Download finished
'''


@pytest.fixture
def uploaded_file(b2_cli, bucket, local_file):
    filename = 'file1.txt'
    b2_cli.run(['upload-file', bucket, str(local_file), filename])
    return {
        'bucket': bucket,
        'filename': filename,
        'content': local_file.read_text(),
    }


def test_download_file_by_name(b2_cli, local_file, uploaded_file, tmp_path):
    output_path = tmp_path / 'output.txt'

    b2_cli.run(
        [
            'download-file-by-name', '--noProgress', uploaded_file['bucket'],
            uploaded_file['filename'],
            str(output_path)
        ],
        expected_stdout=EXPECTED_STDOUT_DOWNLOAD
    )
    assert output_path.read_text() == uploaded_file['content']


def test_download_file_by_name_quietly(b2_cli, uploaded_file, tmp_path):
    output_path = tmp_path / 'output.txt'

    b2_cli.run(
        [
            'download-file-by-name', '--quiet', uploaded_file['bucket'], uploaded_file['filename'],
            str(output_path)
        ],
        expected_stdout=''
    )
    assert output_path.read_text() == uploaded_file['content']


def test_download_file_by_id(b2_cli, uploaded_file, tmp_path):
    output_path = tmp_path / 'output.txt'

    b2_cli.run(
        ['download-file-by-id', '--noProgress', '9999',
         str(output_path)],
        expected_stdout=EXPECTED_STDOUT_DOWNLOAD
    )
    assert output_path.read_text() == uploaded_file['content']


def test_download_file_by_id_quietly(b2_cli, uploaded_file, tmp_path):
    output_path = tmp_path / 'output.txt'

    b2_cli.run(['download-file-by-id', '--quiet', '9999', str(output_path)], expected_stdout='')
    assert output_path.read_text() == uploaded_file['content']


@skip_on_windows(reason='os.mkfifo is not supported on Windows')
def test_download_file_by_name__named_pipe(b2_cli, local_file, uploaded_file, tmp_path):
    output_path = tmp_path / 'output.txt'
    os.mkfifo(output_path)

    output_string = None

    def reader():
        nonlocal output_string
        output_string = output_path.read_text()

    with RunOrDieExecutor() as executor:
        reader_future = executor.submit(reader)

        b2_cli.run(
            [
                'download-file-by-name', '--noProgress', uploaded_file['bucket'],
                uploaded_file['filename'],
                str(output_path)
            ],
            expected_stdout=EXPECTED_STDOUT_DOWNLOAD
        )
        reader_future.result(timeout=1)
    assert output_string == uploaded_file['content']


def test_download_file_by_name__to_stdout_by_alias(b2_cli, bucket, local_file, tmp_path):
    """Test download_file_by_name stdout alias support"""
    local_file.write_text('non-mocked /dev/stdout test ignore me')
    b2_cli.run(['upload-file', bucket, str(local_file), 'stdout'])

    b2_cli.run(['download-file-by-name', '--noProgress', bucket, 'stdout', '-'],)
    assert True  # the only expectation we have is that this doesn't explode, as we cannot capture /dev/stdout
    assert not pathlib.Path('-').exists()
