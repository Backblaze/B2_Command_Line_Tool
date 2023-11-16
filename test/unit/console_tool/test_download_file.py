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
        'fileName': filename,
        'content': local_file.read_text(),
    }


@pytest.mark.parametrize(
    'flag,expected_stdout', [
        ('--noProgress', EXPECTED_STDOUT_DOWNLOAD),
        ('-q', ''),
        ('--quiet', ''),
    ]
)
def test_download_file_by_name(b2_cli, local_file, uploaded_file, tmp_path, flag, expected_stdout):
    output_path = tmp_path / 'output.txt'

    b2_cli.run(
        [
            'download-file-by-name', uploaded_file['bucket'], uploaded_file['fileName'],
            str(output_path)
        ],
        expected_stdout=EXPECTED_STDOUT_DOWNLOAD
    )
    assert output_path.read_text() == uploaded_file['content']


@pytest.mark.parametrize(
    'flag,expected_stdout', [
        ('--noProgress', EXPECTED_STDOUT_DOWNLOAD),
        ('-q', ''),
        ('--quiet', ''),
    ]
)
def test_download_file_by_id(b2_cli, uploaded_file, tmp_path, flag, expected_stdout):
    output_path = tmp_path / 'output.txt'

    b2_cli.run(
        ['download-file-by-id', flag, '9999', str(output_path)], expected_stdout=expected_stdout
    )
    assert output_path.read_text() == uploaded_file['content']


@skip_on_windows(reason='os.mkfifo is not supported on Windows')
def test_download_file_by_name__named_pipe(
    b2_cli, local_file, uploaded_file, tmp_path, bg_executor
):
    output_path = tmp_path / 'output.txt'
    os.mkfifo(output_path)

    output_string = None

    def reader():
        nonlocal output_string
        output_string = output_path.read_text()

    reader_future = bg_executor.submit(reader)

    b2_cli.run(
        [
            'download-file-by-name', '--noProgress', uploaded_file['bucket'],
            uploaded_file['fileName'],
            str(output_path)
        ],
        expected_stdout=EXPECTED_STDOUT_DOWNLOAD
    )
    reader_future.result(timeout=1)
    assert output_string == uploaded_file['content']


@pytest.fixture
def uploaded_stdout_txt(b2_cli, bucket, local_file, tmp_path):
    local_file.write_text('non-mocked /dev/stdout test ignore me')
    b2_cli.run(['upload-file', bucket, str(local_file), 'stdout.txt'])
    return {
        'bucket': bucket,
        'fileName': 'stdout.txt',
        'content': local_file.read_text(),
    }


def test_download_file_by_name__to_stdout_by_alias(
    b2_cli, bucket, uploaded_stdout_txt, tmp_path, capfd
):
    """Test download_file_by_name stdout alias support"""
    b2_cli.run(
        ['download-file-by-name', '--noProgress', bucket, uploaded_stdout_txt['fileName'], '-'],
    )
    assert capfd.readouterr().out == uploaded_stdout_txt['content']
    assert not pathlib.Path('-').exists()


def test_cat__b2_uri(b2_cli, bucket, uploaded_stdout_txt, tmp_path, capfd):
    """Test download_file_by_name stdout alias support"""
    b2_cli.run(['cat', '--noProgress', f"b2://{bucket}/{uploaded_stdout_txt['fileName']}"],)
    assert capfd.readouterr().out == uploaded_stdout_txt['content']


def test_cat__b2_uri__invalid(b2_cli, capfd):
    b2_cli.run(
        ['cat', "nothing/meaningful"],
        expected_stderr=None,
        expected_status=2,
    )
    assert "argument b2uri: Unsupported URI scheme: ''" in capfd.readouterr().err


def test_cat__b2id_uri(b2_cli, bucket, uploaded_stdout_txt, tmp_path, capfd):
    """Test download_file_by_name stdout alias support"""
    b2_cli.run(['cat', '--noProgress', "b2id://9999"],)
    assert capfd.readouterr().out == uploaded_stdout_txt['content']
