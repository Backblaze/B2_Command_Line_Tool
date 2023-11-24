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


@pytest.mark.parametrize(
    'flag,expected_stdout', [
        ('--noProgress', EXPECTED_STDOUT_DOWNLOAD),
        ('-q', ''),
        ('--quiet', ''),
    ]
)
def test_download_file_by_uri__flag_support(b2_cli, uploaded_file, tmp_path, flag, expected_stdout):
    output_path = tmp_path / 'output.txt'

    b2_cli.run(
        ['download-file', flag, 'b2id://9999',
         str(output_path)], expected_stdout=expected_stdout
    )
    assert output_path.read_text() == uploaded_file['content']


@pytest.mark.parametrize('b2_uri', [
    'b2://my-bucket/file1.txt',
    'b2id://9999',
])
def test_download_file_by_uri__b2_uri_support(b2_cli, uploaded_file, tmp_path, b2_uri):
    output_path = tmp_path / 'output.txt'

    b2_cli.run(
        ['download-file', b2_uri, str(output_path)], expected_stdout=EXPECTED_STDOUT_DOWNLOAD
    )
    assert output_path.read_text() == uploaded_file['content']


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
        expected_stdout=EXPECTED_STDOUT_DOWNLOAD,
        expected_stderr=
        'WARNING: download-file-by-name command is deprecated. Use download-file instead.\n',
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
        ['download-file-by-id', flag, '9999', str(output_path)],
        expected_stdout=expected_stdout,
        expected_stderr=
        'WARNING: download-file-by-id command is deprecated. Use download-file instead.\n',
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
        expected_stdout=EXPECTED_STDOUT_DOWNLOAD,
        expected_stderr=
        'WARNING: download-file-by-name command is deprecated. Use download-file instead.\n',
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
        expected_stderr=
        'WARNING: download-file-by-name command is deprecated. Use download-file instead.\n',
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
    assert "argument B2_URI: Unsupported URI scheme: ''" in capfd.readouterr().err


def test_cat__b2_uri__not_a_file(b2_cli, bucket, capfd):
    b2_cli.run(
        ['cat', "b2://bucket/dir/subdir/"],
        expected_stderr=None,
        expected_status=2,
    )
    assert "argument B2_URI: B2 URI pointing to a file-like object is required" in capfd.readouterr(
    ).err


def test_cat__b2id_uri(b2_cli, bucket, uploaded_stdout_txt, tmp_path, capfd):
    """Test download_file_by_name stdout alias support"""
    b2_cli.run(['cat', '--noProgress', "b2id://9999"],)
    assert capfd.readouterr().out == uploaded_stdout_txt['content']
