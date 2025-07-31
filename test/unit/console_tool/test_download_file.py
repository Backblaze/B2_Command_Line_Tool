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

import pytest

from test.helpers import skip_on_windows

EXPECTED_STDOUT_DOWNLOAD = """
File name:           file1.txt
File id:             9999
Output file path:    {output_path}
File size:           11
Content type:        b2/x-auto
Content sha1:        2aae6c35c94fcfb415dbe95f408b9ce91ee846ed
Encryption:          none
Retention:           none
Legal hold:          <unset>
INFO src_last_modified_millis: 1500111222000
Checksum matches
Download finished
"""


@pytest.mark.parametrize(
    'flag,expected_stdout',
    [
        ('--no-progress', EXPECTED_STDOUT_DOWNLOAD),
        ('-q', ''),
        ('--quiet', ''),
    ],
)
def test_download_file_by_uri__flag_support(b2_cli, uploaded_file, tmp_path, flag, expected_stdout):
    output_path = tmp_path / 'output.txt'

    b2_cli.run(
        ['file', 'download', flag, 'b2id://9999', str(output_path)],
        expected_stdout=expected_stdout.format(output_path=pathlib.Path(output_path).resolve()),
    )
    assert output_path.read_text() == uploaded_file['content']

    b2_cli.run(
        ['download-file', flag, 'b2id://9999', str(output_path)],
        expected_stderr='WARNING: `download-file` command is deprecated. Use `file download` instead.\n',
        expected_stdout=expected_stdout.format(output_path=pathlib.Path(output_path).resolve()),
    )
    assert output_path.read_text() == uploaded_file['content']


@pytest.mark.parametrize(
    'b2_uri',
    [
        'b2://my-bucket/file1.txt',
        'b2id://9999',
    ],
)
def test_download_file_by_uri__b2_uri_support(b2_cli, uploaded_file, tmp_path, b2_uri):
    output_path = tmp_path / 'output.txt'

    b2_cli.run(
        ['file', 'download', b2_uri, str(output_path)],
        expected_stdout=EXPECTED_STDOUT_DOWNLOAD.format(
            output_path=pathlib.Path(output_path).resolve()
        ),
    )
    assert output_path.read_text() == uploaded_file['content']


@pytest.mark.parametrize(
    'flag,expected_stdout',
    [
        ('--no-progress', EXPECTED_STDOUT_DOWNLOAD),
        ('-q', ''),
        ('--quiet', ''),
    ],
)
def test_download_file_by_name(b2_cli, local_file, uploaded_file, tmp_path, flag, expected_stdout):
    output_path = tmp_path / 'output.txt'

    b2_cli.run(
        [
            'download-file-by-name',
            uploaded_file['bucket'],
            uploaded_file['fileName'],
            str(output_path),
        ],
        expected_stdout=EXPECTED_STDOUT_DOWNLOAD.format(
            output_path=pathlib.Path(output_path).resolve()
        ),
        expected_stderr='WARNING: `download-file-by-name` command is deprecated. Use `file download` instead.\n',
    )
    assert output_path.read_text() == uploaded_file['content']


@pytest.mark.parametrize(
    'flag,expected_stdout',
    [
        ('--no-progress', EXPECTED_STDOUT_DOWNLOAD),
        ('-q', ''),
        ('--quiet', ''),
    ],
)
def test_download_file_by_id(b2_cli, uploaded_file, tmp_path, flag, expected_stdout):
    output_path = tmp_path / 'output.txt'

    b2_cli.run(
        ['download-file-by-id', flag, '9999', str(output_path)],
        expected_stdout=expected_stdout.format(output_path=pathlib.Path(output_path).resolve()),
        expected_stderr='WARNING: `download-file-by-id` command is deprecated. Use `file download` instead.\n',
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
            'download-file-by-name',
            '--no-progress',
            uploaded_file['bucket'],
            uploaded_file['fileName'],
            str(output_path),
        ],
        expected_stdout=EXPECTED_STDOUT_DOWNLOAD.format(
            output_path=pathlib.Path(output_path).resolve()
        ),
        expected_stderr='WARNING: `download-file-by-name` command is deprecated. Use `file download` instead.\n',
    )
    reader_future.result(timeout=1)
    assert output_string == uploaded_file['content']


@pytest.fixture
def uploaded_stdout_txt(b2_cli, bucket, local_file, tmp_path):
    local_file.write_text('non-mocked /dev/stdout test ignore me')
    b2_cli.run(['file', 'upload', bucket, str(local_file), 'stdout.txt'])
    return {
        'bucket': bucket,
        'fileName': 'stdout.txt',
        'content': local_file.read_text(),
    }


def test_download_file_by_name__to_stdout_by_alias(
    b2_cli, bucket, uploaded_stdout_txt, tmp_path, capfd
):
    """Test download-file-by-name stdout alias support"""
    b2_cli.run(
        ['download-file-by-name', '--no-progress', bucket, uploaded_stdout_txt['fileName'], '-'],
        expected_stderr='WARNING: `download-file-by-name` command is deprecated. Use `file download` instead.\n',
    )
    assert capfd.readouterr().out == uploaded_stdout_txt['content']
    assert not pathlib.Path('-').exists()


def test_cat__b2_uri(b2_cli, bucket, uploaded_stdout_txt, tmp_path, capfd):
    b2_cli.run(
        ['file', 'cat', '--no-progress', f"b2://{bucket}/{uploaded_stdout_txt['fileName']}"],
    )
    assert capfd.readouterr().out == uploaded_stdout_txt['content']


def test_cat__b2_uri__invalid(b2_cli, capfd):
    b2_cli.run(
        ['file', 'cat', 'nothing/meaningful'],
        expected_stderr=None,
        expected_status=2,
    )
    assert "argument B2_URI: Invalid B2 URI: 'nothing/meaningful'" in capfd.readouterr().err


def test_cat__b2_uri__not_a_file(b2_cli, bucket, capfd):
    b2_cli.run(
        ['file', 'cat', 'b2://bucket/dir/subdir/'],
        expected_stderr=None,
        expected_status=2,
    )
    assert (
        'argument B2_URI: B2 URI pointing to a file-like object is required'
        in capfd.readouterr().err
    )


def test_cat__b2id_uri(b2_cli, bucket, uploaded_stdout_txt, tmp_path, capfd):
    b2_cli.run(
        ['file', 'cat', '--no-progress', 'b2id://9999'],
    )
    assert capfd.readouterr().out == uploaded_stdout_txt['content']

    b2_cli.run(
        ['cat', '--no-progress', 'b2id://9999'],
        expected_stderr='WARNING: `cat` command is deprecated. Use `file cat` instead.\n',
    )
    assert capfd.readouterr().out == uploaded_stdout_txt['content']


def test__download_file__threads(b2_cli, local_file, uploaded_file, tmp_path):
    num_threads = 13
    output_path = tmp_path / 'output.txt'

    b2_cli.run(
        [
            'file',
            'download',
            '--no-progress',
            '--threads',
            str(num_threads),
            'b2://my-bucket/file1.txt',
            str(output_path),
        ]
    )

    assert output_path.read_text() == uploaded_file['content']
    assert b2_cli.console_tool.api.services.download_manager.get_thread_pool_size() == num_threads
