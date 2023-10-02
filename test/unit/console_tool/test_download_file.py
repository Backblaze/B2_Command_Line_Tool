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

def test_download_file_by_name(b2_cli, bucket, tmpdir):
    """Test download-file-by-name command"""
    
    filename = 'file1.txt'
    content = 'hello world'
    local_file = tmpdir.join(filename)
    local_file.write(content)

    mod_time = 1500111222
    os.utime(local_file, (mod_time, mod_time))

    # Upload the file
    b2_cli.run(['upload-file', 'my-bucket', str(local_file), filename])

    expected_stdout = '''
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

    b2_cli._run_command(
        [
            'download-file-by-name', '--noProgress', 'my-bucket', filename,
            str(local_file)
        ], 
        expected_stdout=expected_stdout, 
    )
    assert local_file.read() == content


def test_download_file_by_name_quietly(b2_cli, bucket, tmpdir):
    """Test download-file-by-name command, with --quiet argument"""
    
    filename = 'file1.txt'
    content = 'hello world'
    local_file = tmpdir.join(filename)
    local_file.write(content)

    mod_time = 1500111222
    os.utime(local_file, (mod_time, mod_time))

    # Upload the file
    b2_cli.run(['upload-file', 'my-bucket', str(local_file), filename])

    b2_cli._run_command(
        [
            'download-file-by-name', '--quiet', 'my-bucket', filename,
            str(local_file)
        ], 
        expected_stdout='', 
    )
    assert local_file.read() == content


def test_download_file_by_id(b2_cli, bucket, tmpdir):
    """Test download-file-by-id command"""

    filename = 'file1.txt'
    content = 'hello world'
    local_file = tmpdir.join(filename)
    local_file.write(content)

    mod_time = 1500111222
    os.utime(local_file, (mod_time, mod_time))

    # Upload the file
    b2_cli.run(['upload-file', 'my-bucket', str(local_file), filename])
    
    expected_stdout = '''
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

    b2_cli.run(
        [
            'download-file-by-id', '--noProgress', '9999',
            str(filename)
        ],
        expected_stdout=expected_stdout
    )
    assert local_file.read() == content


def test_download_file_by_id_quietly(b2_cli, bucket, tmpdir):
    """Test download-file-by-id command, with --quiet argument"""

    filename = 'file1.txt'
    content = 'hello world'
    local_file = tmpdir.join(filename)
    local_file.write(content)

    mod_time = 1500111222
    os.utime(local_file, (mod_time, mod_time))

    # Upload the file
    b2_cli.run(['upload-file', 'my-bucket', str(local_file), filename])

    b2_cli.run(
        [
            'download-file-by-id', '--quiet', '9999',
            str(filename)
        ],
        expected_stdout=''
    )
    assert local_file.read() == content
