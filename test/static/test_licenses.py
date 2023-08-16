######################################################################
#
# File: test/static/test_licenses.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from datetime import datetime
from glob import glob
from itertools import islice

import pytest

FIXER_CMD = "python test/static/test_licenses.py"
LICENSE_HEADER_TMPL = """\
######################################################################
#
# File: {path}
#
# Copyright {year} Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""


def get_file_header_errors(file_path_glob: str) -> dict[str, str]:
    failed_files = {}
    for file in glob(file_path_glob, recursive=True):
        if file.startswith('build/'):
            # built files naturally have a different file path than source files
            continue
        with open(file) as fd:
            file = file.replace(
                '\\', '/'
            )  # glob('**/*.py') on Windows returns "b2\console_tool.py" (wrong slash)
            head = ''.join(islice(fd, 9))
            if 'All Rights Reserved' not in head:
                failed_files[file] = 'Missing "All Rights Reserved" in the header'
            elif file not in head:
                failed_files[file] = 'Wrong file name in the header'
    return failed_files


def test_files_headers():
    failed_files = get_file_header_errors('**/*.py')
    if failed_files:
        error_msg = '; '.join(f'{path}:{error}' for path, error in failed_files.items())
        pytest.fail(f'Bad file headers in files (you may want to run {FIXER_CMD!r}): {error_msg}')


def insert_header(file_path: str):
    with open(file_path, 'r+') as fd:
        content = fd.read()
        fd.seek(0)
        fd.write(LICENSE_HEADER_TMPL.format(
            path=file_path,
            year=datetime.now().year,
        ))
        fd.write(content)


def _main():
    failed_files = get_file_header_errors('**/*.py')
    for filepath in failed_files:
        insert_header(filepath)


if __name__ == '__main__':
    _main()
