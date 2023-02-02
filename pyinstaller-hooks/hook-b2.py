######################################################################
#
# File: pyinstaller-hooks/hook-b2.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from pathlib import Path

license_file = Path('b2/licenses_output.txt')
assert license_file.exists()
datas = [
    (str(license_file), '.'),
]
