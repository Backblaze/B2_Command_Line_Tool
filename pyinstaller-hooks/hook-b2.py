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
    # When '.' was provided here, the license file was copied to the root of the executable.
    # Before ApiVer, it pasted the file to the `b2/` directory.
    # I have no idea why it worked before or how it works now.
    # If you mean to debug it in the future, know that `pyinstaller` provides a special
    # attribute in the `sys` module whenever it runs.
    #
    # Example:
    #     import sys
    #     if hasattr(sys, '_MEIPASS'):
    #         self._print(f'{NAME}')
    #         self._print(f'{sys._MEIPASS}')
    #         elems = [elem for elem in pathlib.Path(sys._MEIPASS).glob('**/*')]
    #         self._print(f'{elems}')
    #
    # If used at the very start of the `_run` of `Licenses` command, it will print
    # all the files that were unpacked from the executable.
    (str(license_file), 'b2/'),
]
