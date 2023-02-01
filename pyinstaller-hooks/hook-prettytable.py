######################################################################
#
# File: pyinstaller-hooks/hook-prettytable.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from PyInstaller.utils.hooks import collect_all

# prettytable is excluded because `prettytable` module in provided by `PTable` package;
# pyinstaller fails to resolve this, thus we do it manually here
excludedimports = ['prettytable']
datas, binaries, hiddenimports = collect_all('prettytable')
