######################################################################
#
# File: __main__.py
#
# Copyright 2018 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

from __future__ import absolute_import

from .console_tool import main

import cProfile as profile
import pstats, io
pr = profile.Profile()
pr.enable()

main()

pr.disable()
pr.dump_stats('10.pstats')
s = io.StringIO()
sortby = 'tottime'
ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
ps.print_stats()
print(s.getvalue())
