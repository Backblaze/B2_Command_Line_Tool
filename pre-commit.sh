#!/bin/bash -eux
yapf --in-place b2
pyflakes b2
time python test_b2_command_line.py ./b2 $(head -n 1 ~/.b2_auth) $(tail -n 1 ~/.b2_auth)
