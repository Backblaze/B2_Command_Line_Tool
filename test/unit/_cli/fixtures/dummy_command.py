#!/usr/bin/env python
######################################################################
#
# File: test/unit/_cli/fixtures/dummy_command.py
#
# Copyright 2024 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
import argparse


def main():
    parser = argparse.ArgumentParser(description="Dummy command")
    parser.add_argument("--foo", help="foo help")
    parser.add_argument("--bar", help="bar help")
    args = parser.parse_args()
    print(args.foo)
    print(args.bar)


if __name__ == "__main__":
    main()
