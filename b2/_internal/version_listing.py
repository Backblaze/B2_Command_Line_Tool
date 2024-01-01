######################################################################
#
# File: b2/_internal/version_listing.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import pathlib
import re
from typing import List

RE_VERSION = re.compile(r'[_]*b2v(\d+)')


def get_versions() -> List[str]:
    return [path.name for path in sorted(pathlib.Path(__file__).parent.glob('*b2v*'))]


def get_int_version(version: str) -> int:
    match = RE_VERSION.match(version)
    assert match, f'Version {version} does not match pattern {RE_VERSION.pattern}'
    return int(match.group(1))


CLI_VERSIONS = get_versions()
UNSTABLE_CLI_VERSION = max(CLI_VERSIONS, key=get_int_version)
LATEST_STABLE_VERSION = max(
    [elem for elem in CLI_VERSIONS if not elem.startswith('_')], key=get_int_version
)
