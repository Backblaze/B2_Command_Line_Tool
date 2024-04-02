######################################################################
#
# File: b2/_internal/_cli/b2api.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import os
from typing import Optional

from b2sdk.v2 import (
    AuthInfoCache,
    B2Api,
    B2HttpApiConfig,
    InMemoryAccountInfo,
    InMemoryCache,
    SqliteAccountInfo,
)
from b2sdk.v2.exception import MissingAccountData

from b2._internal._cli.const import B2_USER_AGENT_APPEND_ENV_VAR


def _get_b2api_for_profile(
    profile: Optional[str] = None,
    raise_if_does_not_exist: bool = False,
    **kwargs,
) -> B2Api:

    if raise_if_does_not_exist:
        account_info_file = SqliteAccountInfo.get_user_account_info_path(profile=profile)
        if not os.path.exists(account_info_file):
            raise MissingAccountData(account_info_file)

    account_info = SqliteAccountInfo(profile=profile)
    b2api = B2Api(
        api_config=_get_b2httpapiconfig(),
        account_info=account_info,
        cache=AuthInfoCache(account_info),
        **kwargs,
    )

    if os.getenv('CI', False) and os.getenv(
        'GITHUB_REPOSITORY',
        '',
    ).endswith('/B2_Command_Line_Tool'):
        b2http = b2api.session.raw_api.b2_http
        b2http.CONNECTION_TIMEOUT = 3 + 6 + 1
        b2http.TIMEOUT = 12
        b2http.TIMEOUT_FOR_COPY = 24
        b2http.TIMEOUT_FOR_UPLOAD = 24
        b2http.TRY_COUNT_DATA = 2
        b2http.TRY_COUNT_DOWNLOAD = 2
        b2http.TRY_COUNT_HEAD = 2
        b2http.TRY_COUNT_OTHER = 2
    return b2api


def _get_inmemory_b2api(**kwargs) -> B2Api:
    return B2Api(InMemoryAccountInfo(), cache=InMemoryCache(), **kwargs)


def _get_b2httpapiconfig():
    return B2HttpApiConfig(user_agent_append=os.environ.get(B2_USER_AGENT_APPEND_ENV_VAR),)
