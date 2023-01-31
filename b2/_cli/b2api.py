######################################################################
#
# File: b2/_cli/b2api.py
#
# Copyright 2023 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import os
from typing import Optional

from b2sdk.account_info.sqlite_account_info import SqliteAccountInfo
from b2sdk.api_config import B2HttpApiConfig
from b2sdk.cache import AuthInfoCache
from b2sdk.v2 import B2Api

from b2._cli.const import B2_USER_AGENT_APPEND_ENV_VAR


def _get_b2api_for_profile(profile: Optional[str] = None, **kwargs) -> B2Api:
    account_info = SqliteAccountInfo(profile=profile)
    return B2Api(
        api_config=_get_b2httpapiconfig(),
        account_info=account_info,
        cache=AuthInfoCache(account_info),
        **kwargs,
    )


def _get_b2httpapiconfig():
    return B2HttpApiConfig(user_agent_append=os.environ.get(B2_USER_AGENT_APPEND_ENV_VAR),)
