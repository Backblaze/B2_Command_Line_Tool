######################################################################
#
# File: .sdkharness/tests/lib/simulator_guard.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Keep the repository-owned checks on a loopback simulator with its fixed credential.

The executables here are runnable on their own, so each one applies these rules
itself: they must never carry a real ``B2_*`` credential to any listener, even a
local one, and must never run a CLI child that inherits one.
"""

from __future__ import annotations

from collections.abc import Mapping

# The simulator's fixed test credential. The harness supplies exactly this pair.
SIMULATOR_CREDENTIAL = ('test-key-id', 'test-key')
CREDENTIAL_REFUSAL = 'only the fixed simulator credential is accepted'


def credential_is_fixed(key_id: str, application_key: str) -> bool:
    return (key_id, application_key) == SIMULATOR_CREDENTIAL


# Variables that route (or exempt) HTTP traffic through a proxy, in either case.
PROXY_VARIABLES = frozenset(
    {'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy', 'ftp_proxy', 'grpc_proxy'}
)


def scrubbed_environment(environment: Mapping[str, str]) -> dict[str, str]:
    """A copy of ``environment`` without any ``B2_*`` or proxy variable.

    ``B2_*`` values are set explicitly afterwards. Proxy variables are dropped because a
    proxy configured on a developer or CI machine would otherwise carry the check's
    loopback traffic (and the fixed credential) to a third party, or break it. Loopback
    is then exempted explicitly, so nothing a child process inherits can reroute it.
    """
    kept = {
        name: value
        for name, value in environment.items()
        if not name.startswith('B2_') and name.lower() not in PROXY_VARIABLES
    }
    kept['NO_PROXY'] = kept['no_proxy'] = '127.0.0.1'
    return kept
