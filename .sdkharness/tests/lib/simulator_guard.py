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


def scrubbed_environment(environment: Mapping[str, str]) -> dict[str, str]:
    """A copy of ``environment`` without any ``B2_*`` value (set explicitly afterwards)."""
    return {name: value for name, value in environment.items() if not name.startswith('B2_')}
