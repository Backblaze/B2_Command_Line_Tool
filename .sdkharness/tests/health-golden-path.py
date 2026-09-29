#!/usr/bin/env python3
######################################################################
#
# File: .sdkharness/tests/health-golden-path.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Repository-owned sdkharness customer-health lifecycle for the B2 CLI."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from urllib.parse import urlsplit

LEVEL = 'health'
SCENARIO = 'golden-path'
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

# The executable must load the CLI implementation from this checkout, not from
# whichever b2 distribution happens to provide dependencies in the environment.
sys.path.insert(0, str(REPOSITORY_ROOT))
import b2  # noqa: E402
from b2._internal.version_listing import LATEST_STABLE_VERSION  # noqa: E402


class CheckFailure(Exception):
    """A failed lifecycle step with a credential-safe reason."""

    def __init__(self, step: str, cause: BaseException | str) -> None:
        self.step = step
        self.detail = cause if isinstance(cause, str) else type(cause).__name__
        super().__init__(f'{step}: {self.detail}')


CommandRunner = Callable[[str, list[str], dict[str, str]], str]


def validate_environment(environment: Mapping[str, str]) -> tuple[str, str, str, str]:
    if environment.get('SDKHARNESS_TEST_LEVEL', LEVEL) != LEVEL:
        raise CheckFailure('configuration', 'unexpected test level')
    if environment.get('SDKHARNESS_SCENARIO', SCENARIO) != SCENARIO:
        raise CheckFailure('configuration', 'unexpected scenario')

    simulator_url = environment.get('SDKHARNESS_SIMULATOR_URL', '')
    if not simulator_url or environment.get('HEALTHCHECK_REALM_URL') != simulator_url:
        raise CheckFailure('configuration', 'simulator URL mismatch')

    try:
        parsed = urlsplit(simulator_url)
        port = parsed.port
    except ValueError as error:
        raise CheckFailure('configuration', 'invalid simulator URL') from error
    if (
        parsed.scheme != 'http'
        or parsed.hostname not in {'127.0.0.1', '::1'}
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {'', '/'}
        or parsed.query
        or parsed.fragment
    ):
        raise CheckFailure('configuration', 'simulator URL must be loopback HTTP')

    key_id = environment.get('B2_TEST_APPLICATION_KEY_ID', '')
    application_key = environment.get('B2_TEST_APPLICATION_KEY', '')
    bucket_name = environment.get('B2_BUCKET_NAME', '')
    if not key_id or not application_key or not bucket_name:
        raise CheckFailure('configuration', 'required simulator input is missing')
    return simulator_url, key_id, application_key, bucket_name


def repository_cli_prefix() -> list[str]:
    module_path = Path(b2.__file__).resolve()
    try:
        module_path.relative_to(REPOSITORY_ROOT)
    except ValueError as error:
        raise CheckFailure('setup', 'b2 CLI was not imported from this checkout') from error
    return [sys.executable, '-m', f'b2._internal.{LATEST_STABLE_VERSION}']


def payload_for(object_name: str) -> bytes:
    return b'B2 CLI health check ' + object_name.encode('ascii')


def subprocess_runner(step: str, command: list[str], environment: dict[str, str]) -> str:
    try:
        completed = subprocess.run(
            command,
            cwd=REPOSITORY_ROOT,
            env=environment,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as error:
        raise CheckFailure(step, error) from error
    if completed.returncode != 0:
        raise CheckFailure(step, 'command failed')
    return completed.stdout


def run_health(
    environment: Mapping[str, str],
    *,
    cli_prefix: Sequence[str] | None = None,
    run_command: CommandRunner = subprocess_runner,
    object_name: str | None = None,
    scratch_root: Path | None = None,
) -> None:
    simulator_url, key_id, application_key, bucket_name = validate_environment(environment)
    prefix = list(cli_prefix) if cli_prefix is not None else repository_cli_prefix()
    name = object_name or f'sdkharness-health-check/{uuid.uuid4().hex}.txt'
    payload = payload_for(name)
    cleanup_needed = False
    failure: CheckFailure | None = None

    with tempfile.TemporaryDirectory(dir=scratch_root) as scratch_name:
        scratch = Path(scratch_name)
        upload_path = scratch / 'upload.txt'
        download_path = scratch / 'download.txt'
        upload_path.write_bytes(payload)

        child_environment = dict(environment)
        child_environment.update(
            {
                'B2_ENVIRONMENT': simulator_url,
                'B2_APPLICATION_KEY_ID': key_id,
                'B2_APPLICATION_KEY': application_key,
                'B2_ACCOUNT_INFO': str(scratch / 'account-info'),
                'PYTHONPATH': os.pathsep.join(
                    filter(None, (str(REPOSITORY_ROOT), environment.get('PYTHONPATH', '')))
                ),
            }
        )

        def invoke(step: str, *arguments: str) -> str:
            return run_command(step, [*prefix, *arguments], child_environment)

        invoke('authenticate', 'account', 'authorize')
        try:
            # A failed upload may still have reached the service, so cleanup is
            # required from the moment the upload attempt begins.
            cleanup_needed = True
            invoke(
                'upload',
                'file',
                'upload',
                '--no-progress',
                bucket_name,
                str(upload_path),
                name,
            )
            invoke(
                'download',
                'file',
                'download',
                '--no-progress',
                f'b2://{bucket_name}/{name}',
                str(download_path),
            )
            if not download_path.is_file() or download_path.read_bytes() != payload:
                raise CheckFailure('verify bytes', 'download differs from upload')

            listed = invoke('list', 'ls', '--recursive', f'b2://{bucket_name}/{name}')
            if name not in listed:
                raise CheckFailure('list', 'uploaded object was not listed')

            invoke(
                'delete',
                'rm',
                '--versions',
                '--no-progress',
                '--fail-fast',
                f'b2://{bucket_name}/{name}',
            )
            cleanup_needed = False
            remaining = invoke('confirm gone', 'ls', '--recursive', f'b2://{bucket_name}/{name}')
            if name in remaining:
                raise CheckFailure('confirm gone', 'deleted object was still listed')
        except CheckFailure as error:
            failure = error
        finally:
            if cleanup_needed:
                try:
                    invoke(
                        'cleanup',
                        'rm',
                        '--versions',
                        '--no-progress',
                        '--fail-fast',
                        f'b2://{bucket_name}/{name}',
                    )
                except CheckFailure as error:
                    if failure is None:
                        failure = error
        if failure is not None:
            raise failure


def main() -> int:
    try:
        run_health(os.environ)
    except CheckFailure as error:
        print(f'SDKHARNESS_RESULT\t{LEVEL}\t{SCENARIO}\tFAIL\t{error.step}: {error.detail}')
        return 1
    except Exception as error:
        print(f'SDKHARNESS_RESULT\t{LEVEL}\t{SCENARIO}\tFAIL\tsetup: {type(error).__name__}')
        return 1
    print(f'SDKHARNESS_RESULT\t{LEVEL}\t{SCENARIO}\tPASS\t-')
    return 0


if __name__ == '__main__':
    sys.exit(main())
