#!/usr/bin/env python3
######################################################################
#
# File: .sdkharness/tests/conformance-file-lifecycle.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Repository-owned sdkharness conformance checks for basic file lifecycle operations."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from urllib.parse import urlsplit

LEVEL = 'conformance'
SCENARIOS = {
    'files.delete_version',
    'files.download_by_id',
    'files.download_content',
    'files.hide',
    'files.list',
    'files.metadata',
}
REPOSITORY_ROOT = Path(__file__).resolve().parents[2]

# Resolve the CLI implementation from this checkout. The harness installs the
# package first to supply declared dependencies and distribution metadata.
sys.path.insert(0, str(REPOSITORY_ROOT))
import b2  # noqa: E402
from b2._internal.version_listing import LATEST_STABLE_VERSION  # noqa: E402


class CheckFailure(Exception):
    """A failed scenario step with a credential-safe reason."""

    def __init__(self, step: str, cause: BaseException | str) -> None:
        self.step = step
        self.detail = cause if isinstance(cause, str) else type(cause).__name__
        super().__init__(f'{step}: {self.detail}')


CommandRunner = Callable[[str, list[str], dict[str, str]], str]


def validate_environment(environment: Mapping[str, str]) -> tuple[str, str, str, str, str]:
    if environment.get('SDKHARNESS_TEST_LEVEL') != LEVEL:
        raise CheckFailure('configuration', 'unexpected test level')
    scenario = environment.get('SDKHARNESS_SCENARIO', '')
    if scenario not in SCENARIOS:
        raise CheckFailure('configuration', 'unexpected scenario')

    simulator_url = environment.get('SDKHARNESS_SIMULATOR_URL', '')
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
    return scenario, simulator_url, key_id, application_key, bucket_name


def repository_cli_prefix() -> list[str]:
    module_path = Path(b2.__file__).resolve()
    try:
        module_path.relative_to(REPOSITORY_ROOT)
    except ValueError as error:
        raise CheckFailure('setup', 'b2 CLI was not imported from this checkout') from error
    return [sys.executable, '-m', f'b2._internal.{LATEST_STABLE_VERSION}']


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


def json_document(raw: str, step: str) -> object:
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError) as error:
        raise CheckFailure(step, 'CLI output is not JSON') from error


def versions_from(raw: str, step: str) -> list[dict[str, object]]:
    document = json_document(raw, step)
    if isinstance(document, list):
        versions = document
    elif isinstance(document, dict):
        versions = document.get('files', [])
    else:
        raise CheckFailure(step, 'CLI output has no file list')
    if not isinstance(versions, list) or any(not isinstance(item, dict) for item in versions):
        raise CheckFailure(step, 'CLI output has no file list')
    return versions


def sha1_bytes(payload: bytes) -> str:
    return hashlib.sha1(payload, usedforsecurity=False).hexdigest()


def metadata_sha1(item: Mapping[str, object]) -> str:
    return str(item.get('contentSha1', '')).removeprefix('unverified:')


def metadata_size(item: Mapping[str, object]) -> object:
    return item.get('size', item.get('contentLength'))


class Lifecycle:
    def __init__(
        self,
        environment: Mapping[str, str],
        *,
        cli_prefix: Sequence[str] | None,
        run_command: CommandRunner,
        object_prefix: str | None,
        scratch_root: Path | None,
    ) -> None:
        scenario, simulator_url, key_id, application_key, bucket_name = validate_environment(
            environment
        )
        self.environment = dict(environment)
        self.scenario = scenario
        self.simulator_url = simulator_url
        self.key_id = key_id
        self.application_key = application_key
        self.bucket_name = bucket_name
        self.prefix = list(cli_prefix) if cli_prefix is not None else repository_cli_prefix()
        self.run_command = run_command
        self.object_prefix = object_prefix or f'sdkharness-conformance/{uuid.uuid4().hex}'
        self.scratch_root = scratch_root

    def invoke(self, step: str, *arguments: str) -> str:
        return self.run_command(step, [*self.prefix, *arguments], self.child_environment)

    def object_name(self, leaf: str) -> str:
        return f'{self.object_prefix}/{leaf}'

    def upload(self, step: str, payload: bytes, name: str, *options: str) -> None:
        local_path = self.scratch / f'upload-{uuid.uuid4().hex}.bin'
        local_path.write_bytes(payload)
        self.invoke(
            step,
            'file',
            'upload',
            '--no-progress',
            *options,
            self.bucket_name,
            str(local_path),
            name,
        )

    def list_versions(self, name: str, step: str = 'list versions') -> list[dict[str, object]]:
        raw = self.invoke(
            step,
            'ls',
            '--json',
            '--recursive',
            '--versions',
            f'b2://{self.bucket_name}/{name}',
        )
        return [item for item in versions_from(raw, step) if item.get('fileName') == name]

    def download(self, step: str, remote: str) -> bytes:
        local_path = self.scratch / f'download-{uuid.uuid4().hex}.bin'
        self.invoke(step, 'file', 'download', '--no-progress', remote, str(local_path))
        if not local_path.is_file():
            raise CheckFailure(step, 'download wrote no local file')
        return local_path.read_bytes()

    def select_payload_version(
        self, versions: Sequence[Mapping[str, object]], payload: bytes, step: str
    ) -> Mapping[str, object]:
        expected_sha1 = sha1_bytes(payload)
        matches = [
            item
            for item in versions
            if metadata_size(item) == len(payload) and metadata_sha1(item) == expected_sha1
        ]
        if len(matches) != 1 or not matches[0].get('fileId'):
            raise CheckFailure(step, 'payload version was not listed exactly once')
        return matches[0]

    def files_list(self) -> None:
        payload = b'l' * 256
        second = b'L' * 300
        names = [self.object_name(f'list/{leaf}.txt') for leaf in ('a', 'b', 'c')]
        for name in names:
            self.upload(f'upload {Path(name).stem}', payload, name)
        self.upload('upload c again', second, names[-1])
        outsider = self.object_name('outside.txt')
        self.upload('upload outsider', payload, outsider)

        raw = self.invoke(
            'list names',
            'ls',
            '--json',
            '--recursive',
            f'b2://{self.bucket_name}/{self.object_name("list/")}',
        )
        listed = {str(item.get('fileName', '')) for item in versions_from(raw, 'list names')}
        if listed != set(names):
            raise CheckFailure('list names', 'prefix listing differs from uploaded names')

        versions = self.list_versions(names[-1])
        ids = {str(item.get('fileId', '')) for item in versions}
        if len(versions) != 2 or len(ids) != 2 or '' in ids:
            raise CheckFailure('list versions', 'twice-uploaded key did not return two versions')

    def files_metadata(self) -> None:
        payload = b'metadata-' * 57
        name = self.object_name('meta.txt')
        self.upload(
            'upload fixture',
            payload,
            name,
            '--content-type',
            'text/plain',
            '--info',
            'sdkharness=conformance',
        )
        version = self.select_payload_version(self.list_versions(name), payload, 'locate object')
        file_id = str(version['fileId'])
        for label, remote in (
            ('id', f'b2id://{file_id}'),
            ('name', f'b2://{self.bucket_name}/{name}'),
        ):
            document = json_document(
                self.invoke(f'read metadata by {label}', 'file', 'info', remote), label
            )
            if not isinstance(document, dict):
                raise CheckFailure(f'read metadata by {label}', 'CLI output is not an object')
            info = document.get('fileInfo', {})
            if not isinstance(info, dict):
                raise CheckFailure(f'read metadata by {label}', 'fileInfo is not an object')
            if (
                document.get('fileName') != name
                or metadata_size(document) != len(payload)
                or metadata_sha1(document) != sha1_bytes(payload)
                or document.get('contentType') != 'text/plain'
                or info.get('sdkharness') != 'conformance'
            ):
                raise CheckFailure(f'read metadata by {label}', 'metadata differs from upload')

    def files_download_content(self) -> None:
        payload = (b'download-content-' * 256)[:4096]
        name = self.object_name('download.txt')
        self.upload('upload fixture', payload, name)
        downloaded = self.download('download by name', f'b2://{self.bucket_name}/{name}')
        if downloaded != payload:
            raise CheckFailure('round trip', 'downloaded bytes differ from upload')

    def two_versions(self, leaf: str) -> tuple[str, str, bytes, bytes, str]:
        first = (b'version-one-' * 86)[:1024]
        second = (b'version-two-' * 171)[:2048]
        name = self.object_name(leaf)
        self.upload('upload v1', first, name)
        self.upload('upload v2', second, name)
        versions = self.list_versions(name)
        if len(versions) != 2:
            raise CheckFailure('list versions', 'two uploaded versions were not listed')
        first_version = self.select_payload_version(versions, first, 'list versions')
        second_version = self.select_payload_version(versions, second, 'list versions')
        first_id = str(first_version['fileId'])
        second_id = str(second_version['fileId'])
        if first_id == second_id:
            raise CheckFailure('list versions', 'versions share a fileId')
        return first_id, second_id, first, second, name

    def files_download_by_id(self) -> None:
        first_id, _second_id, first, second, _name = self.two_versions('twice.txt')
        downloaded = self.download('download by id', f'b2id://{first_id}')
        if downloaded != first or downloaded == second:
            raise CheckFailure('download by id', 'older fileId returned the wrong bytes')

    def files_hide(self) -> None:
        payload = b'h' * 1024
        name = self.object_name('hide.txt')
        self.upload('upload fixture', payload, name)
        original = self.select_payload_version(self.list_versions(name), payload, 'locate object')
        original_id = str(original['fileId'])
        self.invoke('hide', 'file', 'hide', f'b2://{self.bucket_name}/{name}')

        raw = self.invoke(
            'list names',
            'ls',
            '--json',
            '--recursive',
            f'b2://{self.bucket_name}/{self.object_prefix}/',
        )
        if any(item.get('fileName') == name for item in versions_from(raw, 'list names')):
            raise CheckFailure('hide', 'hidden object remains in names listing')

        versions = self.list_versions(name)
        hides = [item for item in versions if item.get('action') == 'hide']
        ids = {str(item.get('fileId', '')) for item in versions}
        if len(versions) != 2 or len(hides) != 1 or original_id not in ids:
            raise CheckFailure('hide', 'versions do not contain one hide marker and the original')
        hide_id = str(hides[0].get('fileId', ''))
        if not hide_id or hide_id == original_id:
            raise CheckFailure('hide', 'hide marker has no distinct fileId')
        if self.download('download original by id', f'b2id://{original_id}') != payload:
            raise CheckFailure('hide', 'hidden original no longer downloads by id')

    def files_delete_version(self) -> None:
        first_id, second_id, first, _second, name = self.two_versions('versions.txt')
        self.invoke(
            'delete newer version',
            'rm',
            '--no-progress',
            '--fail-fast',
            f'b2id://{second_id}',
        )
        versions = self.list_versions(name, 'list versions again')
        remaining_ids = {str(item.get('fileId', '')) for item in versions}
        if remaining_ids != {first_id}:
            raise CheckFailure('delete version', 'the wrong version remained after deletion')
        if self.download('download survivor', f'b2id://{first_id}') != first:
            raise CheckFailure('delete version', 'surviving version bytes changed')

    def run(self) -> None:
        with tempfile.TemporaryDirectory(dir=self.scratch_root) as scratch_name:
            self.scratch = Path(scratch_name)
            self.child_environment = dict(self.environment)
            self.child_environment.update(
                {
                    'B2_ENVIRONMENT': self.simulator_url,
                    'B2_APPLICATION_KEY_ID': self.key_id,
                    'B2_APPLICATION_KEY': self.application_key,
                    'B2_ACCOUNT_INFO': str(self.scratch / 'account-info'),
                    'XDG_CONFIG_HOME': str(self.scratch / 'xdg'),
                    'PYTHONPATH': os.pathsep.join(
                        filter(None, (str(REPOSITORY_ROOT), self.environment.get('PYTHONPATH', '')))
                    ),
                }
            )
            self.invoke('authenticate', 'account', 'authorize')
            failure: CheckFailure | None = None
            try:
                getattr(self, self.scenario.replace('.', '_'))()
            except CheckFailure as error:
                failure = error
            finally:
                try:
                    self.invoke(
                        'cleanup',
                        'rm',
                        '--versions',
                        '--recursive',
                        '--no-progress',
                        '--fail-fast',
                        f'b2://{self.bucket_name}/{self.object_prefix}/',
                    )
                except CheckFailure as error:
                    if failure is None:
                        failure = error
            if failure is not None:
                raise failure


def run_check(
    environment: Mapping[str, str],
    *,
    cli_prefix: Sequence[str] | None = None,
    run_command: CommandRunner = subprocess_runner,
    object_prefix: str | None = None,
    scratch_root: Path | None = None,
) -> None:
    Lifecycle(
        environment,
        cli_prefix=cli_prefix,
        run_command=run_command,
        object_prefix=object_prefix,
        scratch_root=scratch_root,
    ).run()


def main() -> int:
    scenario = os.environ.get('SDKHARNESS_SCENARIO', 'unknown')
    try:
        run_check(os.environ)
    except CheckFailure as error:
        print(f'SDKHARNESS_RESULT\t{LEVEL}\t{scenario}\tFAIL\t{error.step}: {error.detail}')
        return 1
    except Exception as error:
        print(f'SDKHARNESS_RESULT\t{LEVEL}\t{scenario}\tFAIL\tsetup: {type(error).__name__}')
        return 1
    print(f'SDKHARNESS_RESULT\t{LEVEL}\t{scenario}\tPASS\t-')
    return 0


if __name__ == '__main__':
    sys.exit(main())
