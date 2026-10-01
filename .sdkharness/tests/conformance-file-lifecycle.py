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
"""Repository-owned sdkharness conformance checks for files and buckets."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from urllib.request import urlopen

LEVEL = 'conformance'
SCENARIOS = {
    'bucket.cors',
    'bucket.crud',
    'bucket.lifecycle',
    'bucket.notification_rules',
    'bucket.replication_config',
    'bucket.replication_helper',
    'client.auth_persistence',
    'client.progress',
    'client.sync',
    'enc.sse_b2',
    'enc.sse_c',
    'lock.bucket_default',
    'lock.bypass_governance',
    'lock.legal_hold',
    'lock.per_file_retention',
    'keys.crud',
    'keys.multi_bucket',
    'files.delete_version',
    'files.download_by_id',
    'files.download_content',
    'files.hide',
    'files.list',
    'files.metadata',
    'files.server_side_copy',
    'large.concurrent_parts',
    'large.multipart',
    'large.parallel_download',
    'large.unbound_incremental',
    'urls.native_download',
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

    def bucket_listing(self, step: str = 'list buckets') -> list[dict[str, object]]:
        document = json_document(self.invoke(step, 'bucket', 'list', '--json'), step)
        buckets = document.get('buckets', []) if isinstance(document, dict) else document
        if not isinstance(buckets, list) or any(not isinstance(item, dict) for item in buckets):
            raise CheckFailure(step, 'CLI output has no bucket list')
        return buckets

    def bucket_named(self, name: str, step: str = 'list buckets') -> dict[str, object] | None:
        matches = [item for item in self.bucket_listing(step) if item.get('bucketName') == name]
        if len(matches) > 1:
            raise CheckFailure(step, 'bucket was listed more than once')
        return matches[0] if matches else None

    def new_bucket(self, step: str = 'create bucket', *options: str) -> str:
        name = f'sdkharness-conf-{uuid.uuid4().hex[:16]}'
        self.created_buckets.append(name)
        self.invoke(step, 'bucket', 'create', *options, name, 'allPrivate')
        return name

    def invoke_expect_failure(self, step: str, *arguments: str) -> None:
        completed = self.invoke_process(step, *arguments)
        if completed.returncode == 0:
            raise CheckFailure(step, 'command unexpectedly succeeded')

    def invoke_process(
        self,
        step: str,
        *arguments: str,
        environment: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> subprocess.CompletedProcess[str]:
        try:
            completed = subprocess.run(
                [*self.prefix, *arguments],
                cwd=REPOSITORY_ROOT,
                env=dict(environment) if environment is not None else self.child_environment,
                stdin=subprocess.DEVNULL,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout,
            )
        except (OSError, subprocess.TimeoutExpired) as error:
            raise CheckFailure(step, error) from error
        return completed

    def delete_bucket(self, name: str, step: str = 'delete bucket') -> None:
        self.invoke(step, 'bucket', 'delete', name)
        self.created_buckets.remove(name)

    @staticmethod
    def replication_value(bucket: Mapping[str, object]) -> dict[str, object]:
        value = bucket.get('replication') or {}
        if isinstance(value, dict) and isinstance(value.get('value'), dict):
            value = value['value']
        return value if isinstance(value, dict) else {}

    def object_name(self, leaf: str) -> str:
        return f'{self.object_prefix}/{leaf}'

    def upload(self, step: str, payload: bytes, name: str, *options: str) -> None:
        self.upload_to_bucket(step, self.bucket_name, payload, name, *options)

    def upload_to_bucket(
        self, step: str, bucket_name: str, payload: bytes, name: str, *options: str
    ) -> None:
        local_path = self.scratch / f'upload-{uuid.uuid4().hex}.bin'
        local_path.write_bytes(payload)
        self.invoke(
            step,
            'file',
            'upload',
            '--no-progress',
            *options,
            bucket_name,
            str(local_path),
            name,
        )

    def list_versions(self, name: str, step: str = 'list versions') -> list[dict[str, object]]:
        return self.list_versions_for_bucket(self.bucket_name, name, step)

    def list_versions_for_bucket(
        self, bucket_name: str, name: str, step: str = 'list versions'
    ) -> list[dict[str, object]]:
        raw = self.invoke(
            step,
            'ls',
            '--json',
            '--recursive',
            '--versions',
            f'b2://{bucket_name}/{name}',
        )
        return [item for item in versions_from(raw, step) if item.get('fileName') == name]

    def download(self, step: str, remote: str) -> bytes:
        local_path = self.scratch / f'download-{uuid.uuid4().hex}.bin'
        self.invoke(step, 'file', 'download', '--no-progress', remote, str(local_path))
        if not local_path.is_file():
            raise CheckFailure(step, 'download wrote no local file')
        return local_path.read_bytes()

    def download_from_bucket(self, step: str, bucket_name: str, name: str, *options: str) -> bytes:
        local_path = self.scratch / f'download-{uuid.uuid4().hex}.bin'
        self.invoke(
            step,
            'file',
            'download',
            '--no-progress',
            *options,
            f'b2://{bucket_name}/{name}',
            str(local_path),
        )
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

    @staticmethod
    def retention_value(version: Mapping[str, object]) -> dict[str, object]:
        retention = version.get('fileRetention') or {}
        if isinstance(retention, dict) and isinstance(retention.get('value'), dict):
            retention = retention['value']
        return retention if isinstance(retention, dict) else {}

    @staticmethod
    def legal_hold_value(version: Mapping[str, object]) -> object:
        hold = version.get('legalHold')
        if isinstance(hold, dict):
            hold = hold.get('value')
        return hold

    def assert_version_absent(self, bucket_name: str, name: str, file_id: str, step: str) -> None:
        versions = self.list_versions_for_bucket(bucket_name, name, step)
        if any(str(item.get('fileId', '')) == file_id for item in versions):
            raise CheckFailure(step, 'deleted version is still listed')

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
        newest = max(versions, key=lambda item: item.get('uploadTimestamp') or 0)
        if newest.get('action') != 'hide':
            raise CheckFailure(
                'hide', f"the newest version reports action {newest.get('action')!r}, expected hide"
            )
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

    def bucket_crud(self) -> None:
        name = self.new_bucket()
        created = self.bucket_named(name, 'read new bucket')
        if created is None or created.get('bucketType') != 'allPrivate':
            raise CheckFailure('create bucket', 'new bucket did not report allPrivate')

        self.invoke('update bucket type', 'bucket', 'update', name, 'allPublic')
        updated = self.bucket_named(name, 'read updated bucket')
        if updated is None or updated.get('bucketType') != 'allPublic':
            raise CheckFailure('update bucket', 'updated bucket did not report allPublic')

        self.delete_bucket(name)
        if self.bucket_named(name, 'read deleted bucket') is not None:
            raise CheckFailure('delete bucket', 'deleted bucket remains in listing')

    def bucket_cors(self) -> None:
        name = self.new_bucket()
        rules = [
            {
                'corsRuleName': 'sdkharnessconf',
                'allowedOrigins': ['https://example.com'],
                'allowedOperations': [
                    'b2_download_file_by_id',
                    'b2_download_file_by_name',
                ],
                'allowedHeaders': ['range'],
                'exposeHeaders': ['x-bz-content-sha1'],
                'maxAgeSeconds': 3600,
            }
        ]
        self.invoke('set CORS rules', 'bucket', 'update', name, '--cors-rules', json.dumps(rules))
        bucket = self.bucket_named(name, 'read CORS rules')
        got = bucket.get('corsRules', []) if bucket else []
        if not isinstance(got, list) or len(got) != 1:
            raise CheckFailure('CORS rules', 'expected one rule after update')
        for key, value in rules[0].items():
            if not isinstance(got[0], dict) or got[0].get(key) != value:
                raise CheckFailure('CORS rules', f'{key} did not round-trip')

    def bucket_lifecycle(self) -> None:
        name = self.new_bucket()
        rule = {
            'daysFromHidingToDeleting': 1,
            'daysFromUploadingToHiding': None,
            'fileNamePrefix': 'st/',
        }
        self.invoke(
            'set lifecycle rule',
            'bucket',
            'update',
            name,
            '--lifecycle-rule',
            json.dumps(rule),
        )
        bucket = self.bucket_named(name, 'read lifecycle rule')
        got = bucket.get('lifecycleRules', []) if bucket else []
        if not isinstance(got, list) or len(got) != 1:
            raise CheckFailure('lifecycle rule', 'expected one rule after update')
        for key, value in rule.items():
            if not isinstance(got[0], dict) or got[0].get(key) != value:
                raise CheckFailure('lifecycle rule', f'{key} did not round-trip')

    def bucket_notification_rules(self) -> None:
        name = self.new_bucket()
        rule_name = f'sdkharness-conf-{uuid.uuid4().hex[:8]}'
        webhook_url = 'https://example.com/sdkharness-conformance'
        signing_secret = uuid.uuid4().hex
        self.invoke(
            'create notification rule',
            'bucket',
            'notification-rule',
            'create',
            '--json',
            '--event-type',
            'b2:ObjectCreated:*',
            '--webhook-url',
            webhook_url,
            '--sign-secret',
            signing_secret,
            f'b2://{name}',
            rule_name,
        )
        document = json_document(
            self.invoke(
                'list notification rules',
                'bucket',
                'notification-rule',
                'list',
                '--json',
                f'b2://{name}',
            ),
            'list notification rules',
        )
        rules = document.get('rules', []) if isinstance(document, dict) else document
        matches = [
            rule for rule in rules if isinstance(rule, dict) and rule.get('name') == rule_name
        ]
        if len(matches) != 1:
            raise CheckFailure('notification rule', 'created rule was not listed exactly once')
        rule = matches[0]
        target = rule.get('targetConfiguration') or {}
        if (
            'b2:ObjectCreated:*' not in (rule.get('eventTypes') or [])
            or not isinstance(target, dict)
            or (target.get('url') or target.get('webhookUrl')) != webhook_url
            or target.get('hmacSha256SigningSecret') != signing_secret
        ):
            raise CheckFailure('notification rule', 'created fields did not round-trip')
        self.invoke(
            'delete notification rule',
            'bucket',
            'notification-rule',
            'delete',
            f'b2://{name}',
            rule_name,
        )
        after = json_document(
            self.invoke(
                'confirm notification deletion',
                'bucket',
                'notification-rule',
                'list',
                '--json',
                f'b2://{name}',
            ),
            'confirm notification deletion',
        )
        remaining = after.get('rules', []) if isinstance(after, dict) else after
        if any(isinstance(rule, dict) and rule.get('name') == rule_name for rule in remaining):
            raise CheckFailure('notification rule', 'deleted rule remains listed')

    def bucket_replication_config(self) -> None:
        source_name = self.new_bucket('create source bucket')
        destination_name = self.new_bucket('create destination bucket')
        destination = self.bucket_named(destination_name, 'read destination bucket')
        destination_id = destination.get('bucketId') if destination else None
        if not isinstance(destination_id, str) or not destination_id:
            raise CheckFailure('replication setup', 'destination has no bucketId')

        key_name = f'sdkharness-conf-{uuid.uuid4().hex[:12]}'
        key_output = self.invoke(
            'create replication key',
            'key',
            'create',
            key_name,
            'listBuckets,listFiles,readFiles,writeFiles,readFileLegalHolds,readFileRetentions',
        )
        source_key_id = key_output.split()[0] if key_output.split() else ''
        if not source_key_id:
            raise CheckFailure('replication setup', 'key creation returned no key id')
        self.created_keys.append(source_key_id)
        rule_name = f'sdkharness-conf-{uuid.uuid4().hex[:8]}'
        mapping = {
            'asReplicationDestination': {
                'sourceToDestinationKeyMapping': {source_key_id: source_key_id}
            }
        }
        replication = {
            'asReplicationSource': {
                'replicationRules': [
                    {
                        'destinationBucketId': destination_id,
                        'fileNamePrefix': 'st/',
                        'includeExistingFiles': False,
                        'isEnabled': True,
                        'priority': 128,
                        'replicationRuleName': rule_name,
                    }
                ],
                'sourceApplicationKeyId': source_key_id,
            }
        }
        self.invoke(
            'set destination key mapping',
            'bucket',
            'update',
            destination_name,
            '--replication',
            json.dumps(mapping),
        )
        self.invoke(
            'set replication source',
            'bucket',
            'update',
            source_name,
            '--replication',
            json.dumps(replication),
        )
        source = self.bucket_named(source_name, 'read replication source')
        value = self.replication_value(source or {})
        source_side = value.get('asReplicationSource') or {}
        if not isinstance(source_side, dict):
            raise CheckFailure('replication config', 'source configuration is not an object')
        rules = source_side.get('replicationRules', []) if isinstance(source_side, dict) else []
        matches = [
            item
            for item in rules
            if isinstance(item, dict) and item.get('replicationRuleName') == rule_name
        ]
        expected_rule = replication['asReplicationSource']['replicationRules'][0]
        if len(matches) != 1 or any(matches[0].get(k) != v for k, v in expected_rule.items()):
            raise CheckFailure('replication config', 'replication rule did not round-trip')
        if source_side.get('sourceApplicationKeyId') != source_key_id:
            raise CheckFailure('replication config', 'source key id did not round-trip')

    def bucket_replication_helper(self) -> None:
        source_name = self.new_bucket('create source bucket')
        destination_name = self.new_bucket('create destination bucket')
        rule_name = f'sdkharness-conf-{uuid.uuid4().hex[:8]}'
        self.invoke(
            'run replication setup helper',
            'replication',
            'setup',
            '--name',
            rule_name,
            '--file-name-prefix',
            'st/',
            source_name,
            destination_name,
        )
        key_output = self.invoke('list helper keys', 'key', 'list')
        for line in key_output.splitlines():
            fields = line.split()
            if len(fields) >= 2 and (source_name in fields[1] or destination_name in fields[1]):
                self.created_keys.append(fields[0])

        source = self.bucket_named(source_name, 'read source bucket')
        destination = self.bucket_named(destination_name, 'read destination bucket')
        source_side = self.replication_value(source or {}).get('asReplicationSource') or {}
        destination_side = (
            self.replication_value(destination or {}).get('asReplicationDestination') or {}
        )
        if (
            not isinstance(source_side, dict)
            or not source_side.get('replicationRules')
            or not source_side.get('sourceApplicationKeyId')
            or not isinstance(destination_side, dict)
            or not destination_side.get('sourceToDestinationKeyMapping')
        ):
            raise CheckFailure('replication helper', 'helper did not configure both buckets')

        payload = b'replication-' * 43
        local_path = self.scratch / 'replication-payload.bin'
        local_path.write_bytes(payload)
        self.invoke(
            'upload replication fixture',
            'file',
            'upload',
            '--no-progress',
            source_name,
            str(local_path),
            'st/replicate.txt',
        )
        status = json_document(
            self.invoke(
                'read replication status',
                'replication',
                'status',
                source_name,
                '--output-format',
                'json',
                '--no-progress',
                '--dont-scan-destination',
            ),
            'read replication status',
        )
        rows: list[object] = []
        if isinstance(status, list):
            rows = status
        elif isinstance(status, dict):
            for value in status.values():
                if isinstance(value, list):
                    rows.extend(value)
        count = sum(
            item.get('count', 0)
            for item in rows
            if isinstance(item, dict) and isinstance(item.get('count'), int)
        )
        if count <= 0:
            raise CheckFailure('replication status', 'uploaded file was not counted')

    def enc_sse_b2(self) -> None:
        bucket_name = self.new_bucket()
        payload = (b'sse-b2-' * 256)[:2048]
        name = 'st/sse-b2.bin'
        self.upload_to_bucket(
            'upload with SSE-B2',
            bucket_name,
            payload,
            name,
            '--destination-server-side-encryption',
            'SSE-B2',
            '--destination-server-side-encryption-algorithm',
            'AES256',
        )
        versions = self.list_versions_for_bucket(bucket_name, name, 'read SSE-B2 metadata')
        version = self.select_payload_version(versions, payload, 'read SSE-B2 metadata')
        encryption = version.get('serverSideEncryption') or {}
        if (
            not isinstance(encryption, dict)
            or encryption.get('mode') != 'SSE-B2'
            or encryption.get('algorithm') != 'AES256'
        ):
            raise CheckFailure('SSE-B2', 'encryption metadata did not round-trip')
        downloaded = self.download_from_bucket('download SSE-B2 object', bucket_name, name)
        if downloaded != payload:
            raise CheckFailure('SSE-B2', 'downloaded plaintext differs from upload')

    def enc_sse_c(self) -> None:
        bucket_name = self.new_bucket()
        payload = (b'sse-c-' * 300)[:2048]
        name = 'st/sse-c.bin'
        key = os.urandom(32)
        encoded_key = base64.b64encode(key).decode()
        key_id = 'sdkharness-conf-ssec'
        self.child_environment['B2_DESTINATION_SSE_C_KEY_B64'] = encoded_key
        self.child_environment['B2_DESTINATION_SSE_C_KEY_ID'] = key_id
        try:
            self.upload_to_bucket(
                'upload with SSE-C',
                bucket_name,
                payload,
                name,
                '--destination-server-side-encryption',
                'SSE-C',
                '--destination-server-side-encryption-algorithm',
                'AES256',
            )
        finally:
            self.child_environment.pop('B2_DESTINATION_SSE_C_KEY_B64', None)
            self.child_environment.pop('B2_DESTINATION_SSE_C_KEY_ID', None)

        versions = self.list_versions_for_bucket(bucket_name, name, 'read SSE-C metadata')
        version = self.select_payload_version(versions, payload, 'read SSE-C metadata')
        encryption = version.get('serverSideEncryption') or {}
        info = version.get('fileInfo') or {}
        if (
            not isinstance(encryption, dict)
            or encryption.get('mode') != 'SSE-C'
            or not isinstance(info, dict)
            or info.get('sse_c_key_id') != key_id
        ):
            raise CheckFailure('SSE-C', 'encryption metadata did not round-trip')

        self.child_environment['B2_SOURCE_SSE_C_KEY_B64'] = encoded_key
        try:
            downloaded = self.download_from_bucket(
                'download SSE-C object',
                bucket_name,
                name,
                '--source-server-side-encryption',
                'SSE-C',
                '--source-server-side-encryption-algorithm',
                'AES256',
            )
        finally:
            self.child_environment.pop('B2_SOURCE_SSE_C_KEY_B64', None)
        if downloaded != payload:
            raise CheckFailure('SSE-C', 'downloaded plaintext differs from upload')
        self.invoke_expect_failure(
            'download SSE-C object without key',
            'file',
            'download',
            '--no-progress',
            f'b2://{bucket_name}/{name}',
            str(self.scratch / 'should-not-exist'),
        )

    def lock_bucket_default(self) -> None:
        bucket_name = self.new_bucket('create lock-enabled bucket', '--file-lock-enabled')
        self.invoke(
            'set bucket default retention',
            'bucket',
            'update',
            bucket_name,
            '--default-retention-mode',
            'governance',
            '--default-retention-period',
            '1 days',
        )
        bucket = self.bucket_named(bucket_name, 'read bucket default retention')
        if bucket is None or bucket.get('isFileLockEnabled') is not True:
            raise CheckFailure('bucket default retention', 'file lock is not enabled')
        default = bucket.get('defaultRetention') or {}
        if isinstance(default, dict) and isinstance(default.get('value'), dict):
            default = default['value']
        if not isinstance(default, dict):
            raise CheckFailure('bucket default retention', 'default retention is missing')
        period = default.get('period') or {}
        if (
            default.get('mode') != 'governance'
            or not isinstance(period, dict)
            or period.get('duration') != 1
            or period.get('unit') != 'days'
        ):
            raise CheckFailure('bucket default retention', 'default retention did not round-trip')

        payload = b'lock-default-' * 40
        name = 'st/inherits.txt'
        uploaded_at = int(time.time() * 1000)
        self.upload_to_bucket('upload object', bucket_name, payload, name)
        version = self.select_payload_version(
            self.list_versions_for_bucket(bucket_name, name, 'read inherited retention'),
            payload,
            'read inherited retention',
        )
        retention = self.retention_value(version)
        retain_until = retention.get('retainUntilTimestamp')
        expected = uploaded_at + 24 * 60 * 60 * 1000
        if (
            retention.get('mode') != 'governance'
            or not isinstance(retain_until, int)
            or abs(retain_until - expected) > 12 * 60 * 60 * 1000
        ):
            raise CheckFailure('inherited retention', 'object did not inherit the one-day default')

    def lock_bypass_governance(self) -> None:
        bucket_name = self.new_bucket('create lock-enabled bucket', '--file-lock-enabled')
        payload = b'bypass-' * 73
        name = 'st/bypass.txt'
        self.upload_to_bucket('upload fixture', bucket_name, payload, name)
        retain_until = int(time.time() * 1000) + 2 * 24 * 60 * 60 * 1000
        self.invoke(
            'set governance retention',
            'file',
            'update',
            f'b2://{bucket_name}/{name}',
            '--file-retention-mode',
            'governance',
            '--retain-until',
            str(retain_until),
        )
        version = self.select_payload_version(
            self.list_versions_for_bucket(bucket_name, name, 'read governance retention'),
            payload,
            'read governance retention',
        )
        if self.retention_value(version).get('mode') != 'governance':
            raise CheckFailure('governance retention', 'retention did not round-trip')
        file_id = str(version['fileId'])
        self.invoke_expect_failure(
            'delete retained version without bypass',
            'rm',
            '--no-progress',
            '--fail-fast',
            f'b2id://{file_id}',
        )
        self.invoke(
            'delete retained version with bypass',
            'rm',
            '--no-progress',
            '--fail-fast',
            '--bypass-governance',
            f'b2id://{file_id}',
        )
        self.assert_version_absent(bucket_name, name, file_id, 'confirm bypassed delete')

    def lock_legal_hold(self) -> None:
        bucket_name = self.new_bucket('create lock-enabled bucket', '--file-lock-enabled')
        payload = b'legal-hold-' * 47
        name = 'st/held.txt'
        self.upload_to_bucket('upload fixture', bucket_name, payload, name)
        version = self.select_payload_version(
            self.list_versions_for_bucket(bucket_name, name, 'locate fixture'),
            payload,
            'locate fixture',
        )
        file_id = str(version['fileId'])
        self.invoke(
            'set legal hold', 'file', 'update', f'b2://{bucket_name}/{name}', '--legal-hold', 'on'
        )
        held = self.select_payload_version(
            self.list_versions_for_bucket(bucket_name, name, 'read legal hold'),
            payload,
            'read legal hold',
        )
        if self.legal_hold_value(held) != 'on':
            raise CheckFailure('legal hold', 'enabled hold did not round-trip')
        self.invoke_expect_failure(
            'delete version under legal hold',
            'rm',
            '--no-progress',
            '--fail-fast',
            f'b2id://{file_id}',
        )
        self.invoke(
            'clear legal hold',
            'file',
            'update',
            f'b2://{bucket_name}/{name}',
            '--legal-hold',
            'off',
        )
        cleared = self.select_payload_version(
            self.list_versions_for_bucket(bucket_name, name, 'read cleared legal hold'),
            payload,
            'read cleared legal hold',
        )
        if self.legal_hold_value(cleared) != 'off':
            raise CheckFailure('legal hold', 'cleared hold did not round-trip')
        self.invoke(
            'delete unheld version', 'rm', '--no-progress', '--fail-fast', f'b2id://{file_id}'
        )
        self.assert_version_absent(bucket_name, name, file_id, 'confirm unheld delete')

    def lock_per_file_retention(self) -> None:
        bucket_name = self.new_bucket('create lock-enabled bucket', '--file-lock-enabled')
        payload = b'per-file-' * 64
        name = 'st/retained.txt'
        self.upload_to_bucket('upload fixture', bucket_name, payload, name)
        retain_until = int(time.time() * 1000) + 2 * 24 * 60 * 60 * 1000
        self.invoke(
            'set per-file retention',
            'file',
            'update',
            f'b2://{bucket_name}/{name}',
            '--file-retention-mode',
            'governance',
            '--retain-until',
            str(retain_until),
        )
        version = self.select_payload_version(
            self.list_versions_for_bucket(bucket_name, name, 'read per-file retention'),
            payload,
            'read per-file retention',
        )
        retention = self.retention_value(version)
        if (
            retention.get('mode') != 'governance'
            or retention.get('retainUntilTimestamp') != retain_until
        ):
            raise CheckFailure('per-file retention', 'retention did not round-trip')
        file_id = str(version['fileId'])
        self.invoke_expect_failure(
            'delete retained version without bypass',
            'rm',
            '--no-progress',
            '--fail-fast',
            f'b2id://{file_id}',
        )

    def client_auth_persistence(self) -> None:
        empty_environment = dict(self.child_environment)
        empty_environment.pop('B2_APPLICATION_KEY_ID', None)
        empty_environment.pop('B2_APPLICATION_KEY', None)
        empty_environment['B2_ACCOUNT_INFO'] = str(self.scratch / 'empty-account-info')
        control = self.invoke_process(
            'uncached process without credentials',
            'bucket',
            'list',
            environment=empty_environment,
        )
        if control.returncode == 0:
            raise CheckFailure('auth persistence control', 'uncached process was authorized')

        persisted_environment = dict(empty_environment)
        persisted_environment['B2_ACCOUNT_INFO'] = self.child_environment['B2_ACCOUNT_INFO']
        persisted = self.invoke_process(
            'authorized call in second process',
            'bucket',
            'list',
            environment=persisted_environment,
        )
        if persisted.returncode != 0:
            raise CheckFailure('auth persistence', 'cached authorization was not reused')
        if 'Using ' in persisted.stderr:
            raise CheckFailure('auth persistence', 'second process re-authorized')

    def client_progress(self) -> None:
        bucket_name = self.new_bucket()
        size = 16 * 1024 * 1024
        payload = (b'progress-sdkharness-' * (size // 20 + 1))[:size]
        local_path = self.scratch / 'progress.bin'
        local_path.write_bytes(payload)
        completed = self.invoke_process(
            'upload with progress reporting',
            'file',
            'upload',
            '--min-part-size',
            '5000000',
            bucket_name,
            str(local_path),
            'st/progress.bin',
        )
        if completed.returncode != 0:
            raise CheckFailure('upload with progress reporting', 'command failed')
        output = (completed.stdout + '\n' + completed.stderr).replace('\r', '\n')
        pairs = re.findall(
            r'\|\s*([0-9]+(?:\.[0-9]+)?)\s*([kKMGT]?)B?/\s*'
            r'([0-9]+(?:\.[0-9]+)?)\s*([kKMGT]?)B?\s*\[',
            output,
        )
        units = {'': 1, 'k': 1e3, 'K': 1e3, 'M': 1e6, 'G': 1e9, 'T': 1e12}
        if pairs:
            samples = [
                (float(done) * units[done_unit], float(total) * units[total_unit])
                for done, done_unit, total, total_unit in pairs
            ]
            done_values = [sample[0] for sample in samples]
            if len(samples) < 2 or any(
                done_values[index] > done_values[index + 1] + 1
                for index in range(len(done_values) - 1)
            ):
                raise CheckFailure('progress', 'byte progress was missing or went backwards')
            if abs(samples[-1][0] - size) > max(size * 0.01, 1024):
                raise CheckFailure('progress', 'final byte progress did not reach the payload size')
        else:
            percentages = [
                int(value) for value in re.findall(r'^\s*([0-9]{1,3})%\s*$', output, re.M)
            ]
            if not percentages or any(
                percentages[index] > percentages[index + 1] for index in range(len(percentages) - 1)
            ):
                raise CheckFailure('progress', 'upload reported no monotonic progress')
        versions = self.list_versions_for_bucket(
            bucket_name, 'st/progress.bin', 'read progress upload'
        )
        matches = [item for item in versions if metadata_size(item) == size and item.get('fileId')]
        if len(matches) != 1:
            raise CheckFailure('progress', 'uploaded size differs from progress payload')

    def client_sync(self) -> None:
        bucket_name = self.new_bucket()
        source = self.scratch / 'sync-source'
        destination = self.scratch / 'sync-destination'
        source.mkdir()
        destination.mkdir()
        initial = {
            'a.txt': b'a' * 512,
            'b.txt': b'b' * 512,
            'c.txt': b'c' * 512,
        }
        for name, payload in initial.items():
            (source / name).write_bytes(payload)
        self.invoke('sync up', 'sync', '--no-progress', str(source), f'b2://{bucket_name}/st/sync')
        listed = self.invoke(
            'list first sync', 'ls', '--json', '--recursive', f'b2://{bucket_name}/st/sync/'
        )
        first_names = {item.get('fileName') for item in versions_from(listed, 'list first sync')}
        expected_first = {f'st/sync/{name}' for name in initial}
        if first_names != expected_first:
            raise CheckFailure('sync up', 'remote names differ from local source')

        changed = b'A' * 1024
        (source / 'a.txt').write_bytes(changed)
        (source / 'b.txt').unlink()
        self.invoke(
            'sync up with delete',
            'sync',
            '--no-progress',
            '--delete',
            str(source),
            f'b2://{bucket_name}/st/sync',
        )
        listed = self.invoke(
            'list second sync', 'ls', '--json', '--recursive', f'b2://{bucket_name}/st/sync/'
        )
        second = versions_from(listed, 'list second sync')
        second_names = {item.get('fileName') for item in second}
        if second_names != {'st/sync/a.txt', 'st/sync/c.txt'}:
            raise CheckFailure('sync delete', 'remote names do not reflect deletion')
        a_version = next(item for item in second if item.get('fileName') == 'st/sync/a.txt')
        if metadata_size(a_version) != len(changed) or metadata_sha1(a_version) != sha1_bytes(
            changed
        ):
            raise CheckFailure('sync update', 'modified file metadata did not round-trip')

        self.invoke(
            'sync down', 'sync', '--no-progress', f'b2://{bucket_name}/st/sync', str(destination)
        )
        if (destination / 'a.txt').read_bytes() != changed:
            raise CheckFailure('sync down', 'modified file bytes differ')
        if (destination / 'c.txt').read_bytes() != initial['c.txt']:
            raise CheckFailure('sync down', 'unchanged file bytes differ')
        if (destination / 'b.txt').exists():
            raise CheckFailure('sync down', 'deleted file returned')

    def keys_crud(self) -> None:
        bucket_name = self.new_bucket()
        key_name = f'sdkharness-conf-{uuid.uuid4().hex[:16]}'
        raw = self.invoke(
            'create key',
            'key',
            'create',
            '--bucket',
            bucket_name,
            key_name,
            'listBuckets,listFiles,readFiles',
        )
        fields = raw.strip().split()
        if len(fields) != 2:
            raise CheckFailure('create key', 'expected key id and secret')
        key_id, secret = fields
        self.created_keys.append(key_id)
        listing = self.invoke('list keys', 'key', 'list', '-l')
        if key_id not in listing or key_name not in listing or secret in listing:
            raise CheckFailure('list keys', 'key metadata or secret exposure is wrong')
        self.invoke('delete key', 'key', 'delete', key_id)
        self.created_keys.remove(key_id)
        if key_id in self.invoke('list keys after delete', 'key', 'list', '-l'):
            raise CheckFailure('delete key', 'deleted key is still listed')

    def keys_multi_bucket(self) -> None:
        buckets = [self.new_bucket() for _ in range(3)]
        key_name = f'sdkharness-conf-{uuid.uuid4().hex[:16]}'
        raw = self.invoke(
            'create multi-bucket key',
            'key',
            'create',
            '--bucket',
            buckets[0],
            '--bucket',
            buckets[1],
            key_name,
            'listBuckets,listFiles,readFiles',
        )
        fields = raw.strip().split()
        if len(fields) != 2:
            raise CheckFailure('create multi-bucket key', 'expected key id and secret')
        key_id, secret = fields
        self.created_keys.append(key_id)
        listing = self.invoke('read multi-bucket key', 'key', 'list', '-l')
        row = next((line for line in listing.splitlines() if key_id in line), '')
        if not row or buckets[0] not in row or buckets[1] not in row:
            raise CheckFailure('multi-bucket key', 'bucket restrictions did not round-trip')

        restricted = dict(self.child_environment)
        restricted.update(
            {
                'B2_ACCOUNT_INFO': str(self.scratch / 'restricted-account-info'),
                'B2_APPLICATION_KEY_ID': key_id,
                'B2_APPLICATION_KEY': secret,
            }
        )
        authorized = self.invoke_process(
            'authorize restricted key', 'account', 'authorize', environment=restricted
        )
        if authorized.returncode != 0:
            raise CheckFailure('multi-bucket key', 'restricted key did not authorize')
        restricted.pop('B2_APPLICATION_KEY_ID', None)
        restricted.pop('B2_APPLICATION_KEY', None)
        for bucket_name in buckets[:2]:
            listed = self.invoke_process(
                'list allowed bucket',
                'ls',
                '--json',
                '--recursive',
                f'b2://{bucket_name}',
                environment=restricted,
            )
            if listed.returncode != 0:
                raise CheckFailure('multi-bucket key', 'allowed bucket was refused')
        denied = self.invoke_process(
            'list denied bucket',
            'ls',
            '--json',
            '--recursive',
            f'b2://{buckets[2]}',
            environment=restricted,
        )
        if denied.returncode == 0:
            raise CheckFailure('multi-bucket key', 'unscoped bucket was accessible')

    @staticmethod
    def large_payload() -> bytes:
        size = 16 * 1024 * 1024
        return (b'large-sdkharness-' * (size // 17 + 1))[:size]

    def assert_large_version(
        self, bucket_name: str, name: str, payload: bytes, step: str
    ) -> Mapping[str, object]:
        versions = self.list_versions_for_bucket(bucket_name, name, step)
        matches = [
            item for item in versions if metadata_size(item) == len(payload) and item.get('fileId')
        ]
        if len(matches) != 1:
            raise CheckFailure(step, 'large version was not listed exactly once')
        version = matches[0]
        info = version.get('fileInfo') or {}
        if (
            metadata_sha1(version) != 'none'
            or not isinstance(info, dict)
            or info.get('large_file_sha1') != sha1_bytes(payload)
        ):
            raise CheckFailure(step, 'multipart digest metadata did not round-trip')
        return version

    def upload_large(
        self, bucket_name: str, name: str, payload: bytes, threads: int, step: str
    ) -> None:
        path = self.scratch / f'large-{uuid.uuid4().hex}.bin'
        path.write_bytes(payload)
        self.invoke(
            step,
            'file',
            'upload',
            '--no-progress',
            '--min-part-size',
            '5000000',
            '--threads',
            str(threads),
            bucket_name,
            str(path),
            name,
        )

    def files_server_side_copy(self) -> None:
        bucket_name = self.new_bucket()
        payload = b'copy-' * 103
        source_name = 'st/copy-src.bin'
        destination_name = 'st/copy-dst.bin'
        self.upload_to_bucket('upload copy source', bucket_name, payload, source_name)
        source = self.select_payload_version(
            self.list_versions_for_bucket(bucket_name, source_name, 'locate copy source'),
            payload,
            'locate copy source',
        )
        self.invoke(
            'server-side copy',
            'file',
            'server-side-copy',
            f'b2://{bucket_name}/{source_name}',
            f'b2://{bucket_name}/{destination_name}',
        )
        destination = self.select_payload_version(
            self.list_versions_for_bucket(bucket_name, destination_name, 'locate copied file'),
            payload,
            'locate copied file',
        )
        if destination['fileId'] == source['fileId']:
            raise CheckFailure('server-side copy', 'destination reused source file id')
        downloaded = self.download_from_bucket(
            'download copied file', bucket_name, destination_name
        )
        if downloaded != payload:
            raise CheckFailure('server-side copy', 'copied bytes differ from source')

    def large_multipart(self) -> None:
        bucket_name = self.new_bucket()
        payload = self.large_payload()
        name = 'st/multipart.bin'
        self.upload_large(bucket_name, name, payload, 4, 'multipart upload')
        self.assert_large_version(bucket_name, name, payload, 'read multipart metadata')
        if self.download_from_bucket('download multipart file', bucket_name, name) != payload:
            raise CheckFailure('multipart round trip', 'downloaded bytes differ from upload')

    def large_concurrent_parts(self) -> None:
        bucket_name = self.new_bucket()
        payload = self.large_payload()
        for threads, name in ((1, 'st/threads-1.bin'), (4, 'st/threads-4.bin')):
            self.upload_large(bucket_name, name, payload, threads, f'upload with {threads} threads')
            self.assert_large_version(bucket_name, name, payload, f'read {threads}-thread metadata')

    def large_parallel_download(self) -> None:
        bucket_name = self.new_bucket()
        payload = self.large_payload()
        name = 'st/parallel-download.bin'
        self.upload_large(bucket_name, name, payload, 4, 'upload parallel-download fixture')
        self.assert_large_version(bucket_name, name, payload, 'read parallel-download metadata')
        for threads in (1, 4):
            path = self.scratch / f'download-{threads}.bin'
            self.invoke(
                f'download with {threads} threads',
                'file',
                'download',
                '--no-progress',
                '--threads',
                str(threads),
                '--max-download-streams-per-file',
                str(threads),
                f'b2://{bucket_name}/{name}',
                str(path),
            )
            if path.read_bytes() != payload:
                raise CheckFailure('parallel download', f'{threads}-thread bytes differ')

    def upload_fifo(
        self, step: str, bucket_name: str, name: str, payload: bytes, *, incremental: bool
    ) -> subprocess.CompletedProcess[str]:
        fifo = self.scratch / f'fifo-{uuid.uuid4().hex}'
        payload_path = self.scratch / f'fifo-payload-{uuid.uuid4().hex}'
        payload_path.write_bytes(payload)
        os.mkfifo(fifo)
        writer = subprocess.Popen(
            [
                sys.executable,
                '-c',
                (
                    'import pathlib, sys, time\n'
                    'fifo, payload = map(pathlib.Path, sys.argv[1:])\n'
                    'data = payload.read_bytes()\n'
                    'while True:\n'
                    '    try:\n'
                    '        fifo.write_bytes(data)\n'
                    '    except BrokenPipeError:\n'
                    '        pass\n'
                    '    time.sleep(0.1)\n'
                ),
                str(fifo),
                str(payload_path),
            ],
            cwd=REPOSITORY_ROOT,
            env=self.child_environment,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        arguments = ['file', 'upload', '--no-progress']
        if incremental:
            arguments.append('--incremental-mode')
        try:
            completed = self.invoke_process(
                step, *arguments, bucket_name, str(fifo), name, timeout=30
            )
        finally:
            if writer.poll() is None:
                writer.terminate()
            writer.wait(timeout=5)
        return completed

    def large_unbound_incremental(self) -> None:
        bucket_name = self.new_bucket()
        payload = b'unbound-' * 512
        name = 'st/unbound.bin'
        uploaded = self.upload_fifo(
            'upload unbound stream', bucket_name, name, payload, incremental=False
        )
        if uploaded.returncode != 0:
            raise CheckFailure('upload unbound stream', 'command failed')
        versions = self.list_versions_for_bucket(bucket_name, name, 'read unbound metadata')
        matches = [item for item in versions if metadata_size(item) == len(payload)]
        if len(matches) != 1:
            raise CheckFailure('unbound stream', 'uploaded stream size did not round-trip')
        version = matches[0]
        info = version.get('fileInfo') or {}
        digest = metadata_sha1(version)
        if digest == 'none':
            digest = str(info.get('large_file_sha1', '')) if isinstance(info, dict) else ''
        if digest != sha1_bytes(payload):
            raise CheckFailure('unbound stream', 'uploaded stream digest did not round-trip')
        if self.download_from_bucket('download unbound stream', bucket_name, name) != payload:
            raise CheckFailure('unbound stream', 'downloaded stream bytes differ')

        incremental = self.upload_fifo(
            'upload incremental unbound stream',
            bucket_name,
            'st/unbound-incremental.bin',
            payload,
            incremental=True,
        )
        if incremental.returncode == 0:
            raise CheckFailure(
                'incremental boundary',
                '--incremental-mode accepted an unbound stream; Backblaze/B2_Command_Line_Tool#1164',
            )

    def urls_native_download(self) -> None:
        bucket_name = self.new_bucket()
        payload = b'native-url-' * 94
        name = 'st/url.txt'
        self.upload_to_bucket('upload URL fixture', bucket_name, payload, name)
        authorized_url = self.invoke(
            'build authorized URL',
            'file',
            'url',
            '--with-auth',
            '--duration',
            '60',
            f'b2://{bucket_name}/{name}',
        ).splitlines()[0]
        query = parse_qs(urlsplit(authorized_url).query)
        if 'Authorization' not in query or any(key.lower().startswith('x-amz-') for key in query):
            raise CheckFailure('native URL', 'authorized URL is not B2-native token shape')
        prefix_token = self.invoke(
            'get prefix authorization',
            'bucket',
            'get-download-auth',
            bucket_name,
            '--prefix',
            'st/',
            '--duration',
            '60',
        ).strip()
        if not prefix_token or '?' in prefix_token or 'x-amz-' in prefix_token.lower():
            raise CheckFailure('native URL', 'prefix authorization is not a bare token')
        with urlopen(authorized_url, timeout=10) as response:
            if response.status != 200 or response.read() != payload:
                raise CheckFailure('authorized fetch', 'response did not return fixture bytes')

        short_url = self.invoke(
            'build expiring URL',
            'file',
            'url',
            '--with-auth',
            '--duration',
            '1',
            f'b2://{bucket_name}/{name}',
        ).splitlines()[0]
        time.sleep(5)
        try:
            urlopen(short_url, timeout=10)
        except HTTPError as error:
            if error.code != 401:
                raise CheckFailure(
                    'URL expiry', f'expired URL returned HTTP {error.code}'
                ) from error
        else:
            raise CheckFailure('URL expiry', 'expired URL still returned 200')

    def run(self) -> None:
        with tempfile.TemporaryDirectory(dir=self.scratch_root) as scratch_name:
            self.scratch = Path(scratch_name)
            self.created_buckets: list[str] = []
            self.created_keys: list[str] = []
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
                for bucket_name in reversed(self.created_buckets):
                    try:
                        self.invoke(
                            'cleanup bucket files',
                            'rm',
                            '--versions',
                            '--recursive',
                            '--no-progress',
                            '--fail-fast',
                            '--bypass-governance',
                            f'b2://{bucket_name}',
                        )
                    except CheckFailure:
                        pass
                    try:
                        self.invoke(
                            'cleanup replication',
                            'bucket',
                            'update',
                            bucket_name,
                            '--replication',
                            '{}',
                        )
                    except CheckFailure:
                        pass
                    try:
                        self.invoke('cleanup bucket', 'bucket', 'delete', bucket_name)
                    except CheckFailure as error:
                        if failure is None:
                            failure = error
                for key_id in reversed(self.created_keys):
                    try:
                        self.invoke('cleanup key', 'key', 'delete', key_id)
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
