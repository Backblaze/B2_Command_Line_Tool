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
    'bucket.cors',
    'bucket.crud',
    'bucket.lifecycle',
    'bucket.notification_rules',
    'bucket.replication_config',
    'bucket.replication_helper',
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

    def new_bucket(self, step: str = 'create bucket') -> str:
        name = f'sdkharness-conf-{uuid.uuid4().hex[:16]}'
        self.created_buckets.append(name)
        self.invoke(step, 'bucket', 'create', name, 'allPrivate')
        return name

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
