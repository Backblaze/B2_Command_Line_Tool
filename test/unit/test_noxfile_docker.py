######################################################################
#
# File: test/unit/test_noxfile_docker.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""Check the docker command that ``nox -s docker_test`` hands to the integration tests."""

import importlib.util
import sys
import types
from pathlib import Path
from unittest import mock

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def noxfile(monkeypatch):
    """Import the real noxfile; only the ``nox`` package it decorates sessions with is stubbed."""
    stub = types.ModuleType('nox')
    stub.options = types.SimpleNamespace()
    stub.Session = object
    stub.session = lambda *args, **kwargs: (lambda function: function)
    monkeypatch.setitem(sys.modules, 'nox', stub)
    spec = importlib.util.spec_from_file_location('noxfile_under_test', ROOT / 'noxfile.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RecordingSession:
    """The part of ``nox.Session`` that the docker sessions use, recording each command."""

    def __init__(self, posargs=()):
        self.posargs = list(posargs)
        self.virtualenv = types.SimpleNamespace(location='/venv')
        self.commands = []

    def run(self, *args, **kwargs):
        if args[:2] == ('id', '-u'):
            return '1001\n'
        if args[:2] == ('id', '-g'):
            return '127\n'
        self.commands.append(args)
        return ''

    def run_install(self, *args, **kwargs):
        pass


def pytest_runs(session):
    return [command for command in session.commands if command[0] == 'pytest']


def sut(command):
    return command[command.index('--sut') + 1]


def test_docker_tests_cover_the_default_command_and_every_versioned_command(noxfile, monkeypatch):
    monkeypatch.delenv('B2_TEST_DOCKER_NETWORK', raising=False)
    session = RecordingSession(['b2:test'])
    with mock.patch.object(noxfile, 'get_versions', return_value=['_b2v4', 'b2v3', 'b2v4']):
        noxfile.docker_test(session)

    base = 'docker run -i --user 1001:127 -v /tmp:/tmp:rw --env-file ENVFILE b2:test'
    assert [sut(command) for command in pytest_runs(session)] == [
        base,
        f'{base} _b2v4',
        f'{base} b2v3',
        f'{base} b2v4',
    ]
    for command in pytest_runs(session):
        assert '--network' not in sut(command)
        assert command[command.index('--env-file-cmd-placeholder') + 1] == 'ENVFILE'


def test_docker_tests_can_share_the_host_network(noxfile, monkeypatch):
    monkeypatch.setenv('B2_TEST_DOCKER_NETWORK', 'host')
    session = RecordingSession(['b2:test'])
    with mock.patch.object(noxfile, 'get_versions', return_value=['b2v4']):
        noxfile.docker_test(session)

    suts = [sut(command) for command in pytest_runs(session)]
    assert len(suts) == 2
    for value in suts:
        assert value.startswith('docker run -i --network host --user 1001:127 -v /tmp:/tmp:rw ')


def test_docker_tests_pass_further_arguments_to_pytest(noxfile, monkeypatch):
    monkeypatch.delenv('B2_TEST_DOCKER_NETWORK', raising=False)
    session = RecordingSession(['b2:test', '-n', '0', '-m', 'require_secrets'])
    with mock.patch.object(noxfile, 'get_versions', return_value=['b2v4']):
        noxfile.docker_test(session)

    runs = pytest_runs(session)
    assert len(runs) == 2
    for command in runs:
        assert list(command[-4:]) == ['-n', '0', '-m', 'require_secrets']
        assert 'b2:test' in sut(command)


def test_docker_tests_still_require_an_image(noxfile):
    with pytest.raises(ValueError, match='docker_image_tag'):
        noxfile.docker_test(RecordingSession())
