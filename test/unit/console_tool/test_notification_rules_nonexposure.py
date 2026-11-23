######################################################################
#
# File: test/unit/console_tool/test_notification_rules_nonexposure.py
#
# Copyright 2026 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
"""
Operation-level secret non-exposure for ``b2 bucket notification-rule create`` and ``update``.

A notification rule can carry an HMAC signing secret and custom header values. Neither may reach
the diagnostics the CLI writes while it runs or the text of an error it reports.

What this adds to ``test_notification_rules.py``: that file checks success-path argument logging
and the stderr text of an invalid signing secret. This file runs the real ``create`` and ``update``
commands with ``--verbose --debug-logs`` through each way they can end (success, failure before
authorization, a service error, and a rejected ``--sign-secret``), and checks that neither
sensitive input appears in the logs or in the error text. Each run is checked for both inputs
at once, and a failure message names the input that leaked.

Each case is one (surface, path) pair, run for both commands:

* ``cli_log``: what the CLI's own loggers wrote (log records and the ``--debug-logs`` file);
* ``sdk_trace``: what the ``b2sdk`` loggers wrote at DEBUG (log records and the ``--debug-logs``
  file). The file also holds the CLI's lines, so this surface and ``cli_log`` are not independent;
  the log records are what tell them apart;
* ``exception``: tracebacks logged for the failure plus the error text the CLI printed.

Only pairs that can show something are included. Not covered, because there is nothing to
observe: the CLI log and SDK trace after a rejected ``--sign-secret`` (the arguments are rejected
before the CLI logs that it started a command), the SDK trace before authorization (no SDK call
has happened yet) and the exception text on success (nothing is raised).

The CLI deliberately prints the rule it created or updated, secrets included, to stdout on
success (``test_notification_rules__sign_secret`` relies on it), so stdout is not asserted
secret-free here.
"""

import logging

import pytest
from b2sdk.v3 import B2Api, InMemoryAccountInfo
from b2sdk.v3.exception import BadRequest

SIGN_SECRET = 'a' * 32
HEADER_VALUE = 'custom-header-value-must-not-leak-9d7c'
INVALID_SIGN_SECRET = 'DistinctiveInput_' + 'x' * 40
LOGGING_FLAGS = ['--verbose', '--debug-logs']
RULE_ARGS = [
    '--event-type',
    'b2:ObjectCreated:*',
    '--webhook-url',
    'https://example.com/webhook',
    '--custom-header',
    f'X-Test-Header={HEADER_VALUE}',
]

# A failing command writes its error report to stderr, which `run` otherwise requires to be empty.
ACCEPT_ERROR_REPORT = {'expected_stderr': None}


@pytest.fixture
def existing_rule(b2_cli, bucket):
    """A rule that ``update`` can modify (and that ``create`` must not collide with)."""
    b2_cli.run(
        [
            'bucket',
            'notification-rule',
            'create',
            f'b2://{bucket}',
            'existing-rule',
            '--event-type',
            'b2:ObjectCreated:*',
            '--webhook-url',
            'https://example.com/webhook',
        ]
    )
    return 'existing-rule'


@pytest.fixture
def unauthorized_cli(b2_cli, monkeypatch):
    """
    A CLI with no authorization. The suite's default stub account info fails with a stub-only
    ``TypeError`` when empty, so a real in-memory account info is used, which raises the same
    ``MissingAccountData`` a real unauthorized CLI does.
    """

    def initialize_unauthorized_api(cls, args, kwargs):
        kwargs.pop('profile', None)
        return B2Api(
            account_info=InMemoryAccountInfo(),
            cache=None,
            api_config=b2_cli.api_config,
            **kwargs,
        )

    monkeypatch.setattr(
        b2_cli.console_tool_class, '_initialize_b2_api', initialize_unauthorized_api
    )
    return b2_cli


@pytest.fixture
def cli_logging(b2_cli_log_fix, cwd_path, caplog, capsys):
    """Capture DEBUG logs from every logger, and undo the handlers the CLI installs."""
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger='b2sdk._internal.bucket')
    caplog.clear()
    capsys.readouterr()
    original_handlers = logging.root.handlers[:]
    yield
    for handler in logging.root.handlers[:]:
        if handler not in original_handlers:
            logging.root.removeHandler(handler)
            handler.close()


@pytest.fixture
def argparse_error_report(capsys):
    """Reads what argparse wrote about an invalid argument, which goes to the process stderr."""
    return lambda: capsys.readouterr().err


def _log_file_text(cwd_path):
    log_file = cwd_path / 'b2_cli.log'
    return log_file.read_text() if log_file.exists() else ''


def _formatted(caplog, logger_prefix):
    formatter = logging.Formatter()
    return '\n'.join(
        formatter.format(record)
        for record in caplog.records
        if record.name.startswith(logger_prefix)
    )


def _exception_text(caplog, error_report):
    """Tracebacks logged for a failure, plus the error text the CLI printed to stderr."""
    formatter = logging.Formatter()
    tracebacks = [
        formatter.formatException(record.exc_info) for record in caplog.records if record.exc_info
    ]
    return '\n'.join([*tracebacks, error_report])


def rejecting_set_call(*args, **kwargs):
    raise BadRequest('rejected by the service', 'bad_request')


# path -> (expected exit status, text proving the run ended the way the path says)
PATHS = {
    'success': (0, None),
    'pre_authorization': (1, 'ERROR'),
    'api_error': (1, 'rejected by the service'),
    'validation_error': (2, 'the secret has to be exactly 32 alphanumeric characters'),
}
CASES = [
    ('cli_log', 'success'),
    ('cli_log', 'pre_authorization'),
    ('cli_log', 'api_error'),
    ('sdk_trace', 'success'),
    ('sdk_trace', 'api_error'),
    ('exception', 'pre_authorization'),
    ('exception', 'api_error'),
    ('exception', 'validation_error'),
]


@pytest.mark.parametrize('subcommand', ['create', 'update'])
@pytest.mark.parametrize(('surface', 'path'), CASES)
def test_notification_rule_sensitive_inputs_stay_out_of_diagnostics(
    request,
    cwd_path,
    caplog,
    argparse_error_report,
    monkeypatch,
    subcommand,
    surface,
    path,
):
    expected_status, expected_marker = PATHS[path]
    sign_secret = INVALID_SIGN_SECRET if path == 'validation_error' else SIGN_SECRET
    if path == 'pre_authorization':
        cli, bucket, rule_name = request.getfixturevalue('unauthorized_cli'), 'my-bucket', 'rule'
    else:
        cli = request.getfixturevalue('b2_cli')
        bucket = request.getfixturevalue('bucket')
        existing_rule = request.getfixturevalue('existing_rule')
        rule_name = 'new-rule' if subcommand == 'create' else existing_rule
    request.getfixturevalue('cli_logging')  # after the CLI and rule exist, so setup is not logged
    if path == 'api_error':
        monkeypatch.setattr(cli.raw_api, 'set_bucket_notification_rules', rejecting_set_call)

    _, _, error_report = cli.run(
        [
            'bucket',
            'notification-rule',
            subcommand,
            *LOGGING_FLAGS,
            f'b2://{bucket}',
            rule_name,
            *RULE_ARGS,
            '--sign-secret',
            sign_secret,
        ],
        expected_status=expected_status,
        **({} if expected_status == 0 else ACCEPT_ERROR_REPORT),
    )
    if path == 'validation_error':
        error_report = argparse_error_report()  # argparse writes to the process stderr

    if surface == 'cli_log':
        text = _formatted(caplog, 'b2._internal') + '\n' + _log_file_text(cwd_path)
        marker = 'arguments hidden'  # the CLI really logged the command start
    elif surface == 'sdk_trace':
        text = _formatted(caplog, 'b2sdk') + '\n' + _log_file_text(cwd_path)
        marker = 'b2sdk'  # the SDK really logged while the command ran
    else:
        text = _exception_text(caplog, error_report)
        marker = expected_marker  # the failure was really reported

    assert sign_secret not in text, 'signing secret leaked'
    assert HEADER_VALUE not in text, 'custom header value leaked'
    assert marker in text
