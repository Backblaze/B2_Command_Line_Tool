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

Complements ``test_notification_rules.py`` (success-path argument logging, and the stderr text of
an invalid signing secret). There is one test per (surface, path); each is run for both commands
(``create``, ``update``) and each sensitive input (``signing_secret``, ``custom_header``), and
asserts exactly that cell:

* surfaces: CLI argument log (what the CLI logs about the command it starts), SDK debug trace
  (everything the ``b2sdk`` loggers write at DEBUG), and exception text (tracebacks logged for
  the failure plus the error the CLI prints). Both log surfaces are read from the captured log
  records and from the ``--debug-logs`` file;
* paths: success (simulator-backed), failure before authorization, API error (the simulator
  answers the set call with a ``bad_request``), and local validation error (an invalid
  ``--sign-secret``, rejected while the arguments are parsed).

Notes on what each cell can honestly assert:

* On success nothing is raised, so the success x exception tests assert that no traceback was
  logged and that the (empty) exception text holds no secret. This is weaker than the
  failure-path exception tests.
* A validation error is raised while parsing, before the CLI logs that it started a command, so
  the validation x CLI argument log tests assert over whatever the CLI logged (possibly nothing).
* The CLI deliberately prints the rule it created or updated, secrets included, to stdout on
  success (``test_notification_rules__sign_secret`` relies on it), so stdout is not asserted
  secret-free here; stdout and stderr are covered by the existing tests.
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

SENSITIVE_VALUES = {'signing_secret': SIGN_SECRET, 'custom_header': HEADER_VALUE}
SENSITIVE_VALUES_ON_VALIDATION_ERROR = {
    'signing_secret': INVALID_SIGN_SECRET,
    'custom_header': HEADER_VALUE,
}


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


def cli_logs(caplog, cwd_path):
    """What the CLI's own loggers wrote: log records plus the ``--debug-logs`` file."""
    return _formatted(caplog, 'b2._internal') + '\n' + _log_file_text(cwd_path)


def sdk_logs(caplog, cwd_path):
    """What the SDK's loggers wrote: log records plus the ``--debug-logs`` file."""
    return _formatted(caplog, 'b2sdk') + '\n' + _log_file_text(cwd_path)


def exception_logs(caplog, error_report):
    """Tracebacks logged for a failure, plus the error text the CLI printed to stderr."""
    formatter = logging.Formatter()
    tracebacks = [
        formatter.formatException(record.exc_info) for record in caplog.records if record.exc_info
    ]
    return '\n'.join([*tracebacks, error_report])


def rejecting_set_call(*args, **kwargs):
    raise BadRequest('rejected by the service', 'bad_request')


# ── CLI argument log ─────────────────────────────────────────────────────────


@pytest.mark.parametrize('subcommand', ['create', 'update'])
@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_notification_rule_sensitive_input_stays_out_of_cli_argument_log_on_success_path(
    b2_cli, bucket, existing_rule, cli_logging, cwd_path, caplog, subcommand, sensitive_input
):
    b2_cli.run(
        [
            'bucket',
            'notification-rule',
            subcommand,
            *LOGGING_FLAGS,
            f'b2://{bucket}',
            'new-rule' if subcommand == 'create' else existing_rule,
            *RULE_ARGS,
            '--sign-secret',
            SIGN_SECRET,
        ]
    )
    cli_argument_log = cli_logs(caplog, cwd_path)
    assert SENSITIVE_VALUES[sensitive_input] not in cli_argument_log
    assert 'arguments hidden' in cli_argument_log  # the CLI really logged the command start


@pytest.mark.parametrize('subcommand', ['create', 'update'])
@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_notification_rule_sensitive_input_stays_out_of_cli_argument_log_on_pre_authorization_path(
    unauthorized_cli, cli_logging, cwd_path, caplog, subcommand, sensitive_input
):
    unauthorized_cli.run(
        [
            'bucket',
            'notification-rule',
            subcommand,
            *LOGGING_FLAGS,
            'b2://my-bucket',
            'rule-name',
            *RULE_ARGS,
            '--sign-secret',
            SIGN_SECRET,
        ],
        expected_status=1,
        **ACCEPT_ERROR_REPORT,
    )
    cli_argument_log = cli_logs(caplog, cwd_path)
    assert SENSITIVE_VALUES[sensitive_input] not in cli_argument_log
    assert 'arguments hidden' in cli_argument_log  # logged before the call is authorized


@pytest.mark.parametrize('subcommand', ['create', 'update'])
@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_notification_rule_sensitive_input_stays_out_of_cli_argument_log_on_api_error_path(
    b2_cli,
    bucket,
    existing_rule,
    cli_logging,
    cwd_path,
    caplog,
    monkeypatch,
    subcommand,
    sensitive_input,
):
    monkeypatch.setattr(b2_cli.raw_api, 'set_bucket_notification_rules', rejecting_set_call)
    b2_cli.run(
        [
            'bucket',
            'notification-rule',
            subcommand,
            *LOGGING_FLAGS,
            f'b2://{bucket}',
            'new-rule' if subcommand == 'create' else existing_rule,
            *RULE_ARGS,
            '--sign-secret',
            SIGN_SECRET,
        ],
        expected_status=1,
        **ACCEPT_ERROR_REPORT,
    )
    cli_argument_log = cli_logs(caplog, cwd_path)
    assert SENSITIVE_VALUES[sensitive_input] not in cli_argument_log
    assert 'arguments hidden' in cli_argument_log  # the CLI really logged the command start


@pytest.mark.parametrize('subcommand', ['create', 'update'])
@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_notification_rule_sensitive_input_stays_out_of_cli_argument_log_on_validation_error_path(
    b2_cli, bucket, existing_rule, cli_logging, cwd_path, caplog, subcommand, sensitive_input
):
    b2_cli.run(
        [
            'bucket',
            'notification-rule',
            subcommand,
            *LOGGING_FLAGS,
            f'b2://{bucket}',
            'new-rule' if subcommand == 'create' else existing_rule,
            *RULE_ARGS,
            '--sign-secret',
            INVALID_SIGN_SECRET,
        ],
        expected_status=2,
        **ACCEPT_ERROR_REPORT,
    )
    cli_argument_log = cli_logs(caplog, cwd_path)
    assert SENSITIVE_VALUES_ON_VALIDATION_ERROR[sensitive_input] not in cli_argument_log


# ── SDK debug trace ──────────────────────────────────────────────────────────


@pytest.mark.parametrize('subcommand', ['create', 'update'])
@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_notification_rule_sensitive_input_stays_out_of_sdk_debug_trace_on_success_path(
    b2_cli, bucket, existing_rule, cli_logging, cwd_path, caplog, subcommand, sensitive_input
):
    b2_cli.run(
        [
            'bucket',
            'notification-rule',
            subcommand,
            *LOGGING_FLAGS,
            f'b2://{bucket}',
            'new-rule' if subcommand == 'create' else existing_rule,
            *RULE_ARGS,
            '--sign-secret',
            SIGN_SECRET,
        ]
    )
    sdk_debug_trace = sdk_logs(caplog, cwd_path)
    assert SENSITIVE_VALUES[sensitive_input] not in sdk_debug_trace
    assert 'b2sdk' in sdk_debug_trace  # the SDK really logged while the command ran


@pytest.mark.parametrize('subcommand', ['create', 'update'])
@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_notification_rule_sensitive_input_stays_out_of_sdk_debug_trace_on_pre_authorization_path(
    unauthorized_cli, cli_logging, cwd_path, caplog, subcommand, sensitive_input
):
    unauthorized_cli.run(
        [
            'bucket',
            'notification-rule',
            subcommand,
            *LOGGING_FLAGS,
            'b2://my-bucket',
            'rule-name',
            *RULE_ARGS,
            '--sign-secret',
            SIGN_SECRET,
        ],
        expected_status=1,
        **ACCEPT_ERROR_REPORT,
    )
    sdk_debug_trace = sdk_logs(caplog, cwd_path)
    assert SENSITIVE_VALUES[sensitive_input] not in sdk_debug_trace


@pytest.mark.parametrize('subcommand', ['create', 'update'])
@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_notification_rule_sensitive_input_stays_out_of_sdk_debug_trace_on_api_error_path(
    b2_cli,
    bucket,
    existing_rule,
    cli_logging,
    cwd_path,
    caplog,
    monkeypatch,
    subcommand,
    sensitive_input,
):
    monkeypatch.setattr(b2_cli.raw_api, 'set_bucket_notification_rules', rejecting_set_call)
    b2_cli.run(
        [
            'bucket',
            'notification-rule',
            subcommand,
            *LOGGING_FLAGS,
            f'b2://{bucket}',
            'new-rule' if subcommand == 'create' else existing_rule,
            *RULE_ARGS,
            '--sign-secret',
            SIGN_SECRET,
        ],
        expected_status=1,
        **ACCEPT_ERROR_REPORT,
    )
    sdk_debug_trace = sdk_logs(caplog, cwd_path)
    assert SENSITIVE_VALUES[sensitive_input] not in sdk_debug_trace
    assert 'b2sdk' in sdk_debug_trace  # the SDK really logged while the command ran


@pytest.mark.parametrize('subcommand', ['create', 'update'])
@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_notification_rule_sensitive_input_stays_out_of_sdk_debug_trace_on_validation_error_path(
    b2_cli, bucket, existing_rule, cli_logging, cwd_path, caplog, subcommand, sensitive_input
):
    b2_cli.run(
        [
            'bucket',
            'notification-rule',
            subcommand,
            *LOGGING_FLAGS,
            f'b2://{bucket}',
            'new-rule' if subcommand == 'create' else existing_rule,
            *RULE_ARGS,
            '--sign-secret',
            INVALID_SIGN_SECRET,
        ],
        expected_status=2,
        **ACCEPT_ERROR_REPORT,
    )
    sdk_debug_trace = sdk_logs(caplog, cwd_path)
    assert SENSITIVE_VALUES_ON_VALIDATION_ERROR[sensitive_input] not in sdk_debug_trace


# ── exception text ───────────────────────────────────────────────────────────


@pytest.mark.parametrize('subcommand', ['create', 'update'])
@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_notification_rule_sensitive_input_stays_out_of_exception_on_success_path(
    b2_cli, bucket, existing_rule, cli_logging, caplog, subcommand, sensitive_input
):
    # Weaker than the failure-path cells: a successful run raises nothing, so there is no
    # exception text that could carry the secret. This pins that nothing was raised or logged
    # as a traceback (the run succeeded) and that the exception text is therefore free of it.
    _, _, error_report = b2_cli.run(
        [
            'bucket',
            'notification-rule',
            subcommand,
            *LOGGING_FLAGS,
            f'b2://{bucket}',
            'new-rule' if subcommand == 'create' else existing_rule,
            *RULE_ARGS,
            '--sign-secret',
            SIGN_SECRET,
        ]
    )
    exception_text = exception_logs(caplog, error_report)
    assert SENSITIVE_VALUES[sensitive_input] not in exception_text
    assert 'Traceback' not in exception_text


@pytest.mark.parametrize('subcommand', ['create', 'update'])
@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_notification_rule_sensitive_input_stays_out_of_exception_on_pre_authorization_path(
    unauthorized_cli, cli_logging, caplog, subcommand, sensitive_input
):
    _, _, error_report = unauthorized_cli.run(
        [
            'bucket',
            'notification-rule',
            subcommand,
            *LOGGING_FLAGS,
            'b2://my-bucket',
            'rule-name',
            *RULE_ARGS,
            '--sign-secret',
            SIGN_SECRET,
        ],
        expected_status=1,
        **ACCEPT_ERROR_REPORT,
    )
    exception_text = exception_logs(caplog, error_report)
    assert SENSITIVE_VALUES[sensitive_input] not in exception_text
    assert 'ERROR' in exception_text  # the failure was really reported


@pytest.mark.parametrize('subcommand', ['create', 'update'])
@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_notification_rule_sensitive_input_stays_out_of_exception_on_api_error_path(
    b2_cli, bucket, existing_rule, cli_logging, caplog, monkeypatch, subcommand, sensitive_input
):
    monkeypatch.setattr(b2_cli.raw_api, 'set_bucket_notification_rules', rejecting_set_call)
    _, _, error_report = b2_cli.run(
        [
            'bucket',
            'notification-rule',
            subcommand,
            *LOGGING_FLAGS,
            f'b2://{bucket}',
            'new-rule' if subcommand == 'create' else existing_rule,
            *RULE_ARGS,
            '--sign-secret',
            SIGN_SECRET,
        ],
        expected_status=1,
        **ACCEPT_ERROR_REPORT,
    )
    exception_text = exception_logs(caplog, error_report)
    assert SENSITIVE_VALUES[sensitive_input] not in exception_text
    assert 'rejected by the service' in exception_text  # the service error was really reported


@pytest.mark.parametrize('subcommand', ['create', 'update'])
@pytest.mark.parametrize('sensitive_input', ['signing_secret', 'custom_header'])
def test_notification_rule_sensitive_input_stays_out_of_exception_on_validation_error_path(
    b2_cli,
    bucket,
    existing_rule,
    cli_logging,
    caplog,
    argparse_error_report,
    subcommand,
    sensitive_input,
):
    b2_cli.run(
        [
            'bucket',
            'notification-rule',
            subcommand,
            *LOGGING_FLAGS,
            f'b2://{bucket}',
            'new-rule' if subcommand == 'create' else existing_rule,
            *RULE_ARGS,
            '--sign-secret',
            INVALID_SIGN_SECRET,
        ],
        expected_status=2,
        **ACCEPT_ERROR_REPORT,
    )
    error_report = argparse_error_report()
    exception_text = exception_logs(caplog, error_report)
    assert SENSITIVE_VALUES_ON_VALIDATION_ERROR[sensitive_input] not in exception_text
    assert 'the secret has to be exactly 32 alphanumeric characters' in exception_text
