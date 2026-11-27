# Testing the B2 Command Line Tool

A green check is evidence only for the behavior and environment it exercised. Run
commands from the repository root with nox, uv and the selected Python installed;
see [CONTRIBUTING.md](../CONTRIBUTING.md) for setup. Examples use Python 3.12;
`NOX_PYTHONS=3.12` selects it for sessions without a version suffix.

| Level | Where | Command | What it proves | What it does not prove |
| --- | --- | --- | --- | --- |
| Unit | `test/unit/`, including `ConsoleTool` and b2sdk `RawSimulator` | `nox -s unit-3.12 -- -n 2` | CLI logic and simulated SDK interactions for the exercised cases. | Real B2, actual network behavior, or every service capability. |
| Offline integration | `test/integration/` | `nox -s integration-3.12 -- -n 2 -m "not require_secrets"` | Selected credential-free tests against each installed CLI executable. | Authenticated operations or B2 service behavior. |
| Live integration | `test/integration/` | `nox -s integration-3.12 -- -n 2 -m "require_secrets" --cleanup` | Selected authenticated CLI interactions with real B2 for that account and run. | Other accounts, regions, future service versions, or unselected behavior. |
| Documentation examples | `test/unit/test_documentation_examples.py`, README and docs | `NOX_PYTHONS=3.12 nox -s doc_examples` | The extracted examples parse with the b2v5 argument parser. | That examples execute successfully or work against B2. |
| Simulator integration | `.github/workflows/simulator-integration.yml`, `scripts/with-b2-simulator.sh`, `test/integration/` | `scripts/with-b2-simulator.sh SIMULATOR_DIR nox -s integration-3.12 -- -n 0 -m "require_secrets"` | The secret-marked integration cases exercised against the configured simulator. | Production B2 behavior or scenarios the simulator does not model. The helper starts the simulator checkout and sets its endpoint and trust bundle; export the fixture credentials (`sim-test-id`, `sim-test-secret`) first. |
| SDK harness | `.sdkharness/tests.tsv`, `.sdkharness/tests/`; `test/unit/test_sdkharness_*.py` | `nox -s unit-3.12 -- -n 2 -k sdkharness` | Repository-owned guard and harness unit tests. The separately invoked scenarios check CLI behavior against a loopback simulator. | The nox selection alone does not run the registered simulator scenarios or prove real B2 behavior. |
| Docker | `noxfile.py` `docker_test`, `test/integration/` | `NOX_PYTHONS=3.12 nox -s docker_test -- b2:test [PYTEST_ARG...]` | Integration tests against the supplied image, for the default and versioned CLI commands. | Other images or platforms; a missing daemon, image or credentials does not verify the container. |

The unit session runs pytest once per CLI API version, passing `--cli b2v3`,
`--cli b2v4`, and `--cli b2v5` itself. Run all three when shared command,
parser, compatibility or SDK-facing behavior may affect more than one version;
check version-specific behavior against every affected version. Integration also
runs each version unless an explicit system under test is supplied.

The live command needs `B2_TEST_APPLICATION_KEY_ID` and
`B2_TEST_APPLICATION_KEY` for a **throwaway B2 account** with suitable
capabilities. Never use a real account's master key. Tests can create and delete
resources; review cleanup and use a disposable account. The simulator command
requires a running simulator and its fixture credentials, endpoint and trust
configuration as in the workflow; these are not production B2 credentials. The
Docker session also runs credential-requiring integration tests and needs a
working Docker daemon, the named image, and throwaway credentials for live B2
or the simulator. Arguments after the image tag go to pytest. Against the
simulator, which listens only on the host's loopback interface, set
`B2_TEST_DOCKER_NETWORK=host` and write the trust bundle under the shared
`/tmp` mount with `B2_SIMULATOR_CA_BUNDLE=/tmp/b2-simulator-ca.pem`, as the
workflow does.

The SDK harness scenarios do not have a nox session: follow
[.sdkharness/README.md](../.sdkharness/README.md) to start a loopback simulator
and run the registered executables with simulator-only credentials. The nox
command in the table covers their unit tests, not the scenario runs. The
simulator workflow is mandatory for PRs; see the integration policy in
[CONTRIBUTING.md](../CONTRIBUTING.md).

## Known defects and proving a fix

`test/known_defect.py` marks tests that assert correct behavior for a known bug.
They are strict expected failures in the normal unit run; a matching failure is
`xfail`, while an unexpected pass fails the suite. Exception type, message and
assertion origin are constrained so unrelated failures do not count. Remove the
marker when fixing the defect so the test becomes a normal regression test.
To expose the actual failures, run:

```sh
nox -s unit-3.12 -- -n 2 -m known_defect --runxfail
```

This intentionally fails while selected defects remain; nox stops on the first
failing API version. For every new regression test, verify the test fails for the
intended reason with the fix removed, then restore the fix and verify it passes.
Record both results in the PR; an unrelated setup failure is not a revert proof.

## When live B2 is needed

Run or add a live check when a change affects a B2 request or depends on service
semantics, such as authentication, retries, encryption, retention, replication,
large files or a service-specific error. Simulator and unit results cannot prove
how B2 behaves. Pure client logic, documentation and CI-only changes generally
do not need a live test. If a needed live check cannot run, explain the reason
and the substitute evidence in the PR; it remains **NOT RUN**.

## Reporting results

List the actual commands, results and relevant skips in the PR. Any check that was
not executed, including one blocked by missing credentials, a simulator, Docker
or a toolchain, is **NOT RUN**, never PASS. A skipped test is not evidence that
its behavior works.
