# Contributing to B2 Command Line Tool

We welcome outside contributions to the codebase. We have merged many such changes.
To make contributions easier, core developers:

* provide guidance (through the issue reporting system)
* provide tool assisted code review (through the Pull Request system)
* maintain a set of unit tests
* maintain a set of integration tests (run with a production cloud)
* maintain development automation tools using [nox](https://github.com/theacodes/nox) that can easily:
  * format the code using [ruff](https://github.com/astral-sh/ruff)
  * run linters to find subtle/potential issues with maintainability
  * run the test suite on multiple Python versions using [pytest](https://github.com/pytest-dev/pytest)
* maintain Continuous Integration (by using GitHub Actions) that:
  * runs all sorts of linters
  * checks if the Python distribution can be built
  * runs all tests on a matrix of supported versions of Python (including PyPy) and 3 operating systems 
    (Linux, Mac OS X, and Windows)
  * checks if the documentation can be built properly
* maintain other Continuous Integration tools (coverage tracker)

## Versioning

This package's versions adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) and the versions are
established by reading git tags, i.e. no code or manifest file changes are required when working on PRs.

## Changelog

Each PR needs a changelog entry in [`.changes/unreleased`](.changes/unreleased). Use
[Changie](https://changie.dev/) with [Go](https://go.dev/doc/install) installed. The pinned
`go run` command downloads Changie on first use; no separate Changie installation is needed:

    go run github.com/miniscruff/changie@v1.24.0 new --kind Fixed --body 'Describe the user-visible fix.'

The configured kinds are Removed, Changed, Fixed, Deprecated, Added, Doc, and Infrastructure.
Write a complete sentence starting with a capital letter and ending with a full stop.
CI validates that form and the 16 KiB fragment limit, preserving the
[pre-migration repository policy](https://github.com/Backblaze/B2_Command_Line_Tool/blob/9194ec532616c4a2044b0e78b08c84fd8e86e386/noxfile.py#L853-L924). For a release, `go run github.com/miniscruff/changie@v1.24.0 batch vX.Y.Z` assembles the
unreleased entries into `.changes/vX.Y.Z.md`;
`go run github.com/miniscruff/changie@v1.24.0 merge` rebuilds [CHANGELOG.md](CHANGELOG.md).
The existing release history is kept in `.changes/v*.md` so rebuilding the changelog preserves it.
 
## Developer Info

You'll need to have [nox](https://github.com/theacodes/nox) and [uv](https://docs.astral.sh/uv/) installed:

* `pip install nox uv`

With `nox`, you can run different sessions (default are `lint` and `test`):

* `format` -> Format the code.
* `lint` -> Run linters.
* `test` (`test-3.10`, `test-3.11`) -> Run test suite.
* `cover` -> Perform coverage analysis.
* `build` -> Build the distribution.
* `generate_dockerfile` -> generate dockerfile
* `docker_test` -> run integration tests against a docker image
* `build_and_test_docker` -> build a docker image and integration tests against it
* `doc` -> Build the documentation.
* `doc_cover` -> Perform coverage analysis for the documentation.

For example:

```bash
$ nox -s format
nox > Running session format
nox > Creating virtual environment (virtualenv) using python3.11 in .nox/format
...

$ nox -s format
nox > Running session format
nox > Re-using existing virtual environment at .nox/format.
...

$ nox --no-venv -s format
nox > Running session format
...
```

Sessions `test` ,`unit`, and `integration` can run on many Python versions.

Sessions other than that use the last given Python version.

You can change it:

```bash
export NOX_PYTHONS=3.10,3.13
```

With the above setting, session `test` will run on Python 3.9 and 3.10, and all other sessions on Python 3.10.

Given Python interpreters should be installed in the operating system or via [pyenv](https://github.com/pyenv/pyenv).

## Managing dependencies

We use [uv](https://docs.astral.sh/uv/) to manage dependencies and develop locally.
If you want to change any of the project requirements (or requirement bounds) in `pyproject.toml`,
make sure that `uv.lock` file reflects those changes by using `uv add`, `uv lock` or other
commands - see [documentation](https://docs.astral.sh/uv/). You can verify that lock file
is up to date by running the linter.

## Linting

Run all available linters:

```bash
nox -s lint
```

## Testing

See the [testing guide](doc/testing.md) for test levels, commands and reporting evidence.

### Pull request integration policy

Every pull request must run the integration suite against the pinned private B2 Simulator. GitHub does not expose
the Simulator's read-only deploy key to untrusted pull request contexts, including forks and Dependabot. Those pull
requests therefore fail closed: a maintainer must reproduce the change on a branch in this repository before it can
pass the Simulator check. Do not bypass or skip the check, and do not use `pull_request_target` to run untrusted code
with the deploy key.

Repository administrators must install a read-only deploy key for `backblaze-labs/b2-simulator`, store its private
half as the `SIMULATOR_DEPLOY_KEY` Actions secret, and require both `simulator-access-policy` and
`simulator-integration` before merging.

Pull request CI never receives real-B2 credentials. The `Weekly Live B2 Integration` workflow
(`.github/workflows/live-b2.yml`) runs the secret-marked integration suite against real B2 every Monday and on
manual dispatch. It fails if `B2_TEST_APPLICATION_KEY_ID` or `B2_TEST_APPLICATION_KEY` is missing, so a live run
cannot pass by skipping its tests. To check a change that depends on real B2 behavior before merge, run that
workflow manually on the branch.

To run all tests on every available Python version:

```bash
nox -s test
```

To run all tests on a specific version:

```bash
nox -s test-3.11
```

To run just unit tests:

```bash
nox -s unit-3.11
```

To run just integration tests:

```bash
export B2_TEST_APPLICATION_KEY=your_app_key
export B2_TEST_APPLICATION_KEY_ID=your_app_key_id
nox -s integration-3.11
```

## Documentation

Follow the [writing style guide](doc/writing-style.md) for new and edited documentation.

To build the documentation and watch for changes (including the source code):

```bash
nox -s doc
```

To just build the documentation:

```bash
nox --non-interactive -s doc
```
