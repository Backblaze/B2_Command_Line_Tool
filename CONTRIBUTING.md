# Contributing to B2 Command Line Tool

We encourage outside contributors to perform changes to our codebase. Many such changes have been merged already. 
In order to make it easier to contribute, core developers of this project:

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

The entry is written for users. CI, test, build and tooling changes use `Infrastructure`; documentation changes
use `Doc`. Behavior changes that can affect scripts (exit codes, output, new stderr messages) must say so. Check
your entries locally with:

    nox -s changie_check changelog_check

A PR that only changes `uv.lock` needs no entry; otherwise a maintainer can add the `-changelog` label to skip the
check.
 
## Working with coding agents

Most of the process here is written for people and coding agents alike:

* [AGENTS.md](AGENTS.md) (read by Claude Code through `CLAUDE.md`): commands, architecture, conventions and the
  mistakes reviewers keep finding. Read it before your first PR.
* [REVIEW.md](REVIEW.md): how PRs are reviewed, the severity of findings and how to answer them.
* [doc/review-lessons.md](doc/review-lessons.md): what reviewers asked for on past PRs, by theme. Check your PR
  against it before asking for review.
* [doc/reviewer-evals.md](doc/reviewer-evals.md) and `nox -s review_metrics`: how the review process itself is
  measured. An AI code review and security review comment on PRs once maintainers enable them; answer their
  findings like any other review comment.
* [intent/README.md](intent/README.md): larger changes start with an `intent.md`, then `spec.md` and `plan.md`,
  each approved by a person before the next. Bug fixes with a clear issue can start from a failing test.
* `.claude/skills/`: the AI-native SDLC playbook skills (`ai-native-sdlc` is the entry point).

If an agent wrote your PR, you own it: you review it before asking others to, and you answer the review.

## Local CI

`nox -s ci` runs what CI's lint, build and test jobs run, on Python 3.12: codespell on the files you changed, `lint`, `changie_check`,
`release_check`, `changelog_check`, `build`, unit tests and the integration tests that need no credentials (plus the live-B2 ones
when `B2_TEST_APPLICATION_KEY` and `B2_TEST_APPLICATION_KEY_ID` are set). Run it before every push. Set
`NOX_CI_PYTHON` to pick the interpreter, `NOX_CI_BASE` for a base other than `origin/master`, and
`NOX_CI_SKIP_CHANGELOG=1` when the PR has the `-changelog` label. Note that `nox -s lint` reformats files in place
rather than failing on formatting, so commit what it changes.

## Releases and security

Maintainers release by following [RELEASE.md](RELEASE.md). Report vulnerabilities privately as described in
[SECURITY.md](SECURITY.md), never in a public issue. Everyone taking part follows the
[Code of Conduct](CODE_OF_CONDUCT.md).

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

We use [uv](https://docs.astral.sh/uv/) for managing dependencies and developing locally.
If you want to change any of the project requirements (or requirement bounds) in `pyproject.toml`,
make sure that `uv.lock` file reflects those changes by using `uv add`, `uv lock` or other
commands - see [documentation](https://docs.astral.sh/uv/). You can verify that lock file
is up to date by running the linter.

## Linting

To run all available linters:

```bash
nox -s lint
```

## Testing

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

To build the documentation and watch for changes (including the source code):

```bash
nox -s doc
```

To just build the documentation:

```bash
nox --non-interactive -s doc
```
