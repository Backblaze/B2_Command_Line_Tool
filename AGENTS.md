# B2 Command Line Tool

The `b2` CLI for Backblaze B2 (PyPI `b2`), built on [`b2sdk`](https://github.com/Backblaze/b2-sdk-python).
This file is the map for coding agents and new contributors; `CLAUDE.md` points here. Keep it under about
150 lines and link out for detail.

## Commands

Install [nox](https://nox.thea.codes) and [uv](https://docs.astral.sh/uv/) (`uv` 0.13.4 or newer).

- Full local gate (CI's lint, build and test jobs on Python 3.12): `nox -s ci`. Run it before every push and
  paste the tail in the PR. `NOX_CI_PYTHON`, `NOX_CI_BASE` and `NOX_CI_SKIP_CHANGELOG` adjust it (see
  `noxfile.py`). It does not run the doc, docker or bundle jobs.
- Format: `nox -s format` (ruff). Lint: `nox -s lint` (ruff check, license check,
  `test/static`, `uv lock --check`).
- Unit tests: `nox -s unit-3.12`. They run once per ApiVer (`--cli b2v3`, `b2v4`, `b2v5`) and need no
  credentials.
- Integration tests: `nox -s integration-3.12 -- -m "not require_secrets"` runs offline. With
  `B2_TEST_APPLICATION_KEY` and `B2_TEST_APPLICATION_KEY_ID` set, `-- -m require_secrets --cleanup` runs
  against live B2. Use a throwaway account. Leftover buckets: `nox -s cleanup_buckets`.
- Changelog: `nox -s towncrier_check` (fragment names and text) and `nox -s changelog_check` (this branch
  adds a fragment). See [CONTRIBUTING.md](CONTRIBUTING.md#changelog).
- Build: `nox -s build`. Docs: `nox --non-interactive -s doc`. Bundle: `nox -s bundle`.
- Spelling: `nox -s ci` runs codespell 2.4.1 on the files your branch changes.
- Dependencies: change `pyproject.toml` with `uv add` / `uv lock`; `uv.lock` must stay in sync.

## Architecture

| Path | Role |
| --- | --- |
| `b2/_internal/console_tool.py` | Every command class (`Command`, `B2`, subcommands), argument definitions, output. Most changes land here. |
| `b2/_internal/b2v3/`, `b2v4/`, `b2v5/` | ApiVer entry points. Each `registry.py` registers the commands for that version and may override old behavior. `b2` is `b2v5`. |
| `b2/_internal/_cli/` | Shared argument types, completers, `obj_dumps` (JSON output), autocomplete install and cache. |
| `b2/_internal/_utils/` | `b2://` and `b2id://` URI parsing. |
| `test/unit/` | Offline tests through `ConsoleTool` with b2sdk's `RawSimulator`; `console_tool/` holds per-command files. |
| `test/integration/` | Runs the CLI as a subprocess (`--sut` can point at a bundled binary) against live B2. |
| `test/static/` | License headers and other static checks. |
| `.sdkharness/` | Contract checks run by the central SDK harness against the B2 simulator on loopback. |
| `changelog.d/` | towncrier fragments; `CHANGELOG.md` is built from them at release. |
| `doc/` | Sphinx docs (`doc/source`), plus Markdown pages for niche topics (`authorization_realm.md`). |
| `intent/` | Per-change `intent.md`, `spec.md`, `plan.md` (see Workflow). |

The SDK does the B2 work; the CLI parses arguments, calls `b2sdk.v3`, and prints. A behavior that belongs
in the SDK (retries, wire shapes, large-file logic) is fixed in `b2-sdk-python`, then picked up here by
bumping the `b2sdk` pin.

## Conventions

- Python 3.10+ (`requires-python`); CI tests 3.10 to 3.14 on Linux, macOS and Windows, and PyPy 3.10 on Linux.
- ApiVer is a compatibility promise. `b2vN` keeps its commands, flags, output and exit codes; a change
  to them goes into a new ApiVer or behind a deprecation, never into an existing `b2vN` in place. Check
  the `registry.py` of every version a command is registered in.
- Command help comes from the class docstring and is part of the product (`--help`, README usage, Sphinx).
  Developer-only flags and environment variables (`--environment`, `--dev`, `B2_ENVIRONMENT`) stay out of the main help.
- Messages that tell the user to run something use the binary name the user invoked
  (`self.console_tool.b2_binary_name` in a `Command`), never a hard-coded `b2`.
- Every source file starts with the Backblaze license header (`test/static` checks it).
- Comments are short and explain non-obvious decisions. Ruff config is in `pyproject.toml`.

## Things agents get wrong here

Each line was raised by a reviewer on at least two PRs.

1. **Missing changelog fragment.** Every PR adds a `changelog.d` file: `{issue}.{type}.md` or
   `+{slug}.{type}.md`. Only a `uv.lock`-only change or the `-changelog` label skips it.
2. **Fragment text and type.** One complete sentence ending in a full stop ("fixed #157" is rejected).
   CI, tests, build and tooling go in `infrastructure`; docs in `doc`. User-facing sections are for users.
3. **Unannounced behavior changes.** A new exit code, changed output or new stderr text breaks scripts.
   Say so in the fragment (`changed`), and keep old ApiVers unchanged.
4. **Tests that do not pin the fix.** Revert the change and the new test must fail. Cover each input
   branch (file and stdin `-`, `b2://` and `b2id://`, every ApiVer the command is registered in), in a
   test placed with its topic, not appended to an unrelated block.
5. **stdout and stderr are a contract.** `--quiet` prints nothing on stdout. JSON output stays parseable.
   Warnings and deprecations go to stderr, and users monitor stderr, so do not add noise there.
6. **Developer knobs in user help.** Keep internal flags and env vars out of `--help` and README; document
   them in `doc/` instead.
7. **Integration tests that share state.** They run in parallel against one live account: no fixed bucket
   names, no assumptions about bucket list order, clean up what you create. Before blaming a PR for a red
   integration job, check whether `master` is red too.
8. **b2sdk pins.** Depend on a released `b2sdk` within the declared range, never a git URL (the release
   session refuses one). Fix SDK behavior in the SDK.
9. **CI hygiene.** Pin third-party actions to a commit SHA, drop runner images GitHub retires, keep
   `cd.yml` and `ci.yml` runner versions aligned.
10. **Proof that did not run.** A skipped check is NOT RUN, never PASS. Tests can pass under nox and fail
    under bare pytest (forked tests); run them through nox.
11. **PR text that disagrees with code.** Re-read the description after the last commit.

## Workflow (AI-native SDLC)

- Non-trivial changes start in `intent/<slug>/`: `intent.md` (problem) → `spec.md` (numbered requirements
  R1..) → `plan.md` (steps cite R numbers). A human approves each before the next. Templates in
  [`intent/README.md`](intent/README.md). Bug fixes with a linked issue may skip straight to a failing test.
- Bug fixes: commit the failing regression test first, then the fix, without editing the test.
- Skills in `.claude/skills/` (`ai-native-sdlc` routes to the right one). A `PostToolUse` hook in
  `.claude/settings.json` rejects edits that leave a Python file unformatted.
- Before opening a PR, review your own diff in a fresh context (`/code-review` and `/security-review`, or a
  subagent that did not write the code) and fix or answer what it finds. Say in the PR what it reported.
- Review: [REVIEW.md](REVIEW.md). Every finding gets fixed or resolved with a reason. Agents never approve
  or merge their own PRs; a maintainer approves.
- The person directing an agent owns its PR: they count as one of its reviewers and fix what it breaks.
- Releases: [RELEASE.md](RELEASE.md). Security reports: [SECURITY.md](SECURITY.md).

## Verifying your work

Run `nox -s ci` and paste the tail of its output. codespell (changed files), lint, `towncrier_check`,
`changelog_check`, build, unit tests and the offline integration tests must pass. If a test fails, fix the code, not the test.
List the live-B2 integration tests you ran, and say NOT RUN for the ones you could not.
