# B2 Command Line Tool

The `b2` CLI for Backblaze B2 (PyPI `b2`), built on [`b2sdk`](https://github.com/Backblaze/b2-sdk-python).
This file is the map for coding agents and new contributors; `CLAUDE.md` points here. Keep it under about
150 lines and link out for detail.

## Commands

Install [nox](https://nox.thea.codes) and [uv](https://docs.astral.sh/uv/) (`uv` 0.13.4 or newer).

- Format: `nox -s format` (ruff). Lint: `nox -s lint` (ruff, license check, `test/static`, `uv lock --check`).
- Unit tests: `nox -s unit-3.12`. They run once per ApiVer (`--cli b2v3`, `b2v4`, `b2v5`) and need no
  credentials. Test levels and what each proves: [doc/testing.md](doc/testing.md).
- Integration tests: `nox -s integration-3.12 -- -m "not require_secrets"` runs offline. Live B2 needs a
  throwaway account (`B2_TEST_APPLICATION_KEY_ID`, `B2_TEST_APPLICATION_KEY`).
- Docs: `nox -s doc` builds Sphinx with warnings as errors. `nox -s doc_examples` checks that every `b2`
  command line in the README, the Sphinx pages and the CLI skill is accepted by the argument parser.
- Docs lint: `nox -s docs_lint` checks changed Markdown for broken relative links. It is warn-only and not
  yet in CI; see [CONTRIBUTING.md](CONTRIBUTING.md).
- Changelog: add an entry with `go run github.com/miniscruff/changie@v1.24.0 new` (needs Go).
  `nox -s changie_check` validates entries. See [CONTRIBUTING.md](CONTRIBUTING.md#changelog).
- Build: `nox -s build`. Dependencies: change `pyproject.toml` with `uv add` or `uv lock`; `uv.lock` must stay
  in sync.

External tools:

- **[b2-mcp](https://github.com/backblaze-labs/b2-mcp)** — a Model Context Protocol server for B2.

## Architecture

| Path | Role |
| --- | --- |
| `b2/_internal/console_tool.py` | Every command class (`Command`, `B2`, subcommands), argument definitions, output. Most changes land here. |
| `b2/_internal/b2v3/`, `b2v4/`, `b2v5/` | ApiVer entry points. Each `registry.py` registers the commands for that version and may override old behavior. `b2` is `b2v5`. |
| `b2/_internal/_cli/` | Shared argument types, completers, `obj_dumps` (JSON output), autocomplete install and cache. |
| `b2/_internal/_utils/` | `b2://` and `b2id://` URI parsing. |
| `test/unit/` | Offline tests through `ConsoleTool` with b2sdk's `RawSimulator`; `console_tool/` holds per-command files. |
| `test/integration/` | Runs the CLI as a subprocess against live B2 or the simulator. |
| `test/static/` | License headers and other static checks. |
| `.sdkharness/` | Contract checks run by the central SDK harness against the B2 simulator on loopback. |
| `.changes/` | Changie entries (`unreleased/*.yaml`) and released sections (`v*.md`); `CHANGELOG.md` is built from them. |
| `doc/` | Sphinx docs (`doc/source`) and Markdown pages: [testing](doc/testing.md), [writing style](doc/writing-style.md). Output formats and exit codes: [doc/source/scripting.rst](doc/source/scripting.rst). |
| `.claude/skills/b2-cli/` | Agent skill for operating the `b2` CLI safely. |

The SDK does the B2 work; the CLI parses arguments, calls `b2sdk.v3`, and prints. A behavior that belongs
in the SDK (retries, wire shapes, large-file logic) is fixed in `b2-sdk-python`, then picked up here by
bumping the `b2sdk` pin.

## Conventions

- Python 3.10+ (`requires-python`); CI tests 3.10 to 3.14 on Linux, macOS and Windows, and PyPy 3.10 on Linux.
- ApiVer is a compatibility promise. `b2vN` keeps its commands, flags, output and exit codes; a change
  to them goes into a new ApiVer or behind a deprecation, never into an existing `b2vN` in place. Check
  the `registry.py` of every version a command is registered in.
- Command help comes from the class docstring and is part of the product (`--help`, README usage, Sphinx).
  Developer-only flags and environment variables (`--environment`, `--dev`, `B2_ENVIRONMENT`) stay out of
  the main help.
- Write new and edited docs as described in [doc/writing-style.md](doc/writing-style.md).
- Messages that tell the user to run something use the binary name the user invoked
  (`self.console_tool.b2_binary_name` in a `Command`), never a hard-coded `b2`.
- Every source file starts with the Backblaze license header (`test/static` checks it).
- Comments are short and explain non-obvious decisions. Ruff config is in `pyproject.toml`.

## Things agents get wrong here

These recur in review feedback (collected in draft PR #1193).

1. **Missing changelog entry.** Every PR adds a `.changes/unreleased/*.yaml` entry (`changie new`). Only a
   `uv.lock`-only change or the `-changelog` label skips it. Docs-only PRs use the `Doc` kind.
2. **Entry text and kind.** One complete sentence ending in a full stop ("fixed #157" is rejected).
   CI, tests, build and tooling go in `Infrastructure`; docs in `Doc`. User-facing sections are for users.
3. **Unannounced behavior changes.** A new exit code, changed output or new stderr text breaks scripts.
   Say so in the entry, and keep old ApiVers unchanged.
4. **Tests that do not pin the fix.** Revert the change and the new test must fail. Cover each input
   branch (file and stdin `-`, `b2://` and `b2id://`, every ApiVer the command is registered in), in a
   test placed with its topic, not appended to an unrelated block.
5. **stdout and stderr are a contract.** `--quiet` prints no progress; results use `enforce_output`.
   JSON output stays parseable. Warnings and deprecations go to stderr, and users monitor stderr, so do
   not add noise there. See [doc/source/scripting.rst](doc/source/scripting.rst).
6. **Developer knobs in user help.** Keep internal flags and env vars out of `--help` and README; document
   them in `doc/` instead.
7. **Integration tests that share state.** They run in parallel against one account: no fixed bucket
   names, no assumptions about bucket list order, clean up what you create. Before blaming a PR for a red
   integration job, check whether `master` is red too.
8. **b2sdk pins.** Depend on a released `b2sdk` within the declared range, never a git URL. Fix SDK
   behavior in the SDK.
9. **CI hygiene.** Pin third-party actions to a commit SHA, drop runner images GitHub retires, keep
   `cd.yml` and `ci.yml` runner versions aligned.
10. **Proof that did not run.** A skipped check is NOT RUN, never PASS. Tests can pass under nox and fail
    under bare pytest (forked tests); run them through nox.
11. **PR text that disagrees with code.** Re-read the description after the last commit.
12. **Undocumented options.** Every new flag goes in the command docstring with what it does, its
    choices and default, plus costs or limits users would otherwise learn from support.
13. **Happy-path-only tests.** Test the failure paths and assert the error message; parametrize instead of
    copying a test to vary one input.
14. **Unsafe destructive defaults.** Commands that delete (`rm`, `sync --delete`) must not be easy to
    trigger by accident, and filters must be tested together with deletion.
15. **Unrelated changes in the PR.** Refactors, formatter runs and side fixes go in their own PR or an
    issue; explain the reason for dependency and config changes in the description.

## Workflow

- Bug fixes: write the failing regression test first, then the fix, without editing the test.
- Before opening a PR, run the command you changed end to end against the simulator or a throwaway bucket
  and paste the output. Review your own diff in a fresh context (`/code-review`, `/security-review`, or a
  subagent that did not write the code) and say in the PR what it reported.
- Keep PRs small and reversible. Agents never approve or merge their own PRs; a maintainer approves.
- The person directing an agent owns its PR and fixes what it breaks.
- Skill for operating the CLI: [.claude/skills/b2-cli/SKILL.md](.claude/skills/b2-cli/SKILL.md).
- Releases: [RELEASE.md](RELEASE.md). Security reports: [SECURITY.md](SECURITY.md).

## Verifying your work

Run `nox -s lint`, `nox -s unit-3.12`, `nox -s changie_check` and, for docs, `nox -s doc doc_examples
docs_lint`. If a test fails, fix the code, not the test. List the live-B2 tests you ran, and say NOT RUN for
the ones you could not.
