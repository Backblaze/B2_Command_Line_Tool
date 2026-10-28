# Review instructions

For human reviewers and review agents. Approval always comes from a maintainer; an agent's findings never
approve or block on their own, and the agent that wrote a change never reviews it.

## What failure looks like here

The B2 CLI fails by breaking a script that depends on it: a changed flag, output format, exit code or
stderr line in an ApiVer that promised to keep it. It also fails by sending the wrong request through
b2sdk (wrong bucket, file, encryption or retention setting), by deleting or overwriting data the user did
not ask to touch (`rm`, `sync --delete`, `--replace-newer`), by leaving buckets, keys or unfinished large
files behind, or by leaking a credential into output, logs or `b2_cli.log`. A test that passes either way
is also a failure: it claims coverage it does not give.

## Before the passes

1. Read the linked issue and, if present, `intent/<slug>/spec.md` and `plan.md`.
2. Check out the PR merged onto current `master` and run `nox -s ci`. Say what you ran.
3. For each behavior the PR claims to fix or add, revert that line and confirm a test fails. Name any
   behavior no test pins.

## Passes

Tag each finding with its pass.

- **Bugs**: logic errors, wrong defaults, error paths that swallow failures or exit 0, unclosed files or
  progress bars, threads and pools left running.
- **Compatibility**: every ApiVer a command is registered in (`b2/_internal/b2v*/registry.py`) keeps its
  flags, output and exit codes; deprecations warn on stderr; `b2` (latest) changes are announced in a
  `changed` or `removed` fragment that says what users must do.
- **Output**: stdout vs stderr is deliberate; `--quiet` prints nothing on stdout; JSON output stays valid
  and stable; help text reads well and keeps developer-only knobs out; messages use the invoked binary name
  (`self.console_tool.b2_binary_name`), not a hard-coded `b2`.
- **Data safety**: destructive commands need the flags the user gave, honor `--dry-run`, and touch only
  the bucket, prefix or versions requested.
- **Security**: application keys, auth tokens and SSE-C keys never printed, logged or put in errors;
  the account info database is never printed or copied; nothing new is sent outside B2.
- **Compliance**: the change matches the issue, `spec.md` and `plan.md`; the PR description matches the
  code; a `changelog.d` fragment exists, has the right type and reads well for users; `AGENTS.md` is
  updated when the change makes it stale or breaks a convention in it.

## Severity

- **Important**: breaks existing scripts or an ApiVer promise, deletes or leaks data or credentials,
  sends a wrong request, or leaves resources behind. A claimed behavior with no test pinning it is
  Important.
- **Nit**: style, naming, comments. At most five per review; summarize the rest as a count.
- **Pre-existing**: a real bug this PR did not introduce. Report it so it is not lost, but it does not
  block this PR; suggest an issue.

## Evidence

Every finding cites `file:line` and says what goes wrong, with the command line or sequence that
triggers it. Do not infer behavior from a function's name; read it, including the b2sdk code it calls.
If you could not verify something (no live B2 credentials, no simulator), say so.

## Converging

- Round 1 reviews the whole diff. From round 3 on, review only the commits since your last review.
- Name the class of bug, not one instance, so the author fixes every occurrence in one round.
- Never repeat a finding already posted on the PR, resolved or not.
- After three rounds with Important findings still open, stop and ask the author to reconsider the
  approach instead of posting more line comments.
- A red live-B2 integration job that is also red on `master` is not a finding against the PR.

## Answering findings

Every finding gets an answer before merge: fix it, or resolve the thread with a one-line reason
(false positive, intentional, out of scope with an issue link). Do not resolve a thread silently.

## Format

Start with a count (`2 Important, 1 Nit, 0 Pre-existing`, or `No issues found`), then what you checked
and against what (issue number, live B2 or simulator, commands run). Then a table sorted by severity with
the columns `Severity | Pass | File:Line | Finding`. Do not report generated files or what CI already
enforces (ruff, codespell, license headers, `towncrier_check`).
