# Review instructions

For human reviewers and review agents. Approval always comes from a maintainer; an agent's findings never
approve or block on their own, and the agent that wrote a change never reviews it. Every PR also gets an
automated AI review and security review (`.github/workflows/ai-review.yml`) once a maintainer adds the
`ANTHROPIC_API_KEY` secret and installs the Claude GitHub App; their findings follow the same rules as
everyone else's.

Review has two jobs: find and fix problems before they ship, and keep the people who own this code
understanding it.

## What failure looks like here

The B2 CLI fails by breaking a script that depends on it: a changed flag, output format, exit code or
stderr line in an ApiVer that promised to keep it. It also fails by sending the wrong request through
b2sdk (wrong bucket, file, encryption or retention setting), by deleting or overwriting data the user did
not ask to touch (`rm`, `sync --delete`, `--replace-newer`), by leaving buckets, keys or unfinished large
files behind, or by leaking a credential into output, logs or `b2_cli.log`. A test that passes either way
is also a failure: it claims coverage it does not give.

## Review depth

Match the effort to the blast radius of the paths the PR touches.

| Depth | Paths | What it means |
| --- | --- | --- |
| Quick | `*.md`, `doc/`, `changelog.d/`, `intent/`, `test/` only | Read the diff; check claims against the code they describe. |
| Standard | `b2/_internal/` (except below), `noxfile.py` test sessions, `.sdkharness/`, b2sdk bumps | All passes below; run `nox -s ci`. |
| Deep | `rm`, `sync`, `delete-*`, file and bucket updates, `account authorize` and account info, `b2v*/registry.py`, `.github/workflows/`, `noxfile.py` bundle/sign/release sessions | All passes, read every caller and the b2sdk code involved, and exercise the command end to end (simulator or live B2). |

## Skip

Do not review or comment on: `uv.lock` (check only that it matches `pyproject.toml`), `CHANGELOG.md`
(generated at release), `doc/source/subcommands/` and other generated docs, vendored license files, and
anything ruff, codespell, the license-header test or `towncrier_check` already enforces. Style,
diagnostic-only and cleanup-only remarks are not findings.

## Before the passes

1. Read the linked issue and, if present, `intent/<slug>/spec.md` and `plan.md`. Keep
   [doc/review-lessons.md](doc/review-lessons.md) at hand: it lists what earlier reviews found.
2. Check out the PR merged onto current `master` and run `nox -s ci`. Say what you ran.
3. For each behavior the PR claims to fix or add, revert that line and confirm a test fails. Name any
   behavior no test pins.
4. Check the diff against the whole repo, not only the changed lines: does it solve the stated problem,
   fit the system, and avoid breaking callers elsewhere? For a bug fix, check that it addresses the root
   cause rather than one symptom, and look for the same bug in sibling commands.

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
- **Security**: application keys, auth tokens and SSE-C keys never reach logs (`--verbose`,
  `--debug-logs`, `b2_cli.log`), error messages or stderr, and stdout only where a command documents it
  (`account get` prints the key and auth token, `key create` prints the new key); no new command or flag
  prints them without saying so in its help. The account info database is never copied, and nothing new
  is sent outside B2.
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

## Verifying findings (review agents)

Find candidates broadly, then verify each one before posting: read the code it depends on, and drop it
unless the input or sequence that triggers it is real. When several review agents run, a separate
verifying pass decides which findings are posted; unverified findings are not posted. Prefer one real
Important over ten speculative remarks.

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
(false positive, intentional, out of scope with an issue link). Do not resolve a thread silently. A
finding that a later commit fixes is resolved by that commit; say which one.

When the same finding shows up on several PRs, fix it at the source in the same change: a ruff rule or
test, a line in `AGENTS.md`, or an entry in [doc/review-lessons.md](doc/review-lessons.md).

## What people still review

Automated review does not replace these, and they are the reason a maintainer approves:

- Whether the change should be built at all: scope, direction and fit with the CLI's interface. Raise
  direction changes early in the issue or the first review, not as blocking comments on a finished PR;
  closing a PR is cheap.
- Risky code: the Deep paths above, especially destructive commands, credentials and CI workflows.
- Patterns others will copy: a new command layout, argument type or output format.
- The evidence: test output, commands run and their results, and the reviewer findings. If you cannot
  say what the checks would miss, review the code itself.
- Ownership: the person who lands a change owns fixing what it breaks, whoever wrote it.

For a trivial diff in code you do not know, ask Claude for an explainer of that slice of the code instead
of reading it line by line.

## Format

Start with a count (`2 Important, 1 Nit, 0 Pre-existing`, or `No issues found`), then what you checked
and against what (issue number, live B2 or simulator, commands run). Then a table sorted by severity with
the columns `Severity | Pass | File:Line | Finding`. Do not report generated files or what CI already
enforces (ruff, codespell, license headers, `towncrier_check`).
