# Review lessons

What reviewers asked for on the merged PRs of this repository, grouped by theme, with the PRs where it came
up. It comes from all 692 reviewer comments (inline, review and conversation, excluding the PR author's own
replies and bots) on the 571 PRs merged up to #1191. Copilot had not reviewed any merged PR at that point.

Use it as a checklist before asking for review. When a review teaches something that will matter again,
add a line here with the PR link in the same change. Rules that apply to every change, and that came up on
at least two PRs, are also in [AGENTS.md](../AGENTS.md#things-agents-get-wrong-here).

PRs before #550 predate the split of the library into
[b2-sdk-python](https://github.com/Backblaze/b2-sdk-python); lessons from them about SDK internals (retries,
upload URLs, account info storage) now belong there, and are kept here only when they still apply to the CLI.

## Compatibility and defaults

- Changing a default or removing behavior that scripts can rely on is a major-version change, even when the
  old behavior is dangerous. Ship a safer opt-in flag in the current major. (#370, #667)
- Do not document a bug as expected behavior; once documented it becomes part of the interface. (#667)
- Do not document a future mode or option before it exists; that commits the next release to it. (#667)
- New warnings on stderr can set off users' alerts. Deprecations go to stderr by default, so add them
  deliberately and keep old ApiVers quiet. (#1000)
- A change that makes a script exit non-zero where it used to exit 0 must say so in the release note.
  (#1165)
- Leave room in a new interface for the next obvious option (for example `--sourceFileId` now, so a name
  option can be added later without breaking it). (#586)

## Data safety

- Destructive operations must not be easy to trigger by accident: an empty sync source must not delete the
  destination (`--allow-empty-source`). (#370)
- Decide and test how filters interact with deletion: is an excluded file deleted at the destination?
  (#153)
- Never catch broad exceptions around code that can delete or reset user state; Ctrl+C during a slow read
  must not wipe account settings. Catch the specific exceptions. (#130)
- When a download fails, remove the partial local file, and test every destination type that inherits the
  behavior. (#533)
- Report unusable input clearly rather than silently skipping it; users expect all of their data to be
  synced. (#424, #1165)

## Tests

- Every new behavior and every bug fix needs a test; reviewers block on this more than on anything else.
  (#48, #153, #179, #227, #307, #332, #424, #470, #512)
- Test failure paths, not only the happy path, and assert the error message, not just that something was
  raised. (#295, #501, #411)
- When an assertion is removed, the function now accepts new input: add a test for that input. (#332)
- One command per test case, with a name that says what it checks, so a failure list reads on its own.
  (#89, #924)
- Do not copy a test to vary one input; extract a helper or parametrize. (#295)
- Cover each input branch: stdin `-` as well as a file, files and directories for filters, both sides of a
  boundary. (#1165, #407, #337)
- Know which layer you are testing: unit tests run against the simulator (`test/unit`), integration tests
  against live B2 (`test/integration`). Behavior both depend on needs both. (#424, #470, #512)
- The simulator can differ from B2 (for example hiding a file that does not exist). Check live behavior
  before trusting a simulator-only test. (#426)
- Use reserved domains (`example.com`) in tests, never real ones like `foo.com`. (#303)
- Use the existing test helpers (`_make_local_file`, `_run_command`) instead of new ad-hoc setup. (#512)
- Put a new test in the section it belongs to, not in an unrelated block. (#1163)
- Mixing forked and non-forked tests in one module breaks under pytest 8; keep them in separate modules.
  (#1054)

## Help text and documentation

- Every new option is documented in the command's docstring: what it does, its choices and its default.
  (#154, #375, #407, #758)
- Document cost and limits users would otherwise discover from support or their bill: listing a bucket
  incurs transactions; copies over 5 GB are not supported. (#423, #586)
- Say whether ranges are inclusive or exclusive; HTTP byte ranges are inclusive at the end. (#465, #534,
  #586)
- Write help for the user, not the implementation: "accepts a JSON string" rather than "formatted JSON".
  (#905, #407, #375)
- Keep developer-only knobs (`--environment`, `--dev`, `B2_ENVIRONMENT`) out of the main help and README;
  document them under `doc/`. (#1163)
- Keep the README in sync with the commands it describes, including paths that changed (profiles moved the
  account database). (#451, #1163)
- `b2 --help` should stay compact and readable. (#629)

## Output and errors

- `--quiet` means nothing on stdout, like `curl` and `wget`; it implies `--no-progress`. (#924, #929)
- An invalid combination of arguments is an error, not a silent fallback or a warning. (#244, #629)
- Error messages tell the user what to run next, using the binary name they invoked. (#1080, #398)
- Format exception messages in the exception class, not at the call site, and define exceptions at module
  level so callers can catch them. Group related exceptions under a common base class. (#202, #303, #459,
  #899)
- Never log authorization tokens or keys; log the URL that helps find the request server-side. (#287)
- When falling back (for example to ASCII when locales are not set), log it so `--verbose` shows why.
  (#768)

## B2 semantics

- Paginate list calls to the end (`nextFileName`, `nextApplicationKeyId`); a CLI user listing keys or files
  wants all of them. (#451)
- Stream large listings instead of building a list in memory; buckets can hold close to a billion files.
  (#423)
- B2 file name limits are in UTF-8 bytes, not characters. (#424)
- B2 names always use `/`; do not use `os.path` on them, which uses `\` on Windows. (#407)
- Hidden files still count toward bucket size; hide markers count as files with size 0. (#423, #426)
- Restricted application keys: give a clear error when the bucket on the command line is outside the key's
  restriction, and document the capabilities each command needs. (#459, #470, #471, #532)

## Python style

These are mostly enforced by ruff now; they still show up in review.

- No mutable default arguments; use `None` and create the value inside. (#139, #899)
- Compare with `is not None` when `None` is the sentinel; an object may define truthiness later. (#173)
- Do not shadow builtins (`file`, `id`, `dir`, `format`). (#83, #93, #139, #650)
- Prefer `@classmethod` to `@staticmethod`; make a value used in two places a constant. (#459, #346, #337)
- Trailing commas on multi-line literals keep diffs to one line. (#191, #412, #459, #512)
- Remove leftover debug prints, commented-out code and stale comments. (#103, #300, #337)
- Split long functions by responsibility (for example one signing function per OS). (#746)
- Use the database's own features (`INSERT OR IGNORE`) instead of reimplementing them. (#459)

## Shell, CI and release

- Quote every shell expansion (`"$var"`, `"$(...)"`). (#62, #398)
- Pin third-party GitHub Actions to a commit SHA. (#760)
- Make skipped CI steps say so (`else echo "skip"`), and do not fail PRs on optional services such as
  coverage upload. (#650)
- Check dependency changes early, not only at package build time (license check, version conflicts with
  b2sdk). (#650, #769)
- Pin around known-bad releases (for example `b2sdk>=1.22.1`), keep CLI and SDK version bounds
  compatible, and require the b2sdk version that ships a feature the CLI uses. (#897, #769, #800, #667)
- Release-tag regular expressions must match every PEP 440 pre-release form: `(a|b|rc|dev)[0-9]+$`.
  (#1108)
- Infrastructure-only changes the user cannot notice stay out of user-facing changelog sections. (#763)
- Credit outside contributors in the changelog entry. (#743)

## PR scope

- Keep unrelated fixes, refactors and formatter runs out of a feature PR; open a separate PR or an issue.
  (#370, #451, #459, #558)
- Explain the reason for a change in the PR description, especially for dependency, domain or config
  changes. (#212, #550, #714)
- File an issue for every follow-up deferred during review, so nothing is lost. (#650, #512)
- Merge current `master` before asking for review when CI failures may come from it. (#213, #436, #512)
