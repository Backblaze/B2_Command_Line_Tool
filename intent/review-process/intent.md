# Intent: review and merge process scaled by risk
Author: Gonzalo Peña-Castellanos (with Claude). Issue: #1192. Status: draft, for the maintainers.
Source: [How the Claude Code team does code review](https://claude.dev/blog/solving-code-review-at-anthropic/),
applied to the B2 CLI; the same proposal as Backblaze/blazer#152, adapted.

## Problem

More B2 CLI PRs are now written with coding agents. Reviewers repeatedly ask for the same things
(changelog fragments, fragment types, tests per input branch, stdout/stderr discipline), and a PR's
live-B2 integration jobs can fail for reasons unrelated to it.

Today:

- Every PR gets the same human attention whatever its risk; a docs fix and a change to `rm`, `sync` or
  credential handling are treated alike.
- There is no `.github/CODEOWNERS`, so nothing routes a change to the person who knows that area.
- Which branch protection applies to `master` could not be confirmed without admin access.
- Review findings can be left unanswered at merge.
- Nothing measures whether a reviewer change (REVIEW.md, AGENTS.md, model) catches more or fewer real
  bugs.

The PR that adds this file already covers what needs no settings change: a short `AGENTS.md`/`CLAUDE.md`,
a formatter hook, `REVIEW.md` (failure modes, review depth by path, paths to skip, severities, verified
findings with `file:line` evidence, a count-first summary, round caps, what people still review), Risk and
How-to-revert in the PR template, `/code-review`, `/simplify` and `/security-review` before a PR opens, an
advisory AI code review and security review workflow (`.github/workflows/ai-review.yml`, inactive until the
secret is set), a seed eval set (`doc/reviewer-evals.md`), and a metrics report (`nox -s review_metrics`).
On the last 120 days it reports 24 merged PRs, a median of 0 human approvals per PR (p95 1), and a median
of 5.8 hours (p95 147) to the first human review.

## Proposed outcome

Human review time goes where the risk is, with no rise in reverts or post-release fixes:

1. **Risk tiers set by the change, not the author.** A first proposal to accept or change:

   | Tier | Paths | Human approvals |
   | --- | --- | --- |
   | Low | `*.md` docs, `doc/`, `test/` only, `changelog.d/`, `intent/` | None once CI and automated review are green, for authors on the trusted list |
   | Medium | `b2/_internal/` (except below), `.sdkharness/`, `pyproject.toml`/`uv.lock` b2sdk bumps | One maintainer |
   | High | Destructive commands (`rm`, `sync`, `delete-*`), authorization and account info, `b2v*/registry.py` (ApiVer promises), `.github/workflows/`, `noxfile.py` release/bundle/sign sessions, release commits | One maintainer always; two for workflow and signing changes |

   Authors outside the trusted list always need a person. A bot's approval never fills a human slot.
   A check (a path classifier first, an AI classifier later) sets the tier from the diff, and a second
   check can still ask for a person when a Low change looks risky. Proposed `.github/CODEOWNERS`, with
   owners to be filled in by the maintainers:

   ```text
   *                                    @Backblaze/<cli-maintainers>
   /.github/workflows/                  @Backblaze/<cli-maintainers> @Backblaze/<security>
   /noxfile.py                          @Backblaze/<cli-maintainers>
   /b2/_internal/b2v*/registry.py       @Backblaze/<cli-maintainers>
   /SECURITY.md                         @Backblaze/<security>
   ```
2. **Approvals cover one commit.** New commits pushed after an approval (agent pushes, hand-resolved
   merges) need re-approval for Medium and High tiers ("dismiss stale approvals" plus "require approval of
   the most recent push").
3. **Every review thread resolved before merge** (GitHub "require conversation resolution").
4. **Automated AI review and security review on every PR**, with open Important findings blocking merge.
   `ai-review.yml` is in place; a maintainer adds the `ANTHROPIC_API_KEY` secret and installs the Claude
   GitHub App to turn it on. Start
   advisory, tune REVIEW.md for a few weeks, then decide on blocking and on the review depth (Deep for the
   High-tier paths). Decide whether it replaces or runs beside Copilot review.
5. **Merge queue**, so checks rerun on `master` plus the change, and two PRs that pass alone cannot
   combine into a broken `master`. The live-B2 jobs make each queue run slow; decide whether the queue
   runs them or only the offline jobs.
6. **Past bugs become reviewer evals.** `doc/reviewer-evals.md` seeds the set with six cases (for
   example #802, a tuple-unpack crash caught in review; #925, `update-bucket` clearing lifecycle rules;
   #1164, incremental mode silently ignored on stdin). Maintainers confirm each case; every revert and
   post-release fix adds one. Score the reviewer whenever REVIEW.md, AGENTS.md, the prompt or the model
   changes, also on known-clean PRs, and classify each live finding as fixed, wrong, intentional or
   didn't matter to measure noise.
7. **Measure it.** Human approvals per PR (median, p95), human comments per PR, review rounds to pass,
   time a PR waits on a person (`nox -s review_metrics`); reverts and post-release fix PRs as the
   guardrail.
8. **Circuit breakers.** If reverts or post-release fixes rise over a month, tighten the tiers
   automatically (Low needs one person again) until a maintainer reviews why.
9. **Nightly review of existing code.** A scheduled run of the AI reviewer over the destructive commands
   and credential handling finds old bugs before users do; findings become issues, not PR comments.
10. **Staged releases and incident reviews.** Use pre-release tags (`vX.Y.ZrcN`, binaries and GitHub
    release only) for risky behavior before PyPI, keep one-step rollback (yank), and after each
    post-release fix ask which layer should have caught it (see `RELEASE.md`).

## Affected users and systems

- Contributors and agents opening PRs; the maintainers.
- Repo settings: branch protection or rulesets on `master`, merge queue, conversation resolution,
  a new `CODEOWNERS`.
- Workflows: `ci.yml`, `ai-review.yml`, possibly a risk-tier check and a nightly review.
- CLI users only indirectly: fewer regressions and faster fixes.

## Constraints

- Settings changes need a repo admin; this intent proposes them, it does not apply them.
- Separation of duties: the agent that wrote a change never approves it, and the person directing it
  counts as one reviewer.
- Live-B2 secrets are only available to trusted runs; external contributors' PRs run the offline jobs.
- Start strict and loosen with evidence; any tier can be tightened if reverts rise.
- Cost of running an AI reviewer per push in CI.

## Open questions

- Who are the code owners, per area, and who is on the trusted-author list for the Low tier?
- Do the tier paths above match where the risk is? Which paths always need two people?
- Should Dependabot PRs keep running the full matrix with live-B2 secrets, or be gated like other
  Backblaze repos?
- Is a merge queue available on this org's plan, and who enables it? (repo admin)
- Where should the eval set live and run: this repo, or the central SDK harness?
- Who owns the `ANTHROPIC_API_KEY` secret, the Claude GitHub App installation and the review budget, and which model and depth should the
  review use?
- What revert or post-release-fix rate trips the circuit breaker?
