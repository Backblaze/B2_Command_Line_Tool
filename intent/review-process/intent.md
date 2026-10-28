# Intent: review and merge process scaled by risk
Author: Gonzalo Peña-Castellanos (with Claude). Issue: #1192. Status: draft, for the maintainers.
Source: [How the Claude Code team does code review](https://claude.dev/blog/solving-code-review-at-anthropic/),
applied to the B2 CLI. Same proposal as Backblaze/blazer#152, adapted.

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

The PR that adds this file already covers what needs no settings change: REVIEW.md severities, evidence
and convergence rules, Risk and How-to-revert in the PR template, and a fresh-context self-review before
a PR opens.

## Proposed outcome

Human review time goes where the risk is, with no rise in reverts or post-release fixes:

1. **Risk tiers set by the change, not the author.** A first proposal to accept or change:

   | Tier | Paths | Human approvals |
   | --- | --- | --- |
   | Low | `*.md` docs, `doc/`, `test/` only, `changelog.d/`, `intent/` | None once CI and automated review are green, for authors on the trusted list |
   | Medium | `b2/_internal/` (except below), `.sdkharness/`, `pyproject.toml`/`uv.lock` b2sdk bumps | One maintainer |
   | High | Destructive commands (`rm`, `sync`, `delete-*`), authorization and account info, `b2v*/registry.py` (ApiVer promises), `.github/workflows/`, `noxfile.py` release/bundle/sign sessions, release commits | One maintainer always; two for workflow and signing changes |

   Authors outside the trusted list always need a person. A bot's approval never fills a human slot.
2. **Approvals cover one commit.** New commits pushed after an approval (agent pushes, hand-resolved
   merges) need re-approval for Medium and High tiers.
3. **Every review thread resolved before merge** (GitHub "require conversation resolution").
4. **Automated AI review and security review on every PR**, with open Important findings blocking merge.
   Start advisory, tune REVIEW.md for a few weeks, then decide on blocking.
5. **Merge queue**, so checks rerun on `master` plus the change, and two PRs that pass alone cannot
   combine into a broken `master`. The live-B2 jobs make each queue run slow; decide whether the queue
   runs them or only the offline jobs.
6. **Past bugs become reviewer evals.** Build a set from bugs reviewers caught or that shipped, confirmed
   by a maintainer (for example #1164, incremental mode with stdin; #1163, developer knobs in the main
   help; #1165, exit codes changed without a release note). Score the reviewer against the set whenever
   REVIEW.md, AGENTS.md or the model changes.
7. **Measure it.** Human approvals per PR (median, p95), review rounds to pass, time a PR waits on a
   person; reverts and post-release fix PRs as the guardrail.

## Affected users and systems

- Contributors and agents opening PRs; the maintainers.
- Repo settings: branch protection or rulesets on `master`, merge queue, conversation resolution,
  a new `CODEOWNERS`.
- Workflows: `ci.yml`, possibly a review workflow and a risk-tier check.
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
