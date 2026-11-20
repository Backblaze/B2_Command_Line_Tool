---
name: sdlc-claude-md
description: Create, trim or audit a repo's CLAUDE.md so it holds exactly what a new joiner needs on day one (commands, conventions, architecture, recurring mistakes, verification). Use when a repo has no CLAUDE.md, when it has grown stale or long, when Claude repeats a mistake, or when asked to "set up CLAUDE.md" or "update our CLAUDE.md".
---

# The CLAUDE.md

`CLAUDE.md` is the context a new joiner would need, read by the agent at the start of every session. It is version controlled and reviewed like code. Keep it under a page: Claude reads all of it every session, and stale lines waste context.

## Create

1. Survey the repo: build files, `Makefile`/package scripts, CI config, README, directory layout, test setup.
2. Draft with `/init` if available, or by hand, then cut it down to day-one essentials.
3. Verify every command by running it (or asking the engineer). Never list a command you have not seen work, and note a healthy output for each.
4. Ask the engineer who knows the codebase best for the "things Claude gets wrong" list; pull more from recent PR review comments and reverted changes.
5. Place it at the repo root and commit, with code owners set so changes get reviewed.

## Template

```markdown
# <Project>

## Commands
- Build: <cmd>
- Test: <cmd> (unit), <cmd> (integration, needs ...)
- Lint: <cmd> (runs in CI; fix before pushing)
- Run: <cmd> (<what comes up, on which ports>)

## Conventions
- <language/framework versions>
- <rules that matter: every endpoint needs a test, dates are ISO 8601, ...>

## Architecture
- <directory map, which component talks to which, hard limits (rate limits, ownership)>

## Things Claude gets wrong
- <each mistake the team has corrected twice>

## Verifying your work
- Build: <cmd> (must finish with "<healthy output>")
- Test: <cmd> (all green; never skip or delete a failing test)
- Lint: <cmd> (zero warnings)
Run all three before reporting any task complete, and paste the output.
If a test fails, fix the code, not the test.
```

If artifacts live in `intent/`, add a line saying `intent/` holds `intent.md`, `spec.md` and `plan.md` per change.

## Audit an existing file

- Remove anything stale, obvious from the code, or already enforced by CI or a hook.
- Move multi-step procedures into a skill (`sdlc-policy-skill`); move must-always-hold rules behind a hook (`.claude/settings.json`) or a CI check.
- Flag missing sections, unverifiable commands, and contradictions with `README` or CI.
- Report the diff and why; do not silently drop lines someone may rely on.

## Maintenance rule

When Claude makes the same mistake twice, the correction goes into `CLAUDE.md` in the same PR. PR review should also flag changes that make `CLAUDE.md` outdated (`sdlc-pr-review`). Trimming changes agent behavior, so watch the next few PRs for regressions.

## Measure

- Leading: how often Claude repeats a mistake `CLAUDE.md` should have caught (track corrections in Git history).
- Lagging: time to first merged PR for a new team member.
