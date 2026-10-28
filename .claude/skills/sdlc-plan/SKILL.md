---
name: sdlc-plan
description: Produce and commit a plan.md before any code is written, working in plan mode from the approved spec.md and intent.md. Use when starting implementation of a change, when asked to "plan this", "make an implementation plan", or before editing code for a change that has an intent/spec; also use to keep plan.md in sync when implementation departs from it.
---

# Plan mode as the default starting point

Work starts with a written, reviewable plan. Plan mode lets Claude read the codebase without changing anything, so course corrections are edits to a document.

## Steps

1. Work in plan mode (Shift+Tab, or start the session with `--permission-mode plan`). Do not edit files until the engineer accepts the plan.
2. Read `intent/<slug>/intent.md`, `spec.md` (the requirement numbers R1.. and concerns), `CLAUDE.md` and the code the change touches. If a spec exists, a plan without it is a smell; if neither exists, ask whether to run `sdlc-intent` / `sdlc-spec` first or to plan from a plain description.
3. Draft the plan using the template below.
4. Let the engineer interrogate it. Be ready to answer: what could this break, which step is riskiest, which alternatives were rejected and why.
5. Iterate until someone who never saw the conversation could implement the change from the plan alone.
6. Commit the approved plan as `intent/<slug>/plan.md`. Then implement; with a solid plan this is often one pass.
7. If implementation departs from the plan, update `plan.md` in the same commit. Consider a hook that flags source changes with no matching plan update.

## Template

```markdown
# Plan: <title> (from intent.md and spec.md)

## Files that change
<paths, marking new files>

## Order of work
1. <step> (R1, R2).
2. <step> (R4).

## Risks
<what could break; cite spec concerns by number>

## Proof
<which tests or checks cover which requirements, and how UI changes are verified>
```

## Rules

- Cite requirement numbers on every step and every proof line so a reviewer can check the plan against the spec line by line.
- The Proof section must be executable by Claude (tests, build, screenshot compare); see `sdlc-feedback-loop`.
- Independent steps (different files) can run in parallel sessions; mark them so `sdlc-subagents` can split the work.
- Plans for changes the organization classes as higher risk go to a tech lead or architect before acceptance.
- Existing records (Jira, requirements tools) stay authoritative when the organization says so: note the record ID in the plan header, and write outcomes back through the connector. Otherwise link both ways.

## Auto mode

Once guardrails mature (a tuned `CLAUDE.md`, policy skills, blocking hooks, a test suite Claude can run), routine work with a tight spec and a small blast radius can run in auto mode after the plan is approved. The plan is still reviewed first.

## Measure

- Leading: share of changes that merge from the first implementation pass; time from plan approval to merged PR.
- Lagging: rework cycles per change; how often the merged diff still matches `plan.md`.
