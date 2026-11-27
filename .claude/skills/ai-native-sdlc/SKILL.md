---
name: ai-native-sdlc
description: Entry point for the AI-native SDLC workflow (intent.md -> spec.md -> plan.md -> build -> verify -> review -> release). Use when starting any new feature, change or incident, when asked "where do we start", "what's the next step", or "which play should we run", or when deciding which sdlc-* skill applies to the current state of a change.
---

# AI-native SDLC

Humans stay accountable for every judgment call. Claude does the drafting, checking and plumbing, and every stage leaves a committed Markdown artifact that the next stage consumes.

## The artifact chain

Each change gets a folder in the repo: `intent/<change-slug>/`.

| Artifact | Written by | Approved by | Skill |
|---|---|---|---|
| `intent.md` | Originator + Claude | Maintainer | `sdlc-intent` |
| `spec.md` | Claude, from the intent | Maintainer (area owners for flagged concerns) | `sdlc-spec` |
| `plan.md` | Claude in plan mode | Engineer (tech lead if higher risk) | `sdlc-plan` |
| code + tests | Claude, verified by its own feedback loop | Code owner at PR | `sdlc-feedback-loop`, `sdlc-pr-review` |
| release | Maintainer, following `RELEASE.md` | Code owner on the release PR | none |

## Pick the next step from what exists

Look in `intent/<slug>/` and the repo, then route:

1. No folder, only an idea or a ticket: `sdlc-intent`.
2. `intent.md` approved, no `spec.md`: `sdlc-spec`.
3. `spec.md` approved, no `plan.md`: `sdlc-plan`.
4. `plan.md` approved: build. Use `sdlc-feedback-loop` so the session verifies its own work, and `sdlc-subagents` to parallelise.
5. Diff ready: `sdlc-pr-review`.
6. Releasing: follow `RELEASE.md`.

Foundations that any stage leans on: `sdlc-claude-md` (shared context in `AGENTS.md`), `sdlc-policy-skill` (organization policy as skills).

## Prerequisites

`sdlc-spec` needs an approved `intent.md`; `sdlc-plan` works from a spec or, for small changes, a plain
description; `sdlc-subagents` and `sdlc-pr-review` lean on `AGENTS.md`, `REVIEW.md` and `nox -s ci`. If a
prerequisite is missing, say so and offer to run that skill first instead of working around it.

## Ground rules for every stage

- Put the artifact in the repo and name its author. Git history is the audit record.
- One source of truth per artifact. If Jira, ServiceNow or a requirements tool already holds the record, either declare it authoritative and write outcomes back through MCP, or link both ways (record ID in the file, commit SHA in the record).
- Advisory controls (skills, `CLAUDE.md`) get a deterministic backstop where the rule must always hold: a hook at the edit, a required check at the PR.
- A human makes each accept or reject decision that moves an artifact to the next stage. Never mark one approved on their behalf.
- Every play has a leading and a lagging indicator. Name them when you set the play up so the team can tell whether it worked.
