---
name: sdlc-spec
description: Turn an approved intent.md into a requirements-and-design spec.md with numbered requirements, a constraint-coverage table and flagged areas of concern, applying the organization's policy skills. Use when an intent.md has been accepted and needs a spec, or when asked to "write the spec", "do requirements and design", or "convert this intent into requirements".
---

# Requirements and design: intent.md to spec.md

Requirements and design happen in one session instead of two hand-offs. The product owner reviews the spec; they do not write it. The goal is a spec engineering can plan against, with every concern surfaced early.

## Before you start

- Locate `intent/<slug>/intent.md` and confirm it is approved (ask if unclear). No intent, run `sdlc-intent` first.
- List the policy skills available (`.claude/skills/`, plugins) for brand, security, compliance and UX, and apply every one that is relevant. If none exist, say so, because the spec will rest on stated constraints only.
- Read `CLAUDE.md` and the parts of the codebase the intent touches so the design integrates with what exists.

## Produce `intent/<slug>/spec.md`

```markdown
# Spec: <title>
From: intent.md (<author>). Status: ready for product owner review.
Skills applied: <skill names>.

## Requirements
- R1. <testable statement>
- R2. ...

## Design
<endpoints, components, data flow, where each piece lives in the existing system>

## How each constraint in the intent is met
| Constraint | Met by |
| :---- | :---- |
| <from intent> | <requirement or design element> |

## Open questions from the intent
- <question>. <answered here, or carried forward as concern N>

## Areas of concern
1. <concern, why it matters>. Owner: <policy owner>.
```

## Rules

- Number every requirement (R1, R2, ...). The plan and the review cite these numbers.
- Make requirements testable: observable behavior, a measurable limit, an exact field set.
- Every constraint in the intent must appear in the coverage table. If one cannot be met, it becomes a concern, not a silent omission.
- When two policies conflict, do not pick a winner. Describe the clash, propose a resolution, name the owner.
- Carry unanswered intent questions forward as concerns with an owner; never guess.
- Record which skills (and versions) were applied, so the audit trail shows the live policy was read.
- Do not write code in this stage.

## Hand-off

Tell the product owner to resolve flagged concerns with their policy owners first (these are the points an analyst would have escalated), then review the spec against the original idea. A human decides whether the spec and intent progress to build; ask a technical lead to weigh in for higher-risk changes. Commit `spec.md` next to `intent.md` once accepted. Next step: `sdlc-plan`.

## Measure

- Leading: elapsed time between the `intent.md` commit and the `spec.md` commit.
- Lagging: `spec.md` commits dated after the first `plan.md` commit for the same change (requirements rework).
