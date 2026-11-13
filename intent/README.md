# Change intents

Each non-trivial change gets a folder `intent/<change-slug>/` holding three files, written in order. A
human approves each one before the next is written; the approval is a commit or a PR review, not an
agent's edit to the status line.

| File | Written by | Approved by | Holds |
| --- | --- | --- | --- |
| `intent.md` | Originator (with Claude) | Maintainer | Problem, outcome, constraints, open questions. No design. |
| `spec.md` | Claude, from the intent | Maintainer; area owner for flagged concerns | Numbered requirements R1.., design, constraint table, concerns. |
| `plan.md` | Claude in plan mode | Engineer doing the work | Files, ordered steps citing R numbers, risks, proof. |

Small fixes that already have a GitHub issue with a clear reproduction can skip `intent/` and start from a
failing test. Link the GitHub issue in the `intent.md` header when one exists, and the intent folder in the
issue.

The `ai-native-sdlc` skill in `.claude/skills/` looks at what exists and routes to the next step.

## Templates

### intent.md

```markdown
# Intent: <short title>
Author: <name>. Issue: #<n>. Status: draft.

## Problem
## Proposed outcome
## Affected users and APIs
## Constraints
## Open questions
```

### spec.md

```markdown
# Spec: <title>
From: intent.md (<author>). Status: ready for review.

## Requirements
- R1. <testable statement: command, flags, output, exit code>

## Design
<which commands and ApiVers change, new flags, output shape, b2sdk calls>

## How each constraint is met
| Constraint | Met by |
| --- | --- |

## Areas of concern
1. <concern>. Owner: <@handle>.
```

### plan.md

```markdown
# Plan: <title>

## Files that change
## Order of work
1. <step> (R1).
## Risks
## Proof
- R1: <test name, and whether it fails when the change is reverted>
```
