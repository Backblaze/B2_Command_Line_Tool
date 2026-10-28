---
name: sdlc-intent
description: Turn an idea, ticket, pain point or incident into a committed intent.md proto-spec by interviewing the originator. Use when someone describes a problem or wants a new feature and there is no intent.md yet, when asked to "capture intent", "write up this idea", or "start a new change".
---

# Capture as intent.md

`intent.md` is a short proto-spec in the originator's own terms: what is wanted, why, and under which constraints. It is human readable and machine actionable, and it kicks off the rest of the chain.

## Steps

1. Let the originator describe the problem in their own words. No formal language needed. If the source is a ticket or incident, read it (via MCP or the pasted text) and start from there.
2. Interview until the idea is concrete, asking what an analyst would: who is affected, what they cannot do today, what better looks like, what is out of scope, which systems are touched, which constraints apply (security, compliance, existing auth), how success is observed. Ask a few questions at a time, not a form.
3. Check for an agreed template (`intent/TEMPLATE.md` or an organization skill) and use it. Otherwise use the template below.
4. Write the draft to `intent/<change-slug>/intent.md` (kebab-case slug, create the folder).
5. Read it back and have the originator correct anything misunderstood. Do not invent answers: unresolved items go under Open questions.
6. Commit when the originator is satisfied (follow the repo's commit conventions). The product owner picks it up from there and decides accept or close.

## Template

```markdown
# Intent: <short title>
Author: <name, team>. Status: draft.

## Problem
<what cannot be done today, who feels it, evidence>

## Proposed outcome
<what better looks like, in the originator's words>

## Affected users and systems
<people, services, data>

## Constraints
<security, compliance, auth, performance, deadlines>

## Open questions
<anything unanswered, with who might know>
```

## Rules

- Stay in the problem space. No design, no implementation, no file names.
- If the originator is not a Git user, commit through the version-control connector on their behalf and record them as author in the header.
- Track an incident-sourced intent the same way, and say in the header that it came from monitoring (`Author: monitoring agent`) so the product owner knows to review harder.

## Measure

- Leading: time from first conversation to a committed `intent.md` (Git timestamps).
- Lagging: share of intents the product owner accepts into design instead of closing, and how many edits `intent.md` receives after the first `spec.md` commit.

Next step once accepted: `sdlc-spec`.
