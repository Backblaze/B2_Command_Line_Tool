---
name: sdlc-policy-skill
description: Write a Claude skill that encodes an organization policy or standard (security, API design, brand, compliance, UX), with a trigger test and a deterministic backstop. Use when asked to "turn this policy into a skill", "encode our standard", "make Claude follow our X guidelines", or when a rule is enforced inconsistently today.
---

# Skills as institutional knowledge

A skill makes institutional knowledge operational: explicit, version controlled, applied broadly, updated centrally. It is an advisory control. Write one for knowledge that must be applied consistently; keep general project context in `CLAUDE.md` and one-off instructions in a prompt.

## Decide where it belongs

| Knowledge | Home |
|---|---|
| Conventions every session needs (commands, layout) | `CLAUDE.md` |
| A policy with an owner, applied when a certain task happens | **a skill** |
| A rule that must hold with no exceptions | skill + hook (+ required CI check) |
| A recurring job needing its own context and tools | subagent (`sdlc-subagents`) |

## Steps

1. Pick one policy that is enforced inconsistently today. Get its written source of truth and its named owner.
2. Create `.claude/skills/<name>/SKILL.md` (ships with the repo) or put it in a plugin for organization-wide distribution.
3. Frontmatter: `name`, and a `description` that says what the skill does and exactly when to trigger (task types, file types, verbs users would say). The description decides whether it ever loads.
4. Body: numbered, testable rules in imperative form; cite the policy section each rule comes from; name any script to run and require its output in the summary.
5. Test triggering: ask Claude for the relevant task in 4-5 different phrasings, including ones that never mention the policy, and confirm the skill loads each time. Tighten the description until it does.
6. Have the policy owner sign off. Policy changes become skill changes, reviewed like code; engineers pick up the new version in their next session.
7. Back it up (below).

## Template

```markdown
---
name: <kebab-name>
description: <What it enforces>. Use whenever <task types and triggers>.
---
# <Title>
When you <do the task>:
1. <Rule>. (Policy §x.y)
2. <Rule>.
Run <script> and include its output in your summary.
```

## Add a deterministic backstop for must-hold rules

- **Hook at the edit**: a `PostToolUse` hook (matcher `Edit|Write`) that runs the same script the skill names, only for the files that changed. Exit code 2 feeds the script's message back to Claude so it fixes the line. Keep build-phase hooks fast and scoped; heavy checks belong at commit or PR.
- **Required check at the PR**: run the same script in CI as a required status check. It also catches changes that bypassed the hook (a shell command rewriting a file, a different tool).
- A hook that asks a human for approval is a gate, not a guardrail; keep those at release, not mid-build.

## Measure

- Leading: time from policy owner approving a change to the updated skill merging.
- Lagging: PR review findings that cite the policy. They should trend to zero; if not, the skill is not triggering or has drifted from the official text.
