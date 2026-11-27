---
name: sdlc-subagents
description: Split a plan into independent streams for parallel Claude sessions in separate git worktrees, and define reusable subagents (verifier, simplifier, researcher) in .claude/agents/. Use when asked to "work on these in parallel", "set up worktrees", "create a verifier agent", or when one engineer has several independent tasks in flight.
---

# Parallel sessions and subagents

A **parallel session** is a full Claude Code instance on its own task in its own git worktree; sessions share nothing except the engineer steering them. A **subagent** is a scoped helper inside one session, with its own context window and tool limits, for jobs that recur across tasks. Sessions raise how many tasks are in flight; subagents keep each session focused. The engineer's job becomes steering and reviewing.

## Prerequisites

`CLAUDE.md` exists (every session reads it). A feedback loop helps, since sessions that can verify themselves need less supervision (`sdlc-feedback-loop`). Permission settings should be tuned so safe commands do not stall sessions on prompts.

## Split the work

1. Read `plan.md` (or the task list). Group steps by the files they touch.
2. Independent groups become parallel sessions. Tasks that share files run in one session, sequentially.
3. Start with two or three sessions. The ceiling is how many streams the engineer can review properly; add more only while review keeps up.
4. Give each its own worktree and branch:
   ```bash
   claude --worktree feature-auth
   claude --worktree fix-rate-limit
   ```
5. Give each session its slice of the plan and its own proof (tests or checks) so it can finish without the others.

## Define subagents

Markdown files in `.claude/agents/`, checked into Git so the team shares them. Each needs a name, a description of when to use it, and the minimal tools it may touch.

```markdown
---
name: verifier
description: Runs the app and checks the change works before the session reports done
tools: Bash, Read
---
Start the app with <run command>. Exercise the changed behavior and the two
nearest neighboring flows. Report what you ran, what you saw, and any
behavior that does not match plan.md. Do not fix anything; report only.
```

Useful starting set:

- **verifier**: fresh-context final check, so the verdict is not colored by the assumptions that produced the code. This complements, and does not replace, the feedback loop running throughout the task.
- **code-simplifier**: strips needless complexity after the main agent finishes.
- **researcher**: explores the codebase and reports back without flooding the main context.

## Governance

More sessions means more output, so controls must live in repo configuration (hooks, permission settings) that all sessions inherit. Activity is attributed to the engineer who ran it and exported via OpenTelemetry if configured.

## Measure

- Leading: concurrent sessions per engineer while review quality holds; share of the day spent steering vs waiting.
- Lagging: changes merged per engineer per week, read alongside the rework rate from PR history.
