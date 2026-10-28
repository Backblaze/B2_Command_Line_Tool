Summary:

## Motivation

Why this change is needed. Link the issue (`Fixes #123`) and, if one exists, `intent/<slug>/`.

## Changes

What changed, and which ApiVers (`b2`, `b2v3`, `b2v4`, `b2v5`) see it. If this PR is stacked on another,
name it and the merge order.

## Testing

Paste the tail of `nox -s ci` and the output of the changed command run end to end. For each behavior
this PR adds or fixes, name the test that fails when the change is reverted. List the live-B2
integration tests you ran, and mark the ones you could not as NOT RUN.

## Risk

What could break for existing scripts: flags, output, exit codes, stderr. Name the riskiest part of the
diff.

## How to revert

Whether reverting this commit is enough, or whether users would need to pin the previous release.

## Checklist

- [ ] `nox -s ci` passes locally.
- [ ] A fresh-context review (`/code-review`, `/security-review` or a subagent) ran on the diff; findings are fixed or answered below.
- [ ] A `changelog.d` fragment exists with the right type, or a maintainer added the `-changelog` label.
- [ ] New or changed behavior is pinned by a test in every ApiVer it affects.
- [ ] Help text, README usage and `AGENTS.md` are updated if needed.
- [ ] The description matches the final code.
