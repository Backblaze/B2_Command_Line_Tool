# Reviewer evals

A set of real bugs, used to check that a reviewer (the AI review in `.github/workflows/ai-review.yml`, a
review agent, or `REVIEW.md` itself) catches what matters, without burying authors in noise. Rescore the
reviewer whenever `REVIEW.md`, `AGENTS.md`, the review prompt, its tools or its model change, and keep a
change only if the score does not drop. Catching real bugs comes first; fewer review rounds second.

## Cases

Each case is a bug that a human reviewer caught or that shipped and was fixed later. A maintainer confirms
each one (the bug and where it lived) before it joins the set. To run a case, check out the commit that
contains the bug (the PR head before the fix, or the parent of the fix), give the reviewer that diff, and
count it as caught only if a finding names the right `file:line` and the failure. Paths are given as they
are today; older cases lived in `b2/console_tool.py`.

| Case | Bug | Where | Commit with the bug | Source |
| --- | --- | --- | --- | --- |
| E1 | A helper returned `False` where callers unpack `found, altered`; the command fails with `TypeError` | `console_tool.py`, `ReplicationRuleChanger` | #802 head before review | Caught in review, #802 |
| E2 | `update-bucket` without `--lifecycle-rules` cleared the bucket's existing lifecycle rules | `console_tool.py`, `BucketUpdate` / `UpdateBucket` | parent of #925 | Shipped, fixed in #925 |
| E3 | `file upload --incremental-mode` on stdin or a pipe silently did a full upload and exited 0 | `console_tool.py`, `UploadFileMixin` | parent of #1165 | Shipped, issue #1164 |
| E4 | `list-keys` printed only the first page of keys | `console_tool.py`, key listing (`KeyListBase`) | #451 head before review | Caught in review, #451 |
| E5 | `sync` with an empty source and `--delete` deleted every file at the destination | `console_tool.py`, `Sync` | parent of #370 | Shipped, fixed in #370 |
| E6 | Developer-only realm settings (`B2_ENVIRONMENT`) were documented in the main `--help` | `console_tool.py`, README | #1163 head before review | Caught in review, #1163 |

Add a case for every revert and every post-release fix PR, in the same change as the fix when possible.
Bugs in library code (retries, upload URLs, account info storage) belong in
[b2-sdk-python](https://github.com/Backblaze/b2-sdk-python)'s set instead.

## Clean PRs

Recall is half the score. Also run the reviewer on known-clean PRs (merged, with no follow-up fix or
revert, for example docs-only and dependency-bump PRs) and count findings tagged Important that would have
led to a harmful or pointless change. That count should be zero.

## Noise on live PRs

For each finding the reviewer posts on a real PR, record how the author answered it: **fixed**, **wrong**
(false positive), **intentional**, or **didn't matter**. Use those words in the thread reply when resolving
(see [REVIEW.md](../REVIEW.md#answering-findings)) so they can be counted. A rising share of wrong and
didn't-matter answers means the reviewer needs tuning, even if recall is fine.

## Reporting

Record each scoring run as a row: date, what changed (prompt, model, `REVIEW.md` commit), cases caught out
of the total, Important findings on clean PRs, and median review rounds to pass from
`nox -s review_metrics`.

| Date | Change | Caught | Important on clean PRs | Median rounds |
| --- | --- | --- | --- | --- |
