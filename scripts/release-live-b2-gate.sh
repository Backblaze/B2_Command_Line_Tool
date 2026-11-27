#!/usr/bin/env bash
# Hold a release unless the latest finished Weekly Live B2 Integration run on master passed
# within LIVE_B2_MAX_AGE_DAYS (default 8: one weekly run plus a day of slack). Cancelled and
# skipped runs are ignored, and only scheduled or dispatched runs of this repository whose commit
# is on origin/master count. To unblock a release, dispatch that workflow on master and wait for
# it to pass. Run from a checkout that has fetched origin/master. Needs GH_TOKEN with actions: read.
set -euo pipefail

max_age_days="${LIVE_B2_MAX_AGE_DAYS:-8}"
now="${LIVE_B2_GATE_NOW:-$(date +%s)}"

runs="$(gh api "repos/$GITHUB_REPOSITORY/actions/workflows/live-b2.yml/runs?branch=master&status=completed&per_page=50")"
# The branch filter matches any ref named master, including a tag, and that ref can carry a
# modified live-b2.yml. Count only scheduled or dispatched runs of this repository, and below,
# only a run whose commit is on origin/master (fetched by the release-gate job).
latest="$(jq -c --arg repository "$GITHUB_REPOSITORY" '[.workflow_runs[]
  | select(.event == "schedule" or .event == "workflow_dispatch")
  | select(.head_repository.full_name == $repository)
  | select(.conclusion != "cancelled" and .conclusion != "skipped")][0] // empty' <<<"$runs")"

if [ -z "$latest" ]; then
  echo "HOLD: no finished Weekly Live B2 Integration run on master. Dispatch it on master and wait for it to pass." >&2
  exit 1
fi

conclusion="$(jq -r '.conclusion' <<<"$latest")"
url="$(jq -r '.html_url' <<<"$latest")"
age_seconds="$(jq -r --argjson now "$now" '$now - (.run_started_at | fromdateiso8601)' <<<"$latest")"

if [ "$conclusion" != success ]; then
  echo "HOLD: the latest live B2 run on master ended with $conclusion: $url" >&2
  exit 1
fi
head_sha="$(jq -r '.head_sha' <<<"$latest")"
if ! git merge-base --is-ancestor "$head_sha" refs/remotes/origin/master 2>/dev/null; then
  echo "HOLD: the latest green live B2 run tested $head_sha, which is not on origin/master: $url" >&2
  exit 1
fi
if [ "$age_seconds" -gt "$((max_age_days * 86400))" ]; then
  echo "HOLD: the latest green live B2 run on master is older than $max_age_days days: $url" >&2
  exit 1
fi
echo "CLEAR: live B2 run $url passed $((age_seconds / 3600)) hours ago."
