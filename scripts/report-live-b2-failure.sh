#!/usr/bin/env bash
# Open one GitHub issue for a failed scheduled live B2 run, or comment on it if it is still open.
# Needs GH_TOKEN with issues: write, and the GITHUB_* variables that Actions sets.
set -euo pipefail

title='Weekly live B2 integration failed'
run_url="$GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID"
body="The scheduled \`Weekly Live B2 Integration\` run failed: $run_url

Find the cause: expired or revoked live credentials, a B2 service problem, or a CLI regression.
Releases wait for a green run of this workflow on master (\`release-gate\` in \`cd.yml\`).
Close this issue once a run passes."

issues="$(gh issue list --repo "$GITHUB_REPOSITORY" --state open --search "\"$title\" in:title" \
  --json number,title --limit 100)"
number="$(jq -r --arg title "$title" '[.[] | select(.title == $title)][0].number // empty' <<<"$issues")"

if [ -n "$number" ]; then
  gh issue comment "$number" --repo "$GITHUB_REPOSITORY" --body "$body"
else
  gh issue create --repo "$GITHUB_REPOSITORY" --title "$title" --body "$body"
fi
