#!/usr/bin/env bash
# Check open release findings before any publishing job starts.
set -euo pipefail

repo=${GITHUB_REPOSITORY:-Backblaze/B2_Command_Line_Tool}
strict=${SECURITY_GATE_STRICT:-false}
hold=0
unverified=0

for tool in gh jq; do
  command -v "$tool" >/dev/null || { echo "HOLD: $tool is required for the release gate." >&2; exit 1; }
done

check_source() {
  local source=$1 endpoint=$2 alerts count
  if ! alerts=$(gh api --paginate "repos/$repo/$endpoint" --jq '.[] | @json' 2>/dev/null); then
    echo "UNVERIFIED: $source alerts could not be read. Check the repository Security tab."
    unverified=1
    return
  fi
  if ! count=$(printf '%s\n' "$alerts" | jq -s --arg source "$source" --arg fixes "${RELEASE_FIXES_ADVISORIES:-}" '
    def severe($value): ($value // "" | ascii_downcase) as $level
      | (["medium", "high", "critical"] | index($level)) != null;
    [ .[] | select(
        if $source == "dependabot" then severe(.security_advisory.severity)
        elif $source == "code-scanning" then severe(.rule.security_severity_level)
        elif $source == "secret-scanning" then true
        else (.state == "triage" or .state == "draft") and severe(.severity)
          and (.state != "draft" or (.ghsa_id as $id | ($fixes | split(",") | index($id)) == null))
        end
      ) ] | length
  '); then
    echo "HOLD: $source alerts could not be evaluated." >&2
    hold=1
    return
  fi
  if (( count > 0 )); then
    echo "HOLD: $count open $source finding(s) meet the release threshold."
    hold=1
  else
    echo "CLEAR: $source"
  fi
}

alerts_token=${RELEASE_ALERTS_TOKEN:-${GH_TOKEN:-}}
GH_TOKEN="$alerts_token" check_source dependabot 'dependabot/alerts?state=open&per_page=100'
GH_TOKEN="$alerts_token" check_source code-scanning 'code-scanning/alerts?state=open&per_page=100'
GH_TOKEN="$alerts_token" check_source secret-scanning 'secret-scanning/alerts?state=open&per_page=100'
advisory_visibility_verified() {
  local metadata scopes
  # Public advisory responses alone do not prove access to unpublished findings.
  metadata=$(gh api --include "repos/$repo" 2>/dev/null) || return 1
  scopes=$(printf '%s\n' "$metadata" | awk 'tolower($0) ~ /^x-oauth-scopes:/ {sub(/^[^:]*: */, ""); print}' | tr -d '\r')
  case ",${scopes// /}," in
    *,repo,*|*,repository_advisories:read,*|*,repository_advisories:write,*) ;;
    *) return 1 ;;
  esac
  printf '%s\n' "$metadata" | awk 'body {print} /^\r?$/ {body=1}' | jq -e '.permissions.admin == true' >/dev/null
}

if [[ "${GITHUB_ACTIONS:-false}" == true && -z "${RELEASE_SECURITY_TOKEN:-}" ]]; then
  echo 'UNVERIFIED: security-advisories requires RELEASE_SECURITY_TOKEN with repository-advisory read access.'
  unverified=1
elif ! GH_TOKEN="${RELEASE_SECURITY_TOKEN:-${GH_TOKEN:-}}" advisory_visibility_verified; then
  echo 'UNVERIFIED: security-advisories visibility could not be confirmed for unpublished findings.'
  unverified=1
else
  GH_TOKEN="${RELEASE_SECURITY_TOKEN:-${GH_TOKEN:-}}" \
    check_source security-advisories 'security-advisories?per_page=100'
fi

if (( unverified > 0 )); then
  echo '::warning title=Release security gate::UNVERIFIED sources require manual Security tab review.'
  if [[ "$strict" == 'true' ]]; then
    echo 'HOLD: SECURITY_GATE_STRICT requires every source to be readable.'
    hold=1
  fi
fi
if (( hold > 0 )); then
  echo 'RELEASE ON HOLD.'
  exit 1
fi
echo 'Release security gate found no blocking readable-source findings.'
