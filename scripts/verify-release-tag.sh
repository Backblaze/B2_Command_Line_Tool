#!/usr/bin/env bash
set -euo pipefail

tag=${1:?Pass the release tag name}
[[ "$tag" == v* ]] || { echo 'Release tag must start with v.' >&2; exit 1; }
git show-ref --verify --quiet refs/remotes/origin/master || {
  echo 'The master ref is unavailable for release-tag verification.' >&2
  exit 1
}
allowed=$(mktemp)
trap 'rm -f "$allowed"' EXIT
git show origin/master:release-keys/maintainers.allowed_signers > "$allowed" || {
  echo 'Current maintainer tag-signing keys are unavailable.' >&2
  exit 1
}
[[ -s "$allowed" ]] || { echo 'Maintainer tag-signing keys are unavailable.' >&2; exit 1; }

tag_object=$(git cat-file tag "$tag" 2>/dev/null) || {
  echo 'Release tag must be an annotated signed tag.' >&2
  exit 1
}
signed_name=$(printf '%s\n' "$tag_object" | awk '
  /^$/ { header_done = 1 }
  !header_done && /^tag / { print substr($0, 5) }
')
[[ "$signed_name" == "$tag" ]] || {
  echo 'Release tag name does not match its signed tag header.' >&2
  exit 1
}

git -c gpg.format=ssh -c "gpg.ssh.allowedSignersFile=$allowed" verify-tag "$tag"
commit=$(git rev-parse "$tag^{commit}")
if [[ -n "${GITHUB_SHA:-}" && "$commit" != "$GITHUB_SHA" ]]; then
  echo 'Release tag does not point to the triggering commit.' >&2
  exit 1
fi
git merge-base --is-ancestor "$commit" origin/master || {
  echo 'Release tag must point to a commit on master.' >&2
  exit 1
}
echo "Verified release tag $tag."
