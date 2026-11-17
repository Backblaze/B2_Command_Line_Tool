#!/usr/bin/env bash
set -euo pipefail

tag=${1:?Pass the release tag name}
[[ "$tag" == v* ]] || { echo 'Release tag must start with v.' >&2; exit 1; }
allowed=release-keys/maintainers.allowed_signers
[[ -s "$allowed" ]] || { echo 'Maintainer tag-signing keys are unavailable.' >&2; exit 1; }

git -c gpg.format=ssh -c "gpg.ssh.allowedSignersFile=$allowed" verify-tag "$tag"
commit=$(git rev-parse "$tag^{commit}")
if [[ -n "${GITHUB_SHA:-}" && "$commit" != "$GITHUB_SHA" ]]; then
  echo 'Release tag does not point to the triggering commit.' >&2
  exit 1
fi
git show-ref --verify --quiet refs/remotes/origin/master || {
  echo 'The master ref is unavailable for release-tag verification.' >&2
  exit 1
}
git merge-base --is-ancestor "$commit" origin/master || {
  echo 'Release tag must point to a commit on master.' >&2
  exit 1
}
echo "Verified release tag $tag."
