#!/usr/bin/env bash
set -euo pipefail

version=${1:?Pass the release version without v}
awk -v heading="## [$version]" '
  index($0, heading) == 1 { found = 1; next }
  found && /^## \[/ { exit }
  found { print }
  END { if (!found) exit 1 }
' CHANGELOG.md
