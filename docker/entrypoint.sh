#!/usr/bin/bash
set -euo pipefail

if [[ "$1" =~ ^_?b2(v[0-9]+)?$ ]]; then
  B2_COMMAND="$1"
  shift
else
  B2_COMMAND="b2"
fi

exec "$B2_COMMAND" "$@"
