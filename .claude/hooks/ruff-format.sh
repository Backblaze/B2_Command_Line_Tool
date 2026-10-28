#!/usr/bin/env bash
# PostToolUse: reject an edit that leaves a Python file unformatted; exit 2 hands the message back to Claude.
set -euo pipefail
command -v jq > /dev/null || { echo "ruff hook: jq not found, skipping (ruff check still runs in nox -s lint)." >&2; exit 0; }
file=$(jq -r '.tool_input.file_path // empty')
case "$file" in
  *.py) ;;
  *) exit 0 ;;
esac
[ -f "$file" ] || exit 0
case "$file" in
  "${CLAUDE_PROJECT_DIR:-$PWD}"/*) ;;
  *) exit 0 ;;
esac
if [ -x "${CLAUDE_PROJECT_DIR:-.}/.venv/bin/ruff" ]; then
  ruff=("${CLAUDE_PROJECT_DIR:-.}/.venv/bin/ruff")
elif command -v uvx > /dev/null; then
  ruff=(uvx --quiet "ruff~=0.8.4")
else
  echo "ruff hook: no ruff found, skipping (ruff check still runs in nox -s lint)." >&2
  exit 0
fi
if out=$("${ruff[@]}" format --check --quiet "$file" 2>&1); then
  exit 0
fi
echo "ruff: $file is not formatted. Run 'nox -s format' (or 'ruff format $file') and re-check.${out:+ $out}" >&2
exit 2
