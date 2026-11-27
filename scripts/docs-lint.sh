#!/usr/bin/env bash
# Check changed Markdown docs and selected root files, including untracked files.
# Unchanged files do not get stale warnings. Use --warn-only for advisory CI.
set -euo pipefail
base=${BASE_REF:-origin/master}
mode=strict
while [ "$#" -gt 0 ]; do
  case "$1" in
    --base) base=$2; shift 2 ;;
    --strict) mode=strict; shift ;;
    --warn-only) mode=warn; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
root=$(git rev-parse --show-toplevel)
cd "$root"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
if ! ancestor=$(git merge-base "$base" HEAD); then
  echo "ERROR: cannot find merge base for $base" >&2
  exit 2
fi
git diff --name-only -z --diff-filter=ACMR "$ancestor" -- 'README.md' 'AGENTS.md' 'CONTRIBUTING.md' 'RELEASE.md' 'SECURITY.md' 'llms.txt' 'doc/*.md' 'doc/**/*.md' > "$tmp/files"
git ls-files --others --exclude-standard -z -- 'README.md' 'AGENTS.md' 'CONTRIBUTING.md' 'RELEASE.md' 'SECURITY.md' 'llms.txt' 'doc/*.md' 'doc/**/*.md' >> "$tmp/files"
errors=0
report() {
  if [ "$mode" = warn ]; then echo "WARNING: $*"; else echo "ERROR: $*"; fi
  errors=$((errors + 1))
}
while IFS= read -r -d '' file; do
  [ -f "$file" ] || continue
  require_meta=0
  case "$file" in
    doc/*.md) if ! git cat-file -e "$ancestor:$file" 2>/dev/null; then require_meta=1; fi ;;
  esac
  if [ "$(head -n 1 "$file")" != '---' ]; then
    if [ "$require_meta" -eq 1 ]; then report "$file: missing frontmatter"; fi
  else
  awk 'NR == 1 {next} /^---[[:space:]]*$/ {exit} {print}' "$file" > "$tmp/meta"
  if ! awk 'NR > 1 && /^---[[:space:]]*$/ {found=1; exit} END {exit !found}' "$file"; then
    if [ "$require_meta" -eq 1 ]; then report "$file: unclosed frontmatter"; fi
  else
  # Remove comments outside quotes before checking the field values.
  awk '
    { quote=""; escaped=0; line=$0
      for (i=1; i<=length(line); i++) {
        c=substr(line,i,1); prev=i>1 ? substr(line,i-1,1) : ""
        if (quote=="\"" && c=="\\" && !escaped) {escaped=1; continue}
        if (!escaped && (c=="\"" || c==sprintf("%c",39))) {
          if (quote=="") quote=c; else if (quote==c) quote=""
        }
        if (c=="#" && quote=="" && prev ~ /[[:space:]]/) {line=substr(line,1,i-1); break}
        escaped=0
      }
      sub(/[[:space:]]+$/, "", line); print line
    }' "$tmp/meta" > "$tmp/meta-clean"
  mv "$tmp/meta-clean" "$tmp/meta"
  if [ "$require_meta" -eq 1 ]; then
  for field in type owner status stale_after; do
    entry=$(sed -n "s/^${field}:[[:space:]]*//p" "$tmp/meta" | head -n 1)
    if [ "$field" = owner ] && [ -z "$entry" ] && awk '/^owner:[[:space:]]*$/ {in_owner=1; next} in_owner && /^[[:space:]]+-[[:space:]]*[^[:space:]#]/ {found=1; exit} in_owner && /^[^[:space:]#]/ {exit} END {exit !found}' "$tmp/meta"; then
      continue
    fi
    case "$entry" in ''|\#*|'""'|"''"|'[]'|null|'~') report "$file: missing $field" ;; esac
  done
  fi
  value=$(sed -nE 's/^stale_after:[[:space:]]*["'"'"']?([^"'"'"'[:space:]]+).*$/\1/p' "$tmp/meta" | head -n 1)
  if [ -n "$value" ]; then
    if epoch=$(date -u -d "$value" +%s 2>/dev/null); then :
    else
      normalized=$(printf '%s' "$value" | sed -E 's/Z$/+0000/; s/([+-][0-9][0-9]):([0-9][0-9])$/\1\2/')
      epoch=$(date -j -u -f '%Y-%m-%dT%H:%M:%S%z' "$normalized" +%s 2>/dev/null) || epoch=$(date -j -u -f '%Y-%m-%d' "$normalized" +%s 2>/dev/null) || epoch=''
    fi
    if [ -z "$epoch" ]; then report "$file: invalid stale_after $value"
    elif [ "$epoch" -lt "$(date -u +%s)" ]; then echo "WARNING: $file: stale_after $value is in the past"; fi
  fi
  fi
  fi
  : > "$tmp/undefined"
  awk -v missing="$tmp/undefined" '
    function norm(x) {gsub(/[[:space:]]+/, " ", x); sub(/^ /, "", x); sub(/ $/, "", x); return tolower(x)}
    /^ ? ? ?(```|~~~)/ {
      marker=$0; sub(/^[[:space:]]*/, "", marker)
      char=substr(marker,1,1); run=marker
      if (char == "`") sub(/[^`].*$/, "", run); else sub(/[^~].*$/, "", run)
      if (!fence) {fence=char; length_open=length(run)}
      else if (char == fence && length(run) >= length_open && substr(marker,length(run)+1) ~ /^[[:space:]]*$/) fence=""
      next
    }
    fence {next}
    { line=$0; gsub(/`[^`]*`/, "", line)
      if (line ~ /^[[:space:]]*\[[^]]+\]:[[:space:]]*/) {
        label=line; sub(/^[[:space:]]*\[/, "", label); sub(/\].*$/, "", label)
        defs[norm(label)]=1
        sub(/^[[:space:]]*\[[^]]+\]:[[:space:]]*/, "", line)
        print line; next
      }
      scan=line
      while (match(scan, /\[[^][]+\]\[[^][]*\]/)) {
        ref=substr(scan,RSTART,RLENGTH); label=ref
        sub(/^\[[^]]+\]\[/, "", label); sub(/\]$/, "", label)
        if (label=="") {label=ref; sub(/^\[/, "", label); sub(/\].*$/, "", label)}
        refs[norm(label)]=1; scan=substr(scan,RSTART+RLENGTH)
      }
      while (match(line, /\]\(/)) {
        rest=substr(line,RSTART+2); depth=1; angle=0; end=0
        for (i=1; i<=length(rest); i++) {
          c=substr(rest,i,1)
          if (i==1 && c=="<") angle=1
          else if (angle && c==">") angle=0
          else if (!angle && c=="(") depth++
          else if (!angle && c==")") {depth--; if (depth==0) {end=i; break}}
        }
        if (!end) break
        print substr(rest,1,end-1); line=substr(rest,end+1)
      }
    }
    END {for (label in refs) if (!(label in defs)) print label > missing}
    ' "$file" > "$tmp/links"
  while IFS= read -r label; do
    [ -n "$label" ] && report "$file: broken reference $label"
  done < "$tmp/undefined"
  while IFS= read -r target; do
    case "$target" in
      '<'*'>'*) target=${target#<}; target=${target%%>*} ;;
      *) target=${target%%[[:space:]]*} ;;
    esac
    target=${target%%\#*}; target=${target%%\?*}
    case "$target" in ''|/*|*://*|mailto:*|tel:*) continue ;; esac
    encoded=$(printf '%s' "$target" | sed -E 's/%([0-9A-Fa-f]{2})/\\x\1/g')
    target=$(printf '%b' "$encoded")
    if [ ! -e "$(dirname "$file")/$target" ]; then report "$file: broken link $target"; fi
  done < "$tmp/links"
done < "$tmp/files"
printf 'docs lint: %s problem(s)\n' "$errors"
if [ "$mode" = strict ] && [ "$errors" -gt 0 ]; then exit 1; fi
